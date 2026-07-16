"""Debezium change-event parsing and schema evolution.

Debezium (configured with the Postgres connector, `pgoutput` plugin)
publishes one JSON message per row change to a topic named
`<server>.<schema>.<table>`. The payload envelope looks like::

    {
      "schema": {...},
      "payload": {
        "before": {...} | null,
        "after": {...} | null,
        "source": {"table": "orders", "lsn": 123456, "ts_ms": 1690000000000, ...},
        "op": "c" | "u" | "d" | "r",
        "ts_ms": 1690000000000
      }
    }

This module turns that envelope into a small, strongly-typed
``ChangeEvent`` and tracks per-table schemas so that column additions,
removals, and type changes ("schema evolution") are handled gracefully
instead of crashing the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class Op(str, Enum):
    """Debezium operation codes."""

    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"  # initial snapshot


class MalformedEventError(ValueError):
    """Raised when a Kafka record cannot be parsed as a Debezium envelope."""


class SchemaValidationError(ValueError):
    """Raised when a parsed event fails required-field validation."""


@dataclass(frozen=True)
class ChangeEvent:
    """A normalized row-level change event."""

    table: str
    op: Op
    key: dict[str, Any]
    before: Optional[dict[str, Any]]
    after: Optional[dict[str, Any]]
    source_lsn: Optional[int]
    ts_ms: int
    kafka_offset: Optional[int] = None
    kafka_partition: Optional[int] = None

    @property
    def row(self) -> Optional[dict[str, Any]]:
        """The row image relevant to sinks: `after` for c/u/r, `before` for d."""
        return self.before if self.op == Op.DELETE else self.after

    @property
    def dedup_key(self) -> tuple:
        """Stable identity for deduplication purposes.

        Debezium can redeliver the same change (e.g. after a connector
        restart or Kafka rebalance). The tuple of (table, primary key,
        source LSN) uniquely identifies one committed WAL change, so two
        deliveries of the same change produce an identical dedup_key.
        """
        key_tuple = tuple(sorted(self.key.items())) if self.key else ()
        return (self.table, key_tuple, self.source_lsn, self.op.value)


REQUIRED_ENVELOPE_FIELDS = ("payload",)
REQUIRED_PAYLOAD_FIELDS = ("op", "source")


def parse_debezium_envelope(raw: dict[str, Any], *, kafka_key: Optional[dict[str, Any]] = None,
                             kafka_offset: Optional[int] = None,
                             kafka_partition: Optional[int] = None) -> ChangeEvent:
    """Parse a raw decoded-JSON Kafka value into a :class:`ChangeEvent`.

    Raises :class:`MalformedEventError` for structurally broken input and
    :class:`SchemaValidationError` for input that parses but is missing
    fields required to process the change safely. Both are caught by the
    pipeline and routed to the dead-letter topic rather than crashing the
    consumer loop.
    """
    if not isinstance(raw, dict):
        raise MalformedEventError(f"expected a JSON object, got {type(raw).__name__}")

    for field_name in REQUIRED_ENVELOPE_FIELDS:
        if field_name not in raw:
            raise MalformedEventError(f"envelope missing required field '{field_name}'")

    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise MalformedEventError("envelope 'payload' must be an object")

    for field_name in REQUIRED_PAYLOAD_FIELDS:
        if field_name not in payload:
            raise SchemaValidationError(f"payload missing required field '{field_name}'")

    op_raw = payload["op"]
    try:
        op = Op(op_raw)
    except ValueError as exc:
        raise SchemaValidationError(f"unknown op code '{op_raw}'") from exc

    source = payload["source"]
    if not isinstance(source, dict) or "table" not in source:
        raise SchemaValidationError("payload.source missing 'table'")

    before = payload.get("before")
    after = payload.get("after")
    if op in (Op.CREATE, Op.UPDATE, Op.READ) and after is None:
        raise SchemaValidationError(f"op '{op.value}' requires a non-null 'after' image")
    if op == Op.DELETE and before is None:
        raise SchemaValidationError("op 'd' requires a non-null 'before' image")

    ts_ms = payload.get("ts_ms") or source.get("ts_ms") or 0

    key = kafka_key or {}
    if not key:
        # Fall back to deriving a key from the row image + a conventional
        # 'id' column, matching Debezium's default single-column key.
        row_image = after if after is not None else before
        if row_image and "id" in row_image:
            key = {"id": row_image["id"]}

    return ChangeEvent(
        table=source["table"],
        op=op,
        key=key,
        before=before,
        after=after,
        source_lsn=source.get("lsn"),
        ts_ms=int(ts_ms),
        kafka_offset=kafka_offset,
        kafka_partition=kafka_partition,
    )


@dataclass
class SchemaChange:
    """Describes a detected difference between an event's row shape and
    the previously-known shape for that table."""

    table: str
    added_columns: frozenset[str]
    removed_columns: frozenset[str]

    @property
    def is_empty(self) -> bool:
        return not self.added_columns and not self.removed_columns


@dataclass
class SchemaRegistry:
    """Tracks the observed set of columns per table and reconciles new
    row shapes against it.

    This implements the pipeline's schema-evolution handling without a
    real Confluent Schema Registry: Debezium/Postgres additive changes
    (``ALTER TABLE ADD COLUMN``) and column drops are the overwhelming
    majority of real-world schema drift, and both are handled here by
    widening the known column set and backfilling missing columns with
    ``None`` so downstream aggregation/sink code never KeyErrors.
    """

    _known_columns: dict[str, frozenset[str]] = field(default_factory=dict)

    def reconcile(self, event: ChangeEvent) -> tuple[dict[str, Any], Optional[SchemaChange]]:
        """Return a (normalized_row, schema_change_or_None) pair.

        The normalized row always contains every column ever seen for
        this table, with ``None`` filled in for columns absent from this
        particular event (e.g. because they were added after this row's
        version, or because Debezium is mid-rollout of an ALTER TABLE).
        """
        row = event.row or {}
        incoming_columns = frozenset(row.keys())
        previously_known = self._known_columns.get(event.table)

        if previously_known is None:
            self._known_columns[event.table] = incoming_columns
            normalized = dict(row)
            return normalized, None

        added = incoming_columns - previously_known
        removed = previously_known - incoming_columns

        all_columns = previously_known | incoming_columns
        self._known_columns[event.table] = all_columns

        normalized = {col: row.get(col) for col in all_columns}

        if not added and not removed:
            return normalized, None

        return normalized, SchemaChange(
            table=event.table, added_columns=frozenset(added), removed_columns=frozenset(removed)
        )

    def known_columns(self, table: str) -> frozenset[str]:
        return self._known_columns.get(table, frozenset())
