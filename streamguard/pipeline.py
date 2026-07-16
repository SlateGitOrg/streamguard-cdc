"""The stream processor: wires Kafka -> dedup -> schema evolution ->
windowed aggregation -> ClickHouse, with a dead-letter branch.

This is the module the architecture diagram's "stream processor" box
refers to. It is infrastructure-agnostic: give it anything that satisfies
``KafkaConsumerLike``/``KafkaProducerLike``/``SinkLike`` and it behaves
identically whether that's the in-memory fakes (unit tests) or the real
Redpanda/ClickHouse adapters (production, via `main.py`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from streamguard.clickhouse_sink import SinkLike
from streamguard.dedup import Deduplicator
from streamguard.dlq import DeadLetterRouter
from streamguard.events import (
    ChangeEvent,
    MalformedEventError,
    Op,
    SchemaRegistry,
    SchemaValidationError,
    parse_debezium_envelope,
)
from streamguard.kafka_io import ConsumerRecord, KafkaConsumerLike
from streamguard.metrics import PipelineMetrics
from streamguard.windowing import TumblingWindowAggregator

logger = logging.getLogger("streamguard.pipeline")


@dataclass
class AggregationRule:
    """Which table/columns feed the windowed aggregation, and where its
    output lands in ClickHouse."""

    source_table: str
    sink_table: str
    key_fn: Callable[[dict], Any]
    value_fn: Callable[[dict], float]


DEFAULT_AGGREGATION_RULE = AggregationRule(
    source_table="orders",
    sink_table="orders_revenue_by_minute",
    key_fn=lambda row: row.get("store_id"),
    value_fn=lambda row: float(row.get("amount") or 0.0),
)


@dataclass
class ProcessResult:
    """What happened to one polled record -- returned for testability."""

    outcome: str  # "processed" | "duplicate" | "dead_lettered" | "empty"
    event: Optional[ChangeEvent] = None
    schema_changed: bool = False
    windows_emitted: int = 0


class StreamProcessor:
    def __init__(
        self,
        *,
        consumer: KafkaConsumerLike,
        dlq_router: DeadLetterRouter,
        sink: SinkLike,
        dedup: Optional[Deduplicator] = None,
        schema_registry: Optional[SchemaRegistry] = None,
        aggregation_rule: AggregationRule = DEFAULT_AGGREGATION_RULE,
        window_size_ms: int = 60_000,
        allowed_lateness_ms: int = 10_000,
        metrics: Optional[PipelineMetrics] = None,
        source_topic: str = "streamguard.public.orders",
    ):
        self.consumer = consumer
        self.dlq_router = dlq_router
        self.sink = sink
        self.dedup = dedup or Deduplicator()
        self.schema_registry = schema_registry or SchemaRegistry()
        self.aggregation_rule = aggregation_rule
        self.aggregator = TumblingWindowAggregator(
            window_size_ms=window_size_ms,
            allowed_lateness_ms=allowed_lateness_ms,
            key_fn=aggregation_rule.key_fn,
            value_fn=aggregation_rule.value_fn,
        )
        self.metrics = metrics or PipelineMetrics()
        self.source_topic = source_topic

    def process_one(self, timeout_s: float = 1.0) -> ProcessResult:
        """Poll and fully process exactly one Kafka record (or return
        ``outcome="empty"`` if none is available)."""
        record = self.consumer.poll(timeout_s)
        if record is None:
            return ProcessResult(outcome="empty")

        self.metrics.record_consumed()

        try:
            event = parse_debezium_envelope(
                record.value,
                kafka_key=record.key,
                kafka_offset=record.offset,
                kafka_partition=record.partition,
            )
        except (MalformedEventError, SchemaValidationError) as exc:
            self._dead_letter(record, exc)
            return ProcessResult(outcome="dead_lettered")

        if self.dedup.is_duplicate(event):
            self.metrics.record_duplicate()
            self.consumer.commit(record)
            self._update_lag()
            return ProcessResult(outcome="duplicate", event=event)

        normalized_row, schema_change = self.schema_registry.reconcile(event)
        if schema_change is not None:
            self.metrics.record_schema_change()
            logger.info(
                "schema evolution on %s: +%s -%s",
                schema_change.table,
                sorted(schema_change.added_columns),
                sorted(schema_change.removed_columns),
            )

        try:
            self._write_to_sink(event, normalized_row)
        except Exception as exc:  # noqa: BLE001 -- any sink failure is dead-lettered, not fatal
            self._dead_letter(record, exc)
            return ProcessResult(outcome="dead_lettered")

        windows_emitted = 0
        if event.table == self.aggregation_rule.source_table and event.op != Op.DELETE:
            self.aggregator.add(normalized_row, event.ts_ms)
            for window in self.aggregator.poll_completed_windows():
                self.sink.write_aggregate(
                    self.aggregation_rule.sink_table,
                    {
                        "group_key": window.key,
                        "window_start_ms": window.window_start_ms,
                        "window_end_ms": window.window_end_ms,
                        "count": window.count,
                        "sum": window.sum,
                        "avg": window.avg,
                        "min": window.min,
                        "max": window.max,
                    },
                )
                windows_emitted += 1
            if windows_emitted:
                self.metrics.record_window_emitted(windows_emitted)

        self.consumer.commit(record)
        self._update_lag()
        return ProcessResult(
            outcome="processed",
            event=event,
            schema_changed=schema_change is not None,
            windows_emitted=windows_emitted,
        )

    def run_until_empty(self, max_iterations: int = 1_000_000) -> list[ProcessResult]:
        """Drain the consumer until ``poll`` returns nothing. Used by
        tests and offline demos where the source is a finite fixture."""
        results = []
        for _ in range(max_iterations):
            result = self.process_one()
            if result.outcome == "empty":
                break
            results.append(result)
        return results

    def shutdown(self) -> list[Any]:
        """Force-flush any open aggregation windows so no buffered data
        is lost on a graceful stop."""
        flushed = self.aggregator.flush()
        for window in flushed:
            self.sink.write_aggregate(
                self.aggregation_rule.sink_table,
                {
                    "group_key": window.key,
                    "window_start_ms": window.window_start_ms,
                    "window_end_ms": window.window_end_ms,
                    "count": window.count,
                    "sum": window.sum,
                    "avg": window.avg,
                    "min": window.min,
                    "max": window.max,
                },
            )
        if flushed:
            self.metrics.record_window_emitted(len(flushed))
        return flushed

    def _write_to_sink(self, event: ChangeEvent, normalized_row: dict) -> None:
        version = event.source_lsn if event.source_lsn is not None else (event.kafka_offset or 0)
        if event.op == Op.DELETE:
            self.sink.delete_row(event.table, event.key, version=version)
        else:
            self.sink.upsert_row(event.table, normalized_row, version=version)
        self.metrics.record_sink_write()

    def _dead_letter(self, record: ConsumerRecord, error: Exception) -> None:
        self.dlq_router.route(
            original_value=record.value,
            error=error,
            source_topic=record.topic,
            kafka_offset=record.offset,
            kafka_partition=record.partition,
        )
        self.metrics.record_dlq()
        self.consumer.commit(record)
        self._update_lag()

    def _update_lag(self) -> None:
        try:
            self.metrics.set_lag(self.consumer.lag())
        except Exception:  # noqa: BLE001 -- lag reporting must never break processing
            pass
