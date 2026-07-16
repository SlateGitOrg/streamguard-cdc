"""ClickHouse sink abstraction.

Like ``kafka_io``, the pipeline depends only on the ``SinkLike``
protocol. ``InMemorySink`` (used by every unit test) is a faithful model
of ClickHouse's upsert-by-replace semantics for the raw mirror table
(ReplacingMergeTree keyed on primary key + version) and simple
append-only semantics for the aggregate table. ``RealClickHouseSink``
talks to a real cluster over HTTP and is exercised only by the optional
Testcontainers integration test.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol


class SinkLike(Protocol):
    def upsert_row(self, table: str, row: dict[str, Any], *, version: int) -> None:
        ...

    def delete_row(self, table: str, key: dict[str, Any], *, version: int) -> None:
        ...

    def write_aggregate(self, table: str, row: dict[str, Any]) -> None:
        ...


@dataclass
class InMemorySink:
    """In-process model of the two ClickHouse tables streamguard writes:

    * a raw mirror table per source table, keyed by primary key, using
      ReplacingMergeTree(version) semantics: the row with the highest
      `version` wins, and a `deleted` flag marks tombstones instead of a
      physical delete (matching how ClickHouse handles CDC deletes).
    * an aggregates table, append-only, one row per finalized window.
    """

    rows: dict[str, dict[tuple, dict[str, Any]]] = field(default_factory=dict)
    aggregates: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    write_count: int = 0

    def _pk(self, row: dict[str, Any]) -> tuple:
        if "id" in row:
            return ("id", row["id"])
        return tuple(sorted(row.items()))

    def upsert_row(self, table: str, row: dict[str, Any], *, version: int) -> None:
        table_rows = self.rows.setdefault(table, {})
        pk = self._pk(row)
        existing = table_rows.get(pk)
        if existing is not None and existing["_version"] >= version:
            return  # ReplacingMergeTree: higher version wins, stale write is a no-op
        stored = dict(row)
        stored["_version"] = version
        stored["_deleted"] = 0
        table_rows[pk] = stored
        self.write_count += 1

    def delete_row(self, table: str, key: dict[str, Any], *, version: int) -> None:
        table_rows = self.rows.setdefault(table, {})
        pk = self._pk(key)
        existing = table_rows.get(pk)
        if existing is not None and existing["_version"] >= version:
            return
        table_rows[pk] = {**key, "_version": version, "_deleted": 1}
        self.write_count += 1

    def write_aggregate(self, table: str, row: dict[str, Any]) -> None:
        self.aggregates.setdefault(table, []).append(row)
        self.write_count += 1

    def live_rows(self, table: str) -> list[dict[str, Any]]:
        """Rows that are not tombstoned -- what a `WHERE _deleted = 0`
        query against the real table would return."""
        return [r for r in self.rows.get(table, {}).values() if not r["_deleted"]]


class RealClickHouseSink:
    """Writes to a real ClickHouse server over its HTTP interface.

    Only ``requests`` (already a common transitive dependency) is used,
    imported lazily so it is not a hard requirement for running the unit
    test suite.
    """

    def __init__(self, url: str = "http://localhost:8123", database: str = "streamguard"):
        self._url = url
        self._database = database

    def _execute(self, query: str) -> None:
        import requests  # lazy import -- optional dependency

        resp = requests.post(self._url, params={"database": self._database}, data=query.encode("utf-8"), timeout=10)
        resp.raise_for_status()

    def upsert_row(self, table: str, row: dict[str, Any], *, version: int) -> None:
        payload = {**row, "_version": version, "_deleted": 0}
        self._execute(f"INSERT INTO {table} FORMAT JSONEachRow\n{json.dumps(payload)}")

    def delete_row(self, table: str, key: dict[str, Any], *, version: int) -> None:
        payload = {**key, "_version": version, "_deleted": 1}
        self._execute(f"INSERT INTO {table} FORMAT JSONEachRow\n{json.dumps(payload)}")

    def write_aggregate(self, table: str, row: dict[str, Any]) -> None:
        self._execute(f"INSERT INTO {table} FORMAT JSONEachRow\n{json.dumps(row)}")
