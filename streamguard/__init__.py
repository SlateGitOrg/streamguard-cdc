"""streamguard-cdc: a change-data-capture stream processing pipeline.

Postgres WAL -> Debezium -> Kafka -> streamguard (dedupe, schema evolution,
windowed aggregation, dead-letter routing) -> ClickHouse.

This package contains only the *stream processor* -- the piece of the
architecture that sits between Kafka and ClickHouse. It is deliberately
built so that every unit of business logic (event parsing, schema
evolution, deduplication, windowing, dead-letter routing, sink writes) is
testable in-process with no network, no Docker daemon, and no external
services. The real Kafka/ClickHouse/Postgres clients are imported lazily
(only when you actually construct a "real" adapter), so the test suite
never needs them installed.
"""

__version__ = "1.0.0"
