#!/usr/bin/env python3
"""Runs the full stream-processor pipeline end-to-end with zero external
services: an in-memory Kafka-like broker feeds a synthetic Debezium event
stream (redeliveries, malformed records, and a mid-stream schema change
included) through StreamProcessor into an in-memory ClickHouse-like sink,
and prints a summary. Useful for a fast, offline sanity check of the core
logic, and as a readable example of how the pieces wire together.

Usage:
    python3 scripts/run_offline_demo.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streamguard.clickhouse_sink import InMemorySink
from streamguard.dlq import DeadLetterRouter
from streamguard.kafka_io import InMemoryBroker, InMemoryConsumer, InMemoryProducer
from streamguard.loadgen import LoadGenConfig, synthetic_debezium_events
from streamguard.pipeline import StreamProcessor

SOURCE_TOPIC = "streamguard.public.orders"


def main() -> None:
    broker = InMemoryBroker()
    producer = InMemoryProducer(broker)
    consumer = InMemoryConsumer(broker, SOURCE_TOPIC)
    sink = InMemorySink()
    dlq_router = DeadLetterRouter(producer, dlq_topic="streamguard.dlq")

    processor = StreamProcessor(
        consumer=consumer,
        dlq_router=dlq_router,
        sink=sink,
        window_size_ms=60_000,
        allowed_lateness_ms=5_000,
        source_topic=SOURCE_TOPIC,
    )

    config = LoadGenConfig(num_orders=500, seed=2024)
    print(f"Generating {config.num_orders} synthetic orders (deterministic, seed={config.seed}) ...")
    for envelope in synthetic_debezium_events(config):
        producer.send(SOURCE_TOPIC, key=None, value=envelope)

    print(f"Published {broker.high_watermark(SOURCE_TOPIC)} raw Kafka records. Processing ...")
    results = processor.run_until_empty()
    flushed = processor.shutdown()

    outcomes: dict[str, int] = {}
    for r in results:
        outcomes[r.outcome] = outcomes.get(r.outcome, 0) + 1

    print()
    print("=== Pipeline results ===")
    for outcome, count in sorted(outcomes.items()):
        print(f"  {outcome:>14}: {count}")
    print()
    print("=== Metrics ===")
    print(processor.metrics.to_prometheus_text())
    print(f"Live (non-deleted) rows in ClickHouse 'orders' mirror: {len(sink.live_rows('orders'))}")
    print(f"Aggregation windows emitted (incl. final flush): {len(sink.aggregates.get('orders_revenue_by_minute', [])) }")
    print(f"Dead-lettered records on '{dlq_router._dlq_topic}': {dlq_router.routed_count}")
    print(f"Windows force-flushed on shutdown: {len(flushed)}")


if __name__ == "__main__":
    main()
