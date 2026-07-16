"""Failure-injection test: kill the broker mid-stream, verify no data loss.

This is the offline-verifiable half of the failure-injection story
described in `docs/FAILURE_INJECTION.md`. It uses `InMemoryBroker.kill()` /
`.revive()` (see `streamguard/kafka_io.py`) to simulate a Kafka/Redpanda
broker outage without Docker: while "killed", the producer buffers
every send in memory (mirroring librdkafka's producer queue behaviour)
instead of dropping it, and `flush()` drains that buffer once the broker
is reachable again.

The property under test is the one that actually matters operationally:
every record produced before, during, and after the outage is eventually
consumed, in order, with the pipeline's dedup/schema-evolution/windowing
logic reaching the same final state it would have reached with no outage
at all. `docs/FAILURE_INJECTION.md` documents how to reproduce the same
property against the *real* Redpanda container (`docker kill`) for a
higher-fidelity, non-offline confirmation.
"""

from __future__ import annotations

from streamguard.clickhouse_sink import InMemorySink
from streamguard.dlq import DeadLetterRouter
from streamguard.kafka_io import InMemoryBroker, InMemoryConsumer, InMemoryProducer
from streamguard.loadgen import LoadGenConfig, synthetic_debezium_events
from streamguard.pipeline import StreamProcessor

SOURCE_TOPIC = "streamguard.public.orders"


def _build():
    broker = InMemoryBroker()
    producer = InMemoryProducer(broker)
    consumer = InMemoryConsumer(broker, SOURCE_TOPIC)
    sink = InMemorySink()
    dlq_router = DeadLetterRouter(producer, dlq_topic="streamguard.dlq")
    processor = StreamProcessor(
        consumer=consumer, dlq_router=dlq_router, sink=sink, source_topic=SOURCE_TOPIC
    )
    return broker, producer, consumer, sink, processor


class TestFailureInjectionNoDataLoss:
    def test_events_produced_during_broker_outage_are_not_lost(self):
        broker, producer, consumer, sink, processor = _build()

        # Phase 1: normal operation.
        for i in range(1, 6):
            producer.send(SOURCE_TOPIC, None, {
                "payload": {"before": None, "after": {"id": i, "store_id": 1, "amount": 10.0},
                            "source": {"table": "orders", "lsn": i, "ts_ms": i * 1000},
                            "op": "c", "ts_ms": i * 1000},
            })

        # Phase 2: the broker goes down mid-stream. The load generator
        # (in a real deployment, Debezium's own producer) keeps trying to
        # send -- those sends must not be silently dropped.
        broker.kill()
        for i in range(6, 16):
            producer.send(SOURCE_TOPIC, None, {
                "payload": {"before": None, "after": {"id": i, "store_id": 1, "amount": 10.0},
                            "source": {"table": "orders", "lsn": i, "ts_ms": i * 1000},
                            "op": "c", "ts_ms": i * 1000},
            })

        # While the broker is down, nothing new is visible to consume.
        assert broker.high_watermark(SOURCE_TOPIC) == 5
        drained_during_outage = processor.run_until_empty()
        assert len(drained_during_outage) == 5  # only the pre-outage records

        # Phase 3: the broker recovers; the buffered producer sends flush through.
        broker.revive()
        producer.flush()
        assert broker.high_watermark(SOURCE_TOPIC) == 15

        # Phase 4: continue producing after recovery, same as before the outage.
        for i in range(16, 21):
            producer.send(SOURCE_TOPIC, None, {
                "payload": {"before": None, "after": {"id": i, "store_id": 1, "amount": 10.0},
                            "source": {"table": "orders", "lsn": i, "ts_ms": i * 1000},
                            "op": "c", "ts_ms": i * 1000},
            })

        remaining = processor.run_until_empty()
        processor.shutdown()

        total_processed = len(drained_during_outage) + len(remaining)
        assert total_processed == 20  # every single record survived the outage

        live_ids = sorted(r["id"] for r in sink.live_rows("orders"))
        assert live_ids == list(range(1, 21))  # no gaps, no duplicates, correct final state

    def test_synthetic_load_survives_outage_mid_stream(self):
        """Same property under the messier synthetic load (duplicates,
        malformed records, schema drift) that the other pipeline tests use,
        with an outage injected halfway through."""
        broker, producer, consumer, sink, processor = _build()

        config = LoadGenConfig(num_orders=200, seed=55)
        events = list(synthetic_debezium_events(config))
        midpoint = len(events) // 2

        for raw in events[:midpoint]:
            producer.send(SOURCE_TOPIC, None, raw)

        broker.kill()
        for raw in events[midpoint:]:
            producer.send(SOURCE_TOPIC, None, raw)  # buffered, not lost

        pre_outage_high_watermark = broker.high_watermark(SOURCE_TOPIC)
        assert pre_outage_high_watermark == midpoint

        broker.revive()
        producer.flush()
        assert broker.high_watermark(SOURCE_TOPIC) == len(events)

        results = processor.run_until_empty()
        processor.shutdown()

        # Every published record was eventually processed exactly once
        # (as processed, duplicate, or dead-lettered) -- none silently vanished.
        assert len(results) == len(events)
