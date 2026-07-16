from streamguard.clickhouse_sink import InMemorySink
from streamguard.dlq import DeadLetterRouter
from streamguard.kafka_io import InMemoryBroker, InMemoryConsumer, InMemoryProducer
from streamguard.loadgen import LoadGenConfig, synthetic_debezium_events
from streamguard.pipeline import AggregationRule, StreamProcessor


SOURCE_TOPIC = "streamguard.public.orders"


def build_processor(window_size_ms=60_000, allowed_lateness_ms=10_000):
    broker = InMemoryBroker()
    producer = InMemoryProducer(broker)
    consumer = InMemoryConsumer(broker, SOURCE_TOPIC)
    sink = InMemorySink()
    dlq_router = DeadLetterRouter(producer, dlq_topic="streamguard.dlq")

    processor = StreamProcessor(
        consumer=consumer,
        dlq_router=dlq_router,
        sink=sink,
        aggregation_rule=AggregationRule(
            source_table="orders",
            sink_table="orders_revenue_by_minute",
            key_fn=lambda row: row.get("store_id"),
            value_fn=lambda row: float(row.get("amount") or 0.0),
        ),
        window_size_ms=window_size_ms,
        allowed_lateness_ms=allowed_lateness_ms,
        source_topic=SOURCE_TOPIC,
    )
    return broker, producer, sink, processor


def feed(producer, events):
    for raw in events:
        # Real Kafka delivers whatever JSON-decoded value Debezium produced,
        # which is a dict for well-formed envelopes and (for our injected
        # malformed cases) sometimes a bare string -- either way it's
        # whatever `json.loads` would have returned.
        producer.send(SOURCE_TOPIC, key=None, value=raw)


class TestStreamProcessorHappyPath:
    def test_single_create_event_is_upserted_to_sink(self):
        broker, producer, sink, processor = build_processor()
        feed(producer, [
            {
                "payload": {
                    "before": None,
                    "after": {"id": 1, "store_id": 1, "amount": 25.0, "status": "created"},
                    "source": {"table": "orders", "lsn": 100, "ts_ms": 1000},
                    "op": "c",
                    "ts_ms": 1000,
                }
            }
        ])
        results = processor.run_until_empty()
        assert len(results) == 1
        assert results[0].outcome == "processed"
        assert sink.live_rows("orders")[0]["amount"] == 25.0
        assert processor.metrics.consumed_total == 1
        assert processor.metrics.sink_writes_total == 1

    def test_update_then_delete_reaches_correct_final_state(self):
        broker, producer, sink, processor = build_processor()
        feed(producer, [
            {"payload": {"before": None, "after": {"id": 1, "store_id": 1, "amount": 10.0},
                         "source": {"table": "orders", "lsn": 1, "ts_ms": 1000}, "op": "c", "ts_ms": 1000}},
            {"payload": {"before": {"id": 1, "store_id": 1, "amount": 10.0},
                         "after": {"id": 1, "store_id": 1, "amount": 12.0},
                         "source": {"table": "orders", "lsn": 2, "ts_ms": 2000}, "op": "u", "ts_ms": 2000}},
            {"payload": {"before": {"id": 1, "store_id": 1, "amount": 12.0}, "after": None,
                         "source": {"table": "orders", "lsn": 3, "ts_ms": 3000}, "op": "d", "ts_ms": 3000}},
        ])
        processor.run_until_empty()
        assert sink.live_rows("orders") == []  # deleted -- tombstoned, not visible


class TestStreamProcessorDeduplication:
    def test_redelivered_event_is_not_double_written(self):
        broker, producer, sink, processor = build_processor()
        envelope = {
            "payload": {
                "before": None,
                "after": {"id": 1, "store_id": 1, "amount": 10.0},
                "source": {"table": "orders", "lsn": 1, "ts_ms": 1000},
                "op": "c",
                "ts_ms": 1000,
            }
        }
        feed(producer, [envelope, envelope])  # redelivered
        results = processor.run_until_empty()
        outcomes = [r.outcome for r in results]
        assert outcomes == ["processed", "duplicate"]
        assert processor.metrics.duplicates_total == 1
        assert sink.write_count == 1


class TestStreamProcessorSchemaEvolution:
    def test_added_column_flows_through_without_crashing(self):
        broker, producer, sink, processor = build_processor()
        feed(producer, [
            {"payload": {"before": None, "after": {"id": 1, "store_id": 1, "amount": 10.0},
                         "source": {"table": "orders", "lsn": 1, "ts_ms": 1000}, "op": "c", "ts_ms": 1000}},
            {"payload": {"before": None,
                         "after": {"id": 2, "store_id": 1, "amount": 20.0, "discount_code": "SAVE10"},
                         "source": {"table": "orders", "lsn": 2, "ts_ms": 2000}, "op": "c", "ts_ms": 2000}},
        ])
        results = processor.run_until_empty()
        assert results[1].schema_changed is True
        assert processor.metrics.schema_changes_total == 1
        row2 = [r for r in sink.live_rows("orders") if r["id"] == 2][0]
        assert row2["discount_code"] == "SAVE10"
        # The earlier row is untouched (schema evolution doesn't rewrite history)
        row1 = [r for r in sink.live_rows("orders") if r["id"] == 1][0]
        assert "discount_code" not in row1 or row1.get("discount_code") is None


class TestStreamProcessorDeadLettering:
    def test_malformed_event_is_dead_lettered_not_fatal(self):
        broker, producer, sink, processor = build_processor()
        feed(producer, ["not-a-json-object", {"payload": {"op": "c", "source": {"table": "orders"}}}])
        # second event above is missing 'after' for a create -> SchemaValidationError
        results = processor.run_until_empty()
        assert [r.outcome for r in results] == ["dead_lettered", "dead_lettered"]
        assert processor.metrics.dlq_total == 2
        assert broker.high_watermark("streamguard.dlq") == 2

    def test_valid_events_still_processed_after_a_dead_letter(self):
        broker, producer, sink, processor = build_processor()
        feed(producer, [
            "garbage",
            {"payload": {"before": None, "after": {"id": 1, "store_id": 1, "amount": 5.0},
                         "source": {"table": "orders", "lsn": 1, "ts_ms": 1000}, "op": "c", "ts_ms": 1000}},
        ])
        results = processor.run_until_empty()
        assert results[0].outcome == "dead_lettered"
        assert results[1].outcome == "processed"
        assert sink.live_rows("orders")[0]["id"] == 1


class TestStreamProcessorWindowedAggregation:
    def test_completed_window_writes_aggregate_row(self):
        broker, producer, sink, processor = build_processor(window_size_ms=60_000, allowed_lateness_ms=1_000)
        feed(producer, [
            {"payload": {"before": None, "after": {"id": 1, "store_id": 1, "amount": 10.0},
                         "source": {"table": "orders", "lsn": 1, "ts_ms": 1_000}, "op": "c", "ts_ms": 1_000}},
            {"payload": {"before": None, "after": {"id": 2, "store_id": 1, "amount": 20.0},
                         "source": {"table": "orders", "lsn": 2, "ts_ms": 2_000}, "op": "c", "ts_ms": 2_000}},
            # this event's timestamp is far enough ahead to close the [0, 60000) window
            {"payload": {"before": None, "after": {"id": 3, "store_id": 1, "amount": 999.0},
                         "source": {"table": "orders", "lsn": 3, "ts_ms": 200_000}, "op": "c", "ts_ms": 200_000}},
        ])
        results = processor.run_until_empty()
        emitted = [r for r in results if r.windows_emitted > 0]
        assert len(emitted) == 1
        agg_rows = sink.aggregates["orders_revenue_by_minute"]
        assert len(agg_rows) == 1
        assert agg_rows[0]["sum"] == 30.0
        assert agg_rows[0]["count"] == 2
        assert processor.metrics.windows_emitted_total == 1

    def test_shutdown_flushes_open_window(self):
        broker, producer, sink, processor = build_processor()
        feed(producer, [
            {"payload": {"before": None, "after": {"id": 1, "store_id": 1, "amount": 42.0},
                         "source": {"table": "orders", "lsn": 1, "ts_ms": 1_000}, "op": "c", "ts_ms": 1_000}},
        ])
        processor.run_until_empty()
        assert sink.aggregates == {}  # window still open, not yet emitted

        flushed = processor.shutdown()
        assert len(flushed) == 1
        assert sink.aggregates["orders_revenue_by_minute"][0]["sum"] == 42.0

    def test_deletes_do_not_feed_the_aggregation(self):
        broker, producer, sink, processor = build_processor(window_size_ms=60_000, allowed_lateness_ms=0)
        feed(producer, [
            {"payload": {"before": None, "after": {"id": 1, "store_id": 1, "amount": 100.0},
                         "source": {"table": "orders", "lsn": 1, "ts_ms": 1_000}, "op": "c", "ts_ms": 1_000}},
            {"payload": {"before": {"id": 1, "store_id": 1, "amount": 100.0}, "after": None,
                         "source": {"table": "orders", "lsn": 2, "ts_ms": 2_000}, "op": "d", "ts_ms": 2_000}},
        ])
        processor.run_until_empty()
        flushed = processor.shutdown()
        assert flushed[0].count == 1  # only the create contributed, not the delete
        assert flushed[0].sum == 100.0


class TestStreamProcessorAgainstSyntheticLoad:
    def test_full_synthetic_stream_processes_without_crashing(self):
        broker, producer, sink, processor = build_processor(window_size_ms=60_000, allowed_lateness_ms=5_000)
        config = LoadGenConfig(num_orders=300, seed=42)
        events = list(synthetic_debezium_events(config))
        feed(producer, events)

        results = processor.run_until_empty()
        processor.shutdown()

        outcomes = {}
        for r in results:
            outcomes[r.outcome] = outcomes.get(r.outcome, 0) + 1

        assert outcomes.get("dead_lettered", 0) > 0  # malformed records were injected
        assert outcomes.get("duplicate", 0) > 0  # redeliveries were injected
        assert outcomes.get("processed", 0) > 0
        assert processor.metrics.schema_changes_total >= 1  # discount_code column appeared mid-stream

        # every live (non-tombstoned) row respects the final schema shape
        for row in sink.live_rows("orders"):
            assert "id" in row and "amount" in row

        # no exception escaped -- the whole 300-order synthetic stream, with
        # its injected garbage, drained cleanly to completion
        assert len(results) == len(events)

    def test_synthetic_generator_is_deterministic(self):
        events_a = list(synthetic_debezium_events(LoadGenConfig(num_orders=50, seed=7)))
        events_b = list(synthetic_debezium_events(LoadGenConfig(num_orders=50, seed=7)))
        assert events_a == events_b
