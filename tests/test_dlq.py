from streamguard.dlq import DeadLetterRouter
from streamguard.kafka_io import InMemoryBroker, InMemoryProducer


class TestDeadLetterRouter:
    def test_route_publishes_to_dlq_topic(self):
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)
        router = DeadLetterRouter(producer, dlq_topic="streamguard.dlq")

        error = ValueError("boom")
        router.route(
            original_value={"bad": "data"},
            error=error,
            source_topic="streamguard.public.orders",
            kafka_offset=17,
            kafka_partition=0,
        )

        assert broker.high_watermark("streamguard.dlq") == 1
        assert router.routed_count == 1

    def test_dead_letter_record_captures_error_details(self):
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)
        router = DeadLetterRouter(producer, dlq_topic="streamguard.dlq")

        dead_letter = router.route(
            original_value="raw-bad-json",
            error=KeyError("op"),
            source_topic="streamguard.public.orders",
            kafka_offset=5,
            kafka_partition=1,
            retry_count=2,
        )

        record = dead_letter.to_record()
        assert record["error_type"] == "KeyError"
        assert "op" in record["error_message"]
        assert record["source_topic"] == "streamguard.public.orders"
        assert record["kafka_offset"] == 5
        assert record["kafka_partition"] == 1
        assert record["retry_count"] == 2
        assert record["original_value"] == "raw-bad-json"

    def test_multiple_routes_accumulate_count(self):
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)
        router = DeadLetterRouter(producer, dlq_topic="streamguard.dlq")
        for i in range(5):
            router.route(original_value=i, error=ValueError("x"), source_topic="t")
        assert router.routed_count == 5
        assert broker.high_watermark("streamguard.dlq") == 5
