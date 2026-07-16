from streamguard.kafka_io import InMemoryBroker, InMemoryConsumer, InMemoryProducer


class TestInMemoryBrokerPubSub:
    def test_publish_and_consume_in_order(self):
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)
        consumer = InMemoryConsumer(broker, "topic-a")

        producer.send("topic-a", key={"id": 1}, value={"x": 1})
        producer.send("topic-a", key={"id": 2}, value={"x": 2})

        rec1 = consumer.poll()
        rec2 = consumer.poll()
        rec3 = consumer.poll()

        assert rec1.offset == 0 and rec1.value == {"x": 1}
        assert rec2.offset == 1 and rec2.value == {"x": 2}
        assert rec3 is None

    def test_commit_advances_lag_tracking(self):
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)
        consumer = InMemoryConsumer(broker, "topic-a")
        producer.send("topic-a", None, {"x": 1})
        producer.send("topic-a", None, {"x": 2})

        assert consumer.lag() == 2
        rec = consumer.poll()
        consumer.commit(rec)
        assert consumer.lag() == 1

    def test_separate_topics_are_isolated(self):
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)
        producer.send("topic-a", None, {"x": 1})
        producer.send("topic-b", None, {"y": 1})

        consumer_a = InMemoryConsumer(broker, "topic-a")
        consumer_b = InMemoryConsumer(broker, "topic-b")

        assert consumer_a.poll().value == {"x": 1}
        assert consumer_b.poll().value == {"y": 1}


class TestBrokerOutageSimulation:
    def test_producer_buffers_while_broker_killed(self):
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)

        broker.kill()
        producer.send("topic-a", None, {"x": 1})
        producer.send("topic-a", None, {"x": 2})

        assert broker.high_watermark("topic-a") == 0
        assert len(producer.pending) == 2

    def test_flush_drains_buffer_after_revive(self):
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)

        broker.kill()
        producer.send("topic-a", None, {"x": 1})
        producer.send("topic-a", None, {"x": 2})
        broker.revive()
        producer.flush()

        assert broker.high_watermark("topic-a") == 2
        assert len(producer.pending) == 0
        assert producer.sent_count == 2

    def test_no_data_loss_across_outage(self):
        """The core failure-injection property: every message sent during
        an outage is still delivered once the broker comes back, in order."""
        broker = InMemoryBroker()
        producer = InMemoryProducer(broker)
        consumer = InMemoryConsumer(broker, "topic-a")

        producer.send("topic-a", None, {"seq": 0})
        broker.kill()
        for i in range(1, 11):
            producer.send("topic-a", None, {"seq": i})
        broker.revive()
        producer.flush()

        delivered = []
        while True:
            rec = consumer.poll()
            if rec is None:
                break
            delivered.append(rec.value["seq"])

        assert delivered == list(range(11))
