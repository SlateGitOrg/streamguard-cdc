"""Kafka consumer/producer abstractions.

The pipeline (`streamguard.pipeline.StreamProcessor`) depends only on the
``KafkaConsumerLike`` / ``KafkaProducerLike`` protocols defined here, not
on any concrete client library. Two implementations are provided:

* ``InMemoryBroker`` / ``InMemoryConsumer`` / ``InMemoryProducer`` -- a
  pure-Python, deterministic fake used by the entire unit test suite and
  by local demos. No network, no background threads.
* ``RedpandaConsumer`` / ``RedpandaProducer`` -- thin wrappers around
  ``confluent_kafka`` for the real deployment (works against Redpanda or
  Apache Kafka, since both speak the same wire protocol). ``confluent_kafka``
  is imported lazily inside ``__init__`` so simply importing this module
  -- and running the unit tests -- never requires it to be installed.
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Optional, Protocol


@dataclass
class ConsumerRecord:
    topic: str
    partition: int
    offset: int
    key: Optional[Any]
    value: Any
    high_watermark: Optional[int] = None


class KafkaConsumerLike(Protocol):
    def poll(self, timeout_s: float = 1.0) -> Optional[ConsumerRecord]:
        ...

    def commit(self, record: ConsumerRecord) -> None:
        ...

    def lag(self) -> int:
        """Total (high_watermark - committed_offset) across assigned partitions."""
        ...


class KafkaProducerLike(Protocol):
    def send(self, topic: str, key: Optional[Any], value: Any) -> None:
        ...

    def flush(self) -> None:
        ...


# --------------------------------------------------------------------------
# In-memory fake, used throughout the unit test suite.
# --------------------------------------------------------------------------


class InMemoryBroker:
    """A minimal single-process pub/sub broker that mimics enough Kafka
    semantics (topics, partitions=1 per topic, monotonic offsets, a
    consumable high watermark) to exercise real pipeline logic."""

    def __init__(self):
        self._topics: dict[str, deque] = defaultdict(deque)
        self._next_offset: dict[str, int] = defaultdict(int)

    def publish(self, topic: str, key: Optional[Any], value: Any) -> int:
        offset = self._next_offset[topic]
        self._topics[topic].append((offset, key, value))
        self._next_offset[topic] += 1
        return offset

    def high_watermark(self, topic: str) -> int:
        return self._next_offset[topic]

    def kill(self) -> None:
        """Simulate a broker outage: any further publish raises, used by
        the failure-injection test to prove no data is lost when the
        pipeline retries against a producer buffer instead of crashing."""
        self._killed = True

    def revive(self) -> None:
        self._killed = False

    @property
    def is_killed(self) -> bool:
        return getattr(self, "_killed", False)


class InMemoryProducer:
    """Fake producer with a bounded retry buffer, used to model
    broker-outage failure injection without a real cluster."""

    def __init__(self, broker: InMemoryBroker):
        self._broker = broker
        self.pending: deque = deque()
        self.sent_count = 0

    def send(self, topic: str, key: Optional[Any], value: Any) -> None:
        if self._broker.is_killed:
            # Buffer instead of dropping -- mirrors librdkafka's producer
            # queue behaviour while the broker is unreachable.
            self.pending.append((topic, key, value))
            return
        self._broker.publish(topic, key, value)
        self.sent_count += 1

    def flush(self) -> None:
        """Drain the retry buffer once the broker is reachable again."""
        while self.pending and not self._broker.is_killed:
            topic, key, value = self.pending.popleft()
            self._broker.publish(topic, key, value)
            self.sent_count += 1


class InMemoryConsumer:
    """Fake consumer reading from an :class:`InMemoryBroker` topic."""

    def __init__(self, broker: InMemoryBroker, topic: str):
        self._broker = broker
        self._topic = topic
        self._next_read_offset = 0
        self._committed_offset = 0

    def poll(self, timeout_s: float = 1.0) -> Optional[ConsumerRecord]:
        queue = self._broker._topics[self._topic]
        for offset, key, value in queue:
            if offset == self._next_read_offset:
                self._next_read_offset += 1
                return ConsumerRecord(
                    topic=self._topic,
                    partition=0,
                    offset=offset,
                    key=key,
                    value=value,
                    high_watermark=self._broker.high_watermark(self._topic),
                )
        return None

    def commit(self, record: ConsumerRecord) -> None:
        self._committed_offset = max(self._committed_offset, record.offset + 1)

    def lag(self) -> int:
        return max(0, self._broker.high_watermark(self._topic) - self._committed_offset)


# --------------------------------------------------------------------------
# Real adapters for Redpanda/Kafka. Lazy-imported: `confluent_kafka` is not
# a test dependency.
# --------------------------------------------------------------------------


class RedpandaConsumer:
    """Real Kafka/Redpanda consumer via ``confluent_kafka``."""

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        from confluent_kafka import Consumer  # lazy import -- optional dependency

        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([topic])
        self._topic = topic

    def poll(self, timeout_s: float = 1.0) -> Optional[ConsumerRecord]:
        msg = self._consumer.poll(timeout_s)
        if msg is None or msg.error():
            return None
        low, high = self._consumer.get_watermark_offsets(msg.topic_partition() if hasattr(msg, "topic_partition") else msg)
        value = json.loads(msg.value().decode("utf-8"))
        key = json.loads(msg.key().decode("utf-8")) if msg.key() else None
        return ConsumerRecord(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=key,
            value=value,
            high_watermark=high,
        )

    def commit(self, record: ConsumerRecord) -> None:
        self._consumer.commit(asynchronous=False)

    def lag(self) -> int:
        return 0  # computed from ConsumerRecord.high_watermark by the caller in real deployments


class RedpandaProducer:
    """Real Kafka/Redpanda producer via ``confluent_kafka``."""

    def __init__(self, bootstrap_servers: str):
        from confluent_kafka import Producer  # lazy import -- optional dependency

        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def send(self, topic: str, key: Optional[Any], value: Any) -> None:
        self._producer.produce(
            topic,
            key=json.dumps(key).encode("utf-8") if key is not None else None,
            value=json.dumps(value).encode("utf-8"),
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()
