"""Dead-letter routing for events that cannot be processed.

Anything that fails to parse, fails schema validation, or throws while
being written to the sink is wrapped in a :class:`DeadLetter` envelope
(original payload + error class + message + retry count) and published
to the dead-letter topic instead of crashing the consumer loop or
silently dropping data. This keeps the main pipeline moving under
partial failure while preserving every unprocessable record for
inspection/replay.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class DeadLetter:
    original_value: Any
    error_type: str
    error_message: str
    source_topic: str
    kafka_offset: Optional[int]
    kafka_partition: Optional[int]
    failed_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    retry_count: int = 0

    def to_record(self) -> dict:
        """JSON-serializable representation, as would be produced onto
        the `<topic>.dlq` Kafka topic."""
        return {
            "original_value": self.original_value,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "source_topic": self.source_topic,
            "kafka_offset": self.kafka_offset,
            "kafka_partition": self.kafka_partition,
            "failed_at_ms": self.failed_at_ms,
            "retry_count": self.retry_count,
        }


class DeadLetterRouter:
    """Wraps a producer's `send` so callers can route failures uniformly."""

    def __init__(self, producer, dlq_topic: str):
        self._producer = producer
        self._dlq_topic = dlq_topic
        self.routed_count = 0

    def route(
        self,
        *,
        original_value: Any,
        error: Exception,
        source_topic: str,
        kafka_offset: Optional[int] = None,
        kafka_partition: Optional[int] = None,
        retry_count: int = 0,
    ) -> DeadLetter:
        dead_letter = DeadLetter(
            original_value=original_value,
            error_type=type(error).__name__,
            error_message=str(error),
            source_topic=source_topic,
            kafka_offset=kafka_offset,
            kafka_partition=kafka_partition,
            retry_count=retry_count,
        )
        self._producer.send(self._dlq_topic, key=None, value=dead_letter.to_record())
        self.routed_count += 1
        return dead_letter
