"""Exactly-once-ish deduplication for at-least-once Kafka delivery.

Kafka consumers (and Debezium producers) both provide at-least-once
delivery guarantees. A consumer-group rebalance, a producer retry, or a
connector restart from an older offset can all cause the same logical
row-change to arrive twice. ``Deduplicator`` recognizes redeliveries by
their stable :pyattr:`ChangeEvent.dedup_key` and drops them, using a
bounded, TTL-based LRU so memory usage stays flat under sustained load.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass

from streamguard.events import ChangeEvent


@dataclass
class DedupStats:
    seen: int = 0
    duplicates: int = 0
    evicted: int = 0


class Deduplicator:
    """A bounded, TTL-based "seen set" keyed on ``ChangeEvent.dedup_key``.

    Parameters
    ----------
    capacity:
        Maximum number of keys retained. Oldest keys are evicted first
        (LRU) once capacity is exceeded, bounding memory use.
    ttl_seconds:
        Keys older than this are treated as expired and evicted lazily
        on access, so a redelivery arriving after the TTL window is
        (correctly, given bounded memory) treated as new.
    clock:
        Injectable time source for deterministic tests.
    """

    def __init__(self, capacity: int = 100_000, ttl_seconds: float = 3600.0, clock=time.monotonic):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._capacity = capacity
        self._ttl = ttl_seconds
        self._clock = clock
        self._seen: "OrderedDict[tuple, float]" = OrderedDict()
        self.stats = DedupStats()

    def _evict_expired(self) -> None:
        now = self._clock()
        while self._seen:
            oldest_key, seen_at = next(iter(self._seen.items()))
            if now - seen_at > self._ttl:
                self._seen.pop(oldest_key)
                self.stats.evicted += 1
            else:
                break

    def is_duplicate(self, event: ChangeEvent) -> bool:
        """Return True (and record the key) if this event was already seen.

        Idempotent per call: calling twice with the same event returns
        False then True, exactly like a real seen-set.
        """
        self._evict_expired()
        key = event.dedup_key
        now = self._clock()

        if key in self._seen:
            self._seen.move_to_end(key)
            self._seen[key] = now
            self.stats.duplicates += 1
            return True

        self._seen[key] = now
        self.stats.seen += 1

        while len(self._seen) > self._capacity:
            self._seen.popitem(last=False)
            self.stats.evicted += 1

        return False

    def __len__(self) -> int:
        return len(self._seen)
