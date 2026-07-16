"""Tumbling-window aggregation with a watermark for bounded out-of-order data.

Real CDC streams are not perfectly ordered: Debezium snapshots interleave
with streaming events, and Kafka only orders within a partition. This
aggregator buffers rows into fixed-size, non-overlapping ("tumbling")
time windows keyed by an arbitrary grouping key (e.g. `store_id`), and
only *emits* (finalizes) a window once a watermark -- the max event
timestamp seen so far, minus an allowed lateness grace period -- has
advanced past the window's end. Rows arriving after their window has
already been emitted are counted as late-and-dropped rather than
silently corrupting an already-emitted aggregate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class WindowResult:
    """A finalized aggregation for one (key, window) pair."""

    key: Any
    window_start_ms: int
    window_end_ms: int
    count: int
    sum: float
    min: float
    max: float

    @property
    def avg(self) -> float:
        return self.sum / self.count if self.count else 0.0


@dataclass
class _WindowAccumulator:
    count: int = 0
    sum: float = 0.0
    min: float = field(default=float("inf"))
    max: float = field(default=float("-inf"))

    def add(self, value: float) -> None:
        self.count += 1
        self.sum += value
        self.min = min(self.min, value)
        self.max = max(self.max, value)


class TumblingWindowAggregator:
    """Groups numeric values into fixed-size time windows per key.

    Parameters
    ----------
    window_size_ms:
        Width of each tumbling window in milliseconds (e.g. 60_000 for
        1-minute revenue-per-store windows).
    allowed_lateness_ms:
        How far behind the watermark an event's window may still be and
        still be accepted (grace period for out-of-order delivery).
    key_fn:
        Extracts the grouping key from a row (e.g. `row["store_id"]`).
    value_fn:
        Extracts the numeric metric to aggregate (e.g. `row["amount"]`).
    """

    def __init__(
        self,
        window_size_ms: int,
        allowed_lateness_ms: int,
        key_fn: Callable[[dict], Any],
        value_fn: Callable[[dict], float],
    ):
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be positive")
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be >= 0")
        self.window_size_ms = window_size_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self._key_fn = key_fn
        self._value_fn = value_fn

        self._windows: dict[tuple[Any, int], _WindowAccumulator] = {}
        self._watermark_ms: int = 0
        self._emitted: set[tuple[Any, int]] = set()
        self.late_dropped = 0

    def _window_start(self, ts_ms: int) -> int:
        return (ts_ms // self.window_size_ms) * self.window_size_ms

    def add(self, row: dict, ts_ms: int) -> None:
        """Ingest one row's metric value at the given event timestamp."""
        self._watermark_ms = max(self._watermark_ms, ts_ms)
        window_start = self._window_start(ts_ms)
        key = self._key_fn(row)
        window_key = (key, window_start)

        if window_key in self._emitted:
            self.late_dropped += 1
            return

        acc = self._windows.setdefault(window_key, _WindowAccumulator())
        acc.add(self._value_fn(row))

    def poll_completed_windows(self) -> list[WindowResult]:
        """Return (and finalize) every window whose end has fallen behind
        the watermark by more than the allowed lateness."""
        results: list[WindowResult] = []
        cutoff = self._watermark_ms - self.allowed_lateness_ms

        for window_key in list(self._windows.keys()):
            key, window_start = window_key
            window_end = window_start + self.window_size_ms
            if window_end <= cutoff:
                acc = self._windows.pop(window_key)
                self._emitted.add(window_key)
                results.append(
                    WindowResult(
                        key=key,
                        window_start_ms=window_start,
                        window_end_ms=window_end,
                        count=acc.count,
                        sum=acc.sum,
                        min=acc.min,
                        max=acc.max,
                    )
                )

        results.sort(key=lambda r: (r.window_start_ms, str(r.key)))
        return results

    def flush(self) -> list[WindowResult]:
        """Force-emit every open window regardless of watermark (used at
        shutdown so no buffered data is lost)."""
        self._watermark_ms += self.window_size_ms + self.allowed_lateness_ms
        return self.poll_completed_windows()

    @property
    def open_window_count(self) -> int:
        return len(self._windows)
