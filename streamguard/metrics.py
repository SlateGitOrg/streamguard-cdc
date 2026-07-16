"""Minimal, dependency-free Prometheus-text-format metrics.

A real deployment could swap this for ``prometheus_client``, but that
package is not installed in the offline test environment, and pulling in
a metrics client is overkill for what is fundamentally four counters and
a gauge. ``PipelineMetrics`` exposes them as plain attributes (fast,
testable) and can render itself as Prometheus exposition text for the
`/metrics` endpoint Grafana/Prometheus scrape in the real stack.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class PipelineMetrics:
    consumed_total: int = 0
    duplicates_total: int = 0
    dlq_total: int = 0
    schema_changes_total: int = 0
    windows_emitted_total: int = 0
    sink_writes_total: int = 0
    _started_at: float = field(default_factory=time.monotonic)
    _last_lag: int = 0

    def record_consumed(self) -> None:
        self.consumed_total += 1

    def record_duplicate(self) -> None:
        self.duplicates_total += 1

    def record_dlq(self) -> None:
        self.dlq_total += 1

    def record_schema_change(self) -> None:
        self.schema_changes_total += 1

    def record_window_emitted(self, n: int = 1) -> None:
        self.windows_emitted_total += n

    def record_sink_write(self, n: int = 1) -> None:
        self.sink_writes_total += n

    def set_lag(self, lag: int) -> None:
        self._last_lag = lag

    @property
    def lag(self) -> int:
        return self._last_lag

    @property
    def throughput_per_sec(self) -> float:
        elapsed = time.monotonic() - self._started_at
        return self.consumed_total / elapsed if elapsed > 0 else 0.0

    def to_prometheus_text(self) -> str:
        lines = [
            "# HELP streamguard_consumed_total Total CDC events consumed from Kafka.",
            "# TYPE streamguard_consumed_total counter",
            f"streamguard_consumed_total {self.consumed_total}",
            "# HELP streamguard_duplicates_total Events dropped as redeliveries.",
            "# TYPE streamguard_duplicates_total counter",
            f"streamguard_duplicates_total {self.duplicates_total}",
            "# HELP streamguard_dlq_total Events routed to the dead-letter topic.",
            "# TYPE streamguard_dlq_total counter",
            f"streamguard_dlq_total {self.dlq_total}",
            "# HELP streamguard_schema_changes_total Detected schema-evolution events.",
            "# TYPE streamguard_schema_changes_total counter",
            f"streamguard_schema_changes_total {self.schema_changes_total}",
            "# HELP streamguard_windows_emitted_total Finalized aggregation windows written to ClickHouse.",
            "# TYPE streamguard_windows_emitted_total counter",
            f"streamguard_windows_emitted_total {self.windows_emitted_total}",
            "# HELP streamguard_sink_writes_total Total rows written to ClickHouse.",
            "# TYPE streamguard_sink_writes_total counter",
            f"streamguard_sink_writes_total {self.sink_writes_total}",
            "# HELP streamguard_consumer_lag Estimated consumer lag (high watermark - committed offset).",
            "# TYPE streamguard_consumer_lag gauge",
            f"streamguard_consumer_lag {self.lag}",
            "# HELP streamguard_throughput_per_sec Events consumed per second since process start.",
            "# TYPE streamguard_throughput_per_sec gauge",
            f"streamguard_throughput_per_sec {self.throughput_per_sec:.4f}",
        ]
        return "\n".join(lines) + "\n"
