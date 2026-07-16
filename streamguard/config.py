"""Pipeline configuration, loaded from environment variables with sane
defaults so `docker-compose.yml` can wire everything with plain env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


@dataclass(frozen=True)
class PipelineConfig:
    kafka_bootstrap_servers: str = "redpanda:9092"
    source_topics: tuple[str, ...] = (
        "streamguard.public.orders",
        "streamguard.public.customers",
    )
    dlq_topic: str = "streamguard.dlq"
    consumer_group_id: str = "streamguard-processor"

    clickhouse_url: str = "http://clickhouse:8123"
    clickhouse_database: str = "streamguard"

    dedup_capacity: int = 100_000
    dedup_ttl_seconds: float = 3600.0

    window_size_ms: int = 60_000  # 1-minute revenue windows
    allowed_lateness_ms: int = 10_000  # 10s grace period for out-of-order events

    metrics_port: int = 9464

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            dlq_topic=os.environ.get("DLQ_TOPIC", cls.dlq_topic),
            consumer_group_id=os.environ.get("CONSUMER_GROUP_ID", cls.consumer_group_id),
            clickhouse_url=os.environ.get("CLICKHOUSE_URL", cls.clickhouse_url),
            clickhouse_database=os.environ.get("CLICKHOUSE_DATABASE", cls.clickhouse_database),
            dedup_capacity=_env_int("DEDUP_CAPACITY", cls.dedup_capacity),
            dedup_ttl_seconds=float(_env_int("DEDUP_TTL_SECONDS", int(cls.dedup_ttl_seconds))),
            window_size_ms=_env_int("WINDOW_SIZE_MS", cls.window_size_ms),
            allowed_lateness_ms=_env_int("ALLOWED_LATENESS_MS", cls.allowed_lateness_ms),
            metrics_port=_env_int("METRICS_PORT", cls.metrics_port),
        )
