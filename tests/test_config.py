import os

from streamguard.config import PipelineConfig


class TestPipelineConfig:
    def test_defaults_are_sensible(self):
        config = PipelineConfig()
        assert config.window_size_ms == 60_000
        assert config.dedup_capacity > 0
        assert "streamguard.public.orders" in config.source_topics

    def test_from_env_overrides_defaults(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
        monkeypatch.setenv("WINDOW_SIZE_MS", "30000")
        monkeypatch.setenv("CLICKHOUSE_URL", "http://localhost:8124")

        config = PipelineConfig.from_env()

        assert config.kafka_bootstrap_servers == "localhost:19092"
        assert config.window_size_ms == 30_000
        assert config.clickhouse_url == "http://localhost:8124"

    def test_from_env_falls_back_when_unset(self, monkeypatch):
        for var in ["KAFKA_BOOTSTRAP_SERVERS", "WINDOW_SIZE_MS", "CLICKHOUSE_URL"]:
            monkeypatch.delenv(var, raising=False)
        config = PipelineConfig.from_env()
        assert config.kafka_bootstrap_servers == PipelineConfig.kafka_bootstrap_servers
        assert config.window_size_ms == PipelineConfig.window_size_ms
