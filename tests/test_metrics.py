from streamguard.metrics import PipelineMetrics


class TestPipelineMetrics:
    def test_counters_start_at_zero(self):
        m = PipelineMetrics()
        assert m.consumed_total == 0
        assert m.duplicates_total == 0
        assert m.dlq_total == 0
        assert m.lag == 0

    def test_record_methods_increment_counters(self):
        m = PipelineMetrics()
        m.record_consumed()
        m.record_consumed()
        m.record_duplicate()
        m.record_dlq()
        m.record_schema_change()
        m.record_window_emitted(3)
        m.record_sink_write(2)
        m.set_lag(42)

        assert m.consumed_total == 2
        assert m.duplicates_total == 1
        assert m.dlq_total == 1
        assert m.schema_changes_total == 1
        assert m.windows_emitted_total == 3
        assert m.sink_writes_total == 2
        assert m.lag == 42

    def test_prometheus_text_contains_all_metrics(self):
        m = PipelineMetrics()
        m.record_consumed()
        m.set_lag(7)
        text = m.to_prometheus_text()
        assert "streamguard_consumed_total 1" in text
        assert "streamguard_consumer_lag 7" in text
        assert "# TYPE streamguard_consumed_total counter" in text
        assert "# TYPE streamguard_consumer_lag gauge" in text

    def test_throughput_is_non_negative(self):
        m = PipelineMetrics()
        m.record_consumed()
        assert m.throughput_per_sec >= 0.0
