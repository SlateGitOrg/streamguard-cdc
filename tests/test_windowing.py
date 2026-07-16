import pytest

from streamguard.windowing import TumblingWindowAggregator


def make_aggregator(window_size_ms=60_000, allowed_lateness_ms=10_000):
    return TumblingWindowAggregator(
        window_size_ms=window_size_ms,
        allowed_lateness_ms=allowed_lateness_ms,
        key_fn=lambda row: row["store_id"],
        value_fn=lambda row: row["amount"],
    )


class TestTumblingWindowAggregator:
    def test_single_window_aggregates_correctly(self):
        agg = make_aggregator()
        agg.add({"store_id": 1, "amount": 10.0}, ts_ms=1_000)
        agg.add({"store_id": 1, "amount": 20.0}, ts_ms=2_000)
        agg.add({"store_id": 1, "amount": 30.0}, ts_ms=3_000)

        # Advance watermark well past window end + lateness to force emission.
        agg.add({"store_id": 1, "amount": 0.0}, ts_ms=100_000)
        results = agg.poll_completed_windows()

        assert len(results) == 1
        window = results[0]
        assert window.key == 1
        assert window.count == 3
        assert window.sum == 60.0
        assert window.avg == 20.0
        assert window.min == 10.0
        assert window.max == 30.0

    def test_separate_keys_produce_separate_windows(self):
        agg = make_aggregator()
        agg.add({"store_id": 1, "amount": 10.0}, ts_ms=1_000)
        agg.add({"store_id": 2, "amount": 100.0}, ts_ms=1_000)
        agg.add({"store_id": 1, "amount": 0.0}, ts_ms=100_000)

        results = agg.poll_completed_windows()
        by_key = {r.key: r for r in results}
        assert by_key[1].sum == 10.0
        assert by_key[2].sum == 100.0

    def test_windows_not_emitted_before_watermark_passes_lateness(self):
        agg = make_aggregator(window_size_ms=60_000, allowed_lateness_ms=10_000)
        agg.add({"store_id": 1, "amount": 10.0}, ts_ms=1_000)
        # watermark = 1000, window [0, 60000) ends far in the future -- nothing to emit yet
        assert agg.poll_completed_windows() == []
        assert agg.open_window_count == 1

    def test_out_of_order_event_within_lateness_still_counted(self):
        agg = make_aggregator(window_size_ms=60_000, allowed_lateness_ms=10_000)
        agg.add({"store_id": 1, "amount": 10.0}, ts_ms=55_000)  # window [0, 60000)
        agg.add({"store_id": 1, "amount": 5.0}, ts_ms=58_000)  # same window, arrives "late" but in order here
        # watermark is 58000; window end 60000 - lateness 10000 = 50000 cutoff -- not yet emitted
        assert agg.poll_completed_windows() == []
        # now watermark passes 70000, window should emit with both events counted
        agg.add({"store_id": 1, "amount": 0.0}, ts_ms=200_000)
        results = agg.poll_completed_windows()
        assert results[0].count == 2
        assert results[0].sum == 15.0

    def test_late_event_after_window_emitted_is_dropped_not_double_counted(self):
        agg = make_aggregator(window_size_ms=60_000, allowed_lateness_ms=1_000)
        agg.add({"store_id": 1, "amount": 10.0}, ts_ms=1_000)
        agg.add({"store_id": 1, "amount": 0.0}, ts_ms=100_000)
        first = agg.poll_completed_windows()
        assert len(first) == 1

        # A very late arrival for the already-emitted window [0, 60000)
        agg.add({"store_id": 1, "amount": 999.0}, ts_ms=5_000)
        assert agg.late_dropped == 1
        # Nothing new should be waiting to emit for that window
        second = agg.poll_completed_windows()
        assert all(r.window_start_ms != 0 for r in second)

    def test_flush_force_emits_open_windows(self):
        agg = make_aggregator()
        agg.add({"store_id": 1, "amount": 42.0}, ts_ms=1_000)
        assert agg.poll_completed_windows() == []
        results = agg.flush()
        assert len(results) == 1
        assert results[0].sum == 42.0
        assert agg.open_window_count == 0

    def test_multiple_consecutive_windows_emit_in_order(self):
        agg = make_aggregator(window_size_ms=1_000, allowed_lateness_ms=0)
        agg.add({"store_id": 1, "amount": 1.0}, ts_ms=500)   # window [0, 1000)
        agg.add({"store_id": 1, "amount": 2.0}, ts_ms=1_500)  # window [1000, 2000)
        agg.add({"store_id": 1, "amount": 3.0}, ts_ms=2_500)  # window [2000, 3000), advances watermark

        results = agg.poll_completed_windows()
        starts = [r.window_start_ms for r in results]
        assert starts == sorted(starts)
        assert len(results) == 2  # windows [0,1000) and [1000,2000) both closed; [2000,3000) still open

    def test_invalid_window_size_raises(self):
        with pytest.raises(ValueError):
            TumblingWindowAggregator(0, 100, lambda r: 1, lambda r: 1.0)

    def test_invalid_lateness_raises(self):
        with pytest.raises(ValueError):
            TumblingWindowAggregator(1000, -1, lambda r: 1, lambda r: 1.0)

    def test_avg_of_empty_window_is_zero(self):
        from streamguard.windowing import WindowResult

        result = WindowResult(key=1, window_start_ms=0, window_end_ms=1000, count=0, sum=0.0, min=0.0, max=0.0)
        assert result.avg == 0.0
