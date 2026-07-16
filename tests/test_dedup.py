import pytest

from streamguard.dedup import Deduplicator
from streamguard.events import parse_debezium_envelope


def make_event(id_=1, lsn=1, op="c"):
    raw = {
        "payload": {
            "before": {"id": id_} if op == "d" else None,
            "after": None if op == "d" else {"id": id_, "amount": 1.0},
            "source": {"table": "orders", "lsn": lsn},
            "op": op,
            "ts_ms": 1000,
        }
    }
    return parse_debezium_envelope(raw)


class FakeClock:
    def __init__(self, start=0.0):
        self.t = start

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


class TestDeduplicator:
    def test_first_sighting_is_not_duplicate(self):
        dedup = Deduplicator()
        assert dedup.is_duplicate(make_event()) is False

    def test_second_sighting_of_same_event_is_duplicate(self):
        dedup = Deduplicator()
        event = make_event()
        assert dedup.is_duplicate(event) is False
        assert dedup.is_duplicate(event) is True
        assert dedup.stats.duplicates == 1

    def test_different_lsn_is_not_duplicate(self):
        dedup = Deduplicator()
        assert dedup.is_duplicate(make_event(lsn=1)) is False
        assert dedup.is_duplicate(make_event(lsn=2)) is False

    def test_capacity_eviction_bounds_memory(self):
        dedup = Deduplicator(capacity=10)
        for i in range(50):
            dedup.is_duplicate(make_event(id_=i, lsn=i))
        assert len(dedup) <= 10
        assert dedup.stats.evicted > 0

    def test_ttl_expiry_allows_redelivery_after_window(self):
        clock = FakeClock()
        dedup = Deduplicator(ttl_seconds=10.0, clock=clock)
        event = make_event()
        assert dedup.is_duplicate(event) is False
        clock.advance(11.0)
        # expired -- treated as new rather than duplicate
        assert dedup.is_duplicate(event) is False
        assert dedup.stats.evicted >= 1

    def test_within_ttl_still_flagged_duplicate(self):
        clock = FakeClock()
        dedup = Deduplicator(ttl_seconds=10.0, clock=clock)
        event = make_event()
        dedup.is_duplicate(event)
        clock.advance(5.0)
        assert dedup.is_duplicate(event) is True

    def test_invalid_capacity_raises(self):
        with pytest.raises(ValueError):
            Deduplicator(capacity=0)

    def test_invalid_ttl_raises(self):
        with pytest.raises(ValueError):
            Deduplicator(ttl_seconds=0)

    def test_delete_and_create_with_same_key_are_distinct(self):
        dedup = Deduplicator()
        create_event = make_event(id_=1, lsn=1, op="c")
        delete_event = make_event(id_=1, lsn=1, op="d")
        assert dedup.is_duplicate(create_event) is False
        assert dedup.is_duplicate(delete_event) is False
