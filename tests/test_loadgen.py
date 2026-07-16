from streamguard.loadgen import LoadGenConfig, synthetic_debezium_events


class TestSyntheticDebeziumEvents:
    def test_produces_at_least_requested_number_of_creates(self):
        config = LoadGenConfig(num_orders=100, malformed_rate=0.0, duplicate_rate=0.0, seed=1)
        events = list(synthetic_debezium_events(config))
        creates = [e for e in events if isinstance(e, dict) and e["payload"]["op"] == "c"]
        assert len(creates) == 100

    def test_zero_malformed_rate_produces_no_non_dict_events(self):
        config = LoadGenConfig(num_orders=200, malformed_rate=0.0, duplicate_rate=0.0, seed=2)
        events = list(synthetic_debezium_events(config))
        assert all(isinstance(e, dict) for e in events)

    def test_nonzero_malformed_rate_injects_bad_records(self):
        config = LoadGenConfig(num_orders=500, malformed_rate=0.1, duplicate_rate=0.0, seed=3)
        events = list(synthetic_debezium_events(config))
        bad = [e for e in events if not isinstance(e, dict) or "payload" not in e or
               e["payload"].get("op") not in ("c", "u", "d", "r")]
        assert len(bad) > 0

    def test_schema_change_introduces_new_column_after_threshold(self):
        config = LoadGenConfig(num_orders=300, schema_change_after=100, malformed_rate=0.0,
                                duplicate_rate=0.0, seed=4)
        events = list(synthetic_debezium_events(config))
        creates = [e for e in events if isinstance(e, dict) and e["payload"]["op"] == "c"]
        with_discount = [e for e in creates if "discount_code" in e["payload"]["after"]]
        without_discount = [e for e in creates if "discount_code" not in e["payload"]["after"]]
        assert len(with_discount) > 0
        assert len(without_discount) > 0

    def test_includes_a_terminal_update_marking_the_order_refunded(self):
        config = LoadGenConfig(num_orders=20, malformed_rate=0.0, duplicate_rate=0.0, seed=5)
        events = list(synthetic_debezium_events(config))
        updates = [e for e in events if isinstance(e, dict) and e["payload"]["op"] == "u"]
        assert len(updates) >= 1
        assert updates[-1]["payload"]["after"]["status"] == "refunded"

    def test_deterministic_given_seed(self):
        events_1 = list(synthetic_debezium_events(LoadGenConfig(seed=99, num_orders=40)))
        events_2 = list(synthetic_debezium_events(LoadGenConfig(seed=99, num_orders=40)))
        assert events_1 == events_2

    def test_different_seeds_produce_different_streams(self):
        events_1 = list(synthetic_debezium_events(LoadGenConfig(seed=1, num_orders=40, malformed_rate=0.0)))
        events_2 = list(synthetic_debezium_events(LoadGenConfig(seed=2, num_orders=40, malformed_rate=0.0)))
        assert events_1 != events_2

    def test_lsn_is_monotonically_non_decreasing_for_well_formed_events(self):
        config = LoadGenConfig(num_orders=100, malformed_rate=0.0, duplicate_rate=0.0, seed=6)
        events = list(synthetic_debezium_events(config))
        lsns = [e["payload"]["source"]["lsn"] for e in events]
        assert lsns == sorted(lsns)
