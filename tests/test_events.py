import pytest

from streamguard.events import (
    ChangeEvent,
    MalformedEventError,
    Op,
    SchemaRegistry,
    SchemaValidationError,
    parse_debezium_envelope,
)


def envelope(op="c", before=None, after=None, table="orders", lsn=42, ts_ms=1000):
    return {
        "schema": {"type": "struct"},
        "payload": {
            "before": before,
            "after": after,
            "source": {"table": table, "lsn": lsn, "ts_ms": ts_ms},
            "op": op,
            "ts_ms": ts_ms,
        },
    }


class TestParseDebeziumEnvelope:
    def test_parses_create_event(self):
        raw = envelope(op="c", after={"id": 1, "amount": 9.99})
        event = parse_debezium_envelope(raw)
        assert event.table == "orders"
        assert event.op == Op.CREATE
        assert event.after == {"id": 1, "amount": 9.99}
        assert event.source_lsn == 42
        assert event.key == {"id": 1}

    def test_parses_delete_event_using_before_image(self):
        raw = envelope(op="d", before={"id": 5, "amount": 1.0}, after=None)
        event = parse_debezium_envelope(raw)
        assert event.op == Op.DELETE
        assert event.row == {"id": 5, "amount": 1.0}

    def test_kafka_key_overrides_derived_key(self):
        raw = envelope(op="c", after={"id": 1})
        event = parse_debezium_envelope(raw, kafka_key={"id": 999})
        assert event.key == {"id": 999}

    def test_non_dict_raises_malformed(self):
        with pytest.raises(MalformedEventError):
            parse_debezium_envelope("not-a-dict")

    def test_missing_payload_raises_malformed(self):
        with pytest.raises(MalformedEventError):
            parse_debezium_envelope({"schema": {}})

    def test_payload_not_dict_raises_malformed(self):
        with pytest.raises(MalformedEventError):
            parse_debezium_envelope({"payload": "oops"})

    def test_missing_op_raises_schema_validation(self):
        raw = {"payload": {"source": {"table": "orders"}}}
        with pytest.raises(SchemaValidationError):
            parse_debezium_envelope(raw)

    def test_unknown_op_raises_schema_validation(self):
        raw = envelope(op="x", after={"id": 1})
        with pytest.raises(SchemaValidationError):
            parse_debezium_envelope(raw)

    def test_missing_source_table_raises_schema_validation(self):
        raw = {"payload": {"op": "c", "source": {}, "after": {"id": 1}}}
        with pytest.raises(SchemaValidationError):
            parse_debezium_envelope(raw)

    def test_create_without_after_raises_schema_validation(self):
        raw = envelope(op="c", after=None)
        with pytest.raises(SchemaValidationError):
            parse_debezium_envelope(raw)

    def test_delete_without_before_raises_schema_validation(self):
        raw = envelope(op="d", before=None, after=None)
        with pytest.raises(SchemaValidationError):
            parse_debezium_envelope(raw)

    def test_carries_kafka_offset_and_partition(self):
        raw = envelope(op="c", after={"id": 1})
        event = parse_debezium_envelope(raw, kafka_offset=7, kafka_partition=2)
        assert event.kafka_offset == 7
        assert event.kafka_partition == 2


class TestDedupKey:
    def test_same_change_produces_same_dedup_key(self):
        raw = envelope(op="c", after={"id": 1, "amount": 5}, lsn=42)
        e1 = parse_debezium_envelope(raw, kafka_offset=10)
        e2 = parse_debezium_envelope(raw, kafka_offset=99)  # redelivered at a different offset
        assert e1.dedup_key == e2.dedup_key

    def test_different_lsn_produces_different_dedup_key(self):
        raw1 = envelope(op="c", after={"id": 1}, lsn=42)
        raw2 = envelope(op="c", after={"id": 1}, lsn=43)
        e1 = parse_debezium_envelope(raw1)
        e2 = parse_debezium_envelope(raw2)
        assert e1.dedup_key != e2.dedup_key


class TestSchemaRegistry:
    def test_first_event_establishes_baseline_with_no_change(self):
        registry = SchemaRegistry()
        raw = envelope(op="c", after={"id": 1, "amount": 5.0})
        event = parse_debezium_envelope(raw)
        normalized, change = registry.reconcile(event)
        assert normalized == {"id": 1, "amount": 5.0}
        assert change is None

    def test_added_column_detected_and_backfilled_for_old_rows(self):
        registry = SchemaRegistry()
        e1 = parse_debezium_envelope(envelope(op="c", after={"id": 1, "amount": 5.0}))
        registry.reconcile(e1)

        e2 = parse_debezium_envelope(
            envelope(op="c", after={"id": 2, "amount": 6.0, "discount_code": "SAVE10"})
        )
        normalized, change = registry.reconcile(e2)

        assert normalized == {"id": 2, "amount": 6.0, "discount_code": "SAVE10"}
        assert change is not None
        assert change.added_columns == frozenset({"discount_code"})
        assert change.removed_columns == frozenset()
        assert registry.known_columns("orders") == frozenset({"id", "amount", "discount_code"})

    def test_removed_column_detected_and_normalized_row_fills_none(self):
        registry = SchemaRegistry()
        e1 = parse_debezium_envelope(
            envelope(op="c", after={"id": 1, "amount": 5.0, "legacy_field": "x"})
        )
        registry.reconcile(e1)

        e2 = parse_debezium_envelope(envelope(op="c", after={"id": 2, "amount": 6.0}))
        normalized, change = registry.reconcile(e2)

        assert normalized == {"id": 2, "amount": 6.0, "legacy_field": None}
        assert change.removed_columns == frozenset({"legacy_field"})

    def test_identical_shape_produces_no_schema_change(self):
        registry = SchemaRegistry()
        registry.reconcile(parse_debezium_envelope(envelope(op="c", after={"id": 1, "amount": 5.0})))
        _, change = registry.reconcile(
            parse_debezium_envelope(envelope(op="c", after={"id": 2, "amount": 6.0}))
        )
        assert change is None

    def test_delete_event_reconciles_using_before_image(self):
        registry = SchemaRegistry()
        registry.reconcile(parse_debezium_envelope(envelope(op="c", after={"id": 1, "amount": 5.0})))
        e = parse_debezium_envelope(envelope(op="d", before={"id": 1, "amount": 5.0}, after=None))
        normalized, change = registry.reconcile(e)
        assert normalized == {"id": 1, "amount": 5.0}
        assert change is None
