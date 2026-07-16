from streamguard.clickhouse_sink import InMemorySink


class TestInMemorySink:
    def test_upsert_row_is_visible(self):
        sink = InMemorySink()
        sink.upsert_row("orders", {"id": 1, "amount": 5.0}, version=1)
        assert sink.live_rows("orders") == [{"id": 1, "amount": 5.0, "_version": 1, "_deleted": 0}]

    def test_higher_version_overwrites_lower(self):
        sink = InMemorySink()
        sink.upsert_row("orders", {"id": 1, "amount": 5.0}, version=1)
        sink.upsert_row("orders", {"id": 1, "amount": 6.0}, version=2)
        rows = sink.live_rows("orders")
        assert len(rows) == 1
        assert rows[0]["amount"] == 6.0
        assert rows[0]["_version"] == 2

    def test_stale_write_is_ignored_replacing_merge_tree_semantics(self):
        sink = InMemorySink()
        sink.upsert_row("orders", {"id": 1, "amount": 6.0}, version=5)
        sink.upsert_row("orders", {"id": 1, "amount": 999.0}, version=2)  # out-of-order, older version
        rows = sink.live_rows("orders")
        assert rows[0]["amount"] == 6.0  # newer version wins, stale write is a no-op

    def test_delete_row_tombstones_and_hides_from_live_rows(self):
        sink = InMemorySink()
        sink.upsert_row("orders", {"id": 1, "amount": 5.0}, version=1)
        sink.delete_row("orders", {"id": 1}, version=2)
        assert sink.live_rows("orders") == []
        assert sink.rows["orders"][("id", 1)]["_deleted"] == 1

    def test_delete_before_matching_upsert_version_is_ignored(self):
        sink = InMemorySink()
        sink.upsert_row("orders", {"id": 1, "amount": 5.0}, version=10)
        sink.delete_row("orders", {"id": 1}, version=3)  # stale delete, arrives out of order
        assert sink.live_rows("orders")[0]["amount"] == 5.0

    def test_write_aggregate_appends(self):
        sink = InMemorySink()
        sink.write_aggregate("orders_revenue_by_minute", {"group_key": 1, "sum": 10.0})
        sink.write_aggregate("orders_revenue_by_minute", {"group_key": 1, "sum": 20.0})
        assert len(sink.aggregates["orders_revenue_by_minute"]) == 2

    def test_write_count_tracks_all_writes(self):
        sink = InMemorySink()
        sink.upsert_row("orders", {"id": 1}, version=1)
        sink.delete_row("orders", {"id": 2}, version=1)
        sink.write_aggregate("agg", {"a": 1})
        assert sink.write_count == 3

    def test_multiple_tables_are_independent(self):
        sink = InMemorySink()
        sink.upsert_row("orders", {"id": 1}, version=1)
        sink.upsert_row("customers", {"id": 1, "name": "a"}, version=1)
        assert sink.live_rows("orders") != sink.live_rows("customers")
        assert len(sink.rows) == 2
