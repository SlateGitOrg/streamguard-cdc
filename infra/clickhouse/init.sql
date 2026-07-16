-- ClickHouse sink schema. Mounted into the ClickHouse container's
-- /docker-entrypoint-initdb.d/ so it runs automatically on first boot.

CREATE DATABASE IF NOT EXISTS streamguard;

-- Raw mirror tables: one row per source-table primary key. ReplacingMergeTree
-- keeps the highest `_version` (Postgres WAL LSN) row per key, which is how
-- out-of-order/duplicate CDC deliveries converge to the correct final state.
-- `_deleted = 1` marks a tombstone (the source row was deleted) instead of a
-- physical DELETE, since ClickHouse deletes are an expensive, async mutation.

CREATE TABLE IF NOT EXISTS streamguard.orders
(
    id          Int64,
    store_id    Int64,
    amount      Float64,
    status      String,
    discount_code Nullable(String),
    _version    Int64,
    _deleted    UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS streamguard.customers
(
    id      Int64,
    name    String,
    email   String,
    _version Int64,
    _deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

-- Windowed aggregation output: one row per (store, minute) once the
-- stream processor's watermark closes that window.
CREATE TABLE IF NOT EXISTS streamguard.orders_revenue_by_minute
(
    group_key       Int64,
    window_start_ms Int64,
    window_end_ms   Int64,
    count           UInt64,
    sum             Float64,
    avg             Float64,
    min             Float64,
    max             Float64,
    inserted_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (group_key, window_start_ms);

-- Convenience view for the Grafana "live orders" panel: only non-deleted
-- rows, latest version per id (FINAL forces the replace-merge at query time).
CREATE VIEW IF NOT EXISTS streamguard.orders_live AS
SELECT * FROM streamguard.orders FINAL WHERE _deleted = 0;
