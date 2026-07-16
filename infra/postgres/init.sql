-- Bootstraps the OLTP schema Debezium captures via logical replication.
-- postgres.conf (mounted alongside this file) already sets:
--   wal_level = logical
--   max_replication_slots = 4
--   max_wal_senders = 4

CREATE TABLE IF NOT EXISTS customers (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    store_id    INTEGER NOT NULL,
    customer_id BIGINT REFERENCES customers(id),
    amount      NUMERIC(10, 2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'created',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL is required so Debezium's `before` image on
-- UPDATE/DELETE includes every column, not just the primary key -- this
-- is what lets streamguard's dedupe/schema-evolution logic see full
-- before/after row images.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Debezium's Postgres connector consumes changes from this publication.
CREATE PUBLICATION streamguard_publication FOR TABLE customers, orders;

-- A few seed rows so the pipeline has something to mirror even before
-- the load generator starts.
INSERT INTO customers (name, email) VALUES
    ('Ada Lovelace', 'ada@example.com'),
    ('Alan Turing', 'alan@example.com')
ON CONFLICT DO NOTHING;
