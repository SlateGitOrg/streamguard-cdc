"""OLTP load generator.

Two jobs live here:

1. ``PostgresLoadGenerator`` -- the *real* generator used against the
   Docker-composed Postgres in `docker-compose.yml`. It issues plain
   INSERT/UPDATE/DELETE statements against the `orders`/`customers`
   tables; Debezium's logical-replication slot picks up the resulting
   WAL records and publishes them to Kafka. ``psycopg2`` is imported
   lazily, so this class can be imported (just not instantiated) with
   no Postgres driver installed.

2. ``synthetic_debezium_events`` -- a pure-Python generator that
   produces the exact same *shape* of Debezium envelope Postgres+Debezium
   would emit, without any database or network. This is what the unit
   tests, the offline demo (`scripts/run_offline_demo.py`), and the
   failure-injection test drive the pipeline with, and it deliberately
   injects the messy realities a real CDC stream has: redelivered
   duplicates, a schema change partway through, and a handful of
   malformed records that must be dead-lettered.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Iterator, Optional


@dataclass
class LoadGenConfig:
    num_orders: int = 500
    num_stores: int = 5
    duplicate_rate: float = 0.05  # fraction of events redelivered once
    malformed_rate: float = 0.02  # fraction of events corrupted
    schema_change_after: int = 200  # order index after which `discount_code` appears
    seed: int = 1337
    window_size_ms: int = 60_000
    start_ts_ms: int = 1_700_000_000_000


def _make_envelope(op: str, table: str, before: Optional[dict], after: Optional[dict], lsn: int, ts_ms: int) -> dict:
    return {
        "schema": {"type": "struct", "name": f"streamguard.public.{table}.Envelope"},
        "payload": {
            "before": before,
            "after": after,
            "source": {"table": table, "lsn": lsn, "ts_ms": ts_ms, "db": "streamguard"},
            "op": op,
            "ts_ms": ts_ms,
        },
    }


def synthetic_debezium_events(config: LoadGenConfig = LoadGenConfig()) -> Iterator[dict[str, Any]]:
    """Yield raw (still-JSON-shaped) Debezium envelopes for a synthetic
    order stream, in the same order Kafka would deliver them.

    Deterministic given ``config.seed``, which makes it usable both as
    a load generator for local demos and as a fixture generator for
    deterministic tests.
    """
    rng = random.Random(config.seed)
    lsn = 1_000_000

    for i in range(1, config.num_orders + 1):
        lsn += rng.randint(1, 3)
        ts_ms = config.start_ts_ms + i * rng.randint(200, 2000)
        store_id = rng.randint(1, config.num_stores)
        amount = round(rng.uniform(5.0, 500.0), 2)

        after = {
            "id": i,
            "store_id": store_id,
            "amount": amount,
            "status": "created",
        }
        if i > config.schema_change_after:
            # Simulates `ALTER TABLE orders ADD COLUMN discount_code text;`
            after["discount_code"] = rng.choice([None, "SAVE10", "SAVE20", None, None])

        envelope = _make_envelope("c", "orders", None, after, lsn, ts_ms)

        if rng.random() < config.malformed_rate:
            # A handful of structurally-broken records to exercise the DLQ path.
            broken_kind = rng.choice(["not_a_dict", "missing_payload", "bad_op"])
            if broken_kind == "not_a_dict":
                yield "not-json-object"  # will fail isinstance(dict) check
            elif broken_kind == "missing_payload":
                yield {"schema": envelope["schema"]}
            else:
                bad = dict(envelope)
                bad["payload"] = {**envelope["payload"], "op": "x"}
                yield bad
            continue

        yield envelope

        if rng.random() < config.duplicate_rate:
            # Simulate a redelivery (consumer rebalance, producer retry).
            yield envelope

    # A late update + a delete near the end, to exercise the update/delete
    # paths and the ReplacingMergeTree version semantics in the sink.
    last_id = config.num_orders
    update_ts = config.start_ts_ms + (config.num_orders + 1) * 1000
    yield _make_envelope(
        "u",
        "orders",
        {"id": last_id, "store_id": 1, "amount": 10.0, "status": "created"},
        {"id": last_id, "store_id": 1, "amount": 10.0, "status": "refunded"},
        lsn + 10,
        update_ts,
    )


class PostgresLoadGenerator:
    """Generates real INSERT/UPDATE/DELETE traffic against the
    Docker-composed Postgres so Debezium has WAL activity to capture."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    def run(self, config: LoadGenConfig = LoadGenConfig()) -> int:
        import psycopg2  # lazy import -- optional dependency, real deployment only

        rng = random.Random(config.seed)
        written = 0
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                for i in range(1, config.num_orders + 1):
                    store_id = rng.randint(1, config.num_stores)
                    amount = round(rng.uniform(5.0, 500.0), 2)
                    cur.execute(
                        "INSERT INTO orders (id, store_id, amount, status) VALUES (%s, %s, %s, 'created') "
                        "ON CONFLICT (id) DO NOTHING",
                        (i, store_id, amount),
                    )
                    written += cur.rowcount
            conn.commit()
        return written
