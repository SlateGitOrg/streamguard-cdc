# streamguard-cdc

**Change-data-capture pipeline: Postgres WAL → Debezium → Kafka-compatible broker → a Python stream processor (dedupe, schema evolution, windowed aggregation) → ClickHouse.**

[![CI](https://github.com/SlateGitOrg/streamguard-cdc/actions/workflows/ci.yml/badge.svg)](https://github.com/SlateGitOrg/streamguard-cdc/actions/workflows/ci.yml)

![streamguard-cdc](assets/hero.png)

## Why

Every mid-to-large SaaS eventually needs its search index, its cache, its
analytics warehouse, or a downstream microservice to reflect what just
changed in the OLTP database — and the two ways people reach for first
are both bad:

- **Polling** ("SELECT * WHERE updated_at > last_poll") wastes database
  connections, misses hard deletes unless you bolt on soft-delete flags
  everywhere, and either lags behind or hammers the primary.
- **Nightly batch ETL** is stale by definition — a support agent looking
  at "customer's current plan" in the analytics DB can be looking at
  yesterday's answer.

The standard fix is **Change Data Capture (CDC)**: read the database's
own write-ahead log, turn every committed row change into an event, and
stream those events to whoever needs them. That's what this repo builds,
end to end: a real Postgres logical-replication source, a real Debezium
connector, a real Kafka-compatible broker, a stream processor with the
three things that make CDC pipelines hard in practice (deduplication,
schema evolution, windowed aggregation), and a real analytical sink.

## Features

- Real logical-replication source: Postgres with `REPLICA IDENTITY FULL` and a publication, read by a Debezium connector.
- Debezium envelope parsing with dead-lettering of malformed or failing records (`streamguard.dlq`).
- Idempotent processing: bounded TTL seen-set keyed on `(table, primary key, WAL LSN)`.
- Schema evolution: new or missing columns are reconciled instead of crashing or dropping data.
- Tumbling-window aggregation (revenue per store per minute) with a watermark and a lateness grace period.
- ClickHouse sink with highest-LSN-wins upserts, tombstoned deletes and a separate aggregates table.
- Prometheus metrics and a provisioned Grafana lag/throughput dashboard.
- Everything unit-testable offline through in-memory Kafka and ClickHouse fakes.

## Architecture

```mermaid
flowchart LR
    subgraph OLTP["OLTP Layer"]
        PG[(Postgres<br/>orders, customers)]
        LG[Load Generator]
        LG -->|INSERT / UPDATE / DELETE| PG
    end

    PG -->|Logical replication<br/>WAL| DBZ[Debezium connector<br/>Kafka Connect]
    DBZ -->|CDC events| TOPICS[Kafka topics<br/>streamguard.public.orders<br/>streamguard.public.customers]

    TOPICS --> SP{Stream processor}
    SP -->|dedupe| DEDUPE[Dedup<br/>LSN + key seen-set]
    SP -->|schema evolution| SCHEMA[Schema registry<br/>add/drop column reconcile]
    SP -->|windowed aggregation| WINDOW[Tumbling window<br/>revenue per store per minute]

    DEDUPE --> CH
    SCHEMA --> CH[(ClickHouse<br/>raw mirror + aggregates)]
    WINDOW --> CH

    SP -.->|parse / sink failure| DLQ[[Dead-letter topic<br/>streamguard.dlq]]

    SP -->|/metrics| PROM[Prometheus]
    PROM --> GRAF[Grafana<br/>lag + throughput dashboard]

    style DLQ fill:#4a1a1a,stroke:#c0392b,color:#fff
    style GRAF fill:#1a3a4a,stroke:#2980b9,color:#fff
```

Postgres WAL → Debezium connector → Kafka topics → stream processor
(dedupe, schema evolution, windowed aggregation) → ClickHouse sink, with
a dead-letter topic branch and a Grafana lag/throughput monitor tap.

## Quick start

### Run the pipeline logic offline (no Docker, 2 seconds)

```bash
pip install -e ".[dev]"
pytest -v                         # 83 unit tests, all in-memory
python3 scripts/run_offline_demo.py   # end-to-end demo: 500 synthetic
                                       # orders -> dedupe/schema-evolution/
                                       # windowing -> in-memory ClickHouse
```

Requires Python 3.10+. The offline demo prints (trimmed):

```text
Generating 500 synthetic orders (deterministic, seed=2024) ...
Published 532 raw Kafka records. Processing ...

=== Pipeline results ===
   dead_lettered: 12
       duplicate: 31
       processed: 489
...
Live (non-deleted) rows in ClickHouse 'orders' mirror: 488
Aggregation windows emitted (incl. final flush): 73
Dead-lettered records on 'streamguard.dlq': 12
```

### Run the real stack

Requires Docker with Compose v2. The stack runs Postgres, Redpanda, Debezium Connect, ClickHouse, Prometheus and Grafana.

```bash
docker compose up -d
./scripts/register-connector.sh     # wire Debezium to Postgres + Kafka

# generate OLTP load so there's something to capture
docker compose exec streamguard-processor python -c "
from streamguard.loadgen import PostgresLoadGenerator, LoadGenConfig
PostgresLoadGenerator('postgresql://streamguard:streamguard@postgres:5432/streamguard').run(
    LoadGenConfig(num_orders=1000)
)"

open http://localhost:3000           # Grafana (admin/admin) -- lag & throughput dashboard
open http://localhost:8123/play      # ClickHouse SQL playground
```

## How it works

**1. Postgres → Debezium.** `infra/postgres/init.sql` creates the
`orders`/`customers` tables with `REPLICA IDENTITY FULL` and a logical
replication publication. Debezium's Postgres connector
(`infra/debezium/connector-config.json`) reads the WAL through that
publication and emits one JSON envelope per row change
(`{before, after, source: {lsn, table}, op}`) onto a Kafka topic per
table.

**2. Kafka → stream processor.** `streamguard/pipeline.py`'s
`StreamProcessor` polls each topic and, per record:

- **Parses** the Debezium envelope (`streamguard/events.py`); anything
  structurally broken or missing required fields is dead-lettered, not
  fatal.
- **Deduplicates** (`streamguard/dedup.py`) using a bounded, TTL-based
  seen-set keyed on `(table, primary key, WAL LSN)` — the stable identity
  of one committed change — so a Kafka rebalance or Debezium restart
  replaying old offsets doesn't double-count anything.
- **Reconciles schema** (`SchemaRegistry` in `streamguard/events.py`):
  tracks the observed column set per table and, when a row shows up with
  a new or missing column (an `ALTER TABLE` rolling out), widens the
  known set and backfills missing columns with `None` instead of
  crashing or silently dropping data.
- **Feeds the windowed aggregator** (`streamguard/windowing.py`): a
  tumbling-window accumulator with a watermark, so a 1-minute
  "revenue per store" aggregate only closes and gets written once the
  pipeline is confident no more (reasonably) late events are coming for
  that window — and out-of-order events within the lateness grace period
  still land in the right window.
- **Writes to ClickHouse** (`streamguard/clickhouse_sink.py`): row
  upserts use `ReplacingMergeTree`-style version semantics (highest WAL
  LSN wins), deletes are tombstoned rather than physically removed, and
  finalized aggregation windows are appended to a separate table.
- **Anything that throws along the way** — a malformed event, a schema
  validation failure, a sink write error — is routed to the
  `streamguard.dlq` dead-letter topic (`streamguard/dlq.py`) with the
  original payload, error type/message, and source offset, and the
  consumer keeps moving instead of stalling the whole pipeline.

**3. Observability.** `streamguard/metrics.py` tracks consumed/dedup/DLQ/
schema-change/window/sink-write counters and consumer lag, exposed as
Prometheus text on `/metrics`; `infra/grafana/` provisions a dashboard
(`infra/grafana/provisioning/dashboards/streamguard-lag-throughput.json`)
showing lag, throughput, and DLQ/duplicate/schema-change counts alongside
a live ClickHouse revenue-per-store panel.

**4. Load generation.** `streamguard/loadgen.py` has two faces: a real
`PostgresLoadGenerator` that issues INSERT/UPDATE/DELETE against the
Docker-composed Postgres (giving Debezium real WAL traffic), and a pure
Python `synthetic_debezium_events()` generator that deterministically
reproduces the same messy realities — redelivered duplicates, malformed
records, a schema change partway through the stream, a late update and
delete — without any external service, which is what the entire unit
test suite and the offline demo run against.

## Design choices that make this testable without Docker

Every piece of I/O the pipeline needs — a Kafka consumer/producer, a
ClickHouse sink — is defined as a small `Protocol` in
`streamguard/kafka_io.py` / `streamguard/clickhouse_sink.py`. The real
adapters (`RedpandaConsumer`, `RedpandaProducer`, `RealClickHouseSink`)
lazily import their client libraries (`confluent_kafka`, `requests`)
only when actually instantiated, so importing — and unit-testing —
`streamguard` never requires them installed. The in-memory
implementations (`InMemoryBroker`, `InMemoryConsumer`, `InMemoryProducer`,
`InMemorySink`) are faithful enough models of real broker/ClickHouse
semantics (offsets, high watermarks, `ReplacingMergeTree` version
resolution, tombstones, a killable/revivable broker for failure
injection) that `StreamProcessor` runs identically against either.

## Testing

```bash
pytest -v                    # 83 unit tests, pure in-memory, ~0.1s
pytest --cov=streamguard      # coverage
```

The main test command needs no Docker daemon, no network, and no real
cluster — see the "Design choices" section above for why. A fully
optional, Docker-backed integration suite lives at
`tests/integration/test_testcontainers_e2e.py`; it is skipped by default
and only runs if you explicitly opt in:

```bash
pip install -e ".[integration]"
RUN_INTEGRATION_TESTS=1 pytest tests/integration/ -v
```

`docs/FAILURE_INJECTION.md` documents (and `tests/test_failure_injection.py`
verifies, offline) the "kill Kafka mid-stream, verify no data loss" scenario.

## Project layout

```
streamguard/            core package (parsing, dedup, schema evolution, windowing, sink, pipeline, main entrypoint)
tests/                  83 unit tests against in-memory fakes
tests/integration/      optional Testcontainers-based end-to-end test (skipped by default)
infra/postgres/         schema + publication + logical-replication config
infra/debezium/         connector configuration
infra/clickhouse/       sink schema (raw mirror tables + aggregates + a live view)
infra/grafana/          provisioned datasources + lag/throughput dashboard
infra/prometheus/       scrape config
scripts/                connector registration, offline demo
docs/FAILURE_INJECTION.md   documented broker-outage / no-data-loss test
docker-compose.yml      the full stack: Postgres, Redpanda, Debezium Connect, ClickHouse, Prometheus, Grafana
```

## Limitations

- The processor is a single Python consumer loop, not a horizontally scaled stream engine; the dedup seen-set is in memory and bounded, so replays older than its TTL are not detected.
- Only the `orders` and `customers` tables and the revenue-per-store aggregate are modelled.
- The Docker-backed integration test is opt-in and not run in CI.

## License

MIT — see [LICENSE](LICENSE).
