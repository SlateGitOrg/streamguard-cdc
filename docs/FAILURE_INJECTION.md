# Failure injection: killing Kafka mid-stream

**Claim under test:** if the Kafka/Redpanda broker disappears while
Debezium (or the load generator) is producing, and comes back later, no
committed database change is permanently lost from ClickHouse.

There are two ways to verify this in this repo, at two levels of
fidelity:

## 1. Offline, in unit tests (runs in CI, no Docker)

`tests/test_failure_injection.py` simulates the exact same failure mode
using `InMemoryBroker.kill()` / `.revive()`:

* While "killed", `InMemoryProducer.send()` buffers records in memory
  instead of publishing them (mirroring how `librdkafka`'s producer
  queue behaves against an unreachable broker) rather than raising or
  silently dropping them.
* `InMemoryProducer.flush()` drains that buffer once the broker is
  revived, publishing every buffered record in order.
* The test then asserts every record produced before, during, and after
  the outage is eventually consumed exactly once and the pipeline's
  in-memory ClickHouse mirror reaches the correct final state (no gaps,
  no duplicates).

Run it with the rest of the suite:

```bash
pytest tests/test_failure_injection.py -v
```

This is what `verified_here` in this project's manifest is based on: a
deterministic, offline reproduction of the failure mode and the
no-data-loss property, without needing a real cluster.

## 2. Against the real stack (manual, requires Docker)

To reproduce the same property with an actual Redpanda broker instead of
the in-memory fake:

```bash
# 1. Bring the whole stack up and register the Debezium connector.
docker compose up -d
./scripts/register-connector.sh

# 2. Start the load generator against the real Postgres.
docker compose exec streamguard-processor python -c "
from streamguard.loadgen import PostgresLoadGenerator, LoadGenConfig
PostgresLoadGenerator('postgresql://streamguard:streamguard@postgres:5432/streamguard').run(
    LoadGenConfig(num_orders=2000)
)"

# 3. Mid-stream, kill the broker.
docker kill streamguard-redpanda

# 4. Confirm Debezium/the load generator keep retrying rather than
#    crashing (check logs), then bring the broker back.
docker start streamguard-redpanda

# 5. Wait for the connector and stream processor to catch up (watch
#    `streamguard_consumer_lag` on the Grafana dashboard return to 0),
#    then compare row counts:
docker compose exec postgres psql -U streamguard -c "SELECT count(*) FROM orders;"
docker compose exec clickhouse clickhouse-client --query \
  "SELECT count(*) FROM streamguard.orders_live"

# The two counts should match once lag returns to zero -- every order
# written to Postgres before, during, and after the outage made it to
# ClickHouse.
```

### Why this is safe

* **Debezium** persists its replication slot position in Postgres itself
  (`streamguard_slot`), so a Kafka outage doesn't lose track of which WAL
  position it has already published -- it simply pauses and resumes.
* **Kafka Connect / the Debezium producer** buffers and retries sends
  against `librdkafka`'s producer queue (bounded by
  `queue.buffering.max.messages`); a short broker outage is absorbed
  without connector restart.
* **streamguard's consumer** commits offsets only after a record is
  durably reflected in ClickHouse (or dead-lettered), so a stream
  processor crash during the outage re-reads from the last committed
  offset rather than skipping ahead.
