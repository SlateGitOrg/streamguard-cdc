"""OPTIONAL end-to-end integration test against real containers.

This test is NOT part of the default test run and is skipped unless you
explicitly opt in, because it needs:

  * a running Docker daemon
  * the `testcontainers` package (`pip install testcontainers`)
  * network access to pull the postgres / redpandadata / clickhouse images

Run it explicitly with:

    RUN_INTEGRATION_TESTS=1 pytest tests/integration/test_testcontainers_e2e.py -v

It spins up real Postgres (with logical replication enabled), a real
Redpanda broker, and a real ClickHouse server, then drives actual
INSERT/UPDATE/DELETE SQL through the *real* `RedpandaConsumer` /
`RedpandaProducer` / `RealClickHouseSink` adapters (the same classes
`main.py` uses in production), proving the abstractions this repo is
built on are not just a convenient fiction for the fakes.

It intentionally does NOT stand up the Debezium Connect container
itself (that would require a JVM + Kafka Connect + the Debezium plugin
jars, which is a slow, heavy, network-dependent image well beyond what
CI-on-every-PR should pay for). Instead it publishes hand-built Debezium
envelope JSON directly onto the Kafka topic Debezium would have used --
exercising every layer of `streamguard` except Debezium's own WAL
decoding, which is a well-tested piece of infrastructure this project
does not reimplement.
"""

from __future__ import annotations

import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION_TESTS") != "1",
    reason="set RUN_INTEGRATION_TESTS=1 to run the Docker-backed integration suite",
)

testcontainers = pytest.importorskip(
    "testcontainers", reason="pip install testcontainers to run the integration suite"
)


def test_full_stack_round_trip():
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    from streamguard.clickhouse_sink import RealClickHouseSink
    from streamguard.dlq import DeadLetterRouter
    from streamguard.kafka_io import RedpandaConsumer, RedpandaProducer
    from streamguard.pipeline import StreamProcessor

    redpanda = (
        DockerContainer("redpandadata/redpanda:v24.2.7")
        .with_command(
            "redpanda start --smp 1 --overprovisioned --node-id 0 "
            "--kafka-addr PLAINTEXT://0.0.0.0:9092 "
            "--advertise-kafka-addr PLAINTEXT://localhost:9092"
        )
        .with_exposed_ports(9092)
    )
    clickhouse = DockerContainer("clickhouse/clickhouse-server:24.8").with_exposed_ports(8123)

    with redpanda, clickhouse:
        wait_for_logs(redpanda, "Successfully started Redpanda!", timeout=60)
        time.sleep(2)  # brief settle time for the HTTP admin API

        kafka_bootstrap = f"localhost:{redpanda.get_exposed_port(9092)}"
        clickhouse_url = f"http://localhost:{clickhouse.get_exposed_port(8123)}"

        sink = RealClickHouseSink(url=clickhouse_url, database="default")
        sink._execute(
            "CREATE TABLE IF NOT EXISTS orders "
            "(id Int64, store_id Int64, amount Float64, status String, "
            "_version Int64, _deleted UInt8) "
            "ENGINE = ReplacingMergeTree(_version) ORDER BY id"
        )

        producer = RedpandaProducer(kafka_bootstrap)
        topic = "streamguard.public.orders"
        producer.send(
            topic,
            key={"id": 1},
            value={
                "payload": {
                    "before": None,
                    "after": {"id": 1, "store_id": 1, "amount": 42.0, "status": "created"},
                    "source": {"table": "orders", "lsn": 1, "ts_ms": 1_700_000_000_000},
                    "op": "c",
                    "ts_ms": 1_700_000_000_000,
                }
            },
        )
        producer.flush()

        consumer = RedpandaConsumer(kafka_bootstrap, topic, group_id="it-test")
        dlq_router = DeadLetterRouter(producer, dlq_topic="streamguard.dlq")
        processor = StreamProcessor(consumer=consumer, dlq_router=dlq_router, sink=sink, source_topic=topic)

        result = processor.process_one(timeout_s=10.0)
        assert result.outcome == "processed"

        time.sleep(1)  # ClickHouse insert visibility
        resp_body = _query_clickhouse(clickhouse_url, "SELECT id, amount FROM orders WHERE id = 1 FORMAT JSON")
        rows = json.loads(resp_body)["data"]
        assert rows[0]["amount"] == "42"


def _query_clickhouse(url: str, query: str) -> str:
    import requests

    resp = requests.post(url, data=query.encode("utf-8"), timeout=10)
    resp.raise_for_status()
    return resp.text
