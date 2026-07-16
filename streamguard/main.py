"""Production entrypoint: wires the real Redpanda/ClickHouse adapters
into StreamProcessor and runs it forever, exposing /metrics for
Prometheus/Grafana. This is what `Dockerfile` runs inside
`docker-compose.yml` -- it is NOT exercised by the unit test suite
(which uses the in-memory fakes instead), only by an actual docker
compose up.
"""

from __future__ import annotations

import http.server
import logging
import threading
import time

from streamguard.clickhouse_sink import RealClickHouseSink
from streamguard.config import PipelineConfig
from streamguard.dlq import DeadLetterRouter
from streamguard.kafka_io import RedpandaConsumer, RedpandaProducer
from streamguard.pipeline import StreamProcessor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("streamguard.main")


def _serve_metrics(processor: StreamProcessor, port: int) -> None:
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 -- http.server API
            if self.path == "/metrics":
                body = processor.metrics.to_prometheus_text().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args):  # silence default request logging
            pass

    server = http.server.HTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("metrics server listening on :%d/metrics", port)


def main() -> None:
    config = PipelineConfig.from_env()
    logger.info("starting streamguard stream processor: %s", config)

    sink = RealClickHouseSink(url=config.clickhouse_url, database=config.clickhouse_database)
    dlq_producer = RedpandaProducer(config.kafka_bootstrap_servers)
    dlq_router = DeadLetterRouter(dlq_producer, config.dlq_topic)

    processors = []
    for topic in config.source_topics:
        consumer = RedpandaConsumer(config.kafka_bootstrap_servers, topic, config.consumer_group_id)
        processor = StreamProcessor(
            consumer=consumer,
            dlq_router=dlq_router,
            sink=sink,
            window_size_ms=config.window_size_ms,
            allowed_lateness_ms=config.allowed_lateness_ms,
            source_topic=topic,
        )
        processors.append(processor)

    _serve_metrics(processors[0], config.metrics_port)

    logger.info("processing %d source topic(s)", len(processors))
    try:
        while True:
            idle = True
            for processor in processors:
                result = processor.process_one(timeout_s=0.5)
                if result.outcome != "empty":
                    idle = False
            if idle:
                time.sleep(0.2)
    except KeyboardInterrupt:
        logger.info("shutting down, flushing open aggregation windows")
        for processor in processors:
            processor.shutdown()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
