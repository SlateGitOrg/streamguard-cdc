FROM python:3.11-slim AS base

# librdkafka is required by confluent-kafka (used only in the real
# deployment adapters, streamguard/kafka_io.py's Redpanda* classes).
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
COPY streamguard ./streamguard

RUN pip install --no-cache-dir ".[prod]"

ENV PYTHONUNBUFFERED=1
EXPOSE 9464

CMD ["python", "-m", "streamguard.main"]
