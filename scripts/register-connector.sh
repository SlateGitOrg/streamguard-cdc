#!/usr/bin/env bash
# Registers the Debezium Postgres connector with Kafka Connect once the
# docker-compose stack is up. Run after `docker compose up -d`, once the
# `connect` service reports healthy (check with: curl localhost:8083/).
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="$(dirname "$0")/../infra/debezium/connector-config.json"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/" > /dev/null; do
  sleep 2
done

echo "Registering streamguard-postgres-connector ..."
curl -sf -X POST -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors" \
  || curl -sf -X PUT -H "Content-Type: application/json" \
       --data "$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "${CONFIG_FILE}")" \
       "${CONNECT_URL}/connectors/streamguard-postgres-connector/config"

echo
echo "Connector status:"
curl -sf "${CONNECT_URL}/connectors/streamguard-postgres-connector/status" | python3 -m json.tool
