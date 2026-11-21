#!/usr/bin/env bash
# Registers (or updates) the Debezium Postgres connector. Idempotent: PUT /config
# creates the connector if missing and updates it otherwise.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="${CONNECTOR_NAME:-ecom-postgres}"
CONFIG="${1:-connect/postgres-connector.json}"

echo "waiting for Kafka Connect at $CONNECT_URL ..."
for _ in $(seq 1 60); do
  curl -sf "$CONNECT_URL/connectors" >/dev/null && break
  sleep 3
done

if ! curl -sf "$CONNECT_URL/connector-plugins" | grep -q "PostgresConnector"; then
  echo "Debezium PostgresConnector plugin not found in Connect. Check: docker compose logs connect" >&2
  exit 1
fi

curl -sf -X PUT -H "Content-Type: application/json" \
  --data @"$CONFIG" "$CONNECT_URL/connectors/$NAME/config" >/dev/null
echo "connector '$NAME' registered"

sleep 5
curl -s "$CONNECT_URL/connectors/$NAME/status"; echo
