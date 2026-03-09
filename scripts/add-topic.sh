#!/bin/bash
# ============================================================
# Add a new topic with Tableflow — for live demo
# ============================================================
# Creates a datagen connector for a new topic and enables
# Tableflow on it. Run this, then run `python sync.py` to
# show the new table appearing in Unity Catalog.
#
# Usage:
#   ./scripts/add-topic.sh [topic_name] [quickstart_template]
#
# Examples:
#   ./scripts/add-topic.sh                        # defaults: pageviews / PAGEVIEWS
#   ./scripts/add-topic.sh clickstream CLICKSTREAM
#   ./scripts/add-topic.sh inventory INVENTORY
#
# Available quickstart templates:
#   ORDERS, USERS, PAGEVIEWS, CLICKSTREAM, INVENTORY,
#   CREDIT_CARDS, TRANSACTIONS, STORES, PRODUCTS
# ============================================================

set -euo pipefail

TOPIC="${1:-pageviews}"
QUICKSTART="${2:-PAGEVIEWS}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env.topics"

if [ ! -f "$ENV_FILE" ]; then
  echo "Error: $ENV_FILE not found."
  echo "Generate it: cd terraform/confluent-cloud && terraform output -raw topics_env > ../../scripts/.env.topics"
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

CC_API="https://api.confluent.cloud"
CONNECT_URL="${CC_API}/connect/v1/environments/${ENVIRONMENT_ID}/clusters/${CLUSTER_ID}/connectors"
CONNECTOR_NAME="datagen_${TOPIC}"

echo "=== Creating datagen connector: ${CONNECTOR_NAME} (${QUICKSTART} -> ${TOPIC}) ==="
curl -s -X POST "$CONNECT_URL" \
  -u "${CONFLUENT_CLOUD_API_KEY}:${CONFLUENT_CLOUD_API_SECRET}" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"$CONNECTOR_NAME\",
    \"config\": {
      \"connector.class\": \"DatagenSource\",
      \"name\": \"$CONNECTOR_NAME\",
      \"kafka.auth.mode\": \"SERVICE_ACCOUNT\",
      \"kafka.service.account.id\": \"$SERVICE_ACCOUNT_ID\",
      \"kafka.topic\": \"$TOPIC\",
      \"output.data.format\": \"AVRO\",
      \"quickstart\": \"$QUICKSTART\",
      \"tasks.max\": \"1\",
      \"max.interval\": \"1000\"
    }
  }" | python3 -m json.tool 2>/dev/null || echo "(response above)"

echo ""
echo "=== Waiting for connector to start ==="
MAX_WAIT=120
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
  STATUS=$(curl -s "${CONNECT_URL}/${CONNECTOR_NAME}/status" \
    -u "${CONFLUENT_CLOUD_API_KEY}:${CONFLUENT_CLOUD_API_SECRET}" \
    | python3 -c "import json,sys; data=json.load(sys.stdin); print(data.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null)
  if [ "$STATUS" = "RUNNING" ]; then
    echo "Connector running."
    break
  fi
  echo "  Waiting... (${ELAPSED}s, status: $STATUS)"
  sleep 10
  ELAPSED=$((ELAPSED + 10))
done

echo ""
echo "=== Enabling Tableflow on: ${TOPIC} ==="
curl -s -X POST "${CC_API}/tableflow/v1/tableflow-topics" \
  -u "${TABLEFLOW_API_KEY}:${TABLEFLOW_API_SECRET}" \
  -H "Content-Type: application/json" \
  -d "{
    \"spec\": {
      \"display_name\": \"$TOPIC\",
      \"table_formats\": [\"DELTA\"],
      \"environment\": { \"id\": \"$ENVIRONMENT_ID\" },
      \"kafka_cluster\": { \"id\": \"$CLUSTER_ID\" },
      \"storage\": {
        \"kind\": \"ByobAws\",
        \"bucket_name\": \"$S3_BUCKET_NAME\",
        \"provider_integration_id\": \"$PROVIDER_INTEGRATION_ID\"
      }
    }
  }" | python3 -m json.tool 2>/dev/null || echo "(response above)"

echo ""
echo "=== Done ==="
echo "Topic '${TOPIC}' created with Tableflow enabled."
echo "Run 'python sync.py' to register it in Unity Catalog."
echo "(Topics still materializing are automatically skipped and picked up on the next run.)"
