#!/bin/bash
# ============================================================
# Setup Kafka Topics + Tableflow
# ============================================================
# All API calls go through the public Confluent Cloud API
# (api.confluent.cloud) — no PrivateLink or bastion required.
#
# Usage:
#   1. Generate the env file from Terraform:
#      cd terraform/confluent-cloud
#      terraform output -raw topics_env > ../../scripts/.env.topics
#
#   2. Run the script:
#      ./scripts/setup-topics.sh
#
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env.topics"

if [ ! -f "$ENV_FILE" ]; then
  echo "Error: $ENV_FILE not found."
  echo "Generate it from the confluent-cloud terraform outputs:"
  echo "  cd terraform/confluent-cloud"
  echo "  terraform output -raw topics_env > ../../scripts/.env.topics"
  exit 1
fi

source "$ENV_FILE"

CC_API="https://api.confluent.cloud"
CONNECT_URL="${CC_API}/connect/v1/environments/${ENVIRONMENT_ID}/clusters/${CLUSTER_ID}/connectors"

echo "=== Creating Datagen Connectors ==="
TOPICS="orders:ORDERS customers:USERS"

for entry in $TOPICS; do
  topic="${entry%%:*}"
  QUICKSTART="${entry##*:}"
  CONNECTOR_NAME="datagen_${topic}"
  echo "Creating datagen connector: $CONNECTOR_NAME (quickstart: $QUICKSTART -> topic: $topic)"
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
        \"kafka.topic\": \"$topic\",
        \"output.data.format\": \"AVRO\",
        \"quickstart\": \"$QUICKSTART\",
        \"tasks.max\": \"1\",
        \"max.interval\": \"1000\"
      }
    }" | python3 -m json.tool 2>/dev/null || echo "(response above)"
  echo ""
done

echo ""
echo "=== Waiting for connectors to start ==="
MAX_WAIT=120
ELAPSED=0
ALL_RUNNING=false
while [ $ELAPSED -lt $MAX_WAIT ]; do
  ALL_RUNNING=true
  for topic in orders customers; do
    STATUS=$(curl -s "${CONNECT_URL}/datagen_${topic}/status" \
      -u "${CONFLUENT_CLOUD_API_KEY}:${CONFLUENT_CLOUD_API_SECRET}" \
      | python3 -c "import json,sys; data=json.load(sys.stdin); print(data.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null)
    if [ "$STATUS" != "RUNNING" ]; then
      ALL_RUNNING=false
      break
    fi
  done
  if $ALL_RUNNING; then
    echo "All connectors running."
    break
  fi
  echo "  Waiting for connectors... (${ELAPSED}s)"
  sleep 10
  ELAPSED=$((ELAPSED + 10))
done

if ! $ALL_RUNNING; then
  echo "Warning: Not all connectors running after ${MAX_WAIT}s."
  echo "Proceeding with Tableflow enablement anyway — topics may already exist."
fi

echo ""
echo "=== Enabling Tableflow ==="
for topic in orders customers; do
  echo "Enabling Tableflow on topic: $topic"
  curl -s -X POST "${CC_API}/tableflow/v1/tableflow-topics" \
    -u "${TABLEFLOW_API_KEY}:${TABLEFLOW_API_SECRET}" \
    -H "Content-Type: application/json" \
    -d "{
      \"spec\": {
        \"display_name\": \"$topic\",
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
done

echo "=== Done ==="
echo "Topics created and Tableflow enabled."
echo "Once data flows through the topics, Tableflow will materialize"
echo "Delta + Iceberg tables in s3://$S3_BUCKET_NAME/"
