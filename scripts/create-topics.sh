#!/bin/bash
# ============================================================
# Create topics with datagen connectors + Tableflow
# ============================================================
# All API calls go through the public Confluent Cloud API
# (api.confluent.cloud) — runs from laptop, no bastion needed.
#
# Usage:
#   ./scripts/create-topics.sh                          # all defaults (orders, customers)
#   ./scripts/create-topics.sh pageviews                # single topic (infers PAGEVIEWS)
#   ./scripts/create-topics.sh inventory INVENTORY      # single topic with explicit quickstart
#
# Available quickstart templates:
#   ORDERS, USERS, PAGEVIEWS, CLICKSTREAM, INVENTORY,
#   CREDIT_CARDS, TRANSACTIONS, STORES, PRODUCTS
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

set -a
source "$ENV_FILE"
set +a

CC_API="https://api.confluent.cloud"
CONNECT_URL="${CC_API}/connect/v1/environments/${ENVIRONMENT_ID}/clusters/${CLUSTER_ID}/connectors"

# Default topics (topic:QUICKSTART pairs)
DEFAULT_TOPICS="orders:ORDERS customers:USERS"

# Known quickstart mappings for single-topic shorthand
KNOWN_QUICKSTARTS="orders:ORDERS customers:USERS pageviews:PAGEVIEWS clickstream:CLICKSTREAM inventory:INVENTORY"

# Build topic list based on arguments
if [ $# -eq 0 ]; then
  TOPICS="$DEFAULT_TOPICS"
  echo "=== Creating all default topics ==="
else
  TOPIC="$1"
  if [ $# -ge 2 ]; then
    QUICKSTART="$2"
  else
    # Look up known quickstart for this topic name
    QUICKSTART=""
    for entry in $KNOWN_QUICKSTARTS; do
      k="${entry%%:*}"
      v="${entry##*:}"
      if [ "$k" = "$TOPIC" ]; then
        QUICKSTART="$v"
        break
      fi
    done
    if [ -z "$QUICKSTART" ]; then
      QUICKSTART=$(echo "$TOPIC" | tr '[:lower:]' '[:upper:]')
      echo "No known quickstart for '$TOPIC', trying: $QUICKSTART"
    fi
  fi
  TOPICS="${TOPIC}:${QUICKSTART}"
  echo "=== Creating topic: ${TOPIC} (quickstart: ${QUICKSTART}) ==="
fi
echo ""

# --- Step 1: Create datagen connectors ---
echo "=== Step 1: Creating Datagen Connectors ==="
for entry in $TOPICS; do
  topic="${entry%%:*}"
  QUICKSTART="${entry##*:}"
  CONNECTOR_NAME="datagen_${topic}"
  echo "Creating connector: $CONNECTOR_NAME (${QUICKSTART} -> ${topic})"
  RESP=$(curl -s -w "\n%{http_code}" -X POST "$CONNECT_URL" \
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
    }") || true
  HTTP_CODE=$(echo "$RESP" | tail -1)
  BODY=$(echo "$RESP" | head -n -1)
  echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "  $BODY"
  echo ""
done

# --- Step 2: Wait for connectors ---
echo "=== Step 2: Waiting for connectors to start ==="
MAX_WAIT=120
ELAPSED=0
ALL_RUNNING=false
while [ $ELAPSED -lt $MAX_WAIT ]; do
  ALL_RUNNING=true
  for entry in $TOPICS; do
    topic="${entry%%:*}"
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
  echo "  Waiting... (${ELAPSED}s)"
  sleep 10
  ELAPSED=$((ELAPSED + 10))
done

if ! $ALL_RUNNING; then
  echo "Warning: Not all connectors running after ${MAX_WAIT}s."
  echo "Proceeding with Tableflow enablement anyway — topics may already exist."
fi

# --- Step 3: Enable Tableflow ---
echo ""
echo "=== Step 3: Enabling Tableflow ==="
for entry in $TOPICS; do
  topic="${entry%%:*}"
  echo "Enabling Tableflow on: $topic"
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
echo "Topics created with Tableflow enabled."
echo "Run 'python sync.py' to register them in Unity Catalog."
echo "(Topics still materializing are automatically skipped and picked up on the next run.)"
