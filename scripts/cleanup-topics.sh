#!/bin/bash
# ============================================================
# Cleanup — Delete connectors, Tableflow, schemas, and topics
# ============================================================
# Steps 1-3 use Confluent Cloud public APIs — run from anywhere.
# Step 4 (topic deletion) uses Kafka protocol (port 9092) over
# PrivateLink — only works from bastion or VPC-connected host.
#
# Usage:
#   From laptop:  ./scripts/cleanup-topics.sh  (steps 1-3 only)
#   From bastion: ./cleanup-topics.sh          (all steps)
# ============================================================
set -euo pipefail

# Use python3.11 if available (bastion), else python3
PYTHON=$(command -v python3.11 2>/dev/null || command -v python3 2>/dev/null || echo "python3")

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env.topics"

if [ ! -f "$ENV_FILE" ]; then
  echo "Error: $ENV_FILE not found."
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

CC_API="https://api.confluent.cloud"
CONNECT_URL="${CC_API}/connect/v1/environments/${ENVIRONMENT_ID}/clusters/${CLUSTER_ID}/connectors"

echo "=== Step 1: Deleting Datagen Connectors ==="
for name in datagen_orders datagen_customers datagen_pageviews; do
  echo "Deleting connector: $name"
  RESP=$(curl -s -w "\n%{http_code}" -X DELETE "${CONNECT_URL}/${name}" \
    -u "${CONFLUENT_CLOUD_API_KEY}:${CONFLUENT_CLOUD_API_SECRET}") || true
  HTTP_CODE=$(echo "$RESP" | tail -1)
  BODY=$(echo "$RESP" | head -n -1)
  if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
    echo "  Deleted."
  elif [ "$HTTP_CODE" = "404" ]; then
    echo "  Not found (already deleted)."
  else
    echo "  HTTP $HTTP_CODE: $BODY"
  fi
done

echo ""
echo "=== Step 2: Disabling Tableflow ==="
TABLEFLOW_RESP=$(curl -sS -w "\n%{http_code}" \
  "${CC_API}/tableflow/v1/tableflow-topics?spec.kafka_cluster=${CLUSTER_ID}&environment=${ENVIRONMENT_ID}" \
  -u "${TABLEFLOW_API_KEY}:${TABLEFLOW_API_SECRET}") || true
TF_HTTP_CODE=$(echo "$TABLEFLOW_RESP" | tail -1)
TF_BODY=$(echo "$TABLEFLOW_RESP" | head -n -1)

if [ "$TF_HTTP_CODE" != "200" ]; then
  echo "  Failed to list Tableflow topics (HTTP $TF_HTTP_CODE): $TF_BODY"
else
  TABLEFLOW_TOPICS=$($PYTHON -c "
import json, sys
data = json.loads(sys.stdin.read())
for t in data.get('data', []):
    tid = t.get('id', '')
    name = t.get('spec', {}).get('display_name', '')
    if tid:
        print(tid + '|' + name)
" <<< "$TF_BODY" 2>&1) || true

  if [ -z "$TABLEFLOW_TOPICS" ]; then
    echo "No Tableflow topics found (already disabled)."
  else
    while IFS='|' read -r tid tname; do
      [ -z "$tid" ] && continue
      echo "Deleting Tableflow topic: $tname (id: $tid)"
      RESP=$(curl -sS -w "\n%{http_code}" -X DELETE \
        "${CC_API}/tableflow/v1/tableflow-topics/${tid}?environment=${ENVIRONMENT_ID}&spec.kafka_cluster=${CLUSTER_ID}" \
        -u "${TABLEFLOW_API_KEY}:${TABLEFLOW_API_SECRET}") || true
      HTTP_CODE=$(echo "$RESP" | tail -1)
      BODY=$(echo "$RESP" | head -n -1)
      if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "  Deleted."
      else
        echo "  HTTP $HTTP_CODE: $BODY"
      fi
    done <<< "$TABLEFLOW_TOPICS"
  fi
fi

echo ""
echo "=== Step 3: Deleting Schemas ==="
for subject in orders-value customers-value pageviews-value; do
  echo "Deleting schema: $subject"
  # Soft delete first, then hard delete
  curl -s -o /dev/null -X DELETE \
    "${SCHEMA_REGISTRY_URL}/subjects/${subject}" \
    -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}" 2>/dev/null || true
  curl -s -o /dev/null -X DELETE \
    "${SCHEMA_REGISTRY_URL}/subjects/${subject}?permanent=true" \
    -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}" 2>/dev/null || true
  echo "  Done."
done

echo ""
echo "=== Step 4: Deleting Kafka Topics (requires PrivateLink) ==="

# Find the delete-topics.py script
DELETE_SCRIPT="${SCRIPT_DIR}/delete-topics.py"
if [ ! -f "$DELETE_SCRIPT" ]; then
  DELETE_SCRIPT="./delete-topics.py"
fi

if [ ! -f "$DELETE_SCRIPT" ]; then
  echo "delete-topics.py not found. Skipping topic deletion."
else
  # Install confluent-kafka if needed
  $PYTHON -c "import confluent_kafka" 2>/dev/null || {
    echo "Installing confluent-kafka..."
    $PYTHON -m pip install --quiet confluent-kafka
  }

  # The Python script has its own connectivity check with a timeout.
  # If it can't reach the bootstrap server, it exits with a clear message.
  $PYTHON "$DELETE_SCRIPT" || true
fi

# If topics weren't deleted (not on PrivateLink network), show instructions
BOOTSTRAP_HOST=$(echo "$KAFKA_REST_ENDPOINT" | sed 's|https://||' | sed 's|/.*||' | sed 's|:.*||')
if ! getent hosts "$BOOTSTRAP_HOST" > /dev/null 2>&1; then
  echo ""
  echo "Topic deletion requires PrivateLink access. To run from the bastion:"
  echo ""
  echo "  cd terraform/confluent-cloud"
  echo "  BASTION_IP=\$(terraform output -raw bastion_public_ip)"
  echo "  scp -i bastion-key.pem \\"
  echo "    ../../scripts/cleanup-topics.sh ../../scripts/delete-topics.py ../../scripts/.env.topics \\"
  echo "    ec2-user@\${BASTION_IP}:~"
  echo ""
  echo "  ssh -i bastion-key.pem ec2-user@\${BASTION_IP}"
  echo "  ./cleanup-topics.sh"
fi

echo ""
echo "=== Done ==="
echo "Re-run setup-topics.sh to recreate."
