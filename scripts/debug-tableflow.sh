#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

set -a
source "$ROOT_DIR/.env.sync"
set +a

echo "=== Querying Tableflow API ==="
echo "Cluster: $CONFLUENT_CLUSTER_ID"
echo "Environment: $CONFLUENT_ENVIRONMENT_ID"
echo ""

curl -s "https://api.confluent.cloud/tableflow/v1/tableflow-topics?spec.kafka_cluster=${CONFLUENT_CLUSTER_ID}&environment=${CONFLUENT_ENVIRONMENT_ID}" \
  -u "${CONFLUENT_API_KEY}:${CONFLUENT_API_SECRET}" \
  | python3 -m json.tool
