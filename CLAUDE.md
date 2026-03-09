# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Catalog sync engine for customers running Confluent Cloud clusters with private networking (enterprise with PNI, or dedicated with PrivateLink). Tableflow materializes Kafka topics as Delta Lake tables into customer-owned storage (BYOB), but there's no private-network-compatible catalog path to register them in Unity Catalog:

1. **Confluent's Iceberg REST Catalog is not available over private networking** — no inbound private networking support today
2. **Native catalog sync can't traverse private catalog endpoints** — Tableflow's built-in integrations (Unity Catalog, Polaris, etc.) require egress connectivity from Confluent's side to the customer's catalog, and there's currently no supported private egress path when the catalog is behind private networking

This tool bridges the gap: it discovers Tableflow-enabled topics via the Confluent Cloud control-plane API (metadata only — topic names and storage paths) and registers them as external tables in Databricks Unity Catalog. The data plane stays private — no customer data leaves the VPC.

## Prerequisites

Customers must already have:
- A Confluent Cloud enterprise (PNI) or dedicated (PrivateLink) cluster with Tableflow-enabled topics (BYOB)
- A Databricks workspace with Unity Catalog
- A Databricks SQL warehouse for executing statements

## Architecture

- **sync.py**: Self-contained entry point — runs from laptop, auto-loads `.env.sync` from project root, reads Confluent Cloud API for topic discovery, registers tables in Databricks. No imports from catalog_sync modules (except databricks SDK and requests).
- **catalog_sync/**: Modular Python engine (used by Lambda handler)
  - `sources/confluent_cloud.py`: Discovers Tableflow-enabled topics via REST API
  - `targets/unity_catalog.py`: Registers external tables in Databricks via SDK SQL execution
  - `engine.py`: Diff-based sync orchestration (add/update/remove tables)
  - `handler.py`: Builds source + target, runs engine (Lambda entry point)
  - `config.py`: Environment-variable-driven configuration
- **terraform/confluent-cloud/**: Provisions all infrastructure (CC enterprise cluster, VPC, PNI ENIs, BYOB, bastion with NGINX proxy, Databricks resources)
- **terraform/demo/**: Optional Lambda + EventBridge deployment
- **scripts/**: Topic setup (`setup-topics.sh`), cleanup (`cleanup-topics.sh`), live demo (`add-topic.sh`), Lambda packaging (`build_lambda.sh`)

## Three-Phase Workflow

1. **Provision** — `terraform apply` in `terraform/confluent-cloud/` (creates everything including bastion)
2. **Prepare** — Run `setup-topics.sh` from laptop (creates topics + enables Tableflow via public CC API)
3. **Demo** — `python sync.py` from laptop (registers tables in Unity Catalog)

## Where Things Run

| Operation | Where | Why |
|-----------|-------|-----|
| `sync.py` | Laptop (or any compute) | Uses Confluent Cloud public API + Databricks HTTPS |
| `setup-topics.sh` | Laptop | Uses Confluent Cloud public API (Connect, Tableflow) |
| `add-topic.sh` | Laptop | Uses Confluent Cloud public API |
| `cleanup-topics.sh` | Laptop (partial) + Bastion (topics) | Steps 1-3 use public API; topic deletion uses Kafka protocol (9092) via PNI |

## Bastion Host

- Sits in **public subnet** (SSH access via `bastion-key.pem`)
- Runs **NGINX stream proxy** that forwards Kafka (9092) and HTTPS (443) traffic through PNI ENIs
- PNI does not provide private DNS — the NGINX proxy uses SNI passthrough to route traffic
- Kafka clients on the bastion connect to `localhost:9092` (NGINX proxies to Confluent endpoints)
- No NAT gateway needed (bastion has internet via IGW in public subnet)
- To delete topics: SCP `cleanup-topics.sh` + `delete-topics.py` + `.env.topics` to bastion, SSH in, run script

## Commands

```bash
# Install
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run sync (auto-loads .env.sync from project root)
python sync.py

# Tests (must run from project root)
pytest tests/ -v
pytest tests/unit/test_engine.py -v                 # single file
pytest tests/unit/test_engine.py::test_sync_registers_new_tables -v  # single test

# Terraform
cd terraform/confluent-cloud && terraform init && terraform validate
```

## Key Design Decisions

- **Source**: Confluent Cloud API only — fetches topics with Tableflow enabled, gets `storage_location`
- **Table format**: Delta Lake only for demo (`table_formats: ["DELTA"]`). Dual format publishing (`["DELTA", "ICEBERG"]`) fails.
- **External tables, not foreign catalogs**: Tableflow tables registered via `CREATE TABLE ... USING DELTA LOCATION`
- **Metadata only**: Tables registered by storage location reference, no data copied
- **Idempotency**: S3 location stored in SQL COMMENT field; diff compares comment to detect changes
- **Runs anywhere**: sync.py has no cloud-specific dependencies (`requests` + `databricks-sdk`)
- **BYOB required**: Confluent-Managed Storage (CMS) does not work with private networking
- **S3 bucket has `force_destroy = true`**: Tableflow writes data; bucket must be emptied on destroy

## Confluent Cloud API Details

- `api.confluent.cloud` is **public-only** — no private networking option exists
- Tableflow API: `GET /tableflow/v1/tableflow-topics?spec.kafka_cluster={id}&environment={env_id}`
- Connect API: `POST /connect/v1/environments/{env}/clusters/{cluster}/connectors`
- Topic deletion requires the console, CLI, or Kafka protocol access — use the bastion's NGINX proxy (`localhost:9092`)
- Auth: HTTP Basic with API key/secret for all endpoints

## Common Errors

- **LOCATION_OVERLAP**: Old tables in a different schema point to same S3 path. Drop stale tables first.
- **Tableflow "Delta table modified externally"**: Databricks wrote to `_delta_log/` (happens if sync runs before Tableflow materializes). Now mitigated by two safeguards: (1) sync.py checks `status.phase == "RUNNING"` before registering, (2) Databricks external location is read-only. If it still happens: delete `_delta_log/` in S3, re-enable Tableflow.
- **Dual format publishing fails**: Use `["DELTA"]` only, not `["DELTA", "ICEBERG"]`
- **`pip install -e .` "Multiple top-level packages"**: Need `[tool.setuptools.packages.find] include = ["catalog_sync*"]` in pyproject.toml
- **`declare -A` fails with `set -u`**: Use `key:value` string parsing instead of bash associative arrays
- **PNI has no private DNS**: Confluent does not provide private DNS for PNI clusters. The bastion's NGINX proxy handles this via SNI passthrough. Kafka clients use `localhost:9092` on the bastion.
- **S3 bucket won't delete on terraform destroy**: Not empty (Tableflow data). `force_destroy = true` handles this.

## Environment Variables

### sync.py / .env.sync
- `CONFLUENT_API_KEY` / `CONFLUENT_API_SECRET` — Tableflow API key
- `CONFLUENT_CLUSTER_ID` — Kafka cluster ID (e.g., lkc-xxxxx)
- `CONFLUENT_ENVIRONMENT_ID` — Environment ID (e.g., env-xxxxx)
- `DATABRICKS_HOST` / `DATABRICKS_TOKEN` — Workspace URL + PAT
- `DATABRICKS_WAREHOUSE_ID` — SQL warehouse ID
- `TARGET_CATALOG` — Unity Catalog catalog name
- `TARGET_SCHEMA` — Schema name (defaults to "default")

### scripts / .env.topics
- `CONFLUENT_CLOUD_API_KEY` / `CONFLUENT_CLOUD_API_SECRET` — Org-level Cloud API key
- `TABLEFLOW_API_KEY` / `TABLEFLOW_API_SECRET` — Scoped to Tableflow
- `KAFKA_API_KEY` / `KAFKA_API_SECRET` — For Kafka protocol access (bootstrap server)
- `KAFKA_REST_ENDPOINT` — Cluster REST endpoint (PNI)
- `BOOTSTRAP_SERVER` — Kafka bootstrap endpoint (PNI — use via NGINX proxy on bastion)
- `SCHEMA_REGISTRY_URL` / `SCHEMA_REGISTRY_API_KEY` / `SCHEMA_REGISTRY_API_SECRET`
- `S3_BUCKET_NAME` / `PROVIDER_INTEGRATION_ID` — For BYOB setup

## Azure (Not Yet Built)

- `sync.py` is cloud-agnostic and works on Azure today
- Needs: `terraform/confluent-cloud-azure/` with VNet, Azure Private Link, ADLS Gen2, Azure bastion VM, Databricks resources
- BYOB would use ADLS Gen2 instead of S3
