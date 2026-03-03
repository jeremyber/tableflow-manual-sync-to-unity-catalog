# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Catalog sync engine that bridges Confluent Cloud Tableflow (Iceberg tables) to Databricks Unity Catalog over private networks. Tableflow's native Unity Catalog integration can't traverse PrivateLink, so this tool runs inside the customer's VPC/VNet and syncs catalog metadata (no data movement).

## Architecture

- **catalog_sync/**: Cloud-agnostic Python sync engine
  - `sources/`: Pluggable catalog readers (Glue [primary], S3 discovery [fallback], Iceberg REST [future])
  - `targets/`: Catalog writers (Unity Catalog via Databricks SDK)
  - `engine.py`: Diff-based sync orchestration (add/update/remove tables)
  - `handler.py`: Lambda/serverless entry point with source/target factory
  - `config.py`: Environment-variable-driven configuration
- **terraform/aws/**: VPC, PrivateLink, Lambda, EventBridge deployment
- **demo/**: Sample Kafka data producer and Avro schemas

## Private Networking Constraints

- CMS (Confluent-Managed Storage) does NOT work with private networking — BYOB only
- Iceberg REST Catalog NOT available over PN (INIT-6185 in progress)
- External catalog sync (Unity, Snowflake OC) over PN not yet GA (INIT-9047)
- BYOB + S3 Gateway VPC Endpoints works today — this is the supported path

## Commands

```bash
# Install
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Tests
pytest tests/ -v                                    # all tests
pytest tests/unit/test_engine.py -v                 # single file
pytest tests/unit/test_engine.py::test_sync_registers_new_tables -v  # single test

# Build Lambda zip
./scripts/build_lambda.sh

# Terraform
cd terraform/aws && terraform init && terraform validate
cd terraform/aws && terraform plan -var databricks_workspace_url="https://example.databricks.com"
```

## Key Design Decisions

- Unity Catalog "foreign catalogs" are for RDBMS/JDBC only — Iceberg tables are registered as external tables using `CREATE TABLE ... USING ICEBERG LOCATION`
- Three source strategies ordered by PN viability: Glue (primary), S3 discovery (fallback), Iceberg REST (future)
- Sync is metadata-only — tables registered by S3 location reference, no data copied
- Core Python package is cloud-agnostic; cloud deployment handled by per-CSP Terraform modules
