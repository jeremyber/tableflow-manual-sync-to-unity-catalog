# Tableflow Catalog Sync — Design Document

**Date:** 2026-03-02
**Status:** Approved

## Problem

Confluent Cloud Tableflow can natively integrate with Databricks Unity Catalog, but this integration cannot traverse private network boundaries (PrivateLink/Private Endpoints). This limitation applies across AWS, Azure, and GCP.

## Solution

A lightweight Python-based catalog sync engine that runs inside the customer's private network (VPC/VNet), reads Tableflow's Iceberg catalog metadata, and registers those tables as external Iceberg tables in Databricks Unity Catalog. Deployed as a serverless function (AWS Lambda / Azure Functions) on a configurable schedule.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Customer VPC / VNet                     │
│                                                             │
│  ┌──────────────────────┐    ┌──────────────────────────┐   │
│  │  Catalog Sync Engine │    │  Terraform Modules       │   │
│  │  (Python, portable)  │    │  ├── aws/  (Lambda+VPC)  │   │
│  │                      │    │  ├── azure/ (Func+VNet)  │   │
│  │  - Read from source  │    │  └── modules/ (shared)   │   │
│  │    catalog            │    │                          │   │
│  │  - Diff against UC   │    └──────────────────────────┘   │
│  │  - Register/update   │                                   │
│  │    external tables   │                                   │
│  └──────┬───────┬───────┘                                   │
│         │       │                                           │
│    PrivateLink  PrivateLink                                 │
│         │       │                                           │
│         ▼       ▼                                           │
│  ┌──────────┐ ┌──────────────┐                              │
│  │Confluent │ │  Databricks  │                              │
│  │Cloud     │ │  Unity       │                              │
│  │(Tableflow│ │  Catalog     │                              │
│  │ REST Cat)│ │              │                              │
│  └──────────┘ └──────────────┘                              │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

1. **External Iceberg tables, not foreign catalogs.** Unity Catalog's "foreign catalog" is for RDBMS (JDBC) sources. For Iceberg on S3, we register external tables using `CREATE TABLE ... USING iceberg LOCATION 's3://...'`.

2. **Pluggable source catalog.** Three source strategies, ordered by current private-networking viability:
   - **AWS Glue** (primary for demo) — Tableflow supports Glue as external catalog; Glue APIs work over VPC endpoints
   - **S3 metadata.json discovery** (universal fallback) — scans BYOB bucket for Iceberg metadata files directly
   - **Iceberg REST Catalog** (future) — Confluent's IRC is NOT available over private networking today (INIT-6185 in progress); kept as a source for when that ships

3. **Cloud-agnostic core.** The sync engine is pure Python with no cloud-specific dependencies. Cloud deployment handled by per-CSP Terraform modules.

4. **Metadata-only sync.** No data movement. Tables are registered by reference to their S3/storage location.

## Sync Engine

### Project Structure

```
catalog_sync/
├── __init__.py
├── engine.py              # Core sync orchestration
├── models.py              # Shared data models (TableInfo, SchemaInfo)
├── sources/
│   ├── base.py            # Abstract source interface
│   ├── iceberg_rest.py    # Confluent Iceberg REST Catalog
│   ├── glue.py            # AWS Glue Data Catalog
│   └── s3_discovery.py    # Direct S3 metadata.json scanning
├── targets/
│   ├── base.py            # Abstract target interface
│   └── unity_catalog.py   # Databricks Unity Catalog (via SDK)
└── config.py              # Configuration management
```

### Sync Algorithm

1. List all tables from the source catalog
2. List all external Iceberg tables currently in the target Unity Catalog (designated catalog/schema)
3. Diff: tables to add, update (schema changed), or remove
4. Execute registrations via Databricks SDK / REST API

### Dependencies

- `pyiceberg` — Iceberg REST catalog client
- `databricks-sdk` — Unity Catalog table management
- `boto3` — AWS Glue + S3 access (for Glue/S3 discovery sources)

## Deployment (Terraform)

### AWS

| Component | Purpose |
|-----------|---------|
| VPC + private subnets | Hosts Lambda, private networking |
| PrivateLink to Confluent | Private connectivity to Confluent Cloud |
| PrivateLink to Databricks | Private connectivity to Databricks workspace |
| S3 VPC Endpoint | Gateway endpoint for Iceberg data files |
| Lambda | Runs sync engine |
| EventBridge Rule | Configurable cron/rate schedule |
| IAM Role | S3 read, Secrets Manager access |
| Secrets Manager | Confluent API keys, Databricks credentials |

### Azure (Future)

| Component | Purpose |
|-----------|---------|
| VNet + subnets | Hosts Azure Function |
| Private Endpoints | Confluent + Databricks connectivity |
| Azure Functions (Premium) | Runs sync engine with VNet integration |
| Timer trigger | Configurable schedule |
| Key Vault | Credentials storage |

### Configurable Parameters

- Sync interval
- Source type (rest_catalog / glue / s3_discovery)
- Confluent Cloud environment + cluster details
- Databricks workspace URL + target catalog/schema
- S3 bucket for Iceberg tables
- Logging level

## Demo Flow

### Act 1 — Infrastructure

`terraform apply` provisions VPC, PrivateLink, Lambda, EventBridge, and Databricks workspace config. All private, no public endpoints.

### Act 2 — Streaming Data

- Create Kafka topics (`orders`, `customers`) in Confluent Cloud
- Produce sample data with Avro schemas via Schema Registry
- Enable Tableflow → Iceberg tables materialize on S3

### Act 3 — Catalog Sync

- Trigger Lambda manually (first run)
- Lambda discovers Tableflow tables, registers in Unity Catalog
- Open Databricks → tables visible in catalog explorer
- Query: `SELECT * FROM tableflow_catalog.default.orders`
- Show scheduled sync via EventBridge

### Act 4 — Schema Evolution

- Add field to Avro schema in Schema Registry
- Produce records with updated schema
- Tableflow evolves Iceberg table schema
- Next sync detects change, updates Unity Catalog registration
- Query shows new column

### Demo Artifacts

- Sample data producer (Python script)
- Sample Avro schemas
- Databricks notebook for querying

## Private Networking Constraints (Confirmed)

Per Confluent internal and public docs:

1. **CMS (Confluent-Managed Storage) does NOT work with private networking** — BYOB only for private clusters.
2. **Iceberg REST Catalog is NOT available over private networking** — no inbound PN support; INIT-6185 is the roadmap item to add it.
3. **External catalog sync (Unity, Snowflake OC, Polaris) over PN not yet GA** — egress PrivateLink is being addressed by INIT-9047.
4. **BYOB + S3 Gateway VPC Endpoints works today** — this is the supported private-networking path for Tableflow storage.

This means our demo must use BYOB storage with either Glue as the external catalog or direct S3 metadata scanning.

## Open Questions

1. **Databricks external table Iceberg support** — Verify exact `CREATE TABLE ... USING iceberg` syntax on current Databricks Runtime.
2. **Glue as Tableflow catalog** — Confirm Tableflow → Glue external catalog setup works smoothly for BYOB on private clusters.
