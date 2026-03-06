# Tableflow Catalog Sync

Registers Confluent Cloud Tableflow tables in Databricks Unity Catalog — for customers who need their data plane to stay private.

## The Problem

Customers running Confluent Cloud dedicated clusters with private networking have effectively chosen to **avoid public network paths for accessing their data infrastructure**, relying on PrivateLink / Private Endpoints for connectivity. Tableflow materializes Kafka topics as Delta Lake tables into customer-owned storage (BYOB), but two gaps prevent these tables from appearing in Unity Catalog:

1. **No Iceberg REST Catalog over private networking.** Confluent's built-in Iceberg REST Catalog (IRC) — the catalog that query engines would normally use to discover Tableflow tables — is not available over PrivateLink. There is no inbound private networking support for it today.

2. **Native catalog sync can't traverse private catalog endpoints.** Tableflow's built-in integrations (Unity Catalog, Snowflake Open Catalog, Polaris) require egress connectivity from Confluent's side to the customer's catalog. When that catalog is exposed only via PrivateLink / Private Endpoints, there's currently no supported private egress path from Confluent to it.

The data is sitting in the customer's own S3 bucket, accessible within their VPC — but there's no private-network-compatible catalog path to register it in Unity Catalog.

## What This Tool Does

It bridges the gap by discovering Tableflow-enabled topics via the Confluent Cloud control-plane API (metadata only — topic names and storage paths), then registering them as external tables in Unity Catalog. No data is copied. The data plane stays private.

## How It Works

```
Confluent Cloud API                    Your Machine
(control plane)                        (laptop / CI / any compute)

+---------------------+               +---------------------------+
| Confluent Cloud     |               |                           |
|                     |   1. Discover | +---------------------+   |
| Tableflow Topics    |<--------------| | python sync.py      |   |
| (storage locations) | via REST API  | +----------+----------+   |
+---------------------+  (HTTPS)      |            |              |
                                       |   2. Register tables      |
                                       |   (SQL over HTTPS)        |
                                       |            v              |
                                       | +---------------------+  |
                                       | | Databricks          |  |
                                       | | Unity Catalog       |  |
                                       | +---------------------+  |
                                       +---------------------------+
```

**What SQL does the sync run?**

| Action | SQL |
|--------|-----|
| Create catalog | `CREATE CATALOG IF NOT EXISTS <catalog>` |
| Create schema | `CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>` |
| List existing tables | `SELECT table_schema, table_name, comment FROM <catalog>.information_schema.tables WHERE table_type = 'EXTERNAL'` |
| Register new table | `CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<topic> USING DELTA LOCATION '<s3_path>' COMMENT '<s3_path>'` |
| Update changed table | `DROP TABLE IF EXISTS ...` then `CREATE TABLE ...` (metadata only — data untouched) |

The sync is **idempotent** — running it multiple times with no changes produces zero updates.

## Prerequisites

- **Confluent Cloud** account with a Cloud API key (org-level)
- **AWS account** with permissions to create VPC, IAM, S3, EC2 resources
- **Databricks workspace** with Unity Catalog, a personal access token, and a SQL warehouse
- **Tools**: Python 3.11+, Terraform 1.5+, AWS CLI configured

## Using with an Existing Environment

If you already have a Confluent Cloud dedicated cluster, BYOB bucket, and Databricks workspace set up, you can skip the Terraform provisioning and go straight to running the sync.

### What you need

| Component | Requirement |
|-----------|-------------|
| **Confluent Cloud** | Dedicated cluster with Tableflow-enabled topics (BYOB) |
| **Tableflow API key** | Scoped to Tableflow (`managed_resource: tableflow/v1`) — not an org-level Cloud API key |
| **Databricks** | Workspace with Unity Catalog, a SQL warehouse, and a personal access token |
| **Databricks storage credential** | IAM role (AWS) or managed identity (Azure) that can read the BYOB bucket |
| **Databricks external location** | Pointing to the BYOB bucket, using the storage credential above |

### Steps

1. **Create a `.env.sync` file** in the project root with your existing values:

    ```bash
    CONFLUENT_API_KEY=<tableflow-api-key>
    CONFLUENT_API_SECRET=<tableflow-api-secret>
    CONFLUENT_CLUSTER_ID=lkc-xxxxx
    CONFLUENT_ENVIRONMENT_ID=env-xxxxx
    DATABRICKS_HOST=https://dbc-xxxxx.cloud.databricks.com
    DATABRICKS_TOKEN=dapiXXXXXXXX
    DATABRICKS_WAREHOUSE_ID=<warehouse-id>
    TARGET_CATALOG=<catalog-name>
    TARGET_SCHEMA=<schema-name>
    ```

2. **Install and run:**

    ```bash
    # From: project root
    python3 -m venv .venv && source .venv/bin/activate
    pip install -e .

    set -a && source .env.sync && set +a
    python sync.py
    ```

The script will discover all Tableflow-enabled topics on the cluster, then register them as external tables in Unity Catalog.

> **Important:** The Databricks workspace must have a storage credential and external location configured for the BYOB bucket. Without these, `CREATE TABLE ... LOCATION 's3://...'` will fail with `NO_PARENT_EXTERNAL_LOCATION_FOR_PATH`. See the [Databricks docs on external locations](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html) for setup instructions.

## Three-Phase Setup (From Scratch)

All commands below assume you start from the **project root** (where `sync.py` and `pyproject.toml` live). Use this if you're setting up everything from scratch for a demo or POC.

### Phase 1: Provision Infrastructure

Terraform creates everything: Confluent Cloud dedicated cluster with PrivateLink, BYOB S3 bucket, VPC + networking, a bastion host, and Databricks resources (storage credential, external location, catalog, schema).

```bash
# From: project root
cd terraform/confluent-cloud
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your credentials:

```hcl
# Confluent Cloud — org-level Cloud API key
confluent_cloud_api_key    = "XXXXXXXXXXXX"
confluent_cloud_api_secret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# AWS
aws_region   = "us-east-1"
project_name = "tableflow-catalog-sync"   # must be globally unique (used for S3 bucket)

# Databricks
databricks_host       = "https://dbc-xxxxx.cloud.databricks.com"
databricks_token      = "dapiXXXXXXXX"
databricks_account_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

Apply:

```bash
# From: terraform/confluent-cloud/
terraform init && terraform apply
```

This takes ~30 minutes (dedicated cluster provisioning). When done, generate the env files:

```bash
# From: terraform/confluent-cloud/
terraform output -raw topics_env > ../../scripts/.env.topics
terraform output -raw sync_env   > ../../.env.sync
```

> **Note:** Edit `.env.sync` (in the project root) and fill in `DATABRICKS_WAREHOUSE_ID` with your SQL warehouse ID.

A **bastion host** is provisioned in the public subnet for Kafka data-plane operations over PrivateLink. The SSH key is saved to `terraform/confluent-cloud/bastion-key.pem`:

```bash
# From: terraform/confluent-cloud/
ssh -i bastion-key.pem ec2-user@$(terraform output -raw bastion_public_ip)
```

### Phase 2: Create Topics + Enable Tableflow

Create sample topics with datagen connectors and enable Tableflow on them. These scripts use the public Confluent Cloud API (`api.confluent.cloud`) — run from your local machine. Alternatively, you can create topics and enable Tableflow manually in the Confluent Cloud console.

```bash
# From: project root (your local machine)
./scripts/setup-topics.sh
```

The script:
1. Creates **datagen connectors** via the Connect API (auto-generates `orders` and `customers` topics with Avro schemas)
2. Waits for connectors to reach `RUNNING` state
3. Enables **Tableflow** on each topic via the Tableflow API (BYOB to the S3 bucket from Phase 1)

After the script completes, wait 2-3 minutes for Tableflow to materialize the initial Delta files in S3.

### Phase 3: Run the Sync (Demo)

```bash
# From: project root (your local machine)

# Install (one time)
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# Load environment variables
set -a && source .env.sync && set +a

# Run the sync
python sync.py
```

Expected output:

```
Discovering Tableflow topics for cluster lkc-xxxxx...
Found 2 Tableflow topics: customers, orders

Ensuring catalog 'tableflow_sync' and schema 'lkc-xxxxx' exist...
  SQL: CREATE CATALOG IF NOT EXISTS `tableflow_sync`
  SQL: CREATE SCHEMA IF NOT EXISTS `tableflow_sync`.`lkc-xxxxx`

Listing existing tables in tableflow_sync...
  SQL: SELECT table_schema, table_name, comment FROM tableflow_sync.information_schema.tables ...
Found 0 existing tables: (none)

Sync plan: 2 to add, 0 to update, 0 to remove

+ Adding: customers
  SQL: CREATE TABLE IF NOT EXISTS `tableflow_sync`.`lkc-xxxxx`.`customers` USING DELTA ...

+ Adding: orders
  SQL: CREATE TABLE IF NOT EXISTS `tableflow_sync`.`lkc-xxxxx`.`orders` USING DELTA ...

Done: 2 added, 0 updated, 0 removed
```

Tables are now visible in Databricks Unity Catalog under `tableflow_sync.<cluster_id>`.

Run it again to verify idempotency — zero changes:

```
Done: 0 added, 0 updated, 0 removed
```

**Live demo: add a new topic and sync it**

```bash
# From: project root (your local machine) — add a third topic
./scripts/add-topic.sh pageviews PAGEVIEWS

# Wait 2-3 minutes for Tableflow to materialize, then re-run sync
python sync.py
```

Expected: `1 added, 0 updated, 0 removed` — the new `pageviews` table appears in Unity Catalog alongside `orders` and `customers`.

Other available quickstart templates: `CLICKSTREAM`, `INVENTORY`, `CREDIT_CARDS`, `TRANSACTIONS`, `STORES`, `PRODUCTS`.

## Configuration

All configuration is via environment variables. Set them directly or use a `.env.sync` file (in the project root).

| Variable | Required | Description |
|----------|----------|-------------|
| `CONFLUENT_API_KEY` | Yes | Tableflow API key (from Terraform output) |
| `CONFLUENT_API_SECRET` | Yes | Tableflow API secret |
| `CONFLUENT_CLUSTER_ID` | Yes | Kafka cluster ID (e.g., `lkc-xxxxx`) |
| `CONFLUENT_ENVIRONMENT_ID` | Yes | Environment ID (e.g., `env-xxxxx`) |
| `DATABRICKS_HOST` | Yes | Workspace URL (e.g., `https://dbc-xxxxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Yes | Personal access token |
| `DATABRICKS_WAREHOUSE_ID` | Yes | SQL warehouse ID for executing statements |
| `TARGET_CATALOG` | Yes | Unity Catalog catalog name |
| `TARGET_SCHEMA` | No | Schema name (default: `default`) |

## Project Structure

```
sync.py                        # Entry point — run from bastion or any compute
.env.sync                      # Environment variables (generated by Terraform)
catalog_sync/
    engine.py                  # Diff-based sync (compare source vs target)
    handler.py                 # Lambda entry point (optional, for scheduled runs)
    config.py                  # Environment variable configuration
    models.py                  # TableInfo, ColumnInfo data classes
    sources/
        confluent_cloud.py     # Discovers Tableflow topics via REST API
    targets/
        unity_catalog.py       # Registers tables via Databricks SQL
scripts/
    setup-topics.sh            # Phase 2: create topics + enable Tableflow
    add-topic.sh               # Live demo: add a single new topic
    cleanup-topics.sh          # Tear down connectors, Tableflow, schemas, topics
    delete-topics.py           # Topic deletion via Kafka protocol (for bastion)
    .env.topics                # Topic script env vars (generated by Terraform)
terraform/
    confluent-cloud/           # Phase 1: all infrastructure
    demo/                      # Optional: Lambda + EventBridge deployment
```

## Automating the Sync

`sync.py` is a standalone script with two dependencies (`requests`, `databricks-sdk`) and nine environment variables. You can run it on a schedule from any compute that can reach `api.confluent.cloud` (HTTPS) and your Databricks workspace. Below are two examples.

### Option A: AWS Lambda + EventBridge

**What you need:**
- A Lambda deployment package containing `sync.py` and the `catalog_sync/` module plus dependencies
- An EventBridge rule to trigger the Lambda on a schedule
- The nine environment variables set on the Lambda configuration
- IAM role with permissions for CloudWatch Logs (and VPC networking if your Databricks workspace is behind PrivateLink)

**Steps:**

1. **Package the code.** The `scripts/build_lambda.sh` script creates a `dist/lambda.zip` with all dependencies bundled:

    ```bash
    # From: project root
    ./scripts/build_lambda.sh
    ```

2. **Create the Lambda function.** The entry point is `catalog_sync.handler.lambda_handler` — a thin wrapper around the same logic as `sync.py`. Set the nine environment variables (from `.env.sync`) on the Lambda configuration.

3. **Add a schedule.** Create an EventBridge rule with a rate expression (e.g., `rate(15 minutes)`) targeting the Lambda function.

4. **Networking.** The Lambda needs outbound HTTPS access to:
   - `api.confluent.cloud` (Tableflow API — requires NAT gateway if Lambda is in a VPC)
   - Your Databricks workspace URL (may require VPC attachment + PrivateLink if the workspace is private)

A working Terraform example is in `terraform/demo/` — it creates the Lambda, IAM role, and EventBridge schedule using outputs from the `terraform/confluent-cloud/` stack.

### Option B: Azure Functions + Timer Trigger

**What you need:**
- An Azure Function App (Python 3.11+ runtime)
- A timer trigger (cron expression, e.g., `0 */15 * * * *` for every 15 minutes)
- The nine environment variables set as Application Settings
- Outbound HTTPS access to `api.confluent.cloud` and your Databricks workspace

**Steps:**

1. **Create a Function App** with the Python runtime. Add `requests` and `databricks-sdk` to `requirements.txt`.

2. **Add the sync code.** Copy `sync.py` into your function directory and create a timer-triggered `__init__.py`:

    ```python
    # __init__.py
    import azure.functions as func
    import subprocess

    def main(timer: func.TimerRequest) -> None:
        subprocess.run(["python", "sync.py"], check=True)
    ```

    Or inline the logic directly — `sync.py` is self-contained and has no imports from `catalog_sync/`.

3. **Set Application Settings.** Add each variable from `.env.sync` as an Application Setting in the Azure portal (or via `az functionapp config appsettings set`).

4. **Networking.** If your Databricks workspace is behind a Private Endpoint, deploy the Function App into a VNet with a route to the Databricks private endpoint. The Confluent Cloud API is on the public internet and doesn't require special networking.

### Other options

The same pattern works anywhere you can run Python on a schedule:
- **Kubernetes CronJob** — container image with `sync.py` + deps, env vars from a Secret
- **GitHub Actions** — scheduled workflow, env vars from repository secrets
- **systemd timer / cron** — on any Linux host with Python installed

## Teardown

Clean up Confluent Cloud resources **before** running `terraform destroy` — Tableflow topics and connectors can block cluster deletion.

**Quick teardown** (connectors, Tableflow, schemas only — from laptop):

```bash
# From: project root (your local machine)
./scripts/cleanup-topics.sh
```

**Full teardown** (including topic deletion — requires bastion for PrivateLink access):

```bash
# From: project root (your local machine) — copy scripts to bastion
cd terraform/confluent-cloud
BASTION_IP=$(terraform output -raw bastion_public_ip)
scp -i bastion-key.pem \
  ../../scripts/cleanup-topics.sh ../../scripts/delete-topics.py ../../scripts/.env.topics \
  ec2-user@${BASTION_IP}:~

# SSH into bastion and run cleanup
ssh -i bastion-key.pem ec2-user@${BASTION_IP}
pip install confluent-kafka     # one-time setup
./cleanup-topics.sh

# Then destroy infrastructure (back on laptop)
cd terraform/confluent-cloud
terraform destroy
```

> **Tip:** If `terraform destroy` hangs or fails on Confluent resources, delete the environment manually in the Confluent Cloud console, then run `terraform destroy` again to clean up the AWS side.

**Reset the demo** (delete everything and re-create — without destroying infrastructure):

```bash
# From: project root (your local machine) — copy scripts to bastion
cd terraform/confluent-cloud
BASTION_IP=$(terraform output -raw bastion_public_ip)
scp -i bastion-key.pem \
  ../../scripts/cleanup-topics.sh ../../scripts/delete-topics.py ../../scripts/.env.topics \
  ec2-user@${BASTION_IP}:~

# SSH into bastion and run full cleanup (including topic deletion)
ssh -i bastion-key.pem ec2-user@${BASTION_IP}
set -a && source .env.topics && set +a
python delete-topics.py
exit

# Back on laptop — re-create topics
./scripts/setup-topics.sh
```

> **Note:** Topic deletion on dedicated PrivateLink clusters requires running from within the PrivateLink network (e.g., bastion). The Kafka REST API (port 443) is not served over PrivateLink — `delete-topics.py` uses the Kafka protocol (port 9092) via a Python admin client instead.

## Running Tests

```bash
# From: project root
pip install -e ".[dev]"
pytest tests/ -v
```

## Security and Network Architecture

### What stays private

The sensitive data path — customer data flowing from S3 into Databricks — never leaves the private network:

| Path | How | Private? |
|------|-----|----------|
| Databricks queries → S3 (reading table data) | S3 Gateway VPC Endpoint | **Yes** |
| Kafka producers/consumers → Confluent Cloud | AWS PrivateLink | **Yes** |

### What crosses the public internet

The sync script makes one outbound HTTPS call to the Confluent Cloud control plane API (`api.confluent.cloud`) to discover which topics have Tableflow enabled and their S3 storage paths. This is **metadata only** — topic names and S3 paths. No customer data, Kafka messages, or credentials are transmitted.

### Why this is acceptable

The Confluent Cloud control plane (`api.confluent.cloud`) is a **public-only endpoint** — there is no PrivateLink option for it ([docs](https://docs.confluent.io/cloud/current/networking/private-links/index.html)). Every Confluent Cloud customer, including those with fully private PrivateLink clusters, already relies on Confluent's public control-plane APIs for activities like logging into the console, using the CLI, and managing connectors, schemas, and Tableflow.

This sync script makes the same API call that a human would make by logging into the console and copying a storage path. The only difference is automation.

### Bastion host

The bastion host sits in the **public subnet** (for SSH access) and can reach Kafka endpoints via PrivateLink DNS (Route53 private zone associated with the VPC). It's available for any Kafka data-plane operations that require PrivateLink connectivity.

### Credential handling

| Credential | How it's used | Storage recommendation |
|-----------|---------------|----------------------|
| Confluent Cloud API key | Authenticate to Tableflow API (HTTPS) | Environment variable or secrets manager |
| Databricks token | Authenticate to Unity Catalog (HTTPS) | Environment variable or secrets manager |

Both credentials are transmitted over TLS. For automated deployments, store them in AWS Secrets Manager or SSM Parameter Store.

## Design Decisions

- **External tables, not foreign catalogs.** Unity Catalog "foreign catalogs" are for RDBMS/JDBC. Tableflow tables use `CREATE EXTERNAL TABLE ... USING DELTA|ICEBERG LOCATION`.
- **Metadata only.** Tables are registered by storage location reference. No data is copied.
- **Runs anywhere.** The sync script has no cloud-specific dependencies. Run it on your laptop, a bastion host, Lambda, Azure Functions, or a Kubernetes pod.
- **BYOB required.** Confluent-Managed Storage (CMS) does not work with private networking. BYOB is the supported path.
- **Delta and Iceberg.** The engine reads the table format from the Confluent Cloud API and uses `USING DELTA` or `USING ICEBERG` accordingly.

## Future: Native Sync

When Confluent adds private networking support for the Iceberg REST Catalog or enables native catalog sync (Unity Catalog, Polaris, etc.) over PrivateLink, this tool becomes unnecessary. To migrate:

1. Drop the externally registered tables (`DROP TABLE ...` — metadata only, data untouched)
2. Enable Tableflow's built-in catalog integration
3. Remove this tool
