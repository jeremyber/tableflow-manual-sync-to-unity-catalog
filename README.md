# Tableflow Catalog Sync

Syncs Confluent Cloud Tableflow tables and governance tags to Databricks Unity Catalog.

**Two capabilities:**
- **Table sync** — registers Tableflow-materialized tables in Unity Catalog (for customers on private networking where Confluent's native catalog sync can't reach UC)
- **Tag sync** — syncs classification tags (`PII`, `Sensitive`) and business metadata (`DataOwnership.owner=payments-team`) from Confluent Cloud to Unity Catalog table tags (for any Tableflow + UC customer)

## The Problem

**1. No catalog path over private networking.** Customers running Confluent Cloud clusters with private networking (enterprise with PNI, or dedicated with PrivateLink) can't use Confluent's built-in Iceberg REST Catalog or native catalog sync to register Tableflow tables in Unity Catalog. The data sits in the customer's own S3 bucket — but UC doesn't know about it.

**2. Governance metadata is silently dropped.** When Tableflow materializes topics into tables and syncs them to Unity Catalog, classification tags and business metadata applied in Confluent Cloud are not carried over. Customers must manually re-tag every table in Databricks, doubling governance effort and creating drift between the two systems.

## What This Tool Does

```
Confluent Cloud                         Your Machine
                                        (laptop / CI / any compute)

+---------------------+                +---------------------------+
| Tableflow API       |  1. Discover   |                           |
| (topic + storage    |<---------------| python sync.py            |
|  metadata)          |  via REST API  |   or                      |
+---------------------+                | python sync_tags.py       |
                                        |            |              |
+---------------------+  2. Fetch tags |            |              |
| Stream Catalog      |<---------------+            |              |
| GraphQL API         |  (tags + BM)   |            |              |
+---------------------+                |   3. Register tables      |
                                        |      + apply tags         |
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
| List existing tables | `SELECT ... FROM <catalog>.information_schema.tables WHERE table_type = 'EXTERNAL'` |
| Register new table | `CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<topic> USING DELTA LOCATION '<s3_path>'` |
| Update changed table | `DROP TABLE IF EXISTS ...` then `CREATE TABLE ...` (metadata only — data untouched) |
| Read table tags | `SELECT tag_name, tag_value FROM <catalog>.information_schema.table_tags WHERE ...` |
| Apply tags | `ALTER TABLE ... SET TAGS ('PII' = 'true', ...)` |
| Remove stale tags | `ALTER TABLE ... UNSET TAGS ('PII')` |

The sync is **idempotent** — running it multiple times with no changes produces zero updates.

## Prerequisites

- **Confluent Cloud** account with a Tableflow API key (scoped to `tableflow/v1`)
- **Databricks workspace** with Unity Catalog and a SQL warehouse
- **Authentication**: Databricks personal access token or service principal (client ID + secret)
- **For table sync** (`sync.py`): storage credential + external location in UC pointing to the BYOB bucket
- **For tag sync**: Stream Governance Advanced package, Schema Registry API key
- **For Terraform setup** (optional): AWS account, Python 3.11+, Terraform 1.5+

## Using with an Existing Environment

If you already have a Confluent Cloud cluster (enterprise or dedicated) with BYOB, and a Databricks workspace set up, you can skip the Terraform provisioning and go straight to running the sync.

### What you need

| Component | Requirement |
|-----------|-------------|
| **Confluent Cloud** | Enterprise or dedicated cluster with Tableflow-enabled topics (BYOB) |
| **Tableflow API key** | Scoped to Tableflow (`managed_resource: tableflow/v1`) — not an org-level Cloud API key |
| **Schema Registry API key** | For tag sync — scoped to the SR cluster in your environment |
| **Stream Governance** | Advanced package required for classification tags and business metadata |
| **Databricks** | Workspace with Unity Catalog, a SQL warehouse, and a personal access token or service principal |
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

    # Tag sync (optional — enabled by default)
    # SYNC_TAGS=true
    SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.aws.confluent.cloud
    SCHEMA_REGISTRY_API_KEY=<sr-api-key>
    SCHEMA_REGISTRY_API_SECRET=<sr-api-secret>
    ```

2. **Install and run:**

    ```bash
    # From: project root
    python3 -m venv .venv && source .venv/bin/activate
    pip install -e .

    python sync.py
    ```

The script will discover all Tableflow-enabled topics on the cluster, then register them as external tables in Unity Catalog.

> **Important:** The Databricks workspace must have a storage credential and external location configured for the BYOB bucket. Without these, `CREATE TABLE ... LOCATION 's3://...'` will fail with `NO_PARENT_EXTERNAL_LOCATION_FOR_PATH`. See the [Databricks docs on external locations](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html) for setup instructions.

## Three-Phase Setup (From Scratch)

All commands below assume you start from the **project root** (where `sync.py` and `pyproject.toml` live). Use this if you're setting up everything from scratch for a demo or POC.

### Phase 1: Provision Infrastructure

Terraform creates everything: Confluent Cloud enterprise cluster with PNI (Private Network Interface), BYOB S3 bucket, VPC + networking, ENIs, a bastion host with NGINX proxy, and Databricks resources (storage credential, external location, catalog, schema).

> **Note:** The Terraform provisions an enterprise cluster. For dedicated clusters with PrivateLink, see the git history or [Confluent docs](https://docs.confluent.io/cloud/current/networking/private-links/aws-privatelink.html).

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

When done, generate the env files:

```bash
# From: terraform/confluent-cloud/
terraform output -raw topics_env > ../../scripts/.env.topics
terraform output -raw sync_env   > ../../.env.sync
```

> **Note:** Edit `.env.sync` (in the project root) and fill in `DATABRICKS_WAREHOUSE_ID` with your SQL warehouse ID.

A **bastion host** is provisioned in the public subnet with an NGINX stream proxy for Kafka data-plane operations through PNI. The SSH key is saved to `terraform/confluent-cloud/bastion-key.pem`:

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

> **Note:** The demo script does not set up governance tags. To test tag sync, apply tags to your topics via the [Confluent Cloud Console](https://confluent.cloud/) (Stream Catalog UI) or the [Stream Catalog REST API](https://docs.confluent.io/cloud/current/stream-governance/stream-catalog-rest-apis.html) after Phase 2, then run Phase 3 with `SYNC_TAGS=true`.

After the script completes, Tableflow begins materializing Delta files in S3. The sync script automatically checks that each topic's Tableflow status is `RUNNING` before registering it — topics still materializing are skipped and will be picked up on the next run.

### Phase 3: Run the Sync (Demo)

```bash
# From: project root (your local machine)

# Install (one time)
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# Run the sync (auto-loads .env.sync from the project root)
python sync.py
```

Expected output:

```
Discovering Tableflow topics for cluster lkc-xxxxx...
Found 2 Tableflow topics: customers, orders

Ensuring catalog 'tableflow_sync' and schema 'lkc-xxxxx' exist...
  SQL: CREATE CATALOG IF NOT EXISTS `tableflow_sync`
  SQL: CREATE SCHEMA IF NOT EXISTS `tableflow_sync`.`lkc-xxxxx`

Listing existing tables in tableflow_sync.lkc-xxxxx...
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

# Re-run sync (safe to run immediately — topics still materializing are skipped)
python sync.py
```

Expected: `1 added, 0 updated, 0 removed` — the new `pageviews` table appears in Unity Catalog alongside `orders` and `customers`. If the topic hasn't finished materializing yet, it will show as skipped and will be picked up on the next run.

Other available quickstart templates: `CLICKSTREAM`, `INVENTORY`, `CREDIT_CARDS`, `TRANSACTIONS`, `STORES`, `PRODUCTS`.

## Configuration

All configuration is via environment variables. `sync.py` automatically loads `.env.sync` from the project root if present. You can also set variables directly in the environment (env vars take precedence over the file).

| Variable | Required | Description |
|----------|----------|-------------|
| `CONFLUENT_API_KEY` | Yes | Tableflow API key (from Terraform output) |
| `CONFLUENT_API_SECRET` | Yes | Tableflow API secret |
| `CONFLUENT_CLUSTER_ID` | Yes | Kafka cluster ID (e.g., `lkc-xxxxx`) |
| `CONFLUENT_ENVIRONMENT_ID` | Yes | Environment ID (e.g., `env-xxxxx`) |
| `DATABRICKS_HOST` | Yes | Workspace URL (e.g., `https://dbc-xxxxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Yes* | Personal access token (*or use `CLIENT_ID`/`CLIENT_SECRET` instead) |
| `DATABRICKS_CLIENT_ID` | No | Service principal client ID (alternative to token) |
| `DATABRICKS_CLIENT_SECRET` | No | Service principal client secret |
| `DATABRICKS_WAREHOUSE_ID` | Yes | SQL warehouse ID for executing statements |
| `TARGET_CATALOG` | Yes | Unity Catalog catalog name |
| `TARGET_SCHEMA` | No | Schema name (default: `default`) |
| `SYNC_TAGS` | No | Sync governance tags (default: `true`) |
| `SCHEMA_REGISTRY_URL` | If `SYNC_TAGS=true` | Schema Registry endpoint (or Stream Catalog URL for SR PrivateLink) |
| `SCHEMA_REGISTRY_API_KEY` | If `SYNC_TAGS=true` | Schema Registry API key |
| `SCHEMA_REGISTRY_API_SECRET` | If `SYNC_TAGS=true` | Schema Registry API secret |

## Governance Tag Sync

Confluent Cloud classification tags (e.g., `PII`, `Sensitive`) and business metadata (e.g., `DataOwnership.owner=payments-team`) can be automatically synced to Unity Catalog table tags.

### Prerequisites for tag sync

1. **Stream Governance Advanced** package enabled on your Confluent Cloud environment
2. **Tags applied to your Kafka topics** — via the [Confluent Cloud Console](https://confluent.cloud/) (Stream Catalog UI) or the [Stream Catalog REST API](https://docs.confluent.io/cloud/current/stream-governance/stream-catalog-rest-apis.html)
3. **Schema Registry API key** for the environment

### How it works

Tags are fetched from the [Stream Catalog GraphQL API](https://docs.confluent.io/cloud/current/stream-governance/graphql.html) in a single paginated query (1–2 API calls regardless of topic count), then applied to UC tables via `ALTER TABLE SET TAGS`.

| Confluent Source | UC Tag |
|---|---|
| Classification tag `PII` | `PII = true` |
| BM `DataOwnership` → `owner=payments-team` | `DataOwnership_owner = payments-team` |
| BM `DataOwnership` → `priority=1` (int) | `DataOwnership_priority = 1` (stringified) |

**Safety:**
- UC-native tags (added directly in Databricks) are never touched
- A manifest tracks which tags are managed by this tool (stored in table properties for `sync.py`, Databricks workspace file for `sync_tags.py`)
- Tags removed from Confluent are fully removed from UC via `ALTER TABLE UNSET TAGS`
- Tag sync failures don't block table sync

### Which mode to use

1. **Want to sync both tables and tags via this tool?** Use `sync.py` — handles everything in one pass. This is the default and recommended mode.

2. **Only need table sync, no tags?** Use `sync.py` with `SYNC_TAGS=false` — no Schema Registry credentials needed.

3. **Already using Confluent's native Tableflow catalog sync?** Use `sync_tags.py` — syncs tags to tables already in Unity Catalog. No Tableflow API key needed.

> **Important:** Do not run `sync.py` with `SYNC_TAGS=true` and `sync_tags.py` against the same tables. Both manage tags with separate manifests and will conflict. Pick one approach for tags.

| | `sync.py` | `SYNC_TAGS=false sync.py` | `sync_tags.py` |
|---|---|---|---|
| Creates/updates tables | Yes | Yes | No |
| Syncs governance tags | Yes | No | Yes |
| Needs Tableflow API key | Yes | Yes | No |
| Needs SR credentials | Yes | No | Yes |
| Needs CC Environment ID | Yes | Yes | No |

See [BEST_PRACTICES.md](BEST_PRACTICES.md) for deployment patterns, multi-cluster setups, and operational guidance.

### Tag sync configuration

Tag sync is enabled by default in `sync.py`. To disable:

```bash
SYNC_TAGS=false python sync.py
```

Additional environment variables for tag sync (required when `SYNC_TAGS=true`, or when using `sync_tags.py`):

| Variable | Description |
|----------|-------------|
| `SCHEMA_REGISTRY_URL` | Schema Registry endpoint URL. For SR PrivateLink, use the Stream Catalog URL instead. |
| `SCHEMA_REGISTRY_API_KEY` | Schema Registry API key |
| `SCHEMA_REGISTRY_API_SECRET` | Schema Registry API secret |

### Running tags-only sync

Create a `.env.sync` with the tags-only configuration (no Tableflow API key needed):

```bash
CONFLUENT_CLUSTER_ID=lkc-xxxxx
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.aws.confluent.cloud
SCHEMA_REGISTRY_API_KEY=<sr-api-key>
SCHEMA_REGISTRY_API_SECRET=<sr-api-secret>
DATABRICKS_HOST=https://dbc-xxxxx.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXX
# Or use service principal instead of token:
# DATABRICKS_CLIENT_ID=<client-id>
# DATABRICKS_CLIENT_SECRET=<client-secret>
DATABRICKS_WAREHOUSE_ID=<warehouse-id>
TARGET_CATALOG=<catalog-name>
TARGET_SCHEMA=<schema-name>
```

Then run:

```bash
# From: project root
python sync_tags.py
```

Expected output:

```
Listing tables in tableflow_sync.lkc-xxxxx...
Found 2 table(s): customers, orders

Fetching governance tags via GraphQL...
  customers: PII=true, PRIVATE=true
  orders: DataOwnership_owner=payments-team

Syncing governance tags for 2 table(s)...
  customers: 2 tag(s) synced
  orders: 1 tag(s) synced

Done: 3 tag change(s) applied across 2 table(s)
```

### Testing the tag sync

After running `sync.py` or `sync_tags.py`, verify in Databricks:

```sql
-- Check tags on a table
SELECT tag_name, tag_value
FROM <catalog>.information_schema.table_tags
WHERE table_name = '<topic-name>';
```

Test the sync loop:

| Action | Then run | Expected |
|---|---|---|
| Add a classification tag (e.g., `PII`) to a topic in the CC Console | `python sync_tags.py` | `PII=true` appears on UC table |
| Add business metadata to a topic in the CC Console | `python sync_tags.py` | `TypeName_attr=value` appears on UC table |
| Remove a tag from a topic in the CC Console | `python sync_tags.py` | Tag removed from UC |
| Add a tag directly on the UC table in Databricks | `python sync_tags.py` | UC-native tag untouched |
| No changes | `python sync_tags.py` | `0 tag change(s) applied` |

### Private networking considerations

The GraphQL endpoint is served at `{SCHEMA_REGISTRY_URL}/catalog/graphql`. If Schema Registry PrivateLink is enabled, set `SCHEMA_REGISTRY_URL` to the Stream Catalog URL (`STREAM_CATALOG_URL`) — the standard SR endpoint will not work for tag operations.

## Project Structure

```
sync.py                        # Full sync — tables + tags
sync_tags.py                   # Tags-only sync — for use with CC native catalog sync
.env.sync                      # Environment variables (generated by Terraform)
catalog_sync/
    engine.py                  # Diff-based sync (compare source vs target)
    handler.py                 # Lambda entry point (optional, for scheduled runs)
    config.py                  # Environment variable configuration
    models.py                  # TableInfo, ColumnInfo data classes
    sources/
        confluent_cloud.py     # Discovers Tableflow topics + tags via GraphQL
    targets/
        unity_catalog.py       # Registers tables + syncs tags via Databricks SQL
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

Both `sync.py` and `sync_tags.py` are standalone scripts with two dependencies (`requests`, `databricks-sdk`). You can run either on a schedule from any compute that can reach `api.confluent.cloud` (HTTPS) and your Databricks workspace. Below are two examples using `sync.py` — the same patterns apply to `sync_tags.py`.

### Option A: AWS Lambda + EventBridge

**What you need:**
- A Lambda deployment package containing `sync.py` and the `catalog_sync/` module plus dependencies
- An EventBridge rule to trigger the Lambda on a schedule
- The environment variables set on the Lambda configuration (see Configuration table above)
- IAM role with permissions for CloudWatch Logs (and VPC networking if your Databricks workspace is behind PrivateLink)

**Steps:**

1. **Package the code.** The `scripts/build_lambda.sh` script creates a `dist/lambda.zip` with all dependencies bundled:

    ```bash
    # From: project root
    ./scripts/build_lambda.sh
    ```

2. **Create the Lambda function.** The entry point is `catalog_sync.handler.lambda_handler` — a thin wrapper around the same logic as `sync.py`. Set the environment variables (from `.env.sync`) on the Lambda configuration.

3. **Add a schedule.** Create an EventBridge rule with a rate expression (e.g., `rate(15 minutes)`) targeting the Lambda function.

4. **Networking.** The Lambda needs outbound HTTPS access to:
   - `api.confluent.cloud` (Tableflow API — requires NAT gateway if Lambda is in a VPC)
   - Your Databricks workspace URL (may require VPC attachment + PrivateLink if the workspace is private)

A working Terraform example is in `terraform/demo/` — it creates the Lambda, IAM role, and EventBridge schedule using outputs from the `terraform/confluent-cloud/` stack.

### Option B: Azure Functions + Timer Trigger

**What you need:**
- An Azure Function App (Python 3.11+ runtime)
- A timer trigger (cron expression, e.g., `0 */15 * * * *` for every 15 minutes)
- The environment variables set as Application Settings (see Configuration table above)
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

**Full teardown** (including topic deletion — requires bastion for PNI access):

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

> **Note:** Topic deletion on privately-networked clusters requires running from within the private network (e.g., bastion). The bastion runs an NGINX stream proxy that forwards Kafka traffic (port 9092) through the PNI ENIs. `delete-topics.py` uses the Kafka protocol via a Python admin client, connecting through the NGINX proxy on `localhost:9092`.

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
| Kafka producers/consumers → Confluent Cloud | PNI (Private Network Interface) | **Yes** |

### What crosses the public internet

The sync script makes outbound HTTPS calls to:

1. **Confluent Cloud control plane API** (`api.confluent.cloud`) — discovers which topics have Tableflow enabled and their S3 storage paths. Metadata only — topic names and S3 paths.
2. **Stream Catalog GraphQL API** (`{SR_URL}/catalog/graphql`) — fetches classification tags and business metadata. Governance metadata only — tag names and attribute values. (If SR PrivateLink is enabled, use the Stream Catalog URL instead.)

No customer data, Kafka messages, or credentials are transmitted.

### Why this is acceptable

The Confluent Cloud control plane (`api.confluent.cloud`) is a **public-only endpoint** — there is no private networking option for it ([docs](https://docs.confluent.io/cloud/current/networking/private-links/index.html)). Every Confluent Cloud customer, including those with fully private clusters, already relies on Confluent's public control-plane APIs for activities like logging into the console, using the CLI, and managing connectors, schemas, and Tableflow.

This sync script makes the same API call that a human would make by logging into the console and copying a storage path. The only difference is automation.

### Bastion host

The bastion host sits in the **public subnet** (for SSH access) and runs an NGINX stream proxy that forwards Kafka (9092) and HTTPS (443) traffic through the PNI ENIs in the private subnets. PNI does not provide private DNS, so the NGINX proxy uses SNI passthrough to route traffic to the correct Confluent endpoints. Kafka clients on the bastion connect to `localhost:9092`.

### Credential handling

| Credential | How it's used | Storage recommendation |
|-----------|---------------|----------------------|
| Confluent Cloud API key | Authenticate to Tableflow API (HTTPS) | Environment variable or secrets manager |
| Schema Registry API key | Authenticate to Stream Catalog GraphQL API (HTTPS) | Environment variable or secrets manager |
| Databricks token | Authenticate to Unity Catalog (HTTPS) | Environment variable or secrets manager |

All credentials are transmitted over TLS. For automated deployments, store them in AWS Secrets Manager or SSM Parameter Store.

## Design Decisions

- **External tables, not foreign catalogs.** Unity Catalog "foreign catalogs" are for RDBMS/JDBC. Tableflow tables use `CREATE EXTERNAL TABLE ... USING DELTA LOCATION`.
- **Metadata only.** Tables are registered by storage location reference. No data is copied.
- **Safe to run anytime.** The sync checks each topic's Tableflow `status.phase` — only topics in `RUNNING` state are registered. Topics still materializing are skipped. The Databricks external location is read-only, so even a premature registration can't corrupt the Delta log.
- **Runs anywhere.** The sync script has no cloud-specific dependencies. Run it on your laptop, a bastion host, Lambda, Azure Functions, or a Kubernetes pod.
- **BYOB required.** Confluent-Managed Storage (CMS) does not work with private networking. BYOB is the supported path.
- **Delta only.** The engine validates that the table format is Delta before registering. Iceberg tables are skipped — UC does not auto-refresh Iceberg metadata from external storage, so registered Iceberg tables would go stale as Tableflow writes new snapshots.

## Future: Native Sync

When Confluent adds private networking support for the Iceberg REST Catalog or enables native catalog sync over private networking, `sync.py` becomes unnecessary. When Confluent ships native tag sync on Tableflow, `sync_tags.py` also becomes unnecessary. To migrate:

1. Drop the externally registered tables (`DROP TABLE ...` — metadata only, data untouched)
2. Enable Tableflow's built-in catalog integration (with tag sync enabled)
3. Remove this tool
