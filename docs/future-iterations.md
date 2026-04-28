# Future Iterations

**Date:** 2026-03-12
**Status:** Planning / Discussion

## 1. Iceberg Table Format Support

The codebase already handles Iceberg at the code level:
- `TableInfo.table_format` carries the format (default `"DELTA"`)
- `ConfluentCloudSource` reads `table_formats` from the Tableflow API and sets the format dynamically
- `UnityCatalogTarget.register_table` uses `USING {table.table_format}` — so `USING ICEBERG` works without code changes

**Status:** The code already handles this. Dual-format publishing (`["DELTA", "ICEBERG"]`) was previously thought to fail, but works fine in other demos. The current demo hardcodes `["DELTA"]` in `setup-topics.sh` — this can be switched to `["DELTA", "ICEBERG"]` to enable both formats. The source code already prefers Delta when both are available and falls back to the first format otherwise.

**Action items:**
- Switch `setup-topics.sh` to `["DELTA", "ICEBERG"]` and verify
- Remove the "dual format fails" warnings from CLAUDE.md once confirmed
- Verify `CREATE TABLE ... USING ICEBERG LOCATION` syntax on target Databricks Runtime version

### Iceberg Metadata Refresh Issue

**Problem:** Iceberg tables registered as external tables in Unity Catalog (or Snowflake) go stale when Tableflow writes new data. Delta auto-refreshes from `_delta_log/`, but Iceberg requires explicit `REFRESH TABLE` commands because the catalog materializes a snapshot of metadata.

**Solution:** Add `supports_format()` and `refresh_table()` methods to the Target interface. After creating or updating a table, call `refresh_table()` to sync metadata from the source Iceberg catalog.

**Status:** Design spec written (`docs/superpowers/specs/2026-04-28-iceberg-metadata-refresh-design.md`), not yet implemented.

**Unity Catalog implementation:** Will stay Delta-only initially; refresh method is a no-op. Future catalog targets (Snowflake, Polaris) will implement Iceberg refresh.

**User feedback (2026-04-28):** Overall architecture aligns well with how Tableflow is evolving. Key nuances to address when implementing:
- Target-aware format selection (Polaris prefers Iceberg, Unity prefers Delta, not just "pick Delta if both exist")
- Idempotency and drift handling (tolerate tables being dropped/recreated in target catalog)
- Event-based refresh triggering (integrate with S3/ADLS event notifications from Option E below)
- Schema evolution handling (event filters should catch schema changes, not just initial commits)

## 2. Multi-Lakehouse Target Support

The `CatalogTarget` ABC already defines the interface: `list_tables`, `register_table`, `update_table`, `remove_table`. New targets implement this interface.

### Snowflake
- SQL: `CREATE ICEBERG TABLE ... EXTERNAL_VOLUME = '...' CATALOG = 'SNOWFLAKE'`
- Requires: Snowflake External Volume pointing to the same S3/ADLS bucket (Terraform)
- SDK: Snowflake Connector for Python or REST API
- Format: Iceberg (Snowflake's native lakehouse format)

### Azure Fabric (OneLake)
- API-driven: Create OneLake shortcuts via Fabric REST API
- No SQL — shortcuts point to ADLS Gen2 (or S3 via cross-cloud shortcut) locations
- Format: Delta Lake (Fabric's native format) or Iceberg (via Fabric's Iceberg support)

### Snowflake Open Catalog (Polaris)
- Cleanest target — speaks native Iceberg REST Catalog protocol
- Register via `POST /v1/namespaces/{ns}/tables`
- Format: Iceberg only

### AWS Glue Data Catalog
- `boto3` `create_table()` with Iceberg metadata pointer
- Works purely over VPC endpoints (no private networking gap)
- Format: Iceberg or Delta

### Implementation approach
- Source side (`ConfluentCloudSource`) stays unchanged — returns `TableInfo` objects
- `handler.py` factory (`build_target`) gains a `TARGET_TYPE` enum to dispatch
- `config.py` adds target-specific env vars
- Each target gets its own `targets/<name>.py` file
- Terraform modules per cloud/target combination

## 3. Event-Based Sync (Real-Time Catalog Registration)

Currently syncs on a 15-minute EventBridge schedule. Goal: register tables the moment Tableflow status flips to `RUNNING`.

### Option A: Short-Poll the Tableflow API (simplest, recommended for now)
- Reduce polling interval to 30s–1m
- API call is lightweight (metadata only), already authenticated
- Downside: not truly instant, more Lambda invocations
- For demos, "30 seconds" is effectively instant

### Option B: Confluent Cloud Audit Log Topic
- Audit log topic publishes events for control-plane actions (including Tableflow state changes)
- Problem: audit log is a Kafka topic → requires Kafka protocol access → back to the private networking problem
- Would need bastion/NGINX proxy to consume, defeating the elegance

### Option C: Webhook / Event Notification (ideal, doesn't exist yet)
- Ideal: Confluent publishes webhook on Tableflow status change → API Gateway → Lambda → sync
- Truly event-driven, zero polling
- **Confluent Cloud does not offer webhooks for Tableflow state changes today**
- Feature request worth making — solves this for every customer

### Option D: Poller + SNS/SQS Decoupling
- Lightweight poller (Step Function, cron) hits Tableflow API every 10s
- Detects `RUNNING` transitions, publishes to SNS/SQS
- SQS triggers sync Lambda
- Separates "watch" concern from "sync" concern
- More infrastructure, but clean separation

### Option E: S3 / ADLS Event Notifications (recommended)

The BYOB bucket is already in the customer's account. Use cloud-native storage events to trigger sync the moment Tableflow writes files.

**How it works:**

1. Tableflow materializes a topic → writes to the BYOB bucket (e.g., `_delta_log/00000000000000000000.json` for Delta, `metadata/v1.metadata.json` for Iceberg)
2. S3 Event Notification (or Azure Event Grid for ADLS) fires on that write
3. Lambda/Function receives the event, extracts the S3 path prefix from the object key
4. Lambda calls the Tableflow API → finds the topic whose `spec.storage.table_path` matches the path prefix
5. Registers **just that one table** — no full diff sync needed

```
S3 Event Notification (suffix filter: _delta_log/*.json or metadata/*.metadata.json)
  → SQS (optional batching)
    → Lambda
      → Tableflow API (lookup topic by table_path)
        → Unity Catalog (register single table)
```

**Why this is the best option:**

- Truly event-driven — sync fires within seconds of materialization
- No wasted invocations — Lambda only runs when there's something new
- No dependency on Confluent webhooks — watches the data plane (S3), not the control plane
- Works with private networking — S3 Event Notifications and Event Grid are cloud-internal
- Targeted — registers only the specific topic that triggered the event, not a full sync
- The BYOB bucket is already in Terraform — adding an event notification is ~30 lines of HCL

**Considerations:**

- Filter S3 events to initial table creation files only (e.g., `_delta_log/00000000000000000000.json`) to avoid triggering on every data write
- Or just let it be idempotent — `CREATE TABLE IF NOT EXISTS` is cheap
- Keep a periodic full sync as a safety net (hourly) to catch edge cases like topic removal or path changes
- For Azure: Event Grid blob-created event with subject filter → Azure Function

### Recommendation
**Option E (S3/ADLS event notifications)** is the best path forward — it's event-driven, uses infrastructure the customer already has, and doesn't depend on Confluent shipping a new feature. Keep a periodic full sync (Option A, hourly) as a safety net for deletions and edge cases.

## 4. sync.py — Role and Reusability

`sync.py` is **not a one-way door**. It's a demo artifact — intentionally self-contained (~200 lines) so customers can read it top-to-bottom and understand exactly what's happening.

### Two audiences, two paths

| Artifact | Audience | Purpose |
|----------|----------|---------|
| `sync.py` | Customer demos, proof-of-concept | "Look, it's 200 lines, run from your laptop" |
| `catalog_sync/` | Production Lambda, multi-target | Pluggable sources/targets, factory pattern |

### Guidance
- **Don't add imports from `catalog_sync/` into `sync.py`** — the self-contained nature is the feature
- **Don't add `--target` flags to sync.py** — it should stay dead simple
- For new lakehouses, create **separate self-contained scripts** (e.g., `sync_snowflake.py`) following the same top-to-bottom pattern. Copy-paste is fine for demo scripts.
- Let `catalog_sync/` be where multi-target/multi-format production logic lives

## 5. Azure Deployment (Terraform)

`sync.py` is already cloud-agnostic. Needs:
- `terraform/confluent-cloud-azure/` with VNet, Azure Private Link, ADLS Gen2, bastion VM, Databricks workspace
- BYOB uses ADLS Gen2 instead of S3
- Azure Functions (Premium plan with VNet integration) instead of Lambda
- Timer trigger instead of EventBridge
- Key Vault instead of environment variables
