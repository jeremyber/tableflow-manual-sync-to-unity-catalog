# Best Practices

## Choosing the Right Mode

1. **Want to sync both tables and tags via this tool?**
   Use `sync.py` — discovers topics, registers tables, and syncs governance tags automatically in one pass. This is the default and recommended mode.

2. **Only need table sync, no tags?**
   Use `sync.py` with `SYNC_TAGS=false` — no Schema Registry credentials needed.

3. **Already using Confluent's native Tableflow catalog sync with UC integrated?**
   Use `sync_tags.py` — syncs governance tags to tables already in Unity Catalog. No Tableflow API key needed.

**Do not** run `sync.py` with `SYNC_TAGS=true` and `sync_tags.py` against the same tables. Both manage tags with separate manifests and will conflict. Pick one approach for tags.

---

## `sync.py` — Table + Tag Sync

### When to use
- Private networking (PNI/PrivateLink) where Confluent's native catalog sync can't reach your UC
- You want a single tool to manage both table registration and tag sync

### Prerequisites
- Tableflow API key (scoped to `tableflow/v1`)
- Schema Registry API key (if `SYNC_TAGS=true`)
- Databricks workspace with UC, SQL warehouse, and a service principal or PAT
- Storage credential + external location in UC pointing to the BYOB bucket

### Running
```bash
# Tables + tags (default)
python3 sync.py

# Tables only
SYNC_TAGS=false python3 sync.py
```

### How it works
1. Discovers Tableflow-enabled topics via Confluent Cloud API
2. Registers them as external tables in UC via `CREATE TABLE ... USING DELTA LOCATION`
3. Fetches tags and business metadata via Stream Catalog GraphQL API
4. Applies tags to UC tables via `ALTER TABLE SET TAGS`
5. Removes stale tags via `ALTER TABLE UNSET TAGS`

### Manifest
Stored in **table properties** (`_confluent_managed_tags` via `SET TBLPROPERTIES`). Per-table, self-contained. Requires write access to the external location — if the location is read-only, use `sync_tags.py` for tags instead.

### Scheduling
```bash
*/15 * * * * cd /path/to/repo && python3 sync.py >> /var/log/sync.log 2>&1
```

For Lambda: the entry point is `catalog_sync.handler.lambda_handler`. See `terraform/demo/` for a working example.

---

## `sync_tags.py` — Tags Only

### When to use
- Confluent's native catalog sync already handles table creation in UC
- You only need governance metadata (classification tags + business metadata) on existing UC tables
- Your external location is read-only (can't use `sync.py` for tags)

### Prerequisites
- Schema Registry API key
- Databricks workspace with UC, SQL warehouse, and a service principal or PAT
- Tables already exist in UC (created by native catalog sync or `sync.py` with `SYNC_TAGS=false`)
- No Tableflow API key or CC Environment ID needed

### Running
```bash
python3 sync_tags.py
```

### How it works
1. Lists existing external tables in UC via `information_schema.tables`
2. Fetches tags and business metadata via Stream Catalog GraphQL API
3. Compares with current UC tags — adds new, updates changed
4. Removes stale tags via `ALTER TABLE UNSET TAGS`
5. Preserves UC-native tags (added directly in Databricks)

### Manifest
Stored in **Databricks workspace file** (`/Shared/.confluent_tag_manifest.json`). Shared across machines — any compute with the service principal can read/write it. No external location write access needed.

### Scheduling
```bash
*/15 * * * * cd /path/to/repo && python3 sync_tags.py >> /var/log/tag-sync.log 2>&1
```

---

## Tag Mapping Reference

| Confluent Source | UC Tag Key | UC Tag Value |
|---|---|---|
| Classification tag `PII` | `PII` | `true` |
| BM `DataOwnership` attr `owner=payments-team` | `DataOwnership_owner` | `payments-team` |
| BM `DataOwnership` attr `priority=1` (int) | `DataOwnership_priority` | `1` (stringified) |

Characters `. , - = / : ' ; ( )` in keys are replaced with `_`. Tags added directly in Databricks are never modified or removed.

## Sync Interval

| Scenario | Recommended interval |
|---|---|
| Tags rarely change (set once) | 1 hour |
| Active governance team | 15 minutes |
| Compliance-critical (minimize drift) | 5 minutes |
| Demo / testing | On-demand |

Both scripts are idempotent. No changes means no SQL writes — safe to run frequently.

## Multi-Cluster Setup

Create one `.env` file per Kafka cluster:

```
configs/
  lkc-prod.env        # CONFLUENT_CLUSTER_ID=lkc-prod, TARGET_SCHEMA=lkc-prod
  lkc-staging.env      # CONFLUENT_CLUSTER_ID=lkc-staging, TARGET_SCHEMA=lkc-staging
```

SR credentials can be the same across files if the clusters share a Schema Registry.

```bash
*/15 * * * * env $(cat configs/lkc-prod.env | xargs) python3 sync_tags.py
*/15 * * * * env $(cat configs/lkc-staging.env | xargs) python3 sync_tags.py
```

For Lambda: one function per cluster with different environment variable sets, each with its own EventBridge schedule.

## Permissions Checklist

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal>`;
GRANT APPLY TAG ON CATALOG <catalog> TO `<service-principal>`;
```

Also grant **CAN USE** on the SQL warehouse via Databricks workspace settings.

For `sync.py` (table sync), additionally: `GRANT CREATE TABLE ON SCHEMA <catalog>.<schema> TO <principal>;`

## Private Networking

If Schema Registry PrivateLink is enabled, set `SCHEMA_REGISTRY_URL` to the **Stream Catalog URL** — the standard SR endpoint will not work for tag operations.

## Troubleshooting

| Error | Fix |
|---|---|
| `missing required environment variables` | Check `.env.sync` is in project root with all required vars |
| `Confluent Cloud authentication failed` | Verify `CONFLUENT_API_KEY` / `SECRET` |
| `Schema Registry authentication failed` | Verify `SCHEMA_REGISTRY_API_KEY` / `SECRET` |
| `SQL warehouse not found` | Check `DATABRICKS_WAREHOUSE_ID` in Connection Details tab |
| `Databricks permission denied` | Grant CAN USE on warehouse via workspace settings |
| `missing APPLY TAG permission` | `GRANT APPLY TAG ON CATALOG <cat> TO <principal>` |
| `could not reach {url}` | Check network connectivity, VPN, or SR PrivateLink config |

## Known Limitations

- **Delta format only** — Iceberg tables are skipped (UC does not auto-refresh Iceberg metadata)
- **Topic-level tags only** — column-level tags from schema annotations are not yet supported
- **Unity Catalog only** — Snowflake Open Catalog, AWS Glue, and BigLake are not supported
- **One catalog + schema per run** — for multiple, run separate invocations
- **No real-time sync** — tag changes propagate on the next scheduled run

## Monitoring

- **Success**: exit code 0, `Done: N tag change(s)`
- **Failure**: exit code 1, `Error:` with actionable message
- For Lambda: alarm on CloudWatch Lambda error metric
- For cron: `>> /var/log/tag-sync.log 2>&1`, alert on `Error:` lines

## What to Avoid

- **Don't mix tag sync tools** — pick either `sync.py` with `SYNC_TAGS=true` or `sync_tags.py`, not both
- **Don't run concurrent instances** against the same cluster — last-writer-wins on manifest
- **Keep each team's tables in separate schemas** — the sync is schema-scoped
