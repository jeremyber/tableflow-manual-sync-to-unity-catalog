# Best Practices

## Choosing the Right Mode

1. **Using Confluent's native Tableflow catalog sync with UC already integrated?**
   Use `sync_tags.py` — syncs governance tags to tables already in Unity Catalog. No Tableflow API key needed.

2. **Want to sync both tables and tags via this tool?**
   Use `sync.py` with `SYNC_TAGS=true` (the default) — discovers topics, registers tables, and syncs tags in one pass.

3. **Only need table sync, no tags?**
   Use `sync.py` with `SYNC_TAGS=false` — no Schema Registry credentials needed.

**Do not** run `sync.py` with `SYNC_TAGS=true` and `sync_tags.py` against the same tables. Both manage tags with separate manifests and will conflict. Pick one approach for tags.

## Sync Interval

| Scenario | Recommended interval |
|---|---|
| Tags rarely change (set once) | 1 hour |
| Active governance team | 15 minutes |
| Compliance-critical (minimize drift) | 5 minutes |
| Demo / testing | On-demand |

The script is idempotent. No changes means no SQL writes — safe to run frequently.

## Multi-Cluster Setup

Create one `.env` file per cluster:

```
configs/
  cluster-prod.env
  cluster-staging.env
  cluster-dev.env
```

Run each independently using environment variable override (no file copy race):

```bash
# Cron
*/15 * * * * cd /path/to/repo && env $(cat configs/cluster-prod.env | xargs) python3 sync_tags.py
*/15 * * * * cd /path/to/repo && env $(cat configs/cluster-staging.env | xargs) python3 sync_tags.py
```

For Lambda: one function per cluster with different environment variable sets, each with its own EventBridge schedule.

## Multi-Catalog / Multi-Schema

Each run targets one `TARGET_CATALOG` + `TARGET_SCHEMA`. For multiple:

```bash
env TARGET_CATALOG=prod-catalog TARGET_SCHEMA=default ... python3 sync_tags.py
env TARGET_CATALOG=staging-catalog TARGET_SCHEMA=default ... python3 sync_tags.py
```

## Manifest

- `sync_tags.py` stores the manifest in a Databricks workspace file (`/Shared/.confluent_tag_manifest.json`). Shared across machines — any compute with the service principal can read/write it.
- `sync.py` stores the manifest in table properties (`_confluent_managed_tags` via `SET TBLPROPERTIES`). Per-table, but requires write access to the external location.
- If the manifest is lost or corrupted, the next run rebuilds it automatically. Tags won't be removed that run, but the system self-heals on the following run.

## Permissions Checklist

Per Databricks catalog, grant the service principal:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal>`;
GRANT APPLY TAG ON CATALOG <catalog> TO `<service-principal>`;
```

Also grant **CAN USE** on the SQL warehouse via workspace settings.

## Monitoring

- **Success**: exit code 0, output shows `Done: N tag change(s)`
- **Failure**: exit code 1, output shows `Error:` with actionable message
- For Lambda: CloudWatch Logs for errors, alarm on Lambda error metric
- For cron: redirect output to a log file, alert on `Error:` lines

```bash
*/15 * * * * cd /path/to/repo && python3 sync_tags.py >> /var/log/tag-sync.log 2>&1
```

## Private Networking

The GraphQL endpoint is served at `{SCHEMA_REGISTRY_URL}/catalog/graphql`. If Schema Registry PrivateLink is enabled, set `SCHEMA_REGISTRY_URL` to the Stream Catalog URL — the standard SR endpoint will not work for tag operations.

## What to Avoid

- **Don't mix tag sync tools** — pick either `sync.py` with `SYNC_TAGS=true` or `sync_tags.py`, not both
- **Don't run concurrent instances** against the same cluster — last-writer-wins on manifest
- **Don't set `SYNC_TAGS=true`** without SR credentials — will fail at startup with a clear error
- **Keep each team's tables in separate schemas** — the sync is schema-scoped and won't touch tables in other schemas
