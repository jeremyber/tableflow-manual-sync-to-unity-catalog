# Best Practices

## Choosing the Right Mode

1. **Want to sync both tables and tags via this tool?**
   Use `sync.py` — discovers topics, registers tables, and syncs governance tags automatically in one pass. This is the default and recommended mode.

2. **Only need table sync, no tags?**
   Use `sync.py` with `SYNC_TAGS=false` — no Schema Registry credentials needed.

3. **Already using Confluent's native Tableflow catalog sync with UC integrated?**
   Use `sync_tags.py` — syncs governance tags to tables already in Unity Catalog. No Tableflow API key needed.

**Do not** run `sync.py` with `SYNC_TAGS=true` and `sync_tags.py` against the same tables. Both manage tags with separate manifests and will conflict. Pick one approach for tags.

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
