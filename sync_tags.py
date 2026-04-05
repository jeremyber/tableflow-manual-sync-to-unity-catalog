#!/usr/bin/env python3
"""
Tableflow Tag Sync

Syncs Confluent Cloud governance tags (classification tags and business
metadata) to existing Unity Catalog tables. Designed to run alongside
Confluent Cloud's native catalog sync — CC syncs the tables, this script
syncs the metadata.

Usage:
    python sync_tags.py

    Automatically loads .env.sync from the same directory if present.

Environment variables:
    CONFLUENT_CLUSTER_ID        - Kafka cluster ID (e.g. lkc-xxxxx)
    SCHEMA_REGISTRY_URL         - Schema Registry URL
    SCHEMA_REGISTRY_API_KEY     - Schema Registry API key
    SCHEMA_REGISTRY_API_SECRET  - Schema Registry API secret
    DATABRICKS_HOST             - Workspace URL
    DATABRICKS_TOKEN            - Personal access token
    DATABRICKS_WAREHOUSE_ID     - SQL warehouse ID
    TARGET_CATALOG              - Unity Catalog catalog name
    TARGET_SCHEMA               - Schema name (default: "default")
"""

import os
import re
from pathlib import Path
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format

# ── Load .env.sync if present ────────────────────────────────

_env_file = Path(__file__).resolve().parent / ".env.sync"
if _env_file.is_file():
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#"):
                continue
            if "=" in _line:
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip().strip('"').strip("'")
                os.environ.setdefault(_key, _val)

# ── Config ──────────────────────────────────────────────────

CLUSTER_ID           = os.environ["CONFLUENT_CLUSTER_ID"]
SR_URL               = os.environ["SCHEMA_REGISTRY_URL"].rstrip("/")
SR_API_KEY           = os.environ["SCHEMA_REGISTRY_API_KEY"]
SR_API_SECRET        = os.environ["SCHEMA_REGISTRY_API_SECRET"]

DATABRICKS_HOST      = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID         = os.environ.get("DATABRICKS_WAREHOUSE_ID")

CATALOG              = os.environ["TARGET_CATALOG"]
SCHEMA               = os.environ.get("TARGET_SCHEMA", "default")

_UC_TAG_KEY_INVALID = re.compile(r"[.,\-=/:\s]+")
MANAGED_TAGS_KEY = "_confluent_managed_tags"
TOMBSTONE_VALUE = "__tombstone__"

_GRAPHQL_PAGE_SIZE = 500
_GRAPHQL_QUERY = """\
query($limit: Int!, $offset: Int!) {
  kafka_topic(limit: $limit, offset: $offset) {
    qualifiedName
    tags
    business_metadata { name value }
  }
}"""


def _sanitize_tag_key(key: str) -> str:
    return _UC_TAG_KEY_INVALID.sub("_", key).strip("_")


# ── Step 1: List existing tables in Unity Catalog ────────────

ws = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)


def run_sql(sql):
    """Execute a SQL statement on the Databricks warehouse."""
    print(f"  SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}")
    result = ws.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    if result.status and result.status.state and result.status.state.value == "FAILED":
        error = result.status.error.message if result.status.error else "Unknown"
        raise RuntimeError(f"SQL failed: {error}")
    return result


print(f"Listing tables in {CATALOG}.{SCHEMA}...")
result = run_sql(
    f"SELECT table_name "
    f"FROM {CATALOG}.information_schema.tables "
    f"WHERE table_schema = '{SCHEMA.replace(chr(39), chr(39)+chr(39))}' "
    f"AND table_type = 'EXTERNAL'"
)

table_names: list[str] = []
if result.result and result.result.data_array:
    for row in result.result.data_array:
        table_names.append(row[0])

print(f"Found {len(table_names)} table(s): {', '.join(table_names) or '(none)'}")

if not table_names:
    print("No tables to sync tags for.")
    raise SystemExit(0)


# ── Step 2: Fetch governance tags via GraphQL ────────────────

print(f"\nFetching governance tags via GraphQL...")
sr_auth = (SR_API_KEY, SR_API_SECRET)
source_tags: dict[str, dict[str, str]] = {}

try:
    offset = 0
    while True:
        resp = requests.post(
            f"{SR_URL}/catalog/graphql",
            auth=sr_auth,
            json={
                "query": _GRAPHQL_QUERY,
                "variables": {"limit": _GRAPHQL_PAGE_SIZE, "offset": offset},
            },
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()

        if body.get("errors"):
            print(f"  Warning: GraphQL error: {body['errors'][0].get('message', '')}")
            break

        topics = body.get("data", {}).get("kafka_topic") or []
        for topic in topics:
            qualified_name = topic.get("qualifiedName", "")
            parts = qualified_name.split(":")
            if len(parts) < 3:
                continue
            entity_cluster_id = parts[1]
            topic_name = ":".join(parts[2:])

            if entity_cluster_id != CLUSTER_ID:
                continue
            if topic_name not in table_names:
                continue

            tags: dict[str, str] = {}

            for tag_name in topic.get("tags") or []:
                if tag_name:
                    tags[_sanitize_tag_key(tag_name)] = "true"

            for bm in topic.get("business_metadata") or []:
                bm_name = bm.get("name", "")
                bm_value = bm.get("value")
                if bm_name and bm_value is not None:
                    tags[_sanitize_tag_key(bm_name)] = str(bm_value)

            if tags:
                source_tags[topic_name] = tags

        if len(topics) < _GRAPHQL_PAGE_SIZE:
            break
        offset += _GRAPHQL_PAGE_SIZE

except requests.RequestException as e:
    print(f"  Warning: failed to fetch tags via GraphQL: {e}")

for name in table_names:
    t = source_tags.get(name, {})
    if t:
        tag_summary = ", ".join(f"{k}={v}" for k, v in sorted(t.items()))
        print(f"  {name}: {tag_summary}")
    else:
        print(f"  {name}: (no tags)")


# ── Step 3: Sync tags ───────────────────────────────────────

tags_changed = 0
print(f"\nSyncing governance tags for {len(table_names)} table(s)...")

for name in sorted(table_names):
    topic_tags = source_tags.get(name, {})
    ftn = f"`{CATALOG}`.`{SCHEMA}`.`{name}`"

    # Read current tags
    current_uc_tags: dict[str, str] = {}
    escaped_schema = SCHEMA.replace("'", "''")
    escaped_name = name.replace("'", "''")
    try:
        tag_result = run_sql(
            f"SELECT tag_name, tag_value "
            f"FROM {CATALOG}.information_schema.table_tags "
            f"WHERE schema_name = '{escaped_schema}' AND table_name = '{escaped_name}'"
        )
        if tag_result.result and tag_result.result.data_array:
            for row in tag_result.result.data_array:
                current_uc_tags[row[0]] = row[1] or ""
    except RuntimeError:
        pass

    # Parse manifest
    managed_csv = current_uc_tags.get(MANAGED_TAGS_KEY, "")
    previously_managed = {k for k in managed_csv.split(",") if k} if managed_csv else set()

    tags_to_set: dict[str, str] = {}
    table_changes = 0

    # Add or update
    for key, value in topic_tags.items():
        if current_uc_tags.get(key) != value:
            tags_to_set[key] = value
            table_changes += 1

    # Tombstone removed
    for key in previously_managed - set(topic_tags.keys()):
        if key in current_uc_tags and current_uc_tags[key] != TOMBSTONE_VALUE:
            tags_to_set[key] = TOMBSTONE_VALUE
            table_changes += 1

    # Update manifest
    new_managed = set(topic_tags.keys())
    if new_managed != previously_managed or MANAGED_TAGS_KEY not in current_uc_tags:
        tags_to_set[MANAGED_TAGS_KEY] = ",".join(sorted(new_managed))

    if tags_to_set:
        tag_pairs = ", ".join(
            f"'{k.replace(chr(39), chr(39)+chr(39))}' = "
            f"'{v.replace(chr(39), chr(39)+chr(39))}'"
            for k, v in sorted(tags_to_set.items())
            if k
        )
        if tag_pairs:
            try:
                run_sql(f"ALTER TABLE {ftn} SET TAGS ({tag_pairs})")
                if table_changes:
                    print(f"  {name}: {table_changes} tag(s) synced")
            except RuntimeError as e:
                print(f"  {name}: failed to sync tags: {e}")
    else:
        print(f"  {name}: tags up to date")

    tags_changed += table_changes

print(f"\nDone: {tags_changed} tag change(s) applied across {len(table_names)} table(s)")
