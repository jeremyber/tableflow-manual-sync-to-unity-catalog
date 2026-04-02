#!/usr/bin/env python3
"""
Tableflow Catalog Sync

Discovers Confluent Cloud Tableflow-enabled topics and registers them
as external tables in Databricks Unity Catalog. Optionally syncs
Confluent Cloud governance tags (classification tags and business
metadata) to Unity Catalog table tags.

Usage:
    python sync.py

    Automatically loads .env.sync from the same directory if present.
    You can also export env vars manually or use: set -a && source .env.sync && set +a

Environment variables:
    CONFLUENT_API_KEY           - Tableflow API key
    CONFLUENT_API_SECRET        - Tableflow API secret
    CONFLUENT_CLUSTER_ID        - Kafka cluster ID (e.g. lkc-xxxxx)
    CONFLUENT_ENVIRONMENT_ID    - Environment ID (e.g. env-xxxxx)
    DATABRICKS_HOST             - Workspace URL
    DATABRICKS_TOKEN            - Personal access token
    DATABRICKS_WAREHOUSE_ID     - SQL warehouse ID
    TARGET_CATALOG              - Unity Catalog catalog name
    TARGET_SCHEMA               - Schema name (default: "default")
    SYNC_TAGS                   - Sync governance tags (default: "true")
    SCHEMA_REGISTRY_URL         - Schema Registry URL (required if SYNC_TAGS=true)
    SCHEMA_REGISTRY_API_KEY     - Schema Registry API key (required if SYNC_TAGS=true)
    SCHEMA_REGISTRY_API_SECRET  - Schema Registry API secret (required if SYNC_TAGS=true)
"""

import os
import re
from pathlib import Path
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format

# ── Load .env.sync if present ────────────────────────────────
# Looks for .env.sync next to this script so you can just run
# `python sync.py` without manually exporting env vars.

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

CONFLUENT_API_KEY    = os.environ["CONFLUENT_API_KEY"]
CONFLUENT_API_SECRET = os.environ["CONFLUENT_API_SECRET"]
CLUSTER_ID           = os.environ["CONFLUENT_CLUSTER_ID"]
ENVIRONMENT_ID       = os.environ["CONFLUENT_ENVIRONMENT_ID"]

DATABRICKS_HOST      = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID         = os.environ.get("DATABRICKS_WAREHOUSE_ID")

CATALOG              = os.environ["TARGET_CATALOG"]
SCHEMA               = os.environ.get("TARGET_SCHEMA", "default")

SYNC_TAGS            = os.environ.get("SYNC_TAGS", "true").lower() == "true"
SR_URL               = os.environ.get("SCHEMA_REGISTRY_URL", "")
SR_API_KEY           = os.environ.get("SCHEMA_REGISTRY_API_KEY", "")
SR_API_SECRET        = os.environ.get("SCHEMA_REGISTRY_API_SECRET", "")

if SYNC_TAGS and not all([SR_URL, SR_API_KEY, SR_API_SECRET]):
    raise ValueError(
        "SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_API_KEY, and "
        "SCHEMA_REGISTRY_API_SECRET are required when SYNC_TAGS=true"
    )

_UC_TAG_KEY_INVALID = re.compile(r"[.,\-=/:\s]+")
MANAGED_TAGS_KEY = "_confluent_managed_tags"
TOMBSTONE_VALUE = "__tombstone__"


def _sanitize_tag_key(key: str) -> str:
    return _UC_TAG_KEY_INVALID.sub("_", key).strip("_")


# ── Step 1: Discover Tableflow topics ──────────────────────
# Calls the Confluent Cloud API to find topics with Tableflow
# enabled and get their storage locations (S3 paths to Delta
# or Iceberg tables).

print(f"Discovering Tableflow topics for cluster {CLUSTER_ID}...")

source_tables = {}  # name -> {location, format}
url = (
    f"https://api.confluent.cloud/tableflow/v1/tableflow-topics"
    f"?spec.kafka_cluster={CLUSTER_ID}"
    f"&environment={ENVIRONMENT_ID}"
)

while url:
    resp = requests.get(url, auth=(CONFLUENT_API_KEY, CONFLUENT_API_SECRET), timeout=30)
    resp.raise_for_status()
    body = resp.json()

    for topic in body.get("data") or []:
        spec = topic.get("spec", {})
        storage = spec.get("storage", {})
        location = storage.get("table_path")
        if not location:
            continue

        name = spec.get("display_name", "")

        # Only register tables that Tableflow has fully materialized.
        # Without this check, CREATE TABLE ... USING DELTA LOCATION on
        # an empty path causes Databricks to write its own _delta_log,
        # which Tableflow then rejects ("Delta table modified externally").
        phase = topic.get("status", {}).get("phase", "")
        if phase != "RUNNING":
            print(f"  Skipping '{name}' — Tableflow phase is '{phase}', not RUNNING")
            continue

        formats = spec.get("table_formats", ["DELTA"])
        fmt = "DELTA" if "DELTA" in formats else formats[0].upper()

        source_tables[name] = {"location": location, "format": fmt}

    url = body.get("metadata", {}).get("next")

print(f"Found {len(source_tables)} Tableflow topics: {', '.join(source_tables.keys())}")


# ── Step 1b: Fetch governance tags ─────────────────────────
# Uses the Stream Catalog GraphQL API to fetch classification tags
# and business metadata for ALL topics in a single paginated query.

source_tags = {}  # name -> {tag_key: tag_value}
_GRAPHQL_PAGE_SIZE = 500
_GRAPHQL_QUERY = """\
query($limit: Int!, $offset: Int!) {
  kafka_topic(limit: $limit, offset: $offset) {
    qualifiedName
    tags
    business_metadata { name value }
  }
}"""

if SYNC_TAGS:
    sr_url = SR_URL.rstrip("/")
    sr_auth = (SR_API_KEY, SR_API_SECRET)

    print(f"\nFetching governance tags via GraphQL...")
    try:
        offset = 0
        while True:
            resp = requests.post(
                f"{sr_url}/catalog/graphql",
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
                if topic_name not in source_tables:
                    continue

                tags: dict[str, str] = {}

                # Classification tags
                for tag_name in topic.get("tags") or []:
                    if tag_name:
                        tags[_sanitize_tag_key(tag_name)] = "true"

                # Business metadata
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

    for topic_name in source_tables:
        tags = source_tags.get(topic_name, {})
        if tags:
            tag_summary = ", ".join(f"{k}={v}" for k, v in sorted(tags.items()))
            print(f"  {topic_name}: {tag_summary}")
        else:
            print(f"  {topic_name}: (no tags)")


# ── Step 2: Connect to Databricks ─────────────────────────

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


# ── Step 3: Ensure catalog and schema exist ────────────────

print(f"\nEnsuring catalog '{CATALOG}' and schema '{SCHEMA}' exist...")
run_sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
run_sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")


# ── Step 4: List existing tables in Unity Catalog ──────────
# We store the S3 location in the table COMMENT so we can
# detect changes on subsequent runs.

print(f"\nListing existing tables in {CATALOG}...")
result = run_sql(
    f"SELECT table_schema, table_name, comment "
    f"FROM {CATALOG}.information_schema.tables "
    f"WHERE table_type = 'EXTERNAL'"
)

target_tables = {}  # name -> location
if result.result and result.result.data_array:
    for row in result.result.data_array:
        target_tables[row[1]] = row[2] or ""

print(f"Found {len(target_tables)} existing tables: {', '.join(target_tables.keys()) or '(none)'}")


# ── Step 5: Diff and sync ─────────────────────────────────

source_names = set(source_tables.keys())
target_names = set(target_tables.keys())

to_add    = source_names - target_names
to_remove = target_names - source_names
to_check  = source_names & target_names

# Tables whose location changed
to_update = {
    name for name in to_check
    if source_tables[name]["location"] != target_tables[name]
}

print(f"\nSync plan: {len(to_add)} to add, {len(to_update)} to update, {len(to_remove)} to remove")

for name in sorted(to_add):
    t = source_tables[name]
    print(f"\n+ Adding: {name}")
    run_sql(
        f"CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{name}` "
        f"USING {t['format']} "
        f"LOCATION '{t['location']}' "
        f"COMMENT '{t['location']}'"
    )

for name in sorted(to_update):
    t = source_tables[name]
    print(f"\n~ Updating: {name}")
    run_sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.`{name}`")
    run_sql(
        f"CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{name}` "
        f"USING {t['format']} "
        f"LOCATION '{t['location']}' "
        f"COMMENT '{t['location']}'"
    )

for name in sorted(to_remove):
    print(f"\n- Removing: {name}")
    run_sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.`{name}`")

print(f"\nDone: {len(to_add)} added, {len(to_update)} updated, {len(to_remove)} removed")


# ── Step 6: Sync governance tags ───────────────────────────
# For each table that exists in UC (newly added or existing),
# diff Confluent tags against current UC tags and apply changes.
# Uses a manifest tag (_confluent_managed_tags) to track which
# tags we manage, so we never clobber UC-native tags.

if SYNC_TAGS:
    tags_changed = 0
    # Sync tags for all tables that now exist in UC
    all_synced_tables = (to_add | to_check) - to_remove

    print(f"\nSyncing governance tags for {len(all_synced_tables)} table(s)...")

    for name in sorted(all_synced_tables):
        topic_tags = source_tags.get(name, {})
        ftn = f"`{CATALOG}`.`{SCHEMA}`.`{name}`"

        # Read current tags on this UC table
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
            pass  # Table may not have tags yet

        # Parse manifest of previously managed keys
        managed_csv = current_uc_tags.get(MANAGED_TAGS_KEY, "")
        previously_managed = {k for k in managed_csv.split(",") if k} if managed_csv else set()

        tags_to_set: dict[str, str] = {}
        table_changes = 0

        # Add or update tags from source
        for key, value in topic_tags.items():
            if current_uc_tags.get(key) != value:
                tags_to_set[key] = value
                table_changes += 1

        # Tombstone tags we previously managed but are no longer in source
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
                if k  # skip empty keys
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

    print(f"\nTags: {tags_changed} change(s) applied")
