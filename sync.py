#!/usr/bin/env python3
"""
Tableflow Catalog Sync

Discovers Confluent Cloud Tableflow-enabled topics and registers them
as external tables in Databricks Unity Catalog.

Usage:
    python sync.py

    Automatically loads .env.sync from the same directory if present.
    You can also export env vars manually or use: set -a && source .env.sync && set +a

Environment variables:
    CONFLUENT_API_KEY        - Tableflow API key
    CONFLUENT_API_SECRET     - Tableflow API secret
    CONFLUENT_CLUSTER_ID     - Kafka cluster ID (e.g. lkc-xxxxx)
    CONFLUENT_ENVIRONMENT_ID - Environment ID (e.g. env-xxxxx)
    DATABRICKS_HOST          - Workspace URL
    DATABRICKS_TOKEN         - Personal access token
    DATABRICKS_WAREHOUSE_ID  - SQL warehouse ID
    TARGET_CATALOG           - Unity Catalog catalog name
    TARGET_SCHEMA            - Schema name (default: "default")
"""

import os
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
