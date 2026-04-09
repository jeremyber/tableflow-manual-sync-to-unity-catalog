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

import base64
import json
import logging
import os
import re
from pathlib import Path
import requests
import databricks.sdk.errors
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
                os.environ[_key] = _val

# ── Config ──────────────────────────────────────────────────

_REQUIRED_VARS = [
    "CONFLUENT_CLUSTER_ID",
    "SCHEMA_REGISTRY_URL", "SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET",
    "DATABRICKS_HOST", "DATABRICKS_WAREHOUSE_ID", "TARGET_CATALOG",
]
_missing = [v for v in _REQUIRED_VARS if not os.environ.get(v)]
if _missing:
    print(f"Error: missing required environment variables: {', '.join(_missing)}")
    print(f"Set them in .env.sync or export them directly.")
    print(f"See README.md for the full list of required variables.")
    raise SystemExit(1)

CLUSTER_ID           = os.environ["CONFLUENT_CLUSTER_ID"]
SR_URL               = os.environ["SCHEMA_REGISTRY_URL"].rstrip("/")
SR_API_KEY           = os.environ["SCHEMA_REGISTRY_API_KEY"]
SR_API_SECRET        = os.environ["SCHEMA_REGISTRY_API_SECRET"]

DATABRICKS_HOST      = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN     = os.environ.get("DATABRICKS_TOKEN")
DATABRICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET")
WAREHOUSE_ID         = os.environ["DATABRICKS_WAREHOUSE_ID"]

CATALOG              = os.environ["TARGET_CATALOG"]
SCHEMA               = os.environ.get("TARGET_SCHEMA", "default")

_UC_TAG_KEY_INVALID = re.compile(r"[.,\-=/:\s';\(\)`]+")
_UC_TAG_KEY_VALID = re.compile(r"^[a-zA-Z0-9_]+$")
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_\-]+$")
_GRAPHQL_PAGE_SIZE = 500
_MANIFEST_PATH = "/Shared/.confluent_tag_manifest.json"

logger = logging.getLogger(__name__)


def _validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate a SQL identifier to prevent injection."""
    if not value or not _SAFE_IDENTIFIER.match(value):
        raise ValueError(
            f"Unsafe {label}: {value!r} — only alphanumeric, "
            f"underscore, and hyphen are allowed"
        )
    return value


def _sanitize_tag_key(key: str) -> str:
    sanitized = _UC_TAG_KEY_INVALID.sub("_", key).strip("_")
    if sanitized and not _UC_TAG_KEY_VALID.match(sanitized):
        return ""
    return sanitized


# Validate CATALOG and SCHEMA identifiers
_validate_identifier(CATALOG, "TARGET_CATALOG")
_validate_identifier(SCHEMA, "TARGET_SCHEMA")


# ── Step 1: List existing tables in Unity Catalog ────────────

if DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET:
    ws = WorkspaceClient(
        host=DATABRICKS_HOST,
        client_id=DATABRICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
    )
elif DATABRICKS_TOKEN:
    ws = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
else:
    print("Error: set either DATABRICKS_TOKEN or both DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET")
    raise SystemExit(1)


def _read_manifest() -> dict[str, list[str]]:
    """Read the tag manifest from Databricks workspace."""
    try:
        resp = ws.workspace.download(_MANIFEST_PATH)
        return json.loads(resp.read())
    except (databricks.sdk.errors.NotFound, json.JSONDecodeError):
        return {}
    except Exception:
        logger.warning("Unexpected error reading manifest", exc_info=True)
        return {}


def _write_manifest(manifest: dict[str, list[str]]) -> None:
    """Write the tag manifest to Databricks workspace."""
    from databricks.sdk.service.workspace import ImportFormat
    data = json.dumps(manifest, indent=2, sort_keys=True)
    b64 = base64.b64encode(data.encode()).decode()
    try:
        ws.workspace.import_(
            path=_MANIFEST_PATH,
            content=b64,
            format=ImportFormat.AUTO,
            overwrite=True,
        )
    except databricks.sdk.errors.NotFound:
        # Fallback: try mkdirs + import
        try:
            ws.workspace.mkdirs("/Shared")
        except databricks.sdk.errors.ResourceAlreadyExists:
            pass
        except Exception:
            logger.warning("Failed to create /Shared directory", exc_info=True)
        ws.workspace.import_(
            path=_MANIFEST_PATH,
            content=b64,
            format=ImportFormat.AUTO,
            overwrite=True,
        )


def run_sql(sql):
    """Execute a SQL statement on the Databricks warehouse."""
    print(f"  SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}")
    try:
        result = ws.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s",
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
    except databricks.sdk.errors.NotFound:
        print(f"Error: SQL warehouse '{WAREHOUSE_ID}' not found — check DATABRICKS_WAREHOUSE_ID")
        raise SystemExit(1)
    except databricks.sdk.errors.Unauthorized:
        print("Error: Databricks authentication failed — check DATABRICKS_TOKEN or CLIENT_ID/SECRET")
        raise SystemExit(1)
    except databricks.sdk.errors.Forbidden:
        print("Error: Databricks permission denied — ensure the service principal has CAN USE on the SQL warehouse")
        raise SystemExit(1)
    if result.status and result.status.state:
        state = result.status.state.value
        if state == "FAILED":
            error = result.status.error.message if result.status.error else "Unknown"
            if "PERMISSION_DENIED" in error and "APPLY TAG" in error.upper():
                print(f"Error: missing APPLY TAG permission — run: GRANT APPLY TAG ON CATALOG `{CATALOG}` TO <principal>")
                raise SystemExit(1)
            raise RuntimeError(f"SQL failed: {error}")
        if state != "SUCCEEDED":
            raise RuntimeError(f"SQL did not complete (state={state})")
    return result


print(f"Listing tables in {CATALOG}.{SCHEMA}...")
try:
    result = run_sql(
        f"SELECT table_name "
        f"FROM `{CATALOG}`.information_schema.tables "
        f"WHERE table_schema = '{SCHEMA.replace(chr(39), chr(39)+chr(39))}' "
        f"AND table_type = 'EXTERNAL'"
    )
except RuntimeError as e:
    if "USE_CATALOG" in str(e) or "USE_SCHEMA" in str(e):
        print(f"Error: missing catalog/schema permissions — run:")
        print(f"  GRANT USE CATALOG ON CATALOG `{CATALOG}` TO <principal>;")
        print(f"  GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{SCHEMA}` TO <principal>;")
        raise SystemExit(1)
    raise

table_names: list[str] = []
if result.result and result.result.data_array:
    for row in result.result.data_array:
        name = row[0]
        try:
            _validate_identifier(name, "table name")
            table_names.append(name)
        except ValueError as e:
            print(f"  Skipping table with unsafe name: {e}")

print(f"Found {len(table_names)} table(s): {', '.join(table_names) or '(none)'}")

if not table_names:
    print("No tables to sync tags for.")
    raise SystemExit(0)


# ── Step 2: Fetch governance tags via GraphQL ────────────────

print(f"\nFetching governance tags via GraphQL...")
sr_auth = (SR_API_KEY, SR_API_SECRET)
source_tags: dict[str, dict[str, str]] = {}
_tag_fetch_failed = False

try:
    offset = 0
    while True:
        query = (
            "{ kafka_topic(limit: %d, offset: %d) "
            "{ qualifiedName tags business_metadata { name value } } }"
            % (_GRAPHQL_PAGE_SIZE, offset)
        )
        try:
            resp = requests.post(
                f"{SR_URL}/catalog/graphql",
                auth=sr_auth,
                json={"query": query},
                timeout=30,
            )
        except requests.ConnectionError:
            print(f"  Error: could not reach {SR_URL} — check SCHEMA_REGISTRY_URL and network connectivity")
            _tag_fetch_failed = True
            break
        except requests.Timeout:
            print(f"  Error: request to {SR_URL} timed out")
            _tag_fetch_failed = True
            break
        if resp.status_code == 401:
            print("  Error: Schema Registry authentication failed — check SCHEMA_REGISTRY_API_KEY/SECRET")
            _tag_fetch_failed = True
            break
        if resp.status_code == 403:
            print("  Error: Schema Registry access denied — check API key permissions")
            _tag_fetch_failed = True
            break
        resp.raise_for_status()
        body = resp.json()

        if body.get("errors"):
            print(f"  Warning: GraphQL error: {body['errors'][0].get('message', '')}")
            _tag_fetch_failed = True
            break

        topics = (body.get("data") or {}).get("kafka_topic") or []
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
                    key = _sanitize_tag_key(tag_name)
                    if key:
                        tags[key] = "true"

            for bm in topic.get("business_metadata") or []:
                bm_name = bm.get("name", "")
                bm_value = bm.get("value")
                if bm_name and bm_value is not None:
                    key = _sanitize_tag_key(bm_name)
                    if key:
                        tags[key] = str(bm_value)

            if tags:
                source_tags[topic_name] = tags

        if len(topics) < _GRAPHQL_PAGE_SIZE:
            break
        offset += _GRAPHQL_PAGE_SIZE

except requests.RequestException as e:
    print(f"  Error: failed to fetch tags — {e}")
    _tag_fetch_failed = True

for name in table_names:
    t = source_tags.get(name, {})
    if t:
        tag_summary = ", ".join(f"{k}={v}" for k, v in sorted(t.items()))
        print(f"  {name}: {tag_summary}")
    else:
        print(f"  {name}: (no tags)")


# ── Step 3: Sync tags ───────────────────────────────────────

if _tag_fetch_failed:
    print("\nSkipping tag sync — tag fetch failed (see warnings above).")
    raise SystemExit(1)

manifest = _read_manifest()
tags_changed = 0
print(f"\nSyncing governance tags for {len(table_names)} table(s)...")

for name in sorted(table_names):
    topic_tags = source_tags.get(name, {})
    ftn = f"`{CATALOG}`.`{SCHEMA}`.`{name}`"
    previously_managed = set(manifest.get(name, []))

    # Read current UC tags
    current_uc_tags: dict[str, str] = {}
    escaped_schema = SCHEMA.replace("'", "''")
    escaped_name = name.replace("'", "''")
    try:
        tag_result = run_sql(
            f"SELECT tag_name, tag_value "
            f"FROM `{CATALOG}`.information_schema.table_tags "
            f"WHERE schema_name = '{escaped_schema}' AND table_name = '{escaped_name}'"
        )
        if tag_result.result and tag_result.result.data_array:
            for row in tag_result.result.data_array:
                current_uc_tags[row[0]] = row[1] or ""
    except RuntimeError:
        pass

    tags_to_set: dict[str, str] = {}
    tags_to_remove: set[str] = set()
    table_changes = 0

    # Add or update tags from Confluent
    for key, value in topic_tags.items():
        if not key:
            continue
        if current_uc_tags.get(key) != value:
            tags_to_set[key] = value
            table_changes += 1

    # Remove tags that were managed by us but no longer in Confluent
    for key in previously_managed - set(topic_tags.keys()):
        if key in current_uc_tags:
            tags_to_remove.add(key)
            table_changes += 1

    # Apply tag additions/updates
    set_ok = True
    if tags_to_set:
        tag_pairs = ", ".join(
            f"'{k.replace(chr(39), chr(39)+chr(39))}' = "
            f"'{v.replace(chr(39), chr(39)+chr(39))}'"
            for k, v in sorted(tags_to_set.items())
        )
        try:
            run_sql(f"ALTER TABLE {ftn} SET TAGS ({tag_pairs})")
        except RuntimeError as e:
            set_ok = False
            err = str(e)
            if "PERMISSION_DENIED" in err:
                print(f"  {name}: missing APPLY TAG permission — run: GRANT APPLY TAG ON CATALOG `{CATALOG}` TO <principal>")
            else:
                print(f"  {name}: failed to set tags: {e}")

    # Remove stale tags
    unset_ok = True
    if tags_to_remove:
        key_list = ", ".join(
            f"'{k.replace(chr(39), chr(39)+chr(39))}'"
            for k in sorted(tags_to_remove)
        )
        try:
            run_sql(f"ALTER TABLE {ftn} UNSET TAGS ({key_list})")
        except RuntimeError as e:
            unset_ok = False
            err = str(e)
            if "PERMISSION_DENIED" in err:
                print(f"  {name}: missing APPLY TAG permission — run: GRANT APPLY TAG ON CATALOG `{CATALOG}` TO <principal>")
            else:
                print(f"  {name}: failed to remove tags: {e}")

    # Only update manifest if tag operations succeeded
    new_managed = sorted(k for k in topic_tags.keys() if k)
    manifest_changed = sorted(previously_managed) != new_managed if previously_managed else bool(new_managed)
    if set_ok and unset_ok:
        manifest[name] = new_managed

    if table_changes:
        added = len(tags_to_set)
        removed = len(tags_to_remove)
        parts = []
        if added:
            parts.append(f"{added} added/updated")
        if removed:
            parts.append(f"{removed} removed")
        print(f"  {name}: {', '.join(parts)}")
    else:
        print(f"  {name}: tags up to date")

    tags_changed += table_changes

    # Write manifest only when managed keys changed
    if manifest_changed:
        _write_manifest(manifest)

print(f"\nDone: {tags_changed} tag change(s) applied across {len(table_names)} table(s)")
