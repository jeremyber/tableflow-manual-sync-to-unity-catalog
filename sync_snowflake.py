#!/usr/bin/env python3
"""
Tableflow Catalog Sync for Snowflake (Iceberg)

Discovers Confluent Cloud Tableflow-enabled topics and registers them as
Iceberg tables in Snowflake, then keeps them on the latest snapshot.

Snowflake cannot federate to the Tableflow Iceberg REST Catalog over private
networking, so tables are created through an object-store catalog integration
and pinned to a metadata file. Snowflake does not auto-refresh those tables,
so every run moves each table to the newest metadata.json in storage. Run it
on a schedule (every minute is fine) from compute that can reach the bucket or
container privately.

Every run:
  1. Lists RUNNING Iceberg topics via the Tableflow API (metadata only)
  2. Finds each table's newest metadata/*.metadata.json in S3 or ADLS
  3. Creates missing tables, refreshes tables whose snapshot moved, and drops
     tables it created whose topic no longer has Tableflow

Usage:
    python sync_snowflake.py

    Automatically loads .env.snowflake from the same directory if present.
    pip install -e ".[snowflake,azure]"   (or ".[snowflake,aws]" for S3)

Environment variables:
    CONFLUENT_API_KEY              - Tableflow API key
    CONFLUENT_API_SECRET           - Tableflow API secret
    CONFLUENT_CLUSTER_ID           - Kafka cluster ID (e.g. lkc-xxxxx)
    CONFLUENT_ENVIRONMENT_ID       - Environment ID (e.g. env-xxxxx)
    SNOWFLAKE_ACCOUNT              - <org>-<account> identifier
    SNOWFLAKE_USER                 - Service user
    SNOWFLAKE_PRIVATE_KEY_PATH     - PKCS8 key for key-pair auth (or SNOWFLAKE_PASSWORD)
    SNOWFLAKE_ROLE                 - Role with CREATE ICEBERG TABLE on the schema
    SNOWFLAKE_WAREHOUSE            - Warehouse
    SNOWFLAKE_DATABASE             - Database
    TARGET_SCHEMA                  - Schema (default: "PUBLIC")
    SNOWFLAKE_EXTERNAL_VOLUME      - External volume on the Tableflow bucket/container
    SNOWFLAKE_CATALOG_INTEGRATION  - CATALOG_SOURCE = OBJECT_STORE, TABLE_FORMAT = ICEBERG
    SNOWFLAKE_STORAGE_BASE_PATH    - Path the volume's STORAGE_BASE_URL points at inside
                                     the bucket/container (default: "" = root)
"""

import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import requests

# ── Load .env.snowflake if present ───────────────────────────

_env_file = Path(__file__).resolve().parent / ".env.snowflake"
if _env_file.is_file():
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#"):
                continue
            if "=" in _line:
                _key, _, _val = _line.partition("=")
                os.environ[_key.strip()] = _val.strip().strip('"').strip("'")

# ── Config ──────────────────────────────────────────────────

_REQUIRED_VARS = [
    "CONFLUENT_API_KEY", "CONFLUENT_API_SECRET",
    "CONFLUENT_CLUSTER_ID", "CONFLUENT_ENVIRONMENT_ID",
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_EXTERNAL_VOLUME", "SNOWFLAKE_CATALOG_INTEGRATION",
]
_missing = [v for v in _REQUIRED_VARS if not os.environ.get(v)]
if _missing:
    print(f"Error: missing required environment variables: {', '.join(_missing)}")
    print("Set them in .env.snowflake or export them directly.")
    raise SystemExit(1)

_SNOWFLAKE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _validate_identifier(value: str, label: str) -> str:
    if not value or not _SNOWFLAKE_IDENTIFIER.match(value):
        raise ValueError(f"Unsafe {label}: {value!r}: expected an unquoted Snowflake identifier")
    return value


DATABASE    = _validate_identifier(os.environ["SNOWFLAKE_DATABASE"], "SNOWFLAKE_DATABASE")
SCHEMA      = _validate_identifier(os.environ.get("TARGET_SCHEMA", "PUBLIC"), "TARGET_SCHEMA")
VOLUME      = _validate_identifier(os.environ["SNOWFLAKE_EXTERNAL_VOLUME"], "SNOWFLAKE_EXTERNAL_VOLUME")
INTEGRATION = _validate_identifier(os.environ["SNOWFLAKE_CATALOG_INTEGRATION"], "SNOWFLAKE_CATALOG_INTEGRATION")
BASE_PATH   = os.environ.get("SNOWFLAKE_STORAGE_BASE_PATH", "").strip("/")

# Tables this script created carry the topic and table_path in their COMMENT
COMMENT_PATTERN = re.compile(r"^catalog-sync topic=(\S+) location=(\S+)$")

# ── 1. Discover Tableflow Iceberg topics ────────────────────


def tableflow_topics() -> dict[str, str]:
    """Return {topic_name: table_path} for RUNNING Iceberg topics."""
    topics: dict[str, str] = {}
    url = (
        "https://api.confluent.cloud/tableflow/v1/tableflow-topics"
        f"?spec.kafka_cluster={os.environ['CONFLUENT_CLUSTER_ID']}"
        f"&environment={os.environ['CONFLUENT_ENVIRONMENT_ID']}"
    )
    auth = (os.environ["CONFLUENT_API_KEY"], os.environ["CONFLUENT_API_SECRET"])
    while url:
        resp = requests.get(url, auth=auth, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        for t in body.get("data") or []:
            spec = t.get("spec", {})
            name = spec.get("display_name", "")
            path = spec.get("storage", {}).get("table_path")
            phase = t.get("status", {}).get("phase", "")
            if phase != "RUNNING" or not path:
                print(f"  skip {name}: phase={phase}")
                continue
            if "ICEBERG" not in [f.upper() for f in spec.get("table_formats", [])]:
                print(f"  skip {name}: not published as Iceberg")
                continue
            topics[name] = path
        url = body.get("metadata", {}).get("next") or None
    return topics


# ── 2. Find the newest metadata file in storage ─────────────

_VERSION = re.compile(r"^v?(\d+)[-.]")


def _newest(candidates):
    """candidates: [(key, last_modified)] -> key of the highest-version metadata.json."""
    def sort_key(item):
        match = _VERSION.match(item[0].rsplit("/", 1)[-1])
        return (int(match.group(1)) if match else -1, item[1], item[0])

    files = [c for c in candidates if c[0].endswith(".metadata.json")]
    return max(files, key=sort_key)[0] if files else None


_clients: dict = {}


def latest_metadata_file(table_path: str) -> str | None:
    """Object key of the newest metadata/*.metadata.json under a Tableflow table_path."""
    parsed = urlparse(table_path)
    prefix = parsed.path.strip("/") + "/metadata/"

    if parsed.scheme in ("abfss", "abfs"):
        # abfss://<container>@<account>.dfs.core.windows.net/<path>; list via the Blob endpoint
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import ContainerClient

        container, _, host = parsed.netloc.partition("@")
        key = ("adls", host, container)
        if key not in _clients:
            _clients[key] = ContainerClient(
                account_url=f"https://{host.split('.')[0]}.blob.core.windows.net",
                container_name=container,
                credential=DefaultAzureCredential(),
            )
        blobs = _clients[key].list_blobs(name_starts_with=prefix)
        return _newest([(b.name, b.last_modified) for b in blobs])

    if parsed.scheme in ("s3", "s3a"):
        import boto3

        _clients.setdefault("s3", boto3.client("s3"))
        candidates = []
        for page in _clients["s3"].get_paginator("list_objects_v2").paginate(
            Bucket=parsed.netloc, Prefix=prefix,
        ):
            candidates += [(o["Key"], o["LastModified"]) for o in page.get("Contents", [])]
        return _newest(candidates)

    raise ValueError(f"Unsupported table path: {table_path}")


def relative_to_volume(key: str) -> str:
    if BASE_PATH:
        if not key.startswith(BASE_PATH + "/"):
            raise ValueError(f"{key} is outside SNOWFLAKE_STORAGE_BASE_PATH={BASE_PATH}")
        return key[len(BASE_PATH) + 1:]
    return key


# ── 3. Sync into Snowflake ──────────────────────────────────


def snowflake_connection():
    import snowflake.connector

    params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
    }
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        from cryptography.hazmat.primitives import serialization

        with open(key_path, "rb") as f:
            key = serialization.load_pem_private_key(f.read(), password=None)
        params["private_key"] = key.private_bytes(
            serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
        )
    else:
        params["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(**{k: v for k, v in params.items() if v})


def table_name(topic: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_]", "_", topic).upper()
    return f"_{name}" if name[:1].isdigit() else name


def main():
    print("Discovering Tableflow topics...")
    desired: dict[str, tuple[str, str]] = {}  # topic -> (table_path, metadata path)
    for topic, path in tableflow_topics().items():
        key = latest_metadata_file(path)
        if key is None:
            print(f"  skip {topic}: no Iceberg snapshot yet")
            continue
        desired[topic] = (path, relative_to_volume(key))
    print(f"  {len(desired)} Iceberg table(s) ready")

    added = refreshed = recreated = removed = 0
    with snowflake_connection() as conn, conn.cursor() as cur:
        cur.execute(f"SHOW ICEBERG TABLES IN SCHEMA {DATABASE}.{SCHEMA}")
        cols = [c[0].lower() for c in cur.description]
        existing = {}  # topic -> (table name, table_path), only tables this script created
        for row in (dict(zip(cols, r)) for r in cur.fetchall()):
            match = COMMENT_PATTERN.match(row.get("comment") or "")
            if match:
                existing[match.group(1)] = (row["name"], match.group(2))

        for topic, (path, metadata) in sorted(desired.items()):
            fqn = f"{DATABASE}.{SCHEMA}.{table_name(topic)}"
            if topic in existing and existing[topic][1] != path:
                # Tableflow re-enabled on a new path: re-create on it
                cur.execute(f"DROP ICEBERG TABLE IF EXISTS {fqn}")
                del existing[topic]
                recreated += 1
            if topic not in existing:
                cur.execute(
                    f"CREATE ICEBERG TABLE IF NOT EXISTS {fqn} "
                    f"EXTERNAL_VOLUME = '{VOLUME}' CATALOG = '{INTEGRATION}' "
                    f"METADATA_FILE_PATH = %s COMMENT = %s",
                    (metadata, f"catalog-sync topic={topic} location={path}"),
                )
                print(f"+ Adding: {fqn}")
                added += 1
                continue
            cur.execute("SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(%s)", (fqn,))
            current = json.loads(cur.fetchone()[0]).get("metadataLocation", "")
            if not current.endswith(metadata):
                cur.execute(f"ALTER ICEBERG TABLE {fqn} REFRESH %s", (metadata,))
                print(f"~ Refreshing: {fqn} -> {metadata.rsplit('/', 1)[-1]}")
                refreshed += 1

        for topic, (name, _) in sorted(existing.items()):
            if topic not in desired:
                cur.execute(f"DROP ICEBERG TABLE IF EXISTS {DATABASE}.{SCHEMA}.{name}")
                print(f"- Removing: {DATABASE}.{SCHEMA}.{name}")
                removed += 1

    print(f"Done: {added} added, {refreshed} refreshed, {recreated} re-created, {removed} removed")


if __name__ == "__main__":
    main()
