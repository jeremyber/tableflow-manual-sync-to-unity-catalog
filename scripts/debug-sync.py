#!/usr/bin/env python3
"""Debug script — runs the sync with verbose output."""
import logging
import os

logging.basicConfig(level=logging.DEBUG)

from catalog_sync.targets.unity_catalog import UnityCatalogTarget
from catalog_sync.sources.confluent_cloud import ConfluentCloudSource

# Source
source = ConfluentCloudSource(
    api_key=os.environ["CONFLUENT_API_KEY"],
    api_secret=os.environ["CONFLUENT_API_SECRET"],
    cluster_id=os.environ["CONFLUENT_CLUSTER_ID"],
    environment_id=os.environ["CONFLUENT_ENVIRONMENT_ID"],
)

print("=== Source tables ===")
tables = source.list_tables()
for t in tables:
    print(f"  {t.name} | format={t.table_format} | location={t.location}")

if not tables:
    print("  (none found)")
    exit(0)

# Target
target = UnityCatalogTarget(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"],
    catalog_name=os.environ["TARGET_CATALOG"],
    warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
    schema_name=os.environ.get("TARGET_SCHEMA", "default"),
)

print("\n=== Existing target tables ===")
existing = target.list_tables()
for t in existing:
    print(f"  {t.full_name} | location={t.location}")
if not existing:
    print("  (none)")

print("\n=== Registering first table as test ===")
table = tables[0]
print(f"  Table: {table.name}")
print(f"  Format: {table.table_format}")
print(f"  Location: {table.location}")

result = target._execute(
    f"CREATE TABLE IF NOT EXISTS "
    f"`{os.environ['TARGET_CATALOG']}`.`default`.`{table.name}` "
    f"USING {table.table_format} "
    f"LOCATION '{table.location}'"
)
print(f"  Result status: {result.status}")
if result.status and hasattr(result.status, 'error'):
    print(f"  Error: {result.status.error}")
print(f"  Full result: {result}")

print("\n=== Checking if table exists now ===")
existing = target.list_tables()
for t in existing:
    print(f"  {t.full_name} | location={t.location}")
if not existing:
    print("  (none)")
