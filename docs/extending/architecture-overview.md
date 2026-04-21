# Architecture Overview

This document explains the core architecture of the Tableflow Catalog Sync tool. Read this first to understand how the system works before adding new catalog targets or cloud providers.

## High-Level Concepts

### What is Catalog Sync?

Catalog sync is the process of discovering tables in one system (the **source**) and registering them in another system (the **target**). In this tool:

- **Source**: Confluent Cloud Tableflow API — tells us which Kafka topics have been materialized as Delta Lake tables, and where they live in object storage
- **Target**: Data catalog (currently Unity Catalog) — where we register external table metadata so query engines can discover and read the tables
- **Sync**: The engine that compares source vs target, then adds/updates/removes tables to keep them in sync

### What is BYOB?

**Bring Your Own Bucket (BYOB)** means Tableflow writes table data to storage you own and control (your S3 bucket, ADLS container, or GCS bucket) instead of Confluent-managed storage. BYOB is required for private networking deployments because Confluent-Managed Storage doesn't support private data plane access.

### What are External Tables?

**External tables** are catalog metadata entries that reference data files in object storage. The catalog stores metadata (schema, location, partitions), but the data itself stays in your storage. When you run `DROP TABLE`, only the metadata is deleted — the data files remain untouched.

Example in Unity Catalog:
```sql
CREATE TABLE my_catalog.my_schema.orders
USING DELTA
LOCATION 's3://my-bucket/tableflow/lkc-xxxxx/orders/'
```

The catalog knows how to read Delta files at that location, but doesn't copy or manage the data.

## The Source/Target Pattern

This tool separates concerns into three layers:

```
┌─────────────────────┐
│   Source            │  Discovers what tables exist
│ (Confluent Cloud)   │  Returns: List[TableInfo]
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│   Engine            │  Compares source vs target
│ (Diff Logic)        │  Decides: add, update, or remove
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│   Target            │  Registers tables in catalog
│ (Unity Catalog)     │  Executes: CREATE/DROP TABLE
└─────────────────────┘
```

### Why This Pattern?

**Separation of concerns**: Sources know how to discover tables, targets know how to register them, but neither needs to understand the other's implementation. The engine orchestrates without knowing specifics of either side.

**Plug-and-play extensibility**: Want to add Snowflake support? Implement the `Target` interface. Want to add a new source? Implement the `Source` interface. The engine doesn't change.

**Testability**: Each layer can be tested independently with mocked dependencies.

## Interface Contracts

### Source Interface

Located in: `catalog_sync/sources/base.py`

```python
class Source(Protocol):
    def list_tables(self) -> List[TableInfo]:
        """
        Discover all tables from the source system.
        
        Returns:
            List of TableInfo objects with name, location, columns, tags, format
        
        Raises:
            AuthenticationError: If credentials are invalid
            NetworkError: If source system is unreachable
        """
        ...
```

**Current implementation**: `catalog_sync/sources/confluent_cloud.py`
- Calls Tableflow API to list topics with materialization enabled
- Calls Stream Catalog GraphQL API to fetch governance tags
- Returns `TableInfo` for each topic

### Target Interface

Located in: `catalog_sync/targets/base.py`

```python
class Target(Protocol):
    def ensure_schema(self, catalog: str, schema: str) -> None:
        """Ensure catalog and schema exist (create if needed)"""
        ...
    
    def list_tables(self) -> List[TableInfo]:
        """List existing tables in the target catalog"""
        ...
    
    def create_table(self, table: TableInfo) -> None:
        """Register a new table in the catalog"""
        ...
    
    def update_table(self, table: TableInfo) -> None:
        """Update table metadata (location changed)"""
        ...
    
    def delete_table(self, table_name: str) -> None:
        """Remove table from catalog (metadata only)"""
        ...
    
    def sync_tags(self, table_name: str, tags: Dict[str, str]) -> None:
        """Apply governance tags to a table (optional)"""
        ...
```

**Current implementation**: `catalog_sync/targets/unity_catalog.py`
- Uses Databricks SQL via `databricks-sdk`
- Executes `CREATE TABLE ... USING DELTA LOCATION` for registration
- Stores location in `COMMENT` field for idempotency checks
- Applies tags via `ALTER TABLE SET TAGS`

### TableInfo Data Model

Located in: `catalog_sync/models.py`

```python
@dataclass
class TableInfo:
    name: str                      # Table name (e.g., "orders")
    location: str                  # Storage path (e.g., "s3://bucket/path")
    columns: List[ColumnInfo]      # Schema
    tags: Dict[str, str]           # Governance tags
    format: str                    # "DELTA" or "ICEBERG"
```

## Data Flow

Here's what happens when you run `sync.py`:

### 1. Configuration Loading
- Load environment variables from `.env.sync` or system environment
- Validate required vars (API keys, cluster ID, warehouse ID, etc.)
- Fail fast if anything is missing

### 2. Source Discovery
- `Source.list_tables()` is called
- Confluent Cloud Tableflow API returns topics with materialization enabled
- Stream Catalog GraphQL API returns governance tags
- Returns `List[TableInfo]` with all discovered tables

### 3. Target Listing
- `Target.ensure_schema()` creates catalog and schema if needed
- `Target.list_tables()` returns existing tables in the catalog
- Each table's location is extracted from metadata (COMMENT field in UC)

### 4. Engine Diff
- Compare source list vs target list by table name
- For each table:
  - **Not in target**: add to `tables_to_add`
  - **In both, same location**: no action needed
  - **In both, different location**: add to `tables_to_update`
  - **In target but not source**: add to `tables_to_remove`

### 5. Target Operations
- For each table to add: `Target.create_table(table)`
- For each table to update: `Target.update_table(table)` (usually DROP + CREATE)
- For each table to remove: `Target.delete_table(table.name)`
- For each table with tags: `Target.sync_tags(table.name, table.tags)`

### 6. Idempotency
Running the sync multiple times with no changes produces **zero writes**:
- Tables with matching locations are skipped
- Engine only acts on differences
- Tag sync only updates changed tags

## Module Responsibilities

### `sync.py`
- **Purpose**: Standalone entry point for running from laptop, CI, or any Python environment
- **Self-contained**: Auto-loads `.env.sync`, builds source and target objects, runs sync
- **No imports from catalog_sync**: Uses `requests` and `databricks-sdk` directly (duplicates some logic for simplicity)
- **Use case**: Quick runs, local testing, manual syncs

### `catalog_sync/handler.py`
- **Purpose**: AWS Lambda entry point
- **What it does**: Loads config, builds source and target, calls engine
- **Use case**: Scheduled Lambda runs via EventBridge

### `catalog_sync/engine.py`
- **Purpose**: Orchestrates the sync (diff and apply)
- **Key function**: `sync_tables(source, target) -> SyncResult`
- **Responsibilities**: Compare source vs target, decide add/update/remove, call target methods
- **Does NOT**: Make API calls directly (delegates to source and target)

### `catalog_sync/sources/`
- **Current implementations**: `confluent_cloud.py`
- **Purpose**: Discover tables from data platforms
- **Returns**: `List[TableInfo]`
- **Extension point**: Add new source implementations here (e.g., `aws_glue.py`, `custom_metadata_store.py`)

### `catalog_sync/targets/`
- **Current implementations**: `unity_catalog.py`
- **Purpose**: Register tables in catalogs
- **Methods**: `create_table()`, `update_table()`, `delete_table()`, `sync_tags()`
- **Extension point**: Add new target implementations here (e.g., `snowflake.py`, `polaris.py`, `bigquery.py`)

### `catalog_sync/config.py`
- **Purpose**: Environment variable loading and validation
- **Responsibilities**: Read env vars, provide defaults, fail fast on missing required vars
- **Extension point**: Add new config vars for new sources or targets

### `catalog_sync/models.py`
- **Purpose**: Shared data models (`TableInfo`, `ColumnInfo`, `SyncResult`)
- **Why separate**: Ensures source and target agree on data structure
- **Extension point**: Add fields to `TableInfo` if new catalogs need additional metadata

## Tags in the Flow

Tags are handled separately from table registration:

1. **Source discovery**: Tags are fetched via Stream Catalog GraphQL API
2. **TableInfo includes tags**: `table.tags = {"PII": "true", "DataOwnership_owner": "team"}`
3. **After table registration**: `Target.sync_tags(table.name, table.tags)` is called
4. **Manifest tracking**: Target tracks which tags it manages (via table properties or workspace file)
5. **Removal**: Tags removed from source are removed from target via `UNSET TAGS`

Tag sync failures are **isolated**: If tag sync fails for one table, it's logged but doesn't block other tables or table registration.

## Next Steps

- **Adding a catalog target?** Read [add-catalog-target.md](add-catalog-target.md)
- **Adding a cloud provider?** Read [add-cloud-provider.md](add-cloud-provider.md)
- **Want to understand design principles?** Read [best-practices.md](best-practices.md)
