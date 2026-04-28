# Iceberg Metadata Refresh Design

**Date:** 2026-04-28  
**Status:** Future Work (Not Yet Implemented)  
**Author:** Claude Sonnet 4.5

## Problem Statement

### Current Limitation

The Tableflow Catalog Sync tool currently supports **Delta Lake only** and explicitly skips Iceberg tables. The reason is documented in the code:

```python
VALID_TABLE_FORMATS = {"DELTA"}  # Iceberg tables are skipped
```

And in README.md:
> "Iceberg tables are skipped — UC does not auto-refresh Iceberg metadata from external storage, so registered Iceberg tables would go stale as Tableflow writes new snapshots."

### Why This Happens: Delta vs Iceberg Architecture

**Delta Lake (works today):**
- **Source of truth**: `_delta_log/` transaction log in object storage (S3/ADLS/GCS)
- **How registration works**: `CREATE EXTERNAL TABLE ... USING DELTA LOCATION 's3://...'` stores only a pointer to the storage location in Unity Catalog
- **Auto-refresh behavior**: On every query, Unity Catalog reads the latest Delta transaction log from storage to reconstruct the current table snapshot
- **Result**: New data written by Tableflow (or any Delta writer) is automatically visible — no manual refresh needed

**Iceberg (broken today):**
- **Source of truth**: **Iceberg catalog metadata** (Tableflow's REST catalog, Polaris, Horizon, etc.) + manifest/snapshot files in storage
- **How registration works**: When you register an Iceberg table in Unity Catalog as an external table, UC materializes a **snapshot** of the Iceberg catalog's metadata into its own catalog representation
- **No auto-refresh**: When Tableflow writes new data and updates the Iceberg catalog, Unity Catalog doesn't know about it — UC is still pointing at the old snapshot
- **Result**: Queries return stale data — the table appears frozen at the moment of registration

### Evidence from Internal Research

From Glean chat with internal Confluent documentation:

> "For Iceberg, the catalog is the source of truth, and many engines materialize that into their own metadata layer, requiring explicit or configured refresh to stay current."

> "Unity Catalog today doesn't directly support Tableflow Iceberg; when you use Iceberg via UC/Delta Uniform, metadata does not auto-update from an external writer, and you need explicit or scheduled refresh logic."

> "For Databricks: Unity Catalog stores only a pointer to the Delta location and some table metadata. The authoritative metadata lives in the Delta transaction log under `_delta_log` in object storage. Unity Catalog does not need an explicit refresh to 'see' new rows: on query, Databricks reads the latest Delta log from storage and reconstructs the current table snapshot."

### Customer Impact

Customers want to use Tableflow with Iceberg format for:
- **Multi-catalog compatibility**: Iceberg works with Snowflake, Polaris, BigQuery, not just Unity Catalog
- **Open standard**: Iceberg is an Apache project, less vendor lock-in than Delta
- **Advanced features**: Iceberg's hidden partitioning, partition evolution, etc.

But they're blocked because registered Iceberg tables go stale immediately.

## Solution: Refresh-on-Sync Pattern

### High-Level Approach

Add metadata refresh to the sync flow:
1. Register table (or update existing table)
2. **Immediately refresh metadata** via catalog-specific refresh command
3. Repeat on every sync run (time-based refresh every 5 minutes)

**Why time-based refresh is acceptable:**
- Tableflow doesn't update snapshots frequently (user stated "every 5m should be plenty")
- Simpler than conditional refresh (no need to track snapshot IDs)
- Works across all catalog types (catalog-agnostic)

### Architecture Changes

#### 1. Target Interface Extension

Add two new methods to `catalog_sync/targets/base.py`:

```python
class Target(Protocol):
    # Existing methods (unchanged)
    def ensure_schema(self, catalog: str, schema: str) -> None: ...
    def list_tables(self) -> List[TableInfo]: ...
    def create_table(self, table: TableInfo) -> None: ...
    def update_table(self, table: TableInfo) -> None: ...
    def delete_table(self, table_name: str) -> None: ...
    def sync_tags(self, table_name: str, tags: Dict[str, str]) -> None: ...
    
    # NEW: Format support declaration
    def supports_format(self, format: str) -> bool:
        """
        Return True if this catalog can query tables in the given format.
        
        Args:
            format: Table format (e.g., "DELTA", "ICEBERG")
            
        Returns:
            True if catalog supports this format
            
        Examples:
            Unity Catalog: supports_format("DELTA") = True, supports_format("ICEBERG") = True (technically)
            Snowflake: supports_format("DELTA") = True, supports_format("ICEBERG") = True
            AWS Glue: supports_format("ICEBERG") = True, supports_format("DELTA") = ?
        """
        ...
    
    # NEW: Metadata refresh
    def refresh_table(self, table_name: str, format: str) -> None:
        """
        Refresh table metadata from underlying storage/catalog.
        
        Implementation decides whether refresh is actually needed for this format.
        For Delta tables in Unity Catalog, this is typically a no-op (auto-refreshes).
        For Iceberg tables, this triggers metadata refresh (e.g., REFRESH TABLE).
        
        Args:
            table_name: Name of the table to refresh
            format: Table format ("DELTA" or "ICEBERG")
            
        Raises:
            Should NOT raise on refresh failures - log and continue
        """
        ...
```

**Design rationale:**
- `supports_format()` is customer-facing: "Does this catalog support Iceberg?" (Yes/No, simple)
- `refresh_table()` encapsulates implementation detail: whether refresh is actually needed for a given format
- Delta no-op is hidden from customers — they just call refresh, target decides what to do

#### 2. Unity Catalog Implementation (Delta-Only for Now)

Update `catalog_sync/targets/unity_catalog.py`:

```python
class UnityCatalog:
    # ... existing __init__ and methods ...
    
    def supports_format(self, format: str) -> bool:
        """
        Unity Catalog implementation currently supports Delta only.
        
        Note: Unity Catalog can technically handle Iceberg tables, but this
        implementation doesn't support it yet. Future enhancement will add
        Iceberg support by returning True and implementing refresh logic.
        """
        return format == "DELTA"  # NOT "ICEBERG" yet
    
    def refresh_table(self, table_name: str, format: str) -> None:
        """
        Refresh table metadata.
        
        Currently Delta-only (no-op since Delta auto-refreshes from _delta_log).
        When Iceberg support is added to Unity Catalog implementation,
        this will execute REFRESH TABLE for Iceberg format.
        
        Args:
            table_name: Name of the table to refresh
            format: Table format ("DELTA" or "ICEBERG")
        """
        # Delta: no-op (auto-refreshes from transaction log)
        # Iceberg: not supported yet in this implementation
        pass
```

**Why Delta is a no-op:**
- Delta tables auto-refresh from `_delta_log/` on every query
- Unity Catalog reads the latest log from storage automatically
- No explicit `REFRESH TABLE` command needed

**Future Iceberg support (when enabled):**
```python
def supports_format(self, format: str) -> bool:
    return format in {"DELTA", "ICEBERG"}  # Enable Iceberg

def refresh_table(self, table_name: str, format: str) -> None:
    if format == "ICEBERG":
        sql = f"REFRESH TABLE `{self.catalog}`.`{self.schema}`.`{table_name}`"
        try:
            self._execute_sql(sql)
            logger.info(f"Refreshed Iceberg table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to refresh table {table_name}: {e}")
            # Don't raise - refresh failures shouldn't block sync
    # Delta: no-op
```

#### 3. Engine Integration

Update `catalog_sync/engine.py`:

```python
def sync_tables(source: Source, target: Target) -> SyncResult:
    """Sync tables from source to target with metadata refresh"""
    
    # ... existing diff logic (unchanged) ...
    
    # Apply changes with refresh
    for table in tables_to_add:
        target.create_table(table)
        target.refresh_table(table.name, table.format)  # NEW
        logger.info(f"Created and refreshed table: {table.name} ({table.format})")
    
    for table in tables_to_update:
        target.update_table(table)
        target.refresh_table(table.name, table.format)  # NEW
        logger.info(f"Updated and refreshed table: {table.name} ({table.format})")
    
    for table_name in tables_to_remove:
        target.delete_table(table_name)
        # No refresh needed - table is deleted
    
    # ... tag sync logic (unchanged) ...
    
    return SyncResult(
        added=len(tables_to_add),
        updated=len(tables_to_update),
        removed=len(tables_to_remove)
    )
```

**Refresh timing:**
- Happens immediately after `create_table()` or `update_table()`
- Ensures newly registered tables are queryable right away
- No separate refresh loop or scheduling needed

#### 4. Standalone Script Updates

Update `sync.py`:

```python
# Keep format validation as Delta-only for now
_VALID_TABLE_FORMATS = {"DELTA"}  # Unchanged

# In the sync loop, after creating tables:
for name, t in source_tables.items():
    if name not in target_tables:
        # Create new table
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{name}`
        USING {t['format']}
        LOCATION '{t['location']}'
        COMMENT '{t['location']}'
        """
        _execute_sql(sql, warehouse_id, workspace_client)
        
        # NEW: Refresh metadata (currently no-op for Delta, future-ready)
        if t['format'] == "ICEBERG":
            refresh_sql = f"REFRESH TABLE `{CATALOG}`.`{SCHEMA}`.`{name}`"
            try:
                _execute_sql(refresh_sql, warehouse_id, workspace_client)
                print(f"  Refreshed Iceberg table: {name}")
            except Exception as e:
                print(f"  Warning: Failed to refresh {name}: {e}")
        
        print(f"+ Adding: {name}")
    
    elif target_tables[name] != t['location']:
        # Update existing table (location changed)
        # DROP + CREATE
        _execute_sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.`{name}`", ...)
        _execute_sql(f"CREATE TABLE ... USING {t['format']} ...", ...)
        
        # NEW: Refresh after recreate
        if t['format'] == "ICEBERG":
            refresh_sql = f"REFRESH TABLE `{CATALOG}`.`{SCHEMA}`.`{name}`"
            try:
                _execute_sql(refresh_sql, warehouse_id, workspace_client)
            except Exception as e:
                print(f"  Warning: Failed to refresh {name}: {e}")
```

**Note:** Iceberg code is present but unreachable since `_VALID_TABLE_FORMATS = {"DELTA"}`. When Iceberg is enabled, change to `{"DELTA", "ICEBERG"}`.

### Error Handling

**Refresh failures should NOT block sync:**

```python
try:
    target.refresh_table(table_name, format)
except Exception as e:
    logger.error(f"Failed to refresh table {table_name}: {e}")
    # Don't raise - continue with next table
```

**Why:**
- Follows same pattern as tag sync (isolation principle)
- One table's refresh failure shouldn't prevent other tables from syncing
- Refresh is an enhancement, not critical path — if it fails, table is still registered

**Common refresh failure scenarios:**
- Iceberg catalog temporarily unreachable
- Permission errors on `REFRESH TABLE` command
- Network timeouts to external catalog

## Testing Strategy

### Unit Tests

Add to `tests/unit/test_target_unity_catalog.py`:

```python
def test_supports_format_delta_only():
    """Verify Unity Catalog supports Delta but not Iceberg yet"""
    target = UnityCatalog(
        host="https://test.databricks.com",
        token="test-token",
        warehouse_id="test-warehouse",
        catalog="test_catalog",
        schema="test_schema"
    )
    
    assert target.supports_format("DELTA") == True
    assert target.supports_format("ICEBERG") == False


def test_refresh_table_is_noop_for_delta():
    """Verify refresh_table doesn't execute SQL for Delta tables"""
    with patch("databricks.sdk.WorkspaceClient") as mock_client:
        target = UnityCatalog(...)
        
        # Should not raise, should not execute any SQL
        target.refresh_table("orders", "DELTA")
        
        # Verify no refresh SQL was executed
        # (create_table would execute, but not refresh)


def test_refresh_table_iceberg_not_supported():
    """Verify refresh_table is no-op for Iceberg (not supported yet)"""
    target = UnityCatalog(...)
    
    # Should not raise even for unsupported format
    target.refresh_table("orders", "ICEBERG")
```

Add to `tests/unit/test_engine.py`:

```python
def test_engine_calls_refresh_after_create():
    """Verify engine calls refresh_table after creating a table"""
    mock_source = MagicMock()
    mock_target = MagicMock()
    
    mock_source.list_tables.return_value = [
        TableInfo(name="orders", location="s3://bucket/orders", format="DELTA", columns=[], tags={})
    ]
    mock_target.list_tables.return_value = []
    mock_target.supports_format.return_value = True
    
    engine.sync_tables(mock_source, mock_target)
    
    # Verify refresh was called after create
    mock_target.create_table.assert_called_once()
    mock_target.refresh_table.assert_called_with("orders", "DELTA")


def test_engine_calls_refresh_after_update():
    """Verify engine calls refresh_table after updating a table"""
    mock_source = MagicMock()
    mock_target = MagicMock()
    
    # Source has new location for existing table
    mock_source.list_tables.return_value = [
        TableInfo(name="orders", location="s3://bucket/orders-v2", format="DELTA", columns=[], tags={})
    ]
    mock_target.list_tables.return_value = [
        TableInfo(name="orders", location="s3://bucket/orders-v1", format="DELTA", columns=[], tags={})
    ]
    mock_target.supports_format.return_value = True
    
    engine.sync_tables(mock_source, mock_target)
    
    # Verify refresh was called after update
    mock_target.update_table.assert_called_once()
    mock_target.refresh_table.assert_called_with("orders", "DELTA")


def test_engine_refresh_failure_does_not_block_sync():
    """Verify refresh failures don't stop the sync"""
    mock_target = MagicMock()
    mock_target.refresh_table.side_effect = Exception("Catalog unreachable")
    
    # Should not raise - log and continue
    engine.sync_tables(mock_source, mock_target)
    
    # Verify other operations still completed
    mock_target.create_table.assert_called()
```

### Integration Testing (Manual)

Document what needs manual verification:

1. **Delta table registration** (current behavior, should still work):
   - Register Delta table via sync
   - Verify no errors in logs
   - Query table in Databricks → data visible
   - Run sync again → zero changes (idempotent)

2. **Refresh infrastructure** (no-op for Delta, ready for Iceberg):
   - Verify `refresh_table()` is called after create/update (check logs)
   - Verify no extra SQL executed for Delta (Delta auto-refreshes)
   - Verify sync completes successfully

3. **Future Iceberg validation** (when enabled):
   - Enable Iceberg support (`VALID_TABLE_FORMATS = {"DELTA", "ICEBERG"}`)
   - Register Iceberg table
   - Verify `REFRESH TABLE` is executed (check Databricks query history)
   - Write new data via Tableflow
   - Run sync → verify refresh happens
   - Query table → verify new data visible

## Documentation Updates

### CLAUDE.md

**Before:**
```markdown
- **Table format**: Delta Lake only for demo (`table_formats: ["DELTA"]`). Dual format publishing (`["DELTA", "ICEBERG"]`) fails.
```

**After:**
```markdown
- **Table format**: Delta Lake only for now (`table_formats: ["DELTA"]`). Interface designed for Iceberg support in future catalog targets (Snowflake, Polaris).
- **Metadata refresh**: Target interface includes `refresh_table()` method for Iceberg support in future catalogs. Delta tables auto-refresh, no explicit refresh needed.
```

### README.md

**Remove from "Design Decisions":**
```markdown
- **Delta only.** The engine validates that the table format is Delta before registering. Iceberg tables are skipped — UC does not auto-refresh Iceberg metadata from external storage, so registered Iceberg tables would go stale as Tableflow writes new snapshots.
```

**Keep:**
```markdown
- **Delta Lake for Unity Catalog.** Current implementation supports Delta format only. External tables registered via `CREATE TABLE ... USING DELTA LOCATION`.
```

### BEST_PRACTICES.md

**Keep in "Known Limitations":**
```markdown
- **Delta format only** — Unity Catalog implementation supports Delta only. Iceberg support planned for future catalog targets (Snowflake, Polaris).
```

### docs/extending/add-catalog-target.md

**Add new section after "Target Interface Requirements":**

```markdown
## Iceberg Metadata Refresh (For Future Catalog Implementations)

**Note**: The current Unity Catalog implementation supports Delta only. This section documents how to add Iceberg support when implementing new catalog targets like Snowflake or Polaris.

### Why Iceberg Needs Explicit Refresh

Delta Lake and Iceberg handle metadata differently:

**Delta Lake:**
- Metadata lives in `_delta_log/` transaction log in object storage
- Catalogs read this log on every query → auto-refresh behavior
- No explicit refresh command needed

**Iceberg:**
- Metadata lives in the Iceberg catalog (REST catalog, Polaris, Horizon, Glue)
- When you register an Iceberg table in Unity Catalog or Snowflake as an external table, the catalog materializes a **snapshot** of the Iceberg catalog's metadata
- New data written by Tableflow updates the source Iceberg catalog, but the target catalog doesn't know about it
- **Requires explicit refresh** to sync metadata from source catalog to target catalog

### Implementing Iceberg Support

Your catalog target must implement two methods:

```python
def supports_format(self, format: str) -> bool:
    """Return True if catalog can query this format"""
    return format in {"DELTA", "ICEBERG"}

def refresh_table(self, table_name: str, format: str) -> None:
    """Refresh metadata after registration or updates"""
    if format == "ICEBERG":
        # Execute catalog-specific refresh command
        self.execute_sql(f"REFRESH TABLE {table_name}")
    # Delta: no-op (auto-refreshes)
```

### Catalog-Specific Refresh Commands

**Unity Catalog (when Iceberg support is added):**
```sql
REFRESH TABLE catalog.schema.table_name
```

**Snowflake:**
```sql
ALTER ICEBERG TABLE database.schema.table_name REFRESH
```

**AWS Glue (via Athena):**
```sql
MSCK REPAIR TABLE database.table_name
-- or
REFRESH TABLE database.table_name
```

**Polaris/REST Catalog:**
May not need refresh if Polaris is the authoritative catalog (not an external table scenario).

### Error Handling

Refresh failures should NOT block sync:

```python
def refresh_table(self, table_name: str, format: str) -> None:
    if format == "ICEBERG":
        try:
            self.execute_sql(f"REFRESH TABLE {table_name}")
            logger.info(f"Refreshed Iceberg table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to refresh {table_name}: {e}")
            # Don't raise - log and continue
```

### Testing Iceberg Support

1. **Unit tests**: Mock catalog API, verify refresh SQL is generated correctly
2. **Integration test**: 
   - Register Iceberg table via sync
   - Write new data via Tableflow
   - Run sync → verify refresh executes
   - Query table → verify new data visible

### Performance Considerations

The current implementation refreshes **all Iceberg tables on every sync run**, regardless of whether data changed. This is simple and works well when:
- Tableflow doesn't write frequently (< 1 update per 5 minutes per table)
- Number of Iceberg tables is manageable (< 100)

For large deployments with hundreds of Iceberg tables, consider:
- Conditional refresh (track snapshot IDs, only refresh if changed)
- Separate refresh loop (refresh on different cadence than table sync)
- Catalog-specific auto-refresh policies (e.g., Snowflake `AUTO_REFRESH`)
```

## Migration Path and Backward Compatibility

### Backward Compatibility

**Existing Unity Catalog deployments:**
- No breaking changes to existing interface methods
- New methods (`supports_format`, `refresh_table`) must be added
- Implementations are straightforward (Delta-only, no-op refresh)

**Code changes required:**
```python
# Add to catalog_sync/targets/unity_catalog.py
def supports_format(self, format: str) -> bool:
    return format == "DELTA"

def refresh_table(self, table_name: str, format: str) -> None:
    pass  # No-op for Delta
```

**No user action required:**
- Sync behavior unchanged for Delta tables
- No new environment variables
- No schema changes in Unity Catalog
- Existing `.env.sync` files work as-is

### Future Iceberg Enablement

When Unity Catalog + Iceberg support is added:

1. **Update `supports_format()`:**
   ```python
   return format in {"DELTA", "ICEBERG"}
   ```

2. **Implement `refresh_table()` for Iceberg:**
   ```python
   if format == "ICEBERG":
       self.execute_sql(f"REFRESH TABLE {self.catalog}.{self.schema}.{table_name}")
   ```

3. **Enable Iceberg in validation:**
   ```python
   VALID_TABLE_FORMATS = {"DELTA", "ICEBERG"}
   ```

4. **Update documentation** to announce Iceberg support

### For New Catalog Targets

Future catalog implementations (Snowflake, Polaris, BigQuery) will:
- Implement both `supports_format()` and `refresh_table()` from day one
- Follow patterns documented in `docs/extending/add-catalog-target.md`
- Test with real Iceberg tables during implementation

## Success Criteria

1. ✅ Target interface extended with `supports_format()` and `refresh_table()`
2. ✅ Unity Catalog implementation stays Delta-only, refresh is no-op
3. ✅ Engine calls refresh after create/update operations
4. ✅ Refresh failures are logged but don't block sync
5. ✅ Unit tests verify interface compliance and refresh behavior
6. ✅ Extension docs explain Iceberg refresh for future implementers
7. ✅ No breaking changes to existing deployments
8. ✅ Clear migration path for enabling Iceberg support later

## Non-Goals

- **Not implementing actual Iceberg support for Unity Catalog** — interface only, Delta-only for now
- **Not adding conditional refresh** (snapshot tracking) — time-based refresh is sufficient
- **Not adding separate refresh scheduling** — uses existing sync cadence
- **Not supporting other Iceberg catalogs** — Polaris, Glue, etc. are future work

## References

- Glean research on Delta vs Iceberg metadata handling
- Unity Catalog `REFRESH TABLE` documentation
- Snowflake Iceberg external tables documentation
- Internal Slack discussions on Tableflow + Iceberg gaps
