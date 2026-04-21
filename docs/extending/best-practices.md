# Best Practices for Extending This Tool

This document outlines design principles and patterns to follow when adding new catalog targets, cloud providers, or other extensions to the Tableflow Catalog Sync tool.

## 1. Cloud-Agnostic Core

### Principle

Keep `sync.py` and the `catalog_sync/` module free of cloud-specific logic. The tool should run anywhere — laptop, AWS Lambda, Azure Functions, Google Cloud Run, Kubernetes, or any Python environment.

### Why It Matters

- **Portability**: Same code runs on all clouds and locally
- **Testability**: No mocking of cloud SDKs needed
- **Maintainability**: One codebase to maintain, not per-cloud variants
- **User experience**: Users can run sync from anywhere without cloud-specific setup

### Where Cloud Logic Belongs

| Layer | Cloud-Agnostic ✅ | Cloud-Specific ⚠️ |
|-------|------------------|-------------------|
| `sync.py` | Yes — runs anywhere | No cloud imports |
| `catalog_sync/` modules | Yes — portable Python | No cloud imports |
| Terraform (`terraform/`) | **No** — per-cloud infrastructure | All cloud resources here |
| Databricks config | **No** — storage credentials differ by cloud | IAM role vs managed identity |
| Bastion config | **No** — cloud-specific endpoints | NGINX proxy URLs |

### Implementation Pattern

**Good** ✅:
```python
# sync.py or catalog_sync/targets/unity_catalog.py

def create_table(self, table: TableInfo) -> None:
    """Register external table in catalog"""
    # Location comes from Tableflow API (already cloud-aware)
    location = table.location  # "s3://..." or "abfss://..." or "gs://..."
    
    # Pass location directly to catalog — let catalog handle the URI
    sql = f"""
    CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.{table.name}
    USING DELTA
    LOCATION '{location}'
    """
    
    self.execute_sql(sql)
```

**Bad** ❌:
```python
# DON'T DO THIS
def create_table(self, table: TableInfo) -> None:
    # Hard-coding cloud detection
    if table.location.startswith("s3://"):
        # AWS-specific handling
        ...
    elif table.location.startswith("abfss://"):
        # Azure-specific handling
        ...
    elif table.location.startswith("gs://"):
        # GCP-specific handling
        ...
```

**Why the bad example is wrong**: The catalog (Unity Catalog, Snowflake, etc.) already knows how to handle different storage URIs. Don't parse or convert them — pass them through.

### How Storage Paths Work

1. **Tableflow API** returns storage location in cloud-specific format
2. **`sync.py`** receives it as a string (`table.location`)
3. **Catalog target** passes it to `CREATE TABLE ... LOCATION '<path>'`
4. **Catalog** handles cloud-specific details (credentials, URI parsing, etc.)

No cloud detection needed in Python code.

### Anti-Patterns to Avoid

```python
# DON'T DO THIS
import boto3  # AWS SDK
import azure.storage.blob  # Azure SDK
from google.cloud import storage  # GCP SDK

# Never import cloud SDKs in sync.py or catalog_sync/ modules
# All cloud access is through catalog APIs (Databricks SDK, Snowflake connector, etc.)
```

**Exception**: Databricks SDK is allowed because it's the catalog client, not a cloud SDK. It works across AWS, Azure, and GCP.

## 2. Interface Contracts

### Principle

All `Source` and `Target` implementations must implement the full interface defined in `catalog_sync/sources/base.py` and `catalog_sync/targets/base.py`. The engine depends on consistent behavior.

### Required Methods

**Source interface** (`catalog_sync/sources/base.py`):
```python
class Source(Protocol):
    def list_tables(self) -> List[TableInfo]:
        """Discover all tables from source system"""
        ...
```

**Target interface** (`catalog_sync/targets/base.py`):
```python
class Target(Protocol):
    def ensure_schema(self, catalog: str, schema: str) -> None:
        """Ensure catalog and schema exist"""
        ...
    
    def list_tables(self) -> List[TableInfo]:
        """List existing tables in target"""
        ...
    
    def create_table(self, table: TableInfo) -> None:
        """Register new table"""
        ...
    
    def update_table(self, table: TableInfo) -> None:
        """Update table metadata (location changed)"""
        ...
    
    def delete_table(self, table_name: str) -> None:
        """Remove table from catalog (metadata only)"""
        ...
    
    def sync_tags(self, table_name: str, tags: Dict[str, str]) -> None:
        """Apply governance tags (optional)"""
        ...
```

### Type Hints Are Required

```python
# Good ✅
def list_tables(self) -> List[TableInfo]:
    """List tables with proper return type"""
    tables: List[TableInfo] = []
    # ... populate tables ...
    return tables

# Bad ❌
def list_tables(self):  # No return type
    return some_list  # Type checker can't verify
```

**Why**: Type hints enable static analysis, catch bugs early, and serve as documentation.

### Error Contracts

**What exceptions to raise**:

| Error Type | When | Example |
|------------|------|---------|
| `AuthenticationError` | Invalid credentials | `raise AuthenticationError("Databricks token invalid or expired")` |
| `PermissionError` | Insufficient privileges | `raise PermissionError("User lacks CREATE TABLE privilege on schema")` |
| `NetworkError` | Connection failures | `raise NetworkError("Could not reach Snowflake account")` |
| `ValueError` | Invalid configuration | `raise ValueError("WAREHOUSE_ID is required but not set")` |
| `TableNotFoundError` | Delete non-existent table | Usually safe to ignore in `delete_table()` |

**Example**:
```python
def create_table(self, table: TableInfo) -> None:
    try:
        self.execute_sql(f"CREATE TABLE ... {table.name} ...")
    except SomeDBError as e:
        if "authentication failed" in str(e).lower():
            raise AuthenticationError(
                f"Databricks authentication failed. Check DATABRICKS_TOKEN is valid. Error: {e}"
            )
        elif "permission denied" in str(e).lower():
            raise PermissionError(
                f"Permission denied creating table {table.name}. "
                f"Grant CREATE TABLE on schema. Error: {e}"
            )
        else:
            raise
```

### Return Value Contracts

**`list_tables()` must return**:
- `List[TableInfo]` with at minimum: `name`, `location`
- `columns` can be empty list if schema discovery is not needed
- `tags` can be empty dict if tag sync is not used
- `format` should be `"DELTA"` or `"ICEBERG"`

**`create_table()`, `update_table()`, `delete_table()` must**:
- Return `None` (no return value)
- Raise exceptions on failure (don't silently fail)
- Be idempotent where possible (e.g., `CREATE TABLE IF NOT EXISTS`)

## 3. Testing Strategy

### Unit Tests with Mocked APIs

**Never require live infrastructure for unit tests**. Mock external APIs (Confluent Cloud, Databricks, Snowflake, etc.) so tests run fast and don't need credentials.

**Example: Testing Unity Catalog target**:
```python
import pytest
from unittest.mock import MagicMock, patch
from catalog_sync.targets.unity_catalog import UnityCatalog
from catalog_sync.models import TableInfo, ColumnInfo

@pytest.fixture
def mock_databricks_client():
    """Mock Databricks SDK WorkspaceClient"""
    with patch("databricks.sdk.WorkspaceClient") as mock_client:
        mock_wc = MagicMock()
        mock_wc.statement_execution.execute_statement.return_value = MagicMock(
            status=MagicMock(state="SUCCEEDED")
        )
        mock_client.return_value = mock_wc
        yield mock_wc

def test_create_table_generates_correct_sql(mock_databricks_client):
    """Verify CREATE TABLE SQL is correct"""
    target = UnityCatalog(
        host="https://test.databricks.com",
        token="test-token",
        warehouse_id="test-warehouse",
        catalog="test_catalog",
        schema="test_schema"
    )
    
    table = TableInfo(
        name="orders",
        location="s3://bucket/path/",
        columns=[ColumnInfo("id", "BIGINT"), ColumnInfo("amount", "DOUBLE")],
        tags={},
        format="DELTA"
    )
    
    target.create_table(table)
    
    # Verify SQL was executed
    call_args = mock_databricks_client.statement_execution.execute_statement.call_args
    sql = call_args.kwargs.get("statement") or call_args[1]["statement"]
    
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "test_catalog.test_schema.orders" in sql
    assert "USING DELTA" in sql
    assert "s3://bucket/path/" in sql
```

**Reference existing tests**:
- `tests/unit/test_target_unity_catalog.py` — Mocks Databricks SDK
- `tests/unit/test_source_confluent_cloud.py` — Mocks HTTP requests
- `tests/unit/test_engine.py` — Mocks both source and target

### What to Test

| Test Type | What to Verify | Example |
|-----------|----------------|---------|
| **Interface compliance** | All required methods implemented | `test_target_implements_full_interface()` |
| **SQL generation** | Correct CREATE/DROP/ALTER statements | `test_create_table_sql_format()` |
| **Error handling** | Auth/permission errors raised correctly | `test_authentication_failure_raises_error()` |
| **Idempotency** | Re-running with no changes = no writes | `test_sync_with_no_changes_is_noop()` |
| **Tag mapping** | Confluent tags → catalog tags correctly | `test_tag_sync_maps_keys_correctly()` |
| **Edge cases** | Empty lists, missing fields, null values | `test_list_tables_when_no_tables_exist()` |

### Integration Tests (Manual Verification)

**When**: After unit tests pass, before merging

**What**: Verify with real infrastructure:
1. Authenticate to real catalog (Snowflake, Unity Catalog, etc.)
2. Run sync against real Tableflow topics
3. Verify tables created with correct schema and location
4. Query tables to ensure data is readable
5. Verify tags applied correctly
6. Run sync again — verify zero changes (idempotency)

**Document what you tested**:
```python
def test_snowflake_integration():
    """
    Integration test with live Snowflake account.
    
    Prerequisites:
    - Snowflake account with storage integration for S3
    - Tableflow topics materialized in S3
    - SNOWFLAKE_* env vars set
    
    Verified manually on 2026-04-21:
    - Tables created: orders, customers
    - Storage locations match Tableflow paths
    - Tags applied: PII=true on customers
    - SELECT COUNT(*) returns expected row counts
    - Second run produced zero changes (idempotent)
    """
    # Can run manually or in CI with real credentials
    pass
```

### Test Data Best Practices

**Use minimal, realistic examples**:
```python
# Good ✅ — Simple but realistic
table = TableInfo(
    name="orders",
    location="s3://my-bucket/lkc-12345/orders/",
    columns=[
        ColumnInfo("order_id", "BIGINT"),
        ColumnInfo("customer_id", "BIGINT"),
        ColumnInfo("total", "DOUBLE"),
    ],
    tags={"PII": "false"},
    format="DELTA"
)

# Bad ❌ — Too complex for a unit test
table = TableInfo(
    name="super_complex_table_with_100_columns",
    location="s3://...",
    columns=[...100 columns...],  # Unnecessary noise
    tags={...50 tags...},
    format="DELTA"
)
```

## 4. Error Handling Patterns

### Actionable Error Messages

**Bad** ❌:
```python
raise Exception("Authentication failed")
```

**Good** ✅:
```python
raise AuthenticationError(
    "Databricks authentication failed. "
    "Check that DATABRICKS_TOKEN is valid and not expired. "
    "Generate a new token at: https://docs.databricks.com/en/dev-tools/auth.html#personal-access-tokens"
)
```

**Why**: Tell users **exactly what's wrong** and **how to fix it**.

### Error Categories

Design errors to be **distinguishable** so users can troubleshoot:

| Category | Error Type | Example Message |
|----------|-----------|-----------------|
| **Authentication** | `AuthenticationError` | "Confluent API key invalid. Verify CONFLUENT_API_KEY and CONFLUENT_API_SECRET." |
| **Authorization** | `PermissionError` | "Permission denied creating table `orders`. Grant CREATE TABLE privilege on schema." |
| **Network** | `NetworkError` | "Could not reach Snowflake account `abc123`. Check network connectivity and firewall rules." |
| **Configuration** | `ValueError` | "Missing required environment variable: DATABRICKS_WAREHOUSE_ID" |
| **Catalog-specific** | Custom exceptions | "Unity Catalog error: LOCATION_OVERLAP — another table points to this S3 path." |

### Fail Fast (Configuration Validation)

**Validate configuration at startup** before making any API calls:

```python
# Good ✅
def validate_config():
    """Validate required environment variables before starting sync"""
    required = [
        "CONFLUENT_API_KEY",
        "CONFLUENT_API_SECRET",
        "DATABRICKS_HOST",
        "DATABRICKS_WAREHOUSE_ID",
        "TARGET_CATALOG",
    ]
    
    missing = [var for var in required if not os.getenv(var)]
    
    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"See README.md for configuration instructions."
        )

# Call this BEFORE any API calls
validate_config()
source = build_source()
target = build_target()
```

**Why**: Better to fail immediately with a clear message than to fail deep in the sync with a cryptic API error.

### Isolation (Tag Sync Failures)

**Principle**: Tag sync failures for one table should not block other tables or table registration.

```python
# Good ✅
for table in tables_to_sync:
    try:
        target.sync_tags(table.name, table.tags)
        logger.info(f"Synced tags for {table.name}")
    except Exception as e:
        # Log and continue — don't let one tag failure block everything
        logger.error(f"Failed to sync tags for {table.name}: {e}")
        continue
```

**Why**: Tag sync is **additive** — it enhances tables with metadata but isn't critical to table registration. If PII tagging fails, the table should still be queryable.

### Logging Best Practices

**Log actionable information**:
```python
import logging

logger = logging.getLogger(__name__)

# Good ✅
logger.info(f"Creating table {table.name} at {table.location}")
logger.error(f"Failed to create table {table.name}: {e}", exc_info=True)

# Bad ❌
logger.info("Creating table")  # Which table?
logger.error("Error")  # What error?
```

**Never log secrets**:
```python
# Bad ❌
logger.debug(f"Using token: {os.getenv('DATABRICKS_TOKEN')}")

# Good ✅
logger.debug(f"Using token: {os.getenv('DATABRICKS_TOKEN')[:8]}***")  # Masked
```

### Reference Existing Patterns

**See how Unity Catalog handles errors**:
- `catalog_sync/targets/unity_catalog.py` — Authentication, permission, and SQL errors
- `sync.py` — Configuration validation and error formatting

## 5. Configuration Management

### Environment Variable Naming

**Use descriptive names**:
```bash
# Good ✅
SNOWFLAKE_ACCOUNT=abc123.us-east-1
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
DATABRICKS_WAREHOUSE_ID=abc123def456

# Bad ❌
SF_ACCT=abc123  # Too abbreviated
WAREHOUSE=...   # Ambiguous (Snowflake? Databricks?)
WH_ID=...       # Unclear
```

### Required vs Optional

**Mark clearly in validation and docstrings**:
```python
# Required
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")  # Required
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")  # Required

# Optional with defaults
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "default")  # Optional, defaults to "default"
SYNC_TAGS = os.getenv("SYNC_TAGS", "true").lower() == "true"  # Optional, defaults to true
```

**Validate required vars**:
```python
required = ["DATABRICKS_HOST", "DATABRICKS_WAREHOUSE_ID"]
missing = [v for v in required if not os.getenv(v)]
if missing:
    raise ValueError(f"Missing required vars: {missing}")
```

### Secrets Handling

**Never log or print credentials**:
```python
# Bad ❌
print(f"Using token: {token}")
logger.info(f"API key: {api_key}")

# Good ✅
logger.info("Authenticated successfully")  # Don't log the token
```

**Mask secrets in error messages**:
```python
def mask_secret(value: str, visible_chars: int = 4) -> str:
    """Mask secret, showing only first few characters"""
    if len(value) <= visible_chars:
        return "***"
    return f"{value[:visible_chars]}***"

# Usage
logger.error(f"Auth failed for key {mask_secret(api_key)}")
# Output: "Auth failed for key ABCD***"
```

### Multi-Target Support

**Pattern for multiple catalog instances**:
```bash
# Support multiple Unity Catalog targets
TARGET_TYPE=unity_catalog
TARGET_CATALOG=prod_catalog
TARGET_SCHEMA=lkc_prod

# OR

TARGET_TYPE=snowflake
SNOWFLAKE_DATABASE=analytics
SNOWFLAKE_SCHEMA=lkc_prod
```

**Configuration model**:
```python
@dataclass
class TargetConfig:
    type: str  # "unity_catalog", "snowflake", "polaris"
    catalog: str
    schema: str
    # Target-specific fields
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    snowflake_account: Optional[str] = None
    # ...

def build_target(config: TargetConfig) -> Target:
    if config.type == "unity_catalog":
        return UnityCatalog(...)
    elif config.type == "snowflake":
        return SnowflakeCatalog(...)
    else:
        raise ValueError(f"Unknown target type: {config.type}")
```

## 6. Code Organization

### One Target Per File

```
catalog_sync/
  targets/
    base.py              # Abstract interface
    unity_catalog.py     # Unity Catalog implementation
    snowflake.py         # Snowflake implementation
    polaris.py           # Polaris implementation
    bigquery.py          # BigQuery implementation
```

**Don't put multiple targets in one file** — keeps each implementation focused.

### Keep Targets Independent

**Good** ✅:
```python
# catalog_sync/targets/snowflake.py
from catalog_sync.models import TableInfo
from catalog_sync.targets.base import Target

# No imports from other target implementations
```

**Bad** ❌:
```python
# catalog_sync/targets/snowflake.py
from catalog_sync.targets.unity_catalog import UnityCatalog  # Don't cross-reference targets

class SnowflakeCatalog:
    def some_method(self):
        # Don't call Unity Catalog methods from Snowflake
        uc = UnityCatalog(...)
        ...
```

**Why**: Targets should be plug-and-play. Snowflake shouldn't depend on Unity Catalog being installed.

### Shared Utilities

**If multiple targets need the same logic**, put it in `catalog_sync/utils.py`:

```python
# catalog_sync/utils.py

def sanitize_tag_key(key: str) -> str:
    """
    Sanitize tag keys to be catalog-compatible.
    
    Unity Catalog prohibits: . , - = / :
    Snowflake prohibits: similar set
    
    Replace all with underscore.
    """
    prohibited = ['.', ',', '-', '=', '/', ':', ' ', ';', '(', ')']
    sanitized = key
    for char in prohibited:
        sanitized = sanitized.replace(char, '_')
    return sanitized
```

**Usage in targets**:
```python
from catalog_sync.utils import sanitize_tag_key

class SnowflakeCatalog:
    def sync_tags(self, table_name: str, tags: Dict[str, str]) -> None:
        sanitized_tags = {sanitize_tag_key(k): v for k, v in tags.items()}
        # ...
```

### Documentation (Docstrings)

**All public methods need docstrings**:
```python
def create_table(self, table: TableInfo) -> None:
    """
    Register a new external table in the catalog.
    
    Args:
        table: TableInfo with name, location, columns, format, tags
    
    Raises:
        AuthenticationError: If credentials are invalid
        PermissionError: If user lacks CREATE TABLE privilege
        TableAlreadyExistsError: If table already exists
    
    Notes:
        - Table data is NOT copied — only metadata is registered
        - If table exists, this is a no-op (CREATE TABLE IF NOT EXISTS)
        - Location must be accessible by the catalog's storage credential
    """
    ...
```

**Explain non-obvious choices**:
```python
def update_table(self, table: TableInfo) -> None:
    """
    Update table metadata when location changes.
    
    Implemented as DROP + CREATE because Unity Catalog does not support
    ALTER TABLE SET LOCATION. The DROP is metadata-only — data files
    in S3 are never touched.
    """
    self.delete_table(table.name)
    self.create_table(table)
```

## 7. Idempotency and Safety

### Idempotent Operations

**Running sync multiple times with no changes should produce zero writes**:

```python
# First run
sync()  # Output: "2 tables added, 0 updated, 0 removed"

# Second run (no changes)
sync()  # Output: "0 tables added, 0 updated, 0 removed"  ✅
```

**How to achieve this**:
1. **Store location in metadata** (e.g., COMMENT field in Unity Catalog)
2. **Compare source vs target by location**
3. **Skip tables with matching locations**

**Example**:
```python
def list_tables(self) -> List[TableInfo]:
    """List existing tables with locations from COMMENT field"""
    sql = """
    SELECT table_name, comment
    FROM catalog.information_schema.tables
    WHERE table_type = 'EXTERNAL'
    """
    result = self.execute_sql(sql)
    
    tables = []
    for row in result:
        tables.append(TableInfo(
            name=row.table_name,
            location=row.comment,  # Location stored in COMMENT
            columns=[],
            tags={},
            format="DELTA"
        ))
    return tables
```

**Engine compares**:
```python
# If locations match, skip
if source_table.location == target_table.location:
    # No action needed
    continue
else:
    # Location changed, update table
    target.update_table(source_table)
```

### Metadata-Only Changes

**Table updates must never touch data files**:

```python
def update_table(self, table: TableInfo) -> None:
    """
    Update table metadata (location changed).
    
    CRITICAL: This is metadata-only. The DROP TABLE removes catalog
    metadata but does NOT delete data files in S3/ADLS/GCS.
    """
    # Drop metadata
    sql = f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.{table.name}"
    self.execute_sql(sql)
    
    # Re-create with new location (data files untouched)
    self.create_table(table)
```

**Why**: External tables are **pointers** to data. Dropping the table drops the pointer, not the data.

### Tag Manifest Tracking

**Problem**: How to distinguish Confluent-managed tags from user-created tags?

**Solution**: Store a manifest of managed tags.

**Unity Catalog approach** (table properties):
```sql
-- Store manifest in table properties
ALTER TABLE catalog.schema.orders
SET TBLPROPERTIES ('_confluent_managed_tags' = 'PII,DataOwnership_owner');

-- Read manifest
SELECT property_value
FROM catalog.information_schema.table_properties
WHERE table_name = 'orders' AND property_key = '_confluent_managed_tags';
```

**Alternative approach** (workspace file for `sync_tags.py`):
```python
# Store manifest in Databricks workspace
manifest = {
    "orders": ["PII", "DataOwnership_owner"],
    "customers": ["PII", "PRIVATE"]
}

# Write to /Shared/.confluent_tag_manifest.json
workspace_client.files.upload(
    "/Shared/.confluent_tag_manifest.json",
    json.dumps(manifest)
)

# Read on next run
manifest = json.loads(
    workspace_client.files.download("/Shared/.confluent_tag_manifest.json")
)
```

**Tag removal**:
```python
# Tags in manifest but not in Confluent = remove from catalog
managed_keys = manifest.get(table_name, [])
current_keys = set(tags.keys())
to_remove = [k for k in managed_keys if k not in current_keys]

if to_remove:
    sql = f"ALTER TABLE {table_name} UNSET TAGS ({', '.join(to_remove)})"
    self.execute_sql(sql)
```

## Summary Checklist

When implementing a new catalog target or cloud provider extension:

- [ ] Keep `sync.py` and `catalog_sync/` cloud-agnostic
- [ ] Implement full `Source` or `Target` interface with type hints
- [ ] Write unit tests with mocked APIs (no live infrastructure)
- [ ] Provide actionable error messages with fix instructions
- [ ] Validate configuration at startup (fail fast)
- [ ] Isolate tag sync failures (don't block table sync)
- [ ] Use descriptive environment variable names
- [ ] Never log or print secrets
- [ ] Keep target implementations independent (no cross-imports)
- [ ] Document all public methods with docstrings
- [ ] Make operations idempotent (no changes = no writes)
- [ ] Ensure table updates are metadata-only (data files never touched)
- [ ] Track managed tags in a manifest
- [ ] Test with live infrastructure before merging
- [ ] Document what was verified in integration tests

## Next Steps

- Review [architecture-overview.md](architecture-overview.md) for the source/target pattern
- Study existing implementations: `catalog_sync/targets/unity_catalog.py`, `catalog_sync/sources/confluent_cloud.py`
- Follow these patterns when adding new targets or cloud providers
