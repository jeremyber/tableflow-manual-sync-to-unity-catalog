# Adding Catalog Targets

This guide explains how to add support for new catalog targets like Snowflake Open Catalog, Apache Polaris, BigQuery, AWS Glue enhancements, or any other catalog system.

## Overview

### What Makes a Catalog Compatible?

A catalog is compatible with this tool if it supports **external tables by storage location reference**. Specifically, it must allow:

1. **Creating tables that point to existing data files** (not copying data into the catalog)
2. **Specifying a storage path** (S3, ADLS, GCS) where the data lives
3. **Reading Delta Lake or Iceberg format** (or both)

Examples of compatible catalogs:
- ✅ Databricks Unity Catalog
- ✅ Snowflake Open Catalog
- ✅ Apache Polaris
- ✅ AWS Glue Data Catalog
- ✅ Google BigQuery with BigLake
- ❌ Traditional RDBMS catalogs (PostgreSQL, MySQL) — these don't support external tables by storage path

### Current Implementation

**Unity Catalog** via Databricks SQL:
- Location: `catalog_sync/targets/unity_catalog.py`
- Authentication: Databricks SDK (token or service principal)
- Table creation: `CREATE TABLE ... USING DELTA LOCATION 's3://...'`
- Tag sync: `ALTER TABLE SET TAGS ('key' = 'value')`
- Idempotency: Stores location in `COMMENT` field

### Next Known Target: Snowflake Open Catalog

Snowflake is the most requested catalog target. This guide uses Snowflake as a reference example, but the patterns apply to any catalog.

## Target Interface Requirements

All catalog targets must implement the `Target` protocol defined in `catalog_sync/targets/base.py`.

### Required Methods

```python
from catalog_sync.models import TableInfo
from typing import List, Dict

class Target(Protocol):
    def ensure_schema(self, catalog: str, schema: str) -> None:
        """
        Ensure catalog and schema exist. Create them if they don't.
        
        Args:
            catalog: Catalog name (e.g., "my_catalog")
            schema: Schema name (e.g., "lkc_12345")
        
        Raises:
            AuthenticationError: If credentials are invalid
            PermissionError: If user lacks CREATE SCHEMA privilege
        """
        ...
    
    def list_tables(self) -> List[TableInfo]:
        """
        List all existing external tables in the target catalog.schema.
        
        Returns:
            List of TableInfo with name, location, columns, tags, format
            
        Notes:
            - Must extract storage location from table metadata
            - Should only return external tables (not managed tables)
        """
        ...
    
    def create_table(self, table: TableInfo) -> None:
        """
        Register a new external table in the catalog.
        
        Args:
            table: TableInfo with name, location, columns, format, tags
            
        Raises:
            TableAlreadyExistsError: If table already exists (idempotency check)
            PermissionError: If user lacks CREATE TABLE privilege
            LocationError: If storage location is inaccessible
        """
        ...
    
    def update_table(self, table: TableInfo) -> None:
        """
        Update table metadata (typically when location changes).
        
        Args:
            table: TableInfo with updated location
            
        Notes:
            - Usually implemented as DROP + CREATE (metadata only, data untouched)
            - Must be atomic if possible
        """
        ...
    
    def delete_table(self, table_name: str) -> None:
        """
        Remove table from catalog (metadata only, data untouched).
        
        Args:
            table_name: Name of the table to drop
            
        Raises:
            TableNotFoundError: If table doesn't exist (can be ignored)
        """
        ...
    
    def sync_tags(self, table_name: str, tags: Dict[str, str]) -> None:
        """
        Apply governance tags to a table (optional).
        
        Args:
            table_name: Name of the table
            tags: Dict of tag key-value pairs
            
        Notes:
            - Should preserve tags not managed by this tool
            - Track managed tags in a manifest
            - If tags are not supported, this method can be a no-op
        """
        ...
```

### When Each Method is Called

**During sync execution:**

1. **`ensure_schema(catalog, schema)`** — Called once at the beginning
2. **`list_tables()`** — Called to get current state of target
3. **`create_table(table)`** — Called for each new table discovered in source
4. **`update_table(table)`** — Called when a table's location has changed
5. **`delete_table(table_name)`** — Called for tables that exist in target but not source
6. **`sync_tags(table_name, tags)`** — Called after table registration (if tags are present)

## Implementation Checklist

### Step 1: Create Target Implementation File

Create a new file: `catalog_sync/targets/<catalog_name>.py`

Example for Snowflake:
```python
# catalog_sync/targets/snowflake.py

from typing import List, Dict
from catalog_sync.models import TableInfo, ColumnInfo
import snowflake.connector  # Add to pyproject.toml dependencies

class SnowflakeCatalog:
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str,
    ):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self._conn = None
    
    def _connect(self):
        """Establish connection (lazy initialization)"""
        if self._conn is None:
            self._conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )
        return self._conn
    
    def ensure_schema(self, catalog: str, schema: str) -> None:
        """Create database and schema if they don't exist"""
        # Implementation here
        ...
    
    def list_tables(self) -> List[TableInfo]:
        """List external tables in Snowflake"""
        # Implementation here
        ...
    
    def create_table(self, table: TableInfo) -> None:
        """Create external table in Snowflake"""
        # Implementation here
        ...
    
    # ... implement other required methods
```

### Step 2: Add Configuration Variables

Update `catalog_sync/config.py` to support the new target:

```python
# Add Snowflake-specific vars
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
```

Add validation in `validate_config()`:
```python
if TARGET_TYPE == "snowflake":
    required = [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise ValueError(f"Missing required Snowflake variables: {missing}")
```

### Step 3: Update Handler to Support New Target

In `catalog_sync/handler.py`, add logic to instantiate the new target:

```python
from catalog_sync.targets.snowflake import SnowflakeCatalog

def build_target() -> Target:
    target_type = config.TARGET_TYPE or "unity_catalog"
    
    if target_type == "unity_catalog":
        return UnityCatalog(...)
    elif target_type == "snowflake":
        return SnowflakeCatalog(
            account=config.SNOWFLAKE_ACCOUNT,
            user=config.SNOWFLAKE_USER,
            password=config.SNOWFLAKE_PASSWORD,
            warehouse=config.SNOWFLAKE_WAREHOUSE,
            database=config.SNOWFLAKE_DATABASE,
            schema=config.SNOWFLAKE_SCHEMA,
        )
    else:
        raise ValueError(f"Unknown target type: {target_type}")
```

### Step 4: Add Unit Tests

Create `tests/unit/test_target_snowflake.py`:

```python
import pytest
from unittest.mock import MagicMock, patch
from catalog_sync.targets.snowflake import SnowflakeCatalog
from catalog_sync.models import TableInfo, ColumnInfo

@pytest.fixture
def mock_snowflake_connection():
    with patch("snowflake.connector.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor

def test_ensure_schema_creates_database_and_schema(mock_snowflake_connection):
    """Test that ensure_schema creates database and schema"""
    mock_conn, mock_cursor = mock_snowflake_connection
    
    target = SnowflakeCatalog(
        account="test", user="user", password="pass",
        warehouse="wh", database="db", schema="schema"
    )
    
    target.ensure_schema("my_db", "my_schema")
    
    # Assert SQL statements were executed
    assert mock_cursor.execute.call_count >= 2
    # Check for CREATE DATABASE and CREATE SCHEMA

def test_create_table_registers_external_table(mock_snowflake_connection):
    """Test that create_table runs CREATE EXTERNAL TABLE"""
    mock_conn, mock_cursor = mock_snowflake_connection
    
    target = SnowflakeCatalog(...)
    table = TableInfo(
        name="orders",
        location="s3://my-bucket/tableflow/orders/",
        columns=[ColumnInfo("id", "BIGINT"), ColumnInfo("amount", "DOUBLE")],
        tags={},
        format="DELTA"
    )
    
    target.create_table(table)
    
    # Assert CREATE EXTERNAL TABLE was called
    sql = mock_cursor.execute.call_args[0][0]
    assert "CREATE EXTERNAL TABLE" in sql
    assert "s3://my-bucket/tableflow/orders/" in sql

# Add more tests for list_tables, update_table, delete_table, sync_tags
```

Run tests:
```bash
pytest tests/unit/test_target_snowflake.py -v
```

## Snowflake Reference Example

### Authentication

**Unity Catalog approach** (for reference):
- Uses `databricks-sdk` package
- Authenticates via PAT (token) or service principal (OAuth)
- See: `catalog_sync/targets/unity_catalog.py`

**Snowflake approach**:
- Use `snowflake-connector-python` package (add to `pyproject.toml` dependencies)
- Authenticate via username/password, key-pair, or OAuth
- See: [Snowflake Python Connector docs](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)

Example:
```python
import snowflake.connector

conn = snowflake.connector.connect(
    account="your_account",
    user="your_user",
    password="your_password",  # or use key-pair auth
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema"
)
```

### External Table Syntax

**Unity Catalog** (reference):
```sql
CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`table_name`
USING DELTA
LOCATION 's3://bucket/path/'
COMMENT 's3://bucket/path/'  -- Used for idempotency check
```

**Snowflake Open Catalog** (adapt this):
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS database.schema.table_name
LOCATION = 's3://bucket/path/'
FILE_FORMAT = (TYPE = PARQUET)  -- Delta uses Parquet files
COMMENT = 's3://bucket/path/';  -- Store location for idempotency
```

**Key differences:**
- Snowflake uses `LOCATION =` (not `LOCATION` keyword in `USING` clause)
- Snowflake requires explicit `FILE_FORMAT` (Delta tables use Parquet)
- Snowflake's external tables need a storage integration for cloud credentials

**Snowflake storage integration** (one-time setup):
```sql
CREATE STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/my-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/tableflow/');
```

Reference: [Snowflake External Tables docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-table)

### Tag Mapping

**Unity Catalog** (reference):
```sql
-- Apply tags
ALTER TABLE catalog.schema.table_name
SET TAGS ('PII' = 'true', 'DataOwnership_owner' = 'team');

-- Remove tags
ALTER TABLE catalog.schema.table_name
UNSET TAGS ('PII');

-- Read tags
SELECT tag_name, tag_value
FROM catalog.information_schema.table_tags
WHERE table_name = 'table_name';
```

**Snowflake** (adapt this):
```sql
-- Apply tags
ALTER TABLE database.schema.table_name
SET TAG PII = 'true', DataOwnership_owner = 'team';

-- Remove tags
ALTER TABLE database.schema.table_name
UNSET TAG PII;

-- Read tags
SELECT tag_name, tag_value
FROM database.information_schema.tag_references
WHERE object_name = 'TABLE_NAME';
```

**Key differences:**
- Snowflake uses `SET TAG` (not `SET TAGS`)
- Snowflake tag names are identifiers (not quoted strings)
- Snowflake stores tag references in `information_schema.tag_references`

Reference: [Snowflake Tags docs](https://docs.snowflake.com/en/user-guide/object-tagging)

### API vs SQL

**Unity Catalog approach** (reference):
- Uses Databricks SQL execution via `databricks-sdk`
- All operations are SQL statements executed through a warehouse
- No direct REST API calls to Unity Catalog

**Snowflake options**:
1. **SQL via Python connector** (recommended for simplicity):
   - Use `snowflake.connector` to execute SQL statements
   - Similar pattern to Unity Catalog
   
2. **Snowflake REST API** (alternative):
   - `POST /api/v2/statements` to execute SQL
   - `GET /api/v2/databases/{db}/schemas/{schema}/tables` to list tables
   - More complex, requires OAuth setup

For consistency with Unity Catalog, **use the SQL connector approach**.

### Example Implementation Skeleton

```python
# catalog_sync/targets/snowflake.py

from typing import List, Dict
from catalog_sync.models import TableInfo, ColumnInfo
import snowflake.connector

class SnowflakeCatalog:
    def __init__(self, account: str, user: str, password: str, 
                 warehouse: str, database: str, schema: str):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self._conn = None
    
    def _connect(self):
        if self._conn is None:
            self._conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )
        return self._conn
    
    def ensure_schema(self, catalog: str, schema: str) -> None:
        """Create database and schema if needed"""
        conn = self._connect()
        cursor = conn.cursor()
        
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
        
        # Create schema
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        cursor.close()
    
    def list_tables(self) -> List[TableInfo]:
        """List existing external tables"""
        conn = self._connect()
        cursor = conn.cursor()
        
        # Query information_schema for external tables
        cursor.execute(f"""
            SELECT table_name, comment
            FROM {self.database}.information_schema.tables
            WHERE table_schema = '{self.schema}'
            AND table_type = 'EXTERNAL TABLE'
        """)
        
        tables = []
        for row in cursor:
            table_name, comment = row
            # Parse location from comment (if stored there for idempotency)
            tables.append(TableInfo(
                name=table_name,
                location=comment or "",  # Stored in COMMENT
                columns=[],  # Can query columns from information_schema.columns
                tags={},     # Can query tags from information_schema.tag_references
                format="DELTA"
            ))
        
        cursor.close()
        return tables
    
    def create_table(self, table: TableInfo) -> None:
        """Register external table"""
        conn = self._connect()
        cursor = conn.cursor()
        
        # Build column definitions
        columns = ", ".join([f"{col.name} {col.type}" for col in table.columns])
        
        # Create external table (adapt for Snowflake syntax)
        sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{self.schema}.{table.name}
        ({columns})
        LOCATION = '{table.location}'
        FILE_FORMAT = (TYPE = PARQUET)
        COMMENT = '{table.location}'
        """
        
        cursor.execute(sql)
        cursor.close()
    
    def update_table(self, table: TableInfo) -> None:
        """Update table (DROP + CREATE for location change)"""
        self.delete_table(table.name)
        self.create_table(table)
    
    def delete_table(self, table_name: str) -> None:
        """Drop table (metadata only)"""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {self.database}.{self.schema}.{table_name}")
        cursor.close()
    
    def sync_tags(self, table_name: str, tags: Dict[str, str]) -> None:
        """Apply tags to table"""
        if not tags:
            return
        
        conn = self._connect()
        cursor = conn.cursor()
        
        # Apply tags (Snowflake syntax)
        tag_pairs = ", ".join([f"{key} = '{value}'" for key, value in tags.items()])
        sql = f"ALTER TABLE {self.database}.{self.schema}.{table_name} SET TAG {tag_pairs}"
        cursor.execute(sql)
        cursor.close()
```

**This is a skeleton** — you'll need to:
- Add proper error handling (connection failures, permission errors)
- Implement column discovery for `list_tables()`
- Handle tag manifest tracking (which tags are managed vs user-created)
- Add integration with Snowflake storage integration for cloud credentials
- Test with a real Snowflake account

## Testing Strategy

### Unit Tests

**Mock external dependencies**:
- Mock `snowflake.connector.connect()` to avoid requiring a live Snowflake account
- Mock cursor execution results
- Test that correct SQL statements are generated
- Test error handling (authentication failures, permission errors)

**Example** (see Step 4 above):
```python
@patch("snowflake.connector.connect")
def test_create_table(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    target = SnowflakeCatalog(...)
    target.create_table(TableInfo(...))
    
    # Assert CREATE EXTERNAL TABLE was called
    assert "CREATE EXTERNAL TABLE" in mock_cursor.execute.call_args[0][0]
```

**Reference existing tests**:
- `tests/unit/test_target_unity_catalog.py` — shows how to mock Databricks SDK
- `tests/unit/test_engine.py` — shows how to test the full sync flow

### Integration Tests

**Manual verification with live infrastructure**:
1. Set up a Snowflake account (trial accounts work)
2. Create a storage integration for S3/ADLS/GCS
3. Run the sync against real Tableflow topics
4. Verify tables appear in Snowflake with correct location
5. Query the tables to ensure data is readable
6. Verify tags are applied correctly

**Document what you tested**:
```python
def test_snowflake_integration():
    """
    Integration test with live Snowflake account.
    
    Prerequisites:
    - Snowflake account with storage integration configured
    - Tableflow topics materialized in S3
    - SNOWFLAKE_* env vars set
    
    Verified:
    - Tables created successfully
    - Storage location matches Tableflow path
    - Tags applied correctly
    - SELECT queries return data
    """
    # Test code here (can be run manually, not in CI)
```

## Next Steps

- Review [best-practices.md](best-practices.md) for design principles
- Look at `catalog_sync/targets/unity_catalog.py` as a complete reference implementation
- Review Snowflake docs for external tables, storage integrations, and tag operations
- Start with unit tests, then validate with live Snowflake account
