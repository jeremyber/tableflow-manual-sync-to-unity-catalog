# Tableflow Catalog Sync — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Python-based catalog sync engine that reads Tableflow Iceberg catalog metadata over a private network and registers those tables in Databricks Unity Catalog, deployed as an AWS Lambda on a configurable schedule via Terraform.

**Architecture:** A pluggable sync engine with source adapters (Iceberg REST, Glue, S3 discovery) and a Unity Catalog target adapter. The core is cloud-agnostic Python; deployment is per-CSP Terraform. All communication traverses PrivateLink.

**Tech Stack:** Python 3.11, pyiceberg, databricks-sdk, boto3, pytest, Terraform (AWS provider)

---

### Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `catalog_sync/__init__.py`
- Create: `catalog_sync/sources/__init__.py`
- Create: `catalog_sync/targets/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`

**Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "catalog-sync"
version = "0.1.0"
description = "Sync Tableflow Iceberg catalog metadata into Databricks Unity Catalog over private networks"
requires-python = ">=3.11"
dependencies = [
    "pyiceberg>=0.7.0",
    "databricks-sdk>=0.30.0",
    "boto3>=1.34.0",
    "pydantic>=2.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-mock>=3.14.0",
    "moto[glue,s3]>=5.0.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

**Step 2: Create directory structure**

```bash
mkdir -p catalog_sync/sources catalog_sync/targets tests/unit
touch catalog_sync/__init__.py catalog_sync/sources/__init__.py catalog_sync/targets/__init__.py
touch tests/__init__.py tests/unit/__init__.py
```

**Step 3: Create virtual environment and install**

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

**Step 4: Verify pytest runs with no errors**

Run: `pytest -v`
Expected: "no tests ran" with exit code 5 (no tests collected), no import errors

**Step 5: Commit**

```bash
git init
echo -e ".venv/\n__pycache__/\n*.egg-info/\n.pytest_cache/\ndist/\nbuild/" > .gitignore
git add .
git commit -m "chore: project scaffolding with pyproject.toml and directory structure"
```

---

### Task 2: Data Models

**Files:**
- Create: `catalog_sync/models.py`
- Create: `tests/unit/test_models.py`

**Step 1: Write the failing test**

```python
# tests/unit/test_models.py
from catalog_sync.models import ColumnInfo, TableInfo


def test_column_info_creation():
    col = ColumnInfo(name="id", type="long", nullable=False)
    assert col.name == "id"
    assert col.type == "long"
    assert col.nullable is False


def test_column_info_defaults_nullable_true():
    col = ColumnInfo(name="name", type="string")
    assert col.nullable is True


def test_table_info_creation():
    table = TableInfo(
        namespace="default",
        name="orders",
        location="s3://bucket/warehouse/default/orders",
        columns=[
            ColumnInfo(name="id", type="long", nullable=False),
            ColumnInfo(name="product", type="string"),
        ],
    )
    assert table.namespace == "default"
    assert table.name == "orders"
    assert table.location == "s3://bucket/warehouse/default/orders"
    assert len(table.columns) == 2


def test_table_info_full_name():
    table = TableInfo(
        namespace="default",
        name="orders",
        location="s3://bucket/warehouse/default/orders",
        columns=[],
    )
    assert table.full_name == "default.orders"


def test_table_info_equality_by_namespace_and_name():
    t1 = TableInfo(namespace="default", name="orders", location="s3://a", columns=[])
    t2 = TableInfo(namespace="default", name="orders", location="s3://b", columns=[])
    assert t1.full_name == t2.full_name
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError` or `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/models.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str
    nullable: bool = True


@dataclass(frozen=True)
class TableInfo:
    namespace: str
    name: str
    location: str
    columns: list[ColumnInfo] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.namespace}.{self.name}"
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_models.py -v`
Expected: All 5 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/models.py tests/unit/test_models.py
git commit -m "feat: add TableInfo and ColumnInfo data models"
```

---

### Task 3: Source Base Class

**Files:**
- Create: `catalog_sync/sources/base.py`
- Create: `tests/unit/test_source_base.py`

**Step 1: Write the failing test**

```python
# tests/unit/test_source_base.py
import pytest
from catalog_sync.sources.base import CatalogSource


def test_catalog_source_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        CatalogSource()


def test_catalog_source_requires_list_tables():
    class IncompleteSource(CatalogSource):
        pass

    with pytest.raises(TypeError, match="abstract"):
        IncompleteSource()


def test_concrete_source_can_instantiate():
    class ConcreteSource(CatalogSource):
        def list_tables(self) -> list:
            return []

    source = ConcreteSource()
    assert source.list_tables() == []
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_source_base.py -v`
Expected: FAIL — `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/sources/base.py
from __future__ import annotations

from abc import ABC, abstractmethod

from catalog_sync.models import TableInfo


class CatalogSource(ABC):
    @abstractmethod
    def list_tables(self) -> list[TableInfo]:
        """List all tables available in this catalog source."""
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_source_base.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/sources/base.py tests/unit/test_source_base.py
git commit -m "feat: add CatalogSource abstract base class"
```

---

### Task 4: Target Base Class

**Files:**
- Create: `catalog_sync/targets/base.py`
- Create: `tests/unit/test_target_base.py`

**Step 1: Write the failing test**

```python
# tests/unit/test_target_base.py
import pytest
from catalog_sync.targets.base import CatalogTarget


def test_catalog_target_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        CatalogTarget()


def test_concrete_target_can_instantiate():
    class ConcreteTarget(CatalogTarget):
        def list_tables(self) -> list:
            return []

        def register_table(self, table) -> None:
            pass

        def update_table(self, table) -> None:
            pass

        def remove_table(self, namespace: str, name: str) -> None:
            pass

    target = ConcreteTarget()
    assert target.list_tables() == []
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_target_base.py -v`
Expected: FAIL — `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/targets/base.py
from __future__ import annotations

from abc import ABC, abstractmethod

from catalog_sync.models import TableInfo


class CatalogTarget(ABC):
    @abstractmethod
    def list_tables(self) -> list[TableInfo]:
        """List all tables currently registered in this target."""

    @abstractmethod
    def register_table(self, table: TableInfo) -> None:
        """Register a new external Iceberg table."""

    @abstractmethod
    def update_table(self, table: TableInfo) -> None:
        """Update an existing table registration (e.g., schema change)."""

    @abstractmethod
    def remove_table(self, namespace: str, name: str) -> None:
        """Remove a table registration."""
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_target_base.py -v`
Expected: All 2 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/targets/base.py tests/unit/test_target_base.py
git commit -m "feat: add CatalogTarget abstract base class"
```

---

### Task 5: Sync Engine (Core Orchestration)

**Files:**
- Create: `catalog_sync/engine.py`
- Create: `tests/unit/test_engine.py`

**Step 1: Write the failing tests**

```python
# tests/unit/test_engine.py
from catalog_sync.engine import SyncEngine, SyncResult
from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.sources.base import CatalogSource
from catalog_sync.targets.base import CatalogTarget


class FakeSource(CatalogSource):
    def __init__(self, tables: list[TableInfo]):
        self._tables = tables

    def list_tables(self) -> list[TableInfo]:
        return self._tables


class FakeTarget(CatalogTarget):
    def __init__(self, tables: list[TableInfo] | None = None):
        self._tables = {t.full_name: t for t in (tables or [])}
        self.registered: list[TableInfo] = []
        self.updated: list[TableInfo] = []
        self.removed: list[str] = []

    def list_tables(self) -> list[TableInfo]:
        return list(self._tables.values())

    def register_table(self, table: TableInfo) -> None:
        self.registered.append(table)

    def update_table(self, table: TableInfo) -> None:
        self.updated.append(table)

    def remove_table(self, namespace: str, name: str) -> None:
        self.removed.append(f"{namespace}.{name}")


def _table(ns: str, name: str, loc: str = "s3://b/w", cols: list | None = None) -> TableInfo:
    return TableInfo(
        namespace=ns,
        name=name,
        location=loc,
        columns=cols or [ColumnInfo(name="id", type="long")],
    )


def test_sync_registers_new_tables():
    source = FakeSource([_table("default", "orders")])
    target = FakeTarget()
    engine = SyncEngine(source, target)

    result = engine.sync()

    assert len(target.registered) == 1
    assert target.registered[0].name == "orders"
    assert result.added == 1
    assert result.updated == 0
    assert result.removed == 0


def test_sync_removes_stale_tables():
    source = FakeSource([])
    target = FakeTarget([_table("default", "old_table")])
    engine = SyncEngine(source, target)

    result = engine.sync()

    assert len(target.removed) == 1
    assert target.removed[0] == "default.old_table"
    assert result.removed == 1


def test_sync_updates_tables_with_changed_columns():
    old = _table("default", "orders", cols=[ColumnInfo(name="id", type="long")])
    new = _table(
        "default",
        "orders",
        cols=[ColumnInfo(name="id", type="long"), ColumnInfo(name="total", type="double")],
    )
    source = FakeSource([new])
    target = FakeTarget([old])
    engine = SyncEngine(source, target)

    result = engine.sync()

    assert len(target.updated) == 1
    assert result.updated == 1
    assert len(target.registered) == 0


def test_sync_skips_unchanged_tables():
    table = _table("default", "orders")
    source = FakeSource([table])
    target = FakeTarget([table])
    engine = SyncEngine(source, target)

    result = engine.sync()

    assert len(target.registered) == 0
    assert len(target.updated) == 0
    assert len(target.removed) == 0
    assert result.added == 0


def test_sync_handles_mixed_operations():
    source = FakeSource([
        _table("default", "orders"),       # unchanged
        _table("default", "customers"),    # new
        _table("default", "products", cols=[ColumnInfo(name="id", type="long"), ColumnInfo(name="sku", type="string")]),  # updated
    ])
    target = FakeTarget([
        _table("default", "orders"),
        _table("default", "products", cols=[ColumnInfo(name="id", type="long")]),
        _table("default", "legacy"),       # to remove
    ])
    engine = SyncEngine(source, target)

    result = engine.sync()

    assert result.added == 1
    assert result.updated == 1
    assert result.removed == 1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_engine.py -v`
Expected: FAIL — `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/engine.py
from __future__ import annotations

import logging
from dataclasses import dataclass

from catalog_sync.models import TableInfo
from catalog_sync.sources.base import CatalogSource
from catalog_sync.targets.base import CatalogTarget

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncResult:
    added: int = 0
    updated: int = 0
    removed: int = 0

    @property
    def total_changes(self) -> int:
        return self.added + self.updated + self.removed


class SyncEngine:
    def __init__(self, source: CatalogSource, target: CatalogTarget) -> None:
        self._source = source
        self._target = target

    def sync(self) -> SyncResult:
        source_tables = {t.full_name: t for t in self._source.list_tables()}
        target_tables = {t.full_name: t for t in self._target.list_tables()}

        source_names = set(source_tables.keys())
        target_names = set(target_tables.keys())

        to_add = source_names - target_names
        to_remove = target_names - source_names
        to_check = source_names & target_names

        added = 0
        for name in to_add:
            table = source_tables[name]
            logger.info("Registering new table: %s at %s", name, table.location)
            self._target.register_table(table)
            added += 1

        updated = 0
        for name in to_check:
            if self._needs_update(source_tables[name], target_tables[name]):
                logger.info("Updating table: %s", name)
                self._target.update_table(source_tables[name])
                updated += 1

        removed = 0
        for name in to_remove:
            table = target_tables[name]
            logger.info("Removing stale table: %s", name)
            self._target.remove_table(table.namespace, table.name)
            removed += 1

        result = SyncResult(added=added, updated=updated, removed=removed)
        logger.info("Sync complete: %s", result)
        return result

    def _needs_update(self, source: TableInfo, target: TableInfo) -> bool:
        return source.columns != target.columns or source.location != target.location
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_engine.py -v`
Expected: All 5 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/engine.py tests/unit/test_engine.py
git commit -m "feat: add SyncEngine with diff-based catalog sync"
```

---

### Task 6: Configuration

**Files:**
- Create: `catalog_sync/config.py`
- Create: `tests/unit/test_config.py`

**Step 1: Write the failing test**

```python
# tests/unit/test_config.py
import os
import pytest
from catalog_sync.config import SyncConfig, SourceType


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("SOURCE_TYPE", "iceberg_rest")
    monkeypatch.setenv("ICEBERG_REST_URI", "https://catalog.confluent.cloud")
    monkeypatch.setenv("ICEBERG_REST_CREDENTIAL", "key:secret")
    monkeypatch.setenv("ICEBERG_REST_WAREHOUSE", "s3://bucket/warehouse")
    monkeypatch.setenv("DATABRICKS_HOST", "https://workspace.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
    monkeypatch.setenv("TARGET_CATALOG", "tableflow_catalog")
    monkeypatch.setenv("TARGET_SCHEMA", "default")

    config = SyncConfig.from_env()

    assert config.source_type == SourceType.ICEBERG_REST
    assert config.iceberg_rest_uri == "https://catalog.confluent.cloud"
    assert config.databricks_host == "https://workspace.databricks.com"
    assert config.target_catalog == "tableflow_catalog"


def test_config_defaults():
    config = SyncConfig(
        source_type=SourceType.ICEBERG_REST,
        databricks_host="https://ws.databricks.com",
        databricks_token="dapi123",
        target_catalog="tf_catalog",
    )
    assert config.target_schema == "default"


def test_source_type_enum():
    assert SourceType.ICEBERG_REST.value == "iceberg_rest"
    assert SourceType.GLUE.value == "glue"
    assert SourceType.S3_DISCOVERY.value == "s3_discovery"


def test_config_missing_required_raises(monkeypatch):
    monkeypatch.delenv("SOURCE_TYPE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("TARGET_CATALOG", raising=False)

    with pytest.raises((KeyError, ValueError)):
        SyncConfig.from_env()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_config.py -v`
Expected: FAIL — `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/config.py
from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum


class SourceType(Enum):
    ICEBERG_REST = "iceberg_rest"
    GLUE = "glue"
    S3_DISCOVERY = "s3_discovery"


@dataclass
class SyncConfig:
    source_type: SourceType
    databricks_host: str
    databricks_token: str
    target_catalog: str
    target_schema: str = "default"

    # Iceberg REST source
    iceberg_rest_uri: str | None = None
    iceberg_rest_credential: str | None = None
    iceberg_rest_warehouse: str | None = None

    # Glue source
    glue_database: str | None = None
    glue_region: str | None = None

    # S3 discovery source
    s3_bucket: str | None = None
    s3_prefix: str | None = None

    @classmethod
    def from_env(cls) -> SyncConfig:
        source_type = SourceType(os.environ["SOURCE_TYPE"])
        return cls(
            source_type=source_type,
            databricks_host=os.environ["DATABRICKS_HOST"],
            databricks_token=os.environ["DATABRICKS_TOKEN"],
            target_catalog=os.environ["TARGET_CATALOG"],
            target_schema=os.environ.get("TARGET_SCHEMA", "default"),
            iceberg_rest_uri=os.environ.get("ICEBERG_REST_URI"),
            iceberg_rest_credential=os.environ.get("ICEBERG_REST_CREDENTIAL"),
            iceberg_rest_warehouse=os.environ.get("ICEBERG_REST_WAREHOUSE"),
            glue_database=os.environ.get("GLUE_DATABASE"),
            glue_region=os.environ.get("GLUE_REGION"),
            s3_bucket=os.environ.get("S3_BUCKET"),
            s3_prefix=os.environ.get("S3_PREFIX", ""),
        )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_config.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/config.py tests/unit/test_config.py
git commit -m "feat: add SyncConfig with env-var loading and SourceType enum"
```

---

### Task 7: Iceberg REST Catalog Source

**Files:**
- Create: `catalog_sync/sources/iceberg_rest.py`
- Create: `tests/unit/test_source_iceberg_rest.py`

**Step 1: Write the failing test**

```python
# tests/unit/test_source_iceberg_rest.py
from unittest.mock import MagicMock, patch
from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.sources.iceberg_rest import IcebergRestSource


def _mock_iceberg_table(namespace, name, location, fields):
    """Create a mock pyiceberg Table object."""
    table = MagicMock()
    table.name.return_value = (namespace, name)
    table.location.return_value = location

    mock_schema = MagicMock()
    mock_fields = []
    for fname, ftype, required in fields:
        f = MagicMock()
        f.name = fname
        f.field_type = MagicMock()
        f.field_type.__str__ = lambda self, t=ftype: t
        f.required = required
        mock_fields.append(f)
    mock_schema.fields = mock_fields
    table.schema.return_value = mock_schema

    return table


@patch("catalog_sync.sources.iceberg_rest.load_catalog")
def test_list_tables_returns_table_info(mock_load_catalog):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    mock_catalog.list_namespaces.return_value = [("default",)]
    mock_catalog.list_tables.return_value = [("default", "orders")]

    mock_table = _mock_iceberg_table(
        "default",
        "orders",
        "s3://bucket/warehouse/default/orders",
        [("id", "long", True), ("product", "string", False)],
    )
    mock_catalog.load_table.return_value = mock_table

    source = IcebergRestSource(
        uri="https://catalog.confluent.cloud",
        credential="key:secret",
        warehouse="s3://bucket/warehouse",
    )

    tables = source.list_tables()

    assert len(tables) == 1
    assert tables[0].namespace == "default"
    assert tables[0].name == "orders"
    assert tables[0].location == "s3://bucket/warehouse/default/orders"
    assert len(tables[0].columns) == 2


@patch("catalog_sync.sources.iceberg_rest.load_catalog")
def test_list_tables_multiple_namespaces(mock_load_catalog):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    mock_catalog.list_namespaces.return_value = [("sales",), ("inventory",)]
    mock_catalog.list_tables.side_effect = [
        [("sales", "orders")],
        [("inventory", "products")],
    ]

    mock_t1 = _mock_iceberg_table("sales", "orders", "s3://b/sales/orders", [("id", "long", True)])
    mock_t2 = _mock_iceberg_table("inventory", "products", "s3://b/inventory/products", [("id", "long", True)])
    mock_catalog.load_table.side_effect = [mock_t1, mock_t2]

    source = IcebergRestSource(
        uri="https://catalog.confluent.cloud",
        credential="key:secret",
        warehouse="s3://bucket/warehouse",
    )

    tables = source.list_tables()
    assert len(tables) == 2


@patch("catalog_sync.sources.iceberg_rest.load_catalog")
def test_list_tables_empty_catalog(mock_load_catalog):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog
    mock_catalog.list_namespaces.return_value = []

    source = IcebergRestSource(uri="https://cat", credential="k:s", warehouse="s3://b")

    assert source.list_tables() == []
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_source_iceberg_rest.py -v`
Expected: FAIL — `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/sources/iceberg_rest.py
from __future__ import annotations

import logging

from pyiceberg.catalog import load_catalog

from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.sources.base import CatalogSource

logger = logging.getLogger(__name__)


class IcebergRestSource(CatalogSource):
    def __init__(self, uri: str, credential: str, warehouse: str) -> None:
        self._uri = uri
        self._credential = credential
        self._warehouse = warehouse
        self._catalog = load_catalog(
            "confluent_tableflow",
            **{
                "type": "rest",
                "uri": uri,
                "credential": credential,
                "warehouse": warehouse,
            },
        )

    def list_tables(self) -> list[TableInfo]:
        tables: list[TableInfo] = []

        for ns_tuple in self._catalog.list_namespaces():
            namespace = ns_tuple[0]
            for table_id in self._catalog.list_tables(namespace):
                try:
                    iceberg_table = self._catalog.load_table(table_id)
                    table_info = self._to_table_info(namespace, table_id[1], iceberg_table)
                    tables.append(table_info)
                except Exception:
                    logger.exception("Failed to load table %s", table_id)

        return tables

    def _to_table_info(self, namespace: str, name: str, iceberg_table) -> TableInfo:
        columns = [
            ColumnInfo(
                name=field.name,
                type=str(field.field_type),
                nullable=not field.required,
            )
            for field in iceberg_table.schema().fields
        ]
        return TableInfo(
            namespace=namespace,
            name=name,
            location=iceberg_table.location(),
            columns=columns,
        )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_source_iceberg_rest.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/sources/iceberg_rest.py tests/unit/test_source_iceberg_rest.py
git commit -m "feat: add IcebergRestSource using pyiceberg REST catalog"
```

---

### Task 8: S3 Discovery Source (Fallback)

**Files:**
- Create: `catalog_sync/sources/s3_discovery.py`
- Create: `tests/unit/test_source_s3_discovery.py`

**Step 1: Write the failing test**

```python
# tests/unit/test_source_s3_discovery.py
import json
from unittest.mock import MagicMock, patch
from catalog_sync.sources.s3_discovery import S3DiscoverySource


def _metadata_json(namespace, name, location, schema_fields):
    """Create a minimal Iceberg metadata.json content."""
    return json.dumps({
        "format-version": 2,
        "location": location,
        "current-schema-id": 0,
        "schemas": [{
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {"id": i + 1, "name": f["name"], "type": f["type"], "required": f.get("required", False)}
                for i, f in enumerate(schema_fields)
            ],
        }],
    }).encode()


@patch("catalog_sync.sources.s3_discovery.boto3")
def test_discover_tables_from_s3(mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

    # Simulate S3 listing: find metadata.json files
    mock_s3.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "warehouse/default/orders/metadata/v1.metadata.json"},
                {"Key": "warehouse/default/orders/metadata/v2.metadata.json"},
                {"Key": "warehouse/default/customers/metadata/v1.metadata.json"},
            ]
        }
    ]

    # Return the latest metadata.json for each table
    def get_object(Bucket, Key):
        if Key == "warehouse/default/orders/metadata/v2.metadata.json":
            return {"Body": MagicMock(read=lambda: _metadata_json(
                "default", "orders", "s3://bucket/warehouse/default/orders",
                [{"name": "id", "type": "long", "required": True}],
            ))}
        elif Key == "warehouse/default/customers/metadata/v1.metadata.json":
            return {"Body": MagicMock(read=lambda: _metadata_json(
                "default", "customers", "s3://bucket/warehouse/default/customers",
                [{"name": "id", "type": "long", "required": True}, {"name": "email", "type": "string"}],
            ))}
        raise Exception(f"Unexpected key: {Key}")

    mock_s3.get_object.side_effect = get_object

    source = S3DiscoverySource(bucket="bucket", prefix="warehouse/")
    tables = source.list_tables()

    assert len(tables) == 2
    names = {t.name for t in tables}
    assert names == {"orders", "customers"}


@patch("catalog_sync.sources.s3_discovery.boto3")
def test_discover_empty_bucket(mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    source = S3DiscoverySource(bucket="bucket", prefix="warehouse/")
    assert source.list_tables() == []
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_source_s3_discovery.py -v`
Expected: FAIL — `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/sources/s3_discovery.py
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict

import boto3

from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.sources.base import CatalogSource

logger = logging.getLogger(__name__)

METADATA_PATTERN = re.compile(r"^(.+)/metadata/(v\d+\.metadata\.json)$")


class S3DiscoverySource(CatalogSource):
    def __init__(self, bucket: str, prefix: str = "") -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._s3 = boto3.client("s3")

    def list_tables(self) -> list[TableInfo]:
        metadata_files = self._find_metadata_files()
        tables: list[TableInfo] = []

        for table_path, latest_key in metadata_files.items():
            try:
                table_info = self._load_table_from_metadata(latest_key, table_path)
                tables.append(table_info)
            except Exception:
                logger.exception("Failed to parse metadata at %s", latest_key)

        return tables

    def _find_metadata_files(self) -> dict[str, str]:
        """Find the latest metadata.json for each table path."""
        table_metadata: dict[str, list[str]] = defaultdict(list)

        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []):
                match = METADATA_PATTERN.match(obj["Key"])
                if match:
                    table_path = match.group(1)
                    table_metadata[table_path].append(obj["Key"])

        # Return the latest metadata file (highest version) for each table
        latest: dict[str, str] = {}
        for table_path, keys in table_metadata.items():
            latest[table_path] = sorted(keys)[-1]

        return latest

    def _load_table_from_metadata(self, key: str, table_path: str) -> TableInfo:
        response = self._s3.get_object(Bucket=self._bucket, Key=key)
        metadata = json.loads(response["Body"].read())

        current_schema_id = metadata.get("current-schema-id", 0)
        schema = next(
            s for s in metadata["schemas"] if s["schema-id"] == current_schema_id
        )

        # Infer namespace and name from table path
        parts = table_path.rstrip("/").split("/")
        if self._prefix:
            prefix_parts = self._prefix.rstrip("/").split("/")
            parts = parts[len(prefix_parts):]

        namespace = parts[0] if len(parts) >= 2 else "default"
        name = parts[1] if len(parts) >= 2 else parts[0]

        columns = [
            ColumnInfo(
                name=f["name"],
                type=f["type"] if isinstance(f["type"], str) else str(f["type"]),
                nullable=not f.get("required", False),
            )
            for f in schema["fields"]
        ]

        return TableInfo(
            namespace=namespace,
            name=name,
            location=f"s3://{self._bucket}/{table_path}",
            columns=columns,
        )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_source_s3_discovery.py -v`
Expected: All 2 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/sources/s3_discovery.py tests/unit/test_source_s3_discovery.py
git commit -m "feat: add S3DiscoverySource for fallback metadata scanning"
```

---

### Task 9: Unity Catalog Target

**Files:**
- Create: `catalog_sync/targets/unity_catalog.py`
- Create: `tests/unit/test_target_unity_catalog.py`

**Step 1: Write the failing test**

```python
# tests/unit/test_target_unity_catalog.py
from unittest.mock import MagicMock, patch, call
from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.targets.unity_catalog import UnityCatalogTarget


def _table(ns="default", name="orders", loc="s3://b/w/default/orders"):
    return TableInfo(
        namespace=ns,
        name=name,
        location=loc,
        columns=[ColumnInfo(name="id", type="long", nullable=False)],
    )


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_register_table_calls_create(mock_ws_cls):
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    target = UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
    )

    table = _table()
    target.register_table(table)

    mock_ws.statement_execution.execute_statement.assert_called_once()
    sql = mock_ws.statement_execution.execute_statement.call_args[1]["statement"]
    assert "CREATE TABLE" in sql
    assert "tf_catalog" in sql
    assert "orders" in sql
    assert "USING iceberg" in sql.lower() or "USING ICEBERG" in sql


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_remove_table_calls_drop(mock_ws_cls):
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    target = UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
    )

    target.remove_table("default", "old_table")

    mock_ws.statement_execution.execute_statement.assert_called_once()
    sql = mock_ws.statement_execution.execute_statement.call_args[1]["statement"]
    assert "DROP TABLE" in sql
    assert "old_table" in sql


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_list_tables_queries_information_schema(mock_ws_cls):
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    mock_result = MagicMock()
    mock_result.result.data_array = [
        ["default", "orders", "s3://b/w/default/orders"],
    ]
    mock_ws.statement_execution.execute_statement.return_value = mock_result

    target = UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
        warehouse_id="abc123",
    )

    tables = target.list_tables()
    assert len(tables) == 1
    assert tables[0].name == "orders"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_target_unity_catalog.py -v`
Expected: FAIL — `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/targets/unity_catalog.py
from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient

from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.targets.base import CatalogTarget

logger = logging.getLogger(__name__)


class UnityCatalogTarget(CatalogTarget):
    def __init__(
        self,
        host: str,
        token: str,
        catalog_name: str,
        warehouse_id: str | None = None,
    ) -> None:
        self._catalog_name = catalog_name
        self._warehouse_id = warehouse_id
        self._ws = WorkspaceClient(host=host, token=token)

    def list_tables(self) -> list[TableInfo]:
        sql = (
            f"SELECT table_schema, table_name, comment "
            f"FROM {self._catalog_name}.information_schema.tables "
            f"WHERE table_type = 'EXTERNAL'"
        )
        result = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
        )

        tables: list[TableInfo] = []
        for row in (result.result.data_array or []):
            tables.append(TableInfo(
                namespace=row[0],
                name=row[1],
                location=row[2] or "",
                columns=[],  # Schema details fetched separately if needed
            ))

        return tables

    def register_table(self, table: TableInfo) -> None:
        columns_sql = ", ".join(
            f"`{c.name}` {c.type}" for c in table.columns
        )
        sql = (
            f"CREATE TABLE IF NOT EXISTS "
            f"`{self._catalog_name}`.`{table.namespace}`.`{table.name}` "
            f"({columns_sql}) "
            f"USING ICEBERG "
            f"LOCATION '{table.location}'"
        )
        logger.info("Registering table: %s", sql)
        self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
        )

    def update_table(self, table: TableInfo) -> None:
        # Drop and recreate — simplest approach for schema changes
        self.remove_table(table.namespace, table.name)
        self.register_table(table)

    def remove_table(self, namespace: str, name: str) -> None:
        sql = f"DROP TABLE IF EXISTS `{self._catalog_name}`.`{namespace}`.`{name}`"
        logger.info("Removing table: %s", sql)
        self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
        )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_target_unity_catalog.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/targets/unity_catalog.py tests/unit/test_target_unity_catalog.py
git commit -m "feat: add UnityCatalogTarget with SQL-based table registration"
```

---

### Task 10: Lambda Handler + Factory Wiring

**Files:**
- Create: `catalog_sync/handler.py`
- Create: `tests/unit/test_handler.py`

**Step 1: Write the failing test**

```python
# tests/unit/test_handler.py
from unittest.mock import patch, MagicMock
from catalog_sync.handler import lambda_handler, build_source, build_target
from catalog_sync.config import SyncConfig, SourceType


def _config(**overrides):
    defaults = dict(
        source_type=SourceType.ICEBERG_REST,
        databricks_host="https://ws.databricks.com",
        databricks_token="dapi123",
        target_catalog="tf_catalog",
        target_schema="default",
        iceberg_rest_uri="https://catalog.confluent.cloud",
        iceberg_rest_credential="key:secret",
        iceberg_rest_warehouse="s3://bucket/warehouse",
    )
    defaults.update(overrides)
    return SyncConfig(**defaults)


@patch("catalog_sync.handler.SyncConfig.from_env")
@patch("catalog_sync.handler.build_source")
@patch("catalog_sync.handler.build_target")
@patch("catalog_sync.handler.SyncEngine")
def test_lambda_handler_runs_sync(mock_engine_cls, mock_build_target, mock_build_source, mock_from_env):
    mock_from_env.return_value = _config()
    mock_engine = MagicMock()
    mock_engine.sync.return_value = MagicMock(added=1, updated=0, removed=0, total_changes=1)
    mock_engine_cls.return_value = mock_engine

    result = lambda_handler({}, None)

    mock_engine.sync.assert_called_once()
    assert result["statusCode"] == 200
    assert "added" in result["body"]


def test_build_source_iceberg_rest():
    config = _config(source_type=SourceType.ICEBERG_REST)
    with patch("catalog_sync.handler.IcebergRestSource") as mock_cls:
        source = build_source(config)
        mock_cls.assert_called_once_with(
            uri="https://catalog.confluent.cloud",
            credential="key:secret",
            warehouse="s3://bucket/warehouse",
        )


def test_build_source_s3_discovery():
    config = _config(source_type=SourceType.S3_DISCOVERY, s3_bucket="mybucket", s3_prefix="warehouse/")
    with patch("catalog_sync.handler.S3DiscoverySource") as mock_cls:
        source = build_source(config)
        mock_cls.assert_called_once_with(bucket="mybucket", prefix="warehouse/")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_handler.py -v`
Expected: FAIL — `ImportError`

**Step 3: Write minimal implementation**

```python
# catalog_sync/handler.py
from __future__ import annotations

import json
import logging

from catalog_sync.config import SyncConfig, SourceType
from catalog_sync.engine import SyncEngine
from catalog_sync.sources.base import CatalogSource
from catalog_sync.sources.iceberg_rest import IcebergRestSource
from catalog_sync.sources.s3_discovery import S3DiscoverySource
from catalog_sync.targets.unity_catalog import UnityCatalogTarget

logger = logging.getLogger(__name__)


def build_source(config: SyncConfig) -> CatalogSource:
    if config.source_type == SourceType.ICEBERG_REST:
        return IcebergRestSource(
            uri=config.iceberg_rest_uri,
            credential=config.iceberg_rest_credential,
            warehouse=config.iceberg_rest_warehouse,
        )
    elif config.source_type == SourceType.S3_DISCOVERY:
        return S3DiscoverySource(
            bucket=config.s3_bucket,
            prefix=config.s3_prefix or "",
        )
    else:
        raise ValueError(f"Unsupported source type: {config.source_type}")


def build_target(config: SyncConfig) -> UnityCatalogTarget:
    return UnityCatalogTarget(
        host=config.databricks_host,
        token=config.databricks_token,
        catalog_name=config.target_catalog,
    )


def lambda_handler(event: dict, context) -> dict:
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting catalog sync")

    config = SyncConfig.from_env()
    source = build_source(config)
    target = build_target(config)

    engine = SyncEngine(source, target)
    result = engine.sync()

    body = json.dumps({
        "added": result.added,
        "updated": result.updated,
        "removed": result.removed,
        "total_changes": result.total_changes,
    })

    logger.info("Sync result: %s", body)

    return {
        "statusCode": 200,
        "body": body,
    }
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_handler.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add catalog_sync/handler.py tests/unit/test_handler.py
git commit -m "feat: add Lambda handler with source/target factory wiring"
```

---

### Task 11: Run All Tests

**Step 1: Run the full test suite**

Run: `pytest tests/ -v --tb=short`
Expected: All tests PASS (approximately 21 tests)

**Step 2: Commit (if any fixes needed)**

---

### Task 12: Terraform — AWS VPC and Networking

**Files:**
- Create: `terraform/aws/versions.tf`
- Create: `terraform/aws/variables.tf`
- Create: `terraform/aws/vpc.tf`
- Create: `terraform/aws/outputs.tf`

**Step 1: Create versions.tf**

```hcl
# terraform/aws/versions.tf
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}
```

**Step 2: Create variables.tf**

```hcl
# terraform/aws/variables.tf
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "tableflow-catalog-sync"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "availability_zones" {
  description = "Availability zones for subnets"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

# Sync configuration
variable "sync_schedule" {
  description = "EventBridge schedule expression (e.g., 'rate(15 minutes)' or 'cron(0/30 * * * ? *)')"
  type        = string
  default     = "rate(15 minutes)"
}

variable "source_type" {
  description = "Catalog source type: iceberg_rest, glue, or s3_discovery"
  type        = string
  default     = "iceberg_rest"
}

# Confluent Cloud
variable "confluent_bootstrap_endpoint" {
  description = "Confluent Cloud bootstrap endpoint for PrivateLink"
  type        = string
  default     = ""
}

variable "confluent_privatelink_service_name" {
  description = "Confluent Cloud PrivateLink service name"
  type        = string
  default     = ""
}

# Iceberg REST catalog
variable "iceberg_rest_uri" {
  description = "URI for the Iceberg REST catalog"
  type        = string
  default     = ""
}

# S3 discovery
variable "iceberg_s3_bucket" {
  description = "S3 bucket containing Iceberg tables (for S3 discovery source)"
  type        = string
  default     = ""
}

variable "iceberg_s3_prefix" {
  description = "S3 prefix within the bucket"
  type        = string
  default     = ""
}

# Databricks
variable "databricks_workspace_url" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_privatelink_service_name" {
  description = "Databricks PrivateLink service name (from Databricks account console)"
  type        = string
  default     = ""
}

variable "target_catalog" {
  description = "Unity Catalog catalog name to sync tables into"
  type        = string
  default     = "tableflow_catalog"
}

variable "target_schema" {
  description = "Unity Catalog schema name within the target catalog"
  type        = string
  default     = "default"
}
```

**Step 3: Create vpc.tf**

```hcl
# terraform/aws/vpc.tf
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name    = "${var.project_name}-vpc"
    Project = var.project_name
  }
}

resource "aws_subnet" "private" {
  count             = length(var.private_subnet_cidrs)
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]

  tags = {
    Name    = "${var.project_name}-private-${count.index}"
    Project = var.project_name
  }
}

# S3 Gateway Endpoint (free, no PrivateLink needed)
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.${var.aws_region}.s3"

  tags = {
    Name    = "${var.project_name}-s3-endpoint"
    Project = var.project_name
  }
}

resource "aws_vpc_endpoint_route_table_association" "s3" {
  count           = length(aws_subnet.private)
  route_table_id  = aws_route_table.private.id
  vpc_endpoint_id = aws_vpc_endpoint.s3.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name    = "${var.project_name}-private-rt"
    Project = var.project_name
  }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# Security group for Lambda and VPC endpoints
resource "aws_security_group" "lambda" {
  name_prefix = "${var.project_name}-lambda-"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-lambda-sg"
    Project = var.project_name
  }
}

resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "${var.project_name}-vpce-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  tags = {
    Name    = "${var.project_name}-vpce-sg"
    Project = var.project_name
  }
}

# Secrets Manager VPC Endpoint (for Lambda to fetch secrets)
resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Name    = "${var.project_name}-secretsmanager-endpoint"
    Project = var.project_name
  }
}
```

**Step 4: Create outputs.tf**

```hcl
# terraform/aws/outputs.tf
output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "lambda_security_group_id" {
  description = "Security group ID for Lambda"
  value       = aws_security_group.lambda.id
}
```

**Step 5: Validate Terraform**

Run: `cd terraform/aws && terraform init && terraform validate`
Expected: "Success! The configuration is valid."

**Step 6: Commit**

```bash
git add terraform/
git commit -m "infra: add AWS VPC, subnets, S3 endpoint, and security groups"
```

---

### Task 13: Terraform — Lambda Deployment

**Files:**
- Create: `terraform/aws/lambda.tf`
- Create: `terraform/aws/secrets.tf`
- Create: `terraform/aws/eventbridge.tf`

**Step 1: Create secrets.tf**

```hcl
# terraform/aws/secrets.tf
resource "aws_secretsmanager_secret" "confluent_credentials" {
  name        = "${var.project_name}/confluent-credentials"
  description = "Confluent Cloud API key and secret for catalog sync"

  tags = {
    Project = var.project_name
  }
}

resource "aws_secretsmanager_secret" "databricks_token" {
  name        = "${var.project_name}/databricks-token"
  description = "Databricks personal access token or service principal token"

  tags = {
    Project = var.project_name
  }
}
```

**Step 2: Create lambda.tf**

```hcl
# terraform/aws/lambda.tf
data "aws_caller_identity" "current" {}

resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_role_policy" "lambda" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Resource = [
          aws_secretsmanager_secret.confluent_credentials.arn,
          aws_secretsmanager_secret.databricks_token.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = var.iceberg_s3_bucket != "" ? [
          "arn:aws:s3:::${var.iceberg_s3_bucket}",
          "arn:aws:s3:::${var.iceberg_s3_bucket}/*",
        ] : []
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTables",
          "glue:GetTable",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_lambda_function" "catalog_sync" {
  function_name = "${var.project_name}-sync"
  role          = aws_iam_role.lambda.arn
  handler       = "catalog_sync.handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 256

  # Placeholder — replaced by CI/CD or manual deploy
  filename         = "${path.module}/../../dist/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../dist/lambda.zip")

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      SOURCE_TYPE            = var.source_type
      ICEBERG_REST_URI       = var.iceberg_rest_uri
      ICEBERG_REST_WAREHOUSE = var.iceberg_s3_bucket != "" ? "s3://${var.iceberg_s3_bucket}/${var.iceberg_s3_prefix}" : ""
      S3_BUCKET              = var.iceberg_s3_bucket
      S3_PREFIX              = var.iceberg_s3_prefix
      DATABRICKS_HOST        = var.databricks_workspace_url
      TARGET_CATALOG         = var.target_catalog
      TARGET_SCHEMA          = var.target_schema
      # Secrets fetched at runtime from Secrets Manager
      SECRETS_CONFLUENT_ARN  = aws_secretsmanager_secret.confluent_credentials.arn
      SECRETS_DATABRICKS_ARN = aws_secretsmanager_secret.databricks_token.arn
    }
  }

  tags = {
    Project = var.project_name
  }
}
```

**Step 3: Create eventbridge.tf**

```hcl
# terraform/aws/eventbridge.tf
resource "aws_cloudwatch_event_rule" "sync_schedule" {
  name                = "${var.project_name}-schedule"
  description         = "Trigger catalog sync on a schedule"
  schedule_expression = var.sync_schedule

  tags = {
    Project = var.project_name
  }
}

resource "aws_cloudwatch_event_target" "sync_lambda" {
  rule      = aws_cloudwatch_event_rule.sync_schedule.name
  target_id = "catalog-sync-lambda"
  arn       = aws_lambda_function.catalog_sync.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.catalog_sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sync_schedule.arn
}
```

**Step 4: Validate Terraform**

Run: `cd terraform/aws && terraform validate`
Expected: "Success! The configuration is valid."

Note: `terraform validate` will warn about the missing `dist/lambda.zip`. That's expected — it gets built before deployment.

**Step 5: Commit**

```bash
git add terraform/
git commit -m "infra: add Lambda, IAM, Secrets Manager, and EventBridge schedule"
```

---

### Task 14: Lambda Packaging Script

**Files:**
- Create: `scripts/build_lambda.sh`

**Step 1: Create the build script**

```bash
#!/usr/bin/env bash
# scripts/build_lambda.sh
# Packages the catalog_sync module + dependencies into a Lambda deployment zip.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DIST_DIR="$PROJECT_ROOT/dist"
BUILD_DIR="$PROJECT_ROOT/.build/lambda"

echo "==> Cleaning build directory"
rm -rf "$BUILD_DIR" "$DIST_DIR/lambda.zip"
mkdir -p "$BUILD_DIR" "$DIST_DIR"

echo "==> Installing dependencies"
pip install \
  --target "$BUILD_DIR" \
  --platform manylinux2014_x86_64 \
  --only-binary=:all: \
  --implementation cp \
  --python-version 3.11 \
  pyiceberg databricks-sdk boto3 pydantic 2>/dev/null || \
pip install --target "$BUILD_DIR" pyiceberg databricks-sdk boto3 pydantic

echo "==> Copying source code"
cp -r "$PROJECT_ROOT/catalog_sync" "$BUILD_DIR/"

echo "==> Creating zip"
cd "$BUILD_DIR"
zip -r "$DIST_DIR/lambda.zip" . -x '*.pyc' '__pycache__/*' '*.dist-info/*'

echo "==> Built: $DIST_DIR/lambda.zip ($(du -h "$DIST_DIR/lambda.zip" | cut -f1))"
```

**Step 2: Make executable and test**

Run: `chmod +x scripts/build_lambda.sh && ./scripts/build_lambda.sh`
Expected: Creates `dist/lambda.zip`

**Step 3: Add dist/ to .gitignore**

Append `dist/` and `.build/` to `.gitignore`.

**Step 4: Commit**

```bash
git add scripts/build_lambda.sh .gitignore
git commit -m "chore: add Lambda packaging script"
```

---

### Task 15: Demo Data Producer

**Files:**
- Create: `demo/producer.py`
- Create: `demo/schemas/orders.avsc`
- Create: `demo/schemas/customers.avsc`
- Create: `demo/requirements.txt`

**Step 1: Create Avro schemas**

```json
// demo/schemas/orders.avsc
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.tableflow",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "price", "type": "double"},
    {"name": "order_date", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

```json
// demo/schemas/customers.avsc
{
  "type": "record",
  "name": "Customer",
  "namespace": "com.example.tableflow",
  "fields": [
    {"name": "customer_id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "city", "type": "string"},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

**Step 2: Create producer script**

```python
# demo/producer.py
"""
Sample data producer for Tableflow demo.
Produces orders and customers to Confluent Cloud Kafka topics.

Usage:
  export CONFLUENT_BOOTSTRAP=<bootstrap-server>
  export CONFLUENT_API_KEY=<api-key>
  export CONFLUENT_API_SECRET=<api-secret>
  export SCHEMA_REGISTRY_URL=<sr-url>
  export SCHEMA_REGISTRY_API_KEY=<sr-key>
  export SCHEMA_REGISTRY_API_SECRET=<sr-secret>

  python demo/producer.py --topic orders --count 100
  python demo/producer.py --topic customers --count 50
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

PRODUCTS = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"]
CITIES = ["New York", "San Francisco", "Chicago", "Austin", "Seattle", "Denver"]
NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]


def load_schema(schema_file: str) -> str:
    with open(schema_file) as f:
        return f.read()


def make_producer(topic: str, schema_file: str) -> SerializingProducer:
    sr_client = SchemaRegistryClient({
        "url": os.environ["SCHEMA_REGISTRY_URL"],
        "basic.auth.user.info": f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:{os.environ['SCHEMA_REGISTRY_API_SECRET']}",
    })

    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=load_schema(schema_file),
    )

    return SerializingProducer({
        "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONFLUENT_API_KEY"],
        "sasl.password": os.environ["CONFLUENT_API_SECRET"],
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    })


def generate_order() -> dict:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1, 100):04d}",
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(9.99, 499.99), 2),
        "order_date": int(time.time() * 1000),
    }


def generate_customer() -> dict:
    return {
        "customer_id": f"CUST-{random.randint(1, 100):04d}",
        "name": random.choice(NAMES),
        "email": f"{random.choice(NAMES).lower()}@example.com",
        "city": random.choice(CITIES),
        "created_at": int(time.time() * 1000),
    }


def main():
    parser = argparse.ArgumentParser(description="Produce sample data to Confluent Cloud")
    parser.add_argument("--topic", required=True, choices=["orders", "customers"])
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between messages in seconds")
    args = parser.parse_args()

    schema_file = f"demo/schemas/{args.topic}.avsc"
    generator = generate_order if args.topic == "orders" else generate_customer

    producer = make_producer(args.topic, schema_file)

    print(f"Producing {args.count} records to '{args.topic}'...")
    for i in range(args.count):
        record = generator()
        key = record.get("order_id") or record.get("customer_id")
        producer.produce(topic=args.topic, key=key, value=record)
        if (i + 1) % 10 == 0:
            producer.flush()
            print(f"  Produced {i + 1}/{args.count}")
        time.sleep(args.delay)

    producer.flush()
    print(f"Done. Produced {args.count} records to '{args.topic}'.")


if __name__ == "__main__":
    main()
```

**Step 3: Create requirements.txt**

```
# demo/requirements.txt
confluent-kafka[avro,schemaregistry]>=2.3.0
```

**Step 4: Commit**

```bash
git add demo/
git commit -m "demo: add sample data producer and Avro schemas"
```

---

### Task 16: CLAUDE.md

**Files:**
- Create: `CLAUDE.md`

**Step 1: Create CLAUDE.md**

```markdown
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Catalog sync engine that bridges Confluent Cloud Tableflow (Iceberg tables) to Databricks Unity Catalog over private networks. Tableflow's native Unity Catalog integration can't traverse PrivateLink, so this tool runs inside the customer's VPC/VNet and syncs catalog metadata (no data movement).

## Architecture

- **catalog_sync/**: Cloud-agnostic Python sync engine
  - `sources/`: Pluggable catalog readers (Iceberg REST, Glue, S3 discovery)
  - `targets/`: Catalog writers (Unity Catalog via Databricks SDK)
  - `engine.py`: Diff-based sync orchestration
  - `handler.py`: Lambda/serverless entry point
- **terraform/aws/**: VPC, PrivateLink, Lambda, EventBridge deployment
- **demo/**: Sample Kafka data producer and Avro schemas

## Commands

```bash
# Install
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Tests
pytest tests/ -v                                    # all tests
pytest tests/unit/test_engine.py -v                 # single file
pytest tests/unit/test_engine.py::test_sync_registers_new_tables -v  # single test

# Build Lambda zip
./scripts/build_lambda.sh

# Terraform
cd terraform/aws && terraform init && terraform validate
cd terraform/aws && terraform plan -var-file=demo.tfvars
```

## Key Design Decisions

- Unity Catalog "foreign catalogs" are for RDBMS/JDBC only — Iceberg tables are registered as external tables using `CREATE TABLE ... USING ICEBERG LOCATION`
- Three source strategies handle uncertainty around Confluent REST Catalog private availability: Iceberg REST, Glue, S3 metadata discovery
- Sync is metadata-only — tables registered by S3 location reference, no data copied
```

**Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add CLAUDE.md for Claude Code context"
```

---

Plan complete and saved to `docs/plans/2026-03-02-tableflow-catalog-sync-implementation.md`. Two execution options:

**1. Subagent-Driven (this session)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** — Open a new session with `executing-plans`, batch execution with checkpoints

Which approach?

<function_calls>
<invoke name="AskUserQuestion">
<parameter name="questions">[{"question": "How do you want to execute this plan?", "header": "Execution", "options": [{"label": "Subagent-Driven (this session)", "description": "I dispatch fresh subagents per task right now, with review between tasks. Fast iteration in this conversation."}, {"label": "Parallel Session (separate)", "description": "You open a new Claude Code session and use executing-plans skill. Better for long unattended runs."}], "multiSelect": false}]