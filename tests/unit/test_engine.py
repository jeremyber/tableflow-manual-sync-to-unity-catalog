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

    def sync_tags(self, table: TableInfo) -> int:
        return 0


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
        _table("default", "orders"),
        _table("default", "customers"),
        _table(
            "default",
            "products",
            cols=[ColumnInfo(name="id", type="long"), ColumnInfo(name="sku", type="string")],
        ),
    ])
    target = FakeTarget([
        _table("default", "orders"),
        _table("default", "products", cols=[ColumnInfo(name="id", type="long")]),
        _table("default", "legacy"),
    ])
    engine = SyncEngine(source, target)

    result = engine.sync()

    assert result.added == 1
    assert result.updated == 1
    assert result.removed == 1
