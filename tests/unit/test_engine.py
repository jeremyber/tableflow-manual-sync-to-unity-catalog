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


# ── Format filtering + Iceberg refresh ──────────────────────


class IcebergTarget(FakeTarget):
    """Iceberg-only target that records refreshes."""

    def __init__(self, tables=None, refresh_result=True, refresh_error=None):
        super().__init__(tables)
        self.refreshed: list[str] = []
        self._refresh_result = refresh_result
        self._refresh_error = refresh_error

    def supports_format(self, table_format: str) -> bool:
        return table_format == "ICEBERG"

    def refresh_table(self, table: TableInfo) -> bool:
        self.refreshed.append(table.full_name)
        if self._refresh_error and table.name == self._refresh_error:
            raise RuntimeError("catalog unreachable")
        return self._refresh_result


def _iceberg(ns: str, name: str, loc: str = "s3://b/w") -> TableInfo:
    return TableInfo(namespace=ns, name=name, location=loc, columns=[], table_format="ICEBERG")


def test_sync_skips_formats_target_does_not_support():
    source = FakeSource([_table("default", "delta_tbl"), _iceberg("default", "ice_tbl")])
    target = IcebergTarget()

    result = SyncEngine(source, target).sync()

    assert [t.name for t in target.registered] == ["ice_tbl"]
    assert result.added == 1


def test_default_target_still_skips_iceberg():
    source = FakeSource([_iceberg("default", "ice_tbl")])
    target = FakeTarget()

    result = SyncEngine(source, target).sync()

    assert target.registered == []
    assert result.added == 0


def test_sync_refreshes_added_and_unchanged_tables():
    existing = _iceberg("default", "old")
    source = FakeSource([existing, _iceberg("default", "new")])
    target = IcebergTarget(tables=[existing])

    result = SyncEngine(source, target).sync()

    assert target.refreshed == ["default.new", "default.old"]
    assert result.refreshed == 2
    assert result.total_changes == 1  # refreshes are not catalog changes


def test_sync_does_not_count_noop_refreshes():
    existing = _iceberg("default", "old")
    target = IcebergTarget(tables=[existing], refresh_result=False)

    result = SyncEngine(FakeSource([existing]), target).sync()

    assert target.refreshed == ["default.old"]
    assert result.refreshed == 0


def test_refresh_failure_does_not_block_other_tables():
    tables = [_iceberg("default", "a"), _iceberg("default", "b")]
    target = IcebergTarget(tables=tables, refresh_error="a")

    result = SyncEngine(FakeSource(tables), target).sync()

    assert target.refreshed == ["default.a", "default.b"]
    assert result.refreshed == 1


def test_removed_tables_are_not_refreshed():
    target = IcebergTarget(tables=[_iceberg("default", "gone")])

    result = SyncEngine(FakeSource([]), target).sync()

    assert target.removed == ["default.gone"]
    assert target.refreshed == []
    assert result.refreshed == 0
