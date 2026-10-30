import json

import pytest

from catalog_sync.models import TableInfo
from catalog_sync.targets.snowflake import SnowflakeTarget, snowflake_table_name

LOCATION = "abfss://tableflow@acct.dfs.core.windows.net/org/env/lkc-1/v1/abc"
METADATA = "org/env/lkc-1/v1/abc/metadata/00005-x.metadata.json"


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self.description = None
        self._rows = []

    def execute(self, sql, params=None):
        self._conn.statements.append((sql, params))
        columns, rows = self._conn.responder(sql, params)
        self.description = [(c,) for c in columns] if columns else None
        self._rows = rows

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, responder=None):
        self.statements = []
        self.responder = responder or (lambda sql, params: ([], []))

    def cursor(self):
        return FakeCursor(self)


class FakeLocator:
    def __init__(self, key=METADATA):
        self.key = key

    def latest_metadata_file(self, table_path):
        return self.key


def _target(conn=None, locator=None, base_path=""):
    return SnowflakeTarget(
        connection=conn or FakeConnection(),
        database="TF_DB",
        schema_name="CONFLUENT",
        external_volume="TF_VOLUME",
        catalog_integration="TF_OBJECT_STORE",
        metadata_locator=locator or FakeLocator(),
        storage_base_path=base_path,
    )


def _table(name="stock-trades", fmt="ICEBERG", location=LOCATION):
    return TableInfo(namespace="CONFLUENT", name=name, location=location, table_format=fmt)


def test_snowflake_table_name():
    assert snowflake_table_name("stock-trades.v2") == "STOCK_TRADES_V2"
    assert snowflake_table_name("1orders") == "_1ORDERS"


def test_rejects_unsafe_identifiers():
    with pytest.raises(ValueError):
        SnowflakeTarget(FakeConnection(), "DB; DROP", "S", "V", "C", FakeLocator())


def test_supports_iceberg_only():
    target = _target()
    assert target.supports_format("ICEBERG")
    assert target.supports_format("iceberg")
    assert not target.supports_format("DELTA")


def test_list_tables_returns_only_managed_tables():
    rows = [
        ("STOCK_TRADES", f"catalog-sync topic=stock-trades location={LOCATION}"),
        ("HAND_MADE", "created by a human"),
        ("NO_COMMENT", None),
    ]
    conn = FakeConnection(lambda sql, params: (["name", "comment"], rows))

    tables = _target(conn).list_tables()

    assert conn.statements[0][0] == "SHOW ICEBERG TABLES IN SCHEMA TF_DB.CONFLUENT"
    assert tables == [TableInfo(namespace="CONFLUENT", name="stock-trades", location=LOCATION,
                                columns=[], table_format="ICEBERG")]


def test_register_table_pins_latest_metadata_file():
    conn = FakeConnection()

    _target(conn).register_table(_table())

    sql, params = conn.statements[0]
    assert sql.startswith("CREATE ICEBERG TABLE IF NOT EXISTS TF_DB.CONFLUENT.STOCK_TRADES ")
    assert "EXTERNAL_VOLUME = 'TF_VOLUME'" in sql
    assert "CATALOG = 'TF_OBJECT_STORE'" in sql
    assert params == (METADATA, f"catalog-sync topic=stock-trades location={LOCATION}")


def test_register_table_relative_to_storage_base_path():
    conn = FakeConnection()

    _target(conn, base_path="/org/env/").register_table(_table())

    assert conn.statements[0][1][0] == "lkc-1/v1/abc/metadata/00005-x.metadata.json"


def test_register_table_rejects_metadata_outside_base_path():
    with pytest.raises(ValueError, match="outside the external volume base"):
        _target(base_path="other").register_table(_table())


def test_register_table_waits_for_first_snapshot():
    conn = FakeConnection()

    _target(conn, locator=FakeLocator(key=None)).register_table(_table())

    assert conn.statements == []


def test_register_table_rejects_delta():
    with pytest.raises(ValueError, match="Unknown table format"):
        _target().register_table(_table(fmt="DELTA"))


def test_refresh_is_noop_when_already_current():
    info = json.dumps({"metadataLocation": f"azure://acct.blob.core.windows.net/tableflow/{METADATA}"})
    conn = FakeConnection(lambda sql, params: (["info"], [(info,)]))

    assert _target(conn).refresh_table(_table()) is False
    assert len(conn.statements) == 1  # only the lookup, no ALTER


def test_refresh_moves_table_to_newest_metadata():
    old = json.dumps({"metadataLocation": "azure://acct.blob.core.windows.net/tableflow/old.metadata.json"})
    conn = FakeConnection(lambda sql, params: (["info"], [(old,)]) if "SYSTEM$" in sql else ([], []))

    assert _target(conn).refresh_table(_table()) is True
    lookup, alter = conn.statements
    assert lookup == ("SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(%s) AS info", ("TF_DB.CONFLUENT.STOCK_TRADES",))
    assert alter == ("ALTER ICEBERG TABLE TF_DB.CONFLUENT.STOCK_TRADES REFRESH %s", (METADATA,))


def test_refresh_without_snapshot_is_noop():
    conn = FakeConnection()
    assert _target(conn, locator=FakeLocator(key=None)).refresh_table(_table()) is False
    assert conn.statements == []


def test_remove_and_update_table():
    conn = FakeConnection()
    target = _target(conn)

    target.update_table(_table())

    assert conn.statements[0][0] == "DROP ICEBERG TABLE IF EXISTS TF_DB.CONFLUENT.STOCK_TRADES"
    assert conn.statements[1][0].startswith("CREATE ICEBERG TABLE IF NOT EXISTS")


def test_sync_tags_not_implemented_yet():
    assert _target().sync_tags(_table()) == 0
