from unittest.mock import MagicMock, patch
from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.targets.unity_catalog import UnityCatalogTarget


def _table(ns="default", name="orders", loc="s3://b/w/default/orders"):
    return TableInfo(
        namespace=ns,
        name=name,
        location=loc,
        columns=[ColumnInfo(name="id", type="long", nullable=False)],
    )


def _make_target(mock_ws_cls):
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    # Ensure execute_statement returns a result with SUCCEEDED state
    def _default_execute(**kwargs):
        result = MagicMock()
        result.status.state.value = "SUCCEEDED"
        result.result = None
        return result

    mock_ws.statement_execution.execute_statement.side_effect = _default_execute

    target = UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
    )
    # Reset call count after init (CREATE CATALOG + CREATE SCHEMA)
    mock_ws.statement_execution.execute_statement.reset_mock()
    mock_ws.statement_execution.execute_statement.side_effect = _default_execute
    return target, mock_ws


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_init_creates_catalog_and_schema(mock_ws_cls):
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    def _default_execute(**kwargs):
        result = MagicMock()
        result.status.state.value = "SUCCEEDED"
        result.result = None
        return result

    mock_ws.statement_execution.execute_statement.side_effect = _default_execute

    UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
    )

    calls = mock_ws.statement_execution.execute_statement.call_args_list
    assert len(calls) == 2
    assert "CREATE CATALOG IF NOT EXISTS" in calls[0][1]["statement"]
    assert "CREATE SCHEMA IF NOT EXISTS" in calls[1][1]["statement"]


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_register_table_calls_create(mock_ws_cls):
    target, mock_ws = _make_target(mock_ws_cls)

    target.register_table(_table())

    mock_ws.statement_execution.execute_statement.assert_called_once()
    sql = mock_ws.statement_execution.execute_statement.call_args[1]["statement"]
    assert "CREATE TABLE" in sql
    assert "tf_catalog" in sql
    assert "orders" in sql
    assert "USING DELTA" in sql
    assert "COMMENT 's3://b/w/default/orders'" in sql


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_register_table_uses_iceberg_format(mock_ws_cls):
    target, mock_ws = _make_target(mock_ws_cls)

    table = TableInfo(
        namespace="default",
        name="events",
        location="s3://b/w/default/events",
        columns=[ColumnInfo(name="id", type="long", nullable=False)],
        table_format="ICEBERG",
    )
    target.register_table(table)

    sql = mock_ws.statement_execution.execute_statement.call_args[1]["statement"]
    assert "USING ICEBERG" in sql


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_remove_table_calls_drop(mock_ws_cls):
    target, mock_ws = _make_target(mock_ws_cls)

    target.remove_table("default", "old_table")

    mock_ws.statement_execution.execute_statement.assert_called_once()
    sql = mock_ws.statement_execution.execute_statement.call_args[1]["statement"]
    assert "DROP TABLE" in sql
    assert "old_table" in sql


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_update_table_drops_and_recreates(mock_ws_cls):
    target, mock_ws = _make_target(mock_ws_cls)

    target.update_table(_table())

    assert mock_ws.statement_execution.execute_statement.call_count == 2
    calls = mock_ws.statement_execution.execute_statement.call_args_list
    assert "DROP TABLE" in calls[0][1]["statement"]
    assert "CREATE TABLE" in calls[1][1]["statement"]


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_list_tables_queries_information_schema(mock_ws_cls):
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    def _execute_side_effect(**kwargs):
        sql = kwargs.get("statement", "")
        result = MagicMock()
        result.status.state.value = "SUCCEEDED"
        if "information_schema" in sql:
            result.result.data_array = [
                ["default", "orders", "s3://b/w/default/orders"],
            ]
        else:
            result.result = None
        return result

    mock_ws.statement_execution.execute_statement.side_effect = _execute_side_effect

    target = UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
        warehouse_id="abc123",
    )

    tables = target.list_tables()
    assert len(tables) == 1
    assert tables[0].name == "orders"
    assert tables[0].namespace == "default"
