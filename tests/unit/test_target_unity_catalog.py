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


@patch("catalog_sync.targets.unity_catalog.WorkspaceClient")
def test_register_table_calls_create(mock_ws_cls):
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    target = UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
    )

    target.register_table(_table())

    mock_ws.statement_execution.execute_statement.assert_called_once()
    sql = mock_ws.statement_execution.execute_statement.call_args[1]["statement"]
    assert "CREATE TABLE" in sql
    assert "tf_catalog" in sql
    assert "orders" in sql
    assert "USING ICEBERG" in sql


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
def test_update_table_drops_and_recreates(mock_ws_cls):
    mock_ws = MagicMock()
    mock_ws_cls.return_value = mock_ws

    target = UnityCatalogTarget(
        host="https://ws.databricks.com",
        token="dapi123",
        catalog_name="tf_catalog",
    )

    target.update_table(_table())

    # Should have called execute_statement twice: DROP then CREATE
    assert mock_ws.statement_execution.execute_statement.call_count == 2
    calls = mock_ws.statement_execution.execute_statement.call_args_list
    assert "DROP TABLE" in calls[0][1]["statement"]
    assert "CREATE TABLE" in calls[1][1]["statement"]


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
    assert tables[0].namespace == "default"
