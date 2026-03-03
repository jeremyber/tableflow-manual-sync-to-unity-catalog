from unittest.mock import MagicMock, patch
from catalog_sync.sources.glue import GlueSource


def _glue_table(name, location, columns, table_type="ICEBERG"):
    return {
        "Name": name,
        "Parameters": {"table_type": table_type},
        "StorageDescriptor": {
            "Location": location,
            "Columns": [{"Name": c[0], "Type": c[1]} for c in columns],
        },
    }


@patch("catalog_sync.sources.glue.boto3")
def test_list_iceberg_tables(mock_boto3):
    mock_glue = MagicMock()
    mock_boto3.client.return_value = mock_glue

    mock_glue.get_paginator.return_value.paginate.return_value = [
        {
            "TableList": [
                _glue_table("orders", "s3://bucket/wh/orders", [("id", "bigint"), ("product", "string")]),
                _glue_table("customers", "s3://bucket/wh/customers", [("id", "bigint"), ("email", "string")]),
            ]
        }
    ]

    source = GlueSource(database="tableflow_db")
    tables = source.list_tables()

    assert len(tables) == 2
    assert tables[0].name == "orders"
    assert tables[0].namespace == "tableflow_db"
    assert tables[0].location == "s3://bucket/wh/orders"
    assert len(tables[0].columns) == 2
    assert tables[0].columns[0].type == "long"  # bigint -> long


@patch("catalog_sync.sources.glue.boto3")
def test_skips_non_iceberg_tables(mock_boto3):
    mock_glue = MagicMock()
    mock_boto3.client.return_value = mock_glue

    mock_glue.get_paginator.return_value.paginate.return_value = [
        {
            "TableList": [
                _glue_table("iceberg_table", "s3://bucket/wh/ice", [("id", "bigint")], table_type="ICEBERG"),
                _glue_table("hive_table", "s3://bucket/wh/hive", [("id", "bigint")], table_type="HIVE"),
                _glue_table("no_type", "s3://bucket/wh/no", [("id", "bigint")], table_type=""),
            ]
        }
    ]

    source = GlueSource(database="tableflow_db")
    tables = source.list_tables()

    assert len(tables) == 1
    assert tables[0].name == "iceberg_table"


@patch("catalog_sync.sources.glue.boto3")
def test_empty_database(mock_boto3):
    mock_glue = MagicMock()
    mock_boto3.client.return_value = mock_glue

    mock_glue.get_paginator.return_value.paginate.return_value = [{"TableList": []}]

    source = GlueSource(database="empty_db")
    assert source.list_tables() == []
