from unittest.mock import MagicMock, patch
from catalog_sync.sources.iceberg_rest import IcebergRestSource


def _mock_iceberg_table(namespace, name, location, fields):
    """Create a mock pyiceberg Table object."""
    table = MagicMock()
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
        "default", "orders", "s3://bucket/warehouse/default/orders",
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
