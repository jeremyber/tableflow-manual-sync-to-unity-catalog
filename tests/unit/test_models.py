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
