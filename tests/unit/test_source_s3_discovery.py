import json
from unittest.mock import MagicMock, patch
from catalog_sync.sources.s3_discovery import S3DiscoverySource


def _metadata_json(location, schema_fields):
    """Create minimal Iceberg metadata.json content."""
    return json.dumps({
        "format-version": 2,
        "location": location,
        "current-schema-id": 0,
        "schemas": [{
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {
                    "id": i + 1,
                    "name": f["name"],
                    "type": f["type"],
                    "required": f.get("required", False),
                }
                for i, f in enumerate(schema_fields)
            ],
        }],
    }).encode()


@patch("catalog_sync.sources.s3_discovery.boto3")
def test_discover_tables_from_s3(mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

    mock_s3.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "warehouse/default/orders/metadata/v1.metadata.json"},
                {"Key": "warehouse/default/orders/metadata/v2.metadata.json"},
                {"Key": "warehouse/default/customers/metadata/v1.metadata.json"},
            ]
        }
    ]

    def get_object(Bucket, Key):
        if Key == "warehouse/default/orders/metadata/v2.metadata.json":
            return {"Body": MagicMock(read=lambda: _metadata_json(
                "s3://bucket/warehouse/default/orders",
                [{"name": "id", "type": "long", "required": True}],
            ))}
        elif Key == "warehouse/default/customers/metadata/v1.metadata.json":
            return {"Body": MagicMock(read=lambda: _metadata_json(
                "s3://bucket/warehouse/default/customers",
                [
                    {"name": "id", "type": "long", "required": True},
                    {"name": "email", "type": "string"},
                ],
            ))}
        raise Exception(f"Unexpected key: {Key}")

    mock_s3.get_object.side_effect = get_object

    source = S3DiscoverySource(bucket="bucket", prefix="warehouse/")
    tables = source.list_tables()

    assert len(tables) == 2
    names = {t.name for t in tables}
    assert names == {"orders", "customers"}


@patch("catalog_sync.sources.s3_discovery.boto3")
def test_picks_latest_metadata_version(mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

    mock_s3.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "wh/ns/t1/metadata/v1.metadata.json"},
                {"Key": "wh/ns/t1/metadata/v2.metadata.json"},
                {"Key": "wh/ns/t1/metadata/v3.metadata.json"},
            ]
        }
    ]

    mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: _metadata_json(
        "s3://bucket/wh/ns/t1",
        [{"name": "id", "type": "long", "required": True}],
    ))}

    source = S3DiscoverySource(bucket="bucket", prefix="wh/")
    source.list_tables()

    # Should only read v3 (latest)
    mock_s3.get_object.assert_called_once_with(
        Bucket="bucket", Key="wh/ns/t1/metadata/v3.metadata.json"
    )


@patch("catalog_sync.sources.s3_discovery.boto3")
def test_discover_empty_bucket(mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    source = S3DiscoverySource(bucket="bucket", prefix="warehouse/")
    assert source.list_tables() == []
