from unittest.mock import patch, MagicMock
from catalog_sync.handler import lambda_handler, build_source, build_target
from catalog_sync.config import SyncConfig, SourceType


def _config(**overrides):
    defaults = dict(
        source_type=SourceType.GLUE,
        databricks_host="https://ws.databricks.com",
        databricks_token="dapi123",
        target_catalog="tf_catalog",
        target_schema="default",
        glue_database="tableflow_db",
        glue_region="us-east-1",
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


def test_build_source_glue():
    config = _config(source_type=SourceType.GLUE, glue_database="my_db", glue_region="us-west-2")
    with patch("catalog_sync.handler.GlueSource") as mock_cls:
        build_source(config)
        mock_cls.assert_called_once_with(database="my_db", region="us-west-2")


def test_build_source_s3_discovery():
    config = _config(source_type=SourceType.S3_DISCOVERY, s3_bucket="mybucket", s3_prefix="warehouse/")
    with patch("catalog_sync.handler.S3DiscoverySource") as mock_cls:
        build_source(config)
        mock_cls.assert_called_once_with(bucket="mybucket", prefix="warehouse/")


def test_build_source_iceberg_rest():
    config = _config(
        source_type=SourceType.ICEBERG_REST,
        iceberg_rest_uri="https://cat",
        iceberg_rest_credential="k:s",
        iceberg_rest_warehouse="s3://b",
    )
    with patch("catalog_sync.handler.IcebergRestSource") as mock_cls:
        build_source(config)
        mock_cls.assert_called_once_with(uri="https://cat", credential="k:s", warehouse="s3://b")


def test_build_target():
    config = _config()
    with patch("catalog_sync.handler.UnityCatalogTarget") as mock_cls:
        build_target(config)
        mock_cls.assert_called_once_with(
            host="https://ws.databricks.com",
            token="dapi123",
            catalog_name="tf_catalog",
        )
