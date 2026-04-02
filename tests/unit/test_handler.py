from unittest.mock import patch, MagicMock
from catalog_sync.handler import lambda_handler, build_source, build_target
from catalog_sync.config import SyncConfig, SourceType


def _config(**overrides):
    defaults = dict(
        source_type=SourceType.CONFLUENT_API,
        databricks_host="https://ws.databricks.com",
        databricks_token="dapi123",
        target_catalog="tf_catalog",
        target_schema="default",
        confluent_api_key="key",
        confluent_api_secret="secret",
        confluent_cluster_id="lkc-abc",
        confluent_environment_id="env-xyz",
        sync_tags=False,
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
    mock_engine.sync.return_value = MagicMock(
        added=1, updated=0, removed=0, tags_synced=0, total_changes=1,
    )
    mock_engine_cls.return_value = mock_engine

    result = lambda_handler({}, None)

    mock_engine.sync.assert_called_once()
    assert result["statusCode"] == 200
    assert "added" in result["body"]


def test_build_source_confluent_api():
    config = _config()
    with patch("catalog_sync.handler.ConfluentCloudSource") as mock_cls:
        build_source(config)
        mock_cls.assert_called_once_with(
            api_key="key",
            api_secret="secret",
            cluster_id="lkc-abc",
            environment_id="env-xyz",
            namespace="default",
            schema_registry_url=None,
            schema_registry_api_key=None,
            schema_registry_api_secret=None,
            sync_tags=False,
        )


def test_build_target():
    config = _config()
    with patch("catalog_sync.handler.UnityCatalogTarget") as mock_cls:
        build_target(config)
        mock_cls.assert_called_once_with(
            host="https://ws.databricks.com",
            token="dapi123",
            catalog_name="tf_catalog",
            warehouse_id=None,
            schema_name="default",
        )
