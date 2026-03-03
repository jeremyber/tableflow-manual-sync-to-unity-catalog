import pytest
from catalog_sync.config import SyncConfig, SourceType


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("SOURCE_TYPE", "iceberg_rest")
    monkeypatch.setenv("ICEBERG_REST_URI", "https://catalog.confluent.cloud")
    monkeypatch.setenv("ICEBERG_REST_CREDENTIAL", "key:secret")
    monkeypatch.setenv("ICEBERG_REST_WAREHOUSE", "s3://bucket/warehouse")
    monkeypatch.setenv("DATABRICKS_HOST", "https://workspace.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
    monkeypatch.setenv("TARGET_CATALOG", "tableflow_catalog")
    monkeypatch.setenv("TARGET_SCHEMA", "default")

    config = SyncConfig.from_env()

    assert config.source_type == SourceType.ICEBERG_REST
    assert config.iceberg_rest_uri == "https://catalog.confluent.cloud"
    assert config.databricks_host == "https://workspace.databricks.com"
    assert config.target_catalog == "tableflow_catalog"


def test_config_defaults():
    config = SyncConfig(
        source_type=SourceType.ICEBERG_REST,
        databricks_host="https://ws.databricks.com",
        databricks_token="dapi123",
        target_catalog="tf_catalog",
    )
    assert config.target_schema == "default"


def test_source_type_enum():
    assert SourceType.ICEBERG_REST.value == "iceberg_rest"
    assert SourceType.GLUE.value == "glue"
    assert SourceType.S3_DISCOVERY.value == "s3_discovery"


def test_config_missing_required_raises(monkeypatch):
    monkeypatch.delenv("SOURCE_TYPE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("TARGET_CATALOG", raising=False)

    with pytest.raises((KeyError, ValueError)):
        SyncConfig.from_env()
