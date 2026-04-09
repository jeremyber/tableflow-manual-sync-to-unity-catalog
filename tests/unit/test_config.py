import pytest
from catalog_sync.config import SyncConfig, SourceType


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("SOURCE_TYPE", "confluent_api")
    monkeypatch.setenv("CONFLUENT_API_KEY", "mykey")
    monkeypatch.setenv("CONFLUENT_API_SECRET", "mysecret")
    monkeypatch.setenv("CONFLUENT_CLUSTER_ID", "lkc-abc")
    monkeypatch.setenv("CONFLUENT_ENVIRONMENT_ID", "env-xyz")
    monkeypatch.setenv("DATABRICKS_HOST", "https://workspace.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
    monkeypatch.setenv("TARGET_CATALOG", "tableflow_catalog")
    monkeypatch.setenv("TARGET_SCHEMA", "default")
    monkeypatch.setenv("SYNC_TAGS", "false")

    config = SyncConfig.from_env()

    assert config.source_type == SourceType.CONFLUENT_API
    assert config.confluent_api_key == "mykey"
    assert config.confluent_cluster_id == "lkc-abc"
    assert config.databricks_host == "https://workspace.databricks.com"
    assert config.target_catalog == "tableflow_catalog"


def test_config_defaults():
    config = SyncConfig(
        source_type=SourceType.CONFLUENT_API,
        databricks_host="https://ws.databricks.com",
        databricks_token="dapi123",
        target_catalog="tf_catalog",
    )
    assert config.target_schema == "default"


def test_source_type_enum():
    assert SourceType.CONFLUENT_API.value == "confluent_api"


def test_config_confluent_api_from_env(monkeypatch):
    monkeypatch.setenv("SOURCE_TYPE", "confluent_api")
    monkeypatch.setenv("CONFLUENT_API_KEY", "mykey")
    monkeypatch.setenv("CONFLUENT_API_SECRET", "mysecret")
    monkeypatch.setenv("CONFLUENT_CLUSTER_ID", "lkc-abc")
    monkeypatch.setenv("CONFLUENT_ENVIRONMENT_ID", "env-xyz")
    monkeypatch.setenv("DATABRICKS_HOST", "https://ws.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
    monkeypatch.setenv("TARGET_CATALOG", "tf_catalog")
    monkeypatch.setenv("SYNC_TAGS", "false")

    config = SyncConfig.from_env()

    assert config.source_type == SourceType.CONFLUENT_API
    assert config.confluent_api_key == "mykey"
    assert config.confluent_cluster_id == "lkc-abc"


def test_config_confluent_api_missing_fields_raises(monkeypatch):
    monkeypatch.setenv("SOURCE_TYPE", "confluent_api")
    monkeypatch.setenv("DATABRICKS_HOST", "https://ws.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
    monkeypatch.setenv("TARGET_CATALOG", "tf_catalog")
    # Missing all CONFLUENT_* env vars

    with pytest.raises(ValueError, match="CONFLUENT_API_KEY"):
        SyncConfig.from_env()


def test_config_missing_required_raises(monkeypatch):
    monkeypatch.delenv("SOURCE_TYPE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("TARGET_CATALOG", raising=False)

    with pytest.raises((KeyError, ValueError)):
        SyncConfig.from_env()
