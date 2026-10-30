import pytest
from catalog_sync.config import SyncConfig, SourceType, TargetType


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


def _snowflake_env(monkeypatch):
    monkeypatch.setenv("SOURCE_TYPE", "confluent_api")
    monkeypatch.setenv("TARGET_TYPE", "snowflake")
    monkeypatch.setenv("CONFLUENT_API_KEY", "mykey")
    monkeypatch.setenv("CONFLUENT_API_SECRET", "mysecret")
    monkeypatch.setenv("CONFLUENT_CLUSTER_ID", "lkc-abc")
    monkeypatch.setenv("CONFLUENT_ENVIRONMENT_ID", "env-xyz")
    monkeypatch.setenv("SYNC_TAGS", "false")
    monkeypatch.setenv("TARGET_SCHEMA", "CONFLUENT")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "org-acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "SYNC_USER")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/keys/sync.p8")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "TF_DB")
    monkeypatch.setenv("SNOWFLAKE_EXTERNAL_VOLUME", "TF_VOLUME")
    monkeypatch.setenv("SNOWFLAKE_CATALOG_INTEGRATION", "TF_OBJECT_STORE")
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("TARGET_CATALOG", raising=False)


def test_config_snowflake_target_without_databricks(monkeypatch):
    _snowflake_env(monkeypatch)

    config = SyncConfig.from_env()

    assert config.target_type == TargetType.SNOWFLAKE
    assert config.databricks_host is None
    assert config.snowflake_database == "TF_DB"
    assert config.snowflake_storage_base_path == ""


def test_config_snowflake_requires_credentials(monkeypatch):
    _snowflake_env(monkeypatch)
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    monkeypatch.delenv("SNOWFLAKE_DATABASE")

    with pytest.raises(ValueError, match="SNOWFLAKE_DATABASE.*SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD"):
        SyncConfig.from_env()


def test_target_type_defaults_to_unity_catalog():
    config = SyncConfig(
        source_type=SourceType.CONFLUENT_API,
        databricks_host="https://ws.databricks.com",
        databricks_token="dapi123",
        target_catalog="tf_catalog",
    )
    assert config.target_type == TargetType.UNITY_CATALOG
