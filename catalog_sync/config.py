from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


class SourceType(Enum):
    CONFLUENT_API = "confluent_api"


class TargetType(Enum):
    UNITY_CATALOG = "unity_catalog"
    SNOWFLAKE = "snowflake"


@dataclass
class SyncConfig:
    source_type: SourceType
    databricks_host: str
    databricks_token: str
    target_catalog: str
    target_schema: str = "default"

    # Databricks service principal (alternative to token)
    databricks_client_id: str | None = None
    databricks_client_secret: str | None = None

    # Databricks SQL warehouse
    databricks_warehouse_id: str | None = None

    # Confluent Cloud API source
    confluent_api_key: str | None = None
    confluent_api_secret: str | None = None
    confluent_cluster_id: str | None = None
    confluent_environment_id: str | None = None

    # Tag sync (Stream Catalog / Schema Registry)
    sync_tags: bool = True
    schema_registry_url: str | None = None
    schema_registry_api_key: str | None = None
    schema_registry_api_secret: str | None = None

    # Target selection (Unity Catalog unless TARGET_TYPE=snowflake)
    target_type: TargetType = TargetType.UNITY_CATALOG

    # Snowflake target (Iceberg via object-store catalog integration)
    snowflake_account: str | None = None
    snowflake_user: str | None = None
    snowflake_private_key_path: str | None = None
    snowflake_password: str | None = None
    snowflake_role: str | None = None
    snowflake_warehouse: str | None = None
    snowflake_database: str | None = None
    snowflake_external_volume: str | None = None
    snowflake_catalog_integration: str | None = None
    snowflake_storage_base_path: str = ""

    @classmethod
    def from_env(cls) -> SyncConfig:
        source_type = SourceType(os.environ["SOURCE_TYPE"])
        target_type = TargetType(os.environ.get("TARGET_TYPE", TargetType.UNITY_CATALOG.value))
        is_uc = target_type == TargetType.UNITY_CATALOG
        config = cls(
            source_type=source_type,
            target_type=target_type,
            databricks_host=os.environ["DATABRICKS_HOST"] if is_uc else os.environ.get("DATABRICKS_HOST"),
            databricks_token=os.environ.get("DATABRICKS_TOKEN"),
            databricks_client_id=os.environ.get("DATABRICKS_CLIENT_ID"),
            databricks_client_secret=os.environ.get("DATABRICKS_CLIENT_SECRET"),
            target_catalog=os.environ["TARGET_CATALOG"] if is_uc else os.environ.get("TARGET_CATALOG"),
            target_schema=os.environ.get("TARGET_SCHEMA", "default"),
            databricks_warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
            confluent_api_key=os.environ.get("CONFLUENT_API_KEY"),
            confluent_api_secret=os.environ.get("CONFLUENT_API_SECRET"),
            confluent_cluster_id=os.environ.get("CONFLUENT_CLUSTER_ID"),
            confluent_environment_id=os.environ.get("CONFLUENT_ENVIRONMENT_ID"),
            sync_tags=os.environ.get("SYNC_TAGS", "true").lower() == "true",
            schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL"),
            schema_registry_api_key=os.environ.get("SCHEMA_REGISTRY_API_KEY"),
            schema_registry_api_secret=os.environ.get("SCHEMA_REGISTRY_API_SECRET"),
            snowflake_account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            snowflake_user=os.environ.get("SNOWFLAKE_USER"),
            snowflake_private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
            snowflake_password=os.environ.get("SNOWFLAKE_PASSWORD"),
            snowflake_role=os.environ.get("SNOWFLAKE_ROLE"),
            snowflake_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            snowflake_database=os.environ.get("SNOWFLAKE_DATABASE"),
            snowflake_external_volume=os.environ.get("SNOWFLAKE_EXTERNAL_VOLUME"),
            snowflake_catalog_integration=os.environ.get("SNOWFLAKE_CATALOG_INTEGRATION"),
            snowflake_storage_base_path=os.environ.get("SNOWFLAKE_STORAGE_BASE_PATH", ""),
        )
        if target_type == TargetType.SNOWFLAKE:
            missing_sf = [
                name for name, val in [
                    ("SNOWFLAKE_ACCOUNT", config.snowflake_account),
                    ("SNOWFLAKE_USER", config.snowflake_user),
                    ("SNOWFLAKE_DATABASE", config.snowflake_database),
                    ("SNOWFLAKE_EXTERNAL_VOLUME", config.snowflake_external_volume),
                    ("SNOWFLAKE_CATALOG_INTEGRATION", config.snowflake_catalog_integration),
                ] if not val
            ]
            if not (config.snowflake_private_key_path or config.snowflake_password):
                missing_sf.append("SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
            if missing_sf:
                raise ValueError(
                    f"{', '.join(missing_sf)} required when TARGET_TYPE is snowflake"
                )
        if config.source_type == SourceType.CONFLUENT_API:
            missing = [
                name for name, val in [
                    ("CONFLUENT_API_KEY", config.confluent_api_key),
                    ("CONFLUENT_API_SECRET", config.confluent_api_secret),
                    ("CONFLUENT_CLUSTER_ID", config.confluent_cluster_id),
                    ("CONFLUENT_ENVIRONMENT_ID", config.confluent_environment_id),
                ] if not val
            ]
            if missing:
                raise ValueError(
                    f"{', '.join(missing)} required when SOURCE_TYPE is confluent_api"
                )
            if config.sync_tags:
                missing_sr = [
                    name for name, val in [
                        ("SCHEMA_REGISTRY_URL", config.schema_registry_url),
                        ("SCHEMA_REGISTRY_API_KEY", config.schema_registry_api_key),
                        ("SCHEMA_REGISTRY_API_SECRET", config.schema_registry_api_secret),
                    ] if not val
                ]
                if missing_sr:
                    raise ValueError(
                        f"{', '.join(missing_sr)} required when SYNC_TAGS is true"
                    )
        return config
