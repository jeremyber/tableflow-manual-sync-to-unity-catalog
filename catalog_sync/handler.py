"""AWS Lambda entry point — wraps the sync engine for serverless deployment."""

from __future__ import annotations

import json
import logging

from catalog_sync.config import SyncConfig, SourceType, TargetType
from catalog_sync.engine import SyncEngine
from catalog_sync.sources.confluent_cloud import ConfluentCloudSource
from catalog_sync.targets.unity_catalog import UnityCatalogTarget

logger = logging.getLogger(__name__)


def build_source(config: SyncConfig):
    if config.source_type == SourceType.CONFLUENT_API:
        extra = {}
        if config.target_type == TargetType.SNOWFLAKE:
            # Snowflake registers Iceberg; Unity Catalog stays on the Delta default
            extra["preferred_formats"] = ("ICEBERG",)
        return ConfluentCloudSource(
            api_key=config.confluent_api_key,
            api_secret=config.confluent_api_secret,
            cluster_id=config.confluent_cluster_id,
            environment_id=config.confluent_environment_id,
            namespace=config.target_schema,
            schema_registry_url=config.schema_registry_url,
            schema_registry_api_key=config.schema_registry_api_key,
            schema_registry_api_secret=config.schema_registry_api_secret,
            sync_tags=config.sync_tags,
            **extra,
        )
    raise ValueError(f"Unsupported source type: {config.source_type}")


def build_target(config: SyncConfig):
    if config.target_type == TargetType.SNOWFLAKE:
        # Imported lazily so Unity Catalog deployments don't need the extras
        from catalog_sync.iceberg_metadata import StorageMetadataLocator
        from catalog_sync.targets.snowflake import SnowflakeTarget, connect_snowflake

        return SnowflakeTarget(
            connection=connect_snowflake(
                account=config.snowflake_account,
                user=config.snowflake_user,
                role=config.snowflake_role,
                warehouse=config.snowflake_warehouse,
                private_key_path=config.snowflake_private_key_path,
                password=config.snowflake_password,
            ),
            database=config.snowflake_database,
            schema_name=config.target_schema,
            external_volume=config.snowflake_external_volume,
            catalog_integration=config.snowflake_catalog_integration,
            metadata_locator=StorageMetadataLocator(),
            storage_base_path=config.snowflake_storage_base_path,
        )

    return UnityCatalogTarget(
        host=config.databricks_host,
        token=config.databricks_token,
        catalog_name=config.target_catalog,
        warehouse_id=config.databricks_warehouse_id,
        schema_name=config.target_schema,
        client_id=config.databricks_client_id,
        client_secret=config.databricks_client_secret,
    )


def lambda_handler(event: dict, context) -> dict:
    logging.basicConfig(level=logging.INFO)
    config = SyncConfig.from_env()
    engine = SyncEngine(
        build_source(config),
        build_target(config),
        sync_tags=config.sync_tags,
    )
    result = engine.sync()

    return {
        "statusCode": 200,
        "body": json.dumps({
            "added": result.added,
            "updated": result.updated,
            "removed": result.removed,
            "tags_synced": result.tags_synced,
            "total_changes": result.total_changes,
        }),
    }
