from __future__ import annotations

import json
import logging

from catalog_sync.config import SyncConfig, SourceType
from catalog_sync.engine import SyncEngine
from catalog_sync.sources.base import CatalogSource
from catalog_sync.sources.glue import GlueSource
from catalog_sync.sources.iceberg_rest import IcebergRestSource
from catalog_sync.sources.s3_discovery import S3DiscoverySource
from catalog_sync.targets.unity_catalog import UnityCatalogTarget

logger = logging.getLogger(__name__)


def build_source(config: SyncConfig) -> CatalogSource:
    if config.source_type == SourceType.GLUE:
        return GlueSource(
            database=config.glue_database,
            region=config.glue_region,
        )
    elif config.source_type == SourceType.S3_DISCOVERY:
        return S3DiscoverySource(
            bucket=config.s3_bucket,
            prefix=config.s3_prefix or "",
        )
    elif config.source_type == SourceType.ICEBERG_REST:
        return IcebergRestSource(
            uri=config.iceberg_rest_uri,
            credential=config.iceberg_rest_credential,
            warehouse=config.iceberg_rest_warehouse,
        )
    else:
        raise ValueError(f"Unsupported source type: {config.source_type}")


def build_target(config: SyncConfig) -> UnityCatalogTarget:
    return UnityCatalogTarget(
        host=config.databricks_host,
        token=config.databricks_token,
        catalog_name=config.target_catalog,
    )


def lambda_handler(event: dict, context) -> dict:
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting catalog sync")

    config = SyncConfig.from_env()
    source = build_source(config)
    target = build_target(config)

    engine = SyncEngine(source, target)
    result = engine.sync()

    body = json.dumps({
        "added": result.added,
        "updated": result.updated,
        "removed": result.removed,
        "total_changes": result.total_changes,
    })

    logger.info("Sync result: %s", body)

    return {
        "statusCode": 200,
        "body": body,
    }
