from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


class SourceType(Enum):
    ICEBERG_REST = "iceberg_rest"
    GLUE = "glue"
    S3_DISCOVERY = "s3_discovery"


@dataclass
class SyncConfig:
    source_type: SourceType
    databricks_host: str
    databricks_token: str
    target_catalog: str
    target_schema: str = "default"

    # Iceberg REST source (future — not available over PN today)
    iceberg_rest_uri: str | None = None
    iceberg_rest_credential: str | None = None
    iceberg_rest_warehouse: str | None = None

    # Glue source (primary for private networking)
    glue_database: str | None = None
    glue_region: str | None = None

    # S3 discovery source (universal fallback)
    s3_bucket: str | None = None
    s3_prefix: str | None = None

    @classmethod
    def from_env(cls) -> SyncConfig:
        source_type = SourceType(os.environ["SOURCE_TYPE"])
        return cls(
            source_type=source_type,
            databricks_host=os.environ["DATABRICKS_HOST"],
            databricks_token=os.environ["DATABRICKS_TOKEN"],
            target_catalog=os.environ["TARGET_CATALOG"],
            target_schema=os.environ.get("TARGET_SCHEMA", "default"),
            iceberg_rest_uri=os.environ.get("ICEBERG_REST_URI"),
            iceberg_rest_credential=os.environ.get("ICEBERG_REST_CREDENTIAL"),
            iceberg_rest_warehouse=os.environ.get("ICEBERG_REST_WAREHOUSE"),
            glue_database=os.environ.get("GLUE_DATABASE"),
            glue_region=os.environ.get("GLUE_REGION"),
            s3_bucket=os.environ.get("S3_BUCKET"),
            s3_prefix=os.environ.get("S3_PREFIX", ""),
        )
