from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


class SourceType(Enum):
    CONFLUENT_API = "confluent_api"


@dataclass
class SyncConfig:
    source_type: SourceType
    databricks_host: str
    databricks_token: str
    target_catalog: str
    target_schema: str = "default"

    # Databricks SQL warehouse
    databricks_warehouse_id: str | None = None

    # Confluent Cloud API source
    confluent_api_key: str | None = None
    confluent_api_secret: str | None = None
    confluent_cluster_id: str | None = None
    confluent_environment_id: str | None = None

    @classmethod
    def from_env(cls) -> SyncConfig:
        source_type = SourceType(os.environ["SOURCE_TYPE"])
        config = cls(
            source_type=source_type,
            databricks_host=os.environ["DATABRICKS_HOST"],
            databricks_token=os.environ["DATABRICKS_TOKEN"],
            target_catalog=os.environ["TARGET_CATALOG"],
            target_schema=os.environ.get("TARGET_SCHEMA", "default"),
            databricks_warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
            confluent_api_key=os.environ.get("CONFLUENT_API_KEY"),
            confluent_api_secret=os.environ.get("CONFLUENT_API_SECRET"),
            confluent_cluster_id=os.environ.get("CONFLUENT_CLUSTER_ID"),
            confluent_environment_id=os.environ.get("CONFLUENT_ENVIRONMENT_ID"),
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
        return config
