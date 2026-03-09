from __future__ import annotations

import logging

import requests

from catalog_sync.models import TableInfo
from catalog_sync.sources.base import CatalogSource

logger = logging.getLogger(__name__)

BASE_URL = "https://api.confluent.cloud"


class ConfluentCloudSource(CatalogSource):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        cluster_id: str,
        environment_id: str,
        namespace: str = "default",
    ) -> None:
        self._auth = (api_key, api_secret)
        self._cluster_id = cluster_id
        self._environment_id = environment_id
        self._namespace = namespace

    def list_tables(self) -> list[TableInfo]:
        tables: list[TableInfo] = []
        url: str | None = (
            f"{BASE_URL}/tableflow/v1/tableflow-topics"
            f"?spec.kafka_cluster={self._cluster_id}"
            f"&environment={self._environment_id}"
        )

        while url:
            resp = requests.get(url, auth=self._auth, timeout=30)
            resp.raise_for_status()
            body = resp.json()

            for topic in body.get("data") or []:
                spec = topic.get("spec", {})
                storage = spec.get("storage", {})
                storage_location = storage.get("table_path")
                if not storage_location:
                    continue

                topic_name = spec.get("display_name", "")

                # Only include topics that Tableflow has fully materialized.
                # Registering a table before materialization causes Databricks
                # to write its own _delta_log, corrupting the Tableflow table.
                phase = topic.get("status", {}).get("phase", "")
                if phase != "RUNNING":
                    logger.info(
                        "Skipping topic '%s' — Tableflow phase is '%s', not RUNNING",
                        topic_name, phase,
                    )
                    continue

                # Prefer DELTA if available, otherwise take the first format
                table_formats = spec.get("table_formats", ["DELTA"])
                table_format = (
                    "DELTA" if "DELTA" in table_formats
                    else table_formats[0].upper()
                )

                tables.append(TableInfo(
                    namespace=self._namespace,
                    name=topic_name,
                    location=storage_location,
                    table_format=table_format,
                ))

            next_link = body.get("metadata", {}).get("next")
            url = next_link if next_link else None

        logger.info("Discovered %d Tableflow tables from Confluent Cloud API", len(tables))
        return tables
