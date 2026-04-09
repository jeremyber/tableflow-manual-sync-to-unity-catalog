from __future__ import annotations

import logging

import requests

from catalog_sync.models import TableInfo, sanitize_tag_key, validate_table_format
from catalog_sync.sources.base import CatalogSource

logger = logging.getLogger(__name__)

BASE_URL = "https://api.confluent.cloud"

# GraphQL query that fetches classification tags and business metadata
# for all kafka_topic entities in a single call. Paginated with limit/offset.
def _graphql_tags_query(limit: int, offset: int) -> str:
    return (
        "{ kafka_topic(limit: %d, offset: %d) "
        "{ qualifiedName tags business_metadata { name value } } }"
        % (limit, offset)
    )

_GRAPHQL_PAGE_SIZE = 500


def _parse_graphql_tags(topic: dict) -> dict[str, str]:
    """Parse tags and business metadata from a GraphQL kafka_topic result.

    Classification tags (topic["tags"]):  ["PII", "PRIVATE"] -> {"PII": "true", ...}
    Business metadata (topic["business_metadata"]):
        [{"name": "BM2.test1", "value": "abc"}] -> {"BM2_test1": "abc"}
    """
    tags: dict[str, str] = {}

    for tag_name in topic.get("tags") or []:
        if tag_name:
            key = sanitize_tag_key(tag_name)
            if key:
                tags[key] = "true"

    for bm in topic.get("business_metadata") or []:
        bm_name = bm.get("name", "")
        bm_value = bm.get("value")
        if bm_name and bm_value is not None:
            key = sanitize_tag_key(bm_name)
            if key:
                tags[key] = str(bm_value)

    return tags


class ConfluentCloudSource(CatalogSource):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        cluster_id: str,
        environment_id: str,
        namespace: str = "default",
        schema_registry_url: str | None = None,
        schema_registry_api_key: str | None = None,
        schema_registry_api_secret: str | None = None,
        sync_tags: bool = False,
    ) -> None:
        self._auth = (api_key, api_secret)
        self._cluster_id = cluster_id
        self._environment_id = environment_id
        self._namespace = namespace
        self._sync_tags = sync_tags

        if sync_tags:
            self._sr_url = schema_registry_url.rstrip("/") if schema_registry_url else ""
            self._sr_auth = (
                schema_registry_api_key or "",
                schema_registry_api_secret or "",
            )

    def _fetch_all_topic_tags(self) -> dict[str, dict[str, str]] | None:
        """Fetch classification tags and business metadata for all
        kafka_topic entities using the Stream Catalog GraphQL API.

        Single paginated query returns both tags and business_metadata
        for every topic, avoiding per-topic REST calls.

        Returns {topic_name: {tag_key: tag_value}}, or None if the
        fetch failed (so callers can distinguish "no tags" from "error").
        """
        all_tags: dict[str, dict[str, str]] = {}
        offset = 0

        try:
            while True:
                resp = requests.post(
                    f"{self._sr_url}/catalog/graphql",
                    auth=self._sr_auth,
                    json={
                        "query": _graphql_tags_query(
                            _GRAPHQL_PAGE_SIZE, offset
                        ),
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                body = resp.json()

                if body.get("errors"):
                    logger.warning(
                        "GraphQL errors fetching tags: %s",
                        body["errors"][0].get("message", ""),
                    )
                    return None

                topics = (body.get("data") or {}).get("kafka_topic") or []
                for topic in topics:
                    qualified_name = topic.get("qualifiedName", "")
                    parts = qualified_name.split(":")
                    if len(parts) < 3:
                        continue

                    entity_cluster_id = parts[1]
                    topic_name = ":".join(parts[2:])

                    if entity_cluster_id != self._cluster_id:
                        continue

                    tags = _parse_graphql_tags(topic)
                    if tags:
                        all_tags[topic_name] = tags

                if len(topics) < _GRAPHQL_PAGE_SIZE:
                    break
                offset += _GRAPHQL_PAGE_SIZE

        except requests.RequestException:
            logger.warning(
                "Failed to fetch tags via GraphQL",
                exc_info=True,
            )
            return None

        return all_tags

    def list_tables(self) -> list[TableInfo]:
        # Fetch all tags in one paginated GraphQL query.
        # If fetch fails, _tag_fetch_failed is set so the engine can skip tag sync.
        self._tag_fetch_failed = False
        if self._sync_tags:
            all_topic_tags_result = self._fetch_all_topic_tags()
            if all_topic_tags_result is None:
                self._tag_fetch_failed = True
                all_topic_tags = {}
                logger.warning("Tag fetch failed — tags will not be populated on tables")
            else:
                all_topic_tags = all_topic_tags_result
                if all_topic_tags:
                    logger.info(
                        "Fetched tags for %d topic(s) via GraphQL",
                        len(all_topic_tags),
                    )
        else:
            all_topic_tags = {}

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
                raw_format = (
                    "DELTA" if "DELTA" in table_formats
                    else table_formats[0].upper()
                )
                try:
                    table_format = validate_table_format(raw_format)
                except ValueError:
                    logger.warning(
                        "Skipping topic '%s' — unsupported format '%s'",
                        topic_name, raw_format,
                    )
                    continue

                tags = all_topic_tags.get(topic_name, {})

                tables.append(TableInfo(
                    namespace=self._namespace,
                    name=topic_name,
                    location=storage_location,
                    table_format=table_format,
                    tags=tags,
                ))

            next_link = body.get("metadata", {}).get("next")
            url = next_link if next_link else None

        logger.info("Discovered %d Tableflow tables from Confluent Cloud API", len(tables))
        return tables
