from __future__ import annotations

import json
import logging
import re
from collections import defaultdict

import boto3

from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.sources.base import CatalogSource

logger = logging.getLogger(__name__)

METADATA_PATTERN = re.compile(r"^(.+)/metadata/(v\d+\.metadata\.json)$")


class S3DiscoverySource(CatalogSource):
    def __init__(self, bucket: str, prefix: str = "") -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._s3 = boto3.client("s3")

    def list_tables(self) -> list[TableInfo]:
        metadata_files = self._find_metadata_files()
        tables: list[TableInfo] = []

        for table_path, latest_key in metadata_files.items():
            try:
                table_info = self._load_table_from_metadata(latest_key, table_path)
                tables.append(table_info)
            except Exception:
                logger.exception("Failed to parse metadata at %s", latest_key)

        return tables

    def _find_metadata_files(self) -> dict[str, str]:
        """Find the latest metadata.json for each table path."""
        table_metadata: dict[str, list[str]] = defaultdict(list)

        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []):
                match = METADATA_PATTERN.match(obj["Key"])
                if match:
                    table_path = match.group(1)
                    table_metadata[table_path].append(obj["Key"])

        # Return the latest metadata file (highest version) for each table
        latest: dict[str, str] = {}
        for table_path, keys in table_metadata.items():
            latest[table_path] = sorted(keys)[-1]

        return latest

    def _load_table_from_metadata(self, key: str, table_path: str) -> TableInfo:
        response = self._s3.get_object(Bucket=self._bucket, Key=key)
        metadata = json.loads(response["Body"].read())

        current_schema_id = metadata.get("current-schema-id", 0)
        schema = next(
            s for s in metadata["schemas"] if s["schema-id"] == current_schema_id
        )

        # Infer namespace and name from table path
        parts = table_path.rstrip("/").split("/")
        if self._prefix:
            prefix_parts = self._prefix.rstrip("/").split("/")
            parts = parts[len(prefix_parts):]

        namespace = parts[0] if len(parts) >= 2 else "default"
        name = parts[1] if len(parts) >= 2 else parts[0]

        columns = [
            ColumnInfo(
                name=f["name"],
                type=f["type"] if isinstance(f["type"], str) else str(f["type"]),
                nullable=not f.get("required", False),
            )
            for f in schema["fields"]
        ]

        return TableInfo(
            namespace=namespace,
            name=name,
            location=f"s3://{self._bucket}/{table_path}",
            columns=columns,
        )
