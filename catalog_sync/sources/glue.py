from __future__ import annotations

import logging

import boto3

from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.sources.base import CatalogSource

logger = logging.getLogger(__name__)

# Map Glue/Hive types to Iceberg-style type names
GLUE_TYPE_MAP = {
    "int": "int",
    "bigint": "long",
    "float": "float",
    "double": "double",
    "string": "string",
    "boolean": "boolean",
    "binary": "binary",
    "timestamp": "timestamp",
    "date": "date",
    "decimal": "decimal",
}


class GlueSource(CatalogSource):
    def __init__(self, database: str, region: str | None = None) -> None:
        self._database = database
        self._glue = boto3.client("glue", region_name=region) if region else boto3.client("glue")

    def list_tables(self) -> list[TableInfo]:
        tables: list[TableInfo] = []
        paginator = self._glue.get_paginator("get_tables")

        for page in paginator.paginate(DatabaseName=self._database):
            for glue_table in page.get("TableList", []):
                try:
                    table_info = self._to_table_info(glue_table)
                    if table_info:
                        tables.append(table_info)
                except Exception:
                    logger.exception("Failed to process Glue table %s", glue_table.get("Name"))

        return tables

    def _to_table_info(self, glue_table: dict) -> TableInfo | None:
        # Only process Iceberg tables
        params = glue_table.get("Parameters", {})
        table_type = params.get("table_type", "")
        if table_type.upper() != "ICEBERG":
            logger.debug("Skipping non-Iceberg table: %s (type=%s)", glue_table["Name"], table_type)
            return None

        location = glue_table.get("StorageDescriptor", {}).get("Location", "")
        if not location:
            logger.warning("Skipping table %s: no location", glue_table["Name"])
            return None

        columns = [
            ColumnInfo(
                name=col["Name"],
                type=GLUE_TYPE_MAP.get(col["Type"].lower(), col["Type"]),
                nullable=True,  # Glue doesn't track nullability in the same way
            )
            for col in glue_table.get("StorageDescriptor", {}).get("Columns", [])
        ]

        return TableInfo(
            namespace=self._database,
            name=glue_table["Name"],
            location=location,
            columns=columns,
        )
