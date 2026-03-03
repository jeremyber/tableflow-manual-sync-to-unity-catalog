from __future__ import annotations

import logging

from pyiceberg.catalog import load_catalog

from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.sources.base import CatalogSource

logger = logging.getLogger(__name__)


class IcebergRestSource(CatalogSource):
    """Source that reads from an Iceberg REST Catalog (e.g., Confluent managed catalog).

    NOTE: As of March 2026, Confluent's Iceberg REST Catalog is NOT available
    over private networking. This source is included for future use when
    INIT-6185 (Private Networking for Tableflow REST endpoint) ships.
    For private networking today, use GlueSource or S3DiscoverySource.
    """

    def __init__(self, uri: str, credential: str, warehouse: str) -> None:
        self._catalog = load_catalog(
            "confluent_tableflow",
            **{
                "type": "rest",
                "uri": uri,
                "credential": credential,
                "warehouse": warehouse,
            },
        )

    def list_tables(self) -> list[TableInfo]:
        tables: list[TableInfo] = []

        for ns_tuple in self._catalog.list_namespaces():
            namespace = ns_tuple[0]
            for table_id in self._catalog.list_tables(namespace):
                try:
                    iceberg_table = self._catalog.load_table(table_id)
                    table_info = self._to_table_info(namespace, table_id[1], iceberg_table)
                    tables.append(table_info)
                except Exception:
                    logger.exception("Failed to load table %s", table_id)

        return tables

    def _to_table_info(self, namespace: str, name: str, iceberg_table) -> TableInfo:
        columns = [
            ColumnInfo(
                name=field.name,
                type=str(field.field_type),
                nullable=not field.required,
            )
            for field in iceberg_table.schema().fields
        ]
        return TableInfo(
            namespace=namespace,
            name=name,
            location=iceberg_table.location(),
            columns=columns,
        )
