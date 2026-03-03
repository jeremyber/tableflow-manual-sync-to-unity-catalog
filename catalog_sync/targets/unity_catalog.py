from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient

from catalog_sync.models import ColumnInfo, TableInfo
from catalog_sync.targets.base import CatalogTarget

logger = logging.getLogger(__name__)


class UnityCatalogTarget(CatalogTarget):
    def __init__(
        self,
        host: str,
        token: str,
        catalog_name: str,
        warehouse_id: str | None = None,
    ) -> None:
        self._catalog_name = catalog_name
        self._warehouse_id = warehouse_id
        self._ws = WorkspaceClient(host=host, token=token)

    def list_tables(self) -> list[TableInfo]:
        sql = (
            f"SELECT table_schema, table_name, comment "
            f"FROM {self._catalog_name}.information_schema.tables "
            f"WHERE table_type = 'EXTERNAL'"
        )
        result = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
        )

        tables: list[TableInfo] = []
        for row in (result.result.data_array or []):
            tables.append(TableInfo(
                namespace=row[0],
                name=row[1],
                location=row[2] or "",
                columns=[],
            ))

        return tables

    def register_table(self, table: TableInfo) -> None:
        columns_sql = ", ".join(
            f"`{c.name}` {c.type}" for c in table.columns
        )
        sql = (
            f"CREATE TABLE IF NOT EXISTS "
            f"`{self._catalog_name}`.`{table.namespace}`.`{table.name}` "
            f"({columns_sql}) "
            f"USING ICEBERG "
            f"LOCATION '{table.location}'"
        )
        logger.info("Registering table: %s.%s", table.namespace, table.name)
        self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
        )

    def update_table(self, table: TableInfo) -> None:
        self.remove_table(table.namespace, table.name)
        self.register_table(table)

    def remove_table(self, namespace: str, name: str) -> None:
        sql = f"DROP TABLE IF EXISTS `{self._catalog_name}`.`{namespace}`.`{name}`"
        logger.info("Removing table: %s.%s", namespace, name)
        self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
        )
