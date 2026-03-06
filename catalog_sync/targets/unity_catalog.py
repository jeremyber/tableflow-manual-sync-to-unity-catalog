from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format

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
        schema_name: str = "default",
    ) -> None:
        self._catalog_name = catalog_name
        self._warehouse_id = warehouse_id
        self._schema_name = schema_name
        self._ws = WorkspaceClient(host=host, token=token)
        self._ensure_catalog_and_schema()

    def _ensure_catalog_and_schema(self) -> None:
        logger.info("Ensuring catalog %s and schema %s exist", self._catalog_name, self._schema_name)
        self._execute(f"CREATE CATALOG IF NOT EXISTS `{self._catalog_name}`")
        self._execute(f"CREATE SCHEMA IF NOT EXISTS `{self._catalog_name}`.`{self._schema_name}`")

    def _execute(self, sql: str):
        logger.info("Executing SQL: %s", sql)
        result = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            wait_timeout="30s",
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if result.status and result.status.state and result.status.state.value == "FAILED":
            error_msg = result.status.error.message if result.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {error_msg}")
        return result

    def list_tables(self) -> list[TableInfo]:
        sql = (
            f"SELECT table_schema, table_name, comment "
            f"FROM {self._catalog_name}.information_schema.tables "
            f"WHERE table_type = 'EXTERNAL'"
        )
        result = self._execute(sql)

        tables: list[TableInfo] = []
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                tables.append(TableInfo(
                    namespace=row[0],
                    name=row[1],
                    location=row[2] or "",
                    columns=[],
                ))

        return tables

    def register_table(self, table: TableInfo) -> None:
        columns_part = ""
        if table.columns:
            columns_sql = ", ".join(
                f"`{c.name}` {c.type}" for c in table.columns
            )
            columns_part = f" ({columns_sql})"
        escaped_location = table.location.replace("'", "\\'")
        sql = (
            f"CREATE TABLE IF NOT EXISTS "
            f"`{self._catalog_name}`.`{table.namespace}`.`{table.name}`"
            f"{columns_part} "
            f"USING {table.table_format} "
            f"LOCATION '{table.location}' "
            f"COMMENT '{escaped_location}'"
        )
        logger.info("Registering table: %s.%s", table.namespace, table.name)
        self._execute(sql)

    def update_table(self, table: TableInfo) -> None:
        self.remove_table(table.namespace, table.name)
        self.register_table(table)

    def remove_table(self, namespace: str, name: str) -> None:
        sql = f"DROP TABLE IF EXISTS `{self._catalog_name}`.`{namespace}`.`{name}`"
        logger.info("Removing table: %s.%s", namespace, name)
        self._execute(sql)
