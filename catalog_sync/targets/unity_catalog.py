from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format

from catalog_sync.models import ColumnInfo, TableInfo, MANAGED_TAGS_KEY, TOMBSTONE_VALUE
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

    def _full_table_name(self, namespace: str, name: str) -> str:
        return f"`{self._catalog_name}`.`{namespace}`.`{name}`"

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
            f"{self._full_table_name(table.namespace, table.name)}"
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
        sql = f"DROP TABLE IF EXISTS {self._full_table_name(namespace, name)}"
        logger.info("Removing table: %s.%s", namespace, name)
        self._execute(sql)

    # ── Tag sync ─────────────────────────────────────────────

    def _read_table_tags(self, namespace: str, name: str) -> dict[str, str]:
        """Read all tags currently set on a table via information_schema.table_tags."""
        escaped_ns = namespace.replace("'", "''")
        escaped_name = name.replace("'", "''")
        sql = (
            f"SELECT tag_name, tag_value "
            f"FROM {self._catalog_name}.information_schema.table_tags "
            f"WHERE schema_name = '{escaped_ns}' AND table_name = '{escaped_name}'"
        )
        try:
            result = self._execute(sql)
        except RuntimeError:
            logger.debug("Could not read tags for %s.%s", namespace, name)
            return {}

        tags: dict[str, str] = {}
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                tags[row[0]] = row[1] or ""
        return tags

    def _set_tags(self, namespace: str, name: str, tags: dict[str, str]) -> None:
        """Apply tags to a table via ALTER TABLE SET TAGS."""
        if not tags:
            return
        ftn = self._full_table_name(namespace, name)
        tag_pairs = ", ".join(
            f"'{k.replace(chr(39), chr(39)+chr(39))}' = "
            f"'{v.replace(chr(39), chr(39)+chr(39))}'"
            for k, v in sorted(tags.items())
            if k  # skip empty keys
        )
        if not tag_pairs:
            return
        sql = f"ALTER TABLE {ftn} SET TAGS ({tag_pairs})"
        self._execute(sql)

    def sync_tags(self, table: TableInfo) -> int:
        """Sync governance tags from Confluent to a Unity Catalog table.

        - Adds new tags from Confluent
        - Updates tags whose values changed
        - Tombstones tags that were previously managed by Confluent
          but no longer exist in the source
        - Preserves tags added directly in UC (not managed by Confluent)
        - Tracks managed keys via the _confluent_managed_tags manifest

        Returns the number of tag changes applied.
        """
        current_tags = self._read_table_tags(table.namespace, table.name)
        source_tags = table.tags

        # Parse the manifest of previously managed keys
        managed_csv = current_tags.get(MANAGED_TAGS_KEY, "")
        previously_managed = (
            {k for k in managed_csv.split(",") if k} if managed_csv else set()
        )

        tags_to_set: dict[str, str] = {}
        changes = 0

        # Add or update tags from source
        for key, value in source_tags.items():
            current_value = current_tags.get(key)
            if current_value != value:
                tags_to_set[key] = value
                changes += 1

        # Tombstone tags that were managed by us but removed from source
        to_tombstone = previously_managed - set(source_tags.keys())
        for key in to_tombstone:
            current_value = current_tags.get(key)
            if current_value is not None and current_value != TOMBSTONE_VALUE:
                tags_to_set[key] = TOMBSTONE_VALUE
                changes += 1

        # Update the manifest with current source keys
        new_managed = set(source_tags.keys())
        if new_managed != previously_managed or MANAGED_TAGS_KEY not in current_tags:
            tags_to_set[MANAGED_TAGS_KEY] = ",".join(sorted(new_managed))
            # Only count as a change if actual tags changed, not just manifest
            if not (changes == 0 and new_managed == previously_managed):
                pass  # already counted above

        if tags_to_set:
            logger.info(
                "Syncing %d tag change(s) for %s.%s",
                changes, table.namespace, table.name,
            )
            self._set_tags(table.namespace, table.name, tags_to_set)

        return changes
