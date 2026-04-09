from __future__ import annotations

import logging
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format

from catalog_sync.models import TableInfo, validate_identifier, validate_table_format

# Allowed column types for Unity Catalog
_VALID_COLUMN_TYPES = {
    "STRING", "INT", "BIGINT", "LONG", "FLOAT", "DOUBLE",
    "BOOLEAN", "DATE", "TIMESTAMP", "BINARY", "DECIMAL",
    "ARRAY", "MAP", "STRUCT",
}
from catalog_sync.targets.base import CatalogTarget

logger = logging.getLogger(__name__)


class UnityCatalogTarget(CatalogTarget):
    def __init__(
        self,
        host: str,
        token: str | None = None,
        catalog_name: str = "default",
        warehouse_id: str | None = None,
        schema_name: str = "default",
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        self._catalog_name = catalog_name
        self._warehouse_id = warehouse_id
        self._schema_name = schema_name
        if client_id and client_secret:
            self._ws = WorkspaceClient(
                host=host, client_id=client_id, client_secret=client_secret,
            )
        else:
            self._ws = WorkspaceClient(host=host, token=token)
        self._ensure_catalog_and_schema()

    def _ensure_catalog_and_schema(self) -> None:
        validate_identifier(self._catalog_name, "catalog name")
        validate_identifier(self._schema_name, "schema name")
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
        if result.status and result.status.state:
            state = result.status.state.value
            if state == "FAILED":
                error_msg = result.status.error.message if result.status.error else "Unknown error"
                raise RuntimeError(f"SQL execution failed: {error_msg}")
            if state != "SUCCEEDED":
                raise RuntimeError(f"SQL did not complete (state={state})")
        return result

    def _full_table_name(self, namespace: str, name: str) -> str:
        validate_identifier(namespace, "namespace")
        validate_identifier(name, "table name")
        return f"`{self._catalog_name}`.`{namespace}`.`{name}`"

    def list_tables(self) -> list[TableInfo]:
        escaped_schema = self._schema_name.replace("'", "''")
        sql = (
            f"SELECT table_schema, table_name, comment "
            f"FROM `{self._catalog_name}`.information_schema.tables "
            f"WHERE table_schema = '{escaped_schema}' "
            f"AND table_type = 'EXTERNAL'"
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

    @staticmethod
    def _validate_location(location: str) -> None:
        if not re.match(r"^(s3|s3a|abfss|gs)://", location):
            raise ValueError(
                f"Invalid storage location: {location!r} "
                f"— expected s3://, abfss://, or gs:// URI"
            )

    def register_table(self, table: TableInfo) -> None:
        self._validate_location(table.location)
        columns_part = ""
        if table.columns:
            for c in table.columns:
                validate_identifier(c.name, "column name")
                col_type_upper = c.type.upper()
                if col_type_upper not in _VALID_COLUMN_TYPES:
                    raise ValueError(
                        f"Invalid column type: {c.type!r} for column {c.name!r} "
                        f"— expected one of {_VALID_COLUMN_TYPES}"
                    )
            columns_sql = ", ".join(
                f"`{c.name}` {c.type}" for c in table.columns
            )
            columns_part = f" ({columns_sql})"
        escaped_location = table.location.replace("'", "''")
        table_format = validate_table_format(table.table_format)
        sql = (
            f"CREATE TABLE IF NOT EXISTS "
            f"{self._full_table_name(table.namespace, table.name)}"
            f"{columns_part} "
            f"USING {table_format} "
            f"LOCATION '{escaped_location}' "
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
            f"FROM `{self._catalog_name}`.information_schema.table_tags "
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

    _MANAGED_KEYS_PROP = "_confluent_managed_tags"

    def _read_managed_keys(self, namespace: str, name: str) -> set[str]:
        """Read the set of tag keys managed by this tool from table properties."""
        ftn = self._full_table_name(namespace, name)
        try:
            result = self._execute(
                f"SHOW TBLPROPERTIES {ftn} ('{self._MANAGED_KEYS_PROP}')"
            )
            if result.result and result.result.data_array:
                val = result.result.data_array[0][1] or ""
                if val and "does not have property" not in val:
                    return {k for k in val.split(",") if k}
        except RuntimeError:
            pass
        return set()

    def _write_managed_keys(self, namespace: str, name: str, keys: set[str]) -> None:
        """Store managed tag keys in table properties (invisible in tags view)."""
        ftn = self._full_table_name(namespace, name)
        val = ",".join(sorted(keys)).replace("'", "''")
        try:
            self._execute(
                f"ALTER TABLE {ftn} SET TBLPROPERTIES ('{self._MANAGED_KEYS_PROP}' = '{val}')"
            )
        except RuntimeError:
            logger.debug("Could not write managed keys for %s.%s", namespace, name)

    def _unset_tags(self, namespace: str, name: str, keys: set[str]) -> None:
        """Remove tags from a table via ALTER TABLE UNSET TAGS."""
        if not keys:
            return
        ftn = self._full_table_name(namespace, name)
        key_list = ", ".join(
            f"'{k.replace(chr(39), chr(39)+chr(39))}'" for k in sorted(keys)
        )
        self._execute(f"ALTER TABLE {ftn} UNSET TAGS ({key_list})")

    def sync_tags(self, table: TableInfo) -> int:
        """Sync governance tags from Confluent to a Unity Catalog table.

        - Adds new tags from Confluent
        - Updates tags whose values changed
        - Removes tags previously managed by us that no longer exist in Confluent
        - Preserves tags added directly in UC (not in manifest)
        - Tracks managed keys in table properties (invisible in tags view)

        Returns the number of tag changes applied.
        """
        current_tags = self._read_table_tags(table.namespace, table.name)
        source_tags = table.tags
        previously_managed = self._read_managed_keys(table.namespace, table.name)

        tags_to_set: dict[str, str] = {}
        tags_to_remove: set[str] = set()
        changes = 0

        # Add or update
        for key, value in source_tags.items():
            if not key:
                continue
            if current_tags.get(key) != value:
                tags_to_set[key] = value
                changes += 1

        # Remove tags we previously managed but are no longer in source
        for key in previously_managed - set(source_tags.keys()):
            if key in current_tags:
                tags_to_remove.add(key)
                changes += 1

        if tags_to_set:
            logger.info("Setting %d tag(s) for %s.%s", len(tags_to_set), table.namespace, table.name)
            self._set_tags(table.namespace, table.name, tags_to_set)

        if tags_to_remove:
            logger.info("Removing %d stale tag(s) for %s.%s", len(tags_to_remove), table.namespace, table.name)
            self._unset_tags(table.namespace, table.name, tags_to_remove)

        # Update manifest
        new_managed = {k for k in source_tags.keys() if k}
        if new_managed != previously_managed:
            self._write_managed_keys(table.namespace, table.name, new_managed)

        return changes
