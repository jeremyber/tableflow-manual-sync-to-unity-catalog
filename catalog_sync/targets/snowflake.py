"""Snowflake target: registers Tableflow Iceberg tables from object storage.

Snowflake can't federate to the Tableflow Iceberg REST Catalog over private
networking, so tables are created through an object-store catalog integration
(``CATALOG_SOURCE = OBJECT_STORE, TABLE_FORMAT = ICEBERG``) and pinned to a
specific metadata file. Snowflake does not auto-refresh such tables, so
``refresh_table`` moves each table to the newest metadata file on every sync.

Data files are read through a Snowflake external volume on the same
bucket/container; only metadata (paths, DDL) flows through this target.
"""

from __future__ import annotations

import json
import logging
import re

from catalog_sync.iceberg_metadata import IcebergMetadataLocator, parse_table_path
from catalog_sync.models import TableInfo, validate_table_format
from catalog_sync.targets.base import CatalogTarget

logger = logging.getLogger(__name__)

# Unquoted Snowflake identifiers (database, schema, volume, integration)
_SNOWFLAKE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# Marks tables created by this tool; the topic name and table_path are kept in
# the COMMENT so list_tables can diff against the source (like UC's COMMENT).
_COMMENT_PREFIX = "catalog-sync"
_COMMENT_PATTERN = re.compile(rf"^{_COMMENT_PREFIX} topic=(\S+) location=(\S+)$")

_ICEBERG_ONLY = {"ICEBERG"}


def _validate_snowflake_identifier(value: str, label: str) -> str:
    if not value or not _SNOWFLAKE_IDENTIFIER.match(value):
        raise ValueError(f"Unsafe {label}: {value!r}: expected an unquoted Snowflake identifier")
    return value


def snowflake_table_name(topic: str) -> str:
    """Map a topic name to an unquoted Snowflake identifier (``orders-v2`` -> ``ORDERS_V2``)."""
    name = re.sub(r"[^A-Za-z0-9_]", "_", topic).upper()
    return f"_{name}" if name[:1].isdigit() else name


def connect_snowflake(
    account: str,
    user: str,
    role: str | None = None,
    warehouse: str | None = None,
    private_key_path: str | None = None,
    password: str | None = None,
):
    """Open a Snowflake connection, preferring key-pair auth for service users."""
    import snowflake.connector

    params = {"account": account, "user": user, "role": role, "warehouse": warehouse}
    if private_key_path:
        from cryptography.hazmat.primitives import serialization

        with open(private_key_path, "rb") as f:
            key = serialization.load_pem_private_key(f.read(), password=None)
        params["private_key"] = key.private_bytes(
            serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    elif password:
        params["password"] = password
    else:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD is required")
    return snowflake.connector.connect(**{k: v for k, v in params.items() if v is not None})


class SnowflakeTarget(CatalogTarget):
    def __init__(
        self,
        connection,
        database: str,
        schema_name: str,
        external_volume: str,
        catalog_integration: str,
        metadata_locator: IcebergMetadataLocator,
        storage_base_path: str = "",
    ) -> None:
        """
        Args:
            connection: DB-API connection (see ``connect_snowflake``).
            storage_base_path: Path inside the container/bucket that the
                external volume's STORAGE_BASE_URL points at ("" when it points
                at the container/bucket root). Metadata file paths passed to
                Snowflake are relative to it.
        """
        self._conn = connection
        self._database = _validate_snowflake_identifier(database, "database")
        self._schema = _validate_snowflake_identifier(schema_name, "schema")
        self._volume = _validate_snowflake_identifier(external_volume, "external volume")
        self._catalog = _validate_snowflake_identifier(catalog_integration, "catalog integration")
        self._locator = metadata_locator
        self._base_path = storage_base_path.strip("/")

    def _execute(self, sql: str, params: tuple | None = None) -> list[dict]:
        logger.info("Executing SQL: %s", sql)
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            if not cur.description:
                return []
            columns = [c[0].lower() for c in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def _full_table_name(self, name: str) -> str:
        return f"{self._database}.{self._schema}.{snowflake_table_name(name)}"

    def _relative_metadata_path(self, table_path: str) -> str | None:
        """Newest metadata file for the table, relative to the external volume base."""
        key = self._locator.latest_metadata_file(table_path)
        if key is None:
            return None
        if self._base_path:
            if not key.startswith(f"{self._base_path}/"):
                raise ValueError(
                    f"Metadata file {key!r} is outside the external volume base {self._base_path!r}"
                )
            key = key[len(self._base_path) + 1:]
        return key

    def supports_format(self, table_format: str) -> bool:
        return table_format.upper() in _ICEBERG_ONLY

    def list_tables(self) -> list[TableInfo]:
        rows = self._execute(f"SHOW ICEBERG TABLES IN SCHEMA {self._database}.{self._schema}")
        tables: list[TableInfo] = []
        for row in rows:
            match = _COMMENT_PATTERN.match(row.get("comment") or "")
            if not match:
                continue  # not created by catalog-sync; leave it alone
            tables.append(TableInfo(
                namespace=self._schema,
                name=match.group(1),
                location=match.group(2),
                columns=[],
                table_format="ICEBERG",
            ))
        return tables

    def register_table(self, table: TableInfo) -> None:
        validate_table_format(table.table_format, _ICEBERG_ONLY)
        parse_table_path(table.location)  # reject unsupported schemes early
        metadata_path = self._relative_metadata_path(table.location)
        if metadata_path is None:
            logger.warning(
                "No Iceberg snapshot yet for %s under %s; will register on a later run",
                table.name, table.location,
            )
            return
        comment = f"{_COMMENT_PREFIX} topic={table.name} location={table.location}"
        logger.info("Registering Iceberg table: %s -> %s", table.name, metadata_path)
        self._execute(
            f"CREATE ICEBERG TABLE IF NOT EXISTS {self._full_table_name(table.name)} "
            f"EXTERNAL_VOLUME = '{self._volume}' "
            f"CATALOG = '{self._catalog}' "
            f"METADATA_FILE_PATH = %s "
            f"COMMENT = %s",
            (metadata_path, comment),
        )

    def update_table(self, table: TableInfo) -> None:
        # table_path changed (e.g. Tableflow re-enabled): re-create on the new path
        self.remove_table(table.namespace, table.name)
        self.register_table(table)

    def remove_table(self, namespace: str, name: str) -> None:
        logger.info("Removing Iceberg table: %s", name)
        self._execute(f"DROP ICEBERG TABLE IF EXISTS {self._full_table_name(name)}")

    def refresh_table(self, table: TableInfo) -> bool:
        metadata_path = self._relative_metadata_path(table.location)
        if metadata_path is None:
            return False
        full_name = self._full_table_name(table.name)
        rows = self._execute("SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(%s) AS info", (full_name,))
        current = json.loads(rows[0]["info"]).get("metadataLocation", "") if rows else ""
        if current.endswith(metadata_path):
            return False
        logger.info("Refreshing %s -> %s", full_name, metadata_path)
        self._execute(f"ALTER ICEBERG TABLE {full_name} REFRESH %s", (metadata_path,))
        return True

    def sync_tags(self, table: TableInfo) -> int:
        # Snowflake object tags (ALTER ICEBERG TABLE ... SET TAG) are future work.
        return 0
