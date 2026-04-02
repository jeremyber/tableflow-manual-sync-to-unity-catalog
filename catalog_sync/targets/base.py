from __future__ import annotations

from abc import ABC, abstractmethod

from catalog_sync.models import TableInfo


class CatalogTarget(ABC):
    @abstractmethod
    def list_tables(self) -> list[TableInfo]:
        """List all tables currently registered in this target."""

    @abstractmethod
    def register_table(self, table: TableInfo) -> None:
        """Register a new external Iceberg table."""

    @abstractmethod
    def update_table(self, table: TableInfo) -> None:
        """Update an existing table registration (e.g., schema change)."""

    @abstractmethod
    def remove_table(self, namespace: str, name: str) -> None:
        """Remove a table registration."""

    @abstractmethod
    def sync_tags(self, table: TableInfo) -> int:
        """Sync governance tags for a registered table.

        Compares source tags with current tags on the target table.
        Adds new tags, updates changed tags, and tombstones removed tags.
        Preserves tags not managed by Confluent.

        Returns the number of tag changes applied.
        """
