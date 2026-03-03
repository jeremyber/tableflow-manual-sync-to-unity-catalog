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
