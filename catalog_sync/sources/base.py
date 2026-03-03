from __future__ import annotations

from abc import ABC, abstractmethod

from catalog_sync.models import TableInfo


class CatalogSource(ABC):
    @abstractmethod
    def list_tables(self) -> list[TableInfo]:
        """List all tables available in this catalog source."""
