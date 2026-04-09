import pytest
from catalog_sync.targets.base import CatalogTarget


def test_catalog_target_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        CatalogTarget()


def test_concrete_target_can_instantiate():
    class ConcreteTarget(CatalogTarget):
        def list_tables(self) -> list:
            return []

        def register_table(self, table) -> None:
            pass

        def update_table(self, table) -> None:
            pass

        def remove_table(self, namespace: str, name: str) -> None:
            pass

        def sync_tags(self, table) -> int:
            return 0

    target = ConcreteTarget()
    assert target.list_tables() == []
