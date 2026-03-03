import pytest
from catalog_sync.sources.base import CatalogSource


def test_catalog_source_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        CatalogSource()


def test_catalog_source_requires_list_tables():
    class IncompleteSource(CatalogSource):
        pass

    with pytest.raises(TypeError, match="abstract"):
        IncompleteSource()


def test_concrete_source_can_instantiate():
    class ConcreteSource(CatalogSource):
        def list_tables(self) -> list:
            return []

    source = ConcreteSource()
    assert source.list_tables() == []
