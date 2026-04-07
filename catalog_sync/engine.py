from __future__ import annotations

import logging
from dataclasses import dataclass

from catalog_sync.models import TableInfo
from catalog_sync.sources.base import CatalogSource
from catalog_sync.targets.base import CatalogTarget

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncResult:
    added: int = 0
    updated: int = 0
    removed: int = 0
    tags_synced: int = 0

    @property
    def total_changes(self) -> int:
        return self.added + self.updated + self.removed


class SyncEngine:
    def __init__(
        self,
        source: CatalogSource,
        target: CatalogTarget,
        sync_tags: bool = False,
    ) -> None:
        self._source = source
        self._target = target
        self._sync_tags = sync_tags

    def sync(self) -> SyncResult:
        source_table_list = self._source.list_tables()
        source_tables = {t.full_name: t for t in source_table_list}
        target_tables = {t.full_name: t for t in self._target.list_tables()}

        # Check if tag fetch failed — skip tag sync to avoid removing
        # managed tags due to empty source tags from a transient error
        tag_fetch_failed = getattr(self._source, "_tag_fetch_failed", False)
        do_tag_sync = self._sync_tags and not tag_fetch_failed
        if self._sync_tags and tag_fetch_failed:
            logger.warning("Tag fetch failed — skipping tag sync to avoid data loss")

        source_names = set(source_tables.keys())
        target_names = set(target_tables.keys())

        to_add = source_names - target_names
        to_remove = target_names - source_names
        to_check = source_names & target_names

        added = 0
        tags_synced = 0
        for name in sorted(to_add):
            table = source_tables[name]
            logger.info("Registering new table: %s at %s", name, table.location)
            self._target.register_table(table)
            added += 1
            if do_tag_sync:
                tags_synced += self._target.sync_tags(table)

        updated = 0
        for name in sorted(to_check):
            source_table = source_tables[name]
            if self._needs_update(source_table, target_tables[name]):
                logger.info("Updating table: %s", name)
                self._target.update_table(source_table)
                updated += 1
            if do_tag_sync:
                tags_synced += self._target.sync_tags(source_table)

        removed = 0
        for name in sorted(to_remove):
            table = target_tables[name]
            logger.info("Removing stale table: %s", name)
            self._target.remove_table(table.namespace, table.name)
            removed += 1

        result = SyncResult(
            added=added, updated=updated, removed=removed,
            tags_synced=tags_synced,
        )
        logger.info("Sync complete: %s", result)
        return result

    def _needs_update(self, source: TableInfo, target: TableInfo) -> bool:
        return source.columns != target.columns or source.location != target.location
