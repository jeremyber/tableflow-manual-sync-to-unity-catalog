from __future__ import annotations

import re
from dataclasses import dataclass, field


# Characters prohibited in Unity Catalog tag keys
_UC_TAG_KEY_INVALID = re.compile(r"[.,\-=/:\s]+")

# Reserved key used to track which tags were synced from Confluent
MANAGED_TAGS_KEY = "_confluent_managed_tags"

# Sentinel value for tombstoned tags (preserves audit trail)
TOMBSTONE_VALUE = "__tombstone__"


def sanitize_tag_key(key: str) -> str:
    """Sanitize a tag key for Unity Catalog.

    UC tag keys cannot contain: . , - = / : or whitespace.
    Replaces prohibited characters with underscores and strips
    leading/trailing underscores.
    """
    return _UC_TAG_KEY_INVALID.sub("_", key).strip("_")


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str
    nullable: bool = True


@dataclass(frozen=True)
class TableInfo:
    namespace: str
    name: str
    location: str
    columns: list[ColumnInfo] = field(default_factory=list)
    table_format: str = "DELTA"
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        return f"{self.namespace}.{self.name}"
