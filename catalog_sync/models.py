from __future__ import annotations

import re
from dataclasses import dataclass, field


# Characters prohibited in Unity Catalog tag keys
_UC_TAG_KEY_INVALID = re.compile(r"[.,\-=/:\s';\(\)`]+")

# Safe pattern for SQL identifiers (catalog, schema, table names)
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_\-]+$")

# Allowed table formats for USING clause
VALID_TABLE_FORMATS = {"DELTA", "ICEBERG"}


def validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate a SQL identifier to prevent injection.

    Only allows alphanumeric characters, underscores, and hyphens.
    Raises ValueError if the identifier contains unsafe characters
    (e.g., backticks, semicolons, quotes).
    """
    if not value or not _SAFE_IDENTIFIER.match(value):
        raise ValueError(
            f"Unsafe {label}: {value!r} — only alphanumeric, "
            f"underscore, and hyphen are allowed"
        )
    return value


def validate_table_format(fmt: str) -> str:
    """Validate table format against known values."""
    upper = fmt.upper()
    if upper not in VALID_TABLE_FORMATS:
        raise ValueError(
            f"Unknown table format: {fmt!r} — expected one of {VALID_TABLE_FORMATS}"
        )
    return upper


_UC_TAG_KEY_VALID = re.compile(r"^[a-zA-Z0-9_]+$")


def sanitize_tag_key(key: str) -> str:
    """Sanitize a tag key for Unity Catalog.

    UC tag keys cannot contain: . , - = / : ' ; ( ) ` or whitespace.
    Replaces prohibited characters with underscores and strips
    leading/trailing underscores. Returns empty string for
    keys that consist entirely of prohibited characters or that
    contain characters outside [a-zA-Z0-9_] after sanitization.
    """
    sanitized = _UC_TAG_KEY_INVALID.sub("_", key).strip("_")
    if sanitized and not _UC_TAG_KEY_VALID.match(sanitized):
        return ""
    return sanitized


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
