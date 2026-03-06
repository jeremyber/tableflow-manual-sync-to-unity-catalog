from __future__ import annotations

from dataclasses import dataclass, field


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

    @property
    def full_name(self) -> str:
        return f"{self.namespace}.{self.name}"
