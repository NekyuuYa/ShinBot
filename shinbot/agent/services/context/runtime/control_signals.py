"""Explicit control signals emitted by context runtimes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

CacheReleaseReason = Literal["token_budget_exceeded", "manual", "none"]


@dataclass(frozen=True, slots=True)
class CacheReleaseSignal:
    """Signal emitted when short-term cache blocks are selected or released."""

    triggered: bool
    reason: CacheReleaseReason = "none"
    total_tokens: int = 0
    released_block_ids: list[str] = field(default_factory=list)
    remaining_count: int = 0
    compressed_memory_added: bool = False
    alias_rebuild_requested: bool = False
    inactive_alias_snapshot_reset: bool = False
    source_text: str = ""

    @classmethod
    def inactive(
        cls,
        *,
        total_tokens: int = 0,
        remaining_count: int = 0,
    ) -> CacheReleaseSignal:
        return cls(
            triggered=False,
            total_tokens=total_tokens,
            remaining_count=remaining_count,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "cache_release",
            "triggered": self.triggered,
            "reason": self.reason,
            "total_tokens": self.total_tokens,
            "released_block_ids": list(self.released_block_ids),
            "remaining_count": self.remaining_count,
            "compressed_memory_added": self.compressed_memory_added,
            "alias_rebuild_requested": self.alias_rebuild_requested,
            "inactive_alias_snapshot_reset": self.inactive_alias_snapshot_reset,
            "source_text": self.source_text,
        }
