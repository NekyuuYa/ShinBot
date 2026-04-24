"""Persistent state storage for context packing sessions."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from shinbot.agent.context.alias_table import SessionAliasTable
from shinbot.agent.context.ring_buffer import StableRingIdAllocator


@dataclass(slots=True)
class ContextBlockState:
    block_id: str
    kind: str = "context"
    token_estimate: int = 0
    sealed: bool = False
    contents: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "block_id": self.block_id,
            "kind": self.kind,
            "token_estimate": self.token_estimate,
            "sealed": self.sealed,
            "contents": list(self.contents),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ContextBlockState:
        return cls(
            block_id=str(payload.get("block_id", "") or ""),
            kind=str(payload.get("kind", "context") or "context"),
            token_estimate=int(payload.get("token_estimate", 0) or 0),
            sealed=bool(payload.get("sealed", False)),
            contents=list(payload.get("contents", [])),
            metadata=dict(payload.get("metadata", {})),
        )


@dataclass(slots=True)
class CompressedMemoryState:
    text: str = ""
    created_at_ms: int = 0
    source_block_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "created_at_ms": self.created_at_ms,
            "source_block_ids": list(self.source_block_ids),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CompressedMemoryState:
        return cls(
            text=str(payload.get("text", "") or ""),
            created_at_ms=int(payload.get("created_at_ms", 0) or 0),
            source_block_ids=[str(item) for item in payload.get("source_block_ids", [])],
            metadata=dict(payload.get("metadata", {})),
        )


@dataclass(slots=True)
class OpenBlockState:
    """Mutable tail block in short-term context memory."""

    block: ContextBlockState | None = None

    def to_legacy_block(self) -> ContextBlockState | None:
        return self.block


@dataclass(slots=True)
class SealedBlockDequeState:
    """Time-ordered sealed short-term blocks."""

    blocks: list[ContextBlockState] = field(default_factory=list)

    def append(self, block: ContextBlockState) -> None:
        self.blocks.append(block)

    def popleft(self) -> ContextBlockState | None:
        if not self.blocks:
            return None
        return self.blocks.pop(0)

    def head(self, count: int) -> list[ContextBlockState]:
        if count <= 0:
            return []
        return list(self.blocks[:count])

    def drop_head(self, count: int) -> list[ContextBlockState]:
        if count <= 0:
            return []
        removed = self.head(count)
        self.blocks = self.blocks[len(removed) :]
        return removed

    def to_legacy_blocks(self) -> list[ContextBlockState]:
        return list(self.blocks)


@dataclass(slots=True)
class ShortTermMemoryState:
    """Short-term memory view backed by legacy context blocks during migration."""

    sealed_blocks: SealedBlockDequeState = field(default_factory=SealedBlockDequeState)
    open_block: OpenBlockState = field(default_factory=OpenBlockState)

    @classmethod
    def from_legacy_blocks(cls, blocks: list[ContextBlockState]) -> ShortTermMemoryState:
        if not blocks:
            return cls()
        if blocks[-1].sealed:
            return cls(sealed_blocks=SealedBlockDequeState(list(blocks)))
        sealed = list(blocks[:-1])
        open_block = blocks[-1]
        return cls(
            sealed_blocks=SealedBlockDequeState(sealed),
            open_block=OpenBlockState(open_block),
        )

    def to_legacy_blocks(self) -> list[ContextBlockState]:
        blocks = self.sealed_blocks.to_legacy_blocks()
        open_block = self.open_block.to_legacy_block()
        if open_block is not None:
            blocks.append(open_block)
        return blocks

    def has_blocks(self) -> bool:
        return bool(self.sealed_blocks.blocks or self.open_block.block is not None)

    def block_count(self) -> int:
        return len(self.to_legacy_blocks())

    def cacheable_prefix_count(self) -> int:
        sealed_count = len(self.sealed_blocks.blocks)
        if self.open_block.block is None:
            return max(0, sealed_count - 1)
        return sealed_count

    def cacheable_prefix_blocks(self) -> list[ContextBlockState]:
        return self.sealed_blocks.head(self.cacheable_prefix_count())

    def select_head_for_eviction(self, evict_ratio: float) -> list[ContextBlockState]:
        sealed_count = len(self.sealed_blocks.blocks)
        if sealed_count:
            evict_count = max(1, int(math.ceil(sealed_count * evict_ratio)))
            return self.sealed_blocks.head(evict_count)
        open_block = self.open_block.to_legacy_block()
        return [open_block] if open_block is not None else []

    def evict_head(self, evict_ratio: float) -> list[ContextBlockState]:
        sealed_count = len(self.sealed_blocks.blocks)
        if sealed_count:
            evict_count = max(1, int(math.ceil(sealed_count * evict_ratio)))
            return self.sealed_blocks.drop_head(evict_count)
        open_block = self.open_block.to_legacy_block()
        if open_block is None:
            return []
        self.open_block = OpenBlockState()
        return [open_block]

    def evict_selected_head(self, selected_blocks: list[ContextBlockState]) -> list[ContextBlockState]:
        if not selected_blocks:
            return []

        selected_ids = [block.block_id for block in selected_blocks]
        sealed_ids = [block.block_id for block in self.sealed_blocks.blocks[: len(selected_ids)]]
        if sealed_ids == selected_ids:
            return self.sealed_blocks.drop_head(len(selected_blocks))

        open_block = self.open_block.to_legacy_block()
        if len(selected_blocks) == 1 and open_block is not None:
            if selected_blocks[0].block_id == open_block.block_id:
                self.open_block = OpenBlockState()
                return [open_block]

        return []


@dataclass(slots=True)
class ContextSessionState:
    session_id: str
    alias_table: SessionAliasTable = field(default_factory=lambda: SessionAliasTable(session_id=""))
    message_ids: StableRingIdAllocator = field(
        default_factory=lambda: StableRingIdAllocator(capacity=9999)
    )
    image_ids: StableRingIdAllocator = field(
        default_factory=lambda: StableRingIdAllocator(capacity=9999)
    )
    blocks: list[ContextBlockState] = field(default_factory=list)
    compressed_memories: list[CompressedMemoryState] = field(default_factory=list)
    inactive_alias_entries: list[dict[str, str]] = field(default_factory=list)
    inactive_alias_table_frozen: bool = False
    last_cache_refresh_ms: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.alias_table.session_id:
            self.alias_table.session_id = self.session_id

    def legacy_blocks(self) -> list[ContextBlockState]:
        """Return the legacy prompt-shaped block list."""
        return self.short_term_memory().to_legacy_blocks()

    def set_legacy_blocks(self, blocks: list[ContextBlockState]) -> None:
        """Replace the legacy prompt-shaped block list."""
        self.set_short_term_memory(ShortTermMemoryState.from_legacy_blocks(list(blocks)))

    def has_legacy_blocks(self) -> bool:
        return self.short_term_memory().has_blocks()

    def short_term_memory(self) -> ShortTermMemoryState:
        return ShortTermMemoryState.from_legacy_blocks(list(self.blocks))

    def set_short_term_memory(self, memory: ShortTermMemoryState) -> None:
        self.blocks = memory.to_legacy_blocks()

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "alias_table": self.alias_table.to_dict(),
            "message_ids": self.message_ids.to_dict(),
            "image_ids": self.image_ids.to_dict(),
            "blocks": [block.to_dict() for block in self.blocks],
            "compressed_memories": [item.to_dict() for item in self.compressed_memories],
            "inactive_alias_entries": list(self.inactive_alias_entries),
            "inactive_alias_table_frozen": self.inactive_alias_table_frozen,
            "last_cache_refresh_ms": self.last_cache_refresh_ms,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> ContextSessionState:
        data = payload or {}
        session_id = str(data.get("session_id", "") or "")
        return cls(
            session_id=session_id,
            alias_table=SessionAliasTable.from_dict(data.get("alias_table", {})),
            message_ids=StableRingIdAllocator.from_dict(data.get("message_ids", {})),
            image_ids=StableRingIdAllocator.from_dict(data.get("image_ids", {})),
            blocks=[
                ContextBlockState.from_dict(item)
                for item in data.get("blocks", [])
                if isinstance(item, dict)
            ],
            compressed_memories=[
                CompressedMemoryState.from_dict(item)
                for item in data.get("compressed_memories", [])
                if isinstance(item, dict)
            ],
            inactive_alias_entries=[
                {
                    "alias": str(item.get("alias", "") or ""),
                    "platform_id": str(item.get("platform_id", "") or ""),
                    "display_name": str(item.get("display_name", "") or ""),
                }
                for item in data.get("inactive_alias_entries", [])
                if isinstance(item, dict)
            ],
            inactive_alias_table_frozen=bool(data.get("inactive_alias_table_frozen", False)),
            last_cache_refresh_ms=int(data.get("last_cache_refresh_ms", 0) or 0),
            metadata=dict(data.get("metadata", {})),
        )


class ContextStateStore:
    """Persist per-session context packing state as JSON files."""

    def __init__(self, data_dir: Path | str | None = "data") -> None:
        self._base_dir: Path | None = None
        if data_dir is not None:
            self._base_dir = Path(data_dir) / "temp" / "context_state"
            self._base_dir.mkdir(parents=True, exist_ok=True)

    def _state_path(self, session_id: str) -> Path | None:
        if self._base_dir is None:
            return None
        sanitized = session_id.replace(":", "_").replace("/", "_")
        return self._base_dir / f"{sanitized}.json"

    def load(self, session_id: str) -> ContextSessionState | None:
        path = self._state_path(session_id)
        if path is None or not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return ContextSessionState.from_dict(payload)

    def save(self, state: ContextSessionState) -> None:
        path = self._state_path(state.session_id)
        if path is None:
            return
        content = json.dumps(state.to_dict(), ensure_ascii=False, indent=2)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, path)

    def delete(self, session_id: str) -> None:
        path = self._state_path(session_id)
        if path is not None and path.exists():
            path.unlink()
