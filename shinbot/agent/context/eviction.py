"""Block-based context eviction helpers."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from shinbot.agent.context.state_store import (
    CompressedMemoryState,
    ContextBlockState,
    ContextSessionState,
)


@dataclass(slots=True)
class ContextEvictionConfig:
    max_context_tokens: int = 32_000
    evict_ratio: float = 0.6
    max_compressed_entries: int = 3


def extract_total_tokens(usage: dict[str, Any] | None) -> int:
    payload = usage or {}
    input_tokens = int(payload.get("input_tokens", 0) or 0)
    output_tokens = int(payload.get("output_tokens", 0) or 0)
    return max(0, input_tokens + output_tokens)


def select_blocks_for_eviction(
    state: ContextSessionState,
    *,
    total_tokens: int,
    config: ContextEvictionConfig,
) -> list[ContextBlockState]:
    memory = state.short_term_memory()
    if total_tokens < config.max_context_tokens or not memory.has_blocks():
        return []
    return memory.select_head_for_eviction(config.evict_ratio)


def evict_context_blocks(
    state: ContextSessionState,
    *,
    total_tokens: int,
    config: ContextEvictionConfig,
    compressed_text: str = "",
    created_at_ms: int | None = None,
) -> dict[str, Any]:
    evicted_blocks = select_blocks_for_eviction(
        state,
        total_tokens=total_tokens,
        config=config,
    )
    if not evicted_blocks:
        return {
            "triggered": False,
            "total_tokens": total_tokens,
            "evicted_count": 0,
            "remaining_count": len(state.legacy_blocks()),
        }

    memory = state.short_term_memory()
    evicted_blocks = memory.evict_selected_head(evicted_blocks)
    state.set_short_term_memory(memory)
    if not evicted_blocks:
        return {
            "triggered": False,
            "total_tokens": total_tokens,
            "evicted_count": 0,
            "remaining_count": len(state.legacy_blocks()),
        }

    compressed = compressed_text.strip()
    if compressed:
        state.compressed_memories.append(
            CompressedMemoryState(
                text=compressed,
                created_at_ms=created_at_ms if created_at_ms is not None else int(time.time() * 1000),
                source_block_ids=[block.block_id for block in evicted_blocks],
                metadata={"evicted_count": len(evicted_blocks)},
            )
        )
        state.compressed_memories = state.compressed_memories[-config.max_compressed_entries :]

    return {
        "triggered": True,
        "total_tokens": total_tokens,
        "evicted_count": len(evicted_blocks),
        "remaining_count": len(state.legacy_blocks()),
        "compressed_added": bool(compressed),
    }
