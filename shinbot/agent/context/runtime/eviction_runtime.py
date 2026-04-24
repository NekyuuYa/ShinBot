"""Runtime coordinator for context eviction control signals."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.context.projectors.alias_projector import AliasContextProjector
from shinbot.agent.context.projectors.compressed_memory_projector import CompressedMemoryProjector
from shinbot.agent.context.runtime.control_signals import CacheReleaseSignal
from shinbot.agent.context.state.state_store import ContextSessionState
from shinbot.agent.context.utils.eviction import (
    ContextEvictionConfig,
    evict_context_blocks,
    extract_total_tokens,
    select_blocks_for_eviction,
)


@dataclass(slots=True)
class ContextEvictionRuntime:
    """Coordinate high-level side effects around short-term block eviction."""

    alias_projector: AliasContextProjector = field(default_factory=AliasContextProjector)
    compressed_memory_projector: CompressedMemoryProjector = field(
        default_factory=CompressedMemoryProjector
    )

    def apply(
        self,
        state: ContextSessionState,
        usage: dict[str, Any] | None,
        *,
        max_context_tokens: int = 32_000,
        evict_ratio: float = 0.6,
        compressed_text: str = "",
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        result = evict_context_blocks(
            state,
            total_tokens=extract_total_tokens(usage),
            config=ContextEvictionConfig(
                max_context_tokens=max_context_tokens,
                evict_ratio=evict_ratio,
            ),
            compressed_text=compressed_text,
            created_at_ms=now_ms,
        )
        source_block_ids = list(result.get("source_block_ids") or [])
        if result.get("triggered"):
            state.alias_table.request_rebuild()
            self.alias_projector.reset_inactive_snapshot(state)
            signal = CacheReleaseSignal(
                triggered=True,
                reason="token_budget_exceeded",
                total_tokens=int(result.get("total_tokens", 0) or 0),
                released_block_ids=source_block_ids,
                remaining_count=int(result.get("remaining_count", 0) or 0),
                compressed_memory_added=bool(result.get("compressed_added")),
                alias_rebuild_requested=True,
                inactive_alias_snapshot_reset=True,
            )
        else:
            signal = CacheReleaseSignal.inactive(
                total_tokens=int(result.get("total_tokens", 0) or 0),
                remaining_count=int(result.get("remaining_count", 0) or 0),
            )
        result["control_signal"] = signal.to_dict()
        return result

    def preview(
        self,
        state: ContextSessionState,
        usage: dict[str, Any] | None,
        *,
        max_context_tokens: int = 32_000,
        evict_ratio: float = 0.6,
    ) -> dict[str, Any]:
        total_tokens = extract_total_tokens(usage)
        config = ContextEvictionConfig(
            max_context_tokens=max_context_tokens,
            evict_ratio=evict_ratio,
        )
        evicted_blocks = select_blocks_for_eviction(
            state,
            total_tokens=total_tokens,
            config=config,
        )
        if not evicted_blocks:
            signal = CacheReleaseSignal.inactive(
                total_tokens=total_tokens,
                remaining_count=state.short_term_memory().block_count(),
            )
            return {
                "triggered": False,
                "total_tokens": total_tokens,
                "evicted_count": 0,
                "remaining_count": state.short_term_memory().block_count(),
                "source_text": "",
                "source_block_ids": [],
                "control_signal": signal.to_dict(),
            }

        source_text = self.compressed_memory_projector.build_source_text(
            alias_table=state.alias_table,
            blocks=evicted_blocks,
        )
        source_block_ids = [block.block_id for block in evicted_blocks]
        remaining_count = max(0, state.short_term_memory().block_count() - len(evicted_blocks))
        signal = CacheReleaseSignal(
            triggered=True,
            reason="token_budget_exceeded",
            total_tokens=total_tokens,
            released_block_ids=source_block_ids,
            remaining_count=remaining_count,
            source_text=source_text,
        )
        return {
            "triggered": True,
            "total_tokens": total_tokens,
            "evicted_count": len(evicted_blocks),
            "remaining_count": remaining_count,
            "source_text": source_text,
            "source_block_ids": source_block_ids,
            "control_signal": signal.to_dict(),
        }
