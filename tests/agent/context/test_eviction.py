from __future__ import annotations

from shinbot.agent.context.state.state_store import ContextBlockState, ContextSessionState
from shinbot.agent.context.utils.eviction import ContextEvictionConfig, evict_context_blocks


def test_eviction_removes_sealed_queue_head_before_open_block() -> None:
    state = ContextSessionState(session_id="s-evict")
    state.set_short_term_blocks(
        [
            ContextBlockState(block_id="ctx-1", sealed=True),
            ContextBlockState(block_id="ctx-2", sealed=True),
            ContextBlockState(block_id="ctx-3", sealed=False),
        ]
    )

    result = evict_context_blocks(
        state,
        total_tokens=100,
        config=ContextEvictionConfig(max_context_tokens=1, evict_ratio=1.0),
        compressed_text="summary",
        created_at_ms=123,
    )

    assert result["triggered"] is True
    assert result["evicted_count"] == 2
    assert result["remaining_count"] == 1
    assert [block.block_id for block in state.short_term_blocks()] == ["ctx-3"]
    assert len(state.compressed_memories) == 1
    assert state.compressed_memories[0].source_block_ids == ["ctx-1", "ctx-2"]


def test_eviction_can_fallback_to_open_block_when_no_sealed_blocks_exist() -> None:
    state = ContextSessionState(session_id="s-evict-open")
    state.set_short_term_blocks([ContextBlockState(block_id="ctx-open", sealed=False)])

    result = evict_context_blocks(
        state,
        total_tokens=100,
        config=ContextEvictionConfig(max_context_tokens=1, evict_ratio=0.6),
    )

    assert result["triggered"] is True
    assert result["evicted_count"] == 1
    assert result["remaining_count"] == 0
    assert state.short_term_blocks() == []
