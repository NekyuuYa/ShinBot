from __future__ import annotations

from shinbot.agent.context import ShortTermMemoryState
from shinbot.agent.context.state.state_store import ContextBlockState, ContextSessionState


def test_short_term_memory_wraps_prompt_blocks_without_reordering() -> None:
    blocks = [
        ContextBlockState(block_id="ctx-1", sealed=True),
        ContextBlockState(block_id="ctx-2", sealed=True),
        ContextBlockState(block_id="ctx-3", sealed=False),
    ]

    memory = ShortTermMemoryState.from_short_term_blocks(blocks)

    assert [block.block_id for block in memory.sealed_blocks.to_blocks()] == [
        "ctx-1",
        "ctx-2",
    ]
    assert memory.open_block.block is not None
    assert memory.open_block.block.block_id == "ctx-3"
    assert [block.block_id for block in memory.to_short_term_blocks()] == [
        "ctx-1",
        "ctx-2",
        "ctx-3",
    ]
    assert memory.block_count() == 3
    assert memory.cacheable_prefix_count() == 2
    assert [block.block_id for block in memory.cacheable_prefix_blocks()] == ["ctx-1", "ctx-2"]

    all_sealed_memory = ShortTermMemoryState.from_short_term_blocks(
        [
            ContextBlockState(block_id="ctx-1", sealed=True),
            ContextBlockState(block_id="ctx-2", sealed=True),
        ]
    )
    assert all_sealed_memory.cacheable_prefix_count() == 1


def test_context_session_state_persists_short_term_memory_as_primary_state() -> None:
    state = ContextSessionState(session_id="s-state")
    state.set_short_term_blocks(
        [
            ContextBlockState(block_id="ctx-1", sealed=True),
            ContextBlockState(block_id="ctx-2", sealed=False),
        ]
    )

    payload = state.to_dict()
    restored = ContextSessionState.from_dict(payload)

    assert "short_term_memory" in payload
    assert "blocks" not in payload
    assert [block.block_id for block in restored.short_term_memory().to_short_term_blocks()] == [
        "ctx-1",
        "ctx-2",
    ]
    assert [block.block_id for block in restored.short_term_blocks()] == ["ctx-1", "ctx-2"]


def test_context_session_state_ignores_old_blocks_when_short_term_memory_is_absent() -> None:
    restored = ContextSessionState.from_dict(
        {
            "session_id": "s-legacy",
            "blocks": [
                {
                    "block_id": "ctx-old",
                    "sealed": False,
                    "contents": [],
                    "metadata": {},
                }
            ],
        }
    )

    assert restored.short_term_memory().has_blocks() is False
    assert restored.short_term_blocks() == []
