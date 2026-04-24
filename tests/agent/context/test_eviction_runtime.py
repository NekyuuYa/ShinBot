from __future__ import annotations

from shinbot.agent.context import ContextEvictionRuntime
from shinbot.agent.context.state.alias_table import AliasEntry
from shinbot.agent.context.state.state_store import ContextBlockState, ContextSessionState


def _state_with_sealed_blocks() -> ContextSessionState:
    state = ContextSessionState(session_id="s-eviction-runtime")
    state.alias_table.entries = {
        "user-1": AliasEntry(platform_id="user-1", alias="P0", display_name="Alice")
    }
    state.inactive_alias_entries = [
        {"alias": "P0", "platform_id": "user-1", "display_name": "Alice"}
    ]
    state.inactive_alias_table_frozen = True
    state.set_short_term_blocks(
        [
            ContextBlockState(
                block_id="ctx-1",
                sealed=True,
                contents=[
                    {
                        "type": "text",
                        "text": "[msgid: 0001]P0: older message [@ P0/user-1]",
                    }
                ],
            ),
            ContextBlockState(
                block_id="ctx-2",
                sealed=True,
                contents=[{"type": "text", "text": "[msgid: 0002]assistant: reply"}],
            ),
            ContextBlockState(
                block_id="ctx-3",
                sealed=False,
                contents=[{"type": "text", "text": "[msgid: 0003]P0: current"}],
            ),
        ]
    )
    return state


def test_eviction_runtime_preview_projects_compression_source_without_mutating_state() -> None:
    state = _state_with_sealed_blocks()
    runtime = ContextEvictionRuntime()

    result = runtime.preview(
        state,
        {"input_tokens": 100, "output_tokens": 20},
        max_context_tokens=1,
        evict_ratio=0.5,
    )

    assert result["triggered"] is True
    assert result["evicted_count"] == 1
    assert result["remaining_count"] == 2
    assert result["source_block_ids"] == ["ctx-1"]
    assert result["control_signal"] == {
        "type": "cache_release",
        "triggered": True,
        "reason": "token_budget_exceeded",
        "total_tokens": 120,
        "released_block_ids": ["ctx-1"],
        "remaining_count": 2,
        "compressed_memory_added": False,
        "alias_rebuild_requested": False,
        "inactive_alias_snapshot_reset": False,
        "source_text": result["source_text"],
    }
    assert "P0 = Alice / user-1" in result["source_text"]
    assert "[msgid: 0001]Alice: older message [@ Alice/user-1]" in result["source_text"]
    assert [block.block_id for block in state.short_term_blocks()] == ["ctx-1", "ctx-2", "ctx-3"]
    assert state.alias_table.pending_rebuild is False
    assert state.inactive_alias_table_frozen is True


def test_eviction_runtime_apply_resets_alias_snapshots_after_eviction() -> None:
    state = _state_with_sealed_blocks()
    runtime = ContextEvictionRuntime()

    result = runtime.apply(
        state,
        {"input_tokens": 100, "output_tokens": 20},
        max_context_tokens=1,
        evict_ratio=0.5,
        compressed_text="summary",
        now_ms=123,
    )

    assert result["triggered"] is True
    assert result["evicted_count"] == 1
    assert result["source_block_ids"] == ["ctx-1"]
    assert result["control_signal"] == {
        "type": "cache_release",
        "triggered": True,
        "reason": "token_budget_exceeded",
        "total_tokens": 120,
        "released_block_ids": ["ctx-1"],
        "remaining_count": 2,
        "compressed_memory_added": True,
        "alias_rebuild_requested": True,
        "inactive_alias_snapshot_reset": True,
        "source_text": "",
    }
    assert [block.block_id for block in state.short_term_blocks()] == ["ctx-2", "ctx-3"]
    assert state.compressed_memories[0].text == "summary"
    assert state.compressed_memories[0].source_block_ids == ["ctx-1"]
    assert state.alias_table.pending_rebuild is True
    assert state.inactive_alias_entries == []
    assert state.inactive_alias_table_frozen is False


def test_eviction_runtime_inactive_signal_preserves_noop_result() -> None:
    state = _state_with_sealed_blocks()
    runtime = ContextEvictionRuntime()

    result = runtime.preview(
        state,
        {"input_tokens": 1, "output_tokens": 1},
        max_context_tokens=10_000,
    )

    assert result["triggered"] is False
    assert result["control_signal"] == {
        "type": "cache_release",
        "triggered": False,
        "reason": "none",
        "total_tokens": 2,
        "released_block_ids": [],
        "remaining_count": 3,
        "compressed_memory_added": False,
        "alias_rebuild_requested": False,
        "inactive_alias_snapshot_reset": False,
        "source_text": "",
    }
