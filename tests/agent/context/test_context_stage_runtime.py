from __future__ import annotations

from shinbot.agent.context import ContextStageRuntime
from shinbot.agent.context.state.state_store import (
    CompressedMemoryState,
    ContextBlockState,
    ContextSessionState,
)


class FakeTimelineRuntime:
    def __init__(self) -> None:
        self.force_rebuild = False

    def build_prompt_messages(
        self,
        read_history,
        *,
        alias_table,
        session_state,
        force_rebuild=False,
        self_platform_id="",
    ):
        self.force_rebuild = force_rebuild
        return [{"role": "user", "content": [{"type": "text", "text": "short-term"}]}]


def test_context_stage_runtime_prepends_compressed_memory_and_keeps_stable_alias_snapshot() -> None:
    timeline_runtime = FakeTimelineRuntime()
    runtime = ContextStageRuntime(timeline_runtime=timeline_runtime)
    state = ContextSessionState(session_id="s-stage")
    state.compressed_memories = [CompressedMemoryState(text="summary")]
    state.inactive_alias_entries = [
        {"alias": "P0", "platform_id": "user-1", "display_name": "Alice"}
    ]
    state.inactive_alias_table_frozen = True
    state.set_short_term_blocks([ContextBlockState(block_id="ctx-1", sealed=False)])

    messages = runtime.build_messages(
        [],
        alias_table=state.alias_table,
        session_state=state,
        alias_changed=False,
    )

    assert timeline_runtime.force_rebuild is False
    assert messages[0]["content"][0]["text"] == "### 压缩记忆\nsummary"
    assert messages[1]["content"][0]["text"] == "short-term"
    assert state.inactive_alias_table_frozen is True


def test_context_stage_runtime_resets_alias_snapshot_when_rebuilding_timeline() -> None:
    timeline_runtime = FakeTimelineRuntime()
    runtime = ContextStageRuntime(timeline_runtime=timeline_runtime)
    state = ContextSessionState(session_id="s-stage")
    state.inactive_alias_entries = [
        {"alias": "P0", "platform_id": "user-1", "display_name": "Alice"}
    ]
    state.inactive_alias_table_frozen = True
    state.set_short_term_blocks([ContextBlockState(block_id="ctx-1", sealed=False)])

    runtime.build_messages(
        [],
        alias_table=state.alias_table,
        session_state=state,
        alias_changed=True,
    )

    assert timeline_runtime.force_rebuild is True
    assert state.inactive_alias_entries == []
    assert state.inactive_alias_table_frozen is False
