from __future__ import annotations

from collections import deque
from typing import Any

from shinbot.agent.context import ContextPromptRuntime
from shinbot.agent.context.state.alias_table import SessionAliasTable
from shinbot.agent.context.state.state_store import ContextBlockState, ContextSessionState


class FakePoolRuntime:
    def __init__(self) -> None:
        self.pool = type("Pool", (), {"messages": deque([{"id": 1, "role": "user"}])})()

    def get_pool(self, session_id: str):
        return self.pool

    def get_recent_messages(self, session_id: str, *, read_only: bool = True):
        return [{"id": 1, "role": "user", "raw_text": "hello"}]


class FakeSessionRuntime:
    def __init__(self) -> None:
        self.state = ContextSessionState(session_id="s-prompt")
        self.saved: list[str] = []

    def get_state(self, session_id: str) -> ContextSessionState:
        return self.state

    def save(self, session_id: str) -> bool:
        self.saved.append(session_id)
        return True


class FakeAliasRuntime:
    def needs_table_rebuild(
        self,
        table: SessionAliasTable,
        now_ms: int,
        *,
        force: bool = False,
    ) -> bool:
        return force or not table.entries

    def rebuild_table(
        self,
        table: SessionAliasTable,
        messages: list[dict[str, Any]],
        *,
        now_ms: int,
        force: bool = False,
        identity_store=None,
    ):
        table.entries.setdefault("user-1", type("Entry", (), {"alias": "A0"})())
        return table, True

    def sync_identity_display_name(self, table, identity_store, *, user_id: str, now_ms: int):
        return False

    def needs_inactive_context_refresh(self, *, state, blocks, now_ms: int) -> bool:
        return not blocks

    def build_inactive_context_message(self, *, state, blocks, unread_records=None):
        return {"role": "user", "content": [{"type": "text", "text": "inactive"}]}, True

    def needs_active_context_refresh(self, blocks) -> bool:
        return not blocks

    def needs_active_alias_rebuild(self, table, now_ms: int) -> bool:
        return False

    def build_active_constraint_text(self, *, alias_table, blocks, unread_records=None) -> str:
        return "active"


class FakeContextStageRuntime:
    def build_messages(
        self,
        read_history,
        *,
        alias_table,
        session_state,
        alias_changed=False,
        self_platform_id="",
    ):
        session_state.set_short_term_blocks([ContextBlockState(block_id="ctx-1", sealed=False)])
        return [{"role": "user", "content": [{"type": "text", "text": "context"}]}]


class FakeInstructionRuntime:
    def build_content_blocks(
        self,
        unread_records,
        *,
        alias_table,
        session_state,
        previous_summary="",
        self_platform_id="",
        now_ms=None,
    ):
        return [{"type": "text", "text": "instruction"}]


def test_prompt_runtime_builds_context_stage_and_saves_state() -> None:
    session_runtime = FakeSessionRuntime()
    runtime = ContextPromptRuntime(
        pool_runtime=FakePoolRuntime(),  # type: ignore[arg-type]
        session_runtime=session_runtime,  # type: ignore[arg-type]
        alias_runtime=FakeAliasRuntime(),  # type: ignore[arg-type]
        context_stage_runtime=FakeContextStageRuntime(),  # type: ignore[arg-type]
        instruction_runtime=FakeInstructionRuntime(),  # type: ignore[arg-type]
    )

    messages = runtime.build_context_stage_messages("s-prompt", now_ms=1)

    assert messages[0]["content"][0]["text"] == "context"
    assert session_runtime.saved == ["s-prompt", "s-prompt"]
    assert runtime.get_cacheable_context_message_count("s-prompt") == 0


def test_prompt_runtime_builds_alias_and_instruction_projection() -> None:
    session_runtime = FakeSessionRuntime()
    runtime = ContextPromptRuntime(
        pool_runtime=FakePoolRuntime(),  # type: ignore[arg-type]
        session_runtime=session_runtime,  # type: ignore[arg-type]
        alias_runtime=FakeAliasRuntime(),  # type: ignore[arg-type]
        context_stage_runtime=FakeContextStageRuntime(),  # type: ignore[arg-type]
        instruction_runtime=FakeInstructionRuntime(),  # type: ignore[arg-type]
    )

    inactive = runtime.build_inactive_alias_context_message("s-prompt", now_ms=1)
    instruction = runtime.build_instruction_stage_content(
        "s-prompt",
        [{"id": 2, "role": "user", "raw_text": "new"}],
        now_ms=1,
    )
    active = runtime.build_active_alias_constraint_text("s-prompt", now_ms=1)

    assert inactive is not None
    assert inactive["content"][0]["text"] == "inactive"
    assert instruction[0]["text"] == "instruction"
    assert active == "active"
