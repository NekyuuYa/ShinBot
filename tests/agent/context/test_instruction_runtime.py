from __future__ import annotations

from shinbot.agent.context import InstructionRuntime
from shinbot.agent.context.state.state_store import ContextSessionState


def test_instruction_runtime_projects_unread_records_with_session_projection_state() -> None:
    class FakeBuilder:
        image_registry = object()

        def build_content_blocks(
            self,
            unread_records,
            *,
            alias_table,
            projection_state,
            previous_summary="",
            self_platform_id="",
            now_ms=None,
        ):
            message_id = projection_state.assign_message_id(unread_records[0])
            return [
                {
                    "type": "text",
                    "text": f"{alias_table.session_id}:{message_id}:{previous_summary}:{self_platform_id}:{now_ms}",
                }
            ]

    state = ContextSessionState(session_id="s-instruction")
    blocks = InstructionRuntime(FakeBuilder()).build_content_blocks(
        [{"id": 42, "raw_text": "hello"}],
        alias_table=state.alias_table,
        session_state=state,
        previous_summary="summary",
        self_platform_id="bot",
        now_ms=123,
    )

    assert blocks == [
        {
            "type": "text",
            "text": "s-instruction:0001:summary:bot:123",
        }
    ]
