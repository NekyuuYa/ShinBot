from __future__ import annotations

from shinbot.agent.context import (
    LongTermMemoryItem,
    PromptMemoryAssembler,
    PromptMemoryProjectionRequest,
)


def test_prompt_memory_assembler_orders_context_instruction_and_constraints() -> None:
    class FakeRuntime:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def build_context_stage_messages(self, session_id, *, self_platform_id="", now_ms=None):
            self.calls.append(f"context:{session_id}:{self_platform_id}:{now_ms}")
            return [{"role": "user", "content": [{"type": "text", "text": "context"}]}]

        def build_inactive_alias_context_message(
            self,
            session_id,
            *,
            unread_records=None,
            now_ms=None,
        ):
            self.calls.append(f"inactive:{session_id}:{len(unread_records or [])}:{now_ms}")
            return {"role": "user", "content": [{"type": "text", "text": "inactive aliases"}]}

        def get_cacheable_context_message_count(self, session_id):
            self.calls.append(f"cacheable:{session_id}")
            return 1

        def build_instruction_stage_content(
            self,
            session_id,
            unread_records,
            *,
            previous_summary="",
            self_platform_id="",
            now_ms=None,
        ):
            self.calls.append(
                f"instruction:{session_id}:{len(unread_records)}:{previous_summary}:{self_platform_id}:{now_ms}"
            )
            return [{"type": "text", "text": "instruction"}]

        def build_active_alias_constraint_text(self, session_id, *, unread_records=None, now_ms=None):
            self.calls.append(f"constraint:{session_id}:{len(unread_records or [])}:{now_ms}")
            return "constraint"

    class FakeLongTermProvider:
        def __init__(self, runtime) -> None:
            self.runtime = runtime

        def retrieve(self, request):
            self.runtime.calls.append(
                f"long-term:{request.session_id}:{len(request.unread_records)}"
            )
            return []

    runtime = FakeRuntime()
    bundle = PromptMemoryAssembler(
        runtime,
        long_term_provider=FakeLongTermProvider(runtime),
    ).assemble(
        PromptMemoryProjectionRequest(
            session_id="s-assemble",
            unread_records=[{"id": 1, "raw_text": "hello"}],
            previous_summary="summary",
            self_platform_id="bot",
            now_ms=123,
        )
    )

    assert [message["content"][0]["text"] for message in bundle.context_messages] == [
        "inactive aliases",
        "context",
    ]
    assert bundle.instruction_blocks == [{"type": "text", "text": "instruction"}]
    assert bundle.constraint_text == "constraint"
    assert bundle.cacheable_message_count == 2
    assert bundle.metadata == {"session_id": "s-assemble", "message_count": 1}
    assert runtime.calls == [
        "long-term:s-assemble:1",
        "context:s-assemble:bot:123",
        "inactive:s-assemble:1:123",
        "cacheable:s-assemble",
        "instruction:s-assemble:1:summary:bot:123",
        "constraint:s-assemble:1:123",
    ]


def test_prompt_memory_assembler_prepends_long_term_memory_messages() -> None:
    class FakeRuntime:
        def build_context_stage_messages(self, session_id, *, self_platform_id="", now_ms=None):
            return [{"role": "user", "content": [{"type": "text", "text": "short term"}]}]

        def build_inactive_alias_context_message(
            self,
            session_id,
            *,
            unread_records=None,
            now_ms=None,
        ):
            return None

        def get_cacheable_context_message_count(self, session_id):
            return 1

        def build_instruction_stage_content(
            self,
            session_id,
            unread_records,
            *,
            previous_summary="",
            self_platform_id="",
            now_ms=None,
        ):
            return []

        def build_active_alias_constraint_text(self, session_id, *, unread_records=None, now_ms=None):
            return ""

    class FakeLongTermProvider:
        def retrieve(self, request):
            return [
                LongTermMemoryItem(text="likes green tea"),
                LongTermMemoryItem(text=""),
                LongTermMemoryItem(text="prefers concise replies"),
            ]

    bundle = PromptMemoryAssembler(
        FakeRuntime(),
        long_term_provider=FakeLongTermProvider(),
    ).assemble(PromptMemoryProjectionRequest(session_id="s-long-term"))

    assert [message["content"][0]["text"] for message in bundle.context_messages] == [
        "### 长期记忆\n- likes green tea\n- prefers concise replies",
        "short term",
    ]
    assert bundle.cacheable_message_count == 1
