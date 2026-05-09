from __future__ import annotations

import json
from typing import Any

import pytest

from shinbot.agent.context.active_chat_context import ActiveChatContextBuilderAdapter
from shinbot.agent.model_runtime import GenerateResult, ModelCallError
from shinbot.agent.models.active_chat import (
    ActiveChatActionKind,
    ActiveChatBatch,
    ActiveChatMessageSignal,
    ActiveChatNoReplyIntensity,
)
from shinbot.agent.models.review import ReviewWorkflowExplanation
from shinbot.agent.prompts import PromptRegistry
from shinbot.agent.prompts.active_chat_prompt_registration import (
    register_active_chat_prompt_components,
)
from shinbot.agent.scheduler import ActiveChatDisposition, ActiveChatState
from shinbot.agent.tools.schema import ToolCallRequest, ToolCallResult
from shinbot.agent.workflows.active_chat import ActiveChatFastRunner


class FakeModelRuntime:
    def __init__(self, responses: list[GenerateResult]) -> None:
        self.responses = list(responses)
        self.calls: list[Any] = []

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        return self.responses.pop(0)


class FailingModelRuntime:
    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        raise ModelCallError("model failed")


class FailingRepairModelRuntime:
    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        if len(self.calls) == 1:
            return make_result(text="I would reply without tools.")
        raise ModelCallError("repair failed")


class FakeToolManager:
    def __init__(self) -> None:
        self.calls: list[ToolCallRequest] = []

    def export_model_tools(self, **_kwargs) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "send_reply",
                    "description": "Send reply",
                    "parameters": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "no_reply",
                    "description": "Do not reply",
                    "parameters": {
                        "type": "object",
                        "properties": {"internal_summary": {"type": "string"}},
                        "required": [],
                    },
                },
            },
        ]

    async def execute(self, call: ToolCallRequest) -> ToolCallResult:
        self.calls.append(call)
        return ToolCallResult(
            tool_name=call.tool_name,
            success=True,
            output={"action": call.tool_name, "message_log_id": 9001},
        )


class FakeMessageStore:
    def get(self, msg_id: int) -> dict[str, Any] | None:
        return {
            "id": msg_id,
            "session_id": "bot:group:room",
            "sender_id": "alice",
            "sender_name": "Alice",
            "role": "user",
            "raw_text": f"message {msg_id}",
            "created_at": 1234.0 + msg_id,
        }


class FakeContextManager:
    def __init__(self) -> None:
        self.instruction_calls: list[dict[str, Any]] = []
        self.context_calls: list[dict[str, Any]] = []

    def build_instruction_stage_content(
        self,
        session_id: str,
        unread_records: list[dict[str, Any]],
        *,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        self.instruction_calls.append(
            {
                "session_id": session_id,
                "message_ids": [record["id"] for record in unread_records],
                "previous_summary": previous_summary,
                "self_platform_id": self_platform_id,
                "now_ms": now_ms,
            }
        )
        return [{"type": "text", "text": "Active batch from context builder"}]

    def build_context_stage_messages(
        self,
        session_id: str,
        *,
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        self.context_calls.append(
            {
                "session_id": session_id,
                "self_platform_id": self_platform_id,
                "now_ms": now_ms,
            }
        )
        return [
            {
                "role": "user",
                "content": [{"type": "text", "text": "Recent tail context"}],
            }
        ]


class BrokenContextBuilder:
    def build_for_messages(self, **_kwargs: Any) -> object:
        raise RuntimeError("context build failed")


def make_result(*, text: str = "", tool_calls: list[dict[str, Any]] | None = None) -> GenerateResult:
    return GenerateResult(
        text=text,
        tool_calls=list(tool_calls or []),
        raw_response={},
        execution_id="exec-1",
        route_id="",
        provider_id="",
        model_id="",
        usage={},
    )


def make_tool_call(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"call-{name}",
        "type": "function",
        "function": {
            "name": name,
            "arguments": json.dumps(arguments),
        },
    }


def make_batch(
    *,
    review_result_summary: Any | None = None,
    self_platform_id: str = "",
    conversation_summary: str = "",
    conversation_messages: list[dict[str, Any]] | None = None,
) -> ActiveChatBatch:
    active_state = ActiveChatState(
        session_id="bot:group:room",
        interest_value=42.0,
        decay_half_life_seconds=20.0,
        entered_at=1000.0,
        updated_at=1000.0,
    )
    return ActiveChatBatch(
        session_id="bot:group:room",
        messages=[
            ActiveChatMessageSignal(
                session_id="bot:group:room",
                message_log_id=101,
                sender_id="alice",
                response_profile="balanced",
                self_platform_id=self_platform_id,
            )
        ],
        active_chat_state=active_state,
        response_profile="balanced",
        review_result_summary=(
            {"summary": "review found a running topic"}
            if review_result_summary is None
            else review_result_summary
        ),
        conversation_summary=conversation_summary,
        conversation_messages=list(conversation_messages or []),
    )


@pytest.mark.asyncio
async def test_active_chat_fast_runner_uses_prompt_registry_and_tool_loop() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call(
                        "send_reply",
                        {"text": "收到", "intensity": "engaged"},
                    )
                ]
            )
        ]
    )
    tool_manager = FakeToolManager()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=tool_manager,
        message_store=FakeMessageStore(),
    )

    result = await runner.run(make_batch())

    assert result.success is True
    assert result.action == ActiveChatActionKind.SEND_REPLY
    assert tool_manager.calls[0].tool_name == "send_reply"
    tool_names = [
        tool["function"]["name"]
        for tool in model_runtime.calls[0].tools
    ]
    assert tool_names == ["send_reply", "no_reply", "request_think_mode", "exit_active"]
    assert model_runtime.calls[0].purpose == "active_chat_fast"
    assert model_runtime.calls[0].metadata["message_log_ids"] == [101]
    assert model_runtime.calls[0].metadata["review_result_summary"] == {
        "summary": "review found a running topic"
    }
    assert result.consumed_message_log_ids == [101]
    assert [message["role"] for message in result.conversation_messages_delta] == [
        "assistant",
        "tool",
    ]
    assert result.conversation_messages_delta[0]["tool_calls"][0]["function"]["name"] == (
        "send_reply"
    )
    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "current active_chat batch is the primary target" in rendered_prompt_text
    assert "never output numeric interest" in rendered_prompt_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_injects_previous_conversation_trace() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    await runner.run(
        make_batch(
            conversation_messages=[
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        make_tool_call("send_reply", {"text": "上一轮回复"})
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call-send_reply",
                    "content": "{\"ok\": true}",
                },
            ]
        )
    )

    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "send_reply" in rendered_prompt_text
    assert "call-send_reply" in rendered_prompt_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_sanitizes_conversation_trace() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    await runner.run(
        make_batch(
            conversation_messages=[
                {
                    "role": "tool",
                    "tool_call_id": "call-orphan",
                    "content": "{\"action\": \"send_reply\"}",
                },
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        make_tool_call("send_reply", {"text": "missing result"}),
                    ],
                },
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        make_tool_call("send_reply", {"text": "valid result"}),
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call-send_reply",
                    "content": "{\"action\": \"send_reply\"}",
                },
            ]
        )
    )

    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "call-orphan" not in rendered_prompt_text
    assert "missing result" not in rendered_prompt_text
    assert "valid result" in rendered_prompt_text
    assert "call-send_reply" in rendered_prompt_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_orders_context_before_current_batch() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    context_manager = FakeContextManager()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        context_builder=ActiveChatContextBuilderAdapter(context_manager),
    )

    await runner.run(
        make_batch(
            self_platform_id="bot-self",
            conversation_summary="{\"compacted_messages\": 2}",
            conversation_messages=[
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        make_tool_call("send_reply", {"text": "previous reply"}),
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call-send_reply",
                    "content": "{\"action\": \"send_reply\"}",
                },
            ],
        )
    )

    messages = model_runtime.calls[0].messages
    assert [message["role"] for message in messages] == [
        "system",
        "user",
        "user",
        "assistant",
        "tool",
        "user",
    ]
    assert "Recent tail context" in str(messages[1]["content"])
    assert "compacted_messages" in str(messages[2]["content"])
    assert messages[3]["tool_calls"][0]["id"] == "call-send_reply"
    assert messages[4]["tool_call_id"] == "call-send_reply"
    assert "Message log ids: [101]" in str(messages[-1]["content"])
    assert "current active_chat batch is the primary target" in str(messages[-1]["content"])


@pytest.mark.asyncio
async def test_active_chat_fast_runner_injects_compacted_conversation_summary() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    await runner.run(
        make_batch(
            conversation_summary='{"compacted_messages": 4, "recent_tool_actions": ["send_reply"]}'
        )
    )

    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "Active chat compacted conversation trace summary" in rendered_prompt_text
    assert "compacted_messages" in rendered_prompt_text
    assert "send_reply" in rendered_prompt_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_repairs_toolless_output_once() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(text="I would say nothing."),
            make_result(
                tool_calls=[
                    make_tool_call(
                        "no_reply",
                        {"internal_summary": "low value", "intensity": "strong"},
                    )
                ]
            ),
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    result = await runner.run(make_batch())

    assert result.success is True
    assert result.action == ActiveChatActionKind.NO_REPLY
    assert result.no_reply_intensity == ActiveChatNoReplyIntensity.STRONG
    assert len(model_runtime.calls) == 2
    assert model_runtime.calls[1].metadata["repair_attempt"] == 1
    assert model_runtime.calls[1].messages[-1]["role"] == "system"


@pytest.mark.asyncio
async def test_active_chat_fast_runner_reports_unconsumable_prompt_build_failure() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime([])
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        context_builder=BrokenContextBuilder(),
    )

    result = await runner.run(make_batch())

    assert result.success is False
    assert result.action == ActiveChatActionKind.RETRY_FAILED
    assert result.reason == "active_chat_prompt_build_failed"
    assert result.consumed_message_log_ids == []
    assert model_runtime.calls == []


@pytest.mark.asyncio
async def test_active_chat_fast_runner_reports_unconsumable_model_failure() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FailingModelRuntime()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    result = await runner.run(make_batch())

    assert result.success is False
    assert result.action == ActiveChatActionKind.RETRY_FAILED
    assert result.reason == "active_chat_model_call_failed"
    assert result.consumed_message_log_ids == []
    assert len(model_runtime.calls) == 1


@pytest.mark.asyncio
async def test_active_chat_fast_runner_merges_pending_messages_into_repair() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(text="I would reply without tools."),
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "merged batch"})
                ]
            ),
        ]
    )

    async def pending_provider(batch: ActiveChatBatch) -> list[ActiveChatMessageSignal]:
        return [
            ActiveChatMessageSignal(
                session_id=batch.session_id,
                message_log_id=102,
                sender_id="bob",
                response_profile="balanced",
                active_chat_state=batch.active_chat_state,
            )
        ]

    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        pending_message_provider=pending_provider,
    )

    result = await runner.run(make_batch())

    assert result.success is True
    assert result.action == ActiveChatActionKind.NO_REPLY
    assert result.consumed_message_log_ids == [101, 102]
    assert model_runtime.calls[0].metadata["message_log_ids"] == [101]
    assert model_runtime.calls[1].metadata["message_log_ids"] == [101, 102]


@pytest.mark.asyncio
async def test_active_chat_fast_runner_restores_repair_batch_on_failed_repair() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FailingRepairModelRuntime()

    async def pending_provider(batch: ActiveChatBatch) -> list[ActiveChatMessageSignal]:
        return [
            ActiveChatMessageSignal(
                session_id=batch.session_id,
                message_log_id=102,
                sender_id="bob",
                response_profile="balanced",
                active_chat_state=batch.active_chat_state,
            )
        ]

    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        pending_message_provider=pending_provider,
    )

    result = await runner.run(make_batch())

    assert result.success is False
    assert result.action == ActiveChatActionKind.RETRY_FAILED
    assert result.reason == "active_chat_toolless_repair_failed"
    assert result.consumed_message_log_ids == []
    assert [message.message_log_id for message in result.restored_messages] == [101, 102]
    assert model_runtime.calls[0].metadata["message_log_ids"] == [101]
    assert model_runtime.calls[1].metadata["message_log_ids"] == [101, 102]


@pytest.mark.asyncio
async def test_active_chat_fast_runner_injects_active_context_messages() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    context_manager = FakeContextManager()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        context_builder=ActiveChatContextBuilderAdapter(context_manager),
    )

    result = await runner.run(make_batch(self_platform_id="bot-self"))

    assert result.success is True
    assert context_manager.instruction_calls[0]["self_platform_id"] == "bot-self"
    assert context_manager.context_calls[0]["self_platform_id"] == "bot-self"
    assert any(
        "Recent tail context" in str(message.get("content", ""))
        for message in model_runtime.calls[0].messages
    )


@pytest.mark.asyncio
async def test_active_chat_fast_runner_accepts_dataclass_review_handoff_summary() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    handoff = ReviewWorkflowExplanation(
        review_started_at=123.0,
        candidate_message_ids=[99],
        active_chat_disposition=ActiveChatDisposition.EXIT_SOON,
        active_chat_reason="low interest",
    )
    result = await runner.run(make_batch(review_result_summary=handoff))

    assert result.success is True
    assert result.action == ActiveChatActionKind.NO_REPLY
    assert model_runtime.calls[0].metadata["review_result_summary"][
        "active_chat_disposition"
    ] == "exit_soon"
    handoff_blocks = [
        block
        for message in model_runtime.calls[0].messages
        for block in (
            message.get("content", [])
            if isinstance(message.get("content"), list)
            else []
        )
        if "Review handoff summary JSON" in str(block.get("text", ""))
    ]
    assert handoff_blocks
