from __future__ import annotations

import json
from typing import Any

import pytest

from shinbot.agent.active_chat import (
    ActiveChatActionKind,
    ActiveChatBatch,
    ActiveChatFastRunner,
    ActiveChatMessageSignal,
    ActiveChatNoReplyIntensity,
    register_active_chat_prompt_components,
)
from shinbot.agent.model_runtime import GenerateResult
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.review.models import ReviewWorkflowExplanation
from shinbot.agent.scheduler import ActiveChatDisposition, ActiveChatState
from shinbot.agent.tools.schema import ToolCallRequest, ToolCallResult


class FakeModelRuntime:
    def __init__(self, responses: list[GenerateResult]) -> None:
        self.responses = list(responses)
        self.calls: list[Any] = []

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        return self.responses.pop(0)


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


def make_batch(*, review_result_summary: Any | None = None) -> ActiveChatBatch:
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
            )
        ],
        active_chat_state=active_state,
        response_profile="balanced",
        review_result_summary=(
            {"summary": "review found a running topic"}
            if review_result_summary is None
            else review_result_summary
        ),
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
