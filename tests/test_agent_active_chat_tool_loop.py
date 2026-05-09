from __future__ import annotations

import json
from typing import Any

import pytest

from shinbot.agent.active_chat import (
    ActiveChatActionKind,
    ActiveChatNoReplyIntensity,
    ActiveChatReplyIntensity,
    ActiveChatToolLoop,
)
from shinbot.agent.tools.schema import ToolCallRequest, ToolCallResult


class FakeToolManager:
    def __init__(self) -> None:
        self.calls: list[ToolCallRequest] = []

    async def execute(self, call: ToolCallRequest) -> ToolCallResult:
        self.calls.append(call)
        if call.tool_name == "send_reply":
            return ToolCallResult(
                tool_name=call.tool_name,
                success=True,
                output={
                    "action": "send_reply",
                    "sent": True,
                    "length": len(str(call.arguments.get("text", ""))),
                    "platform_msg_id": "platform-secret",
                    "message_log_id": 9001,
                    "quote_message_id": "quoted-platform-message",
                    "terminate_round": True,
                    "hint": "internal send hint",
                },
            )
        if call.tool_name == "send_poke":
            return ToolCallResult(
                tool_name=call.tool_name,
                success=True,
                output={
                    "action": "send_poke",
                    "sent": True,
                    "user_id": call.arguments.get("user_id", ""),
                    "session_type": "group",
                    "adapter_result": {"raw": "platform detail"},
                    "terminate_round": True,
                    "hint": "internal poke hint",
                },
            )
        if call.tool_name == "fail_tool":
            return ToolCallResult(
                tool_name=call.tool_name,
                success=False,
                error_code="failed",
                error_message="tool failed",
            )
        return ToolCallResult(
            tool_name=call.tool_name,
            success=True,
            output={"action": call.tool_name, "arguments": call.arguments},
        )


def make_tool_call(name: str, arguments: dict[str, Any], *, call_id: str = "") -> dict[str, Any]:
    return {
        "id": call_id or f"call-{name}",
        "type": "function",
        "function": {
            "name": name,
            "arguments": json.dumps(arguments),
        },
    }


@pytest.mark.asyncio
async def test_active_chat_tool_loop_allows_multiple_replies_in_order() -> None:
    manager = FakeToolManager()
    loop = ActiveChatToolLoop()

    result = await loop.execute(
        [
            make_tool_call("send_reply", {"text": "first"}, call_id="call-1"),
            make_tool_call(
                "send_reply",
                {"text": "second", "intensity": "engaged"},
                call_id="call-2",
            ),
        ],
        tool_manager=manager,
        instance_id="bot",
        session_id="bot:group:room",
        run_id="run-1",
    )

    assert [call.tool_name for call in manager.calls] == ["send_reply", "send_reply"]
    assert [call.arguments["text"] for call in manager.calls] == ["first", "second"]
    assert result.round_result.success is True
    assert result.round_result.action == ActiveChatActionKind.SEND_REPLY
    assert result.round_result.reply_intensity == ActiveChatReplyIntensity.ENGAGED
    assert [message["tool_call_id"] for message in result.tool_messages] == [
        "call-1",
        "call-2",
    ]
    first_tool_content = json.loads(result.tool_messages[0]["content"])
    assert first_tool_content == {
        "success": True,
        "action": "send_reply",
        "sent": True,
        "message_log_id": 9001,
        "quote_message_id": "quoted-platform-message",
        "terminate_round": True,
        "text_length": 5,
    }
    assert "platform-secret" not in result.tool_messages[0]["content"]
    assert "internal send hint" not in result.tool_messages[0]["content"]


@pytest.mark.asyncio
async def test_active_chat_tool_loop_allows_poke_as_independent_action() -> None:
    manager = FakeToolManager()
    loop = ActiveChatToolLoop()

    result = await loop.execute(
        [make_tool_call("send_poke", {"user_id": "alice"})],
        tool_manager=manager,
        instance_id="bot",
        session_id="bot:group:room",
    )

    assert [call.tool_name for call in manager.calls] == ["send_poke"]
    assert result.round_result.success is True
    assert result.round_result.action == ActiveChatActionKind.SEND_POKE
    assert json.loads(result.tool_messages[0]["content"]) == {
        "success": True,
        "action": "send_poke",
        "sent": True,
        "user_id": "alice",
        "session_type": "group",
        "terminate_round": True,
    }
    assert "platform detail" not in result.tool_messages[0]["content"]


@pytest.mark.asyncio
async def test_active_chat_tool_loop_maps_strong_no_reply() -> None:
    manager = FakeToolManager()
    loop = ActiveChatToolLoop()

    result = await loop.execute(
        [
            make_tool_call(
                "no_reply",
                {"internal_summary": "not worth joining", "intensity": "strong"},
            )
        ],
        tool_manager=manager,
        instance_id="bot",
        session_id="bot:group:room",
    )

    assert manager.calls[0].arguments["intensity"] == "strong"
    assert result.round_result.success is True
    assert result.round_result.action == ActiveChatActionKind.NO_REPLY
    assert result.round_result.no_reply_intensity == ActiveChatNoReplyIntensity.STRONG


@pytest.mark.asyncio
async def test_active_chat_tool_loop_treats_exit_active_as_virtual_reasoned_action() -> None:
    manager = FakeToolManager()
    loop = ActiveChatToolLoop()

    result = await loop.execute(
        [make_tool_call("exit_active", {"reason": "conversation cooled down"})],
        tool_manager=manager,
        instance_id="bot",
        session_id="bot:group:room",
    )

    assert manager.calls == []
    assert result.round_result.success is True
    assert result.round_result.action == ActiveChatActionKind.EXIT_ACTIVE
    assert result.round_result.reason == "conversation cooled down"


@pytest.mark.asyncio
async def test_active_chat_tool_loop_rejects_exit_active_without_reason() -> None:
    manager = FakeToolManager()
    loop = ActiveChatToolLoop()

    result = await loop.execute(
        [make_tool_call("exit_active", {})],
        tool_manager=manager,
        instance_id="bot",
        session_id="bot:group:room",
    )

    assert manager.calls == []
    assert result.round_result.success is True
    assert result.round_result.action == ActiveChatActionKind.RETRY_FAILED
    assert result.invalid_reason == "exit_active_missing_reason"


@pytest.mark.asyncio
async def test_active_chat_tool_loop_maps_all_failed_calls_to_retry_failed_action() -> None:
    manager = FakeToolManager()
    loop = ActiveChatToolLoop()

    result = await loop.execute(
        [make_tool_call("fail_tool", {})],
        tool_manager=manager,
        instance_id="bot",
        session_id="bot:group:room",
    )

    assert [call.tool_name for call in manager.calls] == ["fail_tool"]
    assert result.round_result.success is True
    assert result.round_result.action == ActiveChatActionKind.RETRY_FAILED
    assert result.round_result.reason == "tool failed"
    assert json.loads(result.tool_messages[0]["content"]) == {
        "success": False,
        "action": "fail_tool",
        "error": "tool failed",
    }


@pytest.mark.asyncio
async def test_active_chat_tool_loop_maps_request_think_mode() -> None:
    manager = FakeToolManager()
    loop = ActiveChatToolLoop()

    result = await loop.execute(
        [make_tool_call("request_think_mode", {"reason": "needs careful reply"})],
        tool_manager=manager,
        instance_id="bot",
        session_id="bot:group:room",
    )

    assert manager.calls == []
    assert result.round_result.success is True
    assert result.round_result.action == ActiveChatActionKind.REQUEST_THINK_MODE
    assert result.round_result.reason == "needs careful reply"
