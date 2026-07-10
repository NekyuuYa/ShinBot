from __future__ import annotations

import asyncio

import pytest

from shinbot.agent.services.tools import ToolCallRequest, ToolManager, ToolRegistry
from shinbot.agent.workflows.chat_actions.tool_registration import (
    SendReplyIdempotencyStore,
    register_chat_action_tools,
)


class FakeSendHandle:
    def __init__(self, message_id: str) -> None:
        self.message_id = message_id


class FakeAdapter:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.instance_id = "bot"
        self.platform = "test"
        self.fail_once = fail_once
        self.sent: list[tuple[str, list[object]]] = []
        self.api_calls: list[tuple[str, dict[str, object]]] = []

    async def send(self, session_id: str, elements: list[object]) -> FakeSendHandle:
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("temporary send failure")
        self.sent.append((session_id, list(elements)))
        return FakeSendHandle(message_id=f"platform-{len(self.sent)}")

    async def call_api(self, method: str, params: dict[str, object]) -> dict[str, object]:
        self.api_calls.append((method, dict(params)))
        return {"ok": True, "method": method}


class CancellationAfterAcceptAdapter(FakeAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.accepted = asyncio.Event()
        self.send_attempts = 0

    async def send(self, session_id: str, elements: list[object]) -> FakeSendHandle:
        self.send_attempts += 1
        self.accepted.set()
        await asyncio.Event().wait()
        return FakeSendHandle(message_id="unreachable")


class FakeAdapterManager:
    def __init__(self, adapter: FakeAdapter, *, connected: bool = True) -> None:
        self.adapter = adapter
        self.connected = connected

    def get_instance(self, instance_id: str) -> FakeAdapter | None:
        return self.adapter if instance_id == self.adapter.instance_id else None

    def is_connected(self, instance_id: str) -> bool:
        return instance_id == self.adapter.instance_id and self.connected


def _register_tools(
    adapter: FakeAdapter,
    *,
    store: SendReplyIdempotencyStore | None = None,
    connected: bool = True,
) -> ToolManager:
    registry = ToolRegistry()
    register_chat_action_tools(
        registry,
        adapter_manager=FakeAdapterManager(adapter, connected=connected),  # type: ignore[arg-type]
        send_reply_idempotency_store=store,
    )
    return ToolManager(registry)


def test_send_reply_idempotency_key_is_not_exported_to_model_schema() -> None:
    manager = _register_tools(FakeAdapter())

    tools = manager.build_request_tools(
        ["send_reply"],
        caller="test",
        instance_id="bot",
        session_id="bot:group:room",
    )

    properties = tools[0]["function"]["parameters"]["properties"]
    assert "idempotency_key" not in properties


@pytest.mark.asyncio
async def test_send_reply_idempotency_store_deduplicates_completed_key() -> None:
    adapter = FakeAdapter()
    manager = _register_tools(adapter, store=SendReplyIdempotencyStore())

    first = await manager.execute(
        ToolCallRequest(
            tool_name="send_reply",
            arguments={"text": "hello", "idempotency_key": "review:1:0"},
            caller="test.review",
            instance_id="bot",
            session_id="bot:group:room",
        )
    )
    second = await manager.execute(
        ToolCallRequest(
            tool_name="send_reply",
            arguments={"text": "hello again", "idempotency_key": "review:1:0"},
            caller="test.review",
            instance_id="bot",
            session_id="bot:group:room",
        )
    )

    assert first.success is True
    assert second.success is True
    assert first.output["sent"] is True
    assert second.output["sent"] is False
    assert second.output["deduplicated"] is True
    assert second.output["deduplicated_reason"] == "completed"
    assert len(adapter.sent) == 1


@pytest.mark.asyncio
async def test_send_reply_idempotency_releases_key_after_send_failure() -> None:
    adapter = FakeAdapter(fail_once=True)
    manager = _register_tools(adapter, store=SendReplyIdempotencyStore())

    failed = await manager.execute(
        ToolCallRequest(
            tool_name="send_reply",
            arguments={"text": "hello", "idempotency_key": "review:1:0"},
            caller="test.review",
            instance_id="bot",
            session_id="bot:group:room",
        )
    )
    retried = await manager.execute(
        ToolCallRequest(
            tool_name="send_reply",
            arguments={"text": "hello", "idempotency_key": "review:1:0"},
            caller="test.review",
            instance_id="bot",
            session_id="bot:group:room",
        )
    )

    assert failed.success is False
    assert failed.error_code == "tool_execution_failed"
    assert retried.success is True
    assert retried.output["sent"] is True
    assert len(adapter.sent) == 1


@pytest.mark.asyncio
async def test_send_reply_cancellation_keeps_key_deduplicated() -> None:
    adapter = CancellationAfterAcceptAdapter()
    manager = _register_tools(adapter, store=SendReplyIdempotencyStore())
    call = ToolCallRequest(
        tool_name="send_reply",
        arguments={"text": "hello", "idempotency_key": "review:1:0"},
        caller="test.review",
        instance_id="bot",
        session_id="bot:group:room",
    )

    send_task = asyncio.create_task(manager.execute(call))
    await adapter.accepted.wait()
    send_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await send_task

    retried = await manager.execute(call)

    assert retried.success is True
    assert retried.output["sent"] is False
    assert retried.output["deduplicated"] is True
    assert retried.output["deduplicated_reason"] == "completed"
    assert adapter.send_attempts == 1


def test_send_reply_idempotency_store_prunes_by_ttl_and_capacity() -> None:
    now = [100.0]
    store = SendReplyIdempotencyStore(
        ttl_seconds=10.0,
        max_entries=2,
        now=lambda: now[0],
    )

    assert store.begin("a").accepted is True
    store.finish("a")
    now[0] = 101.0
    assert store.begin("b").accepted is True
    store.finish("b")
    now[0] = 102.0
    assert store.begin("c").accepted is True
    store.finish("c")

    assert store.begin("a").accepted is True
    store.release("a")
    assert store.begin("b").accepted is False
    now[0] = 200.0
    assert store.begin("b").accepted is True


@pytest.mark.asyncio
async def test_send_reply_fails_when_adapter_is_offline() -> None:
    adapter = FakeAdapter()
    manager = _register_tools(adapter, connected=False)

    result = await manager.execute(
        ToolCallRequest(
            tool_name="send_reply",
            arguments={"text": "hello"},
            caller="test.review",
            instance_id="bot",
            session_id="bot:group:room",
        )
    )

    assert result.success is False
    assert result.error_code == "tool_execution_failed"
    assert "offline" in result.error_message.lower()
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_send_reaction_calls_adapter_reaction_api() -> None:
    adapter = FakeAdapter()
    manager = _register_tools(adapter)

    result = await manager.execute(
        ToolCallRequest(
            tool_name="send_reaction",
            arguments={
                "message_id": "platform-msg-1",
                "emoji_id": "128077",
                "reason": "ack",
            },
            caller="test.active_chat",
            instance_id="bot",
            session_id="bot:group:room",
        )
    )

    assert result.success is True
    assert result.output["action"] == "send_reaction"
    assert result.output["sent"] is True
    assert result.output["message_id"] == "platform-msg-1"
    assert result.output["emoji_id"] == "128077"
    assert adapter.api_calls == [
        (
            "reaction.create",
            {
                "message_id": "platform-msg-1",
                "emoji_id": "128077",
                "session_id": "bot:group:room",
            },
        )
    ]


@pytest.mark.asyncio
async def test_send_reaction_invalid_action_fails_tool_call() -> None:
    adapter = FakeAdapter()
    manager = _register_tools(adapter)

    result = await manager.execute(
        ToolCallRequest(
            tool_name="send_reaction",
            arguments={
                "message_id": "platform-msg-1",
                "emoji_id": "128077",
                "action": "toggle",
            },
            caller="test.active_chat",
            instance_id="bot",
            session_id="bot:group:room",
        )
    )

    assert result.success is False
    assert result.error_code == "tool_execution_failed"
    assert "action must be" in result.error_message
    assert adapter.api_calls == []


@pytest.mark.asyncio
async def test_send_reaction_missing_execution_context_fails_tool_call() -> None:
    adapter = FakeAdapter()
    manager = _register_tools(adapter)

    result = await manager.execute(
        ToolCallRequest(
            tool_name="send_reaction",
            arguments={"message_id": "platform-msg-1", "emoji_id": "128077"},
            caller="test.active_chat",
            instance_id="",
            session_id="bot:group:room",
        )
    )

    assert result.success is False
    assert result.error_code == "tool_execution_failed"
    assert "instance_id not available" in result.error_message
    assert adapter.api_calls == []
