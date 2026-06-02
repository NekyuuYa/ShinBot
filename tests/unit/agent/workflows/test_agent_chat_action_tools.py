from __future__ import annotations

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

    async def send(self, session_id: str, elements: list[object]) -> FakeSendHandle:
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("temporary send failure")
        self.sent.append((session_id, list(elements)))
        return FakeSendHandle(message_id=f"platform-{len(self.sent)}")


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
