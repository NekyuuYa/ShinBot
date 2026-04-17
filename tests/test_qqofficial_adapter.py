"""Tests for QQ Official adapter normalization and API mapping."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from shinbot.builtin_plugins.shinbot_adapter_qqofficial.adapter import (
    QQOfficialAdapter,
    QQOfficialConfig,
    SessionRoute,
)
from shinbot.core.platform.adapter_manager import MessageHandle
from shinbot.schema.elements import MessageElement


@pytest.fixture
def adapter() -> QQOfficialAdapter:
    return QQOfficialAdapter(
        instance_id="qq-test",
        platform="qqofficial",
        config=QQOfficialConfig(app_id="app-id", app_secret="app-secret"),
    )


@pytest.mark.asyncio
async def test_decode_c2c_message_event_builds_private_unified_event(adapter: QQOfficialAdapter):
    payload = {
        "id": "msg-1",
        "event_id": "C2C_MESSAGE_CREATE:e-1",
        "timestamp": "2026-04-17T13:00:00+08:00",
        "author": {"user_openid": "user-openid-1"},
        "content": "hello from c2c",
    }

    event = await adapter._decode_dispatch_event("C2C_MESSAGE_CREATE", payload)

    assert event is not None
    assert event.type == "message-created"
    assert event.is_private is True
    assert event.user is not None
    assert event.user.id == "user-openid-1"
    assert event.message is not None
    assert "hello from c2c" in event.message.content

    session_id = f"{adapter.instance_id}:private:user-openid-1"
    assert session_id in adapter._session_routes


@pytest.mark.asyncio
async def test_send_c2c_uses_v2_users_endpoint(adapter: QQOfficialAdapter):
    target_session = "qq-test:private:user-openid-2"
    adapter._session_routes[target_session] = SessionRoute(
        scene="c2c",
        openid="user-openid-2",
        last_message_id="inbound-msg-7",
        last_event_id="event-7",
    )

    calls: list[tuple[str, str, dict]] = []

    async def _fake_request(method: str, path: str, *, query=None, json_payload=None):
        calls.append((method, path, json_payload or {}))
        return {"id": "out-msg-1"}

    adapter._request = _fake_request  # type: ignore[method-assign]

    handle = await adapter.send(target_session, [MessageElement.text("hello")])

    assert isinstance(handle, MessageHandle)
    assert handle.message_id == "out-msg-1"
    assert calls[0][0] == "POST"
    assert calls[0][1] == "/v2/users/user-openid-2/messages"
    assert calls[0][2]["content"] == "hello"
    assert calls[0][2]["msg_id"] == "inbound-msg-7"


@pytest.mark.asyncio
async def test_call_api_channel_message_create_maps_private_channel_to_session(
    adapter: QQOfficialAdapter,
):
    adapter.send = AsyncMock(
        return_value=MessageHandle(message_id="created-1", adapter_ref=adapter)
    )

    result = await adapter.call_api(
        "channel.message.create",
        {
            "channel_id": "private:user-openid-3",
            "content": "<text>hello</text>",
        },
    )

    adapter.send.assert_awaited_once()
    called_session = adapter.send.await_args.args[0]
    assert called_session == "qq-test:private:user-openid-3"
    assert result == [{"id": "created-1"}]


def test_message_elements_parse_attachments_and_quote(adapter: QQOfficialAdapter):
    payload = {
        "content": "text body",
        "message_reference": {"message_id": "source-msg"},
        "attachments": [
            {
                "content_type": "image/png",
                "filename": "a.png",
                "url": "cdn.example.com/a.png",
            },
            {
                "content_type": "video/mp4",
                "filename": "b.mp4",
                "url": "https://cdn.example.com/b.mp4",
            },
        ],
    }

    elements = adapter._message_elements_from_payload(payload)
    kinds = [element.type for element in elements]

    assert "quote" in kinds
    assert "text" in kinds
    assert "img" in kinds
    assert "video" in kinds


@pytest.mark.asyncio
async def test_get_capabilities_contains_expected_actions(adapter: QQOfficialAdapter):
    caps = await adapter.get_capabilities()

    assert "elements" in caps
    assert "actions" in caps
    assert "message.update" in caps["actions"]
    assert "qq:markdown" in caps["elements"]
