"""Tests for the Satori WebSocket adapter (shinbot_adapter_satori)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from shinbot.builtin_plugins.shinbot_adapter_satori.adapter import SatoriAdapter, SatoriConfig
from shinbot.core.adapter_manager import MessageHandle
from shinbot.models.elements import MessageElement
from shinbot.models.events import UnifiedEvent


def _make_adapter(
    instance_id: str = "test-inst",
    platform: str = "llonebot",
    host: str = "localhost:5140",
    token: str = "test-token",
) -> SatoriAdapter:
    config = SatoriConfig(host=host, token=token)
    return SatoriAdapter(instance_id=instance_id, platform=platform, config=config)


class TestSessionIdDecoding:
    """Unit tests for _decode_session_id() helper."""

    def setup_method(self):
        self.adapter = _make_adapter()

    def test_private_session(self):
        sid = "test-inst:private:1917419834"
        channel_id = self.adapter._decode_session_id(sid)
        assert channel_id == "private:1917419834"

    def test_group_flat(self):
        sid = "test-inst:group:696879614"
        channel_id = self.adapter._decode_session_id(sid)
        assert channel_id == "696879614"

    def test_group_nested(self):
        sid = "test-inst:group:guild123:channel456"
        channel_id = self.adapter._decode_session_id(sid)
        assert channel_id == "channel456"

    def test_no_colon_passthrough(self):
        # Edge case: malformed session_id
        assert self.adapter._decode_session_id("plain") == "plain"

    def test_other_type_passthrough(self):
        sid = "inst:direct:abc"
        result = self.adapter._decode_session_id(sid)
        assert result == "direct:abc"


class TestHeaderBuilding:
    def test_headers_with_token(self):
        adapter = _make_adapter(token="secret")
        adapter._detected_platform = "llonebot"
        adapter._self_id = "12345"
        headers = adapter._build_headers()
        assert headers["Authorization"] == "Bearer secret"
        assert headers["X-Platform"] == "llonebot"
        assert headers["X-Self-ID"] == "12345"

    def test_headers_without_token(self):
        adapter = _make_adapter(token="")
        headers = adapter._build_headers()
        assert "Authorization" not in headers


class TestGetCapabilities:
    @pytest.mark.asyncio
    async def test_returns_expected_structure(self):
        adapter = _make_adapter()
        caps = await adapter.get_capabilities()
        assert "elements" in caps
        assert "actions" in caps
        assert "limits" in caps
        assert "text" in caps["elements"]
        assert "channel.message.create" in caps["actions"]


class TestHandleReady:
    @pytest.mark.asyncio
    async def test_extracts_self_id_and_platform(self):
        adapter = _make_adapter()
        ready_body = {
            "logins": [
                {
                    "user": {"id": "3649342015", "name": "yui"},
                    "platform": "llonebot",
                    "status": 1,
                }
            ]
        }
        await adapter._handle_ready(ready_body)
        assert adapter._self_id == "3649342015"
        assert adapter._detected_platform == "llonebot"

    @pytest.mark.asyncio
    async def test_empty_logins_no_error(self):
        adapter = _make_adapter()
        await adapter._handle_ready({"logins": []})
        assert adapter._self_id == ""


class TestHandleEvent:
    @pytest.mark.asyncio
    async def test_dispatches_to_callback(self):
        adapter = _make_adapter()
        received: list[UnifiedEvent] = []

        async def callback(event: UnifiedEvent):
            received.append(event)

        adapter.set_event_callback(callback)

        event_body = {
            "id": 1,
            "sn": 1,
            "type": "message-created",
            "self_id": "3649342015",
            "platform": "llonebot",
            "message": {"id": "msg-1", "content": "hello"},
            "user": {"id": "user-1"},
            "channel": {"id": "private:user-1", "type": 1},
        }
        await adapter._handle_event(event_body)

        assert len(received) == 1
        assert received[0].type == "message-created"
        assert received[0].user.id == "user-1"

    @pytest.mark.asyncio
    async def test_no_callback_no_error(self):
        adapter = _make_adapter()
        event_body = {"type": "message-created", "self_id": "x", "platform": "y"}
        await adapter._handle_event(event_body)  # should not raise

    @pytest.mark.asyncio
    async def test_malformed_body_no_crash(self):
        adapter = _make_adapter()
        received: list = []

        async def callback(event):
            received.append(event)

        adapter.set_event_callback(callback)
        # Missing required "type" field
        await adapter._handle_event({"garbage": True})
        assert len(received) == 0


class TestHandleRaw:
    @pytest.mark.asyncio
    async def test_dispatches_event_op(self):
        adapter = _make_adapter()
        received = []

        async def cb(event):
            received.append(event)

        adapter.set_event_callback(cb)

        raw = '{"op": 0, "body": {"type": "message-created", "self_id": "x", "platform": "y", "user": {"id": "u1"}, "channel": {"id": "c1", "type": 0}}}'
        await adapter._handle_raw(raw)
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_ready_op_updates_self_id(self):
        adapter = _make_adapter()
        raw = '{"op": 4, "body": {"logins": [{"user": {"id": "9999"}, "platform": "testbot"}]}}'
        await adapter._handle_raw(raw)
        assert adapter._self_id == "9999"

    @pytest.mark.asyncio
    async def test_pong_op_no_error(self):
        adapter = _make_adapter()
        await adapter._handle_raw('{"op": 6}')  # PONG, no action

    @pytest.mark.asyncio
    async def test_invalid_json_no_crash(self):
        adapter = _make_adapter()
        await adapter._handle_raw("not json at all")


class TestSend:
    @pytest.mark.asyncio
    async def test_send_posts_to_api(self):
        adapter = _make_adapter()
        adapter._self_id = "3649342015"
        adapter._detected_platform = "llonebot"

        api_calls: list[tuple[str, dict]] = []

        async def mock_call_api(method: str, params: dict) -> Any:
            api_calls.append((method, params))
            return [{"id": "new-msg-1", "content": "<text>hello</text>"}]

        adapter.call_api = mock_call_api

        elements = [MessageElement.text("hello")]
        handle = await adapter.send("test-inst:private:user-1", elements)

        assert len(api_calls) == 1
        method, params = api_calls[0]
        assert method == "channel.message.create"
        assert params["channel_id"] == "private:user-1"
        assert "hello" in params["content"]
        assert isinstance(handle, MessageHandle)
        assert handle.message_id == "new-msg-1"

    @pytest.mark.asyncio
    async def test_send_group_channel(self):
        adapter = _make_adapter()
        api_calls = []

        async def mock_call_api(method, params):
            api_calls.append(params)
            return []

        adapter.call_api = mock_call_api

        elements = [MessageElement.text("msg")]
        await adapter.send("test-inst:group:696879614", elements)

        assert api_calls[0]["channel_id"] == "696879614"


class TestCallApi:
    @pytest.mark.asyncio
    async def test_call_api_constructs_url(self):
        adapter = _make_adapter(host="localhost:5140", token="tok")
        adapter._detected_platform = "llonebot"
        adapter._self_id = "bot1"

        mock_response = MagicMock()
        mock_response.content = b'[{"id": "m1"}]'
        mock_response.json.return_value = [{"id": "m1"}]
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        adapter._http = mock_http

        result = await adapter.call_api(
            "channel.message.create",
            {"channel_id": "private:123", "content": "hello"},
        )

        assert result == [{"id": "m1"}]
        mock_http.post.assert_called_once()
        call_args = mock_http.post.call_args
        assert "channel/message/create" in call_args[0][0] or "channel.message" in str(call_args)

    @pytest.mark.asyncio
    async def test_call_api_internal_route(self):
        adapter = _make_adapter(host="localhost:5140")
        adapter._detected_platform = "qq"

        mock_response = MagicMock()
        mock_response.content = b"{}"
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        adapter._http = mock_http

        await adapter.call_api("internal.qq.poke", {"user_id": "123"})

        call_args = mock_http.post.call_args
        url = call_args[0][0]
        assert "internal" in url

    @pytest.mark.asyncio
    async def test_call_api_not_started_raises(self):
        adapter = _make_adapter()
        # _http is None because start() not called
        with pytest.raises(RuntimeError, match="Adapter not started"):
            await adapter.call_api("message.create", {})
