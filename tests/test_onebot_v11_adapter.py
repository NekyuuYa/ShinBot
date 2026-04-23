"""Tests for OneBot v11 adapter normalization and API mapping."""

from __future__ import annotations

import asyncio
import functools
import json
import socket
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import websockets

from shinbot.builtin_plugins.shinbot_adapter_onebot_v11.adapter import (
    OneBotV11Adapter,
    OneBotV11Config,
)
from shinbot.schema.elements import Message


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A003
        return


def _serve_directory(directory: Path):
    handler = functools.partial(_QuietHandler, directory=str(directory))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


@pytest.fixture
def adapter() -> OneBotV11Adapter:
    return OneBotV11Adapter(
        instance_id="ob11-test",
        platform="onebot_v11",
        config=OneBotV11Config(),
    )


def test_forward_max_depth_default_is_three(adapter: OneBotV11Adapter):
    assert adapter.config.forward_max_depth == 3


@pytest.mark.asyncio
async def test_decode_message_with_rich_segments(adapter: OneBotV11Adapter):
    payload = {
        "post_type": "message",
        "message_type": "group",
        "self_id": 10001,
        "time": 1711111111,
        "message_id": 222,
        "group_id": 778899,
        "user_id": 123456,
        "sender": {"nickname": "Alice", "card": "AliceCard"},
        "message": [
            {"type": "text", "data": {"text": "hello "}},
            {"type": "markdown", "data": {"content": "**world**"}},
            {"type": "mface", "data": {"emoji_id": "abc", "summary": "smile"}},
            {
                "type": "keyboard",
                "data": {"rows": [{"buttons": [{"id": "btn1", "label": "ok"}]}]},
            },
        ],
    }

    event = await adapter._decode_event(payload)
    assert event is not None
    assert event.type == "message-created"
    assert event.guild_id == "778899"
    assert event.user is not None
    assert event.user.nick == "Alice"
    assert event.member is not None
    assert event.member.nick == "AliceCard"
    assert event.message is not None
    assert "qq:markdown" in event.message.content
    assert "qq:mface" in event.message.content
    assert "qq:keyboard" in event.message.content


@pytest.mark.asyncio
async def test_decode_image_segments_preserves_sub_type_values(adapter: OneBotV11Adapter):
    payload = {
        "post_type": "message",
        "message_type": "group",
        "self_id": 10001,
        "time": 1711111111,
        "message_id": 225,
        "group_id": 778899,
        "user_id": 123456,
        "message": [
            {"type": "image", "data": {"file": "/tmp/normal.jpg", "subType": 0}},
            {"type": "image", "data": {"file": "/tmp/custom.jpg", "subType": 1}},
            {"type": "image", "data": {"file": "/tmp/store.gif"}},
        ],
    }

    event = await adapter._decode_event(payload)

    assert event is not None
    assert event.message is not None
    parsed = Message.from_xml(event.message.content)
    image_elements = [element for element in parsed.elements if element.type == "img"]
    assert [element.attrs["sub_type"] for element in image_elements] == ["0", "1", "none"]


@pytest.mark.asyncio
async def test_decode_group_message_fetches_member_card_when_sender_card_missing(
    adapter: OneBotV11Adapter,
    monkeypatch: pytest.MonkeyPatch,
):
    called: dict[str, object] = {}

    async def _fake_call(action: str, params: dict[str, object]):
        called["action"] = action
        called["params"] = params
        return {"card": "GroupAlice", "nickname": "Alice"}

    monkeypatch.setattr(adapter, "_call_ob11_api", _fake_call)

    payload = {
        "post_type": "message",
        "message_type": "group",
        "self_id": 10001,
        "time": 1711111111,
        "message_id": 223,
        "group_id": 778899,
        "user_id": 123456,
        "sender": {"nickname": "Alice"},
        "message": [{"type": "text", "data": {"text": "hello"}}],
    }

    event = await adapter._decode_event(payload)

    assert called["action"] == "get_group_member_info"
    assert called["params"] == {"group_id": 778899, "user_id": 123456}
    assert event is not None
    assert event.user is not None
    assert event.user.nick == "Alice"
    assert event.member is not None
    assert event.member.nick == "GroupAlice"


@pytest.mark.asyncio
async def test_decode_message_inserts_at_other_tag(adapter: OneBotV11Adapter):
    payload = {
        "post_type": "message",
        "message_type": "group",
        "self_id": 10001,
        "time": 1711111111,
        "message_id": 224,
        "group_id": 778899,
        "user_id": 123456,
        "sender": {"nickname": "Alice", "card": "AliceCard"},
        "message": [
            {"type": "at", "data": {"qq": "2345", "name": "Bob"}},
            {"type": "text", "data": {"text": " hello"}},
        ],
    }

    event = await adapter._decode_event(payload)

    assert event is not None
    assert event.message is not None
    assert '<at id="2345" name="Bob"/>' in event.message.content
    parsed = Message.from_xml(event.message.content)
    assert parsed.get_text(self_id=event.self_id) == "[@Bob(2345)] hello"


@pytest.mark.asyncio
async def test_decode_poke_notice_as_sb_poke(adapter: OneBotV11Adapter):
    payload = {
        "post_type": "notice",
        "notice_type": "notify",
        "sub_type": "poke",
        "self_id": 10001,
        "time": 1711111111,
        "group_id": 9988,
        "user_id": 123,
        "target_id": 456,
    }

    event = await adapter._decode_event(payload)
    assert event is not None
    assert event.type == "message-created"
    assert event.message is not None
    assert "<sb:poke" in event.message.content


@pytest.mark.asyncio
async def test_decode_notice_event_keeps_extra_payload_without_duplicate_standard_fields(
    adapter: OneBotV11Adapter,
):
    payload = {
        "post_type": "notice",
        "notice_type": "reaction",
        "sub_type": "add",
        "self_id": 10001,
        "time": 1711111111,
        "group_id": 9988,
        "user_id": 123,
        "operator_id": 456,
        "message_id": 789,
        "code": "128512",
    }

    event = await adapter._decode_event(payload)

    assert event is not None
    assert event.type == "notice-reaction-add"
    assert event.self_id == "10001"
    assert event.platform == "qq"
    assert event.user is not None and event.user.id == "123"
    assert event.operator is not None and event.operator.id == "456"
    assert event.message_id == 789
    assert event.code == "128512"


@pytest.mark.asyncio
async def test_decode_request_event_keeps_extra_payload_without_duplicate_standard_fields(
    adapter: OneBotV11Adapter,
):
    payload = {
        "post_type": "request",
        "request_type": "group",
        "sub_type": "invite",
        "self_id": 10001,
        "time": 1711111111,
        "group_id": 9988,
        "user_id": 123,
        "comment": "join us",
        "flag": "req-1",
    }

    event = await adapter._decode_event(payload)

    assert event is not None
    assert event.type == "guild-request"
    assert event.self_id == "10001"
    assert event.platform == "qq"
    assert event.user is not None and event.user.id == "123"
    assert event.comment == "join us"
    assert event.flag == "req-1"


@pytest.mark.asyncio
async def test_forward_segment_triggers_fetch(
    adapter: OneBotV11Adapter, monkeypatch: pytest.MonkeyPatch
):
    async def _fake_fetch_nodes(forward_id: str, *, forward_depth: int | None = None):
        assert forward_id == "fwd-123"
        assert forward_depth == adapter.config.forward_max_depth - 1
        return []

    monkeypatch.setattr(adapter, "_fetch_forward_nodes", _fake_fetch_nodes)

    payload = {
        "post_type": "message",
        "message_type": "private",
        "self_id": 10001,
        "time": 1711111111,
        "message_id": 333,
        "user_id": 123456,
        "sender": {"nickname": "Alice"},
        "message": [{"type": "forward", "data": {"id": "fwd-123"}}],
    }

    event = await adapter._decode_event(payload)
    assert event is not None
    assert event.message is not None
    assert 'forward="true"' in event.message.content


@pytest.mark.asyncio
async def test_handle_raw_forward_segment_expands_forward_content(adapter: OneBotV11Adapter):
    received = []
    event_received = asyncio.Event()

    async def _on_event(event):
        received.append(event)
        event_received.set()

    class _LoopbackWS:
        async def send(self, raw: str) -> None:
            request = json.loads(raw)
            assert request["action"] == "get_forward_msg"
            asyncio.create_task(
                adapter._handle_raw(
                    json.dumps(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {
                                "messages": [
                                    {
                                        "data": {
                                            "uin": "10002",
                                            "name": "Alice",
                                            "content": [
                                                {"type": "text", "data": {"text": "nested hello"}}
                                            ],
                                        }
                                    }
                                ]
                            },
                            "echo": request["echo"],
                        }
                    )
                )
            )

    adapter.set_event_callback(_on_event)
    adapter.config.request_timeout = 0.5
    adapter._ws = _LoopbackWS()

    await adapter._handle_raw(
        json.dumps(
            {
                "post_type": "message",
                "message_type": "private",
                "self_id": 10001,
                "time": 1711111111,
                "message_id": 444,
                "user_id": 123456,
                "sender": {"nickname": "Bob"},
                "message": [{"type": "forward", "data": {"id": "fwd-123"}}],
            }
        )
    )

    await asyncio.wait_for(event_received.wait(), timeout=1.0)

    assert len(received) == 1
    assert received[0].message is not None
    assert "nested hello" in received[0].message.content
    parsed = Message.from_xml(received[0].message.content)
    assert parsed[0].type == "message"
    assert parsed[0].attrs.get("forward") == "true"
    assert parsed[0].children[0].attrs == {"id": "10002", "name": "Alice"}
    assert parsed[0].children[0].children[0].text_content == "nested hello"


@pytest.mark.asyncio
async def test_fetch_forward_nodes_supports_message_style_payload(adapter: OneBotV11Adapter):
    async def _fake_call(action: str, params: dict[str, object]):
        assert action == "get_forward_msg"
        assert params == {"id": "fwd-456"}
        return {
            "messages": [
                {
                    "message": [{"type": "text", "data": {"text": "first"}}],
                    "sender": {"user_id": 10010, "nickname": "Carol"},
                },
                {
                    "message": [{"type": "text", "data": {"text": "second"}}],
                    "sender": {"user_id": 10011, "card": "DaveCard", "nickname": "Dave"},
                },
            ]
        }

    adapter._call_ob11_api = _fake_call  # type: ignore[method-assign]

    nodes = await adapter._fetch_forward_nodes("fwd-456")

    assert len(nodes) == 2
    assert nodes[0].attrs == {"id": "10010", "name": "Carol"}
    assert nodes[0].children[0].text_content == "first"
    assert nodes[1].attrs == {"id": "10011", "name": "DaveCard"}
    assert nodes[1].children[0].text_content == "second"


@pytest.mark.asyncio
async def test_nested_forward_respects_depth_limit(adapter: OneBotV11Adapter):
    calls: list[str] = []

    async def _fake_call(action: str, params: dict[str, object]):
        assert action == "get_forward_msg"
        forward_id = str(params["id"])
        calls.append(forward_id)
        if forward_id == "outer":
            return {
                "messages": [
                    {
                        "message": [{"type": "forward", "data": {"id": "inner"}}],
                        "sender": {"user_id": 10010, "nickname": "Carol"},
                    }
                ]
            }
        raise AssertionError("depth limit should prevent fetching nested forward id")

    adapter.config.forward_max_depth = 1
    adapter._call_ob11_api = _fake_call  # type: ignore[method-assign]

    event = await adapter._decode_event(
        {
            "post_type": "message",
            "message_type": "private",
            "self_id": 10001,
            "time": 1711111111,
            "message_id": 555,
            "user_id": 123456,
            "sender": {"nickname": "Bob"},
            "message": [{"type": "forward", "data": {"id": "outer"}}],
        }
    )

    assert event is not None
    assert event.message is not None
    assert calls == ["outer"]
    assert 'forward="true"' in event.message.content
    assert 'id="inner"' in event.message.content


@pytest.mark.asyncio
async def test_call_api_internal_poke_maps_to_onebot_action(
    adapter: OneBotV11Adapter, monkeypatch: pytest.MonkeyPatch
):
    called: dict[str, object] = {}

    async def _fake_call(action: str, params: dict):
        called["action"] = action
        called["params"] = params
        return {"ok": True}

    monkeypatch.setattr(adapter, "_call_ob11_api", _fake_call)

    await adapter.call_api("internal.qq.poke", {"group_id": 9988, "user_id": 1234})

    assert called["action"] == "group_poke"
    assert called["params"] == {"group_id": 9988, "user_id": 1234}


@pytest.mark.asyncio
async def test_call_api_internal_poke_falls_back_to_send_poke(
    adapter: OneBotV11Adapter, monkeypatch: pytest.MonkeyPatch
):
    calls: list[tuple[str, dict]] = []

    async def _fake_call(action: str, params: dict):
        calls.append((action, params))
        if action == "group_poke":
            raise RuntimeError("OneBot action failed: retcode=1200 msg=None")
        return {"ok": True}

    monkeypatch.setattr(adapter, "_call_ob11_api", _fake_call)

    result = await adapter.call_api("internal.qq.poke", {"group_id": 9988, "user_id": 1234})

    assert result == {"ok": True}
    assert calls == [
        ("group_poke", {"group_id": 9988, "user_id": 1234}),
        ("send_poke", {"group_id": 9988, "user_id": 1234}),
    ]


@pytest.mark.asyncio
async def test_call_api_member_list_maps_to_onebot_action(
    adapter: OneBotV11Adapter, monkeypatch: pytest.MonkeyPatch
):
    called: dict[str, object] = {}

    async def _fake_call(action: str, params: dict):
        called["action"] = action
        called["params"] = params
        return []

    monkeypatch.setattr(adapter, "_call_ob11_api", _fake_call)

    await adapter.call_api("guild.member.list", {"guild_id": 9988})

    assert called["action"] == "get_group_member_list"
    assert called["params"] == {"group_id": 9988}


@pytest.mark.asyncio
async def test_call_api_member_get_maps_to_onebot_action(
    adapter: OneBotV11Adapter, monkeypatch: pytest.MonkeyPatch
):
    called: dict[str, object] = {}

    async def _fake_call(action: str, params: dict):
        called["action"] = action
        called["params"] = params
        return {"card": "GroupAlice"}

    monkeypatch.setattr(adapter, "_call_ob11_api", _fake_call)

    await adapter.call_api("guild.member.get", {"guild_id": 9988, "user_id": 1234})

    assert called["action"] == "get_group_member_info"
    assert called["params"] == {"group_id": 9988, "user_id": 1234}


@pytest.mark.asyncio
async def test_call_api_set_group_name_maps_to_onebot_action(
    adapter: OneBotV11Adapter, monkeypatch: pytest.MonkeyPatch
):
    called: dict[str, object] = {}

    async def _fake_call(action: str, params: dict):
        called["action"] = action
        called["params"] = params
        return {"ok": True}

    monkeypatch.setattr(adapter, "_call_ob11_api", _fake_call)

    await adapter.call_api("guild.update", {"guild_id": 9988, "name": "New Group"})

    assert called["action"] == "set_group_name"
    assert called["params"] == {"group_id": 9988, "group_name": "New Group"}


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


@pytest.mark.asyncio
async def test_reverse_websocket_standalone_listener(tmp_path: Path):
    port = _pick_free_port()
    adapter = OneBotV11Adapter(
        "ob11-reverse",
        "onebot_v11",
        config=OneBotV11Config(
            mode="reverse",
            self_id="10001",
            reverse_host="127.0.0.1",
            reverse_port=port,
            reverse_path="/onebot/v11",
            access_token="token-123",
            auto_download_media=False,
        ),
    )

    received: list[str] = []
    event_received = asyncio.Event()

    async def _on_event(event):
        received.append(event.type)
        event_received.set()

    adapter.set_event_callback(_on_event)

    await adapter.start()
    try:
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/onebot/v11",
            additional_headers={
                "X-Self-ID": "10001",
                "X-Client-Role": "Universal",
                "Authorization": "Bearer token-123",
            },
        ) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "post_type": "message",
                        "message_type": "private",
                        "self_id": "10001",
                        "time": 1711111111,
                        "message_id": 777,
                        "user_id": 123456,
                        "message": [{"type": "text", "data": {"text": "hello"}}],
                    }
                )
            )

            await asyncio.wait_for(event_received.wait(), timeout=2.0)
    finally:
        await adapter.shutdown()

    assert adapter.config.mode == "reverse"
    assert received == ["message-created"]


@pytest.mark.asyncio
async def test_reverse_websocket_custom_url_path_listener():
    port = _pick_free_port()
    adapter = OneBotV11Adapter(
        "ob11-reverse-custom-url",
        "onebot_v11",
        config=OneBotV11Config(
            mode="reverse",
            self_id="10001",
            reverse_port=None,
            reverse_path=f"ws://127.0.0.1:{port}/ui",
            access_token="token-123",
        ),
    )

    await adapter.start()
    try:
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/ui",
            additional_headers={
                "X-Self-ID": "10001",
                "Authorization": "Bearer token-123",
            },
        ):
            pass
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_reverse_websocket_root_path_rejected_when_custom_path():
    port = _pick_free_port()
    adapter = OneBotV11Adapter(
        "ob11-reverse-strict-path",
        "onebot_v11",
        config=OneBotV11Config(
            mode="reverse",
            self_id="10001",
            reverse_host="127.0.0.1",
            reverse_port=port,
            reverse_path="/onebot/v11",
            access_token="token-123",
        ),
    )

    await adapter.start()
    try:
        with pytest.raises(websockets.exceptions.InvalidStatus):
            async with websockets.connect(
                f"ws://127.0.0.1:{port}",
                additional_headers={
                    "X-Self-ID": "10001",
                    "Authorization": "Bearer token-123",
                },
            ):
                pass
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_reverse_websocket_rejects_invalid_access_token():
    port = _pick_free_port()
    adapter = OneBotV11Adapter(
        "ob11-reverse-auth",
        "onebot_v11",
        config=OneBotV11Config(
            mode="reverse",
            self_id="10001",
            reverse_host="127.0.0.1",
            reverse_port=port,
            access_token="token-123",
        ),
    )

    await adapter.start()
    try:
        with pytest.raises((websockets.ConnectionClosedError, websockets.exceptions.InvalidStatus)):
            async with websockets.connect(
                f"ws://127.0.0.1:{port}/onebot/v11",
                additional_headers={
                    "X-Self-ID": "10001",
                    "Authorization": "Bearer invalid-token",
                },
            ) as websocket:
                await websocket.send("{}")
                await websocket.recv()
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_decode_message_downloads_image_resource(tmp_path: Path):
    asset_dir = tmp_path / "assets"
    asset_dir.mkdir()
    image_path = asset_dir / "image.png"
    image_path.write_bytes(b"png-bytes")

    server = _serve_directory(asset_dir)
    try:
        port = server.server_address[1]
        adapter = OneBotV11Adapter(
            instance_id="ob11-download",
            platform="onebot_v11",
            config=OneBotV11Config(
                auto_download_media=True,
                resource_cache_dir=str(tmp_path / "temp" / "resources"),
            ),
        )

        payload = {
            "post_type": "message",
            "message_type": "private",
            "self_id": 10001,
            "time": 1711111111,
            "message_id": 888,
            "user_id": 123456,
            "message": [
                {"type": "text", "data": {"text": "hello"}},
                {"type": "image", "data": {"url": f"http://127.0.0.1:{port}/image.png"}},
            ],
        }

        event = await adapter._decode_event(payload)
    finally:
        server.shutdown()
        server.server_close()

    assert event is not None
    assert event.message is not None
    parsed = Message.from_xml(event.message.content)
    image_element = next(element for element in parsed.elements if element.type == "img")
    assert image_element.attrs["src"].startswith(str(tmp_path / "temp" / "resources"))
    assert Path(image_element.attrs["src"]).is_file()


@pytest.mark.asyncio
async def test_decode_message_does_not_download_file_resource_by_default(tmp_path: Path):
    asset_dir = tmp_path / "assets"
    asset_dir.mkdir()
    file_path = asset_dir / "archive.zip"
    file_path.write_bytes(b"zip-bytes")

    server = _serve_directory(asset_dir)
    try:
        port = server.server_address[1]
        remote_url = f"http://127.0.0.1:{port}/archive.zip"
        adapter = OneBotV11Adapter(
            instance_id="ob11-file-default",
            platform="onebot_v11",
            config=OneBotV11Config(
                auto_download_media=True,
                resource_cache_dir=str(tmp_path / "temp" / "resources"),
            ),
        )

        payload = {
            "post_type": "message",
            "message_type": "private",
            "self_id": 10001,
            "time": 1711111111,
            "message_id": 889,
            "user_id": 123456,
            "message": [{"type": "file", "data": {"url": remote_url}}],
        }

        event = await adapter._decode_event(payload)
    finally:
        server.shutdown()
        server.server_close()

    assert event is not None
    assert event.message is not None
    parsed = Message.from_xml(event.message.content)
    file_element = next(element for element in parsed.elements if element.type == "file")
    assert file_element.attrs["src"] == remote_url


@pytest.mark.asyncio
async def test_decode_message_downloads_file_resource_when_enabled(tmp_path: Path):
    asset_dir = tmp_path / "assets"
    asset_dir.mkdir()
    file_path = asset_dir / "archive.zip"
    file_path.write_bytes(b"zip-bytes")

    server = _serve_directory(asset_dir)
    try:
        port = server.server_address[1]
        adapter = OneBotV11Adapter(
            instance_id="ob11-file-enabled",
            platform="onebot_v11",
            config=OneBotV11Config(
                auto_download_media=False,
                download_file_resources=True,
                resource_cache_dir=str(tmp_path / "temp" / "resources"),
            ),
        )

        payload = {
            "post_type": "message",
            "message_type": "private",
            "self_id": 10001,
            "time": 1711111111,
            "message_id": 890,
            "user_id": 123456,
            "message": [
                {
                    "type": "file",
                    "data": {"url": f"http://127.0.0.1:{port}/archive.zip"},
                }
            ],
        }

        event = await adapter._decode_event(payload)
    finally:
        server.shutdown()
        server.server_close()

    assert event is not None
    assert event.message is not None
    parsed = Message.from_xml(event.message.content)
    file_element = next(element for element in parsed.elements if element.type == "file")
    assert file_element.attrs["src"].startswith(str(tmp_path / "temp" / "resources"))
    assert Path(file_element.attrs["src"]).is_file()


@pytest.mark.asyncio
async def test_decode_message_respects_resource_size_limit(tmp_path: Path):
    asset_dir = tmp_path / "assets"
    asset_dir.mkdir()
    image_path = asset_dir / "large.png"
    image_path.write_bytes(b"x" * 32)

    server = _serve_directory(asset_dir)
    try:
        port = server.server_address[1]
        remote_url = f"http://127.0.0.1:{port}/large.png"
        adapter = OneBotV11Adapter(
            instance_id="ob11-download-limit",
            platform="onebot_v11",
            config=OneBotV11Config(
                auto_download_media=True,
                max_resource_bytes=8,
                resource_cache_dir=str(tmp_path / "temp" / "resources"),
            ),
        )

        payload = {
            "post_type": "message",
            "message_type": "private",
            "self_id": 10001,
            "time": 1711111111,
            "message_id": 891,
            "user_id": 123456,
            "message": [{"type": "image", "data": {"url": remote_url}}],
        }

        event = await adapter._decode_event(payload)
    finally:
        server.shutdown()
        server.server_close()

    assert event is not None
    assert event.message is not None
    parsed = Message.from_xml(event.message.content)
    image_element = next(element for element in parsed.elements if element.type == "img")
    assert image_element.attrs["src"] == remote_url
