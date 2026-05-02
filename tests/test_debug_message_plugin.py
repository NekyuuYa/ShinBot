from __future__ import annotations

from types import SimpleNamespace

import pytest

import shinbot.builtin_plugins.shinbot_debug_message as debug_message
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteMatchMode, RouteTable
from shinbot.core.message_routes.command import CommandRegistry
from shinbot.core.plugins.context import Plugin
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, User


def test_build_raw_record_preserves_event_payload():
    event = UnifiedEvent(
        id=123,
        type="message-created",
        self_id="10001",
        platform="qq",
        user=User(id="20001", name="Alice"),
        channel=Channel(id="30001", type=0),
        guild=Guild(id="30001"),
        message=MessagePayload(id="msg-1", content='hello <at id="42"/>'),
    )

    record = debug_message._build_raw_record(event)

    assert record["event_type"] == "message-created"
    assert record["platform"] == "qq"
    assert record["self_id"] == "10001"
    assert record["payload"]["message"]["content"] == 'hello <at id="42"/>'


def test_build_ast_record_for_message_event_parses_elements():
    event = UnifiedEvent(
        id=123,
        type="message-created",
        self_id="10001",
        platform="qq",
        user=User(id="20001", name="Alice"),
        channel=Channel(id="30001", type=0),
        guild=Guild(id="30001"),
        message=MessagePayload(
            id="msg-1",
            content='<message forward="true"><message id="2" name="Bob">nested</message></message>',
        ),
    )

    record = debug_message._build_ast_record(event)

    assert record["event"]["message"]["content"].startswith("<message forward=")
    assert record["message_ast"] is not None
    assert record["message_ast"]["id"] == "msg-1"
    assert record["message_ast"]["text"] == "nested"
    assert record["message_ast"]["elements"][0]["type"] == "message"
    assert record["message_ast"]["elements"][0]["attrs"] == {"forward": "true"}
    assert record["message_ast"]["elements"][0]["children"][0]["attrs"] == {
        "id": "2",
        "name": "Bob",
    }


def test_build_ast_record_for_notice_event_has_no_message_ast():
    event = UnifiedEvent(
        id=456,
        type="guild-member-added",
        self_id="10001",
        platform="qq",
        user=User(id="20001", name="Alice"),
        channel=Channel(id="30001", type=0),
        guild=Guild(id="30001"),
    )

    record = debug_message._build_ast_record(event)

    assert record["event_type"] == "guild-member-added"
    assert record["message_ast"] is None


def test_build_records_accept_route_dispatch_context_shape():
    event = UnifiedEvent(
        id=789,
        type="message-created",
        self_id="10001",
        platform="qq",
        user=User(id="20001", name="Alice"),
        channel=Channel(id="30001", type=0),
        guild=Guild(id="30001"),
        message=MessagePayload(id="msg-2", content="hello route"),
    )
    context = SimpleNamespace(event=event)

    raw_record = debug_message._build_raw_record(context)
    ast_record = debug_message._build_ast_record(context)

    assert raw_record["event_type"] == "message-created"
    assert ast_record["message_ast"]["text"] == "hello route"


@pytest.mark.asyncio
async def test_setup_registers_observing_message_route(tmp_path):
    event_bus = EventBus()
    route_table = RouteTable()
    route_targets = RouteTargetRegistry()
    plugin = Plugin(
        "shinbot_debug_message",
        CommandRegistry(),
        event_bus,
        data_dir=tmp_path,
        route_table=route_table,
        route_targets=route_targets,
    )

    debug_message.setup(plugin)
    try:
        assert event_bus.handler_count("*") == 1
        assert len(route_table.rules) == 1
        rule = route_table.rules[0]
        assert rule.id == "shinbot_debug_message.message_observer"
        assert rule.match_mode == RouteMatchMode.OBSERVE
        assert route_targets.get("shinbot_debug_message.message_observer") is not None
    finally:
        await debug_message.on_disable(plugin)
