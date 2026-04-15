from __future__ import annotations

from shinbot.builtin_plugins.shinbot_debug_message import _build_ast_record, _build_raw_record
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

    record = _build_raw_record(event)

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

    record = _build_ast_record(event)

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

    record = _build_ast_record(event)

    assert record["event_type"] == "guild-member-added"
    assert record["message_ast"] is None
