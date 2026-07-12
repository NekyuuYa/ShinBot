"""Tests for side-effect-free model action normalization."""

from __future__ import annotations

import pytest

from shinbot.agent.runtime.session_actor.external_actions import ExternalActionKind
from shinbot.agent.workflows.chat_actions.intents import (
    collect_external_action_intent,
)


@pytest.mark.parametrize(
    ("tool_name", "arguments", "expected_payload"),
    [
        (
            "send_reply",
            {
                "text": "  hello  ",
                "quote_message_log_id": 12,
                "terminate_round": False,
                "intensity": "engaged",
            },
            {"text": "hello", "quote_message_log_id": 12},
        ),
        (
            "send_poke",
            {"user_id": " alice ", "terminate_round": True},
            {"user_id": "alice"},
        ),
        (
            "send_reaction",
            {
                "message_id": " platform-1 ",
                "emoji": " 128077 ",
                "action": "REMOVE",
            },
            {
                "message_id": "platform-1",
                "emoji_id": "128077",
                "action": "remove",
            },
        ),
    ],
)
def test_collect_external_action_intent_normalizes_without_runtime_identity(
    tool_name: str,
    arguments: dict[str, object],
    expected_payload: dict[str, object],
) -> None:
    intent = collect_external_action_intent(
        tool_call_id="call-1",
        tool_name=tool_name,
        arguments=arguments,
        action_ordinal=2,
    )

    assert intent.kind is ExternalActionKind(tool_name)
    assert intent.tool_call_id == "call-1"
    assert intent.action_ordinal == 2
    assert intent.payload == expected_payload
    assert "terminate_round" not in intent.payload


@pytest.mark.parametrize(
    ("tool_name", "arguments", "message"),
    [
        ("send_reply", {"text": ""}, "text must not be empty"),
        (
            "send_reply",
            {
                "text": "hello",
                "quote_message_id": "platform-1",
                "quote_message_log_id": 1,
            },
            "mutually exclusive",
        ),
        ("send_poke", {"user_id": ""}, "user_id must not be empty"),
        (
            "send_reaction",
            {"message_id": "m1", "emoji_id": "1", "action": "replace"},
            "action must be",
        ),
        (
            "send_reaction",
            {"emoji_id": "1"},
            "exactly one",
        ),
        (
            "send_reply",
            {"text": "hello", "idempotency_key": "model-owned"},
            "runtime-reserved",
        ),
        (
            "send_reply",
            {"text": "hello", "unexpected": True},
            "unsupported external action fields",
        ),
    ],
)
def test_collect_external_action_intent_rejects_ambiguous_or_reserved_input(
    tool_name: str,
    arguments: dict[str, object],
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        collect_external_action_intent(
            tool_call_id="call-1",
            tool_name=tool_name,
            arguments=arguments,
            action_ordinal=0,
        )
