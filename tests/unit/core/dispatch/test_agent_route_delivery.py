"""Tests for the versioned Agent route delivery contract."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from typing import Any

import pytest

from shinbot.core.application.bots_config import (
    BotBindingConfig,
    BotServiceConfig,
)
from shinbot.core.dispatch.agent_delivery import (
    AGENT_ROUTE_DELIVERY_VERSION,
    AgentRouteDelivery,
    AgentRouteDeliveryError,
    MissingAgentMessageLogId,
)
from shinbot.core.dispatch.agent_identity import SessionKey, SessionKeyFactory
from shinbot.core.dispatch.dispatchers import (
    AgentEntryDispatcher,
    make_agent_entry_fallback_route_rule,
)
from shinbot.core.dispatch.ingress import RouteDispatchContext
from shinbot.core.dispatch.message_context import MessageContext
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.state.session import Session
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


class _Adapter(BaseAdapter):
    def __init__(self) -> None:
        super().__init__(instance_id="instance-main", platform="mock")

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    async def send(
        self,
        target_session: str,
        elements: list[MessageElement],
    ) -> MessageHandle:
        return MessageHandle(message_id=target_session, adapter_ref=self)

    async def call_api(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        return {"method": method, "params": params}

    async def get_capabilities(self) -> dict[str, Any]:
        return {"elements": ["text", "at", "sb:poke"]}


def _delivery(
    *,
    bot_id: str = "bot-a",
    bot_session_id: str = "bot-a:group:room",
    message_log_id: int | None = 42,
    observed_at: float = 100.0,
    trace_id: str = "trace-a",
    route_rule_id: str = "builtin.agent_entry_fallback",
) -> AgentRouteDelivery:
    base_session_id = "instance-main:group:room"
    key = SessionKeyFactory().create(
        bot_config_id=bot_id,
        bot_id=bot_id,
        bot_session_id=bot_session_id,
        base_session_id=base_session_id,
    )
    return AgentRouteDelivery(
        session_key=key,
        bot_id=bot_id,
        bot_binding_id=f"{bot_id}-binding",
        base_session_id=base_session_id,
        bot_session_id=bot_session_id,
        message_log_id=message_log_id,
        sender_id="user-a",
        instance_id="instance-main",
        platform="mock",
        self_id="bot-self",
        is_private=False,
        is_mentioned=True,
        is_mention_to_other=True,
        is_reply_to_bot=True,
        is_poke_to_bot=True,
        is_poke_to_other=True,
        already_handled=False,
        is_stopped=False,
        trace_id=trace_id,
        observed_at=observed_at,
        route_rule_id=route_rule_id,
    )


def _dispatch_context(
    *,
    bot_id: str = "bot-a",
    message_log_id: int | None = 42,
    observed_at: float = 100.0,
) -> RouteDispatchContext:
    adapter = _Adapter()
    event = UnifiedEvent(
        type="message-created",
        self_id="bot-self",
        platform="mock",
        user=User(id="user-a"),
        channel=Channel(id="room", type=0),
        message=MessagePayload(
            id="platform-message-a",
            content=(
                '<at id="bot-self"/><at id="user-b"/>'
                '<sb:poke target="bot-self"/><sb:poke target="user-b"/>hello'
            ),
        ),
    )
    message = Message.from_xml(event.message_content)
    session = Session(
        id="instance-main:group:room",
        instance_id="instance-main",
        session_type="group",
        platform="mock",
        channel_id="room",
    )
    message_context = MessageContext(
        event=event,
        message=message,
        session=session,
        adapter=adapter,
        permissions=set(),
    )
    message_context.bot_service_config = BotServiceConfig(
        id=bot_id,
        display_name=bot_id,
    )
    message_context.bot_binding_config = BotBindingConfig(
        id=f"{bot_id}-binding",
        adapter_instance_id="instance-main",
        session_patterns=("group:room",),
    )
    message_context.bot_session_id = f"{bot_id}:group:room"
    return RouteDispatchContext(
        event=event,
        adapter=adapter,
        message=message,
        message_context=message_context,
        message_log_id=message_log_id,
        trace_id="trace-a",
        observed_at=observed_at,
    )


def test_delivery_payload_round_trip_recomputes_canonical_identity() -> None:
    delivery = _delivery()

    wire_payload = json.loads(json.dumps(delivery.to_payload()))
    restored = AgentRouteDelivery.from_payload(wire_payload)

    assert restored == delivery
    assert wire_payload["version"] == AGENT_ROUTE_DELIVERY_VERSION
    assert wire_payload["session_key"] == {
        "profile_id": "bot-a",
        "session_id": "bot-a:group:room",
    }
    assert wire_payload["delivery_id"] == delivery.delivery_id
    assert wire_payload["event_id"] == delivery.event_id
    assert wire_payload["idempotency_key"] == delivery.idempotency_key


@pytest.mark.parametrize("tampered_field", ["delivery_id", "event_id", "idempotency_key"])
def test_delivery_payload_rejects_tampered_deterministic_ids(
    tampered_field: str,
) -> None:
    payload = _delivery().to_payload()
    payload[tampered_field] = "forged"

    with pytest.raises(AgentRouteDeliveryError, match=tampered_field):
        AgentRouteDelivery.from_payload(payload)


def test_same_delivery_key_has_stable_ids_independent_of_time_and_trace() -> None:
    first = _delivery(observed_at=100.0, trace_id="trace-a")
    replay = replace(first, observed_at=9_999.0, trace_id="trace-replayed")

    assert first.delivery_key == replay.delivery_key
    assert first.delivery_id == replay.delivery_id
    assert first.event_id == replay.event_id
    assert first.idempotency_key == replay.idempotency_key


def test_same_message_log_is_isolated_by_canonical_bot_profile() -> None:
    bot_a = _delivery(bot_id="bot-a", bot_session_id="bot-a:group:room")
    bot_b = _delivery(bot_id="bot-b", bot_session_id="bot-b:group:room")

    assert bot_a.delivery_key == (
        "bot-a",
        "bot-a:group:room",
        42,
        "builtin.agent_entry_fallback",
    )
    assert bot_b.delivery_key == (
        "bot-b",
        "bot-b:group:room",
        42,
        "builtin.agent_entry_fallback",
    )
    assert bot_a.delivery_id != bot_b.delivery_id
    assert bot_a.event_id != bot_b.event_id
    assert bot_a.idempotency_key != bot_b.idempotency_key


def test_route_rules_have_distinct_deliveries_but_one_actor_message_event() -> None:
    first = _delivery(route_rule_id="builtin.agent_entry_fallback")
    second = _delivery(route_rule_id="plugin.audit_agent_entry")

    assert first.delivery_key != second.delivery_key
    assert first.delivery_id != second.delivery_id
    assert first.event_id == second.event_id
    assert first.idempotency_key != second.idempotency_key
    assert first.to_mailbox_payload() == second.to_mailbox_payload()


def test_direct_delivery_construction_rejects_noncanonical_session_key() -> None:
    valid = _delivery()

    with pytest.raises(AgentRouteDeliveryError, match="canonical routing identity"):
        replace(valid, session_key=SessionKey("bot-b", "bot-b:group:room"))


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"version": True}, "unsupported Agent route delivery version"),
        ({"message_log_id": 1.5}, "message_log_id must be a positive integer"),
        ({"is_private": 1}, "is_private must be a boolean"),
        ({"instance_id": ""}, "instance_id is required"),
        ({"event_type": ""}, "event_type is required"),
    ],
)
def test_direct_delivery_rejects_noncanonical_durable_values(
    changes: dict[str, Any],
    error: str,
) -> None:
    with pytest.raises(AgentRouteDeliveryError, match=error):
        replace(_delivery(), **changes)


@pytest.mark.parametrize("field_name", ["version", "message_log_id"])
def test_delivery_payload_rejects_non_integer_identity_fields(field_name: str) -> None:
    payload = _delivery().to_payload()
    payload[field_name] = float(payload[field_name])

    with pytest.raises(AgentRouteDeliveryError, match=field_name):
        AgentRouteDelivery.from_payload(payload)


def test_importing_core_delivery_does_not_load_agent_package() -> None:
    check = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import shinbot.core.dispatch.agent_delivery; "
                "assert not any(name == 'shinbot.agent' or "
                "name.startswith('shinbot.agent.') for name in sys.modules)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert check.returncode == 0, check.stderr


def test_prepare_delivery_projects_route_context_and_priority_flags() -> None:
    dispatcher = AgentEntryDispatcher()
    context = _dispatch_context()

    delivery = dispatcher.prepare_delivery(
        context,
        make_agent_entry_fallback_route_rule(),
    )

    assert delivery.session_key.profile_id == "bot-a"
    assert delivery.session_key.session_id == "bot-a:group:room"
    assert delivery.base_session_id == "instance-main:group:room"
    assert delivery.bot_session_id == "bot-a:group:room"
    assert delivery.message_log_id == 42
    assert delivery.sender_id == "user-a"
    assert delivery.instance_id == "instance-main"
    assert delivery.platform == "mock"
    assert delivery.self_id == "bot-self"
    assert delivery.is_private is False
    assert delivery.is_mentioned is True
    assert delivery.is_mention_to_other is True
    assert delivery.is_reply_to_bot is False
    assert delivery.is_poke_to_bot is True
    assert delivery.is_poke_to_other is True
    assert delivery.trace_id == "trace-a"
    assert delivery.observed_at == 100.0


@pytest.mark.asyncio
async def test_dispatcher_converts_delivery_back_to_compatible_signal() -> None:
    signals = []
    dispatcher = AgentEntryDispatcher(handler=lambda signal: signals.append(signal))
    context = _dispatch_context()

    await dispatcher(context, make_agent_entry_fallback_route_rule())

    assert len(signals) == 1
    signal = signals[0]
    assert signal.signal_id == "message-ingress:instance-main:group:room:42"
    assert signal.session_id == "instance-main:group:room"
    assert signal.bot_id == "bot-a"
    assert signal.bot_binding_id == "bot-a-binding"
    assert signal.bot_session_id == "bot-a:group:room"
    assert signal.occurred_at == 100.0
    assert signal.message is not None
    assert signal.message.message_log_id == 42
    assert signal.message.is_mentioned is True
    assert signal.message.is_mention_to_other is True
    assert signal.message.is_poke_to_bot is True
    assert signal.message.is_poke_to_other is True
    assert signal.meta["delivery_id"].startswith("agent-route-delivery:v1:")
    assert signal.meta["event_id"].startswith("message-received:")
    assert signal.meta["actor_profile_id"] == "bot-a"
    assert signal.meta["actor_session_id"] == "bot-a:group:room"


def test_actor_message_event_identity_is_stable_across_contract_versions() -> None:
    delivery = _delivery()
    event_id = delivery.event_id
    delivery_id = delivery.delivery_id

    object.__setattr__(delivery, "version", delivery.version + 1)

    assert delivery.event_id == event_id
    assert delivery.delivery_id != delivery_id


@pytest.mark.asyncio
async def test_missing_message_log_rejects_actor_delivery_but_keeps_legacy_signal() -> None:
    signals = []
    dispatcher = AgentEntryDispatcher(handler=lambda signal: signals.append(signal))
    context = _dispatch_context(message_log_id=None)
    rule = make_agent_entry_fallback_route_rule()

    with pytest.raises(MissingAgentMessageLogId, match="message_log_id is required"):
        dispatcher.prepare_delivery(context, rule)

    await dispatcher(context, rule)

    assert len(signals) == 1
    signal = signals[0]
    assert signal.signal_id == "message-ingress:instance-main:group:room:missing"
    assert signal.message is not None
    assert signal.message.message_log_id is None
    assert "delivery_id" not in signal.meta

    compatibility_delivery = _delivery(message_log_id=None)
    assert compatibility_delivery.actor_deliverable is False
    with pytest.raises(MissingAgentMessageLogId):
        _ = compatibility_delivery.delivery_id
    with pytest.raises(MissingAgentMessageLogId):
        compatibility_delivery.to_payload()
