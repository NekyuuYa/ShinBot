"""Agent-entry signal tests."""

from __future__ import annotations

import asyncio

import pytest

from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    AgentEntryDispatcher,
    AgentEntrySignal,
    make_agent_entry_fallback_route_rule,
)
from shinbot.core.dispatch.ingress import MessageIngress, RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteCondition, RouteRule, RouteTable
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import BotConfigRecord
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User

pytestmark = [pytest.mark.integration, pytest.mark.slow]


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id: str = "test-bot", platform: str = "mock") -> None:
        super().__init__(instance_id, platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []
        self.api_calls: list[tuple[str, dict]] = []

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    async def send(
        self,
        target_session: str,
        elements: list[MessageElement],
    ) -> MessageHandle:
        self.sent.append((target_session, elements))
        return MessageHandle(message_id=f"sent-{len(self.sent)}", adapter_ref=self)

    async def call_api(self, method: str, params: dict) -> dict:
        self.api_calls.append((method, params))
        return {"ok": True}

    async def get_capabilities(self) -> dict:
        return {"elements": ["text"], "actions": [], "limits": {}}


def make_event(
    content: str = "hello",
    *,
    user_id: str = "user-1",
    channel_type: int = 1,
) -> UnifiedEvent:
    return UnifiedEvent(
        type="message-created",
        self_id="bot-1",
        platform="mock",
        user=User(id=user_id),
        channel=Channel(
            id=f"private:{user_id}" if channel_type == 1 else "group:1",
            type=channel_type,
        ),
        message=MessagePayload(id="msg-1", content=content),
    )


class RecordingAgentHandler:
    def __init__(self) -> None:
        self.signals: list[AgentEntrySignal] = []

    def __call__(self, signal: AgentEntrySignal) -> None:
        self.signals.append(signal)


def make_agent_ingress(
    db: DatabaseManager,
    handler: RecordingAgentHandler,
    *,
    route_table: RouteTable | None = None,
    route_targets: RouteTargetRegistry | None = None,
) -> MessageIngress:
    table = route_table if route_table is not None else RouteTable()
    targets = route_targets if route_targets is not None else RouteTargetRegistry()
    agent_entry = AgentEntryDispatcher(handler=handler, database=db)
    table.register(make_agent_entry_fallback_route_rule())
    targets.register(AGENT_ENTRY_TARGET, agent_entry)
    return MessageIngress(
        session_manager=SessionManager(session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        route_table=table,
        route_targets=targets,
        database=db,
    )


class TestAgentEntrySignal:
    def setup_method(self) -> None:
        self.adapter = MockAdapter()

    @pytest.mark.asyncio
    async def test_agent_entry_emits_private_signal_with_disabled_profile_by_default(
        self,
        tmp_path,
    ) -> None:
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        handler = RecordingAgentHandler()
        ingress = make_agent_ingress(db, handler)

        await ingress.process_event(make_event("hello private"), self.adapter)
        await asyncio.sleep(0)

        assert len(handler.signals) == 1
        signal = handler.signals[0]
        assert signal.session_id == "test-bot:private:user-1"
        assert signal.response_profile == "disabled"
        assert signal.is_private is True
        assert signal.is_mentioned is False
        assert signal.is_reply_to_bot is False
        assert not hasattr(signal, "message")

    @pytest.mark.asyncio
    async def test_agent_entry_signal_uses_group_and_priority_profiles(
        self,
        tmp_path,
    ) -> None:
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        db.bot_configs.upsert(
            BotConfigRecord(
                uuid="cfg-group-profile",
                instance_id=self.adapter.instance_id,
                config={
                    "response_profile_group": "passive",
                    "response_profile_priority": "balanced",
                },
            )
        )
        handler = RecordingAgentHandler()
        ingress = make_agent_ingress(db, handler)

        await ingress.process_event(make_event("hello group", channel_type=0), self.adapter)
        await ingress.process_event(
            make_event('<at id="bot-1"/>hello group', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert [signal.response_profile for signal in handler.signals] == [
            "passive",
            "balanced",
        ]
        assert [signal.is_mentioned for signal in handler.signals] == [False, True]

    @pytest.mark.asyncio
    async def test_agent_entry_fallback_is_skipped_when_plugin_route_consumes_message(
        self,
        tmp_path,
    ) -> None:
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        handler = RecordingAgentHandler()
        table = RouteTable()
        targets = RouteTargetRegistry()
        plugin_rule = RouteRule(
            id="plugin-reply",
            priority=10,
            condition=RouteCondition(event_types=frozenset({"message-created"})),
            target="plugin-reply",
        )
        table.register(plugin_rule)

        async def plugin_handler(context, _rule) -> None:
            await context.require_message_context().send("plugin reply")

        targets.register("plugin-reply", plugin_handler)
        ingress = make_agent_ingress(db, handler, route_table=table, route_targets=targets)

        result = await ingress.process_event(make_event("hello plugin"), self.adapter)
        await asyncio.sleep(0)

        assert result.matched_rules == [plugin_rule]
        assert len(self.adapter.sent) == 1
        assert Message(elements=self.adapter.sent[0][1]).get_text() == "plugin reply"
        assert handler.signals == []
