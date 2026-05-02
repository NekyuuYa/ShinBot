"""Agent-entry attention scheduling tests."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

import pytest

from shinbot.agent.attention.trigger_strategy import (
    AttentionTriggerActions,
    AttentionTriggerContext,
    AttentionTriggerStrategy,
    default_attention_trigger_strategies,
)
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    AgentEntryDispatcher,
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
    def __init__(self, instance_id="test-bot", platform="mock", **kwargs):
        super().__init__(instance_id, platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []
        self.api_calls: list[tuple[str, dict]] = []

    async def start(self):
        pass

    async def shutdown(self):
        pass

    async def send(self, target_session, elements):
        self.sent.append((target_session, elements))
        return MessageHandle(message_id=f"sent-{len(self.sent)}", adapter_ref=self)

    async def call_api(self, method, params):
        self.api_calls.append((method, params))
        return {"ok": True}

    async def get_capabilities(self):
        return {"elements": ["text"], "actions": [], "limits": {}}


def make_event(content="hello", user_id="user-1", channel_type=1):
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


class KeywordDirectDispatchStrategy:
    def __init__(self, keyword: str) -> None:
        self._keyword = keyword

    def schedule(
        self,
        context: AttentionTriggerContext,
        actions: AttentionTriggerActions,
    ) -> bool:
        if self._keyword not in context.message.get_text(self_id=context.self_platform_id):
            return False
        actions.dispatch_immediately(context)
        return True


class _RecordingTriggerActions:
    def __init__(self, scheduler: RecordingAttentionScheduler) -> None:
        self._scheduler = scheduler

    def accumulate_attention(
        self,
        context: AttentionTriggerContext,
        *,
        is_mentioned: bool = False,
        attention_multiplier: float = 1.0,
    ) -> None:
        self._scheduler.calls.append(
            {
                "session_id": context.session_id,
                "msg_log_id": context.msg_log_id,
                "sender_id": context.sender_id,
                "response_profile": context.response_profile,
                "is_mentioned": is_mentioned,
                "is_reply_to_bot": context.is_reply_to_bot,
                "attention_multiplier": attention_multiplier,
                "self_platform_id": context.self_platform_id,
            }
        )

    def dispatch_immediately(self, context: AttentionTriggerContext) -> None:
        self._scheduler.direct_calls.append(
            {
                "session_id": context.session_id,
                "response_profile": context.response_profile,
            }
        )


class RecordingAttentionScheduler:
    def __init__(
        self,
        *,
        trigger_strategies: Iterable[AttentionTriggerStrategy] | None = None,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.direct_calls: list[dict[str, object]] = []
        self._trigger_strategies = list(
            trigger_strategies
            if trigger_strategies is not None
            else default_attention_trigger_strategies()
        )

    def schedule_message(
        self,
        session_id: str,
        msg_log_id: int | None,
        sender_id: str,
        *,
        response_profile: str = "balanced",
        message: Message | Iterable[MessageElement],
        self_platform_id: str = "",
        is_reply_to_bot: bool = False,
        already_handled: bool = False,
        is_stopped: bool = False,
    ) -> bool:
        if already_handled or is_stopped or msg_log_id is None:
            return False

        normalized_message = (
            message if isinstance(message, Message) else Message(elements=list(message))
        )
        context = AttentionTriggerContext(
            session_id=session_id,
            msg_log_id=msg_log_id,
            sender_id=sender_id,
            response_profile=response_profile,
            message=normalized_message,
            self_platform_id=self_platform_id,
            is_reply_to_bot=is_reply_to_bot,
        )
        actions = _RecordingTriggerActions(self)
        for strategy in self._trigger_strategies:
            if strategy.schedule(context, actions):
                return True
        return False


def make_agent_ingress(
    db: DatabaseManager,
    scheduler: RecordingAttentionScheduler,
    *,
    route_table: RouteTable | None = None,
    route_targets: RouteTargetRegistry | None = None,
) -> MessageIngress:
    table = route_table if route_table is not None else RouteTable()
    targets = route_targets if route_targets is not None else RouteTargetRegistry()
    agent_entry = AgentEntryDispatcher(
        attention_scheduler=scheduler,  # type: ignore[arg-type]
        database=db,
    )
    table.register(make_agent_entry_fallback_route_rule())
    targets.register(AGENT_ENTRY_TARGET, agent_entry)
    return MessageIngress(
        session_manager=SessionManager(session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        route_table=table,
        route_targets=targets,
        database=db,
    )


class TestAgentEntryAttentionScheduling:
    def setup_method(self):
        self.adapter = MockAdapter()

    @pytest.mark.asyncio
    async def test_agent_entry_dispatches_private_messages_directly_by_default(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        ingress = make_agent_ingress(db, scheduler)

        await ingress.process_event(make_event("hello private"), self.adapter)
        await asyncio.sleep(0)

        assert scheduler.calls == []
        assert scheduler.direct_calls == [
            {
                "session_id": "test-bot:private:user-1",
                "response_profile": "disabled",
            }
        ]

    @pytest.mark.asyncio
    async def test_agent_entry_accepts_custom_attention_trigger_strategy(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler(
            trigger_strategies=(
                KeywordDirectDispatchStrategy("!now"),
                *default_attention_trigger_strategies(),
            )
        )
        ingress = make_agent_ingress(db, scheduler)

        await ingress.process_event(make_event("please answer !now", channel_type=0), self.adapter)
        await asyncio.sleep(0)

        assert scheduler.calls == []
        assert scheduler.direct_calls == [
            {
                "session_id": "test-bot:group:group:1",
                "response_profile": "balanced",
            }
        ]

    @pytest.mark.asyncio
    async def test_agent_entry_routes_group_messages_to_balanced_profile_by_default(
        self,
        tmp_path,
    ):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        ingress = make_agent_ingress(db, scheduler)

        await ingress.process_event(make_event("hello group", channel_type=0), self.adapter)
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "balanced"
        assert scheduler.calls[0]["is_mentioned"] is False

    @pytest.mark.asyncio
    async def test_agent_entry_routes_priority_group_messages_to_immediate_profile(
        self,
        tmp_path,
    ):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        ingress = make_agent_ingress(db, scheduler)

        await ingress.process_event(
            make_event('<at id="bot-1"/>hello group', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "immediate"
        assert scheduler.calls[0]["is_mentioned"] is True

    @pytest.mark.asyncio
    async def test_agent_entry_attention_multiplier_for_poke_and_at_other(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        ingress = make_agent_ingress(db, scheduler)

        await ingress.process_event(
            make_event('<sb:poke target="bot-1" type="poke"/>', channel_type=0),
            self.adapter,
        )
        await ingress.process_event(
            make_event('<sb:poke target="user-2" type="poke"/>', channel_type=0),
            self.adapter,
        )
        await ingress.process_event(
            make_event('<at id="user-2"/>hello', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert [call["attention_multiplier"] for call in scheduler.calls] == [2.0, 0.2, 0.6]
        assert [call["is_mentioned"] for call in scheduler.calls] == [False, False, False]

        rows = db.message_logs.get_recent("test-bot:group:group:1", limit=3)
        assert rows[0]["raw_text"] == "[戳一戳: 戳了你一下]"
        assert rows[1]["raw_text"] == "[戳一戳: 戳了用户 user-2 一下]"
        assert rows[2]["raw_text"] == "[@用户 user-2]hello"

    @pytest.mark.asyncio
    async def test_agent_entry_uses_canonical_private_response_profile_config(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        db.bot_configs.upsert(
            BotConfigRecord(
                uuid="cfg-private-profile",
                instance_id=self.adapter.instance_id,
                config={"response_profile_private": "passive"},
            )
        )

        scheduler = RecordingAttentionScheduler()
        ingress = make_agent_ingress(db, scheduler)

        await ingress.process_event(make_event("hello private"), self.adapter)
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "passive"

    @pytest.mark.asyncio
    async def test_agent_entry_uses_canonical_group_and_priority_profiles(self, tmp_path):
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

        scheduler = RecordingAttentionScheduler()
        ingress = make_agent_ingress(db, scheduler)

        await ingress.process_event(make_event("hello group", channel_type=0), self.adapter)
        await ingress.process_event(
            make_event('<at id="bot-1"/>hello group', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 2
        assert scheduler.calls[0]["response_profile"] == "passive"
        assert scheduler.calls[1]["response_profile"] == "balanced"
        assert scheduler.calls[1]["is_mentioned"] is True

    @pytest.mark.asyncio
    async def test_agent_entry_fallback_is_skipped_when_plugin_route_replied(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        table = RouteTable()
        targets = RouteTargetRegistry()
        plugin_rule = RouteRule(
            id="plugin-reply",
            priority=10,
            condition=RouteCondition(event_types=frozenset({"message-created"})),
            target="plugin-reply",
        )
        table.register(plugin_rule)

        async def handler(context, _rule):
            await context.require_message_context().send("plugin reply")

        targets.register("plugin-reply", handler)
        ingress = make_agent_ingress(db, scheduler, route_table=table, route_targets=targets)

        result = await ingress.process_event(make_event("hello plugin"), self.adapter)
        await asyncio.sleep(0)

        assert result.matched_rules == [plugin_rule]
        assert len(self.adapter.sent) == 1
        assert Message(elements=self.adapter.sent[0][1]).get_text() == "plugin reply"
        assert scheduler.calls == []
        assert scheduler.direct_calls == []
