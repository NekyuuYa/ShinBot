"""Tests for message pipeline dispatch."""

import asyncio

import pytest

from shinbot.agent.attention.scheduler import AttentionScheduler
from shinbot.agent.attention.trigger_strategy import (
    AttentionTriggerActions,
    AttentionTriggerContext,
    AttentionTriggerStrategy,
    default_attention_trigger_strategies,
)
from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.pipeline import MessagePipeline
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    BotConfigRecord,
)
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User

pytestmark = [pytest.mark.integration, pytest.mark.slow]

# ── Mock adapter for testing ─────────────────────────────────────────


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


# ── Fixtures ─────────────────────────────────────────────────────────


def make_event(content="hello", user_id="user-1", channel_type=1):
    return UnifiedEvent(
        type="message-created",
        self_id="bot-1",
        platform="mock",
        user=User(id=user_id),
        channel=Channel(
            id=f"private:{user_id}" if channel_type == 1 else "group:1", type=channel_type
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


class RecordingAttentionScheduler(AttentionScheduler):
    def __init__(
        self,
        *,
        trigger_strategies: tuple[AttentionTriggerStrategy, ...] | None = None,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.direct_calls: list[dict[str, object]] = []
        if trigger_strategies is not None:
            self._trigger_strategies = list(trigger_strategies)

    async def dispatch_immediately(
        self,
        session_id: str,
        *,
        response_profile: str = "disabled",
    ) -> None:
        self.direct_calls.append(
            {
                "session_id": session_id,
                "response_profile": response_profile,
            }
        )

    async def on_message(
        self,
        session_id: str,
        msg_log_id: int,
        sender_id: str,
        *,
        response_profile: str = "balanced",
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
        attention_multiplier: float = 1.0,
        self_platform_id: str = "",
    ) -> None:
        self.calls.append(
            {
                "session_id": session_id,
                "msg_log_id": msg_log_id,
                "sender_id": sender_id,
                "response_profile": response_profile,
                "is_mentioned": is_mentioned,
                "is_reply_to_bot": is_reply_to_bot,
                "attention_multiplier": attention_multiplier,
                "self_platform_id": self_platform_id,
            }
        )


class TestMessagePipeline:
    def setup_method(self):
        self.adapter_mgr = AdapterManager()
        self.adapter_mgr.register_adapter("mock", MockAdapter)
        self.session_mgr = SessionManager()
        self.perm_engine = PermissionEngine()
        self.cmd_registry = CommandRegistry()
        self.event_bus = EventBus()
        self.pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
        )
        self.adapter = MockAdapter()

    @pytest.mark.asyncio
    async def test_pipeline_dispatches_private_messages_directly_by_default(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        await pipeline.process_event(make_event("hello private"), self.adapter)
        await asyncio.sleep(0)

        assert scheduler.calls == []
        assert scheduler.direct_calls == [
            {
                "session_id": "test-bot:private:user-1",
                "response_profile": "disabled",
            }
        ]

    @pytest.mark.asyncio
    async def test_pipeline_accepts_custom_attention_trigger_strategy(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler(
            trigger_strategies=(
                KeywordDirectDispatchStrategy("!now"),
                *default_attention_trigger_strategies(),
            )
        )
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        await pipeline.process_event(make_event("please answer !now", channel_type=0), self.adapter)
        await asyncio.sleep(0)

        assert scheduler.calls == []
        assert scheduler.direct_calls == [
            {
                "session_id": "test-bot:group:group:1",
                "response_profile": "balanced",
            }
        ]

    @pytest.mark.asyncio
    async def test_pipeline_routes_group_messages_to_balanced_profile_by_default(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        await pipeline.process_event(make_event("hello group", channel_type=0), self.adapter)
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "balanced"
        assert scheduler.calls[0]["is_mentioned"] is False

    @pytest.mark.asyncio
    async def test_pipeline_routes_priority_group_messages_to_immediate_profile(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        await pipeline.process_event(
            make_event('<at id="bot-1"/>hello group', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "immediate"
        assert scheduler.calls[0]["is_mentioned"] is True

    @pytest.mark.asyncio
    async def test_pipeline_attention_multiplier_for_poke_and_at_other(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        await pipeline.process_event(
            make_event('<sb:poke target="bot-1" type="poke"/>', channel_type=0),
            self.adapter,
        )
        await pipeline.process_event(
            make_event('<sb:poke target="user-2" type="poke"/>', channel_type=0),
            self.adapter,
        )
        await pipeline.process_event(
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
    async def test_pipeline_uses_canonical_private_response_profile_config(self, tmp_path):
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
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        await pipeline.process_event(make_event("hello private"), self.adapter)
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 1
        assert scheduler.calls[0]["response_profile"] == "passive"

    @pytest.mark.asyncio
    async def test_pipeline_uses_canonical_group_and_priority_profiles(self, tmp_path):
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
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        await pipeline.process_event(make_event("hello group", channel_type=0), self.adapter)
        await pipeline.process_event(
            make_event('<at id="bot-1"/>hello group', channel_type=0),
            self.adapter,
        )
        await asyncio.sleep(0)

        assert len(scheduler.calls) == 2
        assert scheduler.calls[0]["response_profile"] == "passive"
        assert scheduler.calls[1]["response_profile"] == "balanced"
        assert scheduler.calls[1]["is_mentioned"] is True

    @pytest.mark.asyncio
    async def test_pipeline_attention_scheduler_skips_when_plugin_already_replied(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        scheduler = RecordingAttentionScheduler()
        pipeline = MessagePipeline(
            adapter_manager=self.adapter_mgr,
            session_manager=self.session_mgr,
            permission_engine=self.perm_engine,
            command_registry=self.cmd_registry,
            event_bus=self.event_bus,
            database=db,
            attention_scheduler=scheduler,  # type: ignore[arg-type]
        )

        async def handler(ctx):
            await ctx.send("plugin reply")

        self.event_bus.on("message-created", handler)
        await pipeline.process_event(make_event("hello plugin"), self.adapter)
        await asyncio.sleep(0)

        assert len(self.adapter.sent) == 1
        assert Message(elements=self.adapter.sent[0][1]).get_text() == "plugin reply"
        assert scheduler.calls == []
