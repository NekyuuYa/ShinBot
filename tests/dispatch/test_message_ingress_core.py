"""Core message ingress behavior tests."""

import asyncio
import time

import pytest

from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    NOTICE_DISPATCHER_TARGET,
    AgentEntryDispatcher,
    AgentEntrySignal,
    NoticeDispatcher,
    make_agent_entry_fallback_route_rule,
    make_notice_route_rule,
)
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import MessageIngress, RouteTargetRegistry
from shinbot.core.dispatch.routing import RouteCondition, RouteRule, RouteTable
from shinbot.core.message_routes import (
    TEXT_COMMAND_DISPATCHER_TARGET,
    TextCommandDispatcher,
    make_text_command_route_rule,
)
from shinbot.core.message_routes.command import CommandDef, CommandRegistry
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord
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


class RecordingAgentHandler:
    def __init__(self) -> None:
        self.signals: list[AgentEntrySignal] = []

    def __call__(self, signal: AgentEntrySignal) -> None:
        self.signals.append(signal)


class RecordingMediaService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def ingest_message_media(self, **kwargs):
        self.calls.append(kwargs)
        return []


def add_message_route(table: RouteTable, *, target: str = "recorder") -> RouteRule:
    rule = RouteRule(
        id=f"route.{target}",
        priority=10,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target=target,
    )
    table.register(rule)
    return rule


class TestMessageIngressCore:
    def setup_method(self):
        self.session_mgr = SessionManager()
        self.perm_engine = PermissionEngine()
        self.cmd_registry = CommandRegistry()
        self.event_bus = EventBus()
        self.adapter = MockAdapter()

    def make_ingress(
        self,
        *,
        route_table: RouteTable | None = None,
        route_targets: RouteTargetRegistry | None = None,
        session_manager: SessionManager | None = None,
        database: DatabaseManager | None = None,
        media_service=None,
    ) -> MessageIngress:
        return MessageIngress(
            session_manager=session_manager if session_manager is not None else self.session_mgr,
            permission_engine=self.perm_engine,
            route_table=route_table or RouteTable(),
            route_targets=route_targets,
            database=database,
            media_service=media_service,
        )

    @pytest.mark.asyncio
    async def test_basic_event_processing(self):
        """A message route should create a session and dispatch a route target."""
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        results = []

        async def handler(context, _rule):
            results.append(context.require_message_context().text)

        targets.register("recorder", handler)
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        await ingress.process_event(make_event("hello"), self.adapter)
        await asyncio.sleep(0)

        assert results == ["hello"]
        assert len(self.session_mgr) == 1

    @pytest.mark.asyncio
    async def test_command_dispatch(self):
        """Commands should be resolved and dispatched to handler."""
        results = []

        async def ping_handler(ctx, args):
            results.append(f"pong {args}")

        self.cmd_registry.register(CommandDef(name="ping", handler=ping_handler))
        command_dispatcher = TextCommandDispatcher(self.cmd_registry)
        table = RouteTable()
        command_rule = make_text_command_route_rule(command_dispatcher)
        table.register(command_rule)
        targets = RouteTargetRegistry()
        targets.register(TEXT_COMMAND_DISPATCHER_TARGET, command_dispatcher)
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        result = await ingress.process_event(make_event("/ping 123"), self.adapter)
        await asyncio.sleep(0)

        assert result.matched_rules == [command_rule]
        assert results == ["pong 123"]

    @pytest.mark.asyncio
    async def test_command_not_in_fallback(self):
        """When a command matches, fallback should not be triggered."""
        fallback_calls = []

        async def cmd_handler(ctx, args):
            pass

        self.cmd_registry.register(CommandDef(name="ping", handler=cmd_handler))
        command_dispatcher = TextCommandDispatcher(self.cmd_registry)
        table = RouteTable()
        command_rule = make_text_command_route_rule(command_dispatcher)
        fallback_rule = make_agent_entry_fallback_route_rule()
        table.register(command_rule)
        table.register(fallback_rule)
        targets = RouteTargetRegistry()
        targets.register(TEXT_COMMAND_DISPATCHER_TARGET, command_dispatcher)
        targets.register(AGENT_ENTRY_TARGET, lambda _context, _rule: fallback_calls.append(True))
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        result = await ingress.process_event(make_event("/ping"), self.adapter)
        await asyncio.sleep(0)

        assert result.matched_rules == [command_rule]
        assert fallback_calls == []

    @pytest.mark.asyncio
    async def test_interceptor_blocks(self):
        """Interceptor returning False should block processing."""
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        results = []
        targets.register("recorder", lambda _context, _rule: results.append(True))
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        async def blocker(ctx):
            return False

        ingress.add_interceptor(blocker)

        await ingress.process_event(make_event("hello"), self.adapter)
        await asyncio.sleep(0)

        assert results == []

    @pytest.mark.asyncio
    async def test_interceptor_block_still_persists_but_skips_media_ingest(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        media_service = RecordingMediaService()
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        handled = []
        targets.register("recorder", lambda context, _rule: handled.append(context.message.text))
        ingress = self.make_ingress(
            route_table=table,
            route_targets=targets,
            database=db,
            media_service=media_service,
        )

        async def blocker(ctx):
            return False

        ingress.add_interceptor(blocker)

        await ingress.process_event(make_event("blocked"), self.adapter)

        rows = db.message_logs.get_recent("test-bot:private:user-1", limit=1)
        assert len(rows) == 1
        assert rows[0]["raw_text"] == "blocked"
        assert media_service.calls == []
        assert handled == []

    @pytest.mark.asyncio
    async def test_interceptor_allows(self):
        """Interceptor returning True should allow processing."""
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        results = []
        targets.register("recorder", lambda _context, _rule: results.append(True))
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        async def allower(ctx):
            return True

        ingress.add_interceptor(allower)

        await ingress.process_event(make_event("hello"), self.adapter)
        await asyncio.sleep(0)

        assert results == [True]

    @pytest.mark.asyncio
    async def test_muted_session_skips(self):
        """Muted sessions should not dispatch matched routes."""
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        results = []
        targets.register("recorder", lambda _context, _rule: results.append(True))
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        event = make_event("hello")
        session = self.session_mgr.get_or_create(self.adapter.instance_id, event)
        session.config.is_muted = True

        await ingress.process_event(event, self.adapter)
        await asyncio.sleep(0)

        assert results == []

    @pytest.mark.asyncio
    async def test_muted_session_persists_but_skips_interceptors_and_media_ingest(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        session_manager = SessionManager(session_repo=db.sessions)
        media_service = RecordingMediaService()
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        targets.register("recorder", lambda _context, _rule: None)
        ingress = self.make_ingress(
            route_table=table,
            route_targets=targets,
            session_manager=session_manager,
            database=db,
            media_service=media_service,
        )
        interceptor_calls = []

        async def interceptor(ctx):
            interceptor_calls.append(ctx.text)
            return True

        event = make_event("muted")
        session = session_manager.get_or_create(self.adapter.instance_id, event)
        session.config.is_muted = True
        session_manager.update(session)
        ingress.add_interceptor(interceptor)

        await ingress.process_event(event, self.adapter)

        rows = db.message_logs.get_recent("test-bot:private:user-1", limit=1)
        assert len(rows) == 1
        assert rows[0]["raw_text"] == "muted"
        assert interceptor_calls == []
        assert media_service.calls == []

    @pytest.mark.asyncio
    async def test_command_permission_denied(self):
        """Commands with insufficient permissions should be rejected."""
        results = []

        async def secret_handler(ctx, args):
            results.append(True)

        self.cmd_registry.register(
            CommandDef(name="secret", handler=secret_handler, permission="admin.secret")
        )
        command_dispatcher = TextCommandDispatcher(self.cmd_registry)
        table = RouteTable()
        table.register(make_text_command_route_rule(command_dispatcher))
        targets = RouteTargetRegistry()
        targets.register(TEXT_COMMAND_DISPATCHER_TARGET, command_dispatcher)
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        await ingress.process_event(make_event("/secret"), self.adapter)
        await asyncio.sleep(0)

        assert results == []
        assert len(self.adapter.sent) == 1

    @pytest.mark.asyncio
    async def test_agent_entry_signal_includes_reply_to_bot_flag(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        session_id = "test-bot:group:group:1"
        db.message_logs.insert(
            MessageLogRecord(
                session_id=session_id,
                platform_msg_id="bot-msg-1",
                sender_id="bot-1",
                sender_name="Bot",
                content_json="[]",
                raw_text="earlier bot reply",
                role="assistant",
                is_read=True,
                is_mentioned=False,
                created_at=time.time() * 1000,
            )
        )

        agent_handler = RecordingAgentHandler()
        agent_entry = AgentEntryDispatcher(
            handler=agent_handler,
            database=db,
        )
        table = RouteTable()
        fallback_rule = make_agent_entry_fallback_route_rule()
        table.register(fallback_rule)
        targets = RouteTargetRegistry()
        targets.register(AGENT_ENTRY_TARGET, agent_entry)
        ingress = self.make_ingress(
            route_table=table,
            route_targets=targets,
            session_manager=SessionManager(session_repo=db.sessions),
            database=db,
        )

        event = make_event('<quote id="bot-msg-1"/>follow-up', channel_type=0)
        await ingress.process_event(event, self.adapter)
        await asyncio.sleep(0)

        assert len(agent_handler.signals) == 1
        assert agent_handler.signals[0].is_reply_to_bot is True
        assert agent_handler.signals[0].self_id == "bot-1"
        assert not hasattr(agent_handler.signals[0], "message")

    @pytest.mark.asyncio
    async def test_message_log_is_mentioned_uses_recursive_mention_detection(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        ingress = self.make_ingress(
            route_table=RouteTable(),
            database=db,
            session_manager=SessionManager(session_repo=db.sessions),
        )
        content = Message.from_elements(
            MessageElement.quote(
                "quoted-msg",
                children=[MessageElement.at(id=" bot-1 ")],
            ),
            MessageElement.text(" after quote"),
        ).to_xml()

        await ingress.process_event(make_event(content), self.adapter)

        rows = db.message_logs.get_recent("test-bot:private:user-1", limit=1)
        assert len(rows) == 1
        assert rows[0]["is_mentioned"] is True

    @pytest.mark.asyncio
    async def test_non_message_event(self):
        """Non-message events should go through the notice dispatcher as UnifiedEvent."""
        results = []

        async def handler(event):
            results.append(event.type)

        self.event_bus.on("member-joined", handler)
        notice_dispatcher = NoticeDispatcher(self.event_bus)
        table = RouteTable()
        notice_rule = make_notice_route_rule(notice_dispatcher)
        table.register(notice_rule)
        targets = RouteTargetRegistry()
        targets.register(NOTICE_DISPATCHER_TARGET, notice_dispatcher)
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        event = UnifiedEvent(
            type="member-joined",
            platform="mock",
            user=User(id="user-1"),
            channel=Channel(id="group:1", type=0),
        )
        result = await ingress.process_event(event, self.adapter)
        await asyncio.sleep(0)

        assert result.matched_rules == [notice_rule]
        assert results == ["member-joined"]

    @pytest.mark.asyncio
    async def test_empty_message_content(self):
        """Events with empty message content should still dispatch with an empty AST."""
        table = RouteTable()
        add_message_route(table)
        targets = RouteTargetRegistry()
        results = []

        async def handler(context, _rule):
            results.append(len(context.message.elements))

        targets.register("recorder", handler)
        ingress = self.make_ingress(route_table=table, route_targets=targets)

        event = UnifiedEvent(
            type="message-created",
            platform="mock",
            user=User(id="user-1"),
            channel=Channel(id="private:user-1", type=1),
            message=MessagePayload(id="msg-1", content=""),
        )
        await ingress.process_event(event, self.adapter)
        await asyncio.sleep(0)

        assert results == [0]
