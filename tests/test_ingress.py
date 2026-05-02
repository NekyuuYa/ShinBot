"""Tests for the message ingress routing flow."""

from __future__ import annotations

import asyncio
import logging
import time

import pytest

from shinbot.core.dispatch.command import CommandDef, CommandRegistry
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    KEYWORD_DISPATCHER_TARGET,
    NOTICE_DISPATCHER_TARGET,
    TEXT_COMMAND_DISPATCHER_TARGET,
    AgentEntryDispatcher,
    AgentEntrySignal,
    KeywordDispatcher,
    NoticeDispatcher,
    TextCommandDispatcher,
    make_agent_entry_fallback_route_rule,
    make_keyword_route_rule,
    make_notice_route_rule,
    make_text_command_route_rule,
)
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import (
    ROUTING_SKIP_EXPIRED_MESSAGE,
    ROUTING_SKIP_INTERCEPTOR_BLOCKED,
    ROUTING_SKIP_NO_ROUTE_MATCHED,
    ROUTING_SKIP_SESSION_MUTED,
    ROUTING_SKIP_WAIT_FOR_INPUT,
    MessageIngress,
    RouteDispatchContext,
    RouteTargetRegistry,
    is_event_fresh,
)
from shinbot.core.dispatch.keyword import KeywordDef, KeywordRegistry
from shinbot.core.dispatch.message_context import WaitingInputRegistry
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule, RouteTable
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.schema.elements import MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id: str = "test-bot", platform: str = "mock") -> None:
        super().__init__(instance_id, platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []

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
        return {"method": method, "params": params}

    async def get_capabilities(self) -> dict:
        return {"elements": ["text"], "actions": [], "limits": {}}


class RecordingAgentHandler:
    def __init__(self, *, handled: bool = True) -> None:
        self.handled = handled
        self.signals: list[AgentEntrySignal] = []

    def __call__(self, signal: AgentEntrySignal) -> bool:
        self.signals.append(signal)
        return self.handled


def make_event(
    content: str = "hello",
    *,
    event_type: str = "message-created",
    timestamp: int | None = None,
    private: bool = True,
) -> UnifiedEvent:
    return UnifiedEvent(
        type=event_type,
        self_id="bot-1",
        platform="mock",
        timestamp=timestamp,
        user=User(id="user-1", name="Alice"),
        channel=Channel(id="private:user-1" if private else "group:1", type=1 if private else 0),
        message=MessagePayload(id="msg-1", content=content)
        if event_type.startswith("message-")
        else None,
    )


def build_ingress(
    tmp_path,
    *,
    route_table: RouteTable | None = None,
    route_targets: RouteTargetRegistry | None = None,
    session_manager: SessionManager | None = None,
    waiting_registry: WaitingInputRegistry | None = None,
    max_message_age_seconds: int = 60,
) -> tuple[MessageIngress, DatabaseManager, MockAdapter]:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    ingress = MessageIngress(
        session_manager=session_manager or SessionManager(session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        route_table=route_table or RouteTable(),
        route_targets=route_targets,
        database=db,
        waiting_registry=waiting_registry,
        max_message_age_seconds=max_message_age_seconds,
    )
    return ingress, db, MockAdapter()


def add_message_route(table: RouteTable, *, target: str = "recorder") -> RouteRule:
    rule = RouteRule(
        id=f"route.{target}",
        priority=10,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target=target,
    )
    table.register(rule)
    return rule


async def noop_command(_ctx, _args) -> None:
    pass


@pytest.mark.asyncio
async def test_ingress_persists_then_marks_dispatched_and_schedules_target(tmp_path) -> None:
    table = RouteTable()
    rule = add_message_route(table)
    targets = RouteTargetRegistry()
    calls: list[tuple[str, str, str]] = []

    async def handler(context: RouteDispatchContext, matched_rule: RouteRule) -> None:
        message_context = context.require_message_context()
        calls.append((matched_rule.id, message_context.session.id, message_context.text))

    targets.register("recorder", handler)
    ingress, db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    result = await ingress.process_event(make_event("hello"), adapter)
    await asyncio.sleep(0)

    assert result.message_log_id is not None
    assert result.matched_rules == [rule]
    assert result.skipped_reason is None
    assert calls == [("route.recorder", "test-bot:private:user-1", "hello")]

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["raw_text"] == "hello"
    assert row["routing_status"] == "dispatched"
    assert row["routed_at"] is not None
    assert row["routing_skip_reason"] is None


@pytest.mark.asyncio
async def test_ingress_marks_expired_message_skipped_before_route_dispatch(tmp_path) -> None:
    table = RouteTable()
    add_message_route(table)
    targets = RouteTargetRegistry()
    calls: list[str] = []
    targets.register("recorder", lambda _context, _rule: calls.append("called"))
    ingress, db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    event = make_event(timestamp=int((time.time() - 120) * 1000))
    result = await ingress.process_event(event, adapter)

    assert result.matched_rules == []
    assert result.skipped_reason == ROUTING_SKIP_EXPIRED_MESSAGE
    assert calls == []
    assert result.message_log_id is not None

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "skipped"
    assert row["routing_skip_reason"] == ROUTING_SKIP_EXPIRED_MESSAGE


@pytest.mark.asyncio
async def test_ingress_marks_no_route_match_skipped(tmp_path) -> None:
    ingress, db, adapter = build_ingress(tmp_path)

    result = await ingress.process_event(make_event("quiet"), adapter)

    assert result.matched_rules == []
    assert result.skipped_reason == ROUTING_SKIP_NO_ROUTE_MATCHED
    assert result.message_log_id is not None

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "skipped"
    assert row["routing_skip_reason"] == ROUTING_SKIP_NO_ROUTE_MATCHED


@pytest.mark.asyncio
async def test_wait_for_input_reply_is_persisted_and_skipped(tmp_path) -> None:
    waiting_registry = WaitingInputRegistry()
    future = waiting_registry.register("test-bot:private:user-1")
    ingress, db, adapter = build_ingress(tmp_path, waiting_registry=waiting_registry)

    result = await ingress.process_event(make_event("Nekyuu"), adapter)

    assert future.done()
    assert future.result() == "Nekyuu"
    assert result.matched_rules == []
    assert result.skipped_reason == ROUTING_SKIP_WAIT_FOR_INPUT
    assert result.message_log_id is not None

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["raw_text"] == "Nekyuu"
    assert row["routing_status"] == "skipped"
    assert row["routing_skip_reason"] == ROUTING_SKIP_WAIT_FOR_INPUT


@pytest.mark.asyncio
async def test_ingress_missing_target_still_marks_dispatched(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    table = RouteTable()
    rule = add_message_route(table, target="missing")
    ingress, db, adapter = build_ingress(tmp_path, route_table=table)

    with caplog.at_level(logging.ERROR, logger="shinbot.core.dispatch.ingress"):
        result = await ingress.process_event(make_event("hello"), adapter)

    assert result.matched_rules == [rule]
    assert result.message_log_id is not None
    assert "route_target_missing: rule_id=route.missing target=missing" in caplog.text

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "dispatched"
    assert row["routing_skip_reason"] is None


@pytest.mark.asyncio
async def test_ingress_marks_muted_session_skipped(tmp_path) -> None:
    table = RouteTable()
    add_message_route(table)
    targets = RouteTargetRegistry()
    calls: list[str] = []
    targets.register("recorder", lambda _context, _rule: calls.append("called"))

    event = make_event("muted")
    adapter = MockAdapter()
    session_manager = SessionManager()
    session = session_manager.get_or_create(adapter.instance_id, event)
    session.config.is_muted = True
    session_manager.update(session)

    ingress, db, _adapter = build_ingress(
        tmp_path,
        route_table=table,
        route_targets=targets,
        session_manager=session_manager,
    )

    result = await ingress.process_event(event, adapter)

    assert result.skipped_reason == ROUTING_SKIP_SESSION_MUTED
    assert calls == []
    assert result.message_log_id is not None

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "skipped"
    assert row["routing_skip_reason"] == ROUTING_SKIP_SESSION_MUTED


@pytest.mark.asyncio
async def test_ingress_marks_interceptor_blocked_skipped(tmp_path) -> None:
    table = RouteTable()
    add_message_route(table)
    targets = RouteTargetRegistry()
    calls: list[str] = []
    targets.register("recorder", lambda _context, _rule: calls.append("called"))
    ingress, db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    async def block(_message_context) -> bool:
        return False

    ingress.add_interceptor(block)
    result = await ingress.process_event(make_event("blocked"), adapter)

    assert result.skipped_reason == ROUTING_SKIP_INTERCEPTOR_BLOCKED
    assert calls == []
    assert result.message_log_id is not None

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "skipped"
    assert row["routing_skip_reason"] == ROUTING_SKIP_INTERCEPTOR_BLOCKED


def test_is_event_fresh_uses_platform_timestamp_milliseconds() -> None:
    now = 1000.0
    fresh = make_event(timestamp=995_000)
    stale = make_event(timestamp=900_000)

    assert is_event_fresh(fresh, now=now, max_age_seconds=60)
    assert not is_event_fresh(stale, now=now, max_age_seconds=60)
    assert is_event_fresh(make_event(timestamp=None), now=now, max_age_seconds=60)


@pytest.mark.asyncio
async def test_text_command_dispatcher_executes_command_without_fallback(tmp_path) -> None:
    command_registry = CommandRegistry()
    command_calls: list[str] = []

    async def ping(ctx, args) -> None:
        command_calls.append(f"{ctx.text}|{args}")

    command_registry.register(CommandDef(name="ping", handler=ping))
    command_dispatcher = TextCommandDispatcher(command_registry)

    table = RouteTable()
    command_rule = make_text_command_route_rule(command_dispatcher)
    fallback_rule = RouteRule(
        id="fallback",
        priority=-1000,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target="fallback",
    )
    table.register(command_rule)
    table.register(fallback_rule)

    targets = RouteTargetRegistry()
    fallback_calls: list[str] = []
    targets.register(TEXT_COMMAND_DISPATCHER_TARGET, command_dispatcher)
    targets.register("fallback", lambda _context, _rule: fallback_calls.append("fallback"))
    ingress, db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    result = await ingress.process_event(make_event("/ping hello"), adapter)
    await asyncio.sleep(0)

    assert result.matched_rules == [command_rule]
    assert command_calls == ["/ping hello|hello"]
    assert fallback_calls == []
    assert result.message_log_id is not None
    assert db.message_logs.get(result.message_log_id)["routing_status"] == "dispatched"


@pytest.mark.asyncio
async def test_text_command_route_does_not_prevent_fallback_for_plain_text(tmp_path) -> None:
    command_registry = CommandRegistry()
    command_registry.register(CommandDef(name="ping", handler=noop_command))
    command_dispatcher = TextCommandDispatcher(command_registry)

    table = RouteTable()
    fallback_rule = RouteRule(
        id="fallback",
        priority=-1000,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target="fallback",
    )
    table.register(make_text_command_route_rule(command_dispatcher))
    table.register(fallback_rule)

    targets = RouteTargetRegistry()
    fallback_calls: list[str] = []
    targets.register(TEXT_COMMAND_DISPATCHER_TARGET, command_dispatcher)
    targets.register("fallback", lambda _context, _rule: fallback_calls.append("fallback"))
    ingress, _db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    result = await ingress.process_event(make_event("plain text"), adapter)
    await asyncio.sleep(0)

    assert result.matched_rules == [fallback_rule]
    assert fallback_calls == ["fallback"]


@pytest.mark.asyncio
async def test_text_command_dispatcher_uses_session_prefixes_for_route_matching(tmp_path) -> None:
    command_registry = CommandRegistry()
    command_calls: list[str] = []

    async def ping(_ctx, args) -> None:
        command_calls.append(args)

    command_registry.register(CommandDef(name="ping", handler=ping))
    command_dispatcher = TextCommandDispatcher(command_registry)

    table = RouteTable()
    command_rule = make_text_command_route_rule(command_dispatcher)
    table.register(command_rule)
    targets = RouteTargetRegistry()
    targets.register(TEXT_COMMAND_DISPATCHER_TARGET, command_dispatcher)

    adapter = MockAdapter()
    event = make_event("#ping custom")
    session_manager = SessionManager()
    session = session_manager.get_or_create(adapter.instance_id, event)
    session.config.prefixes = ["#"]
    session_manager.update(session)

    ingress, _db, _adapter = build_ingress(
        tmp_path,
        route_table=table,
        route_targets=targets,
        session_manager=session_manager,
    )

    result = await ingress.process_event(event, adapter)
    await asyncio.sleep(0)

    assert result.matched_rules == [command_rule]
    assert command_calls == ["custom"]


@pytest.mark.asyncio
async def test_text_command_dispatcher_denies_missing_permission(tmp_path) -> None:
    command_registry = CommandRegistry()
    command_calls: list[str] = []

    async def secret(_ctx, _args) -> None:
        command_calls.append("secret")

    command_registry.register(
        CommandDef(name="secret", handler=secret, permission="admin.secret")
    )
    command_dispatcher = TextCommandDispatcher(command_registry)

    table = RouteTable()
    command_rule = make_text_command_route_rule(command_dispatcher)
    table.register(command_rule)
    targets = RouteTargetRegistry()
    targets.register(TEXT_COMMAND_DISPATCHER_TARGET, command_dispatcher)
    ingress, _db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    result = await ingress.process_event(make_event("/secret"), adapter)
    await asyncio.sleep(0)

    assert result.matched_rules == [command_rule]
    assert command_calls == []
    assert len(adapter.sent) == 1
    assert adapter.sent[0][1][0].text_content == "权限不足：需要 admin.secret"


@pytest.mark.asyncio
async def test_keyword_dispatcher_executes_keywords_without_fallback(tmp_path) -> None:
    keyword_registry = KeywordRegistry()
    keyword_calls: list[tuple[str, str]] = []

    async def handler(ctx, match) -> None:
        keyword_calls.append((ctx.text, match.matched_text))

    keyword_registry.register(KeywordDef(pattern="needle", handler=handler))
    keyword_dispatcher = KeywordDispatcher(keyword_registry)

    table = RouteTable()
    keyword_rule = make_keyword_route_rule(keyword_dispatcher)
    fallback_rule = make_agent_entry_fallback_route_rule()
    table.register(keyword_rule)
    table.register(fallback_rule)

    targets = RouteTargetRegistry()
    fallback_calls: list[str] = []
    targets.register(KEYWORD_DISPATCHER_TARGET, keyword_dispatcher)
    targets.register(AGENT_ENTRY_TARGET, lambda _context, _rule: fallback_calls.append("fallback"))
    ingress, db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    result = await ingress.process_event(make_event("find the needle"), adapter)
    await asyncio.sleep(0)

    assert result.matched_rules == [keyword_rule]
    assert keyword_calls == [("find the needle", "needle")]
    assert fallback_calls == []
    assert result.message_log_id is not None
    assert db.message_logs.get(result.message_log_id)["routing_status"] == "dispatched"


@pytest.mark.asyncio
async def test_keyword_route_does_not_prevent_fallback_when_no_keyword_matches(tmp_path) -> None:
    keyword_registry = KeywordRegistry()
    keyword_registry.register(KeywordDef(pattern="needle", handler=noop_command))
    keyword_dispatcher = KeywordDispatcher(keyword_registry)

    table = RouteTable()
    fallback_rule = make_agent_entry_fallback_route_rule()
    table.register(make_keyword_route_rule(keyword_dispatcher))
    table.register(fallback_rule)

    targets = RouteTargetRegistry()
    fallback_calls: list[str] = []
    targets.register(KEYWORD_DISPATCHER_TARGET, keyword_dispatcher)
    targets.register(AGENT_ENTRY_TARGET, lambda _context, _rule: fallback_calls.append("fallback"))
    ingress, _db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    result = await ingress.process_event(make_event("plain text"), adapter)
    await asyncio.sleep(0)

    assert result.matched_rules == [fallback_rule]
    assert fallback_calls == ["fallback"]


@pytest.mark.asyncio
async def test_notice_dispatcher_forwards_unified_event_to_event_bus(tmp_path) -> None:
    event_bus = EventBus()
    notice_calls: list[str] = []

    async def handler(event: UnifiedEvent) -> None:
        notice_calls.append(event.type)

    event_bus.on("guild-member-added", handler)
    notice_dispatcher = NoticeDispatcher(event_bus)

    table = RouteTable()
    notice_rule = make_notice_route_rule(notice_dispatcher)
    table.register(notice_rule)

    targets = RouteTargetRegistry()
    targets.register(NOTICE_DISPATCHER_TARGET, notice_dispatcher)
    ingress, db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    result = await ingress.process_event(make_event(event_type="guild-member-added"), adapter)
    await asyncio.sleep(0)

    assert result.matched_rules == [notice_rule]
    assert result.message_log_id is None
    assert result.skipped_reason is None
    assert notice_calls == ["guild-member-added"]
    assert db.message_logs.get_recent("test-bot:private:user-1") == []


@pytest.mark.asyncio
async def test_notice_dispatcher_skips_notice_without_event_bus_handler(tmp_path) -> None:
    notice_dispatcher = NoticeDispatcher(EventBus())

    table = RouteTable()
    table.register(make_notice_route_rule(notice_dispatcher))

    targets = RouteTargetRegistry()
    targets.register(NOTICE_DISPATCHER_TARGET, notice_dispatcher)
    ingress, db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    result = await ingress.process_event(make_event(event_type="guild-member-added"), adapter)

    assert result.matched_rules == []
    assert result.message_log_id is None
    assert result.skipped_reason == ROUTING_SKIP_NO_ROUTE_MATCHED
    assert db.message_logs.get_recent("test-bot:private:user-1") == []


@pytest.mark.asyncio
async def test_notice_without_route_is_skipped_without_persistence(tmp_path) -> None:
    ingress, db, adapter = build_ingress(tmp_path)

    result = await ingress.process_event(make_event(event_type="guild-member-added"), adapter)

    assert result.matched_rules == []
    assert result.message_log_id is None
    assert result.skipped_reason == ROUTING_SKIP_NO_ROUTE_MATCHED
    assert db.message_logs.get_recent("test-bot:private:user-1") == []


@pytest.mark.asyncio
async def test_agent_entry_fallback_notifies_agent_with_minimal_signal(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    agent_handler = RecordingAgentHandler(handled=True)
    agent_entry_dispatcher = AgentEntryDispatcher(handler=agent_handler, database=db)

    table = RouteTable()
    fallback_rule = make_agent_entry_fallback_route_rule()
    table.register(fallback_rule)
    targets = RouteTargetRegistry()
    targets.register(AGENT_ENTRY_TARGET, agent_entry_dispatcher)
    ingress = MessageIngress(
        session_manager=SessionManager(session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        route_table=table,
        route_targets=targets,
        database=db,
    )

    result = await ingress.process_event(make_event("hello group", private=False), MockAdapter())
    await asyncio.sleep(0)

    assert result.matched_rules == [fallback_rule]
    assert result.message_log_id is not None
    assert len(agent_handler.signals) == 1

    signal = agent_handler.signals[0]
    assert signal.session_id == "test-bot:group:group:1"
    assert signal.message_log_id == result.message_log_id
    assert signal.event_type == "message-created"
    assert signal.sender_id == "user-1"
    assert signal.instance_id == "test-bot"
    assert signal.platform == "mock"
    assert signal.response_profile == "balanced"
    assert signal.self_id == "bot-1"
    assert signal.is_private is False
    assert signal.is_mentioned is False
    assert signal.is_reply_to_bot is False
    assert signal.already_handled is False
    assert signal.is_stopped is False
    assert not hasattr(signal, "message")

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "dispatched"
    assert row["is_read"] is False


@pytest.mark.asyncio
async def test_observe_route_does_not_suppress_agent_entry_fallback(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    agent_handler = RecordingAgentHandler(handled=True)
    agent_entry_dispatcher = AgentEntryDispatcher(handler=agent_handler, database=db)
    calls: list[str] = []

    table = RouteTable()
    observe_rule = RouteRule(
        id="debug-observer",
        priority=10_000,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target="debug-observer",
        match_mode=RouteMatchMode.OBSERVE,
    )
    fallback_rule = make_agent_entry_fallback_route_rule()
    table.register(observe_rule)
    table.register(fallback_rule)
    targets = RouteTargetRegistry()
    targets.register("debug-observer", lambda _context, rule: calls.append(rule.id))
    targets.register(AGENT_ENTRY_TARGET, agent_entry_dispatcher)
    ingress = MessageIngress(
        session_manager=SessionManager(session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        route_table=table,
        route_targets=targets,
        database=db,
    )

    result = await ingress.process_event(make_event("observe me", private=False), MockAdapter())
    await asyncio.sleep(0)

    assert result.matched_rules == [observe_rule, fallback_rule]
    assert calls == ["debug-observer"]
    assert len(agent_handler.signals) == 1


@pytest.mark.asyncio
async def test_agent_entry_fallback_marks_read_when_agent_entry_does_not_handle(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    agent_handler = RecordingAgentHandler(handled=False)
    agent_entry_dispatcher = AgentEntryDispatcher(handler=agent_handler, database=db)

    table = RouteTable()
    fallback_rule = make_agent_entry_fallback_route_rule()
    table.register(fallback_rule)
    targets = RouteTargetRegistry()
    targets.register(AGENT_ENTRY_TARGET, agent_entry_dispatcher)
    ingress = MessageIngress(
        session_manager=SessionManager(session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        route_table=table,
        route_targets=targets,
        database=db,
    )

    result = await ingress.process_event(make_event("unhandled group", private=False), MockAdapter())
    await asyncio.sleep(0)

    assert result.matched_rules == [fallback_rule]
    assert result.message_log_id is not None
    assert len(agent_handler.signals) == 1

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "dispatched"
    assert row["is_read"] is True
