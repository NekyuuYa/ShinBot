"""Tests for the message ingress routing flow."""

from __future__ import annotations

import asyncio
import json
import logging
import time

import pytest

from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    NOTICE_DISPATCHER_TARGET,
    AgentEntryDispatcher,
    AgentSignal,
    NoticeDispatcher,
    make_agent_entry_fallback_route_rule,
    make_notice_route_rule,
)
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import (
    ROUTING_SKIP_EXPIRED_MESSAGE,
    ROUTING_SKIP_INTERCEPTOR_BLOCKED,
    ROUTING_SKIP_NO_ROUTE_MATCHED,
    ROUTING_SKIP_SESSION_MUTED,
    ROUTING_SKIP_WAIT_FOR_INPUT,
    ROUTING_SKIP_WAIT_FOR_INPUT_SCOPE_MISMATCH,
    MessageIngress,
    RouteDispatchContext,
    RouteTargetRegistry,
    is_event_fresh,
)
from shinbot.core.dispatch.legacy_ingress_quiescence import (
    LegacyIngressDurableAdmissionRequired,
    LegacyIngressQuiescenceStatus,
)
from shinbot.core.dispatch.message_context import WaitingInputRegistry, WaitingInputScope
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule, RouteTable
from shinbot.core.message_routes import (
    KEYWORD_DISPATCHER_TARGET,
    TEXT_COMMAND_DISPATCHER_TARGET,
    KeywordDispatcher,
    TextCommandDispatcher,
    make_keyword_route_rule,
    make_text_command_route_rule,
)
from shinbot.core.message_routes.command import CommandDef, CommandRegistry
from shinbot.core.message_routes.keyword import KeywordDef, KeywordRegistry
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Member, User
from shinbot.schema.routing import MessageRoutingStatus


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
    def __init__(self) -> None:
        self.signals: list[AgentSignal] = []

    def __call__(self, signal: AgentSignal) -> None:
        self.signals.append(signal)


def make_event(
    content: str = "hello",
    *,
    event_type: str = "message-created",
    timestamp: int | None = None,
    private: bool = True,
    member_roles: list[str] | None = None,
) -> UnifiedEvent:
    user = User(id="user-1", name="Alice")
    return UnifiedEvent(
        type=event_type,
        self_id="bot-1",
        platform="mock",
        timestamp=timestamp,
        user=user,
        member=Member(user=user, roles=member_roles or []) if not private else None,
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
    permission_engine: PermissionEngine | None = None,
    max_message_age_seconds: int = 60,
) -> tuple[MessageIngress, DatabaseManager, MockAdapter]:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    ingress = MessageIngress(
        session_manager=session_manager or SessionManager(session_repo=db.sessions),
        permission_engine=permission_engine or PermissionEngine(),
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
@pytest.mark.parametrize("event_type", ["message-created", "guild-member-added"])
async def test_blocked_owner_is_filtered_before_custom_matcher(
    tmp_path,
    event_type: str,
) -> None:
    owner = "deactivating-plugin"
    table = RouteTable()
    targets = RouteTargetRegistry()
    matcher_calls: list[str] = []
    handler_calls: list[str] = []

    def custom_matcher(_event: UnifiedEvent, _message: Message) -> bool:
        matcher_calls.append(event_type)
        return True

    rule = RouteRule(
        id=f"route.blocked.{event_type}",
        priority=10,
        condition=RouteCondition(
            event_types=frozenset({event_type}),
            custom_matcher=custom_matcher,
        ),
        target="blocked-target",
        owner=owner,
    )
    table.register(rule)
    targets.register(
        "blocked-target",
        lambda _context, _rule: handler_calls.append(event_type),
        owner=owner,
    )
    await targets.cancel_owner_tasks(owner)
    ingress, _db, adapter = build_ingress(
        tmp_path,
        route_table=table,
        route_targets=targets,
    )

    result = await ingress.process_event(make_event(event_type=event_type), adapter)
    await asyncio.sleep(0)

    assert result.matched_rules == []
    assert matcher_calls == []
    assert handler_calls == []


@pytest.mark.asyncio
async def test_ingress_shutdown_cancels_and_awaits_route_target_tasks(tmp_path) -> None:
    table = RouteTable()
    add_message_route(table)
    targets = RouteTargetRegistry()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(_context: RouteDispatchContext, _rule: RouteRule) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    targets.register("recorder", handler)
    ingress, _db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    await ingress.process_event(make_event("hello"), adapter)
    await started.wait()

    assert ingress.pending_target_task_count == 1

    await ingress.shutdown()

    assert cancelled.is_set()
    assert ingress.pending_target_task_count == 0


@pytest.mark.asyncio
async def test_ingress_freeze_drains_a_route_task_admitted_before_freeze(tmp_path) -> None:
    """A target scheduled by pre-freeze ingress stays visible to local drain."""

    table = RouteTable()
    add_message_route(table)
    targets = RouteTargetRegistry()
    pre_route_started = asyncio.Event()
    release_pre_route = asyncio.Event()
    target_started = asyncio.Event()
    release_target = asyncio.Event()

    async def pre_route_hook(_context: RouteDispatchContext) -> None:
        pre_route_started.set()
        await release_pre_route.wait()

    async def handler(_context: RouteDispatchContext, _rule: RouteRule) -> None:
        target_started.set()
        await release_target.wait()

    targets.register("recorder", handler)
    ingress, _db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)
    ingress.add_pre_route_hook(pre_route_hook)
    process_task = asyncio.create_task(ingress.process_event(make_event("hello"), adapter))
    try:
        await asyncio.wait_for(pre_route_started.wait(), timeout=0.5)
        ticket = ingress.freeze_legacy_ingress_session(
            "test-bot:private:user-1",
            cutover_id="cutover-a",
        )

        release_pre_route.set()
        await asyncio.wait_for(target_started.wait(), timeout=0.5)
        await asyncio.wait_for(process_task, timeout=0.5)

        timed_out = await ingress.await_legacy_ingress_quiescent(
            ticket,
            timeout_seconds=0.0,
        )

        assert timed_out.status is LegacyIngressQuiescenceStatus.TIMED_OUT
        assert timed_out.remaining_task_names == ("route.target.route.recorder",)

        release_target.set()
        quiescent = await ingress.await_legacy_ingress_quiescent(
            ticket,
            timeout_seconds=0.5,
        )
        assert quiescent.status is LegacyIngressQuiescenceStatus.QUIESCENT
        assert ingress.thaw_legacy_ingress_session(ticket) is True
    finally:
        release_pre_route.set()
        release_target.set()
        await ingress.shutdown()


@pytest.mark.asyncio
async def test_ingress_freeze_refuses_new_legacy_admission_without_persistence(tmp_path) -> None:
    """A local freeze never creates legacy ownership while rejecting a new event."""

    ingress, db, adapter = build_ingress(tmp_path)
    ticket = ingress.freeze_legacy_ingress_session(
        "test-bot:private:user-1",
        cutover_id="cutover-a",
    )

    with pytest.raises(LegacyIngressDurableAdmissionRequired):
        await ingress.process_event(make_event("blocked"), adapter)

    with db.connect() as conn:
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_runtime_ownership),
                (SELECT COUNT(*) FROM message_logs)
            """
        ).fetchone()
    assert tuple(counts) == (0, 0)

    quiescent = await ingress.await_legacy_ingress_quiescent(
        ticket,
        timeout_seconds=0.0,
    )
    assert quiescent.status is LegacyIngressQuiescenceStatus.QUIESCENT
    assert ingress.thaw_legacy_ingress_session(ticket) is True


@pytest.mark.asyncio
async def test_route_target_registry_cancels_only_selected_owner_tasks(tmp_path) -> None:
    table = RouteTable()
    targets = RouteTargetRegistry()
    started = {owner: asyncio.Event() for owner in ("plugin-a", "plugin-b")}
    cancelled = {owner: asyncio.Event() for owner in ("plugin-a", "plugin-b")}

    def make_handler(owner: str):
        async def handler(_context: RouteDispatchContext, _rule: RouteRule) -> None:
            started[owner].set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled[owner].set()
                raise

        return handler

    for priority, owner in enumerate(("plugin-a", "plugin-b"), start=10):
        target = f"target.{owner}"
        table.register(
            RouteRule(
                id=f"route.{owner}",
                priority=priority,
                condition=RouteCondition(event_types=frozenset({"message-created"})),
                target=target,
                owner=owner,
            )
        )
        targets.register(target, make_handler(owner), owner=owner)

    ingress, _db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)
    await ingress.process_event(make_event("hello"), adapter)
    await asyncio.gather(*(event.wait() for event in started.values()))

    await targets.cancel_owner_tasks("plugin-a")

    assert cancelled["plugin-a"].is_set()
    assert not cancelled["plugin-b"].is_set()
    assert targets.pending_task_count_for_owner("plugin-a") == 0
    assert targets.pending_task_count_for_owner("plugin-b") == 1
    assert ingress.pending_target_task_count == 1

    await ingress.shutdown()
    assert cancelled["plugin-b"].is_set()


@pytest.mark.asyncio
async def test_ingress_shutdown_blocks_target_after_pre_route_hook(tmp_path) -> None:
    table = RouteTable()
    add_message_route(table)
    targets = RouteTargetRegistry()
    hook_started = asyncio.Event()
    release_hook = asyncio.Event()
    handler_called = asyncio.Event()
    handler_started = asyncio.Event()

    async def pre_route_hook(_context: RouteDispatchContext) -> None:
        hook_started.set()
        await release_hook.wait()

    async def run_handler() -> None:
        handler_started.set()

    def handler(_context: RouteDispatchContext, _rule: RouteRule):
        handler_called.set()
        return run_handler()

    targets.register("recorder", handler)
    ingress, _db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)
    ingress.add_pre_route_hook(pre_route_hook)

    process_task = asyncio.create_task(ingress.process_event(make_event("hello"), adapter))
    await hook_started.wait()
    await ingress.shutdown()
    release_hook.set()
    await process_task
    await asyncio.sleep(0)

    assert not handler_called.is_set()
    assert not handler_started.is_set()
    assert ingress.pending_target_task_count == 0


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
    scope = WaitingInputScope.from_routing_identity(
        legacy_session_id="test-bot:private:user-1",
    )
    lease = waiting_registry.acquire(scope, track_owner=False)
    future = lease.future
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
    assert waiting_registry.release(lease)


@pytest.mark.asyncio
async def test_scope_mismatched_waiter_fails_closed_without_waiting_for_session_lock(tmp_path) -> None:
    """A bot-scope mismatch cannot deadlock behind the legacy handler's lock."""

    waiting_registry = WaitingInputRegistry()
    session_id = "test-bot:private:user-1"
    waiter_scope = WaitingInputScope.from_routing_identity(
        legacy_session_id=session_id,
        bot_id="other-bot",
        bot_session_id="other-bot:private:user-1",
    )
    lease = waiting_registry.acquire(waiter_scope, track_owner=False)
    table = RouteTable()
    add_message_route(table)
    targets = RouteTargetRegistry()
    target_calls: list[str] = []
    targets.register("recorder", lambda _context, _rule: target_calls.append("called"))
    ingress, database, adapter = build_ingress(
        tmp_path,
        route_table=table,
        route_targets=targets,
        waiting_registry=waiting_registry,
    )

    async with ingress._session_manager.session_lock(session_id):
        result = await asyncio.wait_for(ingress.process_event(make_event("answer"), adapter), 0.5)

    assert result.message_log_id is not None
    assert result.matched_rules == []
    assert result.skipped_reason == ROUTING_SKIP_WAIT_FOR_INPUT_SCOPE_MISMATCH
    assert not lease.future.done()
    assert target_calls == []
    row = database.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_skip_reason"] == ROUTING_SKIP_WAIT_FOR_INPUT_SCOPE_MISMATCH
    assert waiting_registry.release(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["message-updated", "message-deleted"])
async def test_non_created_message_event_does_not_resolve_waiting_input(
    tmp_path,
    event_type: str,
) -> None:
    waiting_registry = WaitingInputRegistry()
    future = waiting_registry.register("test-bot:private:user-1")
    ingress, database, adapter = build_ingress(
        tmp_path,
        waiting_registry=waiting_registry,
    )

    result = await ingress.process_event(
        make_event("changed", event_type=event_type),
        adapter,
    )

    assert result.message_log_id is not None
    assert not future.done()
    assert waiting_registry.is_waiting("test-bot:private:user-1")
    row = database.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["raw_text"] == "changed"
    waiting_registry.cancel("test-bot:private:user-1")


@pytest.mark.asyncio
async def test_timed_out_waiter_does_not_consume_the_next_routed_message(tmp_path) -> None:
    """A cancelled interactive Future cannot leave a stale ingress fast path."""

    waiting_registry = WaitingInputRegistry()
    table = RouteTable()
    rule = add_message_route(table)
    targets = RouteTargetRegistry()
    timed_out = asyncio.Event()
    routed_messages: list[str] = []
    second_message_routed = asyncio.Event()

    async def handler(context: RouteDispatchContext, _rule: RouteRule) -> None:
        if context.require_message_context().text == "start":
            with pytest.raises(TimeoutError):
                await context.require_message_context().wait_for_input(timeout=0.001)
            timed_out.set()
            return
        routed_messages.append(context.require_message_context().text)
        second_message_routed.set()

    targets.register("recorder", handler)
    ingress, _database, adapter = build_ingress(
        tmp_path,
        route_table=table,
        route_targets=targets,
        waiting_registry=waiting_registry,
    )

    await ingress.process_event(make_event("start"), adapter)
    await asyncio.wait_for(timed_out.wait(), timeout=1.0)
    result = await ingress.process_event(make_event("next"), adapter)
    await asyncio.wait_for(second_message_routed.wait(), timeout=1.0)

    assert result.matched_rules == [rule]
    assert result.skipped_reason is None
    assert routed_messages == ["next"]


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
    assert "route.target.missing" in caplog.text
    assert "rule_id=route.missing" in caplog.text
    assert "target=missing" in caplog.text

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


def test_is_event_fresh_accepts_platform_timestamp_seconds() -> None:
    now = 1_779_210_000.0
    fresh = make_event(timestamp=1_779_209_995)
    stale = make_event(timestamp=1_779_209_900)

    assert is_event_fresh(fresh, now=now, max_age_seconds=60)
    assert not is_event_fresh(stale, now=now, max_age_seconds=60)


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
async def test_text_command_dispatcher_allows_session_admin_role_without_binding(tmp_path) -> None:
    command_registry = CommandRegistry()
    command_calls: list[str] = []

    async def secret(_ctx, _args) -> None:
        command_calls.append("secret")

    command_registry.register(CommandDef(name="secret", handler=secret, permission="cmd.secret"))
    command_dispatcher = TextCommandDispatcher(command_registry)

    table = RouteTable()
    command_rule = make_text_command_route_rule(command_dispatcher)
    table.register(command_rule)
    targets = RouteTargetRegistry()
    targets.register(TEXT_COMMAND_DISPATCHER_TARGET, command_dispatcher)
    permission_engine = PermissionEngine()
    ingress, _db, adapter = build_ingress(
        tmp_path,
        route_table=table,
        route_targets=targets,
        permission_engine=permission_engine,
    )

    result = await ingress.process_event(
        make_event("/secret", private=False, member_roles=["admin"]),
        adapter,
    )
    await asyncio.sleep(0)

    assert result.matched_rules == [command_rule]
    assert command_calls == ["secret"]
    assert adapter.sent == []
    assert permission_engine.groups_for_key("test-bot:user-1") == ()
    assert permission_engine.groups_for_key("test-bot:group:1.user-1") == ()


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
    assert result.message_log_id is not None
    assert result.skipped_reason is None
    assert notice_calls == ["guild-member-added"]

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["session_id"] == "test-bot:private:user-1"
    assert row["role"] == "system"
    assert row["raw_text"] == "[notice:guild-member-added]"
    assert row["routing_status"] == "dispatched"
    assert row["routing_skip_reason"] is None
    payload = json.loads(row["content_json"])
    assert payload[0]["type"] == "sb:notice"
    assert payload[0]["attrs"]["event_type"] == "guild-member-added"


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
    assert result.message_log_id is not None
    assert result.skipped_reason == ROUTING_SKIP_NO_ROUTE_MATCHED
    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "skipped"
    assert row["routing_skip_reason"] == ROUTING_SKIP_NO_ROUTE_MATCHED


@pytest.mark.asyncio
async def test_notice_without_route_is_persisted_and_skipped(tmp_path) -> None:
    ingress, db, adapter = build_ingress(tmp_path)

    result = await ingress.process_event(make_event(event_type="guild-member-added"), adapter)

    assert result.matched_rules == []
    assert result.message_log_id is not None
    assert result.skipped_reason == ROUTING_SKIP_NO_ROUTE_MATCHED
    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["role"] == "system"
    assert row["routing_status"] == "skipped"
    assert row["routing_skip_reason"] == ROUTING_SKIP_NO_ROUTE_MATCHED


@pytest.mark.asyncio
async def test_expired_notice_is_persisted_and_skipped_before_route_dispatch(tmp_path) -> None:
    event_bus = EventBus()
    notice_calls: list[str] = []
    event_bus.on("guild-member-added", lambda event: notice_calls.append(event.type))
    notice_dispatcher = NoticeDispatcher(event_bus)

    table = RouteTable()
    table.register(make_notice_route_rule(notice_dispatcher))
    targets = RouteTargetRegistry()
    targets.register(NOTICE_DISPATCHER_TARGET, notice_dispatcher)
    ingress, db, adapter = build_ingress(tmp_path, route_table=table, route_targets=targets)

    event = make_event(
        event_type="guild-member-added",
        timestamp=int((time.time() - 120) * 1000),
    )
    result = await ingress.process_event(event, adapter)

    assert result.matched_rules == []
    assert result.message_log_id is not None
    assert result.skipped_reason == ROUTING_SKIP_EXPIRED_MESSAGE
    assert notice_calls == []

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "skipped"
    assert row["routing_skip_reason"] == ROUTING_SKIP_EXPIRED_MESSAGE


@pytest.mark.asyncio
async def test_agent_entry_fallback_notifies_agent_with_minimal_signal(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    agent_handler = RecordingAgentHandler()
    agent_entry_dispatcher = AgentEntryDispatcher(handler=agent_handler)

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
    assert result.trace_id.startswith("ingress:test-bot:")
    assert len(agent_handler.signals) == 1

    signal = agent_handler.signals[0]
    assert signal.session_id == "test-bot:group:group:1"
    assert signal.message is not None
    assert signal.message.message_log_id == result.message_log_id
    assert signal.meta["event_type"] == "message-created"
    assert signal.meta["trace_id"] == result.trace_id
    assert signal.message.sender_id == "user-1"
    assert signal.message.instance_id == "test-bot"
    assert signal.message.platform == "mock"
    assert signal.message.self_id == "bot-1"
    assert signal.message.is_private is False
    assert signal.message.is_mentioned is False
    assert signal.message.is_mention_to_other is False
    assert signal.message.is_reply_to_bot is False
    assert signal.message.is_poke_to_bot is False
    assert signal.message.is_poke_to_other is False
    assert signal.message.already_handled is False
    assert signal.message.is_stopped is False

    row = db.message_logs.get(result.message_log_id)
    assert row is not None
    assert row["routing_status"] == "dispatched"
    assert row["is_read"] is False


@pytest.mark.asyncio
async def test_observe_route_does_not_suppress_agent_entry_fallback(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    agent_handler = RecordingAgentHandler()
    agent_entry_dispatcher = AgentEntryDispatcher(handler=agent_handler)
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
async def test_agent_entry_fallback_notifies_agent_without_marking_read(
    tmp_path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    agent_handler = RecordingAgentHandler()
    agent_entry_dispatcher = AgentEntryDispatcher(handler=agent_handler)

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
    assert row["routing_status"] == MessageRoutingStatus.DISPATCHED.value
    assert row["is_read"] is False
