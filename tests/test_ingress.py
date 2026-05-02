"""Tests for the message ingress routing flow."""

from __future__ import annotations

import asyncio
import logging
import time

import pytest

from shinbot.core.dispatch.ingress import (
    ROUTING_SKIP_EXPIRED_MESSAGE,
    ROUTING_SKIP_INTERCEPTOR_BLOCKED,
    ROUTING_SKIP_NO_ROUTE_MATCHED,
    ROUTING_SKIP_SESSION_MUTED,
    MessageIngress,
    RouteDispatchContext,
    RouteTargetRegistry,
    is_event_fresh,
)
from shinbot.core.dispatch.routing import RouteCondition, RouteRule, RouteTable
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
