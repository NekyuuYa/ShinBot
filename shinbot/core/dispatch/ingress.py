"""Ingress flow for normalized platform events.

This module is the new entry point shape for message routing. It parses
message payloads, persists inbound messages, evaluates the route table, and
schedules matched route targets. It intentionally does not replace
MessagePipeline yet; the application wiring moves over after the dispatchers
are migrated.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from shinbot.core.dispatch.message_context import (
    Interceptor,
    MessageContext,
    WaitingInputRegistry,
)
from shinbot.core.dispatch.routing import RouteMatchContext, RouteRule, RouteTable
from shinbot.core.message_analysis import is_self_mentioned
from shinbot.core.platform.adapter_manager import BaseAdapter
from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager, build_session_id
from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.elements import Message
from shinbot.schema.events import UnifiedEvent
from shinbot.utils.resource_ingress import summarize_message_modalities

if TYPE_CHECKING:
    from shinbot.agent.context import ContextManager
    from shinbot.agent.media import MediaInspectionRunner, MediaService
    from shinbot.persistence.engine import DatabaseManager

logger = logging.getLogger(__name__)

MAX_MESSAGE_AGE_SECONDS = 60

ROUTING_SKIP_EXPIRED_MESSAGE = "expired_message"
ROUTING_SKIP_NO_ROUTE_MATCHED = "no_route_matched"
ROUTING_SKIP_SESSION_MUTED = "session_muted"
ROUTING_SKIP_INTERCEPTOR_BLOCKED = "interceptor_blocked"
ROUTING_SKIP_WAIT_FOR_INPUT = "wait_for_input"


@dataclass(slots=True)
class RouteDispatchContext:
    """Context passed to route target dispatchers."""

    event: UnifiedEvent
    adapter: BaseAdapter
    message: Message
    message_context: MessageContext | None = None
    message_log_id: int | None = None

    def require_message_context(self) -> MessageContext:
        if self.message_context is None:
            raise RuntimeError("Route target requires a message context")
        return self.message_context


@dataclass(slots=True)
class IngressResult:
    """Observable result for tests and future application-level tracing."""

    dispatch_context: RouteDispatchContext | None
    matched_rules: list[RouteRule]
    message_log_id: int | None = None
    skipped_reason: str | None = None


RouteTargetHandler = Callable[
    [RouteDispatchContext, RouteRule],
    Awaitable[None] | None,
]


class RouteTargetRegistry:
    """Registry of concrete dispatcher targets addressed by RouteRule.target."""

    def __init__(self) -> None:
        self._handlers: dict[str, RouteTargetHandler] = {}
        self._owners: dict[str, str | None] = {}

    def register(
        self,
        target: str,
        handler: RouteTargetHandler,
        *,
        owner: str | None = None,
    ) -> None:
        if not target:
            raise ValueError("route target must not be empty")
        if target in self._handlers:
            raise ValueError(f"route target already registered: {target}")
        self._handlers[target] = handler
        self._owners[target] = owner

    def unregister(self, target: str) -> RouteTargetHandler | None:
        self._owners.pop(target, None)
        return self._handlers.pop(target, None)

    def unregister_by_owner(self, owner: str) -> int:
        targets = [target for target, target_owner in self._owners.items() if target_owner == owner]
        for target in targets:
            self.unregister(target)
        return len(targets)

    def get(self, target: str) -> RouteTargetHandler | None:
        return self._handlers.get(target)


class MessageIngress:
    """Normalize, persist, route, and schedule incoming events."""

    def __init__(
        self,
        *,
        session_manager: SessionManager,
        permission_engine: PermissionEngine,
        route_table: RouteTable,
        route_targets: RouteTargetRegistry | None = None,
        audit_logger: AuditLogger | None = None,
        database: DatabaseManager | None = None,
        context_manager: ContextManager | None = None,
        media_service: MediaService | None = None,
        media_inspection_runner: MediaInspectionRunner | None = None,
        waiting_registry: WaitingInputRegistry | None = None,
        max_message_age_seconds: int = MAX_MESSAGE_AGE_SECONDS,
    ) -> None:
        self._session_manager = session_manager
        self._permission_engine = permission_engine
        self._route_table = route_table
        self._route_targets = route_targets or RouteTargetRegistry()
        self._audit_logger = audit_logger
        self._database = database
        self._context_manager = context_manager
        self._media_service = media_service
        self._media_inspection_runner = media_inspection_runner
        self._waiting_registry = waiting_registry or WaitingInputRegistry()
        self._max_message_age_seconds = max_message_age_seconds
        self._interceptors: list[tuple[int, Interceptor]] = []

    def add_interceptor(self, interceptor: Interceptor, priority: int = 100) -> None:
        self._interceptors.append((priority, interceptor))
        self._interceptors.sort(key=lambda x: x[0])

    async def process_event(self, event: UnifiedEvent, adapter: BaseAdapter) -> IngressResult:
        """Process one normalized event.

        Message events are persisted before routing. Notice events can already
        be routed through the same route table, but notice persistence is left
        for the dedicated notice dispatcher migration.
        """
        if event.is_notice_event:
            return self._process_notice_event(event, adapter)
        return await self._process_message_event(event, adapter)

    async def _process_message_event(
        self,
        event: UnifiedEvent,
        adapter: BaseAdapter,
    ) -> IngressResult:
        message = Message.from_xml(event.message_content) if event.message_content else Message()
        session_id = build_session_id(adapter.instance_id, event)

        # Preserve the current deadlock-avoidance behavior: a suspended handler
        # may already hold the session lock while waiting for this reply.
        if self._waiting_registry.is_waiting(session_id):
            if self._waiting_registry.resolve(session_id, message.get_text(self_id=event.self_id)):
                return IngressResult(
                    dispatch_context=None,
                    matched_rules=[],
                    skipped_reason=ROUTING_SKIP_WAIT_FOR_INPUT,
                )

        async with self._session_manager.session_lock(session_id):
            return await self._process_message_event_locked(event, adapter, message)

    async def _process_message_event_locked(
        self,
        event: UnifiedEvent,
        adapter: BaseAdapter,
        message: Message,
    ) -> IngressResult:
        session = self._session_manager.get_or_create(adapter.instance_id, event)
        session.touch()

        permissions = self._permission_engine.resolve(
            instance_id=adapter.instance_id,
            session_id=session.id,
            user_id=event.sender_id or "",
            session_base_group=session.permission_group,
        )
        message_context = MessageContext(
            event=event,
            message=message,
            session=session,
            adapter=adapter,
            permissions=permissions,
            waiting_registry=self._waiting_registry,
            database=self._database,
            context_manager=self._context_manager,
        )

        observed_at = time.time()
        message_log_id = self._persist_incoming_message(
            event=event,
            message=message,
            session_id=session.id,
            observed_at=observed_at,
        )
        message_context._msg_log_id = message_log_id

        dispatch_context = RouteDispatchContext(
            event=event,
            adapter=adapter,
            message=message,
            message_context=message_context,
            message_log_id=message_log_id,
        )

        if not is_event_fresh(
            event,
            max_age_seconds=self._max_message_age_seconds,
        ):
            self._mark_skipped(message_log_id, ROUTING_SKIP_EXPIRED_MESSAGE)
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_EXPIRED_MESSAGE,
            )

        if session.is_muted:
            self._mark_skipped(message_log_id, ROUTING_SKIP_SESSION_MUTED)
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_SESSION_MUTED,
            )

        blocked_reason = await self._run_interceptors(message_context)
        if blocked_reason is not None:
            self._mark_skipped(message_log_id, blocked_reason)
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=blocked_reason,
            )

        self._ingest_media(
            adapter=adapter,
            event=event,
            message=message,
            session_id=session.id,
            sender_id=event.sender_id or "",
            message_log_id=message_log_id,
            observed_at=observed_at,
        )
        self._track_message_record(
            event=event,
            message=message,
            session_id=session.id,
            observed_at=observed_at,
            message_log_id=message_log_id,
        )
        self._log_audit(event, message_context)

        matched_rules = self._route_table.match(
            event,
            message,
            RouteMatchContext(
                adapter=adapter,
                session=session,
                message_context=message_context,
            ),
        )
        if not matched_rules:
            self._mark_skipped(message_log_id, ROUTING_SKIP_NO_ROUTE_MATCHED)
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_NO_ROUTE_MATCHED,
            )

        self._mark_dispatched(message_log_id)
        self._schedule_targets(dispatch_context, matched_rules)
        self._session_manager.update(session)
        return IngressResult(
            dispatch_context=dispatch_context,
            matched_rules=matched_rules,
            message_log_id=message_log_id,
        )

    def _process_notice_event(self, event: UnifiedEvent, adapter: BaseAdapter) -> IngressResult:
        message = Message()
        dispatch_context = RouteDispatchContext(
            event=event,
            adapter=adapter,
            message=message,
        )
        matched_rules = self._route_table.match(event, message)
        self._schedule_targets(dispatch_context, matched_rules)
        return IngressResult(
            dispatch_context=dispatch_context,
            matched_rules=matched_rules,
            skipped_reason=ROUTING_SKIP_NO_ROUTE_MATCHED if not matched_rules else None,
        )

    def _persist_incoming_message(
        self,
        *,
        event: UnifiedEvent,
        message: Message,
        session_id: str,
        observed_at: float,
    ) -> int | None:
        if self._database is None:
            return None
        try:
            content_json = json.dumps(
                [el.model_dump(mode="json") for el in message.elements],
                ensure_ascii=False,
            )
            record = MessageLogRecord(
                session_id=session_id,
                platform_msg_id=event.message.id if event.message is not None else "",
                sender_id=event.sender_id or "",
                sender_name=event.sender_name or "",
                content_json=content_json,
                raw_text=message.get_text(self_id=event.self_id),
                role="user",
                is_read=False,
                is_mentioned=is_self_mentioned(message, event.self_id),
                created_at=observed_at * 1000,
            )
            message_log_id = self._database.message_logs.insert(record)
            record.id = message_log_id
            return message_log_id
        except Exception:
            logger.exception("Failed to persist incoming message to message_logs")
            return None

    async def _run_interceptors(self, message_context: MessageContext) -> str | None:
        for _priority, interceptor in self._interceptors:
            try:
                allow = await interceptor(message_context)
                if not allow:
                    logger.debug("Interceptor blocked event: %s", interceptor.__name__)
                    return ROUTING_SKIP_INTERCEPTOR_BLOCKED
            except Exception:
                logger.exception("Interceptor error: %s", interceptor.__name__)
                return ROUTING_SKIP_INTERCEPTOR_BLOCKED
        return None

    def _ingest_media(
        self,
        *,
        adapter: BaseAdapter,
        event: UnifiedEvent,
        message: Message,
        session_id: str,
        sender_id: str,
        message_log_id: int | None,
        observed_at: float,
    ) -> None:
        if self._media_service is None or message_log_id is None:
            return
        try:
            ingested_items = self._media_service.ingest_message_media(
                session_id=session_id,
                sender_id=sender_id,
                platform_msg_id=event.message.id if event.message is not None else "",
                elements=message.elements,
                message_log_id=message_log_id,
                seen_at=observed_at,
            )
            if self._media_inspection_runner is not None and any(
                item.should_request_inspection for item in ingested_items
            ):
                self._media_inspection_runner.schedule_items(
                    instance_id=adapter.instance_id,
                    session_id=session_id,
                    items=ingested_items,
                )
        except Exception:
            logger.exception("Failed to ingest media fingerprints for session %s", session_id)

    def _track_message_record(
        self,
        *,
        event: UnifiedEvent,
        message: Message,
        session_id: str,
        observed_at: float,
        message_log_id: int | None,
    ) -> None:
        if self._context_manager is None or message_log_id is None:
            return
        record = MessageLogRecord(
            id=message_log_id,
            session_id=session_id,
            platform_msg_id=event.message.id if event.message is not None else "",
            sender_id=event.sender_id or "",
            sender_name=event.sender_name or "",
            content_json=json.dumps(
                [el.model_dump(mode="json") for el in message.elements],
                ensure_ascii=False,
            ),
            raw_text=message.get_text(self_id=event.self_id),
            role="user",
            is_read=False,
            is_mentioned=is_self_mentioned(message, event.self_id),
            created_at=observed_at * 1000,
        )
        self._context_manager.track_message_record(record, platform=event.platform)

    def _log_audit(self, event: UnifiedEvent, message_context: MessageContext) -> None:
        if self._audit_logger is None:
            return
        self._audit_logger.log_message(
            event_type=event.type,
            plugin_id="",
            user_id=event.sender_id or "",
            session_id=message_context.session.id,
            instance_id=message_context.adapter.instance_id,
            metadata={
                "platform": event.platform,
                "modality": summarize_message_modalities(message_context.elements),
                "message_id": event.message.id if event.message is not None else "",
            },
        )

    def _mark_dispatched(self, message_log_id: int | None) -> None:
        if self._database is not None and message_log_id is not None:
            self._database.message_logs.mark_routing_dispatched(message_log_id)

    def _mark_skipped(self, message_log_id: int | None, reason: str) -> None:
        if self._database is not None and message_log_id is not None:
            self._database.message_logs.mark_routing_skipped(message_log_id, reason=reason)

    def _schedule_targets(
        self,
        dispatch_context: RouteDispatchContext,
        matched_rules: list[RouteRule],
    ) -> None:
        for rule in matched_rules:
            handler = self._route_targets.get(rule.target)
            if handler is None:
                logger.error("route_target_missing: rule_id=%s target=%s", rule.id, rule.target)
                continue

            try:
                result = handler(dispatch_context, rule)
            except Exception:
                logger.exception("route_target_error: rule_id=%s target=%s", rule.id, rule.target)
                continue

            if inspect.isawaitable(result):
                task = asyncio.create_task(result)
                task.add_done_callback(
                    lambda done, matched_rule=rule: _log_route_target_task_result(
                        done,
                        matched_rule,
                    )
                )


def is_event_fresh(
    event: UnifiedEvent,
    *,
    now: float | None = None,
    max_age_seconds: int = MAX_MESSAGE_AGE_SECONDS,
) -> bool:
    """Return whether an event should be processed by realtime routing."""
    if event.timestamp is None:
        return True
    current_time = time.time() if now is None else now
    age = current_time - event.timestamp / 1000
    return age < max_age_seconds


def _log_route_target_task_result(done: asyncio.Task[Any], rule: RouteRule) -> None:
    try:
        done.result()
    except asyncio.CancelledError:
        logger.debug("route_target_cancelled: rule_id=%s target=%s", rule.id, rule.target)
    except Exception:
        logger.exception("route_target_error: rule_id=%s target=%s", rule.id, rule.target)
