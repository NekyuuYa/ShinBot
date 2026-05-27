"""Ingress flow for normalized platform events.

This module is the new entry point shape for message routing. It parses
message payloads, persists inbound messages, evaluates the route table, and
schedules matched route targets. It intentionally does not replace
the target modules' own processing state; each route target owns its
post-dispatch lifecycle.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from shinbot.core.application.bot_routing import (
    BotRuntimeRouter,
    bot_route_rule_enabled_for_context,
    bot_session_id_for_selection,
    permission_scope_for_event,
)
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
from shinbot.schema.routing import MessageRoutingSkipReason
from shinbot.utils.logger import format_log_event, get_logger
from shinbot.utils.resource_ingress import summarize_message_modalities

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__, source="dispatch", color="cyan")

MAX_MESSAGE_AGE_SECONDS = 60

ROUTING_SKIP_EXPIRED_MESSAGE = MessageRoutingSkipReason.EXPIRED_MESSAGE.value
ROUTING_SKIP_NO_ROUTE_MATCHED = MessageRoutingSkipReason.NO_ROUTE_MATCHED.value
ROUTING_SKIP_SESSION_MUTED = MessageRoutingSkipReason.SESSION_MUTED.value
ROUTING_SKIP_INTERCEPTOR_BLOCKED = MessageRoutingSkipReason.INTERCEPTOR_BLOCKED.value
ROUTING_SKIP_WAIT_FOR_INPUT = MessageRoutingSkipReason.WAIT_FOR_INPUT.value


@dataclass(slots=True)
class RouteDispatchContext:
    """Context passed to route target dispatchers."""

    event: UnifiedEvent
    adapter: BaseAdapter
    message: Message
    message_context: MessageContext | None = None
    message_log_id: int | None = None
    trace_id: str = ""

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
    trace_id: str = ""


RouteTargetHandler = Callable[
    [RouteDispatchContext, RouteRule],
    Awaitable[None] | None,
]
PreRouteHook = Callable[[RouteDispatchContext], Awaitable[None] | None]


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
        waiting_registry: WaitingInputRegistry | None = None,
        max_message_age_seconds: int = MAX_MESSAGE_AGE_SECONDS,
        bot_router: BotRuntimeRouter | None = None,
    ) -> None:
        self._session_manager = session_manager
        self._permission_engine = permission_engine
        self._route_table = route_table
        self._route_targets = route_targets or RouteTargetRegistry()
        self._audit_logger = audit_logger
        self._database = database
        self._waiting_registry = waiting_registry or WaitingInputRegistry()
        self._max_message_age_seconds = max_message_age_seconds
        self._bot_router = bot_router
        self._interceptors: list[tuple[int, Interceptor]] = []
        self._pre_route_hooks: list[PreRouteHook] = []

    def add_interceptor(self, interceptor: Interceptor, priority: int = 100) -> None:
        self._interceptors.append((priority, interceptor))
        self._interceptors.sort(key=lambda x: x[0])

    def add_pre_route_hook(self, hook: PreRouteHook) -> None:
        """Register a lightweight hook that runs after gates and before route matching."""
        if hook not in self._pre_route_hooks:
            self._pre_route_hooks.append(hook)

    def set_bot_router(self, bot_router: BotRuntimeRouter | None) -> None:
        """Install or clear bot service-unit routing policy."""

        self._bot_router = bot_router

    async def process_event(self, event: UnifiedEvent, adapter: BaseAdapter) -> IngressResult:
        """Process one normalized event.

        Message and notice events are persisted before routing. Real-time
        routing is driven by the event signal, not by polling message_logs.
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
        trace_id = build_ingress_trace_id(adapter, event)

        # Preserve the current deadlock-avoidance behavior: a suspended handler
        # may already hold the session lock while waiting for this reply.
        if self._waiting_registry.is_waiting(session_id):
            observed_at = time.time()
            message_log_id = self._persist_incoming_message(
                event=event,
                message=message,
                session_id=session_id,
                observed_at=observed_at,
            )
            self._log_message_ingress(
                event=event,
                adapter=adapter,
                session_id=session_id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                bot_selection=None,
                message=message,
            )
            if not is_event_fresh(
                event,
                max_age_seconds=self._max_message_age_seconds,
            ):
                self._mark_skipped(message_log_id, ROUTING_SKIP_EXPIRED_MESSAGE)
                self._log_routing_result(
                    event=event,
                    adapter=adapter,
                    session_id=session_id,
                    message_log_id=message_log_id,
                    trace_id=trace_id,
                    skipped_reason=ROUTING_SKIP_EXPIRED_MESSAGE,
                )
                return IngressResult(
                    dispatch_context=None,
                    matched_rules=[],
                    message_log_id=message_log_id,
                    skipped_reason=ROUTING_SKIP_EXPIRED_MESSAGE,
                    trace_id=trace_id,
                )
            if self._waiting_registry.resolve(session_id, message.get_text(self_id=event.self_id)):
                self._mark_skipped(message_log_id, ROUTING_SKIP_WAIT_FOR_INPUT)
                self._log_routing_result(
                    event=event,
                    adapter=adapter,
                    session_id=session_id,
                    message_log_id=message_log_id,
                    trace_id=trace_id,
                    skipped_reason=ROUTING_SKIP_WAIT_FOR_INPUT,
                )
                return IngressResult(
                    dispatch_context=None,
                    matched_rules=[],
                    message_log_id=message_log_id,
                    skipped_reason=ROUTING_SKIP_WAIT_FOR_INPUT,
                    trace_id=trace_id,
                )

        async with self._session_manager.session_lock(session_id):
            return await self._process_message_event_locked(
                event,
                adapter,
                message,
                trace_id=trace_id,
            )

    async def _process_message_event_locked(
        self,
        event: UnifiedEvent,
        adapter: BaseAdapter,
        message: Message,
        trace_id: str,
    ) -> IngressResult:
        session = self._session_manager.get_or_create(adapter.instance_id, event)
        session.touch()
        bot_selection = self._resolve_bot_selection(event, adapter)
        permission_scope = permission_scope_for_event(
            bot_selection,
            event=event,
            fallback_identity_id=adapter.instance_id,
            fallback_session_id=session.id,
        )

        permissions = self._permission_engine.resolve(
            instance_id=permission_scope.identity_id,
            session_id=permission_scope.session_id,
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
        )
        if bot_selection is not None:
            message_context.bot_service_config = bot_selection.bot
            message_context.bot_binding_config = bot_selection.binding
            message_context.bot_session_id = bot_session_id_for_selection(
                bot_selection,
                event=event,
            )

        observed_at = time.time()
        message_log_id = self._persist_incoming_message(
            event=event,
            message=message,
            session_id=session.id,
            observed_at=observed_at,
        )
        message_context._msg_log_id = message_log_id
        self._log_message_ingress(
            event=event,
            adapter=adapter,
            session_id=session.id,
            message_log_id=message_log_id,
            trace_id=trace_id,
            bot_selection=bot_selection,
            message=message,
        )

        dispatch_context = RouteDispatchContext(
            event=event,
            adapter=adapter,
            message=message,
            message_context=message_context,
            message_log_id=message_log_id,
            trace_id=trace_id,
        )

        if not is_event_fresh(
            event,
            max_age_seconds=self._max_message_age_seconds,
        ):
            self._mark_skipped(message_log_id, ROUTING_SKIP_EXPIRED_MESSAGE)
            self._log_routing_result(
                event=event,
                adapter=adapter,
                session_id=session.id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                bot_selection=bot_selection,
                skipped_reason=ROUTING_SKIP_EXPIRED_MESSAGE,
            )
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_EXPIRED_MESSAGE,
                trace_id=trace_id,
            )

        if session.is_muted:
            self._mark_skipped(message_log_id, ROUTING_SKIP_SESSION_MUTED)
            self._log_routing_result(
                event=event,
                adapter=adapter,
                session_id=session.id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                bot_selection=bot_selection,
                skipped_reason=ROUTING_SKIP_SESSION_MUTED,
            )
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_SESSION_MUTED,
                trace_id=trace_id,
            )

        if self._bot_router is not None and bot_selection is None:
            self._mark_skipped(message_log_id, ROUTING_SKIP_NO_ROUTE_MATCHED)
            self._log_routing_result(
                event=event,
                adapter=adapter,
                session_id=session.id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                skipped_reason=ROUTING_SKIP_NO_ROUTE_MATCHED,
            )
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_NO_ROUTE_MATCHED,
                trace_id=trace_id,
            )

        blocked_reason = await self._run_interceptors(message_context)
        if blocked_reason is not None:
            self._mark_skipped(message_log_id, blocked_reason)
            self._log_routing_result(
                event=event,
                adapter=adapter,
                session_id=session.id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                bot_selection=bot_selection,
                skipped_reason=blocked_reason,
            )
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=blocked_reason,
                trace_id=trace_id,
            )

        await self._run_pre_route_hooks(dispatch_context)
        self._log_audit(event, message_context)

        matched_rules = self._route_table.match(
            event,
            message,
            RouteMatchContext(
                adapter=adapter,
                session=session,
                message_context=message_context,
                rule_filter=lambda rule: bot_route_rule_enabled_for_context(rule, message_context),
            ),
        )
        if not matched_rules:
            self._mark_skipped(message_log_id, ROUTING_SKIP_NO_ROUTE_MATCHED)
            self._log_routing_result(
                event=event,
                adapter=adapter,
                session_id=session.id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                bot_selection=bot_selection,
                skipped_reason=ROUTING_SKIP_NO_ROUTE_MATCHED,
            )
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_NO_ROUTE_MATCHED,
                trace_id=trace_id,
            )

        self._mark_dispatched(message_log_id)
        self._log_routing_result(
            event=event,
            adapter=adapter,
            session_id=session.id,
            message_log_id=message_log_id,
            trace_id=trace_id,
            bot_selection=bot_selection,
            matched_rules=matched_rules,
        )
        self._schedule_targets(dispatch_context, matched_rules)
        self._session_manager.update(session)
        return IngressResult(
            dispatch_context=dispatch_context,
            matched_rules=matched_rules,
            message_log_id=message_log_id,
            trace_id=trace_id,
        )

    def _resolve_bot_selection(self, event: UnifiedEvent, adapter: BaseAdapter) -> Any | None:
        if self._bot_router is None:
            return None
        return self._bot_router.resolve(adapter_instance_id=adapter.instance_id, event=event)

    def _log_message_ingress(
        self,
        *,
        event: UnifiedEvent,
        adapter: BaseAdapter,
        session_id: str,
        message_log_id: int | None,
        trace_id: str,
        bot_selection: Any | None,
        message: Message,
    ) -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug(
            format_log_event(
                "message.ingress",
                event_type=event.type,
                platform=event.platform,
                instance_id=adapter.instance_id,
                session_id=session_id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                platform_msg_id=event.message.id if event.message is not None else "",
                sender_id=event.sender_id or "",
                bot_id=getattr(getattr(bot_selection, "bot", None), "id", ""),
                binding_id=getattr(getattr(bot_selection, "binding", None), "id", ""),
                modality=summarize_message_modalities(message.elements),
            )
        )

    def _log_routing_result(
        self,
        *,
        event: UnifiedEvent,
        adapter: BaseAdapter,
        session_id: str,
        message_log_id: int | None,
        trace_id: str,
        bot_selection: Any | None = None,
        matched_rules: list[RouteRule] | None = None,
        skipped_reason: str | None = None,
    ) -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug(
            format_log_event(
                "message.routing",
                event_type=event.type,
                instance_id=adapter.instance_id,
                session_id=session_id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                bot_id=getattr(getattr(bot_selection, "bot", None), "id", ""),
                binding_id=getattr(getattr(bot_selection, "binding", None), "id", ""),
                status="skipped" if skipped_reason else "dispatched",
                skipped_reason=skipped_reason,
                rules=[rule.id for rule in matched_rules or []],
                targets=[rule.target for rule in matched_rules or []],
            )
        )

    def _process_notice_event(self, event: UnifiedEvent, adapter: BaseAdapter) -> IngressResult:
        message = Message()
        observed_at = time.time()
        session_id = build_session_id(adapter.instance_id, event)
        trace_id = build_ingress_trace_id(adapter, event)
        message_log_id = self._persist_notice_event(
            event=event,
            session_id=session_id,
            observed_at=observed_at,
        )
        dispatch_context = RouteDispatchContext(
            event=event,
            adapter=adapter,
            message=message,
            message_log_id=message_log_id,
            trace_id=trace_id,
        )
        self._log_message_ingress(
            event=event,
            adapter=adapter,
            session_id=session_id,
            message_log_id=message_log_id,
            trace_id=trace_id,
            bot_selection=None,
            message=message,
        )

        if not is_event_fresh(
            event,
            max_age_seconds=self._max_message_age_seconds,
        ):
            self._mark_skipped(message_log_id, ROUTING_SKIP_EXPIRED_MESSAGE)
            self._log_routing_result(
                event=event,
                adapter=adapter,
                session_id=session_id,
                message_log_id=message_log_id,
                trace_id=trace_id,
                skipped_reason=ROUTING_SKIP_EXPIRED_MESSAGE,
            )
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_EXPIRED_MESSAGE,
                trace_id=trace_id,
            )

        matched_rules = self._route_table.match(event, message)
        if matched_rules:
            self._mark_dispatched(message_log_id)
        else:
            self._mark_skipped(message_log_id, ROUTING_SKIP_NO_ROUTE_MATCHED)

        self._log_routing_result(
            event=event,
            adapter=adapter,
            session_id=session_id,
            message_log_id=message_log_id,
            trace_id=trace_id,
            matched_rules=matched_rules,
            skipped_reason=ROUTING_SKIP_NO_ROUTE_MATCHED if not matched_rules else None,
        )
        self._schedule_targets(dispatch_context, matched_rules)
        skipped_reason = ROUTING_SKIP_NO_ROUTE_MATCHED if not matched_rules else None
        return IngressResult(
            dispatch_context=dispatch_context,
            matched_rules=matched_rules,
            message_log_id=message_log_id,
            skipped_reason=skipped_reason,
            trace_id=trace_id,
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

    def _persist_notice_event(
        self,
        *,
        event: UnifiedEvent,
        session_id: str,
        observed_at: float,
    ) -> int | None:
        if self._database is None:
            return None
        try:
            payload = event.model_dump(mode="json")
            content_json = json.dumps(
                [
                    {
                        "type": "sb:notice",
                        "attrs": {
                            "event_type": event.type,
                            "payload": payload,
                        },
                        "children": [],
                    }
                ],
                ensure_ascii=False,
            )
            platform_event_id = str(getattr(event, "event_id", "") or event.id or "")
            record = MessageLogRecord(
                session_id=session_id,
                platform_msg_id=platform_event_id,
                sender_id=event.sender_id or event.operator_id or "",
                sender_name=event.sender_name or "",
                content_json=content_json,
                raw_text=f"[notice:{event.type}]",
                role="system",
                is_read=True,
                is_mentioned=False,
                created_at=observed_at * 1000,
            )
            message_log_id = self._database.message_logs.insert(record)
            record.id = message_log_id
            return message_log_id
        except Exception:
            logger.exception("Failed to persist notice event to message_logs")
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

    async def _run_pre_route_hooks(self, dispatch_context: RouteDispatchContext) -> None:
        for hook in list(self._pre_route_hooks):
            try:
                result = hook(dispatch_context)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("pre_route_hook_error: hook=%s", hook)

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
                logger.error(
                    format_log_event(
                        "route.target.missing",
                        rule_id=rule.id,
                        target=rule.target,
                        message_log_id=dispatch_context.message_log_id,
                        trace_id=dispatch_context.trace_id,
                    )
                )
                continue

            try:
                result = handler(dispatch_context, rule)
            except Exception:
                logger.exception(
                    format_log_event(
                        "route.target.error",
                        rule_id=rule.id,
                        target=rule.target,
                        message_log_id=dispatch_context.message_log_id,
                        trace_id=dispatch_context.trace_id,
                    )
                )
                continue

            if inspect.isawaitable(result):
                logger.debug(
                    format_log_event(
                        "route.target.scheduled",
                        rule_id=rule.id,
                        target=rule.target,
                        message_log_id=dispatch_context.message_log_id,
                        trace_id=dispatch_context.trace_id,
                    )
                )
                task = asyncio.create_task(result)
                task.add_done_callback(
                    lambda done, matched_rule=rule: _log_route_target_task_result(
                        done,
                        matched_rule,
                        trace_id=dispatch_context.trace_id,
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
    event_seconds = normalize_event_timestamp_seconds(event.timestamp, now=current_time)
    age = current_time - event_seconds
    return age < max_age_seconds


def normalize_event_timestamp_seconds(timestamp: float | int, *, now: float | None = None) -> float:
    """Normalize common platform timestamp units to epoch seconds."""

    value = float(timestamp)
    millisecond_value = value / 1000
    if now is None:
        return millisecond_value if value > 1_000_000_000_000 else value
    return min((value, millisecond_value), key=lambda candidate: abs(now - candidate))


def build_ingress_trace_id(adapter: BaseAdapter, event: UnifiedEvent) -> str:
    """Build a stable-enough trace id for one normalized ingress event."""

    platform_id = event.message.id if event.message is not None else event.id
    token = str(platform_id or uuid.uuid4().hex[:12]).strip()
    if not token:
        token = uuid.uuid4().hex[:12]
    return f"ingress:{adapter.instance_id}:{token}"


def _log_route_target_task_result(
    done: asyncio.Task[Any],
    rule: RouteRule,
    *,
    trace_id: str,
) -> None:
    try:
        done.result()
    except asyncio.CancelledError:
        logger.debug(
            format_log_event(
                "route.target.cancelled",
                rule_id=rule.id,
                target=rule.target,
                trace_id=trace_id,
            )
        )
    except Exception:
        logger.exception(
            format_log_event(
                "route.target.error",
                rule_id=rule.id,
                target=rule.target,
                trace_id=trace_id,
            )
        )
