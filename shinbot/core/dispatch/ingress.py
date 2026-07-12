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
import math
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from shinbot.core.application.bot_routing import (
    AGENT_ENTRY_TARGET_NAME,
    BotRuntimeRouter,
    bot_route_rule_enabled_for_context,
    bot_session_id_for_selection,
    permission_scope_for_event,
)
from shinbot.core.dispatch.agent_identity import SessionKeyFactory
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnership,
    AgentRuntimeOwnershipConflict,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.core.dispatch.durable_routing import (
    IngressRoutingPayload,
    MessageRoutingJobStatus,
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
from shinbot.core.security.permission import PermissionEngine, runtime_group_ids_for_member_roles
from shinbot.core.state.session import SessionManager, build_session_id
from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.elements import Message
from shinbot.schema.events import UnifiedEvent
from shinbot.schema.routing import MessageRoutingSkipReason
from shinbot.utils.logger import format_log_event, get_logger
from shinbot.utils.resource_ingress import summarize_message_modalities

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager
    from shinbot.persistence.repositories.durable_routing import (
        ClaimedMessageRoutingJob,
    )

logger = get_logger(__name__, source="dispatch", color="cyan")

MAX_MESSAGE_AGE_SECONDS = 60

ROUTING_SKIP_EXPIRED_MESSAGE = MessageRoutingSkipReason.EXPIRED_MESSAGE.value
ROUTING_SKIP_NO_ROUTE_MATCHED = MessageRoutingSkipReason.NO_ROUTE_MATCHED.value
ROUTING_SKIP_SESSION_MUTED = MessageRoutingSkipReason.SESSION_MUTED.value
ROUTING_SKIP_INTERCEPTOR_BLOCKED = MessageRoutingSkipReason.INTERCEPTOR_BLOCKED.value
ROUTING_SKIP_WAIT_FOR_INPUT = MessageRoutingSkipReason.WAIT_FOR_INPUT.value


class DurableRoutingReplayDeferred(RuntimeError):
    """Signal that a durable job remains recoverable but cannot run yet."""

    def __init__(self, code: str, message: str) -> None:
        """Store a stable retry classification for supervisor health."""

        self.code = str(code or "routing_replay_deferred").strip()
        super().__init__(message)


def runtime_permission_group_ids_for_event(event: UnifiedEvent) -> tuple[str, ...]:
    """Return permission group IDs derived from the current platform event."""
    if event.is_private or event.member is None:
        return ()
    return runtime_group_ids_for_member_roles(event.member.roles)


@dataclass(slots=True)
class RouteDispatchContext:
    """Context passed to route target dispatchers."""

    event: UnifiedEvent
    adapter: BaseAdapter
    message: Message
    message_context: MessageContext | None = None
    message_log_id: int | None = None
    trace_id: str = ""
    observed_at: float = 0.0

    def require_message_context(self) -> MessageContext:
        """Return the message context or raise if it was not attached.

        Raises:
            RuntimeError: When ``message_context`` is ``None``.
        """
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
        self._target_tasks: set[asyncio.Future[Any]] = set()
        self._target_tasks_by_owner: dict[str, set[asyncio.Future[Any]]] = {}
        self._blocked_task_owners: set[str] = set()
        self._closing = False

    @property
    def pending_task_count(self) -> int:
        """Return the number of route target tasks still in flight."""
        return len(self._target_tasks)

    def pending_task_count_for_owner(self, owner: str) -> int:
        """Return the number of in-flight target tasks owned by ``owner``."""
        return len(self._target_tasks_by_owner.get(owner, ()))

    def accepts_tasks(self, owner: str | None) -> bool:
        """Return whether new target work may be scheduled for ``owner``."""
        return not self._closing and (owner is None or owner not in self._blocked_task_owners)

    def schedule_awaitable(
        self,
        result: Awaitable[None],
        *,
        rule: RouteRule,
        trace_id: str,
    ) -> asyncio.Future[Any] | None:
        """Schedule and track one asynchronous route target invocation."""
        owner = rule.owner
        if not self.accepts_tasks(owner):
            _discard_awaitable(result)
            return None

        task = asyncio.ensure_future(result)
        if isinstance(task, asyncio.Task):
            task.set_name(f"route.target.{rule.id}")
        self._target_tasks.add(task)
        if owner is not None:
            self._target_tasks_by_owner.setdefault(owner, set()).add(task)
        task.add_done_callback(
            lambda done, task_owner=owner, matched_rule=rule, task_trace_id=trace_id: (
                self._route_target_done(
                    done,
                    owner=task_owner,
                    rule=matched_rule,
                    trace_id=task_trace_id,
                )
            )
        )
        return task

    async def run_owned_awaitable(
        self,
        result: Awaitable[Any],
        *,
        owner: str | None,
        name: str,
    ) -> Any:
        """Run one handler invocation under the owning plugin's task scope."""
        if not self.accepts_tasks(owner):
            _discard_awaitable(result)
            raise asyncio.CancelledError(f"route target owner is inactive: {owner or '<none>'}")

        current_task = asyncio.current_task()
        if current_task is not None and current_task in self._target_tasks:
            owner_was_tracked = False
            if owner is not None:
                owner_tasks = self._target_tasks_by_owner.setdefault(owner, set())
                owner_was_tracked = current_task in owner_tasks
                owner_tasks.add(current_task)
            try:
                return await result
            finally:
                if owner is not None and not owner_was_tracked:
                    self._remove_owner_task(current_task, owner=owner)

        task = asyncio.ensure_future(result)
        if isinstance(task, asyncio.Task):
            task.set_name(name)
        self._target_tasks.add(task)
        if owner is not None:
            self._target_tasks_by_owner.setdefault(owner, set()).add(task)
        task.add_done_callback(
            lambda done, task_owner=owner: self._owned_task_done(
                done,
                owner=task_owner,
            )
        )
        return await task

    async def cancel_owner_tasks(
        self,
        owner: str,
        *,
        preserve_task: asyncio.Task[Any] | None = None,
    ) -> None:
        """Block and drain an owner's tasks, preserving its lifecycle caller."""
        self._blocked_task_owners.add(owner)
        tasks = tuple(
            task
            for task in self._target_tasks_by_owner.get(owner, ())
            if task is not preserve_task
        )
        await _cancel_and_await(tasks)

    def resume_owner_tasks(self, owner: str) -> None:
        """Allow route target tasks for an owner blocked during deactivation."""
        self._blocked_task_owners.discard(owner)

    async def shutdown(self) -> None:
        """Stop accepting target work, then cancel and await all in-flight tasks."""
        self._closing = True
        tasks = tuple(self._target_tasks)
        await _cancel_and_await(tasks)

    def _route_target_done(
        self,
        task: asyncio.Future[Any],
        *,
        owner: str | None,
        rule: RouteRule,
        trace_id: str,
    ) -> None:
        self._remove_target_task(task, owner=owner)
        _log_route_target_task_result(task, rule, trace_id=trace_id)

    def _owned_task_done(
        self,
        task: asyncio.Future[Any],
        *,
        owner: str | None,
    ) -> None:
        self._remove_target_task(task, owner=owner)

    def _remove_target_task(
        self,
        task: asyncio.Future[Any],
        *,
        owner: str | None,
    ) -> None:
        self._target_tasks.discard(task)
        self._remove_owner_task(task, owner=owner)

    def _remove_owner_task(
        self,
        task: asyncio.Future[Any],
        *,
        owner: str | None,
    ) -> None:
        if owner is not None:
            owner_tasks = self._target_tasks_by_owner.get(owner)
            if owner_tasks is not None:
                owner_tasks.discard(task)
                if not owner_tasks:
                    self._target_tasks_by_owner.pop(owner, None)

    def register(
        self,
        target: str,
        handler: RouteTargetHandler,
        *,
        owner: str | None = None,
    ) -> None:
        """Register a dispatcher handler for a named route target.

        Args:
            target: Unique target name referenced by ``RouteRule.target``.
            handler: Callable that will be invoked when the rule matches.
            owner: Optional owner ID for bulk unregistration on plugin unload.

        Raises:
            ValueError: If *target* is empty or already registered.
        """
        if not target:
            raise ValueError("route target must not be empty")
        if target in self._handlers:
            raise ValueError(f"route target already registered: {target}")
        self._handlers[target] = handler
        self._owners[target] = owner

    def unregister(self, target: str) -> RouteTargetHandler | None:
        """Remove a dispatcher handler by target name.

        Args:
            target: The target name previously registered.

        Returns:
            The removed handler, or ``None`` if the target was not found.
        """
        self._owners.pop(target, None)
        return self._handlers.pop(target, None)

    def unregister_by_owner(self, owner: str) -> int:
        """Remove all dispatcher handlers registered by a specific owner.

        Args:
            owner: Owner identifier (typically a plugin ID).

        Returns:
            The number of handlers that were removed.
        """
        targets = [target for target, target_owner in self._owners.items() if target_owner == owner]
        for target in targets:
            self.unregister(target)
        return len(targets)

    def get(self, target: str) -> RouteTargetHandler | None:
        """Look up a dispatcher handler by target name.

        Args:
            target: The target name to look up.

        Returns:
            The registered handler, or ``None`` if not found.
        """
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
        durable_recovery_grace_seconds: float = 2.0,
        durable_routing_timeout_seconds: float = 20.0,
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
        if (
            not math.isfinite(float(durable_recovery_grace_seconds))
            or durable_recovery_grace_seconds < 0
        ):
            raise ValueError("durable_recovery_grace_seconds must be finite and non-negative")
        self._durable_recovery_grace_seconds = float(durable_recovery_grace_seconds)
        if (
            not math.isfinite(float(durable_routing_timeout_seconds))
            or durable_routing_timeout_seconds <= 0
        ):
            raise ValueError("durable_routing_timeout_seconds must be finite and positive")
        self._durable_routing_timeout_seconds = float(durable_routing_timeout_seconds)
        self._session_key_factory = SessionKeyFactory()
        self._durable_worker_id = f"message-ingress:{uuid.uuid4().hex}"
        self._durable_wake_callback: Callable[[], None] | None = None
        self._interceptors: list[tuple[int, Interceptor]] = []
        self._pre_route_hooks: list[PreRouteHook] = []

    @property
    def pending_target_task_count(self) -> int:
        """Return the number of route target tasks still in flight."""
        return self._route_targets.pending_task_count

    async def shutdown(self) -> None:
        """Cancel and await all in-flight route target tasks."""
        await self._route_targets.shutdown()

    def add_interceptor(self, interceptor: Interceptor, priority: int = 100) -> None:
        """Register an interceptor that can block events before routing.

        Interceptors are evaluated in ascending priority order.  The first
        interceptor that returns ``False`` prevents the event from reaching
        any route target.

        Args:
            interceptor: Async callable that receives a ``MessageContext`` and
                returns ``True`` to allow or ``False`` to block.
            priority: Evaluation order (lower runs first).
        """
        self._interceptors.append((priority, interceptor))
        self._interceptors.sort(key=lambda x: x[0])

    def add_pre_route_hook(self, hook: PreRouteHook) -> None:
        """Register a lightweight hook that runs after gates and before route matching."""
        if hook not in self._pre_route_hooks:
            self._pre_route_hooks.append(hook)

    def set_bot_router(self, bot_router: BotRuntimeRouter | None) -> None:
        """Install or clear bot service-unit routing policy."""

        self._bot_router = bot_router

    def set_durable_routing_wake_callback(
        self,
        callback: Callable[[], None] | None,
    ) -> None:
        """Install the supervisor notification used after durable state changes."""

        self._durable_wake_callback = callback

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
        permission_user_id = event.sender_id or ""
        if bot_selection is not None and permission_user_id and ":" not in permission_user_id:
            permission_user_id = f"{adapter.instance_id}:{permission_user_id}"

        permissions = self._permission_engine.resolve(
            instance_id=permission_scope.identity_id,
            session_id=permission_scope.session_id,
            user_id=permission_user_id,
            session_base_group=session.permission_group,
            runtime_group_ids=runtime_permission_group_ids_for_event(event),
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
        ownership = self._resolve_or_claim_runtime_ownership(message_context)
        durable_payload: IngressRoutingPayload | None = None
        durable_job_status: MessageRoutingJobStatus | None = None
        if self._requires_durable_actor_routing(ownership):
            assert ownership is not None
            durable_payload = self._build_durable_routing_payload(
                event=event,
                adapter=adapter,
                message_context=message_context,
                trace_id=trace_id,
                observed_at=observed_at,
            )
            message_log_id, durable_job_status, durable_payload = (
                self._persist_or_reuse_durable_message(
                    event=event,
                    message=message,
                    session_id=session.id,
                    payload=durable_payload,
                    ownership=ownership,
                )
            )
            self._notify_durable_routing()
        else:
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
            observed_at=observed_at,
        )

        if durable_job_status is not None and durable_job_status is not (
            MessageRoutingJobStatus.PENDING
        ):
            self._notify_durable_routing()
            self._session_manager.update(session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                trace_id=trace_id,
            )

        return await self._route_persisted_message(
            dispatch_context,
            bot_selection=bot_selection,
            durable_payload=durable_payload,
            durable_claim=None,
            allow_external_dispatch=True,
        )

    async def _route_persisted_message(
        self,
        dispatch_context: RouteDispatchContext,
        *,
        bot_selection: Any | None,
        durable_payload: IngressRoutingPayload | None,
        durable_claim: ClaimedMessageRoutingJob | None,
        allow_external_dispatch: bool,
    ) -> IngressResult:
        """Acquire the durable lease before running any replayable route work."""

        if durable_payload is None:
            return await self._route_persisted_message_after_claim(
                dispatch_context,
                bot_selection=bot_selection,
                durable_payload=None,
                durable_claim=None,
                allow_external_dispatch=allow_external_dispatch,
            )

        claim, live_claim = self._claim_durable_job(
            durable_payload,
            durable_claim=durable_claim,
        )
        if claim is None:
            self._notify_durable_routing()
            message_context = dispatch_context.require_message_context()
            self._session_manager.update(message_context.session)
            return IngressResult(
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=dispatch_context.message_log_id,
                trace_id=dispatch_context.trace_id,
            )

        assert self._database is not None
        timeout_seconds = min(
            self._durable_routing_timeout_seconds,
            self._database.durable_routing.lease_seconds * 0.8,
        )
        try:
            async with asyncio.timeout(timeout_seconds):
                return await self._route_persisted_message_after_claim(
                    dispatch_context,
                    bot_selection=bot_selection,
                    durable_payload=durable_payload,
                    durable_claim=claim,
                    allow_external_dispatch=allow_external_dispatch,
                )
        except BaseException as exc:
            if live_claim:
                self._release_live_routing_claim(claim, exc)
            raise

    async def _route_persisted_message_after_claim(
        self,
        dispatch_context: RouteDispatchContext,
        *,
        bot_selection: Any | None,
        durable_payload: IngressRoutingPayload | None,
        durable_claim: ClaimedMessageRoutingJob | None,
        allow_external_dispatch: bool,
    ) -> IngressResult:
        """Evaluate gates, commit the route decision, then invoke targets."""

        message_context = dispatch_context.require_message_context()
        event = dispatch_context.event
        adapter = dispatch_context.adapter
        session = message_context.session
        message_log_id = dispatch_context.message_log_id
        trace_id = dispatch_context.trace_id

        fresh = (
            durable_payload.fresh_at_ingress
            if durable_payload is not None
            else is_event_fresh(
                event,
                now=dispatch_context.observed_at,
                max_age_seconds=self._max_message_age_seconds,
            )
        )
        skipped_reason: str | None = None
        if not fresh:
            skipped_reason = ROUTING_SKIP_EXPIRED_MESSAGE
        elif session.is_muted:
            skipped_reason = ROUTING_SKIP_SESSION_MUTED
        elif self._bot_router is not None and bot_selection is None:
            skipped_reason = ROUTING_SKIP_NO_ROUTE_MATCHED
        else:
            skipped_reason = await self._run_interceptors(message_context)

        if skipped_reason is not None:
            return self._complete_skipped_route(
                dispatch_context,
                bot_selection=bot_selection,
                durable_payload=durable_payload,
                durable_claim=durable_claim,
                skipped_reason=skipped_reason,
            )

        await self._run_pre_route_hooks(dispatch_context)
        self._log_audit(event, message_context)
        matched_rules = self._route_table.match(
            event,
            dispatch_context.message,
            RouteMatchContext(
                adapter=adapter,
                session=session,
                message_context=message_context,
                rule_filter=lambda rule: bot_route_rule_enabled_for_context(
                    rule,
                    message_context,
                )
                and (
                    durable_payload is not None
                    or self._route_targets.accepts_tasks(rule.owner)
                ),
            ),
        )
        if not matched_rules:
            return self._complete_skipped_route(
                dispatch_context,
                bot_selection=bot_selection,
                durable_payload=durable_payload,
                durable_claim=durable_claim,
                skipped_reason=ROUTING_SKIP_NO_ROUTE_MATCHED,
            )

        if durable_payload is None:
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

        if self._database is None:
            raise RuntimeError("durable Agent routing requires a database")
        if durable_claim is None:
            raise RuntimeError("durable routing decision requires a live claim")
        envelope = durable_claim.envelope
        if (
            envelope.profile_id != durable_payload.session_key.profile_id
            or envelope.session_id != durable_payload.session_key.session_id
            or envelope.ownership_generation < 1
        ):
            raise RuntimeError(
                "durable routing envelope changed the canonical ownership scope"
            )
        ownership = self._database.agent_runtime_ownership.get(durable_payload.session_key)
        if ownership is None:
            raise DurableRoutingReplayDeferred(
                "ownership_missing",
                "durable routing ownership is not available",
            )
        if ownership.status is AgentRuntimeOwnershipStatus.MIGRATING:
            raise DurableRoutingReplayDeferred(
                "ownership_migrating",
                "durable routing waits for ownership migration to settle",
            )
        if ownership.generation != envelope.ownership_generation:
            raise DurableRoutingReplayDeferred(
                "ownership_generation_changed",
                "durable routing ownership changed after ingress acceptance",
            )

        agent_deliveries: list[Any] = []
        external_rules: list[RouteRule] = []
        if ownership.actor_v2_active:
            for rule in matched_rules:
                if rule.target != AGENT_ENTRY_TARGET_NAME:
                    external_rules.append(rule)
                    continue
                handler = self._route_targets.get(rule.target)
                prepare_delivery = getattr(handler, "prepare_delivery", None)
                if not callable(prepare_delivery):
                    raise DurableRoutingReplayDeferred(
                        "agent_delivery_preparer_missing",
                        f"Agent target {rule.target!r} cannot prepare a durable delivery",
                    )
                delivery = prepare_delivery(dispatch_context, rule)
                if delivery.session_key != durable_payload.session_key:
                    raise RuntimeError(
                        "prepared Agent delivery changed the canonical session key"
                    )
                agent_deliveries.append(delivery)
        else:
            external_rules.extend(matched_rules)

        if external_rules and not allow_external_dispatch:
            raise DurableRoutingReplayDeferred(
                "adapter_not_ready",
                "external route targets are disabled before adapter readiness",
            )
        for rule in external_rules:
            if not self._route_targets.accepts_tasks(rule.owner):
                raise DurableRoutingReplayDeferred(
                    "route_target_owner_inactive",
                    f"route target owner is inactive for rule {rule.id!r}",
                )
            if self._route_targets.get(rule.target) is None:
                raise DurableRoutingReplayDeferred(
                    "route_target_missing",
                    f"route target {rule.target!r} is not registered",
                )

        metadata = {
            "rule_ids": [rule.id for rule in matched_rules],
            "targets": [rule.target for rule in matched_rules],
            "ownership_mode": ownership.mode.value,
            "ownership_generation": ownership.generation,
        }
        if agent_deliveries:
            self._database.durable_routing.complete_with_agent_deliveries(
                durable_claim,
                agent_deliveries,
                metadata=metadata,
                expected_ownership_generations={
                    durable_payload.session_key: ownership.generation,
                },
            )
        else:
            self._database.durable_routing.complete_dispatched_without_agent(
                durable_claim,
                metadata=metadata,
            )

        self._notify_durable_routing()
        self._log_routing_result(
            event=event,
            adapter=adapter,
            session_id=session.id,
            message_log_id=message_log_id,
            trace_id=trace_id,
            bot_selection=bot_selection,
            matched_rules=matched_rules,
        )
        self._schedule_targets(dispatch_context, external_rules)
        self._session_manager.update(session)
        return IngressResult(
            dispatch_context=dispatch_context,
            matched_rules=matched_rules,
            message_log_id=message_log_id,
            trace_id=trace_id,
        )

    def _complete_skipped_route(
        self,
        dispatch_context: RouteDispatchContext,
        *,
        bot_selection: Any | None,
        durable_payload: IngressRoutingPayload | None,
        durable_claim: ClaimedMessageRoutingJob | None,
        skipped_reason: str,
    ) -> IngressResult:
        """Commit a skipped decision through the matching persistence mode."""

        message_context = dispatch_context.require_message_context()
        message_log_id = dispatch_context.message_log_id
        if durable_payload is None:
            self._mark_skipped(message_log_id, skipped_reason)
        else:
            if self._database is None:
                raise RuntimeError("durable Agent routing requires a database")
            if durable_claim is None:
                raise RuntimeError("durable skipped decision requires a live claim")
            self._database.durable_routing.complete_without_agent_delivery(
                durable_claim,
                skip_reason=skipped_reason,
                metadata={"skip_reason": skipped_reason},
            )
            self._notify_durable_routing()

        self._log_routing_result(
            event=dispatch_context.event,
            adapter=dispatch_context.adapter,
            session_id=message_context.session.id,
            message_log_id=message_log_id,
            trace_id=dispatch_context.trace_id,
            bot_selection=bot_selection,
            skipped_reason=skipped_reason,
        )
        self._session_manager.update(message_context.session)
        return IngressResult(
            dispatch_context=dispatch_context,
            matched_rules=[],
            message_log_id=message_log_id,
            skipped_reason=skipped_reason,
            trace_id=dispatch_context.trace_id,
        )

    def _claim_durable_job(
        self,
        payload: IngressRoutingPayload,
        *,
        durable_claim: ClaimedMessageRoutingJob | None,
    ) -> tuple[ClaimedMessageRoutingJob | None, bool]:
        """Return a caller claim or acquire the live ingress claim."""

        if self._database is None:
            raise RuntimeError("durable Agent routing requires a database")
        if durable_claim is not None:
            claimed_payload = IngressRoutingPayload.from_payload(
                durable_claim.envelope.payload
            )
            if claimed_payload.to_payload() != payload.to_payload():
                raise RuntimeError("durable routing claim payload changed during replay")
            if durable_claim.message_log_id <= 0:
                raise RuntimeError("durable routing claim has no message log")
            key = payload.session_key
            if (
                durable_claim.envelope.profile_id != key.profile_id
                or durable_claim.envelope.session_id != key.session_id
                or durable_claim.envelope.ownership_generation < 1
            ):
                raise RuntimeError(
                    "durable routing claim has no canonical ownership fence"
                )
            return durable_claim, False
        claim = self._database.durable_routing.claim_job(
            payload.routing_job_id,
            worker_id=self._durable_worker_id,
            ignore_available_at=True,
        )
        if claim is None:
            return None, False
        validated, _caller_owned = self._claim_durable_job(
            payload,
            durable_claim=claim,
        )
        return validated, True

    def _release_live_routing_claim(
        self,
        claim: ClaimedMessageRoutingJob,
        error: BaseException,
    ) -> None:
        """Release a failed live claim so the supervisor can retry it."""

        if self._database is None:
            return
        try:
            self._database.durable_routing.retry_or_fail_job(
                claim,
                error_code=type(error).__name__,
                error_message=str(error),
                retry_at=time.time() + self._durable_recovery_grace_seconds,
            )
            self._notify_durable_routing()
        except Exception:
            logger.exception(
                "failed_to_release_live_routing_claim: job_id=%s",
                claim.routing_job_id,
            )

    async def replay_claimed_routing_job(
        self,
        claim: ClaimedMessageRoutingJob,
        adapter: BaseAdapter,
        *,
        allow_external_dispatch: bool = True,
    ) -> IngressResult:
        """Reconstruct and execute one lease-bound durable routing job."""

        payload = IngressRoutingPayload.from_payload(claim.envelope.payload)
        if claim.routing_job_id != payload.routing_job_id:
            raise RuntimeError("routing claim id does not match its canonical payload")
        if adapter.instance_id != payload.adapter_instance_id:
            raise DurableRoutingReplayDeferred(
                "adapter_instance_unavailable",
                "the persisted adapter instance is not available",
            )
        if adapter.platform != payload.adapter_platform:
            raise RuntimeError("persisted adapter platform changed during replay")

        event = payload.to_event()
        message = Message.from_xml(payload.message_xml) if payload.message_xml else Message()
        base_session_id = build_session_id(adapter.instance_id, event)
        if base_session_id != payload.base_session_id:
            raise RuntimeError("persisted base session identity is not reproducible")

        async with self._session_manager.session_lock(base_session_id):
            session = self._session_manager.get_or_create(adapter.instance_id, event)
            session.touch()
            bot_selection = self._resolve_bot_selection(event, adapter)
            if payload.bot_id:
                if bot_selection is None:
                    raise DurableRoutingReplayDeferred(
                        "bot_binding_unavailable",
                        "the persisted bot binding is not currently available",
                    )
                if (
                    bot_selection.bot.id != payload.bot_id
                    or bot_selection.binding.id != payload.bot_binding_id
                    or bot_session_id_for_selection(bot_selection, event=event)
                    != payload.bot_session_id
                ):
                    raise DurableRoutingReplayDeferred(
                        "bot_binding_changed",
                        "current bot routing does not match the persisted ingress identity",
                    )
            elif bot_selection is not None:
                raise DurableRoutingReplayDeferred(
                    "bot_binding_changed",
                    "an unbound persisted message now resolves to a bot binding",
                )

            permission_scope = permission_scope_for_event(
                bot_selection,
                event=event,
                fallback_identity_id=adapter.instance_id,
                fallback_session_id=session.id,
            )
            permission_user_id = event.sender_id or ""
            if bot_selection is not None and permission_user_id and ":" not in permission_user_id:
                permission_user_id = f"{adapter.instance_id}:{permission_user_id}"
            permissions = self._permission_engine.resolve(
                instance_id=permission_scope.identity_id,
                session_id=permission_scope.session_id,
                user_id=permission_user_id,
                session_base_group=session.permission_group,
                runtime_group_ids=runtime_permission_group_ids_for_event(event),
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
                message_context.bot_session_id = payload.bot_session_id
            message_context._msg_log_id = claim.message_log_id
            dispatch_context = RouteDispatchContext(
                event=event,
                adapter=adapter,
                message=message,
                message_context=message_context,
                message_log_id=claim.message_log_id,
                trace_id=payload.trace_id,
                observed_at=payload.observed_at,
            )
            return await self._route_persisted_message(
                dispatch_context,
                bot_selection=bot_selection,
                durable_payload=payload,
                durable_claim=claim,
                allow_external_dispatch=allow_external_dispatch,
            )

    def _resolve_bot_selection(self, event: UnifiedEvent, adapter: BaseAdapter) -> Any | None:
        if self._bot_router is None:
            return None
        return self._bot_router.resolve(adapter_instance_id=adapter.instance_id, event=event)

    def _resolve_or_claim_runtime_ownership(
        self,
        message_context: MessageContext,
    ) -> AgentRuntimeOwnership | None:
        """Return the durable runtime owner, defaulting new sessions to legacy."""

        if self._database is None:
            return None
        key = self._session_key_factory.create(
            bot_config_id=message_context.bot_id,
            bot_id=message_context.bot_id,
            bot_session_id=message_context.bot_session_id,
            base_session_id=message_context.session_id,
        )
        repository = self._database.agent_runtime_ownership
        existing = repository.get(key)
        if existing is not None:
            return existing
        try:
            return repository.claim(
                key,
                AgentRuntimeOwnershipMode.LEGACY,
                reason="default legacy ownership selected at first ingress",
                legacy_session_id=message_context.session_id,
                requested_by="core.message_ingress",
            ).ownership
        except AgentRuntimeOwnershipConflict:
            # A concurrent explicit actor claim wins. Evidence conflicts without
            # an ownership row remain fatal instead of silently selecting legacy.
            existing = repository.get(key)
            if existing is not None:
                return existing
            raise

    @staticmethod
    def _requires_durable_actor_routing(
        ownership: AgentRuntimeOwnership | None,
    ) -> bool:
        """Return whether ingress must be buffered behind the ownership fence."""

        if ownership is None:
            return False
        return (
            ownership.status is AgentRuntimeOwnershipStatus.MIGRATING
            or ownership.mode is AgentRuntimeOwnershipMode.ACTOR_V2
        )

    def _build_durable_routing_payload(
        self,
        *,
        event: UnifiedEvent,
        adapter: BaseAdapter,
        message_context: MessageContext,
        trace_id: str,
        observed_at: float,
    ) -> IngressRoutingPayload:
        """Capture every input needed to deterministically replay routing."""

        return IngressRoutingPayload(
            event=event.model_dump(mode="json"),
            adapter_instance_id=adapter.instance_id,
            adapter_platform=adapter.platform,
            message_xml=event.message_content,
            trace_id=trace_id,
            observed_at=observed_at,
            base_session_id=message_context.session_id,
            bot_id=message_context.bot_id,
            bot_binding_id=message_context.bot_binding_id,
            bot_session_id=message_context.bot_session_id,
            fresh_at_ingress=is_event_fresh(
                event,
                now=observed_at,
                max_age_seconds=self._max_message_age_seconds,
            ),
        )

    def _persist_or_reuse_durable_message(
        self,
        *,
        event: UnifiedEvent,
        message: Message,
        session_id: str,
        payload: IngressRoutingPayload,
        ownership: AgentRuntimeOwnership,
    ) -> tuple[int, MessageRoutingJobStatus, IngressRoutingPayload]:
        """Atomically persist a message/job or reuse its canonical duplicate."""

        if self._database is None:
            raise RuntimeError("durable Agent routing requires a database")
        repository = self._database.durable_routing

        def reuse_existing() -> tuple[int, MessageRoutingJobStatus, IngressRoutingPayload] | None:
            existing = repository.get_job(payload.routing_job_id)
            if existing is None:
                return None
            persisted_payload = IngressRoutingPayload.from_payload(existing.envelope.payload)
            if not persisted_payload.has_same_ingress_identity(payload):
                from shinbot.persistence.repositories.durable_routing import (
                    DurableRoutingConflict,
                )

                raise DurableRoutingConflict(
                    "duplicate platform event changed its canonical ingress identity"
                )
            return existing.message_log_id, existing.status, persisted_payload

        reused = reuse_existing()
        if reused is not None:
            return reused

        record = self._incoming_message_record(
            event=event,
            message=message,
            session_id=session_id,
            observed_at=payload.observed_at,
        )
        envelope = payload.to_job_envelope(
            ownership_generation=ownership.generation,
            available_at=payload.observed_at + self._durable_recovery_grace_seconds,
        )
        try:
            persisted = repository.persist_message_and_job(record, envelope)
        except Exception:
            # Close the first-insert race without weakening conflict checks. A
            # different durable identity is re-raised by ``reuse_existing``.
            reused = reuse_existing()
            if reused is not None:
                return reused
            raise
        return persisted.message_log_id, persisted.status, payload

    def _incoming_message_record(
        self,
        *,
        event: UnifiedEvent,
        message: Message,
        session_id: str,
        observed_at: float,
    ) -> MessageLogRecord:
        """Build the canonical user message record shared by both ingress modes."""

        content_json = json.dumps(
            [element.model_dump(mode="json") for element in message.elements],
            ensure_ascii=False,
        )
        return MessageLogRecord(
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

    def _notify_durable_routing(self) -> None:
        """Best-effort wake the routing supervisor after a durable commit."""

        callback = self._durable_wake_callback
        if callback is None:
            return
        try:
            callback()
        except Exception:
            logger.exception("durable_routing_wake_failed")

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
                dispatch_context=dispatch_context,
                matched_rules=[],
                message_log_id=message_log_id,
                skipped_reason=ROUTING_SKIP_EXPIRED_MESSAGE,
                trace_id=trace_id,
            )

        matched_rules = self._route_table.match(
            event,
            message,
            RouteMatchContext(
                rule_filter=lambda rule: self._route_targets.accepts_tasks(rule.owner),
            ),
        )
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
            record = self._incoming_message_record(
                event=event,
                message=message,
                session_id=session_id,
                observed_at=observed_at,
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
                logger.exception(
                    "Interceptor error (fail-open): %s", interceptor.__name__
                )
                continue
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
            if not self._route_targets.accepts_tasks(rule.owner):
                logger.debug(
                    format_log_event(
                        "route.target.skipped",
                        rule_id=rule.id,
                        target=rule.target,
                        reason="target_supervisor_closed",
                        message_log_id=dispatch_context.message_log_id,
                        trace_id=dispatch_context.trace_id,
                    )
                )
                continue
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
                task = self._route_targets.schedule_awaitable(
                    result,
                    rule=rule,
                    trace_id=dispatch_context.trace_id,
                )
                if task is not None:
                    logger.debug(
                        format_log_event(
                            "route.target.scheduled",
                            rule_id=rule.id,
                            target=rule.target,
                            message_log_id=dispatch_context.message_log_id,
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


async def _cancel_and_await(tasks: tuple[asyncio.Future[Any], ...]) -> None:
    current_task = asyncio.current_task()
    pending = tuple(task for task in tasks if task is not current_task)
    if not pending:
        return
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


def _discard_awaitable(result: Awaitable[Any]) -> None:
    if inspect.iscoroutine(result):
        result.close()
        return
    if isinstance(result, asyncio.Future):
        result.cancel()
        return
    task = asyncio.ensure_future(result)
    task.cancel()


def _log_route_target_task_result(
    done: asyncio.Future[Any],
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
