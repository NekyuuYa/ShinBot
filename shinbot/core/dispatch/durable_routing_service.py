"""Supervised recovery and relay for durable core message routing."""

from __future__ import annotations

import asyncio
import inspect
import math
import time
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.durable_routing import IngressRoutingPayload
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.ingress import DurableRoutingReplayDeferred
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffNotifier
from shinbot.core.platform.adapter_manager import BaseAdapter
from shinbot.persistence.repositories.durable_routing import (
    ClaimedAgentRouteDelivery,
    ClaimedMessageRoutingJob,
    DurableMessageRoutingRepository,
    PendingRouteWakeDebt,
    RouteRelayResult,
    RouteWakeCursor,
)
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="dispatch", color="cyan")

_LEGACY_WAKE_DISCOVERY_LIMIT = 100
_LEGACY_WAKE_STATE_CAPACITY = _LEGACY_WAKE_DISCOVERY_LIMIT * 2


class DurableRoutingServiceStatus(StrEnum):
    """Lifecycle status of the routing recovery service."""

    STOPPED = "stopped"
    PREPARED = "prepared"
    RUNNING = "running"
    DEGRADED = "degraded"


class AgentMailboxWakeTarget(Protocol):
    """Actor surface used only after a mailbox transaction has committed."""

    def wake(self, key: SessionKey) -> Awaitable[None] | None:
        """Wake the actor that owns an already-persisted mailbox event."""

RoutingReplay = Callable[
    [ClaimedMessageRoutingJob, BaseAdapter],
    Awaitable[Any],
]
AdapterResolver = Callable[[str], BaseAdapter | None]


@dataclass(slots=True, frozen=True)
class DurableRoutingHealthSnapshot:
    """Read-only operational state for diagnostics and readiness checks."""

    status: DurableRoutingServiceStatus
    prepared: bool
    started: bool
    adapters_ready: bool
    actor_consumer_ready: bool
    ready_for_actor_traffic: bool
    degraded_reason: str
    worker_id: str
    pending_job_count: int
    pending_delivery_count: int
    active_actor_ownership_count: int
    wake_debt_count: int
    active_job_id: str
    active_delivery_id: str
    last_scan_at: float
    last_success_at: float
    last_error_at: float
    last_error_code: str
    last_error_message: str
    consecutive_failures: int
    processed_job_count: int
    relayed_delivery_count: int
    fenced_request_scoped: bool = False
    fenced_scope_live: bool = False


class DurableRoutingService:
    """Recover routing jobs and relay Agent outbox work with lease fencing.

    The default service remains a process-wide compatibility service.  The
    ``fenced_request_scope`` mode is reserved for
    :class:`FencedDurableRoutingService`, whose caller already owns a live,
    exact target lifecycle.  It never installs a key-only wake target or an
    advisory notifier, and can claim only the complete fenced request supplied
    at construction.
    """

    def __init__(
        self,
        *,
        repository: DurableMessageRoutingRepository,
        replay: RoutingReplay,
        adapter_resolver: AdapterResolver,
        actor_wake_target: AgentMailboxWakeTarget | None = None,
        mailbox_handoff_notifier: MailboxHandoffNotifier | None = None,
        worker_id: str | None = None,
        poll_interval_seconds: float = 1.0,
        retry_base_seconds: float = 1.0,
        retry_max_seconds: float = 60.0,
        max_attempts: int = 5,
        operation_timeout_seconds: float = 20.0,
        clock: Callable[[], float] | None = None,
        fenced_request_scope: FencedMailboxWakeRequest | None = None,
    ) -> None:
        """Initialize the service without starting background recovery."""

        if fenced_request_scope is not None:
            if not isinstance(fenced_request_scope, FencedMailboxWakeRequest):
                raise TypeError(
                    "fenced_request_scope must be a FencedMailboxWakeRequest or None"
                )
            if not fenced_request_scope.has_admission_fence:
                raise ValueError("fenced_request_scope requires an admission fence")
            if actor_wake_target is not None:
                raise ValueError(
                    "a fenced routing scope cannot install a key-only actor wake target"
                )
            if mailbox_handoff_notifier is not None:
                raise ValueError(
                    "a fenced routing scope cannot install a global handoff notifier"
                )
        self._repository = repository
        self._replay = replay
        self._adapter_resolver = adapter_resolver
        if mailbox_handoff_notifier is not None and not callable(
            getattr(mailbox_handoff_notifier, "notify", None)
        ):
            raise TypeError("mailbox_handoff_notifier must implement notify(mailbox_id)")
        self._actor_wake_target = actor_wake_target
        self._mailbox_handoff_notifier = mailbox_handoff_notifier
        self._fenced_request_scope = fenced_request_scope
        self._worker_id = str(
            worker_id or f"durable-routing:{uuid.uuid4().hex}"
        ).strip()
        if not self._worker_id:
            raise ValueError("worker_id must not be empty")
        self._poll_interval_seconds = _positive_finite(
            poll_interval_seconds,
            "poll_interval_seconds",
        )
        self._retry_base_seconds = _nonnegative_finite(
            retry_base_seconds,
            "retry_base_seconds",
        )
        self._retry_max_seconds = _positive_finite(
            retry_max_seconds,
            "retry_max_seconds",
        )
        self._operation_timeout_seconds = _positive_finite(
            operation_timeout_seconds,
            "operation_timeout_seconds",
        )
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        self._max_attempts = int(max_attempts)
        self._clock = clock or time.time

        self._prepared = False
        self._started = False
        self._adapters_ready = False
        self._closed = False
        self._wake_event = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()
        self._startup_complete = asyncio.Event()
        self._startup_complete.set()
        self._task: asyncio.Task[None] | None = None
        self._active_job: ClaimedMessageRoutingJob | None = None
        self._active_delivery: ClaimedAgentRouteDelivery | None = None
        self._wake_debt: OrderedDict[
            FencedMailboxWakeRequest, PendingRouteWakeDebt
        ] = OrderedDict()
        self._legacy_wake_discovery_cursor: RouteWakeCursor | None = None
        self._legacy_wake_target_epoch = 0
        self._legacy_broad_wake_fenced_debt = False
        self._wake_recovery_lock = asyncio.Lock()

        self._last_scan_at = 0.0
        self._last_success_at = 0.0
        self._last_error_at = 0.0
        self._last_error_code = ""
        self._last_error_message = ""
        self._consecutive_failures = 0
        self._processed_job_count = 0
        self._relayed_delivery_count = 0

    @property
    def started(self) -> bool:
        """Return whether the supervised recovery loop is running."""

        return self._started

    @property
    def fenced_request_scope(self) -> FencedMailboxWakeRequest | None:
        """Return the exact fenced request this service may relay, if scoped."""

        return self._fenced_request_scope

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain used for routing claims and relays."""

        return getattr(self._repository, "persistence_domain", self._repository)

    def set_actor_wake_target(
        self,
        target: AgentMailboxWakeTarget | None,
    ) -> None:
        """Install or clear the actor consumer used after mailbox commits."""

        if self._fenced_request_scope is not None:
            raise RuntimeError(
                "a fenced routing scope cannot bind a key-only actor wake target"
            )
        self._actor_wake_target = target
        self._legacy_wake_target_epoch += 1
        self._legacy_wake_discovery_cursor = None
        self.wake()

    def set_mailbox_handoff_notifier(
        self,
        notifier: MailboxHandoffNotifier | None,
    ) -> None:
        """Install or clear the advisory fenced-mailbox notifier.

        This only changes post-commit hint delivery. It does not bind a handoff
        target, start a dispatcher, or authorize an Actor v2 wake.
        """

        if self._fenced_request_scope is not None:
            raise RuntimeError(
                "a fenced routing scope cannot bind a global handoff notifier"
            )
        if notifier is not None and not callable(getattr(notifier, "notify", None)):
            raise TypeError("notifier must implement notify(mailbox_id)")
        self._mailbox_handoff_notifier = notifier
        self.wake()

    def wake(self) -> None:
        """Notify the service that new durable work may be available."""

        if not self._closed:
            self._wake_event.set()

    async def prepare(self) -> DurableRoutingHealthSnapshot:
        """Relay already-decided outbox rows without replaying route jobs.

        This barrier is safe before adapter readiness because it performs only
        database transactions. Actor wakeups are deferred until :meth:`start`.
        """

        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("a closed durable routing service cannot be prepared")
            if self._prepared:
                return self.health_snapshot()
            self._prepared = True

        if self._delivery_sink_available():
            while await self._process_delivery_once(wake_after_commit=False):
                pass
        return self.health_snapshot()

    async def start(self) -> DurableRoutingHealthSnapshot:
        """Declare adapters ready, recover mailbox wake debt, and start polling."""

        startup_waiter: asyncio.Event | None = None
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("a closed durable routing service cannot be started")
            if self._started:
                if self._task is not None:
                    return self.health_snapshot()
                startup_waiter = self._startup_complete
            else:
                if not self._prepared:
                    self._prepared = True
                self._adapters_ready = True
                self._started = True
                self._startup_complete.clear()
        if startup_waiter is not None:
            await startup_waiter.wait()
            return await self.start()
        try:
            await self._recover_wake_debt(force=True)
            async with self._lifecycle_lock:
                if self._closed:
                    self._startup_complete.set()
                    return self.health_snapshot()
                self._task = asyncio.create_task(
                    self._run(),
                    name=(
                        "core.fenced-durable-routing"
                        if self._fenced_request_scope is not None
                        else "core.durable-routing"
                    ),
                )
                self._startup_complete.set()
        except BaseException:
            async with self._lifecycle_lock:
                if not self._closed and self._task is None:
                    self._started = False
                    self._adapters_ready = False
                self._startup_complete.set()
            raise
        self.wake()
        return self.health_snapshot()

    async def shutdown(self) -> None:
        """Stop recovery and release any interrupted lease for restart."""

        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            self._adapters_ready = False
            self._legacy_wake_target_epoch += 1
            self._startup_complete.set()
            task = self._task
            self._task = None
            self._wake_event.set()
            if task is not None:
                task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        self._release_interrupted_claims()
        self._started = False

    def health_snapshot(self) -> DurableRoutingHealthSnapshot:
        """Return current durable backlog, readiness, and failure state."""

        scoped = self._fenced_request_scope is not None
        fenced_scope_live = False
        try:
            pending_jobs, pending_deliveries = self._repository.pending_counts()
            actor_owners = self._repository.active_actor_ownership_count()
            if self._fenced_request_scope is not None:
                fenced_scope_live = self._repository.is_live_fenced_request(
                    self._fenced_request_scope
                )
        except Exception as exc:
            self._record_failure(exc)
            pending_jobs = -1
            pending_deliveries = -1
            actor_owners = -1

        fenced_handoff_notifier_required = (
            not scoped and self._fenced_handoff_notifier_required()
        )
        consumer_ready = (
            self._legacy_actor_target_available()
            and not self._legacy_broad_wake_fenced_debt
        )
        ready_for_actor = (
            not scoped
            and self._started
            and self._adapters_ready
            and actor_owners >= 0
            and (actor_owners == 0 or consumer_ready)
        )
        degraded_reason = ""
        if scoped and not fenced_scope_live:
            degraded_reason = "fenced_request_scope_inactive"
        elif fenced_handoff_notifier_required:
            degraded_reason = "mailbox_handoff_notifier_unavailable"
        elif not scoped and actor_owners > 0 and not consumer_ready:
            degraded_reason = "actor_consumer_unavailable"
        elif self._consecutive_failures:
            degraded_reason = self._last_error_code or "durable_routing_failure"

        if degraded_reason:
            status = DurableRoutingServiceStatus.DEGRADED
        elif self._started:
            status = DurableRoutingServiceStatus.RUNNING
        elif self._prepared:
            status = DurableRoutingServiceStatus.PREPARED
        else:
            status = DurableRoutingServiceStatus.STOPPED
        return DurableRoutingHealthSnapshot(
            status=status,
            prepared=self._prepared,
            started=self._started,
            adapters_ready=self._adapters_ready,
            actor_consumer_ready=consumer_ready,
            ready_for_actor_traffic=ready_for_actor,
            degraded_reason=degraded_reason,
            worker_id=self._worker_id,
            pending_job_count=pending_jobs,
            pending_delivery_count=pending_deliveries,
            active_actor_ownership_count=actor_owners,
            wake_debt_count=len(self._wake_debt),
            active_job_id=(
                self._active_job.routing_job_id if self._active_job is not None else ""
            ),
            active_delivery_id=(
                self._active_delivery.delivery_id
                if self._active_delivery is not None
                else ""
            ),
            last_scan_at=self._last_scan_at,
            last_success_at=self._last_success_at,
            last_error_at=self._last_error_at,
            last_error_code=self._last_error_code,
            last_error_message=self._last_error_message,
            consecutive_failures=self._consecutive_failures,
            processed_job_count=self._processed_job_count,
            relayed_delivery_count=self._relayed_delivery_count,
            fenced_request_scoped=scoped,
            fenced_scope_live=fenced_scope_live,
        )

    async def _run(self) -> None:
        try:
            while not self._closed:
                try:
                    self._wake_event.clear()
                    self._last_scan_at = self._clock()
                    worked = False
                    delivery_sink_available = self._delivery_sink_available()
                    can_process_jobs = (
                        delivery_sink_available
                        or self._repository.active_actor_ownership_count() == 0
                    )
                    if delivery_sink_available:
                        worked = await self._process_delivery_once(
                            wake_after_commit=True
                        )
                    if can_process_jobs:
                        worked = await self._process_job_once() or worked
                    await self._recover_wake_debt()
                    if worked:
                        continue
                    timeout = self._next_poll_timeout(
                        can_process_jobs=can_process_jobs,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._record_failure(exc)
                    logger.exception("durable_routing_iteration_failed")
                    timeout = self._failure_backoff_seconds()
                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=timeout)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        finally:
            self._release_interrupted_claims()

    async def _process_job_once(self) -> bool:
        claim = self._repository.claim_next_job(
            worker_id=self._worker_id,
            expected_fenced_request=self._fenced_request_scope,
        )
        if claim is None:
            return False
        self._active_job = claim
        try:
            self._require_fenced_scope_matches(
                profile_id=claim.envelope.profile_id,
                session_id=claim.envelope.session_id,
                ownership_generation=claim.envelope.ownership_generation,
                admission_fence_id=claim.envelope.admission_fence_id,
                admission_fence_generation=claim.envelope.admission_fence_generation,
                subject="routing job",
            )
            payload = IngressRoutingPayload.from_payload(claim.envelope.payload)
            adapter = self._adapter_resolver(payload.adapter_instance_id)
            if adapter is None:
                raise DurableRoutingReplayDeferred(
                    "adapter_instance_unavailable",
                    f"adapter instance {payload.adapter_instance_id!r} is unavailable",
                )
            if not self._adapters_ready:
                raise DurableRoutingReplayDeferred(
                    "adapter_not_ready",
                    "durable route replay is blocked before adapter readiness",
                )
            timeout = min(
                self._operation_timeout_seconds,
                self._repository.lease_seconds * 0.8,
            )
            async with asyncio.timeout(timeout):
                await self._replay(claim, adapter)
        except asyncio.CancelledError:
            self._release_job_for_shutdown(claim)
            raise
        except Exception as exc:
            self._record_failure(exc)
            self._retry_or_fail_job(claim, exc)
        else:
            self._processed_job_count += 1
            self._record_success()
        finally:
            self._active_job = None
        return True

    async def _process_delivery_once(self, *, wake_after_commit: bool) -> bool:
        if not self._delivery_sink_available():
            return False
        claim = self._repository.claim_next_delivery(
            worker_id=self._worker_id,
            expected_fenced_request=self._fenced_request_scope,
        )
        if claim is None:
            return False
        self._active_delivery = claim
        try:
            result = self._repository.relay_delivery(claim)
            self._require_fenced_scope_matches(
                profile_id=result.wake_request.key.profile_id,
                session_id=result.wake_request.key.session_id,
                ownership_generation=result.wake_request.ownership_generation,
                admission_fence_id=result.wake_request.admission_fence_id,
                admission_fence_generation=result.wake_request.admission_fence_generation,
                subject="route delivery",
            )
        except asyncio.CancelledError:
            self._release_delivery_for_shutdown(claim)
            raise
        except Exception as exc:
            self._record_failure(exc)
            self._retry_or_fail_delivery(claim, exc)
            self._active_delivery = None
            return True

        self._active_delivery = None
        self._relayed_delivery_count += 1
        self._record_success()
        if result.wake_request.has_admission_fence:
            if wake_after_commit and self._fenced_request_scope is None:
                await self._notify_fenced_mailbox_handoff(result)
            return True
        debt = PendingRouteWakeDebt(request=result.wake_request, event_id=result.event_id)
        if wake_after_commit:
            await self._wake_after_commit(debt)
        else:
            self._remember_wake_debt(debt)
        return True

    def _remember_wake_debt(self, debt: PendingRouteWakeDebt) -> None:
        """Keep one durable wake debt in the bounded local discovery window."""

        request = debt.request
        self._wake_debt.pop(request, None)
        self._wake_debt[request] = debt
        while len(self._wake_debt) > _LEGACY_WAKE_STATE_CAPACITY:
            self._wake_debt.popitem(last=False)

    def _drop_wake_debt(self, debt: PendingRouteWakeDebt) -> None:
        """Forget one local debt while leaving durable redrive evidence intact."""

        if self._wake_debt.get(debt.request) == debt:
            self._wake_debt.pop(debt.request, None)

    def _attemptable_legacy_wake_debts(
        self,
    ) -> tuple[PendingRouteWakeDebt, ...]:
        """Select a bounded page of unfenced legacy-compatible wake debt."""

        result: list[PendingRouteWakeDebt] = []
        for debt in self._wake_debt.values():
            if debt.request.has_admission_fence:
                continue
            result.append(debt)
            if len(result) >= _LEGACY_WAKE_DISCOVERY_LIMIT:
                break
        return tuple(result)

    async def _wake_after_commit(
        self,
        debt: PendingRouteWakeDebt,
        *,
        recover_on_failure: bool = True,
    ) -> None:
        """Wake one committed unfenced mailbox event through the legacy target."""

        request = debt.request
        if request.has_admission_fence:
            raise RuntimeError("fenced route debt must use mailbox handoff delivery")
        self._remember_wake_debt(debt)
        if self._closed:
            return
        target = self._actor_wake_target
        if target is None:
            return
        if not self._repository.is_pending_route_wake_debt(debt):
            self._drop_wake_debt(debt)
            return
        target_epoch = self._legacy_wake_target_epoch
        if (
            self._closed
            or target is not self._actor_wake_target
            or target_epoch != self._legacy_wake_target_epoch
        ):
            return
        try:
            result = target.wake(request.key)
            if inspect.isawaitable(result):
                await result
            if (
                not self._closed
                and target is self._actor_wake_target
                and target_epoch == self._legacy_wake_target_epoch
            ):
                self._drop_wake_debt(debt)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_failure(exc)
            logger.exception(
                format_log_event(
                    "durable_routing.actor_wake_failed",
                    profile_id=request.key.profile_id,
                    session_id=request.key.session_id,
                    ownership_generation=request.ownership_generation,
                )
            )
            if recover_on_failure:
                await self._recover_wake_debt()

    async def _notify_fenced_mailbox_handoff(
        self,
        result: RouteRelayResult,
    ) -> None:
        """Send an advisory hint for one exact durable fenced route mailbox.

        The source service does not inspect, claim, or settle the handoff. A
        missing or failing notifier leaves the sidecar as durable pull debt and
        must never fall back to a key-only wake.
        """

        mailbox_id = result.mailbox_id
        wake_request = result.wake_request
        notifier = self._mailbox_handoff_notifier
        if notifier is None:
            logger.debug(
                format_log_event(
                    "durable_routing.fenced_mailbox_handoff_deferred",
                    mailbox_id=mailbox_id,
                    profile_id=wake_request.key.profile_id,
                    session_id=wake_request.key.session_id,
                    ownership_generation=wake_request.ownership_generation,
                    admission_fence_id=wake_request.admission_fence_id,
                    admission_fence_generation=wake_request.admission_fence_generation,
                )
            )
            return
        try:
            notification = notifier.notify(mailbox_id)
            if inspect.isawaitable(notification):
                await notification
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                format_log_event(
                    "durable_routing.fenced_mailbox_handoff_notify_failed",
                    mailbox_id=mailbox_id,
                    profile_id=wake_request.key.profile_id,
                    session_id=wake_request.key.session_id,
                    ownership_generation=wake_request.ownership_generation,
                    admission_fence_id=wake_request.admission_fence_id,
                    admission_fence_generation=wake_request.admission_fence_generation,
                )
            )

    async def _recover_wake_debt(self, *, force: bool = False) -> None:
        """Serialize durable wake reconciliation across startup and polling."""

        async with self._wake_recovery_lock:
            await self._recover_wake_debt_locked(force=force)

    async def _recover_wake_debt_locked(self, *, force: bool) -> None:
        """Reconcile wake debt while one service pass owns recovery state."""

        if self._closed:
            return
        target = self._actor_wake_target
        if target is None or not self._legacy_actor_target_available():
            return
        try:
            self._wake_debt = OrderedDict(
                (request, debt)
                for request, debt in self._wake_debt.items()
                if not debt.request.has_admission_fence
                and self._repository.is_pending_route_wake_debt(debt)
            )
            self._legacy_broad_wake_fenced_debt = (
                self._repository.has_retained_fenced_mailbox_debt()
            )
            self._refresh_legacy_wake_debt(force=force)
        except Exception as exc:
            self._record_failure(exc)
            logger.exception("durable_routing_legacy_wake_fence_scan_failed")
            return
        if (
            not force
            and not self._wake_debt
            and not self._legacy_broad_wake_fenced_debt
        ):
            return
        target_epoch = self._legacy_wake_target_epoch
        if (
            self._closed
            or target is not self._actor_wake_target
            or target_epoch != self._legacy_wake_target_epoch
        ):
            return
        # A short-lived routing pass cannot prove that a target did not create
        # actors that outlive a recovery permit. It therefore never acquires or
        # delegates a broad-recovery permit; only exact, unfenced wake debt is
        # eligible for this legacy compatibility path.
        await self._wake_legacy_unfenced_debts(target, target_epoch)

    async def _wake_legacy_unfenced_debts(
        self,
        target: AgentMailboxWakeTarget,
        target_epoch: int,
    ) -> None:
        """Wake only locally known unfenced mailbox debt through a legacy target."""

        for debt in self._attemptable_legacy_wake_debts():
            if (
                self._closed
                or target is not self._actor_wake_target
                or target_epoch != self._legacy_wake_target_epoch
            ):
                self.wake()
                return
            await self._wake_after_commit(debt, recover_on_failure=False)

    def _refresh_legacy_wake_debt(self, *, force: bool) -> None:
        """Page exact unfenced debt without invoking broad recovery.

        A legacy target may receive only an individually identified unfenced
        mailbox wake. The routing service never invokes a target's broad
        recovery method, because it cannot retain a durable permit through any
        actor task that target might create.
        """

        if force:
            self._legacy_wake_discovery_cursor = None
            self._wake_debt.clear()
        cursor = self._legacy_wake_discovery_cursor
        debts = self._repository.pending_route_wake_debts(
            limit=_LEGACY_WAKE_DISCOVERY_LIMIT,
            after=cursor,
        )
        if not debts and cursor is not None:
            cursor = None
            debts = self._repository.pending_route_wake_debts(
                limit=_LEGACY_WAKE_DISCOVERY_LIMIT,
            )
        for debt in debts:
            if not debt.request.has_admission_fence:
                self._remember_wake_debt(debt)
        self._legacy_wake_discovery_cursor = (
            _route_wake_cursor_for_debt(debts[-1]) if debts else cursor
        )

    def _retry_or_fail_job(
        self,
        claim: ClaimedMessageRoutingJob,
        error: BaseException,
    ) -> None:
        retry_at = self._retry_at(claim.attempt_count)
        code = (
            error.code
            if isinstance(error, DurableRoutingReplayDeferred)
            else type(error).__name__
        )
        self._repository.retry_or_fail_job(
            claim,
            error_code=code,
            error_message=str(error),
            retry_at=retry_at,
        )

    def _retry_or_fail_delivery(
        self,
        claim: ClaimedAgentRouteDelivery,
        error: BaseException,
    ) -> None:
        self._repository.retry_or_fail_delivery(
            claim,
            error_code=type(error).__name__,
            error_message=str(error),
            retry_at=self._retry_at(claim.attempt_count),
        )

    def _retry_at(self, attempt_count: int) -> float | None:
        if attempt_count >= self._max_attempts:
            return None
        exponent = max(0, attempt_count - 1)
        try:
            delay = self._retry_base_seconds * (2.0**exponent)
        except OverflowError:
            delay = self._retry_max_seconds
        return self._clock() + min(delay, self._retry_max_seconds)

    def _next_poll_timeout(self, *, can_process_jobs: bool) -> float:
        deadlines = (
            [
                self._repository.next_job_available_at(
                    expected_fenced_request=self._fenced_request_scope,
                )
            ]
            if can_process_jobs
            else []
        )
        if self._delivery_sink_available():
            deadlines.append(
                self._repository.next_delivery_available_at(
                    expected_fenced_request=self._fenced_request_scope,
                )
            )
        now = self._clock()
        due = [deadline for deadline in deadlines if deadline is not None]
        if not due:
            return self._poll_interval_seconds
        return min(
            self._poll_interval_seconds,
            max(0.01, min(due) - now),
        )

    def _failure_backoff_seconds(self) -> float:
        exponent = max(0, self._consecutive_failures - 1)
        try:
            delay = self._retry_base_seconds * (2.0**exponent)
        except OverflowError:
            delay = self._retry_max_seconds
        return max(0.01, min(delay, self._retry_max_seconds))

    def _legacy_actor_target_available(self) -> bool:
        """Return whether the legacy key-based consumer can receive unfenced work."""

        target = self._actor_wake_target
        if target is None:
            return False
        accepting = getattr(target, "accepting", True)
        return bool(accepting)

    def _delivery_sink_available(self) -> bool:
        """Return whether a route delivery can make durable post-commit progress."""

        return (
            self._fenced_request_scope is not None
            or self._legacy_actor_target_available()
            or self._mailbox_handoff_notifier is not None
        )

    def _require_fenced_scope_matches(
        self,
        *,
        profile_id: str,
        session_id: str,
        ownership_generation: int,
        admission_fence_id: str,
        admission_fence_generation: int,
        subject: str,
    ) -> None:
        """Reject a repository result that escapes this service's exact scope."""

        scope = self._fenced_request_scope
        if scope is None:
            return
        if (
            profile_id != scope.key.profile_id
            or session_id != scope.key.session_id
            or ownership_generation != scope.ownership_generation
            or admission_fence_id != scope.admission_fence_id
            or admission_fence_generation != scope.admission_fence_generation
        ):
            raise RuntimeError(f"fenced durable routing {subject} escaped its request scope")

    def _fenced_handoff_notifier_required(self) -> bool:
        """Return whether fenced debt lacks even an advisory handoff hint sink."""

        return (
            self._mailbox_handoff_notifier is None
            and self._legacy_broad_wake_fenced_debt
        )

    def _release_job_for_shutdown(self, claim: ClaimedMessageRoutingJob) -> None:
        try:
            self._repository.retry_or_fail_job(
                claim,
                error_code="service_shutdown",
                error_message="durable routing service stopped during replay",
                retry_at=self._clock(),
            )
        except Exception:
            logger.exception("failed_to_release_routing_job_during_shutdown")

    def _release_delivery_for_shutdown(
        self,
        claim: ClaimedAgentRouteDelivery,
    ) -> None:
        try:
            self._repository.retry_or_fail_delivery(
                claim,
                error_code="service_shutdown",
                error_message="durable routing service stopped during relay",
                retry_at=self._clock(),
            )
        except Exception:
            logger.exception("failed_to_release_route_delivery_during_shutdown")

    def _release_interrupted_claims(self) -> None:
        job = self._active_job
        self._active_job = None
        if job is not None:
            self._release_job_for_shutdown(job)
        delivery = self._active_delivery
        self._active_delivery = None
        if delivery is not None:
            self._release_delivery_for_shutdown(delivery)

    def _record_success(self) -> None:
        self._last_success_at = self._clock()
        self._consecutive_failures = 0

    def _record_failure(self, error: BaseException) -> None:
        self._last_error_at = self._clock()
        self._last_error_code = (
            error.code
            if isinstance(error, DurableRoutingReplayDeferred)
            else type(error).__name__
        )
        self._last_error_message = str(error)[:1000]
        self._consecutive_failures += 1


class FencedDurableRoutingService(DurableRoutingService):
    """Relay ingress for one already-published fenced Actor target.

    This service is deliberately not part of ``ShinBot`` startup.  A caller
    must first recover and publish the matching target through a lifecycle that
    retains the same :class:`FencedMailboxWakeRequest`.  The service then
    replays only that request's routing jobs and persists only that request's
    mailbox sidecars.  It has no key-only target and no global notifier; the
    target's own pull supervisor remains responsible for handoff consumption.
    """

    def __init__(
        self,
        *,
        repository: DurableMessageRoutingRepository,
        replay: RoutingReplay,
        adapter_resolver: AdapterResolver,
        request: FencedMailboxWakeRequest,
        worker_id: str | None = None,
        poll_interval_seconds: float = 1.0,
        retry_base_seconds: float = 1.0,
        retry_max_seconds: float = 60.0,
        max_attempts: int = 5,
        operation_timeout_seconds: float = 20.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Create an inactive relay for one complete fenced request.

        Args:
            repository: Durable routing repository shared with the target.
            replay: Canonical ingress replay callback.
            adapter_resolver: Resolves only currently usable adapter instances.
            request: Exact active Actor v2 ownership and admission-fence scope.
            worker_id: Optional durable claim worker identity.
            poll_interval_seconds: Maximum wait between scoped recovery passes.
            retry_base_seconds: Base retry delay for route or relay failures.
            retry_max_seconds: Maximum retry delay for route or relay failures.
            max_attempts: Bounded retry count for one durable claim.
            operation_timeout_seconds: Bound for one replay callback.
            clock: Optional clock used by retry and health accounting.
        """

        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        if not request.has_admission_fence:
            raise ValueError("fenced durable routing requires an admission-fenced request")
        super().__init__(
            repository=repository,
            replay=replay,
            adapter_resolver=adapter_resolver,
            worker_id=worker_id,
            poll_interval_seconds=poll_interval_seconds,
            retry_base_seconds=retry_base_seconds,
            retry_max_seconds=retry_max_seconds,
            max_attempts=max_attempts,
            operation_timeout_seconds=operation_timeout_seconds,
            clock=clock,
            fenced_request_scope=request,
        )

    @property
    def request(self) -> FencedMailboxWakeRequest:
        """Return the immutable ownership request this relay is allowed to claim."""

        request = self.fenced_request_scope
        if request is None:
            raise RuntimeError("fenced durable routing service lost its request scope")
        return request


def _positive_finite(value: float, field_name: str) -> float:
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return normalized


def _nonnegative_finite(value: float, field_name: str) -> float:
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _route_wake_cursor_for_debt(debt: PendingRouteWakeDebt) -> RouteWakeCursor:
    """Return the event-versioned cursor emitted by durable discovery."""

    cursor = debt.cursor
    if cursor is None:
        raise RuntimeError("durable route wake debt must carry a keyset cursor")
    return cursor


__all__ = [
    "AgentMailboxWakeTarget",
    "DurableRoutingHealthSnapshot",
    "DurableRoutingService",
    "DurableRoutingServiceStatus",
    "FencedDurableRoutingService",
]
