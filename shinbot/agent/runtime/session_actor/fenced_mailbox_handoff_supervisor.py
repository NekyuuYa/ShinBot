"""Unmounted supervision for one fenced mailbox-handoff target incarnation.

This module owns only a target lease renewal loop and the corresponding exact
handoff redrive. It deliberately cannot acquire ownership, resume ingress,
publish another target, or advance an Actor v2 migration barrier.
"""

from __future__ import annotations

import asyncio
import math
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    supervised_backoff_seconds,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_target import (
    FencedMailboxHandoffTargetRetirement,
    FencedMailboxHandoffTargetState,
)
from shinbot.agent.runtime.session_actor.mailbox_handoff_dispatcher import (
    MailboxHandoffDispatchDisposition,
    MailboxHandoffDispatchPage,
    MailboxHandoffDispatchResult,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import FencedActorExecutionBinding
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffClaim,
    FencedMailboxHandoffReceipt,
    MailboxHandoffTarget,
)
from shinbot.persistence.repositories.actor_v2_mailbox_handoff import (
    MailboxHandoffDiscoveryCursor,
)
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:fenced-handoff-supervisor", color="yellow")

MAX_FENCED_MAILBOX_HANDOFF_SUPERVISOR_BATCH = 1_000


class FencedMailboxHandoffTargetPort(Protocol):
    """One inactive or active target controlled by this supervisor alone."""

    @property
    def state(self) -> FencedMailboxHandoffTargetState:
        """Return the local fail-closed target lifecycle state."""

        ...

    @property
    def target_identity(self) -> MailboxHandoffTarget:
        """Return the immutable dispatcher-facing target incarnation."""

        ...

    @property
    def execution_binding(self) -> FencedActorExecutionBinding:
        """Return the exact owner and target lease capability."""

        ...

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain shared by target-local work."""

        ...

    async def activate(self) -> None:
        """Start target-local workers without binding the dispatcher."""

        ...

    async def renew_target_lease(
        self,
        *,
        ttl_seconds: float,
    ) -> FencedActorExecutionBinding:
        """Renew the exact target publication capability."""

        ...

    async def wake_handoff(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        """Consume one complete target-bound durable handoff claim."""

        ...

    async def unpublish(self) -> None:
        """Stop accepting further dispatcher claims."""

        ...

    async def retire(
        self,
        *,
        quiescence_timeout_seconds: float | None = None,
    ) -> FencedMailboxHandoffTargetRetirement:
        """Stop local work and release publication only after quiescence."""

        ...


class FencedMailboxHandoffDispatcherPort(Protocol):
    """Exact pull dispatcher supervised for one target incarnation."""

    @property
    def target_bound(self) -> bool:
        """Return whether any target is currently bound."""

        ...

    @property
    def bound_target_identity(self) -> MailboxHandoffTarget | None:
        """Return the currently bound target identity without its port."""

        ...

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain used for sidecar discovery and claims."""

        ...

    @property
    def target_timeout_seconds(self) -> float:
        """Return the maximum duration of one target handoff attempt."""

        ...

    def bind_target(
        self,
        target: FencedMailboxHandoffTargetPort,
        *,
        target_identity: MailboxHandoffTarget,
    ) -> int:
        """Bind exactly one target incarnation for subsequent dispatches."""

        ...

    def unbind_target(self) -> None:
        """Remove the target before its local workers are retired."""

        ...

    async def dispatch_pending(
        self,
        *,
        limit: int,
        after: MailboxHandoffDiscoveryCursor | None,
        profile_id: str | None,
        session_id: str | None,
        expected_request: FencedMailboxWakeRequest,
    ) -> MailboxHandoffDispatchPage:
        """Redrive one exact owner-incarnation page of durable handoffs."""

        ...


class FencedMailboxHandoffSupervisorState(StrEnum):
    """Local lifecycle state for one explicitly composed target supervisor."""

    NEW = "new"
    ACTIVE = "active"
    STOPPING = "stopping"
    BLOCKED = "blocked"
    STOPPED = "stopped"


@dataclass(slots=True, frozen=True)
class FencedMailboxHandoffSupervisorPass:
    """One successful lease-renewal and exact handoff-dispatch observation."""

    target: MailboxHandoffTarget
    lease_expires_at: float
    dispatch_page: MailboxHandoffDispatchPage

    def __post_init__(self) -> None:
        """Keep the outward pass result token-free and fully typed."""

        if not isinstance(self.target, MailboxHandoffTarget):
            raise TypeError("target must be a MailboxHandoffTarget")
        expires_at = _positive_finite(self.lease_expires_at, "lease_expires_at")
        if not isinstance(self.dispatch_page, MailboxHandoffDispatchPage):
            raise TypeError("dispatch_page must be a MailboxHandoffDispatchPage")
        object.__setattr__(self, "lease_expires_at", expires_at)

    @property
    def failed_results(self) -> tuple[MailboxHandoffDispatchResult, ...]:
        """Return only non-terminal local dispatch failures for diagnostics."""

        return tuple(
            result
            for result in self.dispatch_page.results
            if result.disposition is MailboxHandoffDispatchDisposition.FAILED
        )


@dataclass(slots=True, frozen=True)
class FencedMailboxHandoffSupervisorShutdown:
    """Ordered shutdown result without exposing target lease capabilities."""

    state: FencedMailboxHandoffSupervisorState
    target_state: FencedMailboxHandoffTargetState
    retirement: FencedMailboxHandoffTargetRetirement | None = None
    error: str = ""

    def __post_init__(self) -> None:
        """Normalize bounded shutdown diagnostics and typed terminal state."""

        state = FencedMailboxHandoffSupervisorState(self.state)
        target_state = FencedMailboxHandoffTargetState(self.target_state)
        if self.retirement is not None and not isinstance(
            self.retirement,
            FencedMailboxHandoffTargetRetirement,
        ):
            raise TypeError("retirement must be FencedMailboxHandoffTargetRetirement or None")
        if state is FencedMailboxHandoffSupervisorState.STOPPED and (
            self.retirement is None or not self.retirement.target_lease_released
        ):
            raise ValueError("stopped supervisor requires a released target retirement")
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "target_state", target_state)
        object.__setattr__(self, "error", str(self.error or "").strip()[:500])


@dataclass(slots=True, frozen=True)
class FencedMailboxHandoffSupervisorSnapshot:
    """Read-only supervision diagnostics for one target incarnation."""

    state: FencedMailboxHandoffSupervisorState
    target: MailboxHandoffTarget
    target_state: FencedMailboxHandoffTargetState
    target_bound: bool
    binding_matches: bool
    persistence_domain_matches: bool
    health: RuntimeServiceHealthSnapshot
    last_pass: FencedMailboxHandoffSupervisorPass | None = None
    last_shutdown: FencedMailboxHandoffSupervisorShutdown | None = None

    def __post_init__(self) -> None:
        """Reject diagnostic snapshots that lose their immutable target identity."""

        if not isinstance(self.state, FencedMailboxHandoffSupervisorState):
            raise TypeError("state must be a FencedMailboxHandoffSupervisorState")
        if not isinstance(self.target, MailboxHandoffTarget):
            raise TypeError("target must be a MailboxHandoffTarget")
        if not isinstance(self.target_state, FencedMailboxHandoffTargetState):
            raise TypeError("target_state must be a FencedMailboxHandoffTargetState")
        if not isinstance(self.target_bound, bool):
            raise TypeError("target_bound must be a bool")
        if not isinstance(self.binding_matches, bool):
            raise TypeError("binding_matches must be a bool")
        if not isinstance(self.persistence_domain_matches, bool):
            raise TypeError("persistence_domain_matches must be a bool")
        if not isinstance(self.health, RuntimeServiceHealthSnapshot):
            raise TypeError("health must be a RuntimeServiceHealthSnapshot")
        if self.last_pass is not None and not isinstance(
            self.last_pass,
            FencedMailboxHandoffSupervisorPass,
        ):
            raise TypeError("last_pass must be FencedMailboxHandoffSupervisorPass or None")
        if self.last_shutdown is not None and not isinstance(
            self.last_shutdown,
            FencedMailboxHandoffSupervisorShutdown,
        ):
            raise TypeError(
                "last_shutdown must be FencedMailboxHandoffSupervisorShutdown or None"
            )


class FencedMailboxHandoffSupervisorError(RuntimeError):
    """Base error for a misconfigured or failed target supervision boundary."""


class FencedMailboxHandoffDispatchError(FencedMailboxHandoffSupervisorError):
    """Aggregate exact durable handoffs that failed one local dispatch pass."""

    def __init__(self, results: tuple[MailboxHandoffDispatchResult, ...]) -> None:
        """Expose only mailbox ids and stable dispatcher dispositions."""

        if not results or any(
            result.disposition is not MailboxHandoffDispatchDisposition.FAILED
            for result in results
        ):
            raise ValueError("dispatch failure requires one or more failed results")
        self.mailbox_ids = tuple(sorted(result.mailbox_id for result in results))
        super().__init__(
            "fenced mailbox handoff dispatch failed for: "
            + ", ".join(str(mailbox_id) for mailbox_id in self.mailbox_ids)
        )


class FencedMailboxHandoffSupervisor:
    """Renew and redrive exactly one already-published fenced target.

    The supervisor is intentionally terminal: after shutdown or a critical
    lease/binding failure, callers must compose a new target incarnation rather
    than reviving this object. That prevents a stale local worker from being
    silently rebound to a later ownership generation.
    """

    def __init__(
        self,
        *,
        target: FencedMailboxHandoffTargetPort,
        dispatcher: FencedMailboxHandoffDispatcherPort,
        tick_interval_seconds: float = 5.0,
        target_lease_ttl_seconds: float = 60.0,
        dispatch_limit: int = 1,
        quiescence_timeout_seconds: float | None = 30.0,
        runtime_id: str | None = None,
    ) -> None:
        """Bind one new target and an unbound same-domain dispatcher.

        Args:
            target: Inactive target holding one acquired target lease.
            dispatcher: Unbound durable handoff dispatcher for the same domain.
            tick_interval_seconds: Delay between successful supervision passes.
            target_lease_ttl_seconds: Renewal duration, longer than one worst
                case bounded dispatch pass plus the next polling delay.
            dispatch_limit: Maximum exact handoffs dispatched per pass.
            quiescence_timeout_seconds: Bound passed to target retirement.
            runtime_id: Process-local diagnostic identity.
        """

        _require_target_port(target)
        _require_dispatcher_port(dispatcher)
        if target.state is not FencedMailboxHandoffTargetState.NEW:
            raise ValueError("fenced handoff supervisor requires a new target")
        if dispatcher.target_bound:
            raise ValueError("fenced handoff supervisor requires an unbound dispatcher")
        if target.persistence_domain is not dispatcher.persistence_domain:
            raise ValueError("target and dispatcher must share one persistence domain")
        binding = target.execution_binding
        if not isinstance(binding, FencedActorExecutionBinding):
            raise TypeError("target must expose a FencedActorExecutionBinding")
        target_identity = target.target_identity
        if binding.target_lease.lease.target != target_identity:
            raise ValueError("target identity differs from its execution binding")
        self._tick_interval_seconds = _positive_finite(
            tick_interval_seconds,
            "tick_interval_seconds",
        )
        self._target_lease_ttl_seconds = _positive_finite(
            target_lease_ttl_seconds,
            "target_lease_ttl_seconds",
        )
        if (
            isinstance(dispatch_limit, bool)
            or not isinstance(dispatch_limit, int)
            or not 1 <= dispatch_limit <= MAX_FENCED_MAILBOX_HANDOFF_SUPERVISOR_BATCH
        ):
            raise ValueError(
                "dispatch_limit must be between 1 and "
                f"{MAX_FENCED_MAILBOX_HANDOFF_SUPERVISOR_BATCH}"
            )
        target_timeout_seconds = _positive_finite(
            dispatcher.target_timeout_seconds,
            "dispatcher.target_timeout_seconds",
        )
        maximum_dispatch_pass_seconds = target_timeout_seconds * dispatch_limit
        if self._target_lease_ttl_seconds <= (
            maximum_dispatch_pass_seconds + self._tick_interval_seconds
        ):
            raise ValueError(
                "target_lease_ttl_seconds must exceed the bounded dispatch pass "
                "plus tick interval"
            )
        self._quiescence_timeout_seconds = _optional_nonnegative_finite(
            quiescence_timeout_seconds,
            "quiescence_timeout_seconds",
        )
        self._target = target
        self._dispatcher = dispatcher
        self._request = binding.request
        self._target_identity = target_identity
        self._dispatch_limit = dispatch_limit
        self._runtime_id = str(
            runtime_id or f"fenced-handoff-supervisor:{uuid.uuid4().hex}"
        ).strip()
        if not self._runtime_id:
            raise ValueError("runtime_id must not be empty")
        self._state = FencedMailboxHandoffSupervisorState.NEW
        self._health = RuntimeServiceHealth("fenced_mailbox_handoff_supervisor")
        self._lifecycle_lock = asyncio.Lock()
        self._run_lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._lifecycle_epoch = 0
        self._cursor: MailboxHandoffDiscoveryCursor | None = None
        self._last_pass: FencedMailboxHandoffSupervisorPass | None = None
        self._last_shutdown: FencedMailboxHandoffSupervisorShutdown | None = None

    @property
    def runtime_id(self) -> str:
        """Return the local diagnostic identity for this supervisor instance."""

        return self._runtime_id

    @property
    def request(self) -> FencedMailboxWakeRequest:
        """Return the immutable owner request supervised by this instance."""

        return self._request

    @property
    def target_identity(self) -> MailboxHandoffTarget:
        """Return the one target incarnation this supervisor may bind."""

        return self._target_identity

    @property
    def persistence_domain(self) -> object:
        """Return the target domain while retaining dispatcher-match diagnostics."""

        return self._target.persistence_domain

    @property
    def snapshot(self) -> FencedMailboxHandoffSupervisorSnapshot:
        """Return token-free local lifecycle, binding, and health diagnostics."""

        binding_matches = (
            self._dispatcher.bound_target_identity == self._target_identity
            if self._dispatcher.target_bound
            else False
        )
        return FencedMailboxHandoffSupervisorSnapshot(
            state=self._state,
            target=self._target_identity,
            target_state=self._target.state,
            target_bound=self._dispatcher.target_bound,
            binding_matches=binding_matches,
            persistence_domain_matches=(
                self._target.persistence_domain is self._dispatcher.persistence_domain
            ),
            health=self._health.snapshot(),
            last_pass=self._last_pass,
            last_shutdown=self._last_shutdown,
        )

    def health_snapshot(self) -> RuntimeServiceHealthSnapshot:
        """Return the operator-visible health status for this local supervisor."""

        return self._health.snapshot()

    async def start(self) -> FencedMailboxHandoffSupervisorSnapshot:
        """Activate, renew, and bind the exact target before polling starts."""

        async with self._lifecycle_lock:
            if self._state is FencedMailboxHandoffSupervisorState.ACTIVE:
                return self.snapshot
            if self._state is not FencedMailboxHandoffSupervisorState.NEW:
                raise FencedMailboxHandoffSupervisorError(
                    "a stopped or blocked handoff supervisor cannot be restarted"
                )
            try:
                await self._target.activate()
                await self._renew_and_validate()
                self._dispatcher.bind_target(
                    self._target,
                    target_identity=self._target_identity,
                )
                if self._dispatcher.bound_target_identity != self._target_identity:
                    raise FencedMailboxHandoffSupervisorError(
                        "dispatcher did not retain the exact target binding"
                    )
            except asyncio.CancelledError:
                self._state = FencedMailboxHandoffSupervisorState.BLOCKED
                await self._stop_target_locked()
                raise
            except Exception as exc:
                self._health.failed(exc)
                self._state = FencedMailboxHandoffSupervisorState.BLOCKED
                await self._stop_target_locked()
                raise
            self._lifecycle_epoch += 1
            self._state = FencedMailboxHandoffSupervisorState.ACTIVE
            self._health.start()
            self._task = asyncio.create_task(
                self._run_loop(),
                name=f"agent-fenced-handoff-supervisor:{self._runtime_id}",
            )
            return self.snapshot

    async def run_once(self) -> FencedMailboxHandoffSupervisorPass:
        """Renew publication and dispatch one exact durable handoff page."""

        lifecycle_epoch = self._lifecycle_epoch
        async with self._run_lock:
            if self._state is not FencedMailboxHandoffSupervisorState.ACTIVE:
                raise FencedMailboxHandoffSupervisorError(
                    "fenced handoff supervisor is not active"
                )
            if lifecycle_epoch != self._lifecycle_epoch:
                raise FencedMailboxHandoffSupervisorError(
                    "fenced handoff supervisor lifecycle changed before dispatch"
                )
            self._health.scan_started()
            try:
                binding = await self._renew_and_validate()
                if lifecycle_epoch != self._lifecycle_epoch:
                    raise FencedMailboxHandoffSupervisorError(
                        "fenced handoff supervisor stopped during lease renewal"
                    )
                if self._dispatcher.bound_target_identity != self._target_identity:
                    raise FencedMailboxHandoffSupervisorError(
                        "dispatcher target binding changed during supervision"
                    )
                page = await self._dispatcher.dispatch_pending(
                    limit=self._dispatch_limit,
                    after=self._cursor,
                    profile_id=self._request.key.profile_id,
                    session_id=self._request.key.session_id,
                    expected_request=self._request,
                )
                if not isinstance(page, MailboxHandoffDispatchPage):
                    raise TypeError("dispatcher returned an invalid handoff dispatch page")
                if lifecycle_epoch != self._lifecycle_epoch:
                    raise FencedMailboxHandoffSupervisorError(
                        "fenced handoff supervisor stopped during dispatch"
                    )
                if self._target.state is not FencedMailboxHandoffTargetState.ACTIVE:
                    raise FencedMailboxHandoffSupervisorError(
                        "target became unavailable during handoff dispatch"
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._health.failed(exc)
                await self._fail_closed(exc)
                raise

            self._cursor = page.next_cursor if page.has_more else None
            result = FencedMailboxHandoffSupervisorPass(
                target=self._target_identity,
                lease_expires_at=binding.target_lease.lease.expires_at,
                dispatch_page=page,
            )
            self._last_pass = result
            if result.failed_results:
                self._health.failed(FencedMailboxHandoffDispatchError(result.failed_results))
            else:
                self._health.succeeded()
            return result

    async def shutdown(self) -> FencedMailboxHandoffSupervisorShutdown:
        """Stop in strict unbind, unpublish, then retire order.

        This path never resumes ingress, changes ownership, or settles a
        sidecar. A retirement failure remains operator-visible and leaves the
        supervisor blocked for a later explicit shutdown retry.
        """

        async with self._shutdown_lock:
            return await self._shutdown_once()

    async def _shutdown_once(self) -> FencedMailboxHandoffSupervisorShutdown:
        """Run one serialized stop attempt after acquiring the shutdown lock."""

        async with self._lifecycle_lock:
            if self._state is FencedMailboxHandoffSupervisorState.STOPPED:
                if self._last_shutdown is None:
                    raise FencedMailboxHandoffSupervisorError(
                        "stopped handoff supervisor lacks a shutdown result"
                    )
                return self._last_shutdown
            self._state = FencedMailboxHandoffSupervisorState.STOPPING
            self._lifecycle_epoch += 1
            task = self._task
            self._task = None
            self._dispatcher.unbind_target()

        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        async with self._lifecycle_lock:
            retirement, error = await self._unpublish_then_retire_locked()
            state = (
                FencedMailboxHandoffSupervisorState.STOPPED
                if retirement is not None and retirement.target_lease_released
                else FencedMailboxHandoffSupervisorState.BLOCKED
            )
            self._state = state
            if state is FencedMailboxHandoffSupervisorState.STOPPED:
                self._health.stop()
            else:
                self._health.failed(
                    FencedMailboxHandoffSupervisorError(
                        error or "target retirement did not release its lease"
                    )
                )
            self._last_shutdown = FencedMailboxHandoffSupervisorShutdown(
                state=state,
                target_state=self._target.state,
                retirement=retirement,
                error=error,
            )
            return self._last_shutdown

    async def _run_loop(self) -> None:
        """Run bounded exact dispatch passes until a terminal lifecycle change."""

        delay = 0.0
        try:
            while self._state is FencedMailboxHandoffSupervisorState.ACTIVE:
                if delay > 0:
                    await asyncio.sleep(delay)
                try:
                    result = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        format_log_event(
                            "agent.fenced_handoff_supervisor.iteration_failed",
                            runtime_id=self._runtime_id,
                            target_id=self._target_identity.target_id,
                            target_incarnation_id=self._target_identity.incarnation_id,
                            error_code=type(exc).__name__,
                            consecutive_failures=(
                                self._health.snapshot().consecutive_failures
                            ),
                        )
                    )
                    if self._state is not FencedMailboxHandoffSupervisorState.ACTIVE:
                        return
                    delay = supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=self._health.snapshot().consecutive_failures,
                    )
                    continue
                delay = (
                    supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=self._health.snapshot().consecutive_failures,
                    )
                    if result.failed_results
                    else self._tick_interval_seconds
                )
        finally:
            if self._task is asyncio.current_task():
                self._task = None

    async def _renew_and_validate(self) -> FencedActorExecutionBinding:
        """Renew only the original target authority and prove it stayed exact."""

        binding = await self._target.renew_target_lease(
            ttl_seconds=self._target_lease_ttl_seconds,
        )
        if not isinstance(binding, FencedActorExecutionBinding):
            raise TypeError("target renewal returned an invalid execution binding")
        if binding.request != self._request:
            raise FencedMailboxHandoffSupervisorError(
                "target renewal changed the fenced wake request"
            )
        if binding.target_lease.lease.target != self._target_identity:
            raise FencedMailboxHandoffSupervisorError(
                "target renewal changed the target incarnation"
            )
        if self._target.execution_binding != binding:
            raise FencedMailboxHandoffSupervisorError(
                "target did not retain its renewed execution binding"
            )
        if self._target.state is not FencedMailboxHandoffTargetState.ACTIVE:
            raise FencedMailboxHandoffSupervisorError(
                "target is not active after lease renewal"
            )
        return binding

    async def _fail_closed(self, error: BaseException) -> None:
        """Unbind and retire after a lease, binding, or dispatch-boundary error."""

        async with self._lifecycle_lock:
            if self._state in {
                FencedMailboxHandoffSupervisorState.STOPPING,
                FencedMailboxHandoffSupervisorState.STOPPED,
            }:
                return
            self._state = FencedMailboxHandoffSupervisorState.BLOCKED
            self._lifecycle_epoch += 1
            self._dispatcher.unbind_target()
            retirement, retirement_error = await self._unpublish_then_retire_locked()
            self._last_shutdown = FencedMailboxHandoffSupervisorShutdown(
                state=(
                    FencedMailboxHandoffSupervisorState.STOPPED
                    if retirement is not None and retirement.target_lease_released
                    else FencedMailboxHandoffSupervisorState.BLOCKED
                ),
                target_state=self._target.state,
                retirement=retirement,
                error=retirement_error or _error_text(error),
            )
            if retirement is not None and retirement.target_lease_released:
                self._state = FencedMailboxHandoffSupervisorState.STOPPED

    async def _stop_target_locked(self) -> None:
        """Run the strict stop sequence while lifecycle ownership is held."""

        self._dispatcher.unbind_target()
        retirement, error = await self._unpublish_then_retire_locked()
        state = (
            FencedMailboxHandoffSupervisorState.STOPPED
            if retirement is not None and retirement.target_lease_released
            else FencedMailboxHandoffSupervisorState.BLOCKED
        )
        self._state = state
        self._last_shutdown = FencedMailboxHandoffSupervisorShutdown(
            state=state,
            target_state=self._target.state,
            retirement=retirement,
            error=error,
        )

    async def _unpublish_then_retire_locked(
        self,
    ) -> tuple[FencedMailboxHandoffTargetRetirement | None, str]:
        """Unpublish before retirement and retain a bounded failure explanation."""

        try:
            await self._target.unpublish()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return None, "target unpublish failed: " + _error_text(exc)
        try:
            retirement = await self._target.retire(
                quiescence_timeout_seconds=self._quiescence_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return None, "target retirement failed: " + _error_text(exc)
        if not isinstance(retirement, FencedMailboxHandoffTargetRetirement):
            return None, "target retirement returned an invalid result"
        if not retirement.target_lease_released:
            return retirement, retirement.error or "target lease remains published"
        return retirement, ""


def _require_target_port(target: object) -> None:
    """Validate the narrow target capabilities before lifecycle composition."""

    required_properties = (
        "state",
        "target_identity",
        "execution_binding",
        "persistence_domain",
    )
    required_methods = (
        "activate",
        "renew_target_lease",
        "wake_handoff",
        "unpublish",
        "retire",
    )
    if any(not hasattr(target, attribute) for attribute in required_properties) or any(
        not callable(getattr(target, method_name, None)) for method_name in required_methods
    ):
        raise TypeError("target must implement the fenced mailbox handoff target port")


def _require_dispatcher_port(dispatcher: object) -> None:
    """Validate the bounded dispatcher capabilities required by supervision."""

    required_properties = (
        "target_bound",
        "bound_target_identity",
        "persistence_domain",
        "target_timeout_seconds",
    )
    required_methods = ("bind_target", "unbind_target", "dispatch_pending")
    if any(not hasattr(dispatcher, attribute) for attribute in required_properties) or any(
        not callable(getattr(dispatcher, method_name, None))
        for method_name in required_methods
    ):
        raise TypeError("dispatcher must implement the fenced mailbox handoff dispatcher port")


def _positive_finite(value: object, field_name: str) -> float:
    """Normalize one finite positive duration without accepting booleans."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and positive")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return normalized


def _optional_nonnegative_finite(value: object, field_name: str) -> float | None:
    """Normalize an optional finite quiescence bound."""

    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and non-negative")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _error_text(error: BaseException) -> str:
    """Return a bounded operator-visible message without retaining traceback state."""

    return (str(error).strip() or type(error).__name__)[:500]


__all__ = [
    "FencedMailboxHandoffDispatcherPort",
    "FencedMailboxHandoffDispatchError",
    "FencedMailboxHandoffSupervisor",
    "FencedMailboxHandoffSupervisorError",
    "FencedMailboxHandoffSupervisorPass",
    "FencedMailboxHandoffSupervisorShutdown",
    "FencedMailboxHandoffSupervisorSnapshot",
    "FencedMailboxHandoffSupervisorState",
    "FencedMailboxHandoffTargetPort",
    "MAX_FENCED_MAILBOX_HANDOFF_SUPERVISOR_BATCH",
]
