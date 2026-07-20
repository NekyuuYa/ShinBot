"""Restart lifecycle for one already-owned fenced Actor v2 session.

This controller deliberately resumes only durable history that already belongs
to one exact Actor owner request. It neither acquires ownership nor exposes an
ingress, timer, scanner, or management surface. A broader cutover controller
must compose those concerns separately around this lifecycle.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_supervisor import (
    FencedMailboxHandoffSupervisorShutdown,
    FencedMailboxHandoffSupervisorSnapshot,
    FencedMailboxHandoffSupervisorState,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_target import (
    FencedMailboxHandoffTargetHistoryRecovery,
    FencedMailboxHandoffTargetState,
)
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.fenced_wake_target_lease import FencedActorExecutionBinding
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget


class FencedNativeHistoryTargetPort(Protocol):
    """One target-local recovery boundary owned by this lifecycle."""

    @property
    def state(self) -> FencedMailboxHandoffTargetState:
        """Return the local target lifecycle state."""

        ...

    @property
    def execution_binding(self) -> FencedActorExecutionBinding:
        """Return the exact owner and target-lease capability."""

        ...

    @property
    def target_identity(self) -> MailboxHandoffTarget:
        """Return the dispatcher-facing target incarnation."""

        ...

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain shared by target-local components."""

        ...

    async def recover_native_history(self) -> FencedMailboxHandoffTargetHistoryRecovery:
        """Recover one exact target's pre-existing mailbox and effect history."""

        ...


class FencedNativeHistorySupervisorPort(Protocol):
    """One target supervisor that owns binding and ordered retirement."""

    @property
    def request(self) -> FencedMailboxWakeRequest:
        """Return the immutable owner request supervised by this instance."""

        ...

    @property
    def target_identity(self) -> MailboxHandoffTarget:
        """Return the only target incarnation this supervisor can bind."""

        ...

    @property
    def persistence_domain(self) -> object:
        """Return the target's durable persistence domain."""

        ...

    @property
    def snapshot(self) -> FencedMailboxHandoffSupervisorSnapshot:
        """Return token-free supervision diagnostics."""

        ...

    async def start(self) -> FencedMailboxHandoffSupervisorSnapshot:
        """Activate and bind the composed target."""

        ...

    async def shutdown(self) -> FencedMailboxHandoffSupervisorShutdown:
        """Unbind and retire the target before releasing its lease."""

        ...


class FencedNativeHistoryLifecycleState(StrEnum):
    """Local state of one non-reusable fenced native-history lifetime."""

    READY = "ready"
    ACTIVE = "active"
    FAILED = "failed"
    CLOSED = "closed"


@dataclass(slots=True, frozen=True)
class FencedNativeHistoryLifecycleSnapshot:
    """Token-free lifecycle diagnostics for one target incarnation."""

    state: FencedNativeHistoryLifecycleState
    request: FencedMailboxWakeRequest
    target: MailboxHandoffTarget
    target_state: FencedMailboxHandoffTargetState
    supervisor: FencedMailboxHandoffSupervisorSnapshot
    recovery: FencedMailboxHandoffTargetHistoryRecovery | None
    shutdown: FencedMailboxHandoffSupervisorShutdown | None
    persistence_domain_matches: bool
    cleanup_failed: bool

    def __post_init__(self) -> None:
        """Require a complete, typed diagnostic view."""

        if not isinstance(self.state, FencedNativeHistoryLifecycleState):
            raise TypeError("state must be a FencedNativeHistoryLifecycleState")
        if not isinstance(self.request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        if not isinstance(self.target, MailboxHandoffTarget):
            raise TypeError("target must be a MailboxHandoffTarget")
        if not isinstance(self.target_state, FencedMailboxHandoffTargetState):
            raise TypeError("target_state must be a FencedMailboxHandoffTargetState")
        if not isinstance(self.supervisor, FencedMailboxHandoffSupervisorSnapshot):
            raise TypeError("supervisor must be a FencedMailboxHandoffSupervisorSnapshot")
        if self.recovery is not None and not isinstance(
            self.recovery,
            FencedMailboxHandoffTargetHistoryRecovery,
        ):
            raise TypeError("recovery must be FencedMailboxHandoffTargetHistoryRecovery or None")
        if self.shutdown is not None and not isinstance(
            self.shutdown,
            FencedMailboxHandoffSupervisorShutdown,
        ):
            raise TypeError("shutdown must be FencedMailboxHandoffSupervisorShutdown or None")
        if not isinstance(self.persistence_domain_matches, bool):
            raise TypeError("persistence_domain_matches must be a bool")
        if not isinstance(self.cleanup_failed, bool):
            raise TypeError("cleanup_failed must be a bool")


class FencedNativeHistoryLifecycleError(RuntimeError):
    """Raised when one native-history target cannot retain its stop proof."""


class FencedNativeHistoryLifecycleController:
    """Recover and supervise one current Actor owner through proven shutdown.

    Recovery starts before dispatcher publication so a previous process's
    expired actor mailbox claim is released under the replacement target lease.
    The target supervisor then owns lease renewal, exact sidecar pull delivery,
    and ordered retirement. The controller is terminal after shutdown or a
    failed cleanup; callers must compose a new target incarnation to retry.
    """

    def __init__(
        self,
        *,
        target: FencedNativeHistoryTargetPort,
        supervisor: FencedNativeHistorySupervisorPort,
    ) -> None:
        """Bind one new target and its exact same-request supervisor.

        Raises:
            ValueError: If either component is not new, or their request,
                target identity, or persistence domain differs.
            TypeError: If a component lacks the narrow lifecycle port.
        """

        _require_target_port(target)
        _require_supervisor_port(supervisor)
        target_binding = target.execution_binding
        if not isinstance(target_binding, FencedActorExecutionBinding):
            raise TypeError("target must expose a FencedActorExecutionBinding")
        if target.state is not FencedMailboxHandoffTargetState.NEW:
            raise ValueError("native-history lifecycle requires a new target")
        supervisor_snapshot = supervisor.snapshot
        if not isinstance(supervisor_snapshot, FencedMailboxHandoffSupervisorSnapshot):
            raise TypeError("supervisor must return a FencedMailboxHandoffSupervisorSnapshot")
        if supervisor_snapshot.state is not FencedMailboxHandoffSupervisorState.NEW:
            raise ValueError("native-history lifecycle requires a new supervisor")
        if supervisor.request != target_binding.request:
            raise ValueError("target and supervisor must retain the same fenced request")
        if supervisor.target_identity != target.target_identity:
            raise ValueError("target and supervisor must retain the same target identity")
        if supervisor.persistence_domain is not target.persistence_domain:
            raise ValueError("target and supervisor must share one persistence domain")
        if not supervisor_snapshot.persistence_domain_matches:
            raise ValueError("supervisor dispatcher does not share the target persistence domain")

        self._target = target
        self._supervisor = supervisor
        self._request = target_binding.request
        self._target_identity = target.target_identity
        self._persistence_domain = target.persistence_domain
        self._lifecycle_lock = asyncio.Lock()
        self._recovery: FencedMailboxHandoffTargetHistoryRecovery | None = None
        self._shutdown: FencedMailboxHandoffSupervisorShutdown | None = None
        self._active = False
        self._closed = False
        self._cleanup_failed = False

    @property
    def snapshot(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Return a token-free current diagnostic snapshot."""

        supervisor_snapshot = self._supervisor.snapshot
        persistence_domain_matches = self._persistence_domain_matches(supervisor_snapshot)
        if self._closed:
            state = FencedNativeHistoryLifecycleState.CLOSED
        elif (
            self._active
            and self._target.state is FencedMailboxHandoffTargetState.ACTIVE
            and supervisor_snapshot.state is FencedMailboxHandoffSupervisorState.ACTIVE
            and supervisor_snapshot.target_bound
            and supervisor_snapshot.binding_matches
            and persistence_domain_matches
            and not self._cleanup_failed
        ):
            state = FencedNativeHistoryLifecycleState.ACTIVE
        elif (
            self._cleanup_failed
            or self._active
            or self._recovery is not None
            or self._target.state is not FencedMailboxHandoffTargetState.NEW
            or supervisor_snapshot.state is not FencedMailboxHandoffSupervisorState.NEW
            or not persistence_domain_matches
        ):
            state = FencedNativeHistoryLifecycleState.FAILED
        else:
            state = FencedNativeHistoryLifecycleState.READY
        return FencedNativeHistoryLifecycleSnapshot(
            state=state,
            request=self._request,
            target=self._target_identity,
            target_state=self._target.state,
            supervisor=supervisor_snapshot,
            recovery=self._recovery,
            shutdown=self._shutdown,
            persistence_domain_matches=persistence_domain_matches,
            cleanup_failed=self._cleanup_failed,
        )

    @property
    def persistence_domain(self) -> object:
        """Return the immutable durable domain shared by this target lifecycle."""

        return self._persistence_domain

    async def activate(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Recover native history, then publish exactly one target supervisor."""

        async with self._lifecycle_lock:
            if self._closed:
                raise FencedNativeHistoryLifecycleError(
                    "a closed native-history lifecycle cannot activate"
                )
            if self._cleanup_failed:
                raise FencedNativeHistoryLifecycleError(
                    "native-history cleanup failed; only shutdown may retry it"
                )
            if self._active:
                return await self._verify_active_locked()
            if not self._pre_activation_matches():
                await self._terminate()
                raise FencedNativeHistoryLifecycleError(
                    "native-history target or supervisor changed before startup"
                )
            try:
                recovery = await self._target.recover_native_history()
                if (
                    recovery.actor_wake.request != self._request
                    or recovery.actor_wake.disposition is not FencedMailboxWakeDisposition.ACCEPTED
                ):
                    raise FencedNativeHistoryLifecycleError(
                        "native-history recovery did not accept the exact target request"
                    )
                self._recovery = recovery
                started = await self._supervisor.start()
                if not isinstance(started, FencedMailboxHandoffSupervisorSnapshot):
                    raise TypeError("native-history supervisor returned an invalid snapshot")
            except BaseException:
                await self._terminate()
                raise
            if not self._active_components_match():
                await self._terminate()
                raise FencedNativeHistoryLifecycleError(
                    "native-history target did not remain healthy after startup"
                )
            self._active = True
            return self.snapshot

    async def verify_active(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Verify the exact target remains the sole current local consumer."""

        async with self._lifecycle_lock:
            if self._closed:
                raise FencedNativeHistoryLifecycleError(
                    "a closed native-history lifecycle cannot verify its target"
                )
            if self._cleanup_failed:
                raise FencedNativeHistoryLifecycleError(
                    "native-history cleanup failed; only shutdown may retry it"
                )
            return await self._verify_active_locked()

    async def shutdown(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Retire the target before releasing its durable publication lease."""

        async with self._lifecycle_lock:
            if self._closed:
                return self.snapshot
            await self._terminate()
            return self.snapshot

    async def _verify_active_locked(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Validate active target and supervisor invariants under the lifecycle lock."""

        if not self._active:
            raise FencedNativeHistoryLifecycleError("native-history lifecycle has not activated")
        if not self._active_components_match():
            await self._terminate()
            raise FencedNativeHistoryLifecycleError(
                "native-history target or supervisor is no longer healthy"
            )
        return self.snapshot

    def _pre_activation_matches(self) -> bool:
        """Return whether both components remain unchanged and unstarted."""

        supervisor_snapshot = self._supervisor.snapshot
        return (
            self._target.state is FencedMailboxHandoffTargetState.NEW
            and supervisor_snapshot.state is FencedMailboxHandoffSupervisorState.NEW
            and self._target.execution_binding.request == self._request
            and self._target.target_identity == self._target_identity
            and self._supervisor.request == self._request
            and self._supervisor.target_identity == self._target_identity
            and self._persistence_domain_matches(supervisor_snapshot)
        )

    def _active_components_match(self) -> bool:
        """Return whether both components retain the active exact target identity."""

        supervisor_snapshot = self._supervisor.snapshot
        return (
            self._target.state is FencedMailboxHandoffTargetState.ACTIVE
            and supervisor_snapshot.state is FencedMailboxHandoffSupervisorState.ACTIVE
            and supervisor_snapshot.target_bound
            and supervisor_snapshot.binding_matches
            and supervisor_snapshot.target == self._target_identity
            and self._target.execution_binding.request == self._request
            and self._target.target_identity == self._target_identity
            and self._supervisor.request == self._request
            and self._supervisor.target_identity == self._target_identity
            and self._persistence_domain_matches(supervisor_snapshot)
        )

    def _persistence_domain_matches(
        self,
        supervisor_snapshot: FencedMailboxHandoffSupervisorSnapshot,
    ) -> bool:
        """Return whether all mutable components still share the bound domain."""

        return (
            self._target.persistence_domain is self._persistence_domain
            and self._supervisor.persistence_domain is self._persistence_domain
            and supervisor_snapshot.persistence_domain_matches
        )

    async def _terminate(self) -> None:
        """Complete target stop proof despite caller cancellation."""

        task = asyncio.create_task(
            self._terminate_once(),
            name="agent-fenced-native-history-lifecycle-shutdown",
        )
        cancelled_while_waiting = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled_while_waiting = True
        task.result()
        if cancelled_while_waiting:
            raise asyncio.CancelledError

    async def _terminate_once(self) -> None:
        """Delegate strict unbind and retirement to the one target supervisor."""

        try:
            shutdown = await self._supervisor.shutdown()
        except BaseException:
            self._cleanup_failed = True
            raise
        if not isinstance(shutdown, FencedMailboxHandoffSupervisorShutdown):
            self._cleanup_failed = True
            raise FencedNativeHistoryLifecycleError(
                "native-history supervisor returned an invalid shutdown result"
            )
        self._shutdown = shutdown
        if (
            shutdown.state is not FencedMailboxHandoffSupervisorState.STOPPED
            or self._target.state is not FencedMailboxHandoffTargetState.STOPPED
        ):
            self._cleanup_failed = True
            raise FencedNativeHistoryLifecycleError(
                "native-history target did not prove shutdown completion"
            )
        self._active = False
        self._cleanup_failed = False
        self._closed = True


def _require_target_port(target: object) -> None:
    """Validate the target capabilities before lifecycle composition."""

    required_properties = (
        "state",
        "execution_binding",
        "target_identity",
        "persistence_domain",
    )
    if any(not hasattr(target, attribute) for attribute in required_properties) or not callable(
        getattr(target, "recover_native_history", None)
    ):
        raise TypeError("target must implement the fenced native-history target port")


def _require_supervisor_port(supervisor: object) -> None:
    """Validate the supervisor capabilities before lifecycle composition."""

    required_properties = (
        "request",
        "target_identity",
        "persistence_domain",
        "snapshot",
    )
    required_methods = ("start", "shutdown")
    if any(not hasattr(supervisor, attribute) for attribute in required_properties) or any(
        not callable(getattr(supervisor, method_name, None)) for method_name in required_methods
    ):
        raise TypeError("supervisor must implement the fenced native-history supervisor port")


__all__ = [
    "FencedNativeHistoryLifecycleController",
    "FencedNativeHistoryLifecycleError",
    "FencedNativeHistoryLifecycleSnapshot",
    "FencedNativeHistoryLifecycleState",
    "FencedNativeHistorySupervisorPort",
    "FencedNativeHistoryTargetPort",
]
