"""Unmounted complete-history lifecycle for the durable Actor v2 harness.

This controller owns the one unsafe boundary that a short recovery callback
cannot: the permit remains held while recovered actors and effect workers run.
It intentionally does not publish ingress, a wake target, timers, scanners, or
management commands, so it is not a production Actor v2 cutover controller.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum

from shinbot.agent.runtime.session_actor.harness import (
    ActorRuntimeActivationScope,
    ActorRuntimeHarness,
)
from shinbot.agent.runtime.session_actor.legacy_recovery_lifecycle import (
    LegacyRecoveryGate,
)
from shinbot.core.dispatch.legacy_recovery_gate import LegacyRecoveryPermit


class ActorRuntimeHistoryLifecycleState(StrEnum):
    """Process-local state of one complete-history harness lifecycle."""

    READY = "ready"
    ACTIVE = "active"
    FAILED = "failed"
    CLOSED = "closed"


@dataclass(slots=True, frozen=True)
class ActorRuntimeHistoryLifecycleSnapshot:
    """Read-only lifecycle state without exposing a permit or runtime target."""

    state: ActorRuntimeHistoryLifecycleState
    permit_held: bool
    harness_active: bool
    harness_shutdown_complete: bool
    persistence_domain_matches: bool
    cleanup_failed: bool


class ActorRuntimeHistoryLifecycleError(RuntimeError):
    """Raised when complete-history runtime work cannot retain safe ownership."""


class ActorRuntimeHistoryLifecyclePermitLost(ActorRuntimeHistoryLifecycleError):
    """Raised after an active complete-history permit no longer validates."""


class ActorRuntimeHistoryLifecycleController:
    """Own harness workers from permit acquisition through proven shutdown.

    A successful activation starts complete-history actor recovery and the
    effect executor, but still leaves all ingress and ownership cutover paths
    closed. The controller must be retained until :meth:`shutdown` stops the
    harness, because that stop proof is the precondition for permit release.
    """

    def __init__(
        self,
        *,
        harness: ActorRuntimeHarness,
        legacy_recovery_gate: LegacyRecoveryGate,
        holder_id: str,
    ) -> None:
        """Bind one inactive complete-history harness to a same-domain gate.

        Args:
            harness: Complete handler-graph harness owned by this lifecycle.
            legacy_recovery_gate: Durable gate held for the worker lifetime.
            holder_id: Stable diagnostic identity recorded by the gate.

        Raises:
            ValueError: If the harness is not an inactive complete-history
                harness, domains differ, or ``holder_id`` is empty.
        """

        normalized_holder_id = str(holder_id or "").strip()
        if not normalized_holder_id:
            raise ValueError("history lifecycle holder_id must not be empty")
        if harness.activation_scope is not ActorRuntimeActivationScope.COMPLETE_HISTORY:
            raise ValueError("history lifecycle requires a complete-history harness")
        if not harness.complete_history_activation_ready:
            raise ValueError("history lifecycle requires a complete handler graph")
        if harness.active or harness.closed:
            raise ValueError("history lifecycle requires an inactive open harness")
        persistence_domain = harness.persistence_domain
        if legacy_recovery_gate.persistence_domain is not persistence_domain:
            raise ValueError("history lifecycle gate must protect the harness persistence domain")

        self._harness = harness
        self._gate = legacy_recovery_gate
        self._holder_id = normalized_holder_id
        self._persistence_domain = persistence_domain
        self._lifecycle_lock = asyncio.Lock()
        self._permit: LegacyRecoveryPermit | None = None
        self._active = False
        self._closed = False
        self._termination_failed = False

    @property
    def snapshot(self) -> ActorRuntimeHistoryLifecycleSnapshot:
        """Return lifecycle state without publishing mutable runtime surfaces."""

        permit_held = self._permit is not None
        harness_active = self._harness.active
        harness_shutdown_complete = self._harness.shutdown_complete
        persistence_domain_matches = self._persistence_domain_matches()
        if self._closed:
            state = ActorRuntimeHistoryLifecycleState.CLOSED
        elif (
            self._active
            and permit_held
            and harness_active
            and not harness_shutdown_complete
            and persistence_domain_matches
        ):
            state = ActorRuntimeHistoryLifecycleState.ACTIVE
        elif (
            self._termination_failed
            or permit_held
            or self._active
            or self._harness.closed
            or harness_shutdown_complete
            or not persistence_domain_matches
        ):
            state = ActorRuntimeHistoryLifecycleState.FAILED
        else:
            state = ActorRuntimeHistoryLifecycleState.READY
        return ActorRuntimeHistoryLifecycleSnapshot(
            state=state,
            permit_held=permit_held,
            harness_active=harness_active,
            harness_shutdown_complete=harness_shutdown_complete,
            persistence_domain_matches=persistence_domain_matches,
            cleanup_failed=self._termination_failed,
        )

    async def activate(self) -> ActorRuntimeHistoryLifecycleSnapshot:
        """Start complete-history workers while retaining one durable permit."""

        async with self._lifecycle_lock:
            if self._closed:
                raise ActorRuntimeHistoryLifecycleError(
                    "a closed complete-history lifecycle cannot activate"
                )
            if self._termination_failed:
                raise ActorRuntimeHistoryLifecycleError(
                    "complete-history cleanup failed; only shutdown may retry it"
                )
            if self._active:
                return await self._verify_active_locked()
            if not self._persistence_domain_matches():
                await self._terminate()
                raise ActorRuntimeHistoryLifecycleError(
                    "complete-history persistence domain changed before startup"
                )
            if self._harness.closed or self._harness.shutdown_complete:
                await self._terminate()
                raise ActorRuntimeHistoryLifecycleError(
                    "complete-history harness changed before startup"
                )

            permit = self._gate.acquire_legacy_recovery(holder_id=self._holder_id)
            self._permit = permit
            try:
                await self._harness._activate_complete_history_under_legacy_recovery_lifecycle(
                    permit
                )
                self._gate.validate_legacy_recovery_permit(permit)
            except BaseException:
                await self._terminate()
                raise
            if not self._persistence_domain_matches():
                await self._terminate()
                raise ActorRuntimeHistoryLifecycleError(
                    "complete-history persistence domain changed during startup"
                )
            if not self._harness.active:
                await self._terminate()
                raise ActorRuntimeHistoryLifecycleError(
                    "complete-history harness did not remain healthy after startup"
                )
            self._active = True
            return self.snapshot

    async def verify_active_permit(self) -> ActorRuntimeHistoryLifecycleSnapshot:
        """Validate the permit and worker health at an explicit later boundary."""

        async with self._lifecycle_lock:
            if self._closed:
                raise ActorRuntimeHistoryLifecycleError(
                    "a closed complete-history lifecycle cannot verify its permit"
                )
            if self._termination_failed:
                raise ActorRuntimeHistoryLifecycleError(
                    "complete-history cleanup failed; only shutdown may retry it"
                )
            return await self._verify_active_locked()

    async def shutdown(self) -> ActorRuntimeHistoryLifecycleSnapshot:
        """Stop executor and actors before releasing the durable permit."""

        async with self._lifecycle_lock:
            if self._closed:
                return self.snapshot
            await self._terminate()
            return self.snapshot

    async def _verify_active_locked(self) -> ActorRuntimeHistoryLifecycleSnapshot:
        """Verify worker and permit health while the lifecycle lock is held."""

        permit = self._permit
        if not self._active or permit is None:
            raise ActorRuntimeHistoryLifecycleError(
                "complete-history lifecycle has not been activated"
            )
        if not self._persistence_domain_matches() or not self._harness.active:
            await self._terminate()
            raise ActorRuntimeHistoryLifecycleError(
                "complete-history harness or persistence domain is no longer healthy"
            )
        try:
            self._gate.validate_legacy_recovery_permit(permit)
        except BaseException as exc:
            await self._terminate()
            raise ActorRuntimeHistoryLifecyclePermitLost(
                "complete-history permit is no longer valid"
            ) from exc
        return self.snapshot

    def _persistence_domain_matches(self) -> bool:
        """Return whether the mutable harness and gate retain the bound domain."""

        return (
            self._harness.persistence_domain is self._persistence_domain
            and self._gate.persistence_domain is self._persistence_domain
        )

    async def _terminate(self) -> None:
        """Finish stop-before-release despite repeated caller cancellation."""

        task = asyncio.create_task(
            self._terminate_once(),
            name="agent-session-actor-history-lifecycle-shutdown",
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
        """Stop the harness and only then release the owned durable permit."""

        if not self._harness.shutdown_complete:
            try:
                await self._harness.shutdown(drain=False)
            except BaseException:
                self._termination_failed = True
                raise
        if not self._harness.shutdown_complete:
            self._termination_failed = True
            raise ActorRuntimeHistoryLifecycleError(
                "complete-history harness did not prove shutdown completion"
            )
        permit = self._permit
        if permit is not None:
            try:
                self._gate.release_legacy_recovery(permit)
            except BaseException:
                self._termination_failed = True
                raise
            self._permit = None
        self._active = False
        self._termination_failed = False
        self._closed = True


__all__ = [
    "ActorRuntimeHistoryLifecycleController",
    "ActorRuntimeHistoryLifecycleError",
    "ActorRuntimeHistoryLifecyclePermitLost",
    "ActorRuntimeHistoryLifecycleSnapshot",
    "ActorRuntimeHistoryLifecycleState",
]
