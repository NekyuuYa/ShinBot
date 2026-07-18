"""Explicit, unmounted lifecycle control for a future Actor v2 canary.

This module deliberately coordinates only a clean-session harness and an
external isolation lease. It does not select durable ownership, publish an
actor wake target, start timer/recovery supervisors, or route production work.
Those operations need a later controller that can prove the complete cutover
protocol.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.session_actor.harness import (
    ActorRuntimeActivationScope,
    ActorRuntimeHarness,
)


class ActorV2CanaryIsolationLease(Protocol):
    """External proof that clean startup is isolated from live ingress."""

    @property
    def persistence_domain(self) -> object:
        """Return the exact durable domain protected by this lease."""

    @property
    def active(self) -> bool:
        """Return whether the isolation proof remains valid right now."""

    async def release(self) -> None:
        """Release the proof after a proven stop; retries must be idempotent."""


class ActorV2CanaryLifecycleState(StrEnum):
    """Process-local lifecycle state for one clean canary controller."""

    READY = "ready"
    ACTIVE = "active"
    FAILED = "failed"
    CLOSED = "closed"


@dataclass(slots=True, frozen=True)
class ActorV2CanaryLifecycleSnapshot:
    """Read-only lifecycle state without exposing mutable actor components."""

    state: ActorV2CanaryLifecycleState
    harness_active: bool
    harness_shutdown_complete: bool
    isolation_lease_active: bool
    persistence_domain_matches: bool
    cleanup_failed: bool


class ActorV2CanaryLifecycleError(RuntimeError):
    """Base error for a failed or misconfigured clean canary lifecycle."""


class ActorV2CanaryIsolationLost(ActorV2CanaryLifecycleError):
    """Raised when isolation no longer holds at a canary lifecycle boundary."""


class ActorV2CanaryLifecycleController:
    """Order a clean harness behind an external, same-domain isolation proof.

    The controller is intentionally unmounted. Its scope ends after starting
    or stopping the harness, so it cannot accidentally become the future
    ingress/ownership controller by virtue of a compatible method shape.
    """

    def __init__(
        self,
        *,
        harness: ActorRuntimeHarness,
        isolation_lease: ActorV2CanaryIsolationLease,
    ) -> None:
        """Bind one inactive clean harness to one active isolation lease.

        Args:
            harness: Complete clean-session harness that remains unmounted.
            isolation_lease: External proof that protects the harness domain
                from live ingress while the canary lifecycle is active.

        Raises:
            ValueError: If the harness is not an inactive clean-session
                harness, the lease is inactive, or the durable domains differ.
        """

        if harness.activation_scope is not ActorRuntimeActivationScope.CLEAN_SESSION:
            raise ValueError("canary lifecycle requires a clean-session harness")
        if harness.active or harness.closed:
            raise ValueError("canary lifecycle requires an inactive open harness")
        persistence_domain = harness.persistence_domain
        if isolation_lease.persistence_domain is not persistence_domain:
            raise ValueError("canary isolation lease must protect the harness persistence domain")
        if not isolation_lease.active:
            raise ValueError("canary isolation lease must be active at composition")

        self._harness = harness
        self._isolation_lease = isolation_lease
        self._persistence_domain = persistence_domain
        self._lifecycle_lock = asyncio.Lock()
        self._started = False
        self._closed = False
        self._termination_failed = False

    @property
    def snapshot(self) -> ActorV2CanaryLifecycleSnapshot:
        """Return lifecycle state without publishing the harness or its target."""

        harness_active = self._harness.active
        harness_shutdown_complete = self._harness.shutdown_complete
        lease_active = self._isolation_lease.active
        persistence_domain_matches = self._persistence_domain_matches()
        if self._closed:
            state = ActorV2CanaryLifecycleState.CLOSED
        elif self._started and harness_active and lease_active and persistence_domain_matches:
            state = ActorV2CanaryLifecycleState.ACTIVE
        elif (
            self._termination_failed
            or self._started
            or harness_active
            or self._harness.closed
            or not persistence_domain_matches
        ):
            state = ActorV2CanaryLifecycleState.FAILED
        else:
            state = ActorV2CanaryLifecycleState.READY
        return ActorV2CanaryLifecycleSnapshot(
            state=state,
            harness_active=harness_active,
            harness_shutdown_complete=harness_shutdown_complete,
            isolation_lease_active=lease_active,
            persistence_domain_matches=persistence_domain_matches,
            cleanup_failed=self._termination_failed,
        )

    async def activate(self) -> ActorV2CanaryLifecycleSnapshot:
        """Start the clean harness only while same-domain isolation is live.

        A successful result still does not authorize ownership selection or
        ingress publication. The caller must hand control to a later explicit
        cutover controller before any durable Actor v2 work can be accepted.

        Raises:
            ActorV2CanaryIsolationLost: If the isolation proof is inactive
                before startup or disappears during the activation sequence.
            ActorV2CanaryLifecycleError: If this controller is closed or has
                already lost harness health after an earlier activation.
        """

        async with self._lifecycle_lock:
            if self._closed:
                raise ActorV2CanaryLifecycleError(
                    "a closed Actor v2 canary lifecycle cannot activate"
                )
            if self._termination_failed:
                raise ActorV2CanaryLifecycleError(
                    "Actor v2 canary cleanup failed; only shutdown may retry it"
                )
            if self._started:
                return await self._verify_active_isolation_locked()
            current = self.snapshot
            if not current.isolation_lease_active:
                await self._terminate()
                raise ActorV2CanaryIsolationLost(
                    "Actor v2 canary isolation lease is inactive before startup"
                )
            if current.state is ActorV2CanaryLifecycleState.FAILED:
                await self._terminate()
                raise ActorV2CanaryLifecycleError(
                    "Actor v2 canary composition changed before startup"
                )

            try:
                await self._harness.activate()
            except BaseException:
                if self._harness.closed or self._harness.active:
                    await self._terminate()
                raise
            if not self._persistence_domain_matches():
                await self._terminate()
                raise ActorV2CanaryLifecycleError(
                    "Actor v2 canary persistence domain changed during startup"
                )
            if not self._isolation_lease.active:
                await self._terminate()
                raise ActorV2CanaryIsolationLost(
                    "Actor v2 canary isolation lease was lost during startup"
                )
            if not self._harness.active:
                await self._terminate()
                raise ActorV2CanaryLifecycleError(
                    "Actor v2 canary harness did not remain healthy after startup"
                )
            self._started = True
            return self.snapshot

    async def verify_active_isolation(self) -> ActorV2CanaryLifecycleSnapshot:
        """Fail closed unless an already-started canary remains isolated.

        This is an explicit lifecycle-boundary guard, not a background lease
        monitor. A future cutover controller must call it before every later
        ownership or ingress step and provide its own durable revocation path.

        Raises:
            ActorV2CanaryIsolationLost: If the isolation proof is no longer
                active. The harness is stopped before this error is raised.
            ActorV2CanaryLifecycleError: If the lifecycle is closed, has not
                started, or its harness/domain is no longer healthy.
        """

        async with self._lifecycle_lock:
            if self._closed:
                raise ActorV2CanaryLifecycleError(
                    "a closed Actor v2 canary lifecycle cannot verify isolation"
                )
            if self._termination_failed:
                raise ActorV2CanaryLifecycleError(
                    "Actor v2 canary cleanup failed; only shutdown may retry it"
                )
            return await self._verify_active_isolation_locked()

    async def shutdown(self) -> ActorV2CanaryLifecycleSnapshot:
        """Stop the harness before releasing its external isolation proof."""

        async with self._lifecycle_lock:
            if self._closed:
                return self.snapshot
            await self._terminate()
            return self.snapshot

    async def _verify_active_isolation_locked(
        self,
    ) -> ActorV2CanaryLifecycleSnapshot:
        """Validate active isolation while the lifecycle lock is held."""

        current = self.snapshot
        if current.state is ActorV2CanaryLifecycleState.ACTIVE:
            return current
        if not current.isolation_lease_active:
            await self._terminate()
            raise ActorV2CanaryIsolationLost("Actor v2 canary isolation lease is no longer active")
        if not self._started and current.state is ActorV2CanaryLifecycleState.READY:
            raise ActorV2CanaryLifecycleError("Actor v2 canary lifecycle has not been activated")
        await self._terminate()
        raise ActorV2CanaryLifecycleError(
            "Actor v2 canary harness or persistence domain is no longer healthy"
        )

    def _persistence_domain_matches(self) -> bool:
        """Return whether both mutable collaborators retain the bound domain."""

        return (
            self._harness.persistence_domain is self._persistence_domain
            and self._isolation_lease.persistence_domain is self._persistence_domain
        )

    async def _terminate(self) -> None:
        """Complete stop-before-release even when the caller is cancelled."""

        task = asyncio.create_task(
            self._terminate_once(),
            name="agent-session-actor-canary-shutdown",
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
        """Prove the harness is stopped, then release its lease exactly once."""

        if not self._harness.shutdown_complete:
            try:
                await self._harness.shutdown(drain=False)
            except BaseException:
                self._termination_failed = True
                raise
        if not self._harness.shutdown_complete:
            self._termination_failed = True
            raise ActorV2CanaryLifecycleError(
                "Actor v2 canary harness did not prove shutdown completion"
            )
        try:
            await self._isolation_lease.release()
        except BaseException:
            self._termination_failed = True
            raise
        if self._isolation_lease.active:
            self._termination_failed = True
            raise ActorV2CanaryLifecycleError(
                "Actor v2 canary isolation lease remained active after release"
            )
        self._termination_failed = False
        self._closed = True


__all__ = [
    "ActorV2CanaryIsolationLease",
    "ActorV2CanaryIsolationLost",
    "ActorV2CanaryLifecycleController",
    "ActorV2CanaryLifecycleError",
    "ActorV2CanaryLifecycleSnapshot",
    "ActorV2CanaryLifecycleState",
]
