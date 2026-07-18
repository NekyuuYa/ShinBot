"""Unmounted lifetime owner for guarded historical session-actor recovery.

Broad recovery can create actor tasks that remain alive after discovery returns.
This controller therefore retains the durable legacy-recovery permit until its
registry has stopped every actor it started. It intentionally does not publish a
wake target, start an effect executor, or route production traffic.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.core.dispatch.legacy_recovery_gate import LegacyRecoveryPermit


class LegacyRecoveryGate(Protocol):
    """Durable gate operations required by the recovery lifecycle."""

    @property
    def persistence_domain(self) -> object:
        """Return the exact durable domain protected by this gate."""

    def acquire_legacy_recovery(self, *, holder_id: str) -> LegacyRecoveryPermit:
        """Acquire an exclusive permit for the full recovery actor lifetime."""

    def validate_legacy_recovery_permit(self, permit: LegacyRecoveryPermit) -> None:
        """Validate the permit at an explicit lifecycle boundary."""

    def release_legacy_recovery(self, permit: LegacyRecoveryPermit) -> None:
        """Release the permit after the registry has proved shutdown."""


class LegacyRecoveryActorLifecycleState(StrEnum):
    """Process-local state of one unmounted historical recovery controller."""

    READY = "ready"
    ACTIVE = "active"
    FAILED = "failed"
    CLOSED = "closed"


@dataclass(slots=True, frozen=True)
class LegacyRecoveryActorLifecycleSnapshot:
    """Read-only state without exposing the registry or permit capability."""

    state: LegacyRecoveryActorLifecycleState
    permit_held: bool
    registry_accepting: bool
    registry_shutdown_complete: bool
    persistence_domain_matches: bool
    cleanup_failed: bool


class LegacyRecoveryActorLifecycleError(RuntimeError):
    """Raised when historical recovery cannot retain a safe actor lifetime."""


class LegacyRecoveryActorLifecyclePermitLost(LegacyRecoveryActorLifecycleError):
    """Raised after a previously held recovery permit no longer validates."""


class LegacyRecoveryActorLifecycleController:
    """Own broad recovery actors from permit acquisition through final stop.

    This is deliberately not a routing target. A caller may use it only as a
    lifecycle object: activate it, retain it while recovery actors run, then
    shut it down. Releasing its permit before ``registry.shutdown_complete`` is
    structurally impossible through this controller.
    """

    def __init__(
        self,
        *,
        registry: AgentSessionActorRegistry,
        legacy_recovery_gate: LegacyRecoveryGate,
        holder_id: str,
    ) -> None:
        """Bind one inactive registry to one same-domain durable gate.

        Args:
            registry: Private registry whose recovery actors this controller owns.
            legacy_recovery_gate: Durable exclusion gate for historical recovery.
            holder_id: Stable diagnostic identity recorded by the durable gate.

        Raises:
            ValueError: If the registry is already closed, the durable domains
                differ, or ``holder_id`` is empty.
        """

        normalized_holder_id = str(holder_id or "").strip()
        if not normalized_holder_id:
            raise ValueError("legacy recovery lifecycle holder_id must not be empty")
        if not registry.accepting or registry.shutdown_complete:
            raise ValueError("legacy recovery lifecycle requires an inactive open registry")
        persistence_domain = registry.persistence_domain
        if legacy_recovery_gate.persistence_domain is not persistence_domain:
            raise ValueError(
                "legacy recovery gate must protect the registry persistence domain"
            )

        self._registry = registry
        self._gate = legacy_recovery_gate
        self._holder_id = normalized_holder_id
        self._persistence_domain = persistence_domain
        self._lifecycle_lock = asyncio.Lock()
        self._permit: LegacyRecoveryPermit | None = None
        self._active = False
        self._closed = False
        self._termination_failed = False

    @property
    def snapshot(self) -> LegacyRecoveryActorLifecycleSnapshot:
        """Return lifecycle state without leaking a usable permit or registry."""

        permit_held = self._permit is not None
        registry_accepting = self._registry.accepting
        registry_shutdown_complete = self._registry.shutdown_complete
        persistence_domain_matches = self._persistence_domain_matches()
        if self._closed:
            state = LegacyRecoveryActorLifecycleState.CLOSED
        elif (
            self._active
            and permit_held
            and registry_accepting
            and not registry_shutdown_complete
            and persistence_domain_matches
        ):
            state = LegacyRecoveryActorLifecycleState.ACTIVE
        elif (
            self._termination_failed
            or permit_held
            or self._active
            or registry_shutdown_complete
            or not registry_accepting
            or not persistence_domain_matches
        ):
            state = LegacyRecoveryActorLifecycleState.FAILED
        else:
            state = LegacyRecoveryActorLifecycleState.READY
        return LegacyRecoveryActorLifecycleSnapshot(
            state=state,
            permit_held=permit_held,
            registry_accepting=registry_accepting,
            registry_shutdown_complete=registry_shutdown_complete,
            persistence_domain_matches=persistence_domain_matches,
            cleanup_failed=self._termination_failed,
        )

    async def activate(self) -> LegacyRecoveryActorLifecycleSnapshot:
        """Acquire a permit and start broad recovery actors under its lifetime.

        The permit stays held after this method returns. Callers must invoke
        :meth:`shutdown` to stop the registry before the controller releases it.
        """

        async with self._lifecycle_lock:
            if self._closed:
                raise LegacyRecoveryActorLifecycleError(
                    "a closed legacy recovery lifecycle cannot activate"
                )
            if self._termination_failed:
                raise LegacyRecoveryActorLifecycleError(
                    "legacy recovery cleanup failed; only shutdown may retry it"
                )
            if self._active:
                return await self._verify_active_locked()
            if not self._persistence_domain_matches():
                await self._terminate()
                raise LegacyRecoveryActorLifecycleError(
                    "legacy recovery persistence domain changed before startup"
                )
            if not self._registry.accepting or self._registry.shutdown_complete:
                await self._terminate()
                raise LegacyRecoveryActorLifecycleError(
                    "legacy recovery registry changed before startup"
                )

            permit = self._gate.acquire_legacy_recovery(holder_id=self._holder_id)
            self._permit = permit
            try:
                await self._registry._recover_under_legacy_recovery_lifecycle(permit)
                self._gate.validate_legacy_recovery_permit(permit)
            except BaseException:
                await self._terminate()
                raise
            if not self._persistence_domain_matches():
                await self._terminate()
                raise LegacyRecoveryActorLifecycleError(
                    "legacy recovery persistence domain changed during startup"
                )
            if not self._registry.accepting or self._registry.shutdown_complete:
                await self._terminate()
                raise LegacyRecoveryActorLifecycleError(
                    "legacy recovery registry did not remain healthy after startup"
                )
            self._active = True
            return self.snapshot

    async def verify_active_permit(self) -> LegacyRecoveryActorLifecycleSnapshot:
        """Validate a running lifecycle at an explicit later cutover boundary."""

        async with self._lifecycle_lock:
            if self._closed:
                raise LegacyRecoveryActorLifecycleError(
                    "a closed legacy recovery lifecycle cannot verify its permit"
                )
            if self._termination_failed:
                raise LegacyRecoveryActorLifecycleError(
                    "legacy recovery cleanup failed; only shutdown may retry it"
                )
            return await self._verify_active_locked()

    async def shutdown(self) -> LegacyRecoveryActorLifecycleSnapshot:
        """Stop every owned actor before releasing the durable recovery permit."""

        async with self._lifecycle_lock:
            if self._closed:
                return self.snapshot
            await self._terminate()
            return self.snapshot

    async def _verify_active_locked(self) -> LegacyRecoveryActorLifecycleSnapshot:
        """Verify the active lifecycle while the caller holds the lifecycle lock."""

        permit = self._permit
        if not self._active or permit is None:
            raise LegacyRecoveryActorLifecycleError(
                "legacy recovery lifecycle has not been activated"
            )
        if not self._persistence_domain_matches():
            await self._terminate()
            raise LegacyRecoveryActorLifecycleError(
                "legacy recovery persistence domain is no longer healthy"
            )
        if not self._registry.accepting or self._registry.shutdown_complete:
            await self._terminate()
            raise LegacyRecoveryActorLifecycleError(
                "legacy recovery registry is no longer healthy"
            )
        try:
            self._gate.validate_legacy_recovery_permit(permit)
        except BaseException as exc:
            await self._terminate()
            raise LegacyRecoveryActorLifecyclePermitLost(
                "legacy recovery permit is no longer valid"
            ) from exc
        return self.snapshot

    def _persistence_domain_matches(self) -> bool:
        """Return whether the registry and durable gate retain the bound domain."""

        return (
            self._registry.persistence_domain is self._persistence_domain
            and self._gate.persistence_domain is self._persistence_domain
        )

    async def _terminate(self) -> None:
        """Complete stop-before-release despite repeated caller cancellation."""

        task = asyncio.create_task(
            self._terminate_once(),
            name="agent-session-actor-legacy-recovery-shutdown",
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
        """Stop the registry and only then release the owned durable permit."""

        if not self._registry.shutdown_complete:
            try:
                await self._registry.shutdown(drain=False)
            except BaseException:
                self._termination_failed = True
                raise
        if not self._registry.shutdown_complete:
            self._termination_failed = True
            raise LegacyRecoveryActorLifecycleError(
                "legacy recovery registry did not prove shutdown completion"
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
    "LegacyRecoveryActorLifecycleController",
    "LegacyRecoveryActorLifecycleError",
    "LegacyRecoveryActorLifecyclePermitLost",
    "LegacyRecoveryActorLifecycleSnapshot",
    "LegacyRecoveryActorLifecycleState",
    "LegacyRecoveryGate",
]
