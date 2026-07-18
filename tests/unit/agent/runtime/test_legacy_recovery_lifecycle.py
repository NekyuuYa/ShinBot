"""Unit coverage for lifecycle-owned historical actor recovery."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from shinbot.agent.runtime.session_actor.legacy_recovery_lifecycle import (
    LegacyRecoveryActorLifecycleController,
    LegacyRecoveryActorLifecycleError,
    LegacyRecoveryActorLifecycleState,
)
from shinbot.core.dispatch.legacy_recovery_gate import LegacyRecoveryPermit


class _FakeLegacyRecoveryGate:
    """Durable-gate double that records permit ownership and release order."""

    def __init__(self, persistence_domain: object, order: list[str]) -> None:
        self._persistence_domain = persistence_domain
        self._order = order
        self._active_permit: LegacyRecoveryPermit | None = None
        self.release_guard: Callable[[], None] | None = None

    @property
    def persistence_domain(self) -> object:
        """Return the configured durable domain identity."""

        return self._persistence_domain

    def acquire_legacy_recovery(self, *, holder_id: str) -> LegacyRecoveryPermit:
        """Acquire one exact fake permit."""

        if self._active_permit is not None:
            raise RuntimeError("synthetic permit already active")
        self._active_permit = LegacyRecoveryPermit(
            epoch=1,
            holder_id=holder_id,
            holder_token="synthetic-token",
        )
        self._order.append("acquire")
        return self._active_permit

    def validate_legacy_recovery_permit(self, permit: LegacyRecoveryPermit) -> None:
        """Require the exact fake active permit."""

        if permit != self._active_permit:
            raise RuntimeError("synthetic permit was lost")
        self._order.append("validate")

    def release_legacy_recovery(self, permit: LegacyRecoveryPermit) -> None:
        """Release one exact fake permit after the configured stop proof."""

        if permit != self._active_permit:
            raise RuntimeError("synthetic permit was lost before release")
        if self.release_guard is not None:
            self.release_guard()
        self._active_permit = None
        self._order.append("release")


class _FakeRegistry:
    """Registry double with an observable recovery and shutdown lifetime."""

    def __init__(self, persistence_domain: object, order: list[str]) -> None:
        self._persistence_domain = persistence_domain
        self._order = order
        self.accepting = True
        self.shutdown_complete = False
        self.fail_recovery = False
        self.shutdown_started = asyncio.Event()
        self.continue_shutdown = asyncio.Event()
        self.continue_shutdown.set()

    @property
    def persistence_domain(self) -> object:
        """Return the configured durable domain identity."""

        return self._persistence_domain

    async def _recover_under_legacy_recovery_lifecycle(
        self,
        permit: LegacyRecoveryPermit,
    ) -> int:
        """Record recovery under the exact lifecycle-held permit."""

        assert isinstance(permit, LegacyRecoveryPermit)
        self._order.append("recover")
        if self.fail_recovery:
            raise RuntimeError("synthetic guarded recovery failure")
        return 1

    async def shutdown(self, *, drain: bool) -> None:
        """Stop the fake registry after optional cancellation coordination."""

        assert drain is False
        self._order.append("registry_shutdown")
        self.shutdown_started.set()
        await self.continue_shutdown.wait()
        self.accepting = False
        self.shutdown_complete = True


def _components() -> tuple[
    LegacyRecoveryActorLifecycleController,
    _FakeLegacyRecoveryGate,
    _FakeRegistry,
    list[str],
]:
    """Build one controller with a same-domain fake registry and gate."""

    persistence_domain = object()
    order: list[str] = []
    gate = _FakeLegacyRecoveryGate(persistence_domain, order)
    registry = _FakeRegistry(persistence_domain, order)
    gate.release_guard = lambda: _assert_shutdown_complete(registry)
    controller = LegacyRecoveryActorLifecycleController(
        registry=registry,  # type: ignore[arg-type]
        legacy_recovery_gate=gate,
        holder_id="legacy-recovery-test",
    )
    return controller, gate, registry, order


def _assert_shutdown_complete(registry: _FakeRegistry) -> None:
    """Require the registry proof immediately before permit release."""

    assert registry.shutdown_complete is True


def test_lifecycle_rejects_a_gate_from_another_persistence_domain() -> None:
    """A recovery permit must protect exactly the registry's durable domain."""

    registry = _FakeRegistry(object(), [])
    gate = _FakeLegacyRecoveryGate(object(), [])

    with pytest.raises(ValueError, match="persistence domain"):
        LegacyRecoveryActorLifecycleController(
            registry=registry,  # type: ignore[arg-type]
            legacy_recovery_gate=gate,
            holder_id="legacy-recovery-test",
        )


@pytest.mark.asyncio
async def test_lifecycle_holds_permit_until_registry_shutdown_proves_complete() -> None:
    """Release occurs only after every recovery actor has been stopped."""

    controller, gate, registry, order = _components()

    activated = await controller.activate()
    assert activated.state is LegacyRecoveryActorLifecycleState.ACTIVE
    assert activated.permit_held is True
    assert gate._active_permit is not None
    assert await controller.verify_active_permit() == activated

    closed = await controller.shutdown()

    assert closed.state is LegacyRecoveryActorLifecycleState.CLOSED
    assert closed.permit_held is False
    assert registry.shutdown_complete is True
    assert gate._active_permit is None
    assert order == [
        "acquire",
        "recover",
        "validate",
        "validate",
        "registry_shutdown",
        "release",
    ]


@pytest.mark.asyncio
async def test_recovery_start_failure_stops_registry_before_releasing_permit() -> None:
    """A failed discovery cannot leave an actor registry or permit behind."""

    controller, gate, registry, order = _components()
    registry.fail_recovery = True

    with pytest.raises(RuntimeError, match="synthetic guarded recovery failure"):
        await controller.activate()

    assert controller.snapshot.state is LegacyRecoveryActorLifecycleState.CLOSED
    assert registry.shutdown_complete is True
    assert gate._active_permit is None
    assert order == ["acquire", "recover", "registry_shutdown", "release"]


@pytest.mark.asyncio
async def test_cancellation_cannot_interrupt_stop_before_permit_release() -> None:
    """A second lifecycle cancellation waits for the protected cleanup task."""

    controller, gate, registry, order = _components()
    await controller.activate()
    registry.continue_shutdown.clear()

    shutdown = asyncio.create_task(controller.shutdown())
    await asyncio.wait_for(registry.shutdown_started.wait(), timeout=1.0)
    shutdown.cancel()
    registry.continue_shutdown.set()

    with pytest.raises(asyncio.CancelledError):
        await shutdown

    assert controller.snapshot.state is LegacyRecoveryActorLifecycleState.CLOSED
    assert registry.shutdown_complete is True
    assert gate._active_permit is None
    assert order[-2:] == ["registry_shutdown", "release"]


@pytest.mark.asyncio
async def test_closed_lifecycle_cannot_be_reactivated() -> None:
    """A released recovery lifetime cannot silently start a second actor set."""

    controller, _gate, _registry, _order = _components()
    await controller.shutdown()

    with pytest.raises(LegacyRecoveryActorLifecycleError, match="closed"):
        await controller.activate()
