"""Unit coverage for the unmounted complete-history harness lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from shinbot.agent.runtime.session_actor.harness import ActorRuntimeActivationScope
from shinbot.agent.runtime.session_actor.history_lifecycle import (
    ActorRuntimeHistoryLifecycleController,
    ActorRuntimeHistoryLifecycleError,
    ActorRuntimeHistoryLifecycleState,
)
from shinbot.core.dispatch.legacy_recovery_gate import LegacyRecoveryPermit


class _FakeGate:
    """Permit gate double with a release-order assertion hook."""

    def __init__(self, persistence_domain: object, order: list[str]) -> None:
        self._persistence_domain = persistence_domain
        self._order = order
        self._permit: LegacyRecoveryPermit | None = None
        self.release_guard: Callable[[], None] | None = None

    @property
    def persistence_domain(self) -> object:
        """Return the protected fake durable domain."""

        return self._persistence_domain

    def acquire_legacy_recovery(self, *, holder_id: str) -> LegacyRecoveryPermit:
        """Acquire one exact fake permit."""

        if self._permit is not None:
            raise RuntimeError("synthetic permit already active")
        self._permit = LegacyRecoveryPermit(
            epoch=1,
            holder_id=holder_id,
            holder_token="history-lifecycle-token",
        )
        self._order.append("acquire")
        return self._permit

    def validate_legacy_recovery_permit(self, permit: LegacyRecoveryPermit) -> None:
        """Require the currently held fake permit."""

        if permit != self._permit:
            raise RuntimeError("synthetic permit was lost")
        self._order.append("validate")

    def release_legacy_recovery(self, permit: LegacyRecoveryPermit) -> None:
        """Release after the configured harness stop proof."""

        if permit != self._permit:
            raise RuntimeError("synthetic permit was lost before release")
        if self.release_guard is not None:
            self.release_guard()
        self._permit = None
        self._order.append("release")


class _FakeHistoryHarness:
    """Complete-history harness double with controllable startup and shutdown."""

    def __init__(
        self,
        persistence_domain: object,
        order: list[str],
        *,
        activation_scope: ActorRuntimeActivationScope = (
            ActorRuntimeActivationScope.COMPLETE_HISTORY
        ),
        complete_history_activation_ready: bool = True,
    ) -> None:
        self._persistence_domain = persistence_domain
        self._order = order
        self.activation_scope = activation_scope
        self.complete_history_activation_ready = complete_history_activation_ready
        self.active = False
        self.closed = False
        self.shutdown_complete = False
        self.start_error: Exception | None = None
        self.startup_started = asyncio.Event()
        self.continue_startup = asyncio.Event()
        self.continue_startup.set()
        self.shutdown_started = asyncio.Event()
        self.continue_shutdown = asyncio.Event()
        self.continue_shutdown.set()

    @property
    def persistence_domain(self) -> object:
        """Return the fake harness durable domain."""

        return self._persistence_domain

    async def _activate_complete_history_under_legacy_recovery_lifecycle(
        self,
        permit: LegacyRecoveryPermit,
    ) -> None:
        """Record permit-owned startup and apply the configured outcome."""

        assert isinstance(permit, LegacyRecoveryPermit)
        self._order.append("harness_activate")
        self.startup_started.set()
        await self.continue_startup.wait()
        if self.start_error is not None:
            raise self.start_error
        self.active = True

    async def shutdown(self, *, drain: bool) -> None:
        """Stop the fake harness after optional cancellation coordination."""

        assert drain is False
        self._order.append("harness_shutdown")
        self.shutdown_started.set()
        await self.continue_shutdown.wait()
        self.active = False
        self.closed = True
        self.shutdown_complete = True


def _components() -> tuple[
    ActorRuntimeHistoryLifecycleController,
    _FakeGate,
    _FakeHistoryHarness,
    list[str],
]:
    """Build one same-domain fake controller graph."""

    persistence_domain = object()
    order: list[str] = []
    harness = _FakeHistoryHarness(persistence_domain, order)
    gate = _FakeGate(persistence_domain, order)
    gate.release_guard = lambda: _assert_harness_shutdown(harness)
    controller = ActorRuntimeHistoryLifecycleController(
        harness=harness,  # type: ignore[arg-type]
        legacy_recovery_gate=gate,
        holder_id="history-lifecycle-test",
    )
    return controller, gate, harness, order


def _assert_harness_shutdown(harness: _FakeHistoryHarness) -> None:
    """Require full harness shutdown immediately before permit release."""

    assert harness.shutdown_complete is True


def test_history_lifecycle_rejects_clean_session_harness() -> None:
    """A clean canary does not authorize history recovery semantics."""

    persistence_domain = object()
    harness = _FakeHistoryHarness(
        persistence_domain,
        [],
        activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
    )
    gate = _FakeGate(persistence_domain, [])

    with pytest.raises(ValueError, match="complete-history"):
        ActorRuntimeHistoryLifecycleController(
            harness=harness,  # type: ignore[arg-type]
            legacy_recovery_gate=gate,
            holder_id="history-lifecycle-test",
        )


def test_history_lifecycle_rejects_partial_complete_history_harness() -> None:
    """A diagnostic complete-history scope cannot acquire recovery authority."""

    persistence_domain = object()
    harness = _FakeHistoryHarness(
        persistence_domain,
        [],
        complete_history_activation_ready=False,
    )
    gate = _FakeGate(persistence_domain, [])

    with pytest.raises(ValueError, match="complete handler graph"):
        ActorRuntimeHistoryLifecycleController(
            harness=harness,  # type: ignore[arg-type]
            legacy_recovery_gate=gate,
            holder_id="history-lifecycle-test",
        )


@pytest.mark.asyncio
async def test_history_lifecycle_holds_permit_until_harness_shutdown() -> None:
    """Effect workers and actors stop before the durable gate is reopened."""

    controller, gate, harness, order = _components()

    activated = await controller.activate()
    assert activated.state is ActorRuntimeHistoryLifecycleState.ACTIVE
    assert activated.permit_held is True
    assert harness.active is True
    assert gate._permit is not None
    assert await controller.verify_active_permit() == activated

    closed = await controller.shutdown()

    assert closed.state is ActorRuntimeHistoryLifecycleState.CLOSED
    assert harness.shutdown_complete is True
    assert gate._permit is None
    assert order == [
        "acquire",
        "harness_activate",
        "validate",
        "validate",
        "harness_shutdown",
        "release",
    ]


@pytest.mark.asyncio
async def test_history_start_failure_stops_harness_before_permit_release() -> None:
    """A failing effect-worker startup cannot leave a recovery permit live."""

    controller, gate, harness, order = _components()
    harness.start_error = RuntimeError("synthetic history startup failure")

    with pytest.raises(RuntimeError, match="synthetic history startup failure"):
        await controller.activate()

    assert controller.snapshot.state is ActorRuntimeHistoryLifecycleState.CLOSED
    assert harness.shutdown_complete is True
    assert gate._permit is None
    assert order == ["acquire", "harness_activate", "harness_shutdown", "release"]


@pytest.mark.asyncio
async def test_history_startup_cancellation_stops_harness_before_permit_release() -> None:
    """Cancelling activation cannot return a permit while startup work survives."""

    controller, gate, harness, order = _components()
    harness.continue_startup.clear()

    activation = asyncio.create_task(controller.activate())
    await asyncio.wait_for(harness.startup_started.wait(), timeout=1.0)
    activation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await activation

    assert controller.snapshot.state is ActorRuntimeHistoryLifecycleState.CLOSED
    assert harness.shutdown_complete is True
    assert gate._permit is None
    assert order == ["acquire", "harness_activate", "harness_shutdown", "release"]


@pytest.mark.asyncio
async def test_history_shutdown_cancellation_cannot_skip_stop_before_release() -> None:
    """The protected cleanup task survives cancellation of its caller."""

    controller, gate, harness, order = _components()
    await controller.activate()
    harness.continue_shutdown.clear()

    shutdown = asyncio.create_task(controller.shutdown())
    await asyncio.wait_for(harness.shutdown_started.wait(), timeout=1.0)
    shutdown.cancel()
    harness.continue_shutdown.set()

    with pytest.raises(asyncio.CancelledError):
        await shutdown

    assert controller.snapshot.state is ActorRuntimeHistoryLifecycleState.CLOSED
    assert harness.shutdown_complete is True
    assert gate._permit is None
    assert order[-2:] == ["harness_shutdown", "release"]


@pytest.mark.asyncio
async def test_closed_history_lifecycle_cannot_reactivate() -> None:
    """One released history lifetime cannot silently create a second worker set."""

    controller, _gate, _harness, _order = _components()
    await controller.shutdown()

    with pytest.raises(ActorRuntimeHistoryLifecycleError, match="closed"):
        await controller.activate()
