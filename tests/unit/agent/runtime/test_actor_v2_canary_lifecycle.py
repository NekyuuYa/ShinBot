"""Unit coverage for the unmounted Actor v2 clean-canary lifecycle."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from shinbot.agent.runtime.session_actor.canary_lifecycle import (
    ActorV2CanaryIsolationLost,
    ActorV2CanaryLifecycleController,
    ActorV2CanaryLifecycleError,
    ActorV2CanaryLifecycleState,
)
from shinbot.agent.runtime.session_actor.harness import ActorRuntimeActivationScope


class _FakeCanaryHarness:
    """Inert harness double with observable lifecycle ordering."""

    def __init__(self, persistence_domain: object, events: list[str]) -> None:
        self.activation_scope = ActorRuntimeActivationScope.CLEAN_SESSION
        self.persistence_domain = persistence_domain
        self.active = False
        self.closed = False
        self.shutdown_complete = False
        self.events = events
        self.activate_calls = 0
        self.shutdown_drains: list[bool] = []
        self.activation_error: BaseException | None = None
        self.close_on_activation_error = False
        self.after_activate: Callable[[], None] | None = None
        self.shutdown_error: BaseException | None = None

    async def activate(self) -> None:
        """Start the fake harness or reproduce one selected activation failure."""

        self.events.append("harness.activate")
        self.activate_calls += 1
        if self.activation_error is not None:
            if self.close_on_activation_error:
                self.closed = True
            raise self.activation_error
        self.active = True
        if self.after_activate is not None:
            self.after_activate()

    async def shutdown(self, *, drain: bool = True) -> None:
        """Record the stop operation and make the fake harness inert."""

        self.events.append("harness.shutdown")
        self.shutdown_drains.append(drain)
        self.active = False
        self.closed = True
        if self.shutdown_error is not None:
            raise self.shutdown_error
        self.shutdown_complete = True


class _FakeIsolationLease:
    """Mutable same-process isolation proof for lifecycle boundary tests."""

    def __init__(self, persistence_domain: object, events: list[str]) -> None:
        self.persistence_domain = persistence_domain
        self.active = True
        self.events = events
        self.release_calls = 0
        self.release_error: BaseException | None = None

    async def release(self) -> None:
        """Record release and make later observations report an inactive lease."""

        self.events.append("lease.release")
        self.release_calls += 1
        if self.release_error is not None:
            raise self.release_error
        self.active = False


def _controller() -> tuple[
    ActorV2CanaryLifecycleController,
    _FakeCanaryHarness,
    _FakeIsolationLease,
    list[str],
]:
    """Build one inactive clean canary with an active matching lease."""

    events: list[str] = []
    persistence_domain = object()
    harness = _FakeCanaryHarness(persistence_domain, events)
    lease = _FakeIsolationLease(persistence_domain, events)
    controller = ActorV2CanaryLifecycleController(
        harness=harness,
        isolation_lease=lease,
    )
    return controller, harness, lease, events


def test_canary_lifecycle_requires_an_active_same_domain_lease() -> None:
    """Composition refuses a lease that cannot prove clean-domain isolation."""

    events: list[str] = []
    harness = _FakeCanaryHarness(object(), events)
    inactive_lease = _FakeIsolationLease(harness.persistence_domain, events)
    inactive_lease.active = False

    with pytest.raises(ValueError, match="must be active"):
        ActorV2CanaryLifecycleController(
            harness=harness,
            isolation_lease=inactive_lease,
        )

    mismatched_lease = _FakeIsolationLease(object(), events)
    with pytest.raises(ValueError, match="must protect"):
        ActorV2CanaryLifecycleController(
            harness=harness,
            isolation_lease=mismatched_lease,
        )

    assert harness.activate_calls == 0
    assert events == []


@pytest.mark.asyncio
async def test_shutdown_stops_harness_before_releasing_lease() -> None:
    """Normal shutdown has a fixed stop-before-release order and is idempotent."""

    controller, harness, lease, events = _controller()

    activated = await controller.activate()
    assert activated.state is ActorV2CanaryLifecycleState.ACTIVE
    assert activated.harness_active is True
    assert activated.harness_shutdown_complete is False
    assert activated.isolation_lease_active is True
    assert activated.persistence_domain_matches is True
    assert activated.cleanup_failed is False

    closed = await controller.shutdown()
    again = await controller.shutdown()

    assert events == ["harness.activate", "harness.shutdown", "lease.release"]
    assert harness.shutdown_drains == [False]
    assert lease.release_calls == 1
    assert closed.state is ActorV2CanaryLifecycleState.CLOSED
    assert again == closed
    assert closed.harness_active is False
    assert closed.harness_shutdown_complete is True
    assert closed.isolation_lease_active is False
    assert closed.cleanup_failed is False


@pytest.mark.asyncio
async def test_lost_lease_during_activation_stops_harness_and_releases_lease() -> None:
    """A startup race cannot leave an active clean harness behind."""

    controller, harness, lease, events = _controller()
    harness.after_activate = lambda: setattr(lease, "active", False)

    with pytest.raises(ActorV2CanaryIsolationLost, match="lost during startup"):
        await controller.activate()

    assert events == ["harness.activate", "harness.shutdown", "lease.release"]
    assert harness.shutdown_drains == [False]
    assert lease.release_calls == 1
    assert controller.snapshot.state is ActorV2CanaryLifecycleState.CLOSED


@pytest.mark.asyncio
async def test_active_canary_fails_closed_when_later_verification_loses_isolation() -> None:
    """A later cutover boundary observes lease loss before any work can proceed."""

    controller, harness, lease, events = _controller()
    await controller.activate()
    lease.active = False

    with pytest.raises(ActorV2CanaryIsolationLost, match="no longer active"):
        await controller.verify_active_isolation()

    assert events == ["harness.activate", "harness.shutdown", "lease.release"]
    assert harness.shutdown_drains == [False]
    assert lease.release_calls == 1
    assert controller.snapshot.state is ActorV2CanaryLifecycleState.CLOSED


@pytest.mark.asyncio
async def test_preflight_style_failure_keeps_open_canary_retryable() -> None:
    """A harness that rejects before starting does not consume its lease."""

    controller, harness, lease, events = _controller()
    harness.activation_error = RuntimeError("clean preflight rejected")

    with pytest.raises(RuntimeError, match="clean preflight rejected"):
        await controller.activate()

    assert controller.snapshot.state is ActorV2CanaryLifecycleState.READY
    assert harness.closed is False
    assert lease.release_calls == 0
    assert events == ["harness.activate"]

    harness.activation_error = None
    activated = await controller.activate()
    assert activated.state is ActorV2CanaryLifecycleState.ACTIVE

    await controller.shutdown()
    assert events == [
        "harness.activate",
        "harness.activate",
        "harness.shutdown",
        "lease.release",
    ]


@pytest.mark.asyncio
async def test_closed_harness_activation_failure_releases_the_lease() -> None:
    """A failed partial startup cannot strand the controller's isolation proof."""

    controller, harness, lease, events = _controller()
    harness.activation_error = RuntimeError("handler worker startup failed")
    harness.close_on_activation_error = True

    with pytest.raises(RuntimeError, match="handler worker startup failed"):
        await controller.activate()

    assert events == ["harness.activate", "harness.shutdown", "lease.release"]
    assert harness.shutdown_drains == [False]
    assert lease.release_calls == 1
    assert controller.snapshot.state is ActorV2CanaryLifecycleState.CLOSED


@pytest.mark.asyncio
async def test_domain_drift_before_startup_fails_closed_without_activating() -> None:
    """A mutable lease cannot switch durable domains after composition."""

    controller, harness, lease, events = _controller()
    lease.persistence_domain = object()

    with pytest.raises(ActorV2CanaryLifecycleError, match="composition changed"):
        await controller.activate()

    assert harness.activate_calls == 0
    assert events == ["harness.shutdown", "lease.release"]
    assert lease.release_calls == 1
    assert controller.snapshot.state is ActorV2CanaryLifecycleState.CLOSED


@pytest.mark.asyncio
async def test_shutdown_retains_lease_until_harness_stop_is_proven() -> None:
    """A stop failure cannot turn a still-uncertain worker into an unsafe release."""

    controller, harness, lease, events = _controller()
    await controller.activate()
    harness.shutdown_error = RuntimeError("executor stop did not complete")

    with pytest.raises(RuntimeError, match="executor stop did not complete"):
        await controller.shutdown()

    failed = controller.snapshot
    assert events == ["harness.activate", "harness.shutdown"]
    assert lease.release_calls == 0
    assert failed.state is ActorV2CanaryLifecycleState.FAILED
    assert failed.harness_shutdown_complete is False
    assert failed.isolation_lease_active is True
    assert failed.cleanup_failed is True

    harness.shutdown_error = None
    closed = await controller.shutdown()

    assert events == [
        "harness.activate",
        "harness.shutdown",
        "harness.shutdown",
        "lease.release",
    ]
    assert closed.state is ActorV2CanaryLifecycleState.CLOSED
    assert lease.release_calls == 1


@pytest.mark.asyncio
async def test_shutdown_retries_a_failed_lease_release_after_harness_stops() -> None:
    """A release error retains isolation and permits an explicit retry only."""

    controller, harness, lease, events = _controller()
    await controller.activate()
    lease.release_error = RuntimeError("durable lease release unavailable")

    with pytest.raises(RuntimeError, match="durable lease release unavailable"):
        await controller.shutdown()

    failed = controller.snapshot
    assert events == ["harness.activate", "harness.shutdown", "lease.release"]
    assert harness.shutdown_complete is True
    assert failed.state is ActorV2CanaryLifecycleState.FAILED
    assert failed.harness_shutdown_complete is True
    assert failed.isolation_lease_active is True
    assert failed.cleanup_failed is True

    lease.release_error = None
    closed = await controller.shutdown()

    assert events == [
        "harness.activate",
        "harness.shutdown",
        "lease.release",
        "lease.release",
    ]
    assert closed.state is ActorV2CanaryLifecycleState.CLOSED
    assert lease.release_calls == 2
