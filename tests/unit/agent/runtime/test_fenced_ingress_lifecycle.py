"""Unit coverage for the explicit fenced target-plus-ingress lifecycle."""

from __future__ import annotations

import asyncio

import pytest

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealthSnapshot,
    RuntimeServiceStatus,
)
from shinbot.agent.runtime.session_actor.fenced_ingress_lifecycle import (
    FencedIngressLifecycleController,
    FencedIngressLifecycleError,
    FencedIngressLifecycleState,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_supervisor import (
    FencedMailboxHandoffSupervisorSnapshot,
    FencedMailboxHandoffSupervisorState,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_target import (
    FencedMailboxHandoffTargetState,
)
from shinbot.agent.runtime.session_actor.fenced_native_history_lifecycle import (
    FencedNativeHistoryLifecycleSnapshot,
    FencedNativeHistoryLifecycleState,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.durable_routing_service import (
    DurableRoutingHealthSnapshot,
    DurableRoutingServiceStatus,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget


def _request() -> FencedMailboxWakeRequest:
    """Build one exact admission-fenced owner request for lifecycle tests."""

    return FencedMailboxWakeRequest(
        key=SessionKey("profile-ingress", "profile-ingress:group:room"),
        ownership_generation=4,
        admission_fence_id="ingress-admission-fence",
        admission_fence_generation=3,
    )


def _history_snapshot(
    request: FencedMailboxWakeRequest,
    *,
    state: FencedNativeHistoryLifecycleState,
) -> FencedNativeHistoryLifecycleSnapshot:
    """Build typed history diagnostics for one controlled fake lifecycle."""

    target_state = {
        FencedNativeHistoryLifecycleState.READY: FencedMailboxHandoffTargetState.NEW,
        FencedNativeHistoryLifecycleState.ACTIVE: FencedMailboxHandoffTargetState.ACTIVE,
        FencedNativeHistoryLifecycleState.FAILED: FencedMailboxHandoffTargetState.BLOCKED,
        FencedNativeHistoryLifecycleState.CLOSED: FencedMailboxHandoffTargetState.STOPPED,
    }[state]
    supervisor_state = {
        FencedNativeHistoryLifecycleState.READY: FencedMailboxHandoffSupervisorState.NEW,
        FencedNativeHistoryLifecycleState.ACTIVE: FencedMailboxHandoffSupervisorState.ACTIVE,
        FencedNativeHistoryLifecycleState.FAILED: FencedMailboxHandoffSupervisorState.BLOCKED,
        FencedNativeHistoryLifecycleState.CLOSED: FencedMailboxHandoffSupervisorState.STOPPED,
    }[state]
    target = MailboxHandoffTarget("fenced-ingress-test", "target-incarnation")
    return FencedNativeHistoryLifecycleSnapshot(
        state=state,
        request=request,
        target=target,
        target_state=target_state,
        supervisor=FencedMailboxHandoffSupervisorSnapshot(
            state=supervisor_state,
            target=target,
            target_state=target_state,
            target_bound=state is FencedNativeHistoryLifecycleState.ACTIVE,
            binding_matches=state is FencedNativeHistoryLifecycleState.ACTIVE,
            persistence_domain_matches=True,
            health=RuntimeServiceHealthSnapshot(
                service_name="fenced-ingress-history-test",
                status=(
                    RuntimeServiceStatus.RUNNING
                    if state is FencedNativeHistoryLifecycleState.ACTIVE
                    else RuntimeServiceStatus.STOPPED
                ),
            ),
        ),
        recovery=None,
        shutdown=None,
        persistence_domain_matches=True,
        cleanup_failed=state is FencedNativeHistoryLifecycleState.FAILED,
    )


class _History:
    """Controlled target lifecycle that exposes typed, same-request snapshots."""

    def __init__(self, request: FencedMailboxWakeRequest, domain: object, events: list[str]) -> None:
        self._request = request
        self.persistence_domain = domain
        self._events = events
        self._state = FencedNativeHistoryLifecycleState.READY
        self.activate_error: BaseException | None = None
        self.verify_error: BaseException | None = None
        self.activate_started = asyncio.Event()
        self.allow_activate = asyncio.Event()
        self.allow_activate.set()

    @property
    def snapshot(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Return the current typed fake history state."""

        return _history_snapshot(self._request, state=self._state)

    async def activate(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Activate after an optional wait or controlled failure."""

        self._events.append("history.activate")
        self.activate_started.set()
        await self.allow_activate.wait()
        if self.activate_error is not None:
            raise self.activate_error
        self._state = FencedNativeHistoryLifecycleState.ACTIVE
        return self.snapshot

    async def verify_active(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Return active state or fail like a real target lifecycle."""

        self._events.append("history.verify")
        if self.verify_error is not None:
            raise self.verify_error
        if self._state is not FencedNativeHistoryLifecycleState.ACTIVE:
            raise RuntimeError("synthetic history target is not active")
        return self.snapshot

    async def shutdown(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Retire this fake target after the relay has stopped."""

        self._events.append("history.shutdown")
        self._state = FencedNativeHistoryLifecycleState.CLOSED
        return self.snapshot


class _Relay:
    """Controlled request-scoped relay with a shared fake durable domain."""

    def __init__(self, request: FencedMailboxWakeRequest, domain: object, events: list[str]) -> None:
        self.request = request
        self.persistence_domain = domain
        self._events = events
        self.started = False
        self.start_error: BaseException | None = None
        self.scope_live_override: bool | None = None

    def health_snapshot(self) -> DurableRoutingHealthSnapshot:
        """Expose minimal service diagnostics for lifecycle state checks."""

        return DurableRoutingHealthSnapshot(
            status=(
                DurableRoutingServiceStatus.RUNNING
                if self.started
                else DurableRoutingServiceStatus.STOPPED
            ),
            prepared=self.started,
            started=self.started,
            adapters_ready=self.started,
            actor_consumer_ready=self.started,
            ready_for_actor_traffic=self.started,
            degraded_reason="",
            worker_id="fenced-ingress-relay-test",
            pending_job_count=0,
            pending_delivery_count=0,
            active_actor_ownership_count=1 if self.started else 0,
            wake_debt_count=0,
            active_job_id="",
            active_delivery_id="",
            last_scan_at=0.0,
            last_success_at=0.0,
            last_error_at=0.0,
            last_error_code="",
            last_error_message="",
            consecutive_failures=0,
            processed_job_count=0,
            relayed_delivery_count=0,
            fenced_request_scoped=True,
            fenced_scope_live=(
                self.started
                if self.scope_live_override is None
                else self.scope_live_override
            ),
        )

    async def start(self) -> DurableRoutingHealthSnapshot:
        """Start or fail in a controlled way."""

        self._events.append("relay.start")
        if self.start_error is not None:
            raise self.start_error
        self.started = True
        return self.health_snapshot()

    async def shutdown(self) -> None:
        """Stop this fake relay before target retirement."""

        self._events.append("relay.shutdown")
        self.started = False


def _components() -> tuple[FencedIngressLifecycleController, _History, _Relay, list[str]]:
    """Compose one new same-domain target and relay controller."""

    request = _request()
    domain = object()
    events: list[str] = []
    history = _History(request, domain, events)
    relay = _Relay(request, domain, events)
    return FencedIngressLifecycleController(history=history, relay=relay), history, relay, events


@pytest.mark.asyncio
async def test_fenced_ingress_starts_relay_only_after_target_publication() -> None:
    """The target becomes active before its request-scoped routing worker runs."""

    controller, _history, relay, events = _components()

    active = await controller.activate()

    assert active.state is FencedIngressLifecycleState.ACTIVE
    assert relay.started is True
    assert events == ["history.activate", "relay.start"]

    closed = await controller.shutdown()

    assert closed.state is FencedIngressLifecycleState.CLOSED
    assert events == [
        "history.activate",
        "relay.start",
        "relay.shutdown",
        "history.shutdown",
    ]


@pytest.mark.asyncio
async def test_fenced_ingress_start_failure_stops_relay_before_retiring_target() -> None:
    """A failed scoped relay cannot leave a published target behind."""

    controller, _history, relay, events = _components()
    relay.start_error = RuntimeError("synthetic relay startup failure")

    with pytest.raises(RuntimeError, match="synthetic relay startup failure"):
        await controller.activate()

    assert controller.snapshot.state is FencedIngressLifecycleState.CLOSED
    assert relay.started is False
    assert events == [
        "history.activate",
        "relay.start",
        "relay.shutdown",
        "history.shutdown",
    ]


@pytest.mark.asyncio
async def test_fenced_ingress_verification_stops_target_after_relay_loss() -> None:
    """A relay that stops outside the controller forces ordered retirement."""

    controller, _history, relay, events = _components()
    await controller.activate()
    relay.started = False

    with pytest.raises(FencedIngressLifecycleError, match="no longer healthy"):
        await controller.verify_active()

    assert controller.snapshot.state is FencedIngressLifecycleState.CLOSED
    assert events[-2:] == ["relay.shutdown", "history.shutdown"]


@pytest.mark.asyncio
async def test_fenced_ingress_target_verification_failure_stops_relay() -> None:
    """A target-side health error cannot leave scoped ingress polling alive."""

    controller, history, relay, events = _components()
    await controller.activate()
    history.verify_error = RuntimeError("synthetic target verification failure")

    with pytest.raises(RuntimeError, match="synthetic target verification failure"):
        await controller.verify_active()

    assert controller.snapshot.state is FencedIngressLifecycleState.CLOSED
    assert relay.started is False
    assert events[-2:] == ["relay.shutdown", "history.shutdown"]


@pytest.mark.asyncio
async def test_fenced_ingress_retires_target_when_its_fence_scope_is_lost() -> None:
    """A live worker cannot remain active after its exact admission scope expires."""

    controller, _history, relay, events = _components()
    await controller.activate()
    relay.scope_live_override = False

    with pytest.raises(FencedIngressLifecycleError, match="no longer healthy"):
        await controller.verify_active()

    assert controller.snapshot.state is FencedIngressLifecycleState.CLOSED
    assert relay.started is False
    assert events[-2:] == ["relay.shutdown", "history.shutdown"]


@pytest.mark.asyncio
async def test_fenced_ingress_activation_cancellation_still_cleans_up() -> None:
    """Cancelling before target publication cannot leave future relay authority live."""

    controller, history, relay, events = _components()
    history.allow_activate.clear()
    activation = asyncio.create_task(controller.activate())
    await asyncio.wait_for(history.activate_started.wait(), timeout=1.0)
    activation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await activation

    assert controller.snapshot.state is FencedIngressLifecycleState.CLOSED
    assert relay.started is False
    assert events == ["history.activate", "relay.shutdown", "history.shutdown"]
