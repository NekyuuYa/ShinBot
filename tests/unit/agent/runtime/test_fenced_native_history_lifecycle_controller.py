"""Unit coverage for one fenced native-history target lifecycle."""

from __future__ import annotations

import asyncio

import pytest

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealthSnapshot,
    RuntimeServiceStatus,
)
from shinbot.agent.runtime.session_actor.effect_executor import EffectExpiryRecoveryResult
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_supervisor import (
    FencedMailboxHandoffSupervisorShutdown,
    FencedMailboxHandoffSupervisorSnapshot,
    FencedMailboxHandoffSupervisorState,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_target import (
    FencedMailboxHandoffTargetHistoryRecovery,
    FencedMailboxHandoffTargetRetirement,
    FencedMailboxHandoffTargetState,
)
from shinbot.agent.runtime.session_actor.fenced_native_history_lifecycle import (
    FencedNativeHistoryLifecycleController,
    FencedNativeHistoryLifecycleError,
    FencedNativeHistoryLifecycleState,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLease,
    FencedWakeTargetLeaseGrant,
    FencedWakeTargetLeaseStatus,
)
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget


def _binding() -> FencedActorExecutionBinding:
    """Build one local-only target binding for lifecycle tests."""

    request = FencedMailboxWakeRequest(
        key=SessionKey("profile-history", "profile-history:group:room"),
        ownership_generation=3,
        admission_fence_id="history-admission-fence",
        admission_fence_generation=2,
    )
    target = MailboxHandoffTarget("native-history-target", "incarnation-a")
    lease = FencedWakeTargetLease(
        request=request,
        target=target,
        lease_epoch=4,
        status=FencedWakeTargetLeaseStatus.ACTIVE,
        expires_at=120.0,
        created_at=1.0,
        updated_at=1.0,
    )
    return FencedActorExecutionBinding(
        request=request,
        target_lease=FencedWakeTargetLeaseGrant(
            lease=lease,
            holder_token="native-history-test-target-token",
        ),
    )


class _Target:
    """Minimal target that records recovery and supports controlled failures."""

    def __init__(self, domain: object) -> None:
        """Create one new target under a fixed fake persistence domain."""

        self.persistence_domain = domain
        self.execution_binding = _binding()
        self.target_identity = self.execution_binding.target_lease.lease.target
        self.state = FencedMailboxHandoffTargetState.NEW
        self.events: list[str] = []
        self.recovery_error: BaseException | None = None
        self.recovery_started = asyncio.Event()
        self.allow_recovery = asyncio.Event()
        self.allow_recovery.set()

    async def recover_native_history(self) -> FencedMailboxHandoffTargetHistoryRecovery:
        """Return one exact recovery receipt or wait/fail as configured."""

        self.events.append("target.recover")
        self.recovery_started.set()
        await self.allow_recovery.wait()
        if self.recovery_error is not None:
            raise self.recovery_error
        return FencedMailboxHandoffTargetHistoryRecovery(
            actor_wake=FencedMailboxWakeReceipt(
                request=self.execution_binding.request,
                disposition=FencedMailboxWakeDisposition.ACCEPTED,
            ),
            effect_recovery=EffectExpiryRecoveryResult(recovered_count=2),
        )


class _Supervisor:
    """Controlled supervisor double preserving target identity diagnostics."""

    def __init__(self, target: _Target, domain: object) -> None:
        """Bind the fake supervisor to one target and persistence domain."""

        self._target = target
        self.request = target.execution_binding.request
        self.target_identity = target.target_identity
        self.persistence_domain = domain
        self._state = FencedMailboxHandoffSupervisorState.NEW
        self._target_bound = False
        self.events: list[str] = []
        self.stop_blocked = False

    @property
    def snapshot(self) -> FencedMailboxHandoffSupervisorSnapshot:
        """Build a stable token-free snapshot for the current fake state."""

        return FencedMailboxHandoffSupervisorSnapshot(
            state=self._state,
            target=self.target_identity,
            target_state=self._target.state,
            target_bound=self._target_bound,
            binding_matches=(
                self._target_bound
                and self.target_identity == self._target.target_identity
            ),
            persistence_domain_matches=(self.persistence_domain is self._target.persistence_domain),
            health=RuntimeServiceHealthSnapshot(
                service_name="native-history-test-supervisor",
                status=(
                    RuntimeServiceStatus.RUNNING
                    if self._state is FencedMailboxHandoffSupervisorState.ACTIVE
                    else RuntimeServiceStatus.STOPPED
                ),
            ),
        )

    async def start(self) -> FencedMailboxHandoffSupervisorSnapshot:
        """Activate the target and record one target publication."""

        self.events.append("supervisor.start")
        self._target.events.append("target.activate")
        self._target.state = FencedMailboxHandoffTargetState.ACTIVE
        self._state = FencedMailboxHandoffSupervisorState.ACTIVE
        self._target_bound = True
        return self.snapshot

    async def shutdown(self) -> FencedMailboxHandoffSupervisorShutdown:
        """Retire the target or expose a controlled blocked stop."""

        self.events.append("supervisor.shutdown")
        self._target_bound = False
        if self.stop_blocked:
            self._target.state = FencedMailboxHandoffTargetState.BLOCKED
            self._state = FencedMailboxHandoffSupervisorState.BLOCKED
            return FencedMailboxHandoffSupervisorShutdown(
                state=self._state,
                target_state=self._target.state,
                retirement=FencedMailboxHandoffTargetRetirement(
                    state=self._target.state,
                    target_lease_released=False,
                    error="synthetic retirement block",
                ),
                error="synthetic retirement block",
            )
        self._target.events.extend(("target.unpublish", "target.retire"))
        self._target.state = FencedMailboxHandoffTargetState.STOPPED
        self._state = FencedMailboxHandoffSupervisorState.STOPPED
        return FencedMailboxHandoffSupervisorShutdown(
            state=self._state,
            target_state=self._target.state,
            retirement=FencedMailboxHandoffTargetRetirement(
                state=self._target.state,
                target_lease_released=True,
            ),
        )


def _components() -> tuple[FencedNativeHistoryLifecycleController, _Target, _Supervisor]:
    """Compose a same-domain fake target and supervisor lifecycle."""

    domain = object()
    target = _Target(domain)
    supervisor = _Supervisor(target, domain)
    return (
        FencedNativeHistoryLifecycleController(target=target, supervisor=supervisor),
        target,
        supervisor,
    )


@pytest.mark.asyncio
async def test_native_history_lifecycle_recovers_before_target_publication() -> None:
    """Recovery completes before the exact target can bind its dispatcher."""

    controller, target, supervisor = _components()

    active = await controller.activate()

    assert active.state is FencedNativeHistoryLifecycleState.ACTIVE
    assert active.recovery is not None
    assert active.recovery.effect_recovery.recovered_count == 2
    assert target.events[:2] == ["target.recover", "target.activate"]
    assert supervisor.events == ["supervisor.start"]
    assert await controller.verify_active() == active

    closed = await controller.shutdown()

    assert closed.state is FencedNativeHistoryLifecycleState.CLOSED
    assert target.events[-2:] == ["target.unpublish", "target.retire"]
    assert supervisor.events[-1] == "supervisor.shutdown"


@pytest.mark.asyncio
async def test_native_history_start_failure_retires_target_before_returning_error() -> None:
    """A failed recovery cannot leave its actor or target lease alive."""

    controller, target, supervisor = _components()
    target.recovery_error = RuntimeError("synthetic history recovery failure")

    with pytest.raises(RuntimeError, match="synthetic history recovery failure"):
        await controller.activate()

    assert controller.snapshot.state is FencedNativeHistoryLifecycleState.CLOSED
    assert target.state is FencedMailboxHandoffTargetState.STOPPED
    assert supervisor.events == ["supervisor.shutdown"]


@pytest.mark.asyncio
async def test_native_history_activation_cancellation_still_retires_target() -> None:
    """Cancelling startup cannot bypass the target retirement proof."""

    controller, target, supervisor = _components()
    target.allow_recovery.clear()
    activation = asyncio.create_task(controller.activate())
    await asyncio.wait_for(target.recovery_started.wait(), timeout=1.0)
    activation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await activation

    assert controller.snapshot.state is FencedNativeHistoryLifecycleState.CLOSED
    assert target.state is FencedMailboxHandoffTargetState.STOPPED
    assert supervisor.events == ["supervisor.shutdown"]


@pytest.mark.asyncio
async def test_native_history_lifecycle_retains_failed_stop_for_shutdown_retry() -> None:
    """A blocked retirement cannot be reported as a closed lifecycle."""

    controller, target, supervisor = _components()
    await controller.activate()
    supervisor.stop_blocked = True

    with pytest.raises(FencedNativeHistoryLifecycleError, match="shutdown completion"):
        await controller.shutdown()

    assert controller.snapshot.state is FencedNativeHistoryLifecycleState.FAILED
    assert controller.snapshot.cleanup_failed is True
    assert target.state is FencedMailboxHandoffTargetState.BLOCKED
