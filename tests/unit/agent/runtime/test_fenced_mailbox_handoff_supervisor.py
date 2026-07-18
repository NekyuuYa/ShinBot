"""Unit coverage for unmounted fenced mailbox-handoff supervision."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from shinbot.agent.runtime.service_health import RuntimeServiceStatus
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_supervisor import (
    FencedMailboxHandoffSupervisor,
    FencedMailboxHandoffSupervisorState,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_target import (
    FencedMailboxHandoffTargetRetirement,
    FencedMailboxHandoffTargetState,
)
from shinbot.agent.runtime.session_actor.mailbox_handoff_dispatcher import (
    MailboxHandoffDispatchPage,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLease,
    FencedWakeTargetLeaseGrant,
    FencedWakeTargetLeaseStatus,
)
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffClaim,
    FencedMailboxHandoffReceipt,
    MailboxHandoffTarget,
)
from shinbot.persistence.repositories.actor_v2_mailbox_handoff import (
    MailboxHandoffDiscoveryCursor,
)


def _binding() -> FencedActorExecutionBinding:
    """Build one token-bearing test binding that never leaves fake components."""

    request = FencedMailboxWakeRequest(
        key=SessionKey("profile-a", "profile-a:group:room"),
        ownership_generation=2,
        admission_fence_id="admission-fence-a",
        admission_fence_generation=1,
    )
    target = MailboxHandoffTarget("supervisor-target", "incarnation-a")
    lease = FencedWakeTargetLease(
        request=request,
        target=target,
        lease_epoch=1,
        status=FencedWakeTargetLeaseStatus.ACTIVE,
        expires_at=60.0,
        created_at=1.0,
        updated_at=1.0,
    )
    return FencedActorExecutionBinding(
        request=request,
        target_lease=FencedWakeTargetLeaseGrant(
            lease=lease,
            holder_token="test-target-lease-token",
        ),
    )


class _Target:
    """Record target lifecycle calls while retaining an exact renewed binding."""

    def __init__(
        self,
        domain: object,
        *,
        fail_first_renewal: bool = False,
        fail_second_renewal: bool = False,
    ) -> None:
        """Create one inactive test target with one durable authority."""

        self.persistence_domain = domain
        self.execution_binding = _binding()
        self.target_identity = self.execution_binding.target_lease.lease.target
        self.state = FencedMailboxHandoffTargetState.NEW
        self.events: list[str] = []
        self._fail_first_renewal = fail_first_renewal
        self._fail_second_renewal = fail_second_renewal
        self._renewals = 0

    async def activate(self) -> None:
        """Enter the active state before dispatcher binding."""

        self.events.append("target.activate")
        self.state = FencedMailboxHandoffTargetState.ACTIVE

    async def renew_target_lease(
        self,
        *,
        ttl_seconds: float,
    ) -> FencedActorExecutionBinding:
        """Return a same-authority lease with a later expiry or fail deterministically."""

        self.events.append("target.renew")
        self._renewals += 1
        if self._fail_first_renewal or (
            self._fail_second_renewal and self._renewals > 1
        ):
            self.state = FencedMailboxHandoffTargetState.BLOCKED
            raise RuntimeError("renewal lost")
        lease = replace(
            self.execution_binding.target_lease.lease,
            expires_at=60.0 + ttl_seconds + self._renewals,
            updated_at=2.0 + self._renewals,
        )
        self.execution_binding = FencedActorExecutionBinding(
            request=self.execution_binding.request,
            target_lease=FencedWakeTargetLeaseGrant(
                lease=lease,
                holder_token=self.execution_binding.target_lease.holder_token,
            ),
        )
        return self.execution_binding

    async def wake_handoff(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        """Reject accidental direct dispatch outside the fake dispatcher boundary."""

        raise AssertionError(f"unexpected direct handoff: {claim.handoff_id}")

    async def unpublish(self) -> None:
        """Record the required post-unbind publication boundary."""

        self.events.append("target.unpublish")
        if self.state is not FencedMailboxHandoffTargetState.STOPPED:
            self.state = FencedMailboxHandoffTargetState.UNPUBLISHED

    async def retire(
        self,
        *,
        quiescence_timeout_seconds: float | None = None,
    ) -> FencedMailboxHandoffTargetRetirement:
        """Record a successful post-unpublish target retirement."""

        del quiescence_timeout_seconds
        self.events.append("target.retire")
        self.state = FencedMailboxHandoffTargetState.STOPPED
        return FencedMailboxHandoffTargetRetirement(
            state=self.state,
            target_lease_released=True,
        )


class _Dispatcher:
    """Record exact scoped dispatch calls without fabricating handoff work."""

    def __init__(self, domain: object) -> None:
        """Create an unbound dispatcher view over one test persistence domain."""

        self.persistence_domain = domain
        self.target_timeout_seconds = 0.1
        self.target_bound = False
        self.bound_target_identity: MailboxHandoffTarget | None = None
        self.events: list[str] = []
        self.calls: list[dict[str, object]] = []

    def bind_target(
        self,
        _target: _Target,
        *,
        target_identity: MailboxHandoffTarget,
    ) -> int:
        """Bind the one exact target identity for the test."""

        self.events.append("dispatcher.bind")
        self.target_bound = True
        self.bound_target_identity = target_identity
        return 1

    def unbind_target(self) -> None:
        """Forget the target before target-local retirement."""

        self.events.append("dispatcher.unbind")
        self.target_bound = False
        self.bound_target_identity = None

    async def dispatch_pending(
        self,
        *,
        limit: int,
        after: MailboxHandoffDiscoveryCursor | None,
        profile_id: str | None,
        session_id: str | None,
        expected_request: FencedMailboxWakeRequest,
    ) -> MailboxHandoffDispatchPage:
        """Capture complete scope and return an empty terminal page."""

        self.events.append("dispatcher.dispatch")
        self.calls.append(
            {
                "limit": limit,
                "after": after,
                "profile_id": profile_id,
                "session_id": session_id,
                "expected_request": expected_request,
            }
        )
        return MailboxHandoffDispatchPage(
            results=(),
            next_cursor=None,
            has_more=False,
        )


async def _wait_for(predicate: object) -> None:
    """Yield briefly until a synchronous predicate reports its expected state."""

    if not callable(predicate):
        raise TypeError("predicate must be callable")
    for _attempt in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("timed out waiting for supervisor state")


@pytest.mark.asyncio
async def test_supervisor_renews_and_dispatches_only_its_complete_fence_scope() -> None:
    """A target pass supplies ownership and admission fence evidence together."""

    domain = object()
    target = _Target(domain)
    dispatcher = _Dispatcher(domain)
    supervisor = FencedMailboxHandoffSupervisor(
        target=target,
        dispatcher=dispatcher,
        tick_interval_seconds=0.1,
        target_lease_ttl_seconds=1.0,
        dispatch_limit=7,
    )

    await supervisor.start()
    await _wait_for(lambda: bool(dispatcher.calls))
    shutdown = await supervisor.shutdown()

    call = dispatcher.calls[0]
    request = target.execution_binding.request
    assert call["limit"] == 7
    assert call["profile_id"] == request.key.profile_id
    assert call["session_id"] == request.key.session_id
    assert call["expected_request"] == request
    assert shutdown.state is FencedMailboxHandoffSupervisorState.STOPPED
    assert target.state is FencedMailboxHandoffTargetState.STOPPED
    assert dispatcher.target_bound is False
    assert dispatcher.events.index("dispatcher.unbind") < target.events.index(
        "target.unpublish"
    )
    assert target.events.index("target.unpublish") < target.events.index("target.retire")


@pytest.mark.asyncio
async def test_renewal_failure_unbinds_and_retires_the_target_with_degraded_health() -> None:
    """A lost publication capability cannot leave a bound local target alive."""

    domain = object()
    target = _Target(domain, fail_second_renewal=True)
    dispatcher = _Dispatcher(domain)
    supervisor = FencedMailboxHandoffSupervisor(
        target=target,
        dispatcher=dispatcher,
        tick_interval_seconds=0.1,
        target_lease_ttl_seconds=1.0,
    )

    await supervisor.start()
    await _wait_for(lambda: target.state is FencedMailboxHandoffTargetState.STOPPED)

    snapshot = supervisor.snapshot
    assert snapshot.state is FencedMailboxHandoffSupervisorState.STOPPED
    assert snapshot.health.status is RuntimeServiceStatus.DEGRADED
    assert dispatcher.target_bound is False
    assert dispatcher.events.index("dispatcher.unbind") < target.events.index(
        "target.unpublish"
    )
    assert target.events.index("target.unpublish") < target.events.index("target.retire")


@pytest.mark.asyncio
async def test_concurrent_shutdown_serializes_one_target_retirement() -> None:
    """Two lifecycle owners cannot race a second unpublish after a clean stop."""

    domain = object()
    target = _Target(domain)
    dispatcher = _Dispatcher(domain)
    supervisor = FencedMailboxHandoffSupervisor(
        target=target,
        dispatcher=dispatcher,
        tick_interval_seconds=0.1,
        target_lease_ttl_seconds=1.0,
    )

    await supervisor.start()
    first, second = await asyncio.gather(supervisor.shutdown(), supervisor.shutdown())

    assert first.state is FencedMailboxHandoffSupervisorState.STOPPED
    assert second.state is FencedMailboxHandoffSupervisorState.STOPPED
    assert target.events.count("target.unpublish") == 1
    assert target.events.count("target.retire") == 1


@pytest.mark.asyncio
async def test_startup_renewal_failure_records_a_terminal_cleanup_result() -> None:
    """A failed initial renewal cannot leave a retired target as merely blocked."""

    domain = object()
    target = _Target(domain, fail_first_renewal=True)
    dispatcher = _Dispatcher(domain)
    supervisor = FencedMailboxHandoffSupervisor(
        target=target,
        dispatcher=dispatcher,
        tick_interval_seconds=0.1,
        target_lease_ttl_seconds=1.0,
    )

    with pytest.raises(RuntimeError, match="renewal lost"):
        await supervisor.start()

    assert supervisor.snapshot.state is FencedMailboxHandoffSupervisorState.STOPPED
    assert target.state is FencedMailboxHandoffTargetState.STOPPED
    assert (await supervisor.shutdown()).state is FencedMailboxHandoffSupervisorState.STOPPED


def test_supervisor_rejects_a_lease_ttl_shorter_than_its_full_dispatch_budget() -> None:
    """A sequential dispatch page must fit before the next lease renewal window."""

    domain = object()

    with pytest.raises(ValueError, match="bounded dispatch pass"):
        FencedMailboxHandoffSupervisor(
            target=_Target(domain),
            dispatcher=_Dispatcher(domain),
            tick_interval_seconds=0.1,
            target_lease_ttl_seconds=0.3,
            dispatch_limit=3,
        )
