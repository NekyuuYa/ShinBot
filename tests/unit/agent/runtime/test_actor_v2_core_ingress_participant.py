"""Unit coverage for the unmounted process-level core-ingress participant."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from shinbot.agent.runtime.actor_v2_core_ingress_participant import (
    ActorV2CoreIngressParticipantLifecycle,
    ActorV2CoreIngressParticipantLifecycleState,
    ActorV2CoreIngressParticipantRetirementBlocked,
)
from shinbot.agent.runtime.service_health import RuntimeServiceStatus
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainAcknowledgement,
    ActorV2CoreIngressDrainDiscoveryCursor,
    ActorV2CoreIngressDrainDiscoveryPage,
    ActorV2CoreIngressDrainReceipt,
    ActorV2CoreIngressDrainRequest,
)
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressDrainConflict,
    ActorV2IngressParticipant,
    ActorV2IngressParticipantGrant,
    ActorV2IngressParticipantStatus,
)


class _MembershipRepository:
    """Controlled local membership repository with durable-looking grants."""

    def __init__(self, domain: object, events: list[str]) -> None:
        self.persistence_domain = domain
        self._events = events
        self._grants: tuple[ActorV2IngressParticipantGrant, ...] = ()
        self._heartbeat_index = 0
        self.retire_error: BaseException | None = None

    def register_participants(
        self,
        *,
        adapter_instance_ids: tuple[str, ...],
        participant_id: str,
        participant_epoch: int,
    ) -> tuple[ActorV2IngressParticipantGrant, ...]:
        """Register every requested adapter as one active process incarnation."""

        self._events.append("membership.register")
        self._grants = tuple(
            ActorV2IngressParticipantGrant(
                participant=ActorV2IngressParticipant(
                    member_id=f"member:{adapter_instance_id}",
                    adapter_instance_id=adapter_instance_id,
                    participant_id=participant_id,
                    participant_epoch=participant_epoch,
                    status=ActorV2IngressParticipantStatus.ACTIVE,
                    registered_at=1.0,
                    last_heartbeat_at=1.0,
                    updated_at=1.0,
                ),
                holder_token=f"holder:{adapter_instance_id}",
            )
            for adapter_instance_id in adapter_instance_ids
        )
        return self._grants

    def heartbeat_participants(
        self,
        grants: tuple[ActorV2IngressParticipantGrant, ...],
    ) -> tuple[ActorV2IngressParticipant, ...]:
        """Return the same active scope with a monotonic advisory observation."""

        assert grants == self._grants
        self._events.append("membership.heartbeat")
        self._heartbeat_index += 1
        observed_at = 1.0 + self._heartbeat_index
        return tuple(
            ActorV2IngressParticipant(
                member_id=grant.participant.member_id,
                adapter_instance_id=grant.participant.adapter_instance_id,
                participant_id=grant.participant.participant_id,
                participant_epoch=grant.participant.participant_epoch,
                status=ActorV2IngressParticipantStatus.ACTIVE,
                registered_at=1.0,
                last_heartbeat_at=observed_at,
                updated_at=observed_at,
            )
            for grant in grants
        )

    def retire_participants(
        self,
        grants: tuple[ActorV2IngressParticipantGrant, ...],
    ) -> tuple[ActorV2IngressParticipant, ...]:
        """Retire every member or preserve the complete set behind one conflict."""

        assert grants == self._grants
        self._events.append("membership.retire")
        if self.retire_error is not None:
            raise self.retire_error
        return tuple(
            ActorV2IngressParticipant(
                member_id=grant.participant.member_id,
                adapter_instance_id=grant.participant.adapter_instance_id,
                participant_id=grant.participant.participant_id,
                participant_epoch=grant.participant.participant_epoch,
                status=ActorV2IngressParticipantStatus.RETIRED,
                registered_at=1.0,
                last_heartbeat_at=1.0 + self._heartbeat_index,
                updated_at=10.0,
                retired_at=10.0,
            )
            for grant in grants
        )


class _CoreDrainRepository:
    """Empty durable core-drain discovery surface for lifecycle sequencing."""

    def __init__(self, domain: object) -> None:
        self.persistence_domain = domain

    def get(self, request_id: str) -> ActorV2CoreIngressDrainRequest | None:
        """No drain request is configured for these lifecycle-only tests."""

        del request_id
        return None

    def acknowledge_quiescent(
        self,
        *,
        request_id: str,
        participant_grant: ActorV2IngressParticipantGrant,
        receipt: ActorV2CoreIngressDrainReceipt,
    ) -> ActorV2CoreIngressDrainAcknowledgement:
        """Fail if an unexpected worker acknowledgement reaches this fake."""

        del request_id, participant_grant, receipt
        raise AssertionError("empty core drain fake must not receive an acknowledgement")

    def discover_open_for_participant(
        self,
        participant_id: str,
        *,
        limit: int,
        after: ActorV2CoreIngressDrainDiscoveryCursor | None = None,
    ) -> ActorV2CoreIngressDrainDiscoveryPage:
        """Report no local drain work while exercising lifecycle ownership only."""

        del participant_id, limit, after
        return ActorV2CoreIngressDrainDiscoveryPage(requests=())


class _LegacyDrain:
    """Unused local drain port retained for real worker construction."""

    def freeze(self, request: object) -> object:
        """Reject a call because the fake discovery repository is empty."""

        del request
        raise AssertionError("empty core drain fake must not freeze legacy ingress")

    async def drain(self, ticket: object, *, timeout_seconds: float | None = None) -> object:
        """Reject a call because the fake discovery repository is empty."""

        del ticket, timeout_seconds
        raise AssertionError("empty core drain fake must not drain legacy ingress")


@dataclass(slots=True)
class _CallbackIngress:
    """Controlled adapter callback boundary that records ordering evidence."""

    adapter_instance_id: str
    events: list[str]
    membership: _MembershipRepository
    receiving_callbacks: bool = False
    stops: int = 0
    start_error: BaseException | None = None
    start_entered: asyncio.Event | None = None
    allow_start: asyncio.Event | None = None

    async def start_receiving_callbacks(self) -> None:
        """Assert durable membership exists before callback admission begins."""

        assert self.membership._grants
        self.events.append(f"adapter.start:{self.adapter_instance_id}")
        self.receiving_callbacks = True
        if self.start_entered is not None:
            self.start_entered.set()
        if self.allow_start is not None:
            await self.allow_start.wait()
        if self.start_error is not None:
            raise self.start_error

    async def stop_receiving_callbacks(self) -> None:
        """Stop local callback admission before the lifecycle can retire it."""

        self.events.append(f"adapter.stop:{self.adapter_instance_id}")
        self.stops += 1
        self.receiving_callbacks = False


def _lifecycle(
    *,
    callback_b_error: BaseException | None = None,
    callback_a_entered: asyncio.Event | None = None,
    callback_a_allow: asyncio.Event | None = None,
    heartbeat_interval_seconds: float = 60.0,
) -> tuple[
    ActorV2CoreIngressParticipantLifecycle,
    _MembershipRepository,
    dict[str, _CallbackIngress],
    list[str],
]:
    """Compose one inactive process participant over controlled local ports."""

    domain = object()
    events: list[str] = []
    membership = _MembershipRepository(domain, events)
    callbacks = {
        "adapter-a": _CallbackIngress(
            "adapter-a",
            events,
            membership,
            start_entered=callback_a_entered,
            allow_start=callback_a_allow,
        ),
        "adapter-b": _CallbackIngress(
            "adapter-b",
            events,
            membership,
            start_error=callback_b_error,
        ),
    }
    lifecycle = ActorV2CoreIngressParticipantLifecycle(
        membership_repository=membership,
        core_drain_repository=_CoreDrainRepository(domain),
        callback_ingresses=callbacks,
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
        legacy_drain=_LegacyDrain(),  # type: ignore[arg-type]
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        core_drain_tick_interval_seconds=60.0,
    )
    return lifecycle, membership, callbacks, events


@pytest.mark.asyncio
async def test_process_participant_registers_before_callbacks_and_retires_after_stop() -> None:
    """The complete member set brackets all callback admission and retirement."""

    lifecycle, _membership, callbacks, events = _lifecycle()

    active = await lifecycle.activate()

    assert active.state is ActorV2CoreIngressParticipantLifecycleState.ACTIVE
    assert active.receiving_callback_adapter_ids == ("adapter-a", "adapter-b")
    assert tuple(participant.adapter_instance_id for participant in active.participants) == (
        "adapter-a",
        "adapter-b",
    )
    assert events.index("membership.register") < events.index("adapter.start:adapter-a")
    assert events.index("membership.heartbeat") < events.index("adapter.start:adapter-a")

    closed = await lifecycle.shutdown()

    assert closed.state is ActorV2CoreIngressParticipantLifecycleState.CLOSED
    assert closed.members_retired is True
    assert not callbacks["adapter-a"].receiving_callbacks
    assert not callbacks["adapter-b"].receiving_callbacks
    retire_index = events.index("membership.retire")
    assert events.index("adapter.stop:adapter-b") < retire_index
    assert events.index("adapter.stop:adapter-a") < retire_index


@pytest.mark.asyncio
async def test_process_participant_keeps_stopped_member_visible_when_retirement_blocks() -> None:
    """An unacknowledged drain keeps the entire process membership and service live."""

    lifecycle, membership, callbacks, _events = _lifecycle()
    membership.retire_error = ActorV2IngressDrainConflict(
        "participant cannot terminate before acknowledging core drain request core-request-a"
    )
    await lifecycle.activate()

    with pytest.raises(ActorV2CoreIngressParticipantRetirementBlocked):
        await lifecycle.shutdown()

    blocked = lifecycle.snapshot
    assert blocked.state is ActorV2CoreIngressParticipantLifecycleState.RETIRE_BLOCKED
    assert blocked.members_retired is False
    assert blocked.receiving_callback_adapter_ids == ()
    assert blocked.drain_service_health.status is not RuntimeServiceStatus.STOPPED
    assert not callbacks["adapter-a"].receiving_callbacks
    assert not callbacks["adapter-b"].receiving_callbacks
    assert callbacks["adapter-a"].stops == callbacks["adapter-b"].stops == 1

    membership.retire_error = None
    closed = await lifecycle.shutdown()

    assert closed.state is ActorV2CoreIngressParticipantLifecycleState.CLOSED
    assert closed.members_retired is True
    assert callbacks["adapter-a"].stops == callbacks["adapter-b"].stops == 1


@pytest.mark.asyncio
async def test_process_participant_maintains_and_stops_advisory_heartbeats() -> None:
    """Periodic liveness observations continue while active and stop after retirement."""

    lifecycle, _membership, _callbacks, events = _lifecycle(
        heartbeat_interval_seconds=0.01,
    )
    await lifecycle.activate()

    for _ in range(100):
        if events.count("membership.heartbeat") >= 2:
            break
        await asyncio.sleep(0.01)
    else:
        pytest.fail("process participant did not record a periodic heartbeat")

    await lifecycle.shutdown()
    heartbeat_count = events.count("membership.heartbeat")
    await asyncio.sleep(0.03)

    assert events.count("membership.heartbeat") == heartbeat_count


@pytest.mark.asyncio
async def test_process_participant_start_failure_stops_callbacks_before_retiring_members() -> None:
    """A partly started adapter cannot leave a registered process behind."""

    lifecycle, _membership, callbacks, events = _lifecycle(
        callback_b_error=RuntimeError("synthetic callback startup failure"),
    )

    with pytest.raises(RuntimeError, match="synthetic callback startup failure"):
        await lifecycle.activate()

    assert lifecycle.snapshot.state is ActorV2CoreIngressParticipantLifecycleState.CLOSED
    assert not callbacks["adapter-a"].receiving_callbacks
    assert not callbacks["adapter-b"].receiving_callbacks
    retire_index = events.index("membership.retire")
    assert events.index("adapter.stop:adapter-b") < retire_index
    assert events.index("adapter.stop:adapter-a") < retire_index


@pytest.mark.asyncio
async def test_process_participant_activation_cancellation_still_retires_members() -> None:
    """Cancellation while a callback starts cannot bypass ordered local cleanup."""

    entered = asyncio.Event()
    allow = asyncio.Event()
    lifecycle, _membership, callbacks, events = _lifecycle(
        callback_a_entered=entered,
        callback_a_allow=allow,
    )
    activation = asyncio.create_task(lifecycle.activate())
    await asyncio.wait_for(entered.wait(), timeout=1.0)
    activation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await activation

    assert lifecycle.snapshot.state is ActorV2CoreIngressParticipantLifecycleState.CLOSED
    assert not callbacks["adapter-a"].receiving_callbacks
    retire_index = events.index("membership.retire")
    assert events.index("adapter.stop:adapter-a") < retire_index
