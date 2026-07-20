"""Unit coverage for the unmounted core-ingress drain process worker."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pytest

from shinbot.agent.runtime.actor_v2_core_ingress_drain_worker import (
    ActorV2CoreIngressDrainProcessWorker,
    ActorV2CoreIngressDrainWorkerConflict,
    ActorV2CoreIngressDrainWorkerStatus,
)
from shinbot.agent.runtime.legacy_session_local_drain import (
    LegacySessionLocalDrainReceipt,
    LegacySessionLocalDrainRequest,
    LegacySessionLocalDrainTicket,
)
from shinbot.agent.runtime.legacy_session_quiescence import (
    LegacySessionAllProfilesTaskQuiescence,
)
from shinbot.agent.runtime.legacy_signal_admission import (
    LegacyAgentSignalFreezeTicket,
    LegacyAgentSignalQuiescenceReceipt,
    LegacyAgentSignalQuiescenceStatus,
)
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainAcknowledgement,
    ActorV2CoreIngressDrainMember,
    ActorV2CoreIngressDrainReceipt,
    ActorV2CoreIngressDrainRequest,
    ActorV2CoreIngressDrainStatus,
)
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressParticipant,
    ActorV2IngressParticipantGrant,
    ActorV2IngressParticipantStatus,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.legacy_ingress_quiescence import (
    LegacyIngressFreezeTicket,
    LegacyIngressQuiescenceReceipt,
    LegacyIngressQuiescenceStatus,
)
from shinbot.core.dispatch.message_context import (
    WaitingInputFreezeTicket,
    WaitingInputQuiescenceReceipt,
)


def _digest(value: str) -> str:
    """Build deterministic opaque proof metadata for the fake repository."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _member(
    *,
    member_id: str,
    adapter_instance_id: str,
    participant_id: str = "process-a:incarnation-a",
    participant_epoch: int = 1,
) -> ActorV2CoreIngressDrainMember:
    """Build one frozen core-drain member for a test request."""

    return ActorV2CoreIngressDrainMember(
        request_id="core-request-a",
        member_id=member_id,
        adapter_instance_id=adapter_instance_id,
        participant_id=participant_id,
        participant_epoch=participant_epoch,
    )


def _request(
    *members: ActorV2CoreIngressDrainMember,
) -> ActorV2CoreIngressDrainRequest:
    """Build one open barrier-bound request covering the given members."""

    return ActorV2CoreIngressDrainRequest(
        request_id="core-request-a",
        barrier_id="migration-barrier-a",
        key=SessionKey("profile-a", "profile-a:group:room"),
        legacy_session_id="legacy-session-a",
        adapter_instance_ids=tuple(
            sorted({member.adapter_instance_id for member in members})
        ),
        source_generation=4,
        migration_generation=5,
        status=ActorV2CoreIngressDrainStatus.OPEN,
        created_at=1.0,
        updated_at=1.0,
        drained_at=None,
        members=members,
    )


def _grant(member: ActorV2CoreIngressDrainMember) -> ActorV2IngressParticipantGrant:
    """Build one local opaque participant capability for a frozen member."""

    return ActorV2IngressParticipantGrant(
        participant=ActorV2IngressParticipant(
            member_id=member.member_id,
            adapter_instance_id=member.adapter_instance_id,
            participant_id=member.participant_id,
            participant_epoch=member.participant_epoch,
            status=ActorV2IngressParticipantStatus.ACTIVE,
            registered_at=1.0,
            last_heartbeat_at=1.0,
            updated_at=1.0,
        ),
        holder_token=f"holder-token:{member.member_id}",
    )


class _Repository:
    """In-memory durable control-plane stand-in for direct worker tests."""

    def __init__(self, request: ActorV2CoreIngressDrainRequest) -> None:
        self.request = request
        self.acknowledgements: dict[str, ActorV2CoreIngressDrainAcknowledgement] = {}

    def get(self, request_id: str) -> ActorV2CoreIngressDrainRequest | None:
        """Return the configured request only for its exact durable id."""

        return self.request if request_id == self.request.request_id else None

    def acknowledge_quiescent(
        self,
        *,
        request_id: str,
        participant_grant: ActorV2IngressParticipantGrant,
        receipt: ActorV2CoreIngressDrainReceipt,
    ) -> ActorV2CoreIngressDrainAcknowledgement:
        """Persist a deterministic idempotent acknowledgement in memory."""

        member_id = participant_grant.participant.member_id
        existing = self.acknowledgements.get(member_id)
        if existing is not None:
            assert existing.receipt == receipt
            return existing
        acknowledgement = ActorV2CoreIngressDrainAcknowledgement(
            request_id=request_id,
            member_id=member_id,
            core_ingress_digest=receipt.core_ingress_digest,
            legacy_quiescence_digest=receipt.legacy_quiescence_digest,
            proof_epoch=receipt.proof_epoch,
            summary_code=receipt.summary_code,
            acknowledged_at=1.0,
        )
        self.acknowledgements[member_id] = acknowledgement
        return acknowledgement


@dataclass(slots=True)
class _LegacyDrain:
    """Fake local drain with controllable receipts and opaque ticket material."""

    events: list[str]
    outcomes: list[bool]
    defer_signal_freeze: bool = False
    freeze_calls: int = 0
    drain_calls: int = 0

    def freeze(self, request: LegacySessionLocalDrainRequest) -> LegacySessionLocalDrainTicket:
        """Freeze the exact local session once for this request."""

        self.freeze_calls += 1
        self.events.append("legacy.freeze")
        return LegacySessionLocalDrainTicket(
            request=request,
            ingress_ticket=LegacyIngressFreezeTicket(
                session_id=request.legacy_session_id,
                cutover_id=request.cutover_id,
                freeze_epoch=1,
                token="legacy-ingress-token",
            ),
            waiting_input_ticket=WaitingInputFreezeTicket(
                scope=request.waiting_input_scope,
                cutover_id=request.cutover_id,
                token="waiting-input-token",
            ),
            signal_ticket=(
                None
                if self.defer_signal_freeze
                else LegacyAgentSignalFreezeTicket(
                    session_id=request.legacy_session_id,
                    cutover_id=request.cutover_id,
                    freeze_epoch=1,
                    token="legacy-signal-token",
                )
            ),
        )

    async def drain(
        self,
        ticket: LegacySessionLocalDrainTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalDrainReceipt:
        """Return the next complete or retryable local-drain observation."""

        del timeout_seconds
        self.drain_calls += 1
        self.events.append("legacy.drain")
        quiescent = self.outcomes.pop(0)
        active_ticket = ticket
        if ticket.signal_ticket is None:
            active_ticket = ticket.with_signal_ticket(
                LegacyAgentSignalFreezeTicket(
                    session_id=ticket.request.legacy_session_id,
                    cutover_id=ticket.request.cutover_id,
                    freeze_epoch=1,
                    token="legacy-signal-token",
                )
            )
        return LegacySessionLocalDrainReceipt(
            ticket=active_ticket,
            ingress=LegacyIngressQuiescenceReceipt(
                ticket=ticket.ingress_ticket,
                status=(
                    LegacyIngressQuiescenceStatus.QUIESCENT
                    if quiescent
                    else LegacyIngressQuiescenceStatus.TIMED_OUT
                ),
            ),
            waiting_input=WaitingInputQuiescenceReceipt(
                ticket=ticket.waiting_input_ticket,
                quiescent=quiescent,
            ),
            agent_signals=LegacyAgentSignalQuiescenceReceipt(
                ticket=active_ticket.signal_ticket,
                status=(
                    LegacyAgentSignalQuiescenceStatus.QUIESCENT
                    if quiescent
                    else LegacyAgentSignalQuiescenceStatus.TIMED_OUT
                ),
            ),
            agent_tasks=LegacySessionAllProfilesTaskQuiescence(
                session_id=ticket.request.legacy_session_id,
                observations=(),
            ),
        )


def _worker(
    request: ActorV2CoreIngressDrainRequest,
    legacy_drain: _LegacyDrain,
) -> tuple[ActorV2CoreIngressDrainProcessWorker, _Repository]:
    """Build one worker whose grants exactly cover process-a request members."""

    repository = _Repository(request)
    grants = {
        member.adapter_instance_id: _grant(member)
        for member in request.members
        if member.participant_id == "process-a:incarnation-a"
    }
    return (
        ActorV2CoreIngressDrainProcessWorker(
            repository=repository,
            participant_grants=grants,
            legacy_drain=legacy_drain,
        ),
        repository,
    )


@pytest.mark.asyncio
async def test_worker_freezes_and_drains_once_for_all_local_adapter_members() -> None:
    """One process writes all member acknowledgements after one local drain."""

    events: list[str] = []
    member_a = _member(member_id="member-a", adapter_instance_id="adapter-a")
    member_b = _member(member_id="member-b", adapter_instance_id="adapter-b")
    request = _request(member_a, member_b)
    worker, repository = _worker(request, _LegacyDrain(events, [True]))

    outcome = await worker.service_request(request.request_id)

    assert outcome.status is ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED
    assert outcome.core_ingress_quiescent
    assert outcome.local_legacy_quiescent
    assert outcome.acknowledged_member_ids == ("member-a", "member-b")
    assert tuple(repository.acknowledgements) == ("member-a", "member-b")
    assert events == ["legacy.freeze", "legacy.drain"]
    assert "legacy-ingress-token" not in repr(outcome)
    assert "holder-token" not in repr(outcome)
    acknowledgements = tuple(repository.acknowledgements.values())
    assert acknowledgements[0].core_ingress_digest == acknowledgements[1].core_ingress_digest
    assert (
        acknowledgements[0].core_ingress_digest
        != acknowledgements[0].legacy_quiescence_digest
    )


@pytest.mark.asyncio
async def test_worker_retries_local_drain_without_repeating_the_freeze() -> None:
    """A negative local receipt retains its opaque ticket for the next attempt."""

    events: list[str] = []
    member = _member(member_id="member-a", adapter_instance_id="adapter-a")
    request = _request(member)
    worker, repository = _worker(request, _LegacyDrain(events, [False, True]))

    first = await worker.service_request(request.request_id)
    second = await worker.service_request(request.request_id)

    assert first.status is ActorV2CoreIngressDrainWorkerStatus.AWAITING_LOCAL_DRAIN
    assert not first.core_ingress_quiescent
    assert not first.local_legacy_quiescent
    assert second.status is ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED
    assert events == ["legacy.freeze", "legacy.drain", "legacy.drain"]
    assert tuple(repository.acknowledgements) == ("member-a",)


@pytest.mark.asyncio
async def test_worker_accepts_same_request_signal_ticket_upgrade() -> None:
    """The real drainer may attach signal-admission state after ingress drains."""

    events: list[str] = []
    member = _member(member_id="member-a", adapter_instance_id="adapter-a")
    request = _request(member)
    worker, repository = _worker(
        request,
        _LegacyDrain(events, [True], defer_signal_freeze=True),
    )

    outcome = await worker.service_request(request.request_id)

    assert outcome.status is ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED
    assert tuple(repository.acknowledgements) == ("member-a",)
    assert events == ["legacy.freeze", "legacy.drain"]


@pytest.mark.asyncio
async def test_worker_rejects_multiple_local_member_epochs_for_one_adapter() -> None:
    """One process cannot collapse two frozen memberships into one capability."""

    events: list[str] = []
    member_one = _member(
        member_id="member-a-1",
        adapter_instance_id="adapter-a",
        participant_epoch=1,
    )
    member_two = _member(
        member_id="member-a-2",
        adapter_instance_id="adapter-a",
        participant_epoch=2,
    )
    request = _request(member_one, member_two)
    worker, _repository = _worker(request, _LegacyDrain(events, [True]))

    with pytest.raises(ActorV2CoreIngressDrainWorkerConflict, match="multiple local member"):
        await worker.service_request(request.request_id)
    assert events == []
