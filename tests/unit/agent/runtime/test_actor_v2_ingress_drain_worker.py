"""Unit coverage for the unmounted process-wide ingress drain worker."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pytest

from shinbot.agent.runtime.actor_v2_ingress_drain_worker import (
    ActorV2IngressDrainProcessWorker,
    ActorV2IngressDrainWorkerConflict,
    ActorV2IngressDrainWorkerStatus,
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
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressDrainAcknowledgement,
    ActorV2IngressDrainMember,
    ActorV2IngressDrainReceipt,
    ActorV2IngressDrainRequest,
    ActorV2IngressDrainStatus,
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
from shinbot.core.platform.ingress_pause import (
    AdapterIngressPauseDeliveryGuarantee,
    AdapterIngressPauseReceipt,
    AdapterIngressPauseRequest,
    AdapterIngressPauseStatus,
    AdapterIngressPauseTicket,
)


def _digest(value: str) -> str:
    """Build one deterministic opaque digest for test receipt metadata."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _member(
    *,
    member_id: str,
    adapter_instance_id: str,
    participant_id: str = "process-a:incarnation-a",
    participant_epoch: int = 1,
) -> ActorV2IngressDrainMember:
    """Build one frozen process membership in the test request."""

    return ActorV2IngressDrainMember(
        request_id="request-a",
        member_id=member_id,
        adapter_instance_id=adapter_instance_id,
        participant_id=participant_id,
        participant_epoch=participant_epoch,
    )


def _request(
    *members: ActorV2IngressDrainMember,
) -> ActorV2IngressDrainRequest:
    """Build one open request covering all supplied adapter memberships."""

    return ActorV2IngressDrainRequest(
        request_id="request-a",
        cutover_id="cutover-a",
        cutover_epoch=3,
        key=SessionKey("profile-a", "profile-a:group:room"),
        legacy_session_id="legacy-session-a",
        adapter_instance_ids=tuple(
            sorted({member.adapter_instance_id for member in members})
        ),
        admission_fence_id="fence-a",
        admission_fence_generation=1,
        status=ActorV2IngressDrainStatus.OPEN,
        created_at=1.0,
        updated_at=1.0,
        drained_at=None,
        members=members,
    )


def _grant(member: ActorV2IngressDrainMember) -> ActorV2IngressParticipantGrant:
    """Build one local opaque membership capability for a frozen member."""

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
    """In-memory durable control-plane stand-in for one worker test."""

    def __init__(self, request: ActorV2IngressDrainRequest) -> None:
        self.request = request
        self.acknowledgements: dict[str, ActorV2IngressDrainAcknowledgement] = {}

    def get_request(self, request_id: str) -> ActorV2IngressDrainRequest | None:
        """Return the configured request only for its exact identity."""

        return self.request if request_id == self.request.request_id else None

    def acknowledge_quiescent(
        self,
        *,
        request_id: str,
        grant: ActorV2IngressParticipantGrant,
        receipt: ActorV2IngressDrainReceipt,
    ) -> ActorV2IngressDrainAcknowledgement:
        """Persist a deterministic idempotent local acknowledgement."""

        existing = self.acknowledgements.get(grant.participant.member_id)
        if existing is not None:
            assert existing.receipt == receipt
            return existing
        acknowledgement = ActorV2IngressDrainAcknowledgement(
            request_id=request_id,
            member_id=grant.participant.member_id,
            adapter_pause_digest=receipt.adapter_pause_digest,
            legacy_quiescence_digest=receipt.legacy_quiescence_digest,
            proof_epoch=receipt.proof_epoch,
            summary_code=receipt.summary_code,
            acknowledged_at=1.0,
        )
        self.acknowledgements[acknowledgement.member_id] = acknowledgement
        return acknowledgement


class _AdapterParticipant:
    """Controllable local adapter pause participant used for ordering checks."""

    def __init__(
        self,
        *,
        adapter_instance_id: str,
        participant_id: str,
        participant_epoch: int,
        statuses: list[AdapterIngressPauseStatus],
        events: list[str],
    ) -> None:
        self._adapter_instance_id = adapter_instance_id
        self._participant_id = participant_id
        self._participant_epoch = participant_epoch
        self._statuses = statuses
        self._events = events
        self.pause_calls = 0
        self.await_calls = 0

    @property
    def adapter_instance_id(self) -> str:
        """Return the adapter identity covered by this fake participant."""

        return self._adapter_instance_id

    @property
    def participant_id(self) -> str:
        """Return the process-incarnation identity covered by this fake."""

        return self._participant_id

    @property
    def delivery_guarantee(self) -> AdapterIngressPauseDeliveryGuarantee:
        """Declare durable retention after pause for the contract surface."""

        return AdapterIngressPauseDeliveryGuarantee.DURABLE_BUFFER

    def pause_ingress(self, request: AdapterIngressPauseRequest) -> AdapterIngressPauseTicket:
        """Record local pause admission and return a ticket for the exact request."""

        self.pause_calls += 1
        self._events.append(f"pause:{self._adapter_instance_id}")
        return AdapterIngressPauseTicket(
            request=request,
            participant_id=self._participant_id,
            participant_epoch=self._participant_epoch,
            token=f"pause-token:{self._adapter_instance_id}",
        )

    async def await_ingress_quiescent(
        self,
        ticket: AdapterIngressPauseTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> AdapterIngressPauseReceipt:
        """Return the next programmed callback-drain observation."""

        del timeout_seconds
        self.await_calls += 1
        self._events.append(f"await:{self._adapter_instance_id}")
        return AdapterIngressPauseReceipt(
            ticket=ticket,
            status=self._statuses.pop(0),
        )

    def resume_ingress(self, ticket: AdapterIngressPauseTicket) -> bool:
        """Expose the complete participant surface without resuming in this test."""

        return ticket.participant_id == self._participant_id


@dataclass(slots=True)
class _LegacyDrain:
    """Fake local legacy drain that exposes ordering through an event log."""

    events: list[str]
    quiescent: bool = True

    def freeze(self, request: LegacySessionLocalDrainRequest) -> LegacySessionLocalDrainTicket:
        """Freeze the exact local request after all adapter drains are quiet."""

        self.events.append("legacy.freeze")
        ingress_ticket = LegacyIngressFreezeTicket(
            session_id=request.legacy_session_id,
            cutover_id=request.cutover_id,
            freeze_epoch=1,
            token="legacy-ingress-token",
        )
        waiting_ticket = WaitingInputFreezeTicket(
            scope=request.waiting_input_scope,
            cutover_id=request.cutover_id,
            token="waiting-input-token",
        )
        return LegacySessionLocalDrainTicket(
            request=request,
            ingress_ticket=ingress_ticket,
            waiting_input_ticket=waiting_ticket,
            signal_ticket=LegacyAgentSignalFreezeTicket(
                session_id=request.legacy_session_id,
                cutover_id=request.cutover_id,
                freeze_epoch=1,
                token="legacy-signal-token",
            ),
        )

    async def drain(
        self,
        ticket: LegacySessionLocalDrainTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalDrainReceipt:
        """Return a fully typed positive or negative local drain receipt."""

        del timeout_seconds
        self.events.append("legacy.drain")
        ingress_status = (
            LegacyIngressQuiescenceStatus.QUIESCENT
            if self.quiescent
            else LegacyIngressQuiescenceStatus.TIMED_OUT
        )
        signal_status = (
            LegacyAgentSignalQuiescenceStatus.QUIESCENT
            if self.quiescent
            else LegacyAgentSignalQuiescenceStatus.TIMED_OUT
        )
        return LegacySessionLocalDrainReceipt(
            ticket=ticket,
            ingress=LegacyIngressQuiescenceReceipt(
                ticket=ticket.ingress_ticket,
                status=ingress_status,
            ),
            waiting_input=WaitingInputQuiescenceReceipt(
                ticket=ticket.waiting_input_ticket,
                quiescent=self.quiescent,
            ),
            agent_signals=LegacyAgentSignalQuiescenceReceipt(
                ticket=ticket.signal_ticket,
                status=signal_status,
            ),
            agent_tasks=LegacySessionAllProfilesTaskQuiescence(
                session_id=ticket.request.legacy_session_id,
                observations=(),
            ),
        )


def _worker(
    request: ActorV2IngressDrainRequest,
    adapters: dict[str, _AdapterParticipant],
    legacy_drain: _LegacyDrain,
) -> tuple[ActorV2IngressDrainProcessWorker, _Repository]:
    """Build a worker whose grants exactly cover its local request members."""

    repository = _Repository(request)
    grants = {
        member.adapter_instance_id: _grant(member)
        for member in request.members
        if member.participant_id == "process-a:incarnation-a"
    }
    return (
        ActorV2IngressDrainProcessWorker(
            repository=repository,
            participant_grants=grants,
            adapter_participants=adapters,
            legacy_drain=legacy_drain,
        ),
        repository,
    )


@pytest.mark.asyncio
async def test_worker_pauses_every_local_adapter_before_one_legacy_drain() -> None:
    """One process never freezes shared legacy ingress while an adapter is live."""

    events: list[str] = []
    member_a = _member(member_id="member-a", adapter_instance_id="adapter-a")
    member_b = _member(member_id="member-b", adapter_instance_id="adapter-b")
    request = _request(member_a, member_b)
    adapters = {
        "adapter-a": _AdapterParticipant(
            adapter_instance_id="adapter-a",
            participant_id=member_a.participant_id,
            participant_epoch=member_a.participant_epoch,
            statuses=[AdapterIngressPauseStatus.QUIESCENT],
            events=events,
        ),
        "adapter-b": _AdapterParticipant(
            adapter_instance_id="adapter-b",
            participant_id=member_b.participant_id,
            participant_epoch=member_b.participant_epoch,
            statuses=[AdapterIngressPauseStatus.QUIESCENT],
            events=events,
        ),
    }
    worker, repository = _worker(request, adapters, _LegacyDrain(events))

    outcome = await worker.service_request(request.request_id)

    assert outcome.status is ActorV2IngressDrainWorkerStatus.ACKNOWLEDGED
    assert outcome.acknowledged_member_ids == ("member-a", "member-b")
    assert tuple(repository.acknowledgements) == ("member-a", "member-b")
    assert events == [
        "pause:adapter-a",
        "pause:adapter-b",
        "await:adapter-a",
        "await:adapter-b",
        "legacy.freeze",
        "legacy.drain",
    ]
    assert "pause-token" not in repr(outcome)
    assert "legacy-ingress-token" not in repr(outcome)


@pytest.mark.asyncio
async def test_worker_retries_adapter_drain_without_freezing_legacy_early() -> None:
    """A timed-out adapter receipt retains its ticket and blocks local freeze."""

    events: list[str] = []
    member = _member(member_id="member-a", adapter_instance_id="adapter-a")
    request = _request(member)
    adapter = _AdapterParticipant(
        adapter_instance_id="adapter-a",
        participant_id=member.participant_id,
        participant_epoch=member.participant_epoch,
        statuses=[
            AdapterIngressPauseStatus.TIMED_OUT,
            AdapterIngressPauseStatus.QUIESCENT,
        ],
        events=events,
    )
    worker, repository = _worker(
        request,
        {"adapter-a": adapter},
        _LegacyDrain(events),
    )

    first = await worker.service_request(request.request_id)
    second = await worker.service_request(request.request_id)

    assert first.status is ActorV2IngressDrainWorkerStatus.AWAITING_ADAPTER_DRAIN
    assert first.local_legacy_quiescent is False
    assert second.status is ActorV2IngressDrainWorkerStatus.ACKNOWLEDGED
    assert adapter.pause_calls == 1
    assert adapter.await_calls == 2
    assert events == [
        "pause:adapter-a",
        "await:adapter-a",
        "await:adapter-a",
        "legacy.freeze",
        "legacy.drain",
    ]
    assert tuple(repository.acknowledgements) == ("member-a",)


@pytest.mark.asyncio
async def test_worker_rejects_multiple_local_members_for_one_adapter() -> None:
    """A process cannot collapse two member epochs into one adapter capability."""

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
    adapter = _AdapterParticipant(
        adapter_instance_id="adapter-a",
        participant_id=member_one.participant_id,
        participant_epoch=member_one.participant_epoch,
        statuses=[AdapterIngressPauseStatus.QUIESCENT],
        events=events,
    )
    worker = ActorV2IngressDrainProcessWorker(
        repository=_Repository(request),
        participant_grants={"adapter-a": _grant(member_one)},
        adapter_participants={"adapter-a": adapter},
        legacy_drain=_LegacyDrain(events),
    )

    with pytest.raises(ActorV2IngressDrainWorkerConflict, match="multiple local member"):
        await worker.service_request(request.request_id)
    assert events == []
