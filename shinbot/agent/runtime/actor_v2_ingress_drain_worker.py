"""Unmounted local executor for one process's durable Actor v2 drain request.

The worker is intentionally a direct-call primitive, not a background poller
or production lifecycle service.  It groups all adapter memberships owned by
one process incarnation, pauses and drains every one of those adapters before
freezing local legacy ingress, then records token-free acknowledgements for
the exact members it owns.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.legacy_session_local_drain import (
    LegacySessionLocalDrainReceipt,
    LegacySessionLocalDrainRequest,
    LegacySessionLocalDrainTicket,
)
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressDrainAcknowledgement,
    ActorV2IngressDrainMember,
    ActorV2IngressDrainReceipt,
    ActorV2IngressDrainRequest,
    ActorV2IngressDrainStatus,
    ActorV2IngressParticipantGrant,
)
from shinbot.core.dispatch.message_context import WaitingInputScope
from shinbot.core.platform.ingress_pause import (
    AdapterIngressPauseParticipant,
    AdapterIngressPauseReceipt,
    AdapterIngressPauseRequest,
    AdapterIngressPauseStatus,
    AdapterIngressPauseTicket,
)


class ActorV2IngressDrainWorkerError(RuntimeError):
    """Base error for an invalid local drain worker invocation."""


class ActorV2IngressDrainWorkerConflict(ActorV2IngressDrainWorkerError):
    """Raised when local process identity cannot satisfy a frozen request."""


class DurableIngressDrainPort(Protocol):
    """Durable control-plane calls needed by one direct local worker run."""

    def get_request(self, request_id: str) -> ActorV2IngressDrainRequest | None:
        """Return one token-free frozen request by durable identity."""

        ...

    def acknowledge_quiescent(
        self,
        *,
        request_id: str,
        grant: ActorV2IngressParticipantGrant,
        receipt: ActorV2IngressDrainReceipt,
    ) -> ActorV2IngressDrainAcknowledgement:
        """Persist one exact member's token-free local acknowledgement."""

        ...


class LocalLegacySessionDrainPort(Protocol):
    """Process-local legacy drain surface built by ``AgentRuntime``."""

    def freeze(
        self,
        request: LegacySessionLocalDrainRequest,
    ) -> LegacySessionLocalDrainTicket:
        """Freeze local legacy ingress, waiting input, and signal admission."""

        ...

    async def drain(
        self,
        ticket: LegacySessionLocalDrainTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalDrainReceipt:
        """Observe one frozen local legacy session until it is quiescent."""

        ...


class ActorV2IngressDrainWorkerStatus(StrEnum):
    """Safe status of one direct local worker observation."""

    ACKNOWLEDGED = "acknowledged"
    AWAITING_ADAPTER_DRAIN = "awaiting_adapter_drain"
    AWAITING_LOCAL_DRAIN = "awaiting_local_drain"


@dataclass(slots=True, frozen=True)
class ActorV2IngressDrainAdapterObservation:
    """Token-free local pause observation for one adapter membership."""

    adapter_instance_id: str
    status: AdapterIngressPauseStatus | None
    in_flight_callback_count: int = 0
    buffered_event_count: int = 0

    def __post_init__(self) -> None:
        """Normalize safe local callback counters."""

        adapter_instance_id = _identifier(
            self.adapter_instance_id,
            "adapter_instance_id",
        )
        status = (
            None
            if self.status is None
            else AdapterIngressPauseStatus(self.status)
        )
        in_flight = _non_negative_int(
            self.in_flight_callback_count,
            "in_flight_callback_count",
        )
        buffered = _non_negative_int(
            self.buffered_event_count,
            "buffered_event_count",
        )
        if status is AdapterIngressPauseStatus.QUIESCENT and in_flight:
            raise ValueError("quiescent adapter observation cannot retain callbacks")
        object.__setattr__(self, "adapter_instance_id", adapter_instance_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "in_flight_callback_count", in_flight)
        object.__setattr__(self, "buffered_event_count", buffered)


@dataclass(slots=True, frozen=True)
class ActorV2IngressDrainWorkerOutcome:
    """Safe result of one local drain attempt without any ticket capability."""

    request_id: str
    participant_id: str
    member_ids: tuple[str, ...]
    status: ActorV2IngressDrainWorkerStatus
    adapter_observations: tuple[ActorV2IngressDrainAdapterObservation, ...]
    local_legacy_quiescent: bool
    acknowledged_member_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        """Canonicalize diagnostic identities and enforce status consistency."""

        request_id = _identifier(self.request_id, "request_id")
        participant_id = _identifier(self.participant_id, "participant_id")
        member_ids = _member_ids(self.member_ids)
        status = ActorV2IngressDrainWorkerStatus(self.status)
        observations = tuple(self.adapter_observations)
        if any(
            not isinstance(item, ActorV2IngressDrainAdapterObservation)
            for item in observations
        ):
            raise TypeError("adapter_observations must be typed")
        adapter_ids = tuple(item.adapter_instance_id for item in observations)
        if len(set(adapter_ids)) != len(adapter_ids):
            raise ValueError("adapter observations cannot repeat an adapter")
        acknowledged_member_ids = _optional_member_ids(
            self.acknowledged_member_ids
        )
        if not set(acknowledged_member_ids).issubset(member_ids):
            raise ValueError("worker acknowledged a member outside its local scope")
        if status is ActorV2IngressDrainWorkerStatus.ACKNOWLEDGED:
            if not self.local_legacy_quiescent or acknowledged_member_ids != member_ids:
                raise ValueError("acknowledged outcome requires all local confirmations")
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "participant_id", participant_id)
        object.__setattr__(self, "member_ids", member_ids)
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self,
            "adapter_observations",
            tuple(sorted(observations, key=lambda item: item.adapter_instance_id)),
        )
        object.__setattr__(
            self,
            "acknowledged_member_ids",
            acknowledged_member_ids,
        )


@dataclass(slots=True, repr=False)
class _ProcessDrainState:
    """Process-local opaque tickets retained across direct retry attempts."""

    request: ActorV2IngressDrainRequest
    pause_tickets: dict[str, AdapterIngressPauseTicket] = field(default_factory=dict)
    pause_receipts: dict[str, AdapterIngressPauseReceipt] = field(default_factory=dict)
    legacy_ticket: LegacySessionLocalDrainTicket | None = None
    legacy_receipt: LegacySessionLocalDrainReceipt | None = None
    acknowledgements: dict[str, ActorV2IngressDrainAcknowledgement] = field(
        default_factory=dict
    )


class ActorV2IngressDrainProcessWorker:
    """Drain all request members owned by one local process incarnation.

    This class is not registered anywhere automatically.  A future process
    lifecycle service must invoke it only after that process has registered all
    adapters it owns.  The worker has no resume operation because release of
    post-pause events requires a later fenced target-publication protocol.
    """

    def __init__(
        self,
        *,
        repository: DurableIngressDrainPort,
        participant_grants: Mapping[str, ActorV2IngressParticipantGrant],
        adapter_participants: Mapping[str, AdapterIngressPauseParticipant],
        legacy_drain: LocalLegacySessionDrainPort,
    ) -> None:
        """Bind one process incarnation's local adapter and legacy capabilities."""

        if not callable(getattr(repository, "get_request", None)) or not callable(
            getattr(repository, "acknowledge_quiescent", None)
        ):
            raise TypeError("repository must implement durable ingress drain calls")
        if not callable(getattr(legacy_drain, "freeze", None)) or not callable(
            getattr(legacy_drain, "drain", None)
        ):
            raise TypeError("legacy_drain must implement freeze and drain")
        grants = dict(participant_grants)
        participants = dict(adapter_participants)
        if not grants:
            raise ValueError("participant_grants must not be empty")
        participant_ids: set[str] = set()
        for adapter_id, grant in grants.items():
            normalized_adapter_id = _identifier(adapter_id, "adapter_instance_id")
            if not isinstance(grant, ActorV2IngressParticipantGrant):
                raise TypeError("participant_grants must contain active typed grants")
            if grant.participant.adapter_instance_id != normalized_adapter_id:
                raise ValueError("participant grant key does not match its adapter identity")
            participant_ids.add(grant.participant.participant_id)
            adapter = participants.get(normalized_adapter_id)
            if adapter is None:
                raise ValueError("each participant grant requires its local adapter participant")
            if adapter.adapter_instance_id != normalized_adapter_id:
                raise ValueError("adapter participant key does not match its adapter identity")
            if adapter.participant_id != grant.participant.participant_id:
                raise ValueError("adapter participant identity does not match durable grant")
        if len(participant_ids) != 1:
            raise ValueError("all local adapter grants must share one process incarnation")
        self._repository = repository
        self._grants = grants
        self._adapter_participants = participants
        self._legacy_drain = legacy_drain
        self._participant_id = next(iter(participant_ids))
        self._states: dict[str, _ProcessDrainState] = {}
        self._lock = asyncio.Lock()

    @property
    def participant_id(self) -> str:
        """Return the immutable process-incarnation identity this worker serves."""

        return self._participant_id

    async def service_request(
        self,
        request_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> ActorV2IngressDrainWorkerOutcome:
        """Pause all local adapters, drain legacy work once, and acknowledge.

        A non-quiescent adapter or legacy receipt returns a retryable safe
        observation while retaining opaque local tickets in this process.  The
        caller may invoke this method again with the same request id; tickets
        are neither persisted nor exposed in the result.
        """

        normalized_request_id = _identifier(request_id, "request_id")
        timeout = _timeout(timeout_seconds)
        async with self._lock:
            request = self._repository.get_request(normalized_request_id)
            if request is None:
                raise ActorV2IngressDrainWorkerError("durable ingress drain request is absent")
            if request.status is not ActorV2IngressDrainStatus.OPEN:
                raise ActorV2IngressDrainWorkerConflict(
                    "worker cannot reconstruct local pause state after request leaves open"
                )
            local_members = self._local_members(request)
            state = self._state_for(request)
            deadline = (
                None
                if timeout is None
                else asyncio.get_running_loop().time() + timeout
            )
            self._pause_local_adapters(state, local_members)
            await self._await_adapter_quiescence(state, local_members, deadline)
            observations = self._adapter_observations(state, local_members)
            if any(
                observation.status is not AdapterIngressPauseStatus.QUIESCENT
                for observation in observations
            ):
                return self._outcome(
                    state,
                    local_members,
                    status=ActorV2IngressDrainWorkerStatus.AWAITING_ADAPTER_DRAIN,
                    observations=observations,
                )
            await self._drain_local_legacy_session(state, deadline)
            local_receipt = state.legacy_receipt
            if local_receipt is None or not local_receipt.locally_confirmed_quiescent:
                return self._outcome(
                    state,
                    local_members,
                    status=ActorV2IngressDrainWorkerStatus.AWAITING_LOCAL_DRAIN,
                    observations=observations,
                )
            self._acknowledge_members(state, local_members)
            return self._outcome(
                state,
                local_members,
                status=ActorV2IngressDrainWorkerStatus.ACKNOWLEDGED,
                observations=observations,
            )

    def _local_members(
        self,
        request: ActorV2IngressDrainRequest,
    ) -> tuple[ActorV2IngressDrainMember, ...]:
        """Require grants for every frozen member owned by this process."""

        local_members = tuple(
            member
            for member in request.members
            if member.participant_id == self._participant_id
        )
        if not local_members:
            raise ActorV2IngressDrainWorkerConflict(
                "drain request does not include this process incarnation"
            )
        by_adapter: dict[str, ActorV2IngressDrainMember] = {}
        for member in local_members:
            if member.adapter_instance_id in by_adapter:
                raise ActorV2IngressDrainWorkerConflict(
                    "request contains multiple local member epochs for one adapter"
                )
            grant = self._grants.get(member.adapter_instance_id)
            adapter = self._adapter_participants.get(member.adapter_instance_id)
            if grant is None or adapter is None:
                raise ActorV2IngressDrainWorkerConflict(
                    "local process lacks a grant or adapter for a frozen member"
                )
            participant = grant.participant
            if (
                participant.member_id != member.member_id
                or participant.participant_id != member.participant_id
                or participant.participant_epoch != member.participant_epoch
                or participant.adapter_instance_id != member.adapter_instance_id
                or adapter.participant_id != member.participant_id
                or adapter.adapter_instance_id != member.adapter_instance_id
            ):
                raise ActorV2IngressDrainWorkerConflict(
                    "local adapter capability does not match frozen member identity"
                )
            by_adapter[member.adapter_instance_id] = member
        return tuple(by_adapter[adapter_id] for adapter_id in sorted(by_adapter))

    def _state_for(
        self,
        request: ActorV2IngressDrainRequest,
    ) -> _ProcessDrainState:
        """Return retry state while rejecting any mutable request replacement."""

        existing = self._states.get(request.request_id)
        if existing is None:
            state = _ProcessDrainState(request=request)
            self._states[request.request_id] = state
            return state
        if _request_identity(existing.request) != _request_identity(request):
            raise ActorV2IngressDrainWorkerConflict(
                "durable request identity changed while local tickets remain active"
            )
        state_request = existing.request
        if state_request.status is ActorV2IngressDrainStatus.OPEN:
            existing.request = request
        return existing

    def _pause_local_adapters(
        self,
        state: _ProcessDrainState,
        members: tuple[ActorV2IngressDrainMember, ...],
    ) -> None:
        """Close callback admission for every local adapter before local freeze."""

        request = state.request
        for member in members:
            adapter_id = member.adapter_instance_id
            if adapter_id in state.pause_tickets:
                continue
            adapter = self._adapter_participants[adapter_id]
            expected_request = AdapterIngressPauseRequest(
                adapter_instance_id=adapter_id,
                legacy_session_id=request.legacy_session_id,
                cutover_id=request.cutover_id,
                cutover_epoch=request.cutover_epoch,
            )
            ticket = adapter.pause_ingress(expected_request)
            if (
                ticket.request != expected_request
                or ticket.participant_id != member.participant_id
                or ticket.participant_epoch != member.participant_epoch
            ):
                raise ActorV2IngressDrainWorkerConflict(
                    "adapter pause ticket does not match frozen process membership"
                )
            state.pause_tickets[adapter_id] = ticket

    async def _await_adapter_quiescence(
        self,
        state: _ProcessDrainState,
        members: tuple[ActorV2IngressDrainMember, ...],
        deadline: float | None,
    ) -> None:
        """Await every pre-pause callback set before freezing legacy ingress."""

        for member in members:
            adapter_id = member.adapter_instance_id
            receipt = state.pause_receipts.get(adapter_id)
            if receipt is not None and receipt.quiescent:
                continue
            ticket = state.pause_tickets[adapter_id]
            observed = await self._adapter_participants[adapter_id].await_ingress_quiescent(
                ticket,
                timeout_seconds=_remaining_timeout(deadline),
            )
            if observed.ticket != ticket:
                raise ActorV2IngressDrainWorkerConflict(
                    "adapter pause receipt belongs to another local ticket"
                )
            state.pause_receipts[adapter_id] = observed

    async def _drain_local_legacy_session(
        self,
        state: _ProcessDrainState,
        deadline: float | None,
    ) -> None:
        """Freeze and drain local legacy work only after all adapters are quiet."""

        request = state.request
        local_request = LegacySessionLocalDrainRequest(
            legacy_session_id=request.legacy_session_id,
            waiting_input_scope=WaitingInputScope(
                legacy_session_id=request.legacy_session_id,
                session_key=request.key,
            ),
            cutover_id=request.cutover_id,
        )
        if state.legacy_ticket is None:
            ticket = self._legacy_drain.freeze(local_request)
            if ticket.request != local_request:
                raise ActorV2IngressDrainWorkerConflict(
                    "legacy local drain ticket belongs to another cutover request"
                )
            state.legacy_ticket = ticket
        receipt = state.legacy_receipt
        if receipt is None or not receipt.locally_confirmed_quiescent:
            observed = await self._legacy_drain.drain(
                state.legacy_ticket,
                timeout_seconds=_remaining_timeout(deadline),
            )
            if observed.ticket != state.legacy_ticket:
                raise ActorV2IngressDrainWorkerConflict(
                    "legacy local drain receipt belongs to another local ticket"
                )
            state.legacy_receipt = observed

    def _acknowledge_members(
        self,
        state: _ProcessDrainState,
        members: tuple[ActorV2IngressDrainMember, ...],
    ) -> None:
        """Persist one deterministic token-free receipt for every local member."""

        receipt = state.legacy_receipt
        if receipt is None or not receipt.locally_confirmed_quiescent:
            raise ActorV2IngressDrainWorkerConflict(
                "cannot acknowledge a non-quiescent local legacy receipt"
            )
        legacy_digest = _legacy_receipt_digest(receipt)
        for member in members:
            if member.member_id in state.acknowledgements:
                continue
            adapter_id = member.adapter_instance_id
            adapter_receipt = state.pause_receipts.get(adapter_id)
            if adapter_receipt is None or not adapter_receipt.quiescent:
                raise ActorV2IngressDrainWorkerConflict(
                    "cannot acknowledge a non-quiescent adapter receipt"
                )
            durable_receipt = ActorV2IngressDrainReceipt(
                adapter_pause_digest=_adapter_receipt_digest(adapter_receipt),
                legacy_quiescence_digest=legacy_digest,
                proof_epoch=member.participant_epoch,
                summary_code="process.local_quiescent",
            )
            acknowledgement = self._repository.acknowledge_quiescent(
                request_id=state.request.request_id,
                grant=self._grants[adapter_id],
                receipt=durable_receipt,
            )
            if (
                acknowledgement.request_id != state.request.request_id
                or acknowledgement.member_id != member.member_id
                or acknowledgement.receipt != durable_receipt
            ):
                raise ActorV2IngressDrainWorkerConflict(
                    "durable acknowledgement does not match the submitted local receipt"
                )
            state.acknowledgements[member.member_id] = acknowledgement

    @staticmethod
    def _adapter_observations(
        state: _ProcessDrainState,
        members: tuple[ActorV2IngressDrainMember, ...],
    ) -> tuple[ActorV2IngressDrainAdapterObservation, ...]:
        """Project local receipts into safe diagnostics without opaque tickets."""

        observations: list[ActorV2IngressDrainAdapterObservation] = []
        for member in members:
            receipt = state.pause_receipts.get(member.adapter_instance_id)
            if receipt is None:
                observations.append(
                    ActorV2IngressDrainAdapterObservation(
                        adapter_instance_id=member.adapter_instance_id,
                        status=None,
                    )
                )
                continue
            observations.append(
                ActorV2IngressDrainAdapterObservation(
                    adapter_instance_id=member.adapter_instance_id,
                    status=receipt.status,
                    in_flight_callback_count=receipt.in_flight_callback_count,
                    buffered_event_count=receipt.buffered_event_count,
                )
            )
        return tuple(observations)

    def _outcome(
        self,
        state: _ProcessDrainState,
        members: tuple[ActorV2IngressDrainMember, ...],
        *,
        status: ActorV2IngressDrainWorkerStatus,
        observations: tuple[ActorV2IngressDrainAdapterObservation, ...],
    ) -> ActorV2IngressDrainWorkerOutcome:
        """Build a result whose fields cannot expose any local ticket token."""

        local_receipt = state.legacy_receipt
        return ActorV2IngressDrainWorkerOutcome(
            request_id=state.request.request_id,
            participant_id=self._participant_id,
            member_ids=tuple(member.member_id for member in members),
            status=status,
            adapter_observations=observations,
            local_legacy_quiescent=(
                local_receipt is not None
                and local_receipt.locally_confirmed_quiescent
            ),
            acknowledged_member_ids=tuple(sorted(state.acknowledgements)),
        )


def _request_identity(request: ActorV2IngressDrainRequest) -> tuple[object, ...]:
    """Return immutable request fields that local opaque tickets are bound to."""

    return (
        request.request_id,
        request.cutover_id,
        request.cutover_epoch,
        request.key,
        request.legacy_session_id,
        request.adapter_instance_ids,
        request.admission_fence_id,
        request.admission_fence_generation,
        request.members,
    )


def _adapter_receipt_digest(receipt: AdapterIngressPauseReceipt) -> str:
    """Hash a pause receipt after deliberately projecting away its ticket token."""

    if not receipt.quiescent:
        raise ActorV2IngressDrainWorkerConflict(
            "adapter digest requires a quiescent local pause receipt"
        )
    ticket = receipt.ticket
    payload = {
        "adapter_instance_id": ticket.request.adapter_instance_id,
        "buffered_event_count": receipt.buffered_event_count,
        "cutover_epoch": ticket.request.cutover_epoch,
        "cutover_id": ticket.request.cutover_id,
        "in_flight_callback_count": receipt.in_flight_callback_count,
        "legacy_session_id": ticket.request.legacy_session_id,
        "participant_epoch": ticket.participant_epoch,
        "participant_id": ticket.participant_id,
        "status": receipt.status.value,
    }
    return _digest_payload(payload)


def _legacy_receipt_digest(receipt: LegacySessionLocalDrainReceipt) -> str:
    """Hash a positive local drain receipt without serializing freeze tickets."""

    if not receipt.locally_confirmed_quiescent:
        raise ActorV2IngressDrainWorkerConflict(
            "legacy digest requires a quiescent local drain receipt"
        )
    request = receipt.ticket.request
    payload = {
        "agent_signals_status": _receipt_status(receipt.agent_signals),
        "agent_tasks_quiescent": receipt.agent_tasks.locally_confirmed_quiescent
        if receipt.agent_tasks is not None
        else False,
        "cutover_id": request.cutover_id,
        "ingress_status": _receipt_status(receipt.ingress),
        "legacy_session_id": request.legacy_session_id,
        "session_id": request.waiting_input_scope.session_key.session_id
        if request.waiting_input_scope.session_key is not None
        else "",
        "profile_id": request.waiting_input_scope.session_key.profile_id
        if request.waiting_input_scope.session_key is not None
        else "",
        "waiting_input_status": _receipt_status(receipt.waiting_input),
    }
    return _digest_payload(payload)


def _receipt_status(receipt: object | None) -> str:
    """Project a typed receipt status into a bounded token-free string."""

    if receipt is None:
        return ""
    status = getattr(receipt, "status", None)
    value = getattr(status, "value", status)
    return str(value or "")


def _digest_payload(payload: dict[str, object]) -> str:
    """Produce one canonical SHA-256 digest from safe local receipt facts."""

    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _identifier(value: object, field_name: str) -> str:
    """Normalize one required process or request identity."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"ingress drain worker {field_name} must not be empty")
    return normalized


def _member_ids(values: object) -> tuple[str, ...]:
    """Return a canonical non-empty member identity tuple."""

    if isinstance(values, str):
        raise TypeError("member_ids must be iterable, not a string")
    try:
        member_ids = tuple(_identifier(value, "member_id") for value in values)
    except TypeError as exc:
        raise TypeError("member_ids must be iterable") from exc
    if not member_ids:
        raise ValueError("member_ids must not be empty")
    if len(set(member_ids)) != len(member_ids):
        raise ValueError("member_ids must be unique")
    return tuple(sorted(member_ids))


def _optional_member_ids(values: object) -> tuple[str, ...]:
    """Return a canonical member tuple while permitting no completed acks yet."""

    if isinstance(values, str):
        raise TypeError("acknowledged_member_ids must be iterable, not a string")
    try:
        member_ids = tuple(_identifier(value, "member_id") for value in values)
    except TypeError as exc:
        raise TypeError("acknowledged_member_ids must be iterable") from exc
    if len(set(member_ids)) != len(member_ids):
        raise ValueError("acknowledged_member_ids must be unique")
    return tuple(sorted(member_ids))


def _non_negative_int(value: object, field_name: str) -> int:
    """Require one non-negative integer without treating bool as a counter."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"ingress drain worker {field_name} must be non-negative")
    return value


def _timeout(value: float | None) -> float | None:
    """Validate an optional total local drain timeout budget."""

    if value is None:
        return None
    timeout = float(value)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout_seconds must be finite and non-negative")
    return timeout


def _remaining_timeout(deadline: float | None) -> float | None:
    """Return remaining total timeout without granting another component budget."""

    if deadline is None:
        return None
    return max(0.0, deadline - asyncio.get_running_loop().time())


__all__ = [
    "ActorV2IngressDrainAdapterObservation",
    "ActorV2IngressDrainProcessWorker",
    "ActorV2IngressDrainWorkerConflict",
    "ActorV2IngressDrainWorkerError",
    "ActorV2IngressDrainWorkerOutcome",
    "ActorV2IngressDrainWorkerStatus",
    "DurableIngressDrainPort",
    "LocalLegacySessionDrainPort",
]
