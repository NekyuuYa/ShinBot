"""Unmounted local executor for a barrier-bound Actor v2 core-ingress drain.

The worker intentionally has no adapter pause capability.  Once durable
ownership is ``migrating``, a per-process ``MessageIngress`` freeze is the
source boundary for normalized messages: post-freeze events are persisted as
fenced routing jobs instead of entering legacy routing.  This direct-call
primitive groups all memberships held by one process incarnation and freezes
the shared local legacy session exactly once before acknowledging each member.
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
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainAcknowledgement,
    ActorV2CoreIngressDrainMember,
    ActorV2CoreIngressDrainReceipt,
    ActorV2CoreIngressDrainRequest,
    ActorV2CoreIngressDrainStatus,
)
from shinbot.core.dispatch.actor_v2_ingress_drain import ActorV2IngressParticipantGrant
from shinbot.core.dispatch.message_context import WaitingInputScope


class ActorV2CoreIngressDrainWorkerError(RuntimeError):
    """Base error for an invalid local core-drain worker invocation."""


class ActorV2CoreIngressDrainWorkerConflict(ActorV2CoreIngressDrainWorkerError):
    """Raised when local process identity cannot satisfy a frozen request."""


class DurableCoreIngressDrainPort(Protocol):
    """Durable control-plane calls required by one local core-drain run."""

    def get(self, request_id: str) -> ActorV2CoreIngressDrainRequest | None:
        """Return one token-free frozen request by durable identity."""

        ...

    def acknowledge_quiescent(
        self,
        *,
        request_id: str,
        participant_grant: ActorV2IngressParticipantGrant,
        receipt: ActorV2CoreIngressDrainReceipt,
    ) -> ActorV2CoreIngressDrainAcknowledgement:
        """Persist one exact member's token-free local acknowledgement."""

        ...


class LocalLegacySessionDrainPort(Protocol):
    """Process-local legacy drain surface built by ``AgentRuntime``."""

    def freeze(
        self,
        request: LegacySessionLocalDrainRequest,
    ) -> LegacySessionLocalDrainTicket:
        """Freeze local ingress, waiters, and legacy signal admission."""

        ...

    async def drain(
        self,
        ticket: LegacySessionLocalDrainTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalDrainReceipt:
        """Observe one frozen local legacy session until it is quiescent."""

        ...


class ActorV2CoreIngressDrainWorkerStatus(StrEnum):
    """Safe result of one direct local core-drain observation."""

    ACKNOWLEDGED = "acknowledged"
    AWAITING_LOCAL_DRAIN = "awaiting_local_drain"


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainWorkerOutcome:
    """Token-free result of one local core-drain attempt."""

    request_id: str
    barrier_id: str
    participant_id: str
    member_ids: tuple[str, ...]
    status: ActorV2CoreIngressDrainWorkerStatus
    core_ingress_quiescent: bool
    local_legacy_quiescent: bool
    acknowledged_member_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        """Canonicalize local identities and enforce terminal consistency."""

        request_id = _identifier(self.request_id, "request_id")
        barrier_id = _identifier(self.barrier_id, "barrier_id")
        participant_id = _identifier(self.participant_id, "participant_id")
        member_ids = _member_ids(self.member_ids)
        status = ActorV2CoreIngressDrainWorkerStatus(self.status)
        acknowledged_member_ids = _optional_member_ids(self.acknowledged_member_ids)
        if not set(acknowledged_member_ids).issubset(member_ids):
            raise ValueError("worker acknowledged a member outside its local scope")
        if self.local_legacy_quiescent and not self.core_ingress_quiescent:
            raise ValueError("full legacy quiescence requires core ingress quiescence")
        if status is ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED and (
            not self.core_ingress_quiescent
            or not self.local_legacy_quiescent
            or acknowledged_member_ids != member_ids
        ):
            raise ValueError("acknowledged outcome requires all local confirmations")
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "barrier_id", barrier_id)
        object.__setattr__(self, "participant_id", participant_id)
        object.__setattr__(self, "member_ids", member_ids)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "acknowledged_member_ids", acknowledged_member_ids)


@dataclass(slots=True, repr=False)
class _ProcessCoreDrainState:
    """Opaque local freeze state retained only for direct retry attempts."""

    request: ActorV2CoreIngressDrainRequest
    legacy_ticket: LegacySessionLocalDrainTicket | None = None
    legacy_receipt: LegacySessionLocalDrainReceipt | None = None
    acknowledgements: dict[str, ActorV2CoreIngressDrainAcknowledgement] = field(
        default_factory=dict
    )


class ActorV2CoreIngressDrainProcessWorker:
    """Drain all core-request members held by one process incarnation.

    This worker is not registered as a poller or lifecycle service.  A future
    controller must deliver exact request ids to every registered process and
    must separately supervise recovery and target publication.  Opaque local
    tickets never leave this instance or become durable evidence.
    """

    def __init__(
        self,
        *,
        repository: DurableCoreIngressDrainPort,
        participant_grants: Mapping[str, ActorV2IngressParticipantGrant],
        legacy_drain: LocalLegacySessionDrainPort,
    ) -> None:
        """Bind one process incarnation's durable grants and local drainer."""

        if not callable(getattr(repository, "get", None)) or not callable(
            getattr(repository, "acknowledge_quiescent", None)
        ):
            raise TypeError("repository must implement durable core ingress drain calls")
        if not callable(getattr(legacy_drain, "freeze", None)) or not callable(
            getattr(legacy_drain, "drain", None)
        ):
            raise TypeError("legacy_drain must implement freeze and drain")
        grants = dict(participant_grants)
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
        if len(participant_ids) != 1:
            raise ValueError("all local adapter grants must share one process incarnation")
        self._repository = repository
        self._grants = grants
        self._legacy_drain = legacy_drain
        self._participant_id = next(iter(participant_ids))
        self._states: dict[str, _ProcessCoreDrainState] = {}
        self._lock = asyncio.Lock()

    @property
    def participant_id(self) -> str:
        """Return the exact process-incarnation identity served by this worker."""

        return self._participant_id

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain whose request and acknowledgement state is used."""

        return getattr(self._repository, "persistence_domain", self._repository)

    async def service_request(
        self,
        request_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> ActorV2CoreIngressDrainWorkerOutcome:
        """Freeze and drain one local legacy session, then acknowledge members.

        A negative local receipt remains retryable while retaining the opaque
        local ticket.  Adapter pause is intentionally absent: the ownership
        barrier plus core ingress freeze determines durable routing for
        normalized events that arrive after the freeze.
        """

        normalized_request_id = _identifier(request_id, "request_id")
        timeout = _timeout(timeout_seconds)
        async with self._lock:
            request = self._repository.get(normalized_request_id)
            if request is None:
                raise ActorV2CoreIngressDrainWorkerError(
                    "durable core ingress drain request is absent"
                )
            if request.status is not ActorV2CoreIngressDrainStatus.OPEN:
                raise ActorV2CoreIngressDrainWorkerConflict(
                    "worker cannot reconstruct a local freeze after request leaves open"
                )
            local_members = self._local_members(request)
            state = self._state_for(request)
            deadline = (
                None
                if timeout is None
                else asyncio.get_running_loop().time() + timeout
            )
            await self._drain_local_legacy_session(state, deadline)
            receipt = state.legacy_receipt
            if receipt is None or not receipt.locally_confirmed_quiescent:
                return self._outcome(
                    state,
                    local_members,
                    status=ActorV2CoreIngressDrainWorkerStatus.AWAITING_LOCAL_DRAIN,
                )
            self._acknowledge_members(state, local_members)
            return self._outcome(
                state,
                local_members,
                status=ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED,
            )

    def _local_members(
        self,
        request: ActorV2CoreIngressDrainRequest,
    ) -> tuple[ActorV2CoreIngressDrainMember, ...]:
        """Require one current local grant for every member owned by this process."""

        local_members = tuple(
            member
            for member in request.members
            if member.participant_id == self._participant_id
        )
        if not local_members:
            raise ActorV2CoreIngressDrainWorkerConflict(
                "core drain request does not include this process incarnation"
            )
        adapter_ids = tuple(member.adapter_instance_id for member in local_members)
        if len(set(adapter_ids)) != len(adapter_ids):
            raise ActorV2CoreIngressDrainWorkerConflict(
                "request contains multiple local member epochs for one adapter"
            )
        by_adapter: dict[str, ActorV2CoreIngressDrainMember] = {}
        for member in local_members:
            grant = self._grants.get(member.adapter_instance_id)
            if grant is None:
                raise ActorV2CoreIngressDrainWorkerConflict(
                    "local process lacks a grant for a frozen core member"
                )
            participant = grant.participant
            if (
                participant.member_id != member.member_id
                or participant.participant_id != member.participant_id
                or participant.participant_epoch != member.participant_epoch
                or participant.adapter_instance_id != member.adapter_instance_id
            ):
                raise ActorV2CoreIngressDrainWorkerConflict(
                    "local participant capability does not match frozen core member"
                )
            by_adapter[member.adapter_instance_id] = member
        return tuple(by_adapter[adapter_id] for adapter_id in sorted(by_adapter))

    def _state_for(
        self,
        request: ActorV2CoreIngressDrainRequest,
    ) -> _ProcessCoreDrainState:
        """Return retry state while rejecting replacement beneath local tickets."""

        existing = self._states.get(request.request_id)
        if existing is None:
            state = _ProcessCoreDrainState(request=request)
            self._states[request.request_id] = state
            return state
        if _request_identity(existing.request) != _request_identity(request):
            raise ActorV2CoreIngressDrainWorkerConflict(
                "durable core request changed while local tickets remain active"
            )
        if existing.request.status is ActorV2CoreIngressDrainStatus.OPEN:
            existing.request = request
        return existing

    async def _drain_local_legacy_session(
        self,
        state: _ProcessCoreDrainState,
        deadline: float | None,
    ) -> None:
        """Freeze the shared local session once and obtain a current receipt."""

        request = state.request
        local_request = LegacySessionLocalDrainRequest(
            legacy_session_id=request.legacy_session_id,
            waiting_input_scope=WaitingInputScope(
                legacy_session_id=request.legacy_session_id,
                session_key=request.key,
            ),
            cutover_id=request.barrier_id,
        )
        if state.legacy_ticket is None:
            ticket = self._legacy_drain.freeze(local_request)
            if ticket.request != local_request:
                raise ActorV2CoreIngressDrainWorkerConflict(
                    "legacy local drain ticket belongs to another migration barrier"
                )
            state.legacy_ticket = ticket
        receipt = state.legacy_receipt
        if receipt is None or not receipt.locally_confirmed_quiescent:
            observed = await self._legacy_drain.drain(
                state.legacy_ticket,
                timeout_seconds=_remaining_timeout(deadline),
            )
            if observed.ticket != state.legacy_ticket:
                raise ActorV2CoreIngressDrainWorkerConflict(
                    "legacy local drain receipt belongs to another local ticket"
                )
            state.legacy_receipt = observed

    def _acknowledge_members(
        self,
        state: _ProcessCoreDrainState,
        members: tuple[ActorV2CoreIngressDrainMember, ...],
    ) -> None:
        """Persist one core and legacy proof pair for every local member."""

        receipt = state.legacy_receipt
        if receipt is None or not receipt.locally_confirmed_quiescent:
            raise ActorV2CoreIngressDrainWorkerConflict(
                "cannot acknowledge a non-quiescent local legacy receipt"
            )
        core_digest = _core_ingress_receipt_digest(receipt)
        legacy_digest = _legacy_receipt_digest(receipt)
        for member in members:
            if member.member_id in state.acknowledgements:
                continue
            durable_receipt = ActorV2CoreIngressDrainReceipt(
                core_ingress_digest=core_digest,
                legacy_quiescence_digest=legacy_digest,
                proof_epoch=member.participant_epoch,
                summary_code="process.local_quiescent",
            )
            acknowledgement = self._repository.acknowledge_quiescent(
                request_id=state.request.request_id,
                participant_grant=self._grants[member.adapter_instance_id],
                receipt=durable_receipt,
            )
            if (
                acknowledgement.request_id != state.request.request_id
                or acknowledgement.member_id != member.member_id
                or acknowledgement.receipt != durable_receipt
            ):
                raise ActorV2CoreIngressDrainWorkerConflict(
                    "durable acknowledgement does not match the submitted local receipt"
                )
            state.acknowledgements[member.member_id] = acknowledgement

    def _outcome(
        self,
        state: _ProcessCoreDrainState,
        members: tuple[ActorV2CoreIngressDrainMember, ...],
        *,
        status: ActorV2CoreIngressDrainWorkerStatus,
    ) -> ActorV2CoreIngressDrainWorkerOutcome:
        """Build a result that contains no opaque local freeze ticket."""

        receipt = state.legacy_receipt
        core_quiescent = (
            receipt is not None
            and receipt.ingress is not None
            and receipt.ingress.quiescent
        )
        return ActorV2CoreIngressDrainWorkerOutcome(
            request_id=state.request.request_id,
            barrier_id=state.request.barrier_id,
            participant_id=self._participant_id,
            member_ids=tuple(member.member_id for member in members),
            status=status,
            core_ingress_quiescent=core_quiescent,
            local_legacy_quiescent=(
                receipt is not None and receipt.locally_confirmed_quiescent
            ),
            acknowledged_member_ids=tuple(sorted(state.acknowledgements)),
        )


def _request_identity(request: ActorV2CoreIngressDrainRequest) -> tuple[object, ...]:
    """Return immutable fields to which local freeze tickets are bound."""

    return (
        request.request_id,
        request.barrier_id,
        request.key,
        request.legacy_session_id,
        request.adapter_instance_ids,
        request.source_generation,
        request.migration_generation,
        request.members,
    )


def _core_ingress_receipt_digest(receipt: LegacySessionLocalDrainReceipt) -> str:
    """Hash the core ingress boundary without serializing its freeze ticket."""

    ingress = receipt.ingress
    if ingress is None or not ingress.quiescent:
        raise ActorV2CoreIngressDrainWorkerConflict(
            "core ingress digest requires a quiescent local ingress receipt"
        )
    request = receipt.ticket.request
    payload = {
        "barrier_id": request.cutover_id,
        "freeze_epoch": ingress.ticket.freeze_epoch,
        "ingress_status": ingress.status.value,
        "legacy_session_id": request.legacy_session_id,
        "profile_id": request.waiting_input_scope.session_key.profile_id,
        "remaining_task_count": len(ingress.remaining_task_names),
        "session_id": request.waiting_input_scope.session_key.session_id,
    }
    return _digest_payload(payload)


def _legacy_receipt_digest(receipt: LegacySessionLocalDrainReceipt) -> str:
    """Hash full positive legacy-drain evidence without local ticket tokens."""

    if not receipt.locally_confirmed_quiescent:
        raise ActorV2CoreIngressDrainWorkerConflict(
            "legacy digest requires a quiescent local drain receipt"
        )
    request = receipt.ticket.request
    payload = {
        "agent_signals_status": _receipt_status(receipt.agent_signals),
        "agent_tasks_quiescent": receipt.agent_tasks.locally_confirmed_quiescent
        if receipt.agent_tasks is not None
        else False,
        "barrier_id": request.cutover_id,
        "ingress_status": _receipt_status(receipt.ingress),
        "legacy_session_id": request.legacy_session_id,
        "profile_id": request.waiting_input_scope.session_key.profile_id,
        "session_id": request.waiting_input_scope.session_key.session_id,
        "waiting_input_status": _receipt_status(receipt.waiting_input),
    }
    return _digest_payload(payload)


def _receipt_status(receipt: object | None) -> str:
    """Project one typed receipt status into bounded token-free text."""

    if receipt is None:
        return ""
    status = getattr(receipt, "status", None)
    value = getattr(status, "value", status)
    return str(value or "")


def _digest_payload(payload: dict[str, object]) -> str:
    """Return a canonical SHA-256 proof digest for safe local facts."""

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
        raise ValueError(f"core ingress drain worker {field_name} must not be empty")
    return normalized


def _member_ids(values: object) -> tuple[str, ...]:
    """Return one canonical non-empty tuple of durable member identities."""

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
    """Return canonical acknowledgements while permitting an empty retry state."""

    if isinstance(values, str):
        raise TypeError("acknowledged_member_ids must be iterable, not a string")
    try:
        member_ids = tuple(_identifier(value, "member_id") for value in values)
    except TypeError as exc:
        raise TypeError("acknowledged_member_ids must be iterable") from exc
    if len(set(member_ids)) != len(member_ids):
        raise ValueError("acknowledged_member_ids must be unique")
    return tuple(sorted(member_ids))


def _timeout(value: float | None) -> float | None:
    """Validate one optional total local drain timeout budget."""

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
    "ActorV2CoreIngressDrainProcessWorker",
    "ActorV2CoreIngressDrainWorkerConflict",
    "ActorV2CoreIngressDrainWorkerError",
    "ActorV2CoreIngressDrainWorkerOutcome",
    "ActorV2CoreIngressDrainWorkerStatus",
    "DurableCoreIngressDrainPort",
    "LocalLegacySessionDrainPort",
]
