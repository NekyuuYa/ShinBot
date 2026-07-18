"""Durable contracts for an unmounted Actor v2 ingress-drain protocol.

The protocol models two facts a future production cutover controller must be
able to prove without retaining local capabilities in the database:

* every process incarnation that could receive a named adapter's ingress was
  included in one immutable request snapshot; and
* every member in that snapshot acknowledged its local adapter pause and
  legacy-session drain for the exact request.

Membership heartbeats are deliberately advisory.  A missed heartbeat never
means that a process has stopped receiving callbacks, so it cannot remove a
member from a pending request or make a request ready.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from enum import StrEnum

from shinbot.core.dispatch.agent_identity import SessionKey

_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
_SUMMARY_CODE_PATTERN = re.compile(r"[a-z][a-z0-9_.:-]{0,127}")


class ActorV2IngressParticipantStatus(StrEnum):
    """Lifecycle state of one adapter/process-incarnation membership."""

    ACTIVE = "active"
    RETIRED = "retired"
    REVOKED = "revoked"


class ActorV2IngressDrainStatus(StrEnum):
    """Forward-only state of one durable ingress drain request."""

    ASSEMBLING = "assembling"
    OPEN = "open"
    DRAINED = "drained"


class ActorV2IngressDrainProofKind(StrEnum):
    """Token-free proof streams emitted by every acknowledged member."""

    ADAPTER_PAUSE = "adapter_pause"
    LEGACY_QUIESCENCE = "legacy_quiescence"


@dataclass(slots=True, frozen=True)
class ActorV2IngressStopProof:
    """Token-free external evidence that one unresponsive member has stopped.

    This proof may mark a membership terminal, but it does not create a drain
    acknowledgement for any request the member failed to acknowledge.  A
    controller therefore cannot mistake process termination for an observed
    no-skip boundary.
    """

    issuer_id: str
    proof_epoch: int
    digest: str
    summary_code: str

    def __post_init__(self) -> None:
        """Normalize safe diagnostic evidence fields."""

        object.__setattr__(self, "issuer_id", _identifier(self.issuer_id, "issuer_id"))
        object.__setattr__(self, "proof_epoch", _positive_int(self.proof_epoch, "proof_epoch"))
        object.__setattr__(self, "digest", _digest(self.digest, "digest"))
        object.__setattr__(
            self,
            "summary_code",
            _summary_code(self.summary_code, "summary_code"),
        )


@dataclass(slots=True, frozen=True)
class ActorV2IngressParticipant:
    """Token-free durable membership of one adapter/process incarnation."""

    member_id: str
    adapter_instance_id: str
    participant_id: str
    participant_epoch: int
    status: ActorV2IngressParticipantStatus
    registered_at: float
    last_heartbeat_at: float
    updated_at: float
    retired_at: float | None = None
    revoked_at: float | None = None
    stop_proof: ActorV2IngressStopProof | None = None

    def __post_init__(self) -> None:
        """Validate membership identity and terminal-state evidence."""

        member_id = _identifier(self.member_id, "member_id")
        adapter_instance_id = _identifier(
            self.adapter_instance_id,
            "adapter_instance_id",
        )
        participant_id = _identifier(self.participant_id, "participant_id")
        participant_epoch = _positive_int(self.participant_epoch, "participant_epoch")
        status = ActorV2IngressParticipantStatus(self.status)
        registered_at = _finite_time(self.registered_at, "registered_at")
        last_heartbeat_at = _finite_time(
            self.last_heartbeat_at,
            "last_heartbeat_at",
        )
        updated_at = _finite_time(self.updated_at, "updated_at")
        retired_at = _optional_time(self.retired_at, "retired_at")
        revoked_at = _optional_time(self.revoked_at, "revoked_at")
        if last_heartbeat_at < registered_at or updated_at < last_heartbeat_at:
            raise ValueError("participant timestamps must be monotonic")
        if status is ActorV2IngressParticipantStatus.ACTIVE:
            if retired_at is not None or revoked_at is not None or self.stop_proof is not None:
                raise ValueError("active participant cannot retain terminal evidence")
        elif status is ActorV2IngressParticipantStatus.RETIRED:
            if retired_at is None or revoked_at is not None or self.stop_proof is not None:
                raise ValueError("retired participant requires only retired_at")
            if retired_at != updated_at:
                raise ValueError("retired_at must equal the terminal update time")
        else:
            if revoked_at is None or retired_at is not None:
                raise ValueError("revoked participant requires only revoked_at")
            if revoked_at != updated_at or self.stop_proof is None:
                raise ValueError("revoked participant requires terminal stop proof")
        object.__setattr__(self, "member_id", member_id)
        object.__setattr__(self, "adapter_instance_id", adapter_instance_id)
        object.__setattr__(self, "participant_id", participant_id)
        object.__setattr__(self, "participant_epoch", participant_epoch)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "registered_at", registered_at)
        object.__setattr__(self, "last_heartbeat_at", last_heartbeat_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "retired_at", retired_at)
        object.__setattr__(self, "revoked_at", revoked_at)

    @property
    def active(self) -> bool:
        """Return whether this incarnation may still receive callbacks."""

        return self.status is ActorV2IngressParticipantStatus.ACTIVE


@dataclass(slots=True, frozen=True)
class ActorV2IngressParticipantGrant:
    """Local capability held only by the registered participant incarnation."""

    participant: ActorV2IngressParticipant
    holder_token: str = field(repr=False)

    def __post_init__(self) -> None:
        """Require an active participant snapshot and opaque holder token."""

        if not isinstance(self.participant, ActorV2IngressParticipant):
            raise TypeError("participant grant requires an ActorV2IngressParticipant")
        if not self.participant.active:
            raise ValueError("participant grant requires an active membership")
        object.__setattr__(self, "holder_token", _identifier(self.holder_token, "holder_token"))


@dataclass(slots=True, frozen=True)
class ActorV2IngressDrainMember:
    """One immutable member copied into a single request's coverage set."""

    request_id: str
    member_id: str
    adapter_instance_id: str
    participant_id: str
    participant_epoch: int

    def __post_init__(self) -> None:
        """Normalize one exact, process-incarnation-bound snapshot identity."""

        object.__setattr__(self, "request_id", _identifier(self.request_id, "request_id"))
        object.__setattr__(self, "member_id", _identifier(self.member_id, "member_id"))
        object.__setattr__(
            self,
            "adapter_instance_id",
            _identifier(self.adapter_instance_id, "adapter_instance_id"),
        )
        object.__setattr__(
            self,
            "participant_id",
            _identifier(self.participant_id, "participant_id"),
        )
        object.__setattr__(
            self,
            "participant_epoch",
            _positive_int(self.participant_epoch, "participant_epoch"),
        )


@dataclass(slots=True, frozen=True)
class ActorV2IngressDrainReceipt:
    """Token-free local evidence supplied before a member acknowledgement."""

    adapter_pause_digest: str
    legacy_quiescence_digest: str
    proof_epoch: int
    summary_code: str

    def __post_init__(self) -> None:
        """Reject raw tickets, tokens, and non-canonical proof metadata."""

        object.__setattr__(
            self,
            "adapter_pause_digest",
            _digest(self.adapter_pause_digest, "adapter_pause_digest"),
        )
        object.__setattr__(
            self,
            "legacy_quiescence_digest",
            _digest(self.legacy_quiescence_digest, "legacy_quiescence_digest"),
        )
        object.__setattr__(self, "proof_epoch", _positive_int(self.proof_epoch, "proof_epoch"))
        object.__setattr__(
            self,
            "summary_code",
            _summary_code(self.summary_code, "summary_code"),
        )


@dataclass(slots=True, frozen=True)
class ActorV2IngressDrainAcknowledgement:
    """One immutable, token-free acknowledgement for an exact request member."""

    request_id: str
    member_id: str
    adapter_pause_digest: str
    legacy_quiescence_digest: str
    proof_epoch: int
    summary_code: str
    acknowledged_at: float

    def __post_init__(self) -> None:
        """Normalize persisted acknowledgement metadata."""

        object.__setattr__(self, "request_id", _identifier(self.request_id, "request_id"))
        object.__setattr__(self, "member_id", _identifier(self.member_id, "member_id"))
        object.__setattr__(
            self,
            "adapter_pause_digest",
            _digest(self.adapter_pause_digest, "adapter_pause_digest"),
        )
        object.__setattr__(
            self,
            "legacy_quiescence_digest",
            _digest(self.legacy_quiescence_digest, "legacy_quiescence_digest"),
        )
        object.__setattr__(self, "proof_epoch", _positive_int(self.proof_epoch, "proof_epoch"))
        object.__setattr__(
            self,
            "summary_code",
            _summary_code(self.summary_code, "summary_code"),
        )
        object.__setattr__(
            self,
            "acknowledged_at",
            _finite_time(self.acknowledged_at, "acknowledged_at"),
        )

    @property
    def receipt(self) -> ActorV2IngressDrainReceipt:
        """Return the local proof data without durable member/request identity."""

        return ActorV2IngressDrainReceipt(
            adapter_pause_digest=self.adapter_pause_digest,
            legacy_quiescence_digest=self.legacy_quiescence_digest,
            proof_epoch=self.proof_epoch,
            summary_code=self.summary_code,
        )


@dataclass(slots=True, frozen=True)
class ActorV2IngressDrainRequest:
    """A sealed membership snapshot and its forward-only acknowledgement state."""

    request_id: str
    cutover_id: str
    cutover_epoch: int
    key: SessionKey
    legacy_session_id: str
    adapter_instance_ids: tuple[str, ...]
    admission_fence_id: str
    admission_fence_generation: int
    status: ActorV2IngressDrainStatus
    created_at: float
    updated_at: float
    drained_at: float | None
    members: tuple[ActorV2IngressDrainMember, ...]
    acknowledgements: tuple[ActorV2IngressDrainAcknowledgement, ...] = ()

    def __post_init__(self) -> None:
        """Require an exact non-shrinking coverage set and valid acks."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("ingress drain request key must be a SessionKey")
        request_id = _identifier(self.request_id, "request_id")
        cutover_id = _identifier(self.cutover_id, "cutover_id")
        cutover_epoch = _positive_int(self.cutover_epoch, "cutover_epoch")
        legacy_session_id = _identifier(self.legacy_session_id, "legacy_session_id")
        adapter_instance_ids = _adapter_instance_ids(self.adapter_instance_ids)
        admission_fence_id = _identifier(
            self.admission_fence_id,
            "admission_fence_id",
        )
        admission_fence_generation = _positive_int(
            self.admission_fence_generation,
            "admission_fence_generation",
        )
        status = ActorV2IngressDrainStatus(self.status)
        created_at = _finite_time(self.created_at, "created_at")
        updated_at = _finite_time(self.updated_at, "updated_at")
        drained_at = _optional_time(self.drained_at, "drained_at")
        if updated_at < created_at:
            raise ValueError("ingress drain request updated_at must not precede created_at")
        members = tuple(self.members)
        if not members or any(
            not isinstance(member, ActorV2IngressDrainMember) for member in members
        ):
            raise ValueError("ingress drain request requires typed members")
        if any(member.request_id != request_id for member in members):
            raise ValueError("ingress drain member belongs to another request")
        member_ids = tuple(member.member_id for member in members)
        if len(set(member_ids)) != len(member_ids):
            raise ValueError("ingress drain request cannot repeat a member")
        member_adapters = {member.adapter_instance_id for member in members}
        if not set(adapter_instance_ids).issubset(member_adapters):
            raise ValueError("ingress drain request lacks adapter membership coverage")
        acknowledgements = tuple(self.acknowledgements)
        if any(
            not isinstance(item, ActorV2IngressDrainAcknowledgement)
            for item in acknowledgements
        ):
            raise TypeError("ingress drain acknowledgements must be typed")
        acknowledgement_ids = tuple(item.member_id for item in acknowledgements)
        if len(set(acknowledgement_ids)) != len(acknowledgement_ids):
            raise ValueError("ingress drain request cannot repeat an acknowledgement")
        if any(item.request_id != request_id for item in acknowledgements):
            raise ValueError("ingress drain acknowledgement belongs to another request")
        if not set(acknowledgement_ids).issubset(member_ids):
            raise ValueError("ingress drain acknowledgement is not in the member snapshot")
        if status is ActorV2IngressDrainStatus.ASSEMBLING:
            if acknowledgements or drained_at is not None:
                raise ValueError("assembling ingress drain request cannot retain acknowledgements")
        elif status is ActorV2IngressDrainStatus.OPEN:
            if drained_at is not None:
                raise ValueError("open ingress drain request cannot retain drained_at")
        else:
            if drained_at is None or set(acknowledgement_ids) != set(member_ids):
                raise ValueError("drained ingress request requires every member acknowledgement")
            if drained_at != updated_at:
                raise ValueError("drained_at must equal the terminal update time")
        canonical_members = tuple(
            sorted(
                members,
                key=lambda item: (
                    item.adapter_instance_id,
                    item.participant_id,
                    item.participant_epoch,
                    item.member_id,
                ),
            )
        )
        canonical_acknowledgements = tuple(
            sorted(acknowledgements, key=lambda item: item.member_id)
        )
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "cutover_id", cutover_id)
        object.__setattr__(self, "cutover_epoch", cutover_epoch)
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        object.__setattr__(self, "adapter_instance_ids", adapter_instance_ids)
        object.__setattr__(self, "admission_fence_id", admission_fence_id)
        object.__setattr__(
            self,
            "admission_fence_generation",
            admission_fence_generation,
        )
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "drained_at", drained_at)
        object.__setattr__(self, "members", canonical_members)
        object.__setattr__(self, "acknowledgements", canonical_acknowledgements)

    @property
    def unacknowledged_members(self) -> tuple[ActorV2IngressDrainMember, ...]:
        """Return every immutable member that has not supplied a local receipt."""

        acknowledged_member_ids = {
            acknowledgement.member_id for acknowledgement in self.acknowledgements
        }
        return tuple(
            member
            for member in self.members
            if member.member_id not in acknowledged_member_ids
        )

    @property
    def all_members_acknowledged(self) -> bool:
        """Return whether all sealed members have acknowledged this request."""

        return not self.unacknowledged_members

    @property
    def durably_drained(self) -> bool:
        """Return whether the controller has durably confirmed the full snapshot."""

        return (
            self.status is ActorV2IngressDrainStatus.DRAINED
            and self.all_members_acknowledged
        )

    def proof_digest(self, kind: ActorV2IngressDrainProofKind) -> str:
        """Hash one canonical, token-free proof stream for cutover evidence.

        The digest is usable as the opaque value of a future cutover-journal
        evidence record.  It intentionally contains request/member identities,
        local receipt digests, and safe summary metadata only.
        """

        if not self.durably_drained:
            raise ActorV2IngressDrainNotReady(
                "a proof digest requires a durably drained request"
            )
        proof_kind = ActorV2IngressDrainProofKind(kind)
        acknowledgements = {
            acknowledgement.member_id: acknowledgement
            for acknowledgement in self.acknowledgements
        }
        field_name = (
            "adapter_pause_digest"
            if proof_kind is ActorV2IngressDrainProofKind.ADAPTER_PAUSE
            else "legacy_quiescence_digest"
        )
        payload = {
            "admission_fence_generation": self.admission_fence_generation,
            "admission_fence_id": self.admission_fence_id,
            "cutover_id": self.cutover_id,
            "cutover_epoch": self.cutover_epoch,
            "kind": proof_kind.value,
            "legacy_session_id": self.legacy_session_id,
            "members": [
                {
                    "adapter_instance_id": member.adapter_instance_id,
                    "member_id": member.member_id,
                    "participant_epoch": member.participant_epoch,
                    "participant_id": member.participant_id,
                    "proof_digest": getattr(acknowledgements[member.member_id], field_name),
                    "proof_epoch": acknowledgements[member.member_id].proof_epoch,
                    "summary_code": acknowledgements[member.member_id].summary_code,
                }
                for member in self.members
            ],
            "profile_id": self.key.profile_id,
            "request_id": self.request_id,
            "session_id": self.key.session_id,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class ActorV2IngressDrainError(RuntimeError):
    """Base error for the fail-closed durable ingress-drain protocol."""


class ActorV2IngressDrainConflict(ActorV2IngressDrainError):
    """Raised when a stale or incompatible request/member operation is attempted."""


class ActorV2IngressDrainCoverageError(ActorV2IngressDrainConflict):
    """Raised when a cutover cannot snapshot at least one member per adapter."""

    def __init__(self, missing_adapter_instance_ids: tuple[str, ...]) -> None:
        """Expose only missing adapter identifiers, never capabilities."""

        self.missing_adapter_instance_ids = _adapter_instance_ids(
            missing_adapter_instance_ids
        )
        super().__init__(
            "ingress drain lacks active participant coverage for: "
            + ", ".join(self.missing_adapter_instance_ids)
        )


class ActorV2IngressDrainNotFound(ActorV2IngressDrainError):
    """Raised when a durable request or membership row does not exist."""


class ActorV2IngressDrainNotReady(ActorV2IngressDrainConflict):
    """Raised when a controller attempts confirmation before all acks exist."""


def _identifier(value: object, field_name: str) -> str:
    """Normalize one required opaque identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"ingress drain {field_name} must not be empty")
    return normalized


def _positive_int(value: object, field_name: str) -> int:
    """Require a positive integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"ingress drain {field_name} must be a positive integer")
    return value


def _finite_time(value: object, field_name: str) -> float:
    """Normalize one finite timestamp."""

    if isinstance(value, bool):
        raise ValueError(f"ingress drain {field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"ingress drain {field_name} must be finite")
    return normalized


def _optional_time(value: object, field_name: str) -> float | None:
    """Normalize an optional finite timestamp."""

    return None if value is None else _finite_time(value, field_name)


def _digest(value: object, field_name: str) -> str:
    """Require a lower-case SHA-256 digest without raw proof material."""

    normalized = str(value or "").strip().lower()
    if _DIGEST_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"ingress drain {field_name} must be a SHA-256 digest")
    return normalized


def _summary_code(value: object, field_name: str) -> str:
    """Require bounded operator-safe diagnostic metadata."""

    normalized = str(value or "").strip()
    if _SUMMARY_CODE_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"ingress drain {field_name} must be a stable lowercase code")
    return normalized


def _adapter_instance_ids(values: object) -> tuple[str, ...]:
    """Return a non-empty, canonical adapter-instance set."""

    if isinstance(values, str):
        raise TypeError("adapter_instance_ids must be an iterable, not a string")
    try:
        normalized = tuple(_identifier(value, "adapter_instance_id") for value in values)
    except TypeError as exc:
        raise TypeError("adapter_instance_ids must be iterable") from exc
    if not normalized:
        raise ValueError("ingress drain requires at least one adapter instance")
    if len(set(normalized)) != len(normalized):
        raise ValueError("ingress drain adapter instances must be unique")
    return tuple(sorted(normalized))


__all__ = [
    "ActorV2IngressDrainAcknowledgement",
    "ActorV2IngressDrainConflict",
    "ActorV2IngressDrainCoverageError",
    "ActorV2IngressDrainError",
    "ActorV2IngressDrainMember",
    "ActorV2IngressDrainNotFound",
    "ActorV2IngressDrainNotReady",
    "ActorV2IngressDrainProofKind",
    "ActorV2IngressDrainReceipt",
    "ActorV2IngressDrainRequest",
    "ActorV2IngressDrainStatus",
    "ActorV2IngressParticipant",
    "ActorV2IngressParticipantGrant",
    "ActorV2IngressParticipantStatus",
    "ActorV2IngressStopProof",
]
