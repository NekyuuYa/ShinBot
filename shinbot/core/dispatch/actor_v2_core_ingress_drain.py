"""Durable core-ingress drain contracts for a fenced Actor v2 migration.

Unlike an adapter pause proof, this protocol covers normalized messages after
they reach ``MessageIngress``.  A migrating ownership row plus each process's
local core ingress freeze ensures post-freeze messages are persisted behind the
migration generation rather than entering legacy routing.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from enum import StrEnum

from shinbot.core.dispatch.agent_identity import SessionKey

_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
_SUMMARY_CODE_PATTERN = re.compile(r"[a-z][a-z0-9_.:-]{0,127}")


class ActorV2CoreIngressDrainStatus(StrEnum):
    """Forward-only state of one barrier-bound core ingress drain request."""

    ASSEMBLING = "assembling"
    OPEN = "open"
    DRAINED = "drained"


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainMember:
    """One immutable process membership included in a core drain request."""

    request_id: str
    member_id: str
    adapter_instance_id: str
    participant_id: str
    participant_epoch: int

    def __post_init__(self) -> None:
        """Normalize exact member identity copied from durable registration."""

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
            _positive_integer(self.participant_epoch, "participant_epoch"),
        )


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainReceipt:
    """Token-free local evidence submitted by one frozen process member."""

    core_ingress_digest: str
    legacy_quiescence_digest: str
    proof_epoch: int
    summary_code: str

    def __post_init__(self) -> None:
        """Reject raw ticket material and normalize bounded proof metadata."""

        object.__setattr__(
            self,
            "core_ingress_digest",
            _digest(self.core_ingress_digest, "core_ingress_digest"),
        )
        object.__setattr__(
            self,
            "legacy_quiescence_digest",
            _digest(self.legacy_quiescence_digest, "legacy_quiescence_digest"),
        )
        object.__setattr__(self, "proof_epoch", _positive_integer(self.proof_epoch, "proof_epoch"))
        object.__setattr__(
            self,
            "summary_code",
            _summary_code(self.summary_code, "summary_code"),
        )


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainAcknowledgement:
    """Immutable durable acknowledgement for one exact core ingress member."""

    request_id: str
    member_id: str
    core_ingress_digest: str
    legacy_quiescence_digest: str
    proof_epoch: int
    summary_code: str
    acknowledged_at: float

    def __post_init__(self) -> None:
        """Normalize token-free persisted acknowledgement metadata."""

        object.__setattr__(self, "request_id", _identifier(self.request_id, "request_id"))
        object.__setattr__(self, "member_id", _identifier(self.member_id, "member_id"))
        object.__setattr__(
            self,
            "core_ingress_digest",
            _digest(self.core_ingress_digest, "core_ingress_digest"),
        )
        object.__setattr__(
            self,
            "legacy_quiescence_digest",
            _digest(self.legacy_quiescence_digest, "legacy_quiescence_digest"),
        )
        object.__setattr__(self, "proof_epoch", _positive_integer(self.proof_epoch, "proof_epoch"))
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
    def receipt(self) -> ActorV2CoreIngressDrainReceipt:
        """Return proof metadata without durable request/member identity."""

        return ActorV2CoreIngressDrainReceipt(
            core_ingress_digest=self.core_ingress_digest,
            legacy_quiescence_digest=self.legacy_quiescence_digest,
            proof_epoch=self.proof_epoch,
            summary_code=self.summary_code,
        )


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainRequest:
    """Sealed process coverage and receipts for one migration barrier epoch."""

    request_id: str
    barrier_id: str
    key: SessionKey
    legacy_session_id: str
    adapter_instance_ids: tuple[str, ...]
    source_generation: int
    migration_generation: int
    status: ActorV2CoreIngressDrainStatus
    created_at: float
    updated_at: float
    drained_at: float | None
    members: tuple[ActorV2CoreIngressDrainMember, ...]
    acknowledgements: tuple[ActorV2CoreIngressDrainAcknowledgement, ...] = ()

    def __post_init__(self) -> None:
        """Require sealed full adapter coverage and exact acknowledgement state."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("core ingress drain key must be a SessionKey")
        request_id = _identifier(self.request_id, "request_id")
        barrier_id = _identifier(self.barrier_id, "barrier_id")
        legacy_session_id = _identifier(self.legacy_session_id, "legacy_session_id")
        adapter_instance_ids = _adapter_instance_ids(self.adapter_instance_ids)
        source_generation = _positive_integer(
            self.source_generation,
            "source_generation",
        )
        migration_generation = _positive_integer(
            self.migration_generation,
            "migration_generation",
        )
        if migration_generation != source_generation + 1:
            raise ValueError("core ingress drain must bind the next migration generation")
        status = ActorV2CoreIngressDrainStatus(self.status)
        created_at = _finite_time(self.created_at, "created_at")
        updated_at = _finite_time(self.updated_at, "updated_at")
        drained_at = _optional_time(self.drained_at, "drained_at")
        if updated_at < created_at:
            raise ValueError("core ingress drain updated_at must not precede created_at")
        members = tuple(self.members)
        if not members or any(
            not isinstance(member, ActorV2CoreIngressDrainMember) for member in members
        ):
            raise ValueError("core ingress drain requires typed members")
        if any(member.request_id != request_id for member in members):
            raise ValueError("core ingress drain member belongs to another request")
        member_ids = tuple(member.member_id for member in members)
        if len(set(member_ids)) != len(member_ids):
            raise ValueError("core ingress drain cannot repeat a member")
        if not set(adapter_instance_ids).issubset(
            {member.adapter_instance_id for member in members}
        ):
            raise ValueError("core ingress drain lacks adapter membership coverage")
        acknowledgements = tuple(self.acknowledgements)
        if any(
            not isinstance(item, ActorV2CoreIngressDrainAcknowledgement)
            for item in acknowledgements
        ):
            raise TypeError("core ingress drain acknowledgements must be typed")
        acknowledgement_ids = tuple(item.member_id for item in acknowledgements)
        if len(set(acknowledgement_ids)) != len(acknowledgement_ids):
            raise ValueError("core ingress drain cannot repeat an acknowledgement")
        if any(item.request_id != request_id for item in acknowledgements):
            raise ValueError("core ingress acknowledgement belongs to another request")
        if not set(acknowledgement_ids).issubset(member_ids):
            raise ValueError("core ingress acknowledgement is outside member coverage")
        if status is ActorV2CoreIngressDrainStatus.ASSEMBLING:
            if acknowledgements or drained_at is not None:
                raise ValueError("assembling core ingress drain cannot retain receipts")
        elif status is ActorV2CoreIngressDrainStatus.OPEN:
            if drained_at is not None:
                raise ValueError("open core ingress drain cannot retain drained_at")
        else:
            if drained_at is None or set(acknowledgement_ids) != set(member_ids):
                raise ValueError("drained core ingress request requires every acknowledgement")
            if drained_at != updated_at:
                raise ValueError("drained_at must equal terminal update time")
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "barrier_id", barrier_id)
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        object.__setattr__(self, "adapter_instance_ids", adapter_instance_ids)
        object.__setattr__(self, "source_generation", source_generation)
        object.__setattr__(self, "migration_generation", migration_generation)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "drained_at", drained_at)
        object.__setattr__(
            self,
            "members",
            tuple(
                sorted(
                    members,
                    key=lambda item: (
                        item.adapter_instance_id,
                        item.participant_id,
                        item.participant_epoch,
                        item.member_id,
                    ),
                )
            ),
        )
        object.__setattr__(
            self,
            "acknowledgements",
            tuple(sorted(acknowledgements, key=lambda item: item.member_id)),
        )

    @property
    def unacknowledged_members(self) -> tuple[ActorV2CoreIngressDrainMember, ...]:
        """Return frozen members that have not acknowledged local core drain."""

        acknowledged = {item.member_id for item in self.acknowledgements}
        return tuple(member for member in self.members if member.member_id not in acknowledged)

    @property
    def durably_drained(self) -> bool:
        """Return whether every member is acknowledged and controller-confirmed."""

        return (
            self.status is ActorV2CoreIngressDrainStatus.DRAINED
            and not self.unacknowledged_members
        )

    def core_ingress_proof_digest(self) -> str:
        """Derive canonical token-free evidence for journal core ingress proof."""

        return self._proof_digest("core_ingress_digest", "core_ingress_drain")

    def legacy_quiescence_proof_digest(self) -> str:
        """Derive canonical token-free evidence for journal legacy quiescence."""

        return self._proof_digest("legacy_quiescence_digest", "legacy_quiescence")

    def _proof_digest(self, field_name: str, proof_kind: str) -> str:
        """Hash one exact acknowledged proof stream without local capabilities."""

        if not self.durably_drained:
            raise ActorV2CoreIngressDrainNotReady(
                "core ingress proof requires a durably drained request"
            )
        acknowledgements = {
            acknowledgement.member_id: acknowledgement
            for acknowledgement in self.acknowledgements
        }
        payload = {
            "adapter_instance_ids": self.adapter_instance_ids,
            "barrier_id": self.barrier_id,
            "legacy_session_id": self.legacy_session_id,
            "members": [
                {
                    "adapter_instance_id": member.adapter_instance_id,
                    "member_id": member.member_id,
                    "participant_epoch": member.participant_epoch,
                    "participant_id": member.participant_id,
                    "proof_digest": getattr(
                        acknowledgements[member.member_id],
                        field_name,
                    ),
                    "proof_epoch": acknowledgements[member.member_id].proof_epoch,
                    "summary_code": acknowledgements[member.member_id].summary_code,
                }
                for member in self.members
            ],
            "migration_generation": self.migration_generation,
            "profile_id": self.key.profile_id,
            "proof_kind": proof_kind,
            "request_id": self.request_id,
            "session_id": self.key.session_id,
            "source_generation": self.source_generation,
        }
        return hashlib.sha256(
            json.dumps(
                payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainDiscoveryCursor:
    """Stable keyset position for one process's open core-drain discovery."""

    created_at: float
    request_id: str

    def __post_init__(self) -> None:
        """Normalize the immutable request ordering identity."""

        object.__setattr__(self, "created_at", _finite_time(self.created_at, "created_at"))
        object.__setattr__(self, "request_id", _identifier(self.request_id, "request_id"))


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainDiscoveryPage:
    """One bounded token-free page of local open drain work."""

    requests: tuple[ActorV2CoreIngressDrainRequest, ...]
    next_cursor: ActorV2CoreIngressDrainDiscoveryCursor | None = None
    has_more: bool = False

    def __post_init__(self) -> None:
        """Require open, ordered requests and a cursor only for a continued page."""

        requests = tuple(self.requests)
        if any(
            not isinstance(request, ActorV2CoreIngressDrainRequest)
            or request.status is not ActorV2CoreIngressDrainStatus.OPEN
            for request in requests
        ):
            raise TypeError("core ingress discovery page requires open typed requests")
        identities = tuple((request.created_at, request.request_id) for request in requests)
        if len(set(identities)) != len(identities) or identities != tuple(sorted(identities)):
            raise ValueError("core ingress discovery page requests must be uniquely ordered")
        if not isinstance(self.has_more, bool):
            raise TypeError("has_more must be a boolean")
        cursor = self.next_cursor
        if cursor is not None and not isinstance(
            cursor,
            ActorV2CoreIngressDrainDiscoveryCursor,
        ):
            raise TypeError("next_cursor must be a core ingress discovery cursor")
        if self.has_more:
            if not requests or cursor is None:
                raise ValueError("continued core ingress discovery requires a cursor")
            if (cursor.created_at, cursor.request_id) != identities[-1]:
                raise ValueError("core ingress discovery cursor must follow the final request")
        elif cursor is not None:
            raise ValueError("terminal core ingress discovery page cannot retain a cursor")
        object.__setattr__(self, "requests", requests)


class ActorV2CoreIngressDrainError(RuntimeError):
    """Base error for the fail-closed core ingress drain protocol."""


class ActorV2CoreIngressDrainConflict(ActorV2CoreIngressDrainError):
    """Raised when a barrier, member, or request identity no longer matches."""


class ActorV2CoreIngressDrainCoverageError(ActorV2CoreIngressDrainConflict):
    """Raised when a barrier adapter lacks an active ingress process member."""

    def __init__(self, adapter_instance_ids: tuple[str, ...]) -> None:
        """Expose only missing adapter identities, never holder capabilities."""

        self.adapter_instance_ids = _adapter_instance_ids(adapter_instance_ids)
        super().__init__(
            "core ingress drain lacks active coverage for: "
            + ", ".join(self.adapter_instance_ids)
        )


class ActorV2CoreIngressDrainNotFound(ActorV2CoreIngressDrainError):
    """Raised when a request or durable core ingress member is absent."""


class ActorV2CoreIngressDrainNotReady(ActorV2CoreIngressDrainConflict):
    """Raised when controller confirmation lacks one or more member receipts."""


def _identifier(value: object, field_name: str) -> str:
    """Normalize one required opaque durable identity."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"core ingress drain {field_name} must not be empty")
    return normalized


def _positive_integer(value: object, field_name: str) -> int:
    """Require one positive non-boolean generation or epoch."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"core ingress drain {field_name} must be positive")
    return value


def _finite_time(value: object, field_name: str) -> float:
    """Normalize one finite timestamp."""

    if isinstance(value, bool):
        raise ValueError(f"core ingress drain {field_name} must be finite")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"core ingress drain {field_name} must be finite")
    return numeric


def _optional_time(value: object, field_name: str) -> float | None:
    """Normalize one optional finite timestamp."""

    return None if value is None else _finite_time(value, field_name)


def _digest(value: object, field_name: str) -> str:
    """Require a canonical SHA-256 digest instead of raw local evidence."""

    normalized = str(value or "").strip().lower()
    if _DIGEST_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"core ingress drain {field_name} must be a SHA-256 digest")
    return normalized


def _summary_code(value: object, field_name: str) -> str:
    """Require bounded operator-safe diagnostic metadata."""

    normalized = str(value or "").strip()
    if _SUMMARY_CODE_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"core ingress drain {field_name} must be a stable lowercase code")
    return normalized


def _adapter_instance_ids(values: object) -> tuple[str, ...]:
    """Return a canonical non-empty adapter instance set."""

    if isinstance(values, str):
        raise TypeError("adapter_instance_ids must be iterable, not a string")
    try:
        normalized = tuple(_identifier(value, "adapter_instance_id") for value in values)
    except TypeError as exc:
        raise TypeError("adapter_instance_ids must be iterable") from exc
    if not normalized or len(set(normalized)) != len(normalized):
        raise ValueError("core ingress drain adapter instances must be a non-empty unique set")
    return tuple(sorted(normalized))


__all__ = [
    "ActorV2CoreIngressDrainAcknowledgement",
    "ActorV2CoreIngressDrainConflict",
    "ActorV2CoreIngressDrainCoverageError",
    "ActorV2CoreIngressDrainDiscoveryCursor",
    "ActorV2CoreIngressDrainDiscoveryPage",
    "ActorV2CoreIngressDrainError",
    "ActorV2CoreIngressDrainMember",
    "ActorV2CoreIngressDrainNotFound",
    "ActorV2CoreIngressDrainNotReady",
    "ActorV2CoreIngressDrainReceipt",
    "ActorV2CoreIngressDrainRequest",
    "ActorV2CoreIngressDrainStatus",
]
