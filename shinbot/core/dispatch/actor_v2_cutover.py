"""Durable contracts for a future Actor v2 production cutover journal.

The journal is intentionally not a controller or an activation permission. It
records the identity and token-free proofs a future controller must validate
while moving one session through a no-skip cutover sequence. Adapter pause
capabilities, admission holder tokens, and wake-target holder tokens never
belong in these values or their durable representation.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from enum import StrEnum

from shinbot.core.dispatch.agent_identity import SessionKey

_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
_SUMMARY_CODE_PATTERN = re.compile(r"[a-z][a-z0-9_.:-]{0,127}")


class ActorV2CutoverPhase(StrEnum):
    """Durable phases of one forward-only Actor v2 cutover attempt."""

    PREFLIGHTED = "preflighted"
    ADMISSION_RESERVED = "admission_reserved"
    LEGACY_QUIESCED = "legacy_quiesced"
    ACTOR_OWNER_COMMITTED = "actor_owner_committed"
    TARGET_PUBLISHED = "target_published"
    INGRESS_RESUMED = "ingress_resumed"
    BLOCKED = "blocked"


ACTOR_V2_CUTOVER_FORWARD_PHASES: tuple[ActorV2CutoverPhase, ...] = (
    ActorV2CutoverPhase.PREFLIGHTED,
    ActorV2CutoverPhase.ADMISSION_RESERVED,
    ActorV2CutoverPhase.LEGACY_QUIESCED,
    ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED,
    ActorV2CutoverPhase.TARGET_PUBLISHED,
    ActorV2CutoverPhase.INGRESS_RESUMED,
)
"""The only successful forward path. ``BLOCKED`` is terminal and off-path."""


class ActorV2CutoverProofKind(StrEnum):
    """Token-free evidence categories required by each cutover phase."""

    CLEAN_PREFLIGHT = "clean_preflight"
    ADMISSION_RESERVATION = "admission_reservation"
    LEGACY_QUIESCENCE = "legacy_quiescence"
    ADAPTER_PAUSE_DRAIN = "adapter_pause_drain"
    CORE_INGRESS_DRAIN = "core_ingress_drain"
    ACTOR_OWNER_COMMIT = "actor_owner_commit"
    TARGET_PUBLICATION = "target_publication"
    INGRESS_RESUME = "ingress_resume"
    BLOCKED = "blocked"


_ALLOWED_PROOF_KIND_SETS: dict[
    ActorV2CutoverPhase, frozenset[frozenset[ActorV2CutoverProofKind]]
] = {
    ActorV2CutoverPhase.PREFLIGHTED: frozenset(
        {frozenset({ActorV2CutoverProofKind.CLEAN_PREFLIGHT})}
    ),
    ActorV2CutoverPhase.ADMISSION_RESERVED: frozenset(
        {frozenset({ActorV2CutoverProofKind.ADMISSION_RESERVATION})}
    ),
    ActorV2CutoverPhase.LEGACY_QUIESCED: frozenset(
        {
            frozenset(
                {
                    ActorV2CutoverProofKind.LEGACY_QUIESCENCE,
                    ActorV2CutoverProofKind.ADAPTER_PAUSE_DRAIN,
                }
            ),
            frozenset(
                {
                    ActorV2CutoverProofKind.LEGACY_QUIESCENCE,
                    ActorV2CutoverProofKind.CORE_INGRESS_DRAIN,
                }
            ),
        }
    ),
    ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED: frozenset(
        {frozenset({ActorV2CutoverProofKind.ACTOR_OWNER_COMMIT})}
    ),
    ActorV2CutoverPhase.TARGET_PUBLISHED: frozenset(
        {frozenset({ActorV2CutoverProofKind.TARGET_PUBLICATION})}
    ),
    ActorV2CutoverPhase.INGRESS_RESUMED: frozenset(
        {frozenset({ActorV2CutoverProofKind.INGRESS_RESUME})}
    ),
    ActorV2CutoverPhase.BLOCKED: frozenset(
        {frozenset({ActorV2CutoverProofKind.BLOCKED})}
    ),
}


@dataclass(slots=True, frozen=True)
class ActorV2CutoverEvidence:
    """One opaque-proof digest and its safe diagnostic metadata.

    ``digest`` must identify proof material held outside the journal. The
    journal intentionally retains only this digest and stable metadata, never
    the capability or raw pause/quiescence receipt itself.
    """

    kind: ActorV2CutoverProofKind
    issuer_id: str
    proof_epoch: int
    digest: str
    summary_code: str

    def __post_init__(self) -> None:
        """Normalize immutable evidence metadata and reject ambiguous input."""

        kind = ActorV2CutoverProofKind(self.kind)
        issuer_id = _required_identifier(self.issuer_id, "issuer_id")
        proof_epoch = _positive_integer(self.proof_epoch, "proof_epoch")
        digest = str(self.digest or "").strip().lower()
        if _DIGEST_PATTERN.fullmatch(digest) is None:
            raise ValueError("cutover proof digest must be a lowercase SHA-256 hex digest")
        summary_code = _summary_code(self.summary_code, "summary_code")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "issuer_id", issuer_id)
        object.__setattr__(self, "proof_epoch", proof_epoch)
        object.__setattr__(self, "digest", digest)
        object.__setattr__(self, "summary_code", summary_code)


@dataclass(slots=True, frozen=True)
class ActorV2CutoverEvidenceBundle:
    """Exact token-free evidence set for one journal phase transition."""

    phase: ActorV2CutoverPhase
    evidence: tuple[ActorV2CutoverEvidence, ...]

    def __post_init__(self) -> None:
        """Require exactly the proof kinds expected for the target phase."""

        phase = ActorV2CutoverPhase(self.phase)
        evidence = tuple(self.evidence)
        if not evidence:
            raise ValueError("cutover evidence bundle must not be empty")
        if any(not isinstance(item, ActorV2CutoverEvidence) for item in evidence):
            raise TypeError("cutover evidence bundle must contain typed evidence")
        kinds = tuple(item.kind for item in evidence)
        if len(set(kinds)) != len(kinds):
            raise ValueError("cutover evidence bundle cannot repeat a proof kind")
        actual = frozenset(kinds)
        allowed = _ALLOWED_PROOF_KIND_SETS[phase]
        if actual not in allowed:
            expected_values = " or ".join(
                ", ".join(sorted(kind.value for kind in expected))
                for expected in sorted(
                    allowed,
                    key=lambda expected: tuple(sorted(kind.value for kind in expected)),
                )
            )
            raise ValueError(
                f"cutover phase {phase.value!r} requires proof kinds: {expected_values}"
            )
        canonical = tuple(sorted(evidence, key=lambda item: item.kind.value))
        object.__setattr__(self, "phase", phase)
        object.__setattr__(self, "evidence", canonical)


@dataclass(slots=True, frozen=True)
class ActorV2CutoverIdentity:
    """Immutable session and adapter identity for one cutover epoch."""

    key: SessionKey
    cutover_id: str
    cutover_epoch: int
    legacy_session_id: str
    adapter_instance_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        """Canonicalize identity before it reaches a durable primary key."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("cutover identity key must be a SessionKey")
        cutover_id = _required_identifier(self.cutover_id, "cutover_id")
        cutover_epoch = _positive_integer(self.cutover_epoch, "cutover_epoch")
        legacy_session_id = _required_identifier(
            self.legacy_session_id,
            "legacy_session_id",
        )
        adapter_instance_ids = tuple(
            _required_identifier(value, "adapter_instance_id")
            for value in self.adapter_instance_ids
        )
        if not adapter_instance_ids:
            raise ValueError("cutover identity requires at least one adapter instance")
        if len(set(adapter_instance_ids)) != len(adapter_instance_ids):
            raise ValueError("cutover identity adapter instances must be unique")
        object.__setattr__(self, "cutover_id", cutover_id)
        object.__setattr__(self, "cutover_epoch", cutover_epoch)
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        object.__setattr__(self, "adapter_instance_ids", tuple(sorted(adapter_instance_ids)))


@dataclass(slots=True, frozen=True)
class ActorV2CutoverEvent:
    """Immutable proof-bearing event recorded for one journal phase."""

    cutover_id: str
    phase: ActorV2CutoverPhase
    evidence: ActorV2CutoverEvidenceBundle
    occurred_at: float

    def __post_init__(self) -> None:
        """Require evidence to describe exactly the event's target phase."""

        cutover_id = _required_identifier(self.cutover_id, "cutover_id")
        phase = ActorV2CutoverPhase(self.phase)
        if not isinstance(self.evidence, ActorV2CutoverEvidenceBundle):
            raise TypeError("cutover event evidence must be an ActorV2CutoverEvidenceBundle")
        if self.evidence.phase is not phase:
            raise ValueError("cutover event evidence phase does not match event phase")
        occurred_at = _finite_time(self.occurred_at, "occurred_at")
        object.__setattr__(self, "cutover_id", cutover_id)
        object.__setattr__(self, "phase", phase)
        object.__setattr__(self, "occurred_at", occurred_at)


@dataclass(slots=True, frozen=True)
class ActorV2CutoverRecord:
    """Token-free durable snapshot of one forward-only cutover journal."""

    identity: ActorV2CutoverIdentity
    phase: ActorV2CutoverPhase
    initiated_by: str
    created_at: float
    updated_at: float
    admission_fence_id: str = ""
    admission_fence_generation: int = 0
    ownership_generation: int = 0
    target_id: str = ""
    target_incarnation_id: str = ""
    target_lease_epoch: int = 0
    blocked_code: str = ""
    events: tuple[ActorV2CutoverEvent, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        """Validate journal references against the phase reached so far."""

        if not isinstance(self.identity, ActorV2CutoverIdentity):
            raise TypeError("cutover record identity must be an ActorV2CutoverIdentity")
        phase = ActorV2CutoverPhase(self.phase)
        initiated_by = _required_identifier(self.initiated_by, "initiated_by")
        created_at = _finite_time(self.created_at, "created_at")
        updated_at = _finite_time(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ValueError("cutover updated_at must not precede created_at")
        fence_id, fence_generation = _paired_reference(
            self.admission_fence_id,
            self.admission_fence_generation,
            "admission fence",
        )
        ownership_generation = _non_negative_integer(
            self.ownership_generation,
            "ownership_generation",
        )
        target_id, target_lease_epoch = _paired_reference(
            self.target_id,
            self.target_lease_epoch,
            "target lease",
        )
        target_incarnation_id = str(self.target_incarnation_id or "").strip()
        if bool(target_id) != bool(target_incarnation_id):
            raise ValueError("target id and target incarnation id must be set together")
        blocked_code = str(self.blocked_code or "").strip()
        if phase is ActorV2CutoverPhase.PREFLIGHTED:
            _require_empty_references(
                fence_id,
                fence_generation,
                ownership_generation,
                target_id,
                target_incarnation_id,
                target_lease_epoch,
            )
        elif phase in {
            ActorV2CutoverPhase.ADMISSION_RESERVED,
            ActorV2CutoverPhase.LEGACY_QUIESCED,
        }:
            if not fence_id or ownership_generation or target_id:
                raise ValueError("cutover phase requires only an admission fence reference")
        elif phase is ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED:
            if not fence_id or ownership_generation < 1 or target_id:
                raise ValueError("actor owner phase requires owner and admission references")
        elif phase in {
            ActorV2CutoverPhase.TARGET_PUBLISHED,
            ActorV2CutoverPhase.INGRESS_RESUMED,
        }:
            if (
                not fence_id
                or ownership_generation < 1
                or not target_id
                or not target_incarnation_id
            ):
                raise ValueError("target phase requires owner, admission, and target references")
        elif not blocked_code:
            raise ValueError("blocked cutover record requires a stable blocked_code")
        if phase is not ActorV2CutoverPhase.BLOCKED and blocked_code:
            raise ValueError("non-blocked cutover record cannot retain a blocked_code")
        events = tuple(self.events)
        if not events:
            raise ValueError("cutover record requires its preflight event")
        if any(not isinstance(event, ActorV2CutoverEvent) for event in events):
            raise TypeError("cutover record events must contain ActorV2CutoverEvent values")
        if any(event.cutover_id != self.identity.cutover_id for event in events):
            raise ValueError("cutover record event belongs to another cutover identity")
        event_phases = tuple(event.phase for event in events)
        if len(set(event_phases)) != len(event_phases):
            raise ValueError("cutover record cannot repeat a phase event")
        if any(
            event.occurred_at < created_at or event.occurred_at > updated_at
            for event in events
        ):
            raise ValueError("cutover event timestamp falls outside the record lifetime")
        if any(
            later.occurred_at < earlier.occurred_at
            for earlier, later in zip(events, events[1:], strict=False)
        ):
            raise ValueError("cutover events must be ordered by occurrence time")
        _require_event_chain(phase, event_phases)
        object.__setattr__(self, "phase", phase)
        object.__setattr__(self, "initiated_by", initiated_by)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "admission_fence_id", fence_id)
        object.__setattr__(self, "admission_fence_generation", fence_generation)
        object.__setattr__(self, "ownership_generation", ownership_generation)
        object.__setattr__(self, "target_id", target_id)
        object.__setattr__(self, "target_incarnation_id", target_incarnation_id)
        object.__setattr__(self, "target_lease_epoch", target_lease_epoch)
        object.__setattr__(self, "blocked_code", blocked_code)
        object.__setattr__(self, "events", events)


class ActorV2CutoverJournalError(RuntimeError):
    """Base error for durable cutover-journal operations."""


class ActorV2CutoverJournalConflict(ActorV2CutoverJournalError):
    """Raised when journal identity, proof, or phase is no longer current."""


class ActorV2CutoverJournalNotFound(ActorV2CutoverJournalError):
    """Raised when a requested durable cutover journal does not exist."""


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize one non-empty durable identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"cutover {field_name} must not be empty")
    return normalized


def _summary_code(value: object, field_name: str) -> str:
    """Require a stable safe code rather than unbounded raw proof content."""

    normalized = str(value or "").strip()
    if _SUMMARY_CODE_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"cutover {field_name} must be a stable lowercase code")
    return normalized


def _positive_integer(value: object, field_name: str) -> int:
    """Return one strictly positive durable integer."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"cutover {field_name} must be a positive integer")
    return value


def _non_negative_integer(value: object, field_name: str) -> int:
    """Return one non-negative durable integer."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"cutover {field_name} must be a non-negative integer")
    return value


def _paired_reference(
    identifier: object,
    epoch: object,
    field_name: str,
) -> tuple[str, int]:
    """Validate one optional identifier/positive-epoch reference pair."""

    normalized_identifier = str(identifier or "").strip()
    normalized_epoch = _non_negative_integer(epoch, f"{field_name} epoch")
    if bool(normalized_identifier) != bool(normalized_epoch):
        raise ValueError(f"cutover {field_name} identifier and epoch must be set together")
    return normalized_identifier, normalized_epoch


def _require_empty_references(
    fence_id: str,
    fence_generation: int,
    ownership_generation: int,
    target_id: str,
    target_incarnation_id: str,
    target_lease_epoch: int,
) -> None:
    """Reject references before the phase that creates them."""

    if (
        fence_id
        or fence_generation
        or ownership_generation
        or target_id
        or target_incarnation_id
        or target_lease_epoch
    ):
        raise ValueError("preflighted cutover cannot retain later-phase references")


def _require_event_chain(
    phase: ActorV2CutoverPhase,
    event_phases: tuple[ActorV2CutoverPhase, ...],
) -> None:
    """Require journal events to prove every phase without a skip."""

    if phase is ActorV2CutoverPhase.BLOCKED:
        if event_phases[-1] is not ActorV2CutoverPhase.BLOCKED:
            raise ValueError("blocked cutover record must end with a blocked event")
        successful_phases = event_phases[:-1]
    else:
        successful_phases = event_phases
    if not successful_phases:
        raise ValueError("cutover event chain must begin with preflight")
    expected = ACTOR_V2_CUTOVER_FORWARD_PHASES[: len(successful_phases)]
    if successful_phases != expected:
        raise ValueError("cutover event chain contains a skipped or reordered phase")
    if phase is not ActorV2CutoverPhase.BLOCKED:
        if successful_phases[-1] is not phase:
            raise ValueError("cutover record phase does not match its latest event")


def _finite_time(value: object, field_name: str) -> float:
    """Normalize a finite durable timestamp."""

    if isinstance(value, bool):
        raise ValueError(f"cutover {field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"cutover {field_name} must be finite")
    return normalized


__all__ = [
    "ACTOR_V2_CUTOVER_FORWARD_PHASES",
    "ActorV2CutoverEvidence",
    "ActorV2CutoverEvidenceBundle",
    "ActorV2CutoverEvent",
    "ActorV2CutoverIdentity",
    "ActorV2CutoverJournalConflict",
    "ActorV2CutoverJournalError",
    "ActorV2CutoverJournalNotFound",
    "ActorV2CutoverPhase",
    "ActorV2CutoverProofKind",
    "ActorV2CutoverRecord",
]
