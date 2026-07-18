"""Fenced ownership-migration contracts for a future Actor v2 cutover.

``AgentRuntimeOwnershipStatus.MIGRATING`` already forms a durable core ingress
barrier, but the generic migration API has no holder capability.  These types
model the missing controller-owned authority without mounting it on any
production entry point.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnership


class ActorV2MigrationBarrierStatus(StrEnum):
    """Forward-only lifecycle of one legacy-to-Actor migration barrier."""

    MIGRATING = "migrating"
    COMPLETED = "completed"
    ABORTED = "aborted"


@dataclass(slots=True, frozen=True)
class ActorV2MigrationBarrier:
    """Token-free durable snapshot bound to one ownership migration generation."""

    key: SessionKey
    barrier_id: str
    legacy_session_id: str
    adapter_instance_ids: tuple[str, ...]
    source_generation: int
    migration_generation: int
    status: ActorV2MigrationBarrierStatus
    holder_id: str
    created_at: float
    updated_at: float
    aborted_at: float | None = None
    abort_reason: str = ""
    completed_at: float | None = None
    completion_reason: str = ""
    completion_manifest_id: str = ""

    def __post_init__(self) -> None:
        """Validate immutable generation identity and terminal state metadata.

        Completion is represented by a separate immutable handoff-finalization
        record rather than by rewriting the original barrier row.  The snapshot
        nevertheless exposes that sidecar as the barrier's effective terminal
        state so callers cannot mistake a completed source boundary for an
        active migration lease.
        """

        if not isinstance(self.key, SessionKey):
            raise TypeError("migration barrier key must be a SessionKey")
        barrier_id = _identifier(self.barrier_id, "barrier_id")
        legacy_session_id = _identifier(self.legacy_session_id, "legacy_session_id")
        adapter_instance_ids = _adapter_instance_ids(self.adapter_instance_ids)
        source_generation = _positive_generation(
            self.source_generation,
            "source_generation",
        )
        migration_generation = _positive_generation(
            self.migration_generation,
            "migration_generation",
        )
        if migration_generation != source_generation + 1:
            raise ValueError("migration barrier must bind the immediate next generation")
        status = ActorV2MigrationBarrierStatus(self.status)
        holder_id = _identifier(self.holder_id, "holder_id")
        created_at = _finite_time(self.created_at, "created_at")
        updated_at = _finite_time(self.updated_at, "updated_at")
        aborted_at = _optional_time(self.aborted_at, "aborted_at")
        abort_reason = str(self.abort_reason or "").strip()
        completed_at = _optional_time(self.completed_at, "completed_at")
        completion_reason = str(self.completion_reason or "").strip()
        completion_manifest_id = str(self.completion_manifest_id or "").strip()
        if updated_at < created_at:
            raise ValueError("migration barrier updated_at must not precede created_at")
        if status is ActorV2MigrationBarrierStatus.MIGRATING:
            if (
                aborted_at is not None
                or abort_reason
                or completed_at is not None
                or completion_reason
                or completion_manifest_id
            ):
                raise ValueError("migrating barrier cannot retain terminal metadata")
        elif status is ActorV2MigrationBarrierStatus.ABORTED:
            if (
                aborted_at is None
                or aborted_at != updated_at
                or not abort_reason
                or completed_at is not None
                or completion_reason
                or completion_manifest_id
            ):
                raise ValueError("aborted barrier requires only abort terminal metadata")
        elif (
            completed_at is None
            or completed_at != updated_at
            or not completion_reason
            or not completion_manifest_id
            or aborted_at is not None
            or abort_reason
        ):
            raise ValueError("completed barrier requires only completion terminal metadata")
        object.__setattr__(self, "barrier_id", barrier_id)
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        object.__setattr__(self, "adapter_instance_ids", adapter_instance_ids)
        object.__setattr__(self, "source_generation", source_generation)
        object.__setattr__(self, "migration_generation", migration_generation)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "holder_id", holder_id)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "aborted_at", aborted_at)
        object.__setattr__(self, "abort_reason", abort_reason)
        object.__setattr__(self, "completed_at", completed_at)
        object.__setattr__(self, "completion_reason", completion_reason)
        object.__setattr__(self, "completion_manifest_id", completion_manifest_id)

    @property
    def active(self) -> bool:
        """Return whether this barrier still exclusively owns migration control."""

        return self.status is ActorV2MigrationBarrierStatus.MIGRATING


@dataclass(slots=True, frozen=True)
class ActorV2MigrationBarrierGrant:
    """Capability held only by the controller that started the migration barrier."""

    barrier: ActorV2MigrationBarrier
    holder_token: str = field(repr=False)

    def __post_init__(self) -> None:
        """Require an active barrier and opaque holder capability."""

        if not isinstance(self.barrier, ActorV2MigrationBarrier):
            raise TypeError("migration barrier grant requires a typed barrier")
        if not self.barrier.active:
            raise ValueError("migration barrier grant requires an active barrier")
        object.__setattr__(
            self,
            "holder_token",
            _identifier(self.holder_token, "holder_token"),
        )


@dataclass(slots=True, frozen=True)
class ActorV2MigrationBarrierAbortResult:
    """Exact terminal barrier snapshot and the ownership restored by its holder."""

    barrier: ActorV2MigrationBarrier
    ownership: AgentRuntimeOwnership

    def __post_init__(self) -> None:
        """Require an aborted barrier and matching restored legacy ownership."""

        if not isinstance(self.barrier, ActorV2MigrationBarrier):
            raise TypeError("migration barrier abort result requires a typed barrier")
        if not isinstance(self.ownership, AgentRuntimeOwnership):
            raise TypeError("migration barrier abort result requires typed ownership")
        if self.barrier.status is not ActorV2MigrationBarrierStatus.ABORTED:
            raise ValueError("migration barrier abort result requires an aborted barrier")
        if self.ownership.key != self.barrier.key:
            raise ValueError("restored ownership belongs to another migration barrier")
        if self.ownership.generation != self.barrier.migration_generation + 1:
            raise ValueError("restored ownership generation does not follow barrier")


class ActorV2MigrationBarrierError(RuntimeError):
    """Base error for a fail-closed fenced ownership migration."""


class ActorV2MigrationBarrierConflict(ActorV2MigrationBarrierError):
    """Raised when barrier or ownership identity changed incompatibly."""


class ActorV2LegacyStateHandoffRequired(ActorV2MigrationBarrierConflict):
    """Raised when a legacy source has state no current manifest can transfer."""

    def __init__(self, evidence: tuple[str, ...]) -> None:
        """Expose stable source-state classes without raw scheduler payloads."""

        if not evidence:
            raise ValueError("legacy handoff evidence must not be empty")
        self.evidence = tuple(str(item).strip() for item in evidence)
        super().__init__(
            "legacy migration requires a durable source-state handoff for: "
            + ", ".join(self.evidence)
        )


class ActorV2MigrationBarrierNotFound(ActorV2MigrationBarrierError):
    """Raised when a required durable barrier row does not exist."""


class ActorV2MigrationBarrierLost(ActorV2MigrationBarrierConflict):
    """Raised when a holder token no longer names the active barrier epoch."""


def _identifier(value: object, field_name: str) -> str:
    """Normalize one required durable identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"migration barrier {field_name} must not be empty")
    return normalized


def _positive_generation(value: object, field_name: str) -> int:
    """Require one positive non-boolean ownership generation."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"migration barrier {field_name} must be positive")
    return value


def _finite_time(value: object, field_name: str) -> float:
    """Normalize a finite durable timestamp."""

    if isinstance(value, bool):
        raise ValueError(f"migration barrier {field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"migration barrier {field_name} must be finite")
    return normalized


def _optional_time(value: object, field_name: str) -> float | None:
    """Normalize one optional finite timestamp."""

    return None if value is None else _finite_time(value, field_name)


def _adapter_instance_ids(values: Sequence[str]) -> tuple[str, ...]:
    """Return a non-empty canonical adapter instance set for one barrier."""

    if isinstance(values, str):
        raise TypeError("adapter_instance_ids must be an iterable, not a string")
    normalized = tuple(_identifier(value, "adapter_instance_id") for value in values)
    if not normalized:
        raise ValueError("migration barrier requires at least one adapter instance")
    if len(set(normalized)) != len(normalized):
        raise ValueError("migration barrier adapter instances must be unique")
    return tuple(sorted(normalized))


__all__ = [
    "ActorV2MigrationBarrier",
    "ActorV2MigrationBarrierAbortResult",
    "ActorV2MigrationBarrierConflict",
    "ActorV2MigrationBarrierError",
    "ActorV2MigrationBarrierGrant",
    "ActorV2LegacyStateHandoffRequired",
    "ActorV2MigrationBarrierLost",
    "ActorV2MigrationBarrierNotFound",
    "ActorV2MigrationBarrierStatus",
]
