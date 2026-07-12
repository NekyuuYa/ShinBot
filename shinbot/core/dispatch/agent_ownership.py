"""Core-owned durable ownership contract for Agent session runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from shinbot.core.dispatch.agent_identity import SessionKey


class AgentRuntimeOwnershipMode(StrEnum):
    """Mutually exclusive runtime implementations for one session key."""

    LEGACY = "legacy"
    ACTOR_V2 = "actor_v2"


class AgentRuntimeOwnershipStatus(StrEnum):
    """Lifecycle status of a durable runtime ownership record."""

    ACTIVE = "active"
    MIGRATING = "migrating"


class AgentRuntimeOwnershipEventType(StrEnum):
    """Append-only ownership audit event kinds."""

    CLAIMED = "claimed"
    MIGRATION_STARTED = "migration_started"
    MIGRATION_COMPLETED = "migration_completed"
    MIGRATION_ABORTED = "migration_aborted"


@dataclass(slots=True, frozen=True)
class AgentRuntimeOwnership:
    """Durable runtime ownership state for one stable session key."""

    key: SessionKey
    legacy_session_id: str
    mode: AgentRuntimeOwnershipMode
    status: AgentRuntimeOwnershipStatus
    generation: int
    pending_mode: AgentRuntimeOwnershipMode | None = None
    selection_reason: str = ""
    migration_reason: str = ""
    requested_by: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate generation and migration-state consistency."""

        if self.generation < 1:
            raise ValueError("ownership generation must be at least one")
        legacy_session_id = str(self.legacy_session_id or "").strip()
        if not legacy_session_id:
            raise ValueError("legacy_session_id must not be empty")
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        if self.status is AgentRuntimeOwnershipStatus.ACTIVE:
            if self.pending_mode is not None:
                raise ValueError("active ownership cannot have a pending mode")
        elif self.pending_mode is None or self.pending_mode is self.mode:
            raise ValueError(
                "migrating ownership requires a different pending mode"
            )

    @property
    def actor_v2_active(self) -> bool:
        """Return whether actor v2 exclusively owns this session."""

        return (
            self.status is AgentRuntimeOwnershipStatus.ACTIVE
            and self.mode is AgentRuntimeOwnershipMode.ACTOR_V2
        )


@dataclass(slots=True, frozen=True)
class AgentRuntimeOwnershipClaim:
    """Result of an atomic first claim or idempotent same-mode retry."""

    ownership: AgentRuntimeOwnership
    created: bool


@dataclass(slots=True, frozen=True)
class AgentRuntimeOwnershipEvent:
    """Append-only ownership transition audit record."""

    event_id: str
    key: SessionKey
    event_type: AgentRuntimeOwnershipEventType
    generation: int
    from_mode: AgentRuntimeOwnershipMode | None
    to_mode: AgentRuntimeOwnershipMode
    status: AgentRuntimeOwnershipStatus
    reason: str
    requested_by: str = ""
    created_at: float = 0.0


class AgentRuntimeOwnershipError(RuntimeError):
    """Base error for a fail-closed ownership operation."""


class AgentRuntimeOwnershipNotFound(AgentRuntimeOwnershipError):
    """Raised when no ownership decision exists for a session key."""


class AgentRuntimeOwnershipConflict(AgentRuntimeOwnershipError):
    """Raised when the requested mode conflicts with durable ownership."""


class AgentRuntimeOwnershipEvidenceConflict(AgentRuntimeOwnershipConflict):
    """Raised when persisted runtime state contradicts a mode selection."""

    def __init__(self, message: str, *, evidence: tuple[str, ...]) -> None:
        """Store the conflicting durable evidence for diagnostics."""

        self.evidence = evidence
        super().__init__(message)


class AgentRuntimeOwnershipGenerationConflict(AgentRuntimeOwnershipConflict):
    """Raised when migration CAS observes a stale generation."""


class AgentRuntimeOwnershipMigrationConflict(AgentRuntimeOwnershipConflict):
    """Raised when a migration status or target is invalid."""


class AgentRuntimeOwnershipRequired(AgentRuntimeOwnershipConflict):
    """Raised when a transaction requires active actor-v2 ownership."""


__all__ = [
    "AgentRuntimeOwnership",
    "AgentRuntimeOwnershipClaim",
    "AgentRuntimeOwnershipConflict",
    "AgentRuntimeOwnershipError",
    "AgentRuntimeOwnershipEvent",
    "AgentRuntimeOwnershipEventType",
    "AgentRuntimeOwnershipEvidenceConflict",
    "AgentRuntimeOwnershipGenerationConflict",
    "AgentRuntimeOwnershipMigrationConflict",
    "AgentRuntimeOwnershipMode",
    "AgentRuntimeOwnershipNotFound",
    "AgentRuntimeOwnershipRequired",
    "AgentRuntimeOwnershipStatus",
]
