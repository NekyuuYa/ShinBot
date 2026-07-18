"""Durable contracts for a future Actor v2 clean-canary isolation lease.

The lease deliberately has no automatic expiry. A process that died while
holding a clean-canary harness cannot be distinguished from a slow process by
SQLite alone, so expiry-based takeover would permit overlapping harnesses. A
later controller must release after a stop proof or use an explicit, audited
revocation after proving the old holder is gone.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipConflict


class ActorV2CanaryIsolationLeaseStatus(StrEnum):
    """Durable lifecycle state for the database-wide canary isolation slot."""

    ACTIVE = "active"
    RELEASED = "released"
    REVOKED = "revoked"


@dataclass(slots=True, frozen=True)
class ActorV2CanaryIsolationLeaseSnapshot:
    """Token-free durable state for one canary-isolation lease epoch."""

    lease_epoch: int
    holder_id: str
    status: ActorV2CanaryIsolationLeaseStatus
    created_at: float
    updated_at: float
    released_at: float | None = None
    revoked_at: float | None = None
    revocation_reason: str = ""

    def __post_init__(self) -> None:
        """Normalize durable state and reject ambiguous lifecycle encodings."""

        lease_epoch = self.lease_epoch
        if isinstance(lease_epoch, bool) or not isinstance(lease_epoch, int):
            raise ValueError("canary isolation lease_epoch must be an integer")
        if lease_epoch < 1:
            raise ValueError("canary isolation lease_epoch must be positive")
        holder_id = str(self.holder_id or "").strip()
        if not holder_id:
            raise ValueError("canary isolation lease holder_id must not be empty")
        status = ActorV2CanaryIsolationLeaseStatus(self.status)
        created_at = _finite_time(self.created_at, "created_at")
        updated_at = _finite_time(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ValueError("canary isolation updated_at must not precede created_at")
        released_at = (
            None
            if self.released_at is None
            else _finite_time(self.released_at, "released_at")
        )
        revoked_at = (
            None
            if self.revoked_at is None
            else _finite_time(self.revoked_at, "revoked_at")
        )
        reason = str(self.revocation_reason or "").strip()
        if status is ActorV2CanaryIsolationLeaseStatus.ACTIVE:
            if released_at is not None or revoked_at is not None or reason:
                raise ValueError("active canary isolation lease retains terminal state")
        elif status is ActorV2CanaryIsolationLeaseStatus.RELEASED:
            if released_at is None or released_at != updated_at:
                raise ValueError("released canary isolation lease requires released_at")
            if revoked_at is not None or reason:
                raise ValueError("released canary isolation lease retains revocation state")
        else:
            if revoked_at is None or revoked_at != updated_at or not reason:
                raise ValueError("revoked canary isolation lease requires revocation state")
            if released_at is not None:
                raise ValueError("revoked canary isolation lease retains release state")
        object.__setattr__(self, "holder_id", holder_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "released_at", released_at)
        object.__setattr__(self, "revoked_at", revoked_at)
        object.__setattr__(self, "revocation_reason", reason)


@dataclass(slots=True, frozen=True)
class ActorV2CanaryIsolationLeaseGrant:
    """Opaque capability held by exactly one active canary lifecycle."""

    lease: ActorV2CanaryIsolationLeaseSnapshot
    holder_token: str

    def __post_init__(self) -> None:
        """Require an active snapshot and a non-empty opaque holder token."""

        if not isinstance(self.lease, ActorV2CanaryIsolationLeaseSnapshot):
            raise TypeError("canary isolation grant requires a lease snapshot")
        if self.lease.status is not ActorV2CanaryIsolationLeaseStatus.ACTIVE:
            raise ValueError("canary isolation grant requires an active lease")
        holder_token = str(self.holder_token or "").strip()
        if not holder_token:
            raise ValueError("canary isolation grant holder_token must not be empty")
        object.__setattr__(self, "holder_token", holder_token)


class ActorV2CanaryIsolationLeaseError(RuntimeError):
    """Base error for fail-closed canary isolation lease operations."""


class ActorV2CanaryIsolationLeaseConflict(ActorV2CanaryIsolationLeaseError):
    """Raised when a requested lifecycle transition conflicts with durable state."""


class ActorV2CanaryIsolationLeaseBlocked(
    ActorV2CanaryIsolationLeaseConflict,
    AgentRuntimeOwnershipConflict,
):
    """Raised when an active canary lease blocks Actor v2 competing work.

    This remains a distinct canary-lifecycle error while also participating in
    the ownership-conflict family. Transactional Actor consumers already treat
    that family as a lost execution authority, which prevents a newly added
    domain-level interlock from escaping into an unclassified worker retry.
    """


class ActorV2CanaryIsolationLeaseLost(ActorV2CanaryIsolationLeaseError):
    """Raised when a holder token no longer names the current lease epoch."""


class ActorV2CanaryIsolationLeaseNotFound(ActorV2CanaryIsolationLeaseError):
    """Raised when a caller requires a lease epoch that has never existed."""


def _finite_time(value: object, field_name: str) -> float:
    """Normalize one finite timestamp carried by durable lease state."""

    if isinstance(value, bool):
        raise ValueError(f"canary isolation {field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"canary isolation {field_name} must be finite")
    return normalized


__all__ = [
    "ActorV2CanaryIsolationLeaseBlocked",
    "ActorV2CanaryIsolationLeaseConflict",
    "ActorV2CanaryIsolationLeaseError",
    "ActorV2CanaryIsolationLeaseGrant",
    "ActorV2CanaryIsolationLeaseLost",
    "ActorV2CanaryIsolationLeaseNotFound",
    "ActorV2CanaryIsolationLeaseSnapshot",
    "ActorV2CanaryIsolationLeaseStatus",
]
