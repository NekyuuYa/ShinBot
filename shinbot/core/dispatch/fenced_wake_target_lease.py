"""Durable publication contracts for a future fenced Actor wake target."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget


class FencedWakeTargetLeaseStatus(StrEnum):
    """Durable lifecycle state for one target-publication epoch."""

    ACTIVE = "active"
    RELEASED = "released"


@dataclass(slots=True, frozen=True)
class FencedWakeTargetLease:
    """Token-free durable target-publication snapshot for one Actor owner."""

    request: FencedMailboxWakeRequest
    target: MailboxHandoffTarget
    lease_epoch: int
    status: FencedWakeTargetLeaseStatus
    expires_at: float
    created_at: float
    updated_at: float
    released_at: float | None = None

    def __post_init__(self) -> None:
        """Reject a lease that cannot identify one exact current target."""

        if not isinstance(self.request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        if not self.request.has_admission_fence:
            raise ValueError("fenced wake target lease requires an admission-fenced request")
        if not isinstance(self.target, MailboxHandoffTarget):
            raise TypeError("target must be a MailboxHandoffTarget")
        if isinstance(self.lease_epoch, bool) or not isinstance(self.lease_epoch, int):
            raise ValueError("lease_epoch must be an integer")
        if self.lease_epoch < 1:
            raise ValueError("lease_epoch must be positive")
        status = FencedWakeTargetLeaseStatus(self.status)
        expires_at = _finite_time(self.expires_at, "expires_at")
        created_at = _finite_time(self.created_at, "created_at")
        updated_at = _finite_time(self.updated_at, "updated_at")
        released_at = (
            None
            if self.released_at is None
            else _finite_time(self.released_at, "released_at")
        )
        if expires_at <= created_at:
            raise ValueError("expires_at must be after created_at")
        if updated_at < created_at:
            raise ValueError("updated_at must not precede created_at")
        if status is FencedWakeTargetLeaseStatus.ACTIVE:
            if released_at is not None:
                raise ValueError("active wake target lease cannot have released_at")
        else:
            if released_at is None:
                raise ValueError("released wake target lease requires released_at")
            if released_at < created_at:
                raise ValueError("released_at must not precede created_at")
            if released_at != updated_at:
                raise ValueError("released wake target lease must update at released_at")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "released_at", released_at)

    def expired_at(self, now: float) -> bool:
        """Return whether the active publication is no longer live at ``now``."""

        return self.expires_at <= _finite_time(now, "now")


@dataclass(slots=True, frozen=True)
class FencedWakeTargetLeaseGrant:
    """Capability returned only to the target incarnation that acquired a lease."""

    lease: FencedWakeTargetLease
    holder_token: str

    def __post_init__(self) -> None:
        """Require a non-empty opaque token beside an exact target snapshot."""

        if not isinstance(self.lease, FencedWakeTargetLease):
            raise TypeError("lease must be a FencedWakeTargetLease")
        token = str(self.holder_token or "").strip()
        if not token:
            raise ValueError("holder_token must not be empty")
        object.__setattr__(self, "holder_token", token)


@dataclass(slots=True, frozen=True)
class FencedActorExecutionBinding:
    """Bind one actor's durable work to its owner and target lease capability.

    A fenced owner alone is not enough after a wake-target incarnation expires
    or is replaced.  Actor persistence operations that receive this value must
    validate the contained target lease in the same transaction as ownership.
    """

    request: FencedMailboxWakeRequest
    target_lease: FencedWakeTargetLeaseGrant

    def __post_init__(self) -> None:
        """Require the target capability to name the exact actor owner."""

        if not isinstance(self.request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        if not self.request.has_admission_fence:
            raise ValueError("actor execution binding requires an admission-fenced request")
        if not isinstance(self.target_lease, FencedWakeTargetLeaseGrant):
            raise TypeError("target_lease must be a FencedWakeTargetLeaseGrant")
        if self.target_lease.lease.request != self.request:
            raise ValueError("target lease request does not match actor execution binding")
        if self.target_lease.lease.status is not FencedWakeTargetLeaseStatus.ACTIVE:
            raise ValueError("actor execution binding requires an active target lease")

    def has_same_authority(self, other: object) -> bool:
        """Return whether another binding retains this target lease capability."""

        return (
            isinstance(other, FencedActorExecutionBinding)
            and self.request == other.request
            and self.target_lease.lease.target == other.target_lease.lease.target
            and self.target_lease.lease.lease_epoch == other.target_lease.lease.lease_epoch
            and self.target_lease.holder_token == other.target_lease.holder_token
        )


class FencedWakeTargetLeaseError(RuntimeError):
    """Base error for fail-closed target-publication operations."""


class FencedWakeTargetLeaseConflict(FencedWakeTargetLeaseError):
    """Raised when another target or incarnation owns the publication slot."""


class FencedWakeTargetLeaseExpired(FencedWakeTargetLeaseConflict):
    """Raised when a holder uses an otherwise matching expired publication."""


class FencedWakeTargetLeaseLost(FencedWakeTargetLeaseError):
    """Raised when a caller's opaque target-publication capability is stale."""


def _finite_time(value: object, field_name: str) -> float:
    """Normalize one finite timestamp carried by a durable lease."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite")
    return normalized


__all__ = [
    "FencedActorExecutionBinding",
    "FencedWakeTargetLease",
    "FencedWakeTargetLeaseConflict",
    "FencedWakeTargetLeaseError",
    "FencedWakeTargetLeaseExpired",
    "FencedWakeTargetLeaseGrant",
    "FencedWakeTargetLeaseLost",
    "FencedWakeTargetLeaseStatus",
]
