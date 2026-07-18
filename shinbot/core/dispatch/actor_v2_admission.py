"""Core contracts for a future fenced Actor v2 session admission.

An admission fence is a durable reservation primitive, not an independently
sufficient ingress-isolation lease. It becomes meaningful only when the core
ownership, ingress, routing, and wake-publication paths all validate the same
token and generation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from shinbot.core.dispatch.agent_identity import SessionKey


class ActorV2AdmissionFenceStatus(StrEnum):
    """Durable lifecycle state for one prospective Actor v2 admission."""

    RESERVED = "reserved"
    COMMITTED = "committed"
    REVOKED = "revoked"


@dataclass(slots=True, frozen=True)
class ActorV2AdmissionFence:
    """Public, token-free snapshot of one durable admission reservation."""

    key: SessionKey
    fence_id: str
    generation: int
    status: ActorV2AdmissionFenceStatus
    holder_id: str
    expires_at: float
    created_at: float
    updated_at: float
    committed_at: float | None = None
    revoked_at: float | None = None
    revocation_reason: str = ""

    def __post_init__(self) -> None:
        """Normalize identifiers and reject ambiguous fence evidence."""

        fence_id = str(self.fence_id or "").strip()
        holder_id = str(self.holder_id or "").strip()
        if not fence_id:
            raise ValueError("admission fence_id must not be empty")
        if not holder_id:
            raise ValueError("admission fence holder_id must not be empty")
        if isinstance(self.generation, bool) or not isinstance(self.generation, int):
            raise ValueError("admission fence generation must be an integer")
        if self.generation < 1:
            raise ValueError("admission fence generation must be positive")
        for field_name in ("expires_at", "created_at", "updated_at"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError(f"admission fence {field_name} must be finite")
            object.__setattr__(self, field_name, float(value))
        for field_name in ("committed_at", "revoked_at"):
            value = getattr(self, field_name)
            if value is None:
                continue
            if isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError(f"admission fence {field_name} must be finite")
            object.__setattr__(self, field_name, float(value))
        if self.status is ActorV2AdmissionFenceStatus.RESERVED:
            if self.committed_at is not None or self.revoked_at is not None:
                raise ValueError("reserved admission fence cannot have terminal timestamps")
        elif self.status is ActorV2AdmissionFenceStatus.COMMITTED:
            if self.committed_at is None or self.revoked_at is not None:
                raise ValueError("committed admission fence requires committed_at only")
        elif self.revoked_at is None:
            raise ValueError("revoked admission fence requires revoked_at")
        object.__setattr__(self, "fence_id", fence_id)
        object.__setattr__(self, "holder_id", holder_id)
        object.__setattr__(self, "revocation_reason", str(self.revocation_reason or "").strip())

    def expired_at(self, now: float) -> bool:
        """Return whether this fence is no longer live at one verified instant."""

        if isinstance(now, bool) or not math.isfinite(float(now)):
            raise ValueError("admission fence comparison time must be finite")
        return self.expires_at <= float(now)


@dataclass(slots=True, frozen=True)
class ActorV2AdmissionGrant:
    """Capability-bearing reservation returned only to the successful holder."""

    fence: ActorV2AdmissionFence
    holder_token: str

    def __post_init__(self) -> None:
        """Require a non-empty opaque token alongside typed fence evidence."""

        if not isinstance(self.fence, ActorV2AdmissionFence):
            raise TypeError("admission grant requires an ActorV2AdmissionFence")
        token = str(self.holder_token or "").strip()
        if not token:
            raise ValueError("admission grant holder_token must not be empty")
        object.__setattr__(self, "holder_token", token)


class ActorV2AdmissionFenceError(RuntimeError):
    """Base error for a fail-closed Actor v2 admission reservation."""


class ActorV2AdmissionFenceConflict(ActorV2AdmissionFenceError):
    """Raised when a reservation conflicts with durable state or another holder."""


class ActorV2AdmissionFenceNotFound(ActorV2AdmissionFenceError):
    """Raised when a required durable admission reservation does not exist."""


class ActorV2AdmissionFenceExpired(ActorV2AdmissionFenceConflict):
    """Raised when an otherwise matching holder attempts to use an expired fence."""


class ActorV2AdmissionFenceReserved(ActorV2AdmissionFenceConflict):
    """Raised when legacy admission would cross an unresolved Actor v2 fence."""

    def __init__(self, fence: ActorV2AdmissionFence) -> None:
        """Expose the token-free fence snapshot for durable buffering decisions."""

        self.fence = fence
        super().__init__(
            "Actor v2 admission fence reserves legacy ingress for "
            f"{fence.key.profile_id}:{fence.key.session_id}"
        )


__all__ = [
    "ActorV2AdmissionFence",
    "ActorV2AdmissionFenceConflict",
    "ActorV2AdmissionFenceError",
    "ActorV2AdmissionFenceExpired",
    "ActorV2AdmissionFenceNotFound",
    "ActorV2AdmissionFenceReserved",
    "ActorV2AdmissionFenceStatus",
    "ActorV2AdmissionGrant",
]
