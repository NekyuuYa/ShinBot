"""Durable controls for legacy broad mailbox recovery.

The legacy session-actor registry exposes a key-only ``recover()`` API.  It
cannot safely coexist with a committed Actor v2 admission fence unless a
durable gate serializes the two operations.  These values are deliberately
small and import-safe so persistence and core dispatch can share them without
loading an Agent runtime.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum


class LegacyRecoveryGateMode(StrEnum):
    """Lifecycle state for database-wide legacy broad recovery."""

    LEGACY_OPEN = "legacy_open"
    LEGACY_RECOVERY_ACTIVE = "legacy_recovery_active"
    FENCED_ONLY = "fenced_only"


@dataclass(slots=True, frozen=True)
class LegacyRecoveryGateSnapshot:
    """Token-free durable state of the database-wide recovery gate."""

    mode: LegacyRecoveryGateMode
    epoch: int
    holder_id: str = ""
    activated_at: float | None = None
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        """Normalize state fields and reject ambiguous gate snapshots."""

        mode = LegacyRecoveryGateMode(self.mode)
        if isinstance(self.epoch, bool) or not isinstance(self.epoch, int):
            raise ValueError("legacy recovery gate epoch must be an integer")
        if self.epoch < 0:
            raise ValueError("legacy recovery gate epoch must be non-negative")
        holder_id = str(self.holder_id or "").strip()
        updated_at = _finite_time(self.updated_at, "updated_at")
        activated_at = (
            None
            if self.activated_at is None
            else _finite_time(self.activated_at, "activated_at")
        )
        if mode is LegacyRecoveryGateMode.LEGACY_RECOVERY_ACTIVE:
            if not holder_id or activated_at is None:
                raise ValueError(
                    "active legacy recovery gate requires holder_id and activated_at"
                )
        elif holder_id or activated_at is not None:
            raise ValueError("inactive legacy recovery gate retains holder state")
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "holder_id", holder_id)
        object.__setattr__(self, "activated_at", activated_at)
        object.__setattr__(self, "updated_at", updated_at)


@dataclass(slots=True, frozen=True)
class LegacyRecoveryPermit:
    """Opaque capability for one lifecycle-owned legacy recovery period.

    The persistence repository stores only a digest of ``holder_token``.  A
    future controller must retain this permit until every actor it started for
    broad recovery has proved shutdown. Validating it only around key discovery
    or actor startup is insufficient because those actors can outlive a short
    caller scope and later observe a fenced mailbox.
    """

    epoch: int
    holder_id: str
    holder_token: str

    def __post_init__(self) -> None:
        """Require a complete, non-reusable recovery capability."""

        if isinstance(self.epoch, bool) or not isinstance(self.epoch, int):
            raise ValueError("legacy recovery permit epoch must be an integer")
        if self.epoch < 1:
            raise ValueError("legacy recovery permit epoch must be positive")
        holder_id = str(self.holder_id or "").strip()
        holder_token = str(self.holder_token or "").strip()
        if not holder_id or not holder_token:
            raise ValueError("legacy recovery permit requires holder_id and holder_token")
        object.__setattr__(self, "holder_id", holder_id)
        object.__setattr__(self, "holder_token", holder_token)


class LegacyRecoveryGateError(RuntimeError):
    """Base error for a fail-closed legacy recovery gate."""


class LegacyRecoveryGateBlocked(LegacyRecoveryGateError):
    """Raised when a legacy recovery or Actor v2 reservation crosses the gate."""


class LegacyRecoveryPermitLost(LegacyRecoveryGateError):
    """Raised when a permit no longer names the active durable gate holder."""


def _finite_time(value: object, field_name: str) -> float:
    """Normalize one finite timestamp carried by a gate snapshot."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite")
    return normalized


__all__ = [
    "LegacyRecoveryGateBlocked",
    "LegacyRecoveryGateError",
    "LegacyRecoveryGateMode",
    "LegacyRecoveryGateSnapshot",
    "LegacyRecoveryPermit",
    "LegacyRecoveryPermitLost",
]
