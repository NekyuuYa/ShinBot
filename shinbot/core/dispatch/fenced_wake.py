"""Typed post-commit wake contracts for future fenced Actor ownership."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.core.dispatch.agent_identity import SessionKey


@dataclass(slots=True, frozen=True)
class FencedMailboxWakeRequest:
    """Identify one exact Actor ownership incarnation to wake after a commit.

    This value carries no authority by itself. A future wake target must verify
    the complete ownership and admission-fence identity in its own durable
    boundary before it creates or wakes an actor.
    """

    key: SessionKey
    ownership_generation: int
    admission_fence_id: str = ""
    admission_fence_generation: int = 0

    def __post_init__(self) -> None:
        """Normalize optional fence identity and reject ambiguous requests."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("key must be a SessionKey")
        if isinstance(self.ownership_generation, bool) or not isinstance(
            self.ownership_generation,
            int,
        ):
            raise ValueError("ownership_generation must be an integer")
        if self.ownership_generation < 1:
            raise ValueError("ownership_generation must be positive")
        fence_id = str(self.admission_fence_id or "").strip()
        fence_generation = self.admission_fence_generation
        if isinstance(fence_generation, bool) or not isinstance(fence_generation, int):
            raise ValueError("admission_fence_generation must be an integer")
        if fence_generation < 0:
            raise ValueError("admission_fence_generation must not be negative")
        if bool(fence_id) != bool(fence_generation):
            raise ValueError(
                "admission_fence_id and admission_fence_generation must be set together"
            )
        if fence_id and fence_generation < 1:
            raise ValueError("admission_fence_generation must be positive when fenced")
        object.__setattr__(self, "admission_fence_id", fence_id)

    @property
    def has_admission_fence(self) -> bool:
        """Return whether this request is bound to a committed admission fence."""

        return bool(self.admission_fence_id)


class FencedMailboxWakeDisposition(StrEnum):
    """Outcome from attempting to wake one exact ownership incarnation.

    ``DEFERRED`` is deliberately distinct from ``STALE``. A deferred target
    could not currently accept the exact work, but has not proved that the
    ownership incarnation itself is obsolete. Callers must retain or release
    the durable claim for a later explicitly published target. ``STALE`` is a
    terminal result only after the target has proved that the owner/fence is no
    longer current.
    """

    ACCEPTED = "accepted"
    STALE = "stale"
    DEFERRED = "deferred"


@dataclass(slots=True, frozen=True)
class FencedMailboxWakeReceipt:
    """Report whether a fenced wake remained current at its target boundary."""

    request: FencedMailboxWakeRequest
    disposition: FencedMailboxWakeDisposition

    def __post_init__(self) -> None:
        """Validate the target's typed outcome."""

        if not isinstance(self.request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        if not isinstance(self.disposition, FencedMailboxWakeDisposition):
            raise TypeError("disposition must be a FencedMailboxWakeDisposition")


class FencedMailboxWakePort(Protocol):
    """Future target that can verify and wake one fenced Actor incarnation."""

    def wake_fenced(
        self,
        request: FencedMailboxWakeRequest,
    ) -> Awaitable[FencedMailboxWakeReceipt] | FencedMailboxWakeReceipt:
        """Wake an exact Actor incarnation or return a typed disposition."""


__all__ = [
    "FencedMailboxWakeDisposition",
    "FencedMailboxWakePort",
    "FencedMailboxWakeReceipt",
    "FencedMailboxWakeRequest",
]
