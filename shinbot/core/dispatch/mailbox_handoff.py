"""Dormant typed contracts for immutable Actor v2 mailbox handoffs.

The mailbox row is not enough to authorize a post-commit Actor wake.  These
contracts keep the mailbox event identity and the admission-fence evidence
together until a future target performs its own exact wake validation.  They
do not install a target, publish a registry entry, or wake an Actor.
"""

from __future__ import annotations

import math
from collections.abc import Awaitable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
    FencedMailboxWakeRequest,
)


class MailboxHandoffEvidenceState(StrEnum):
    """Classify the immutable wake evidence attached to one mailbox event."""

    FENCED = "fenced"
    UNFENCED_LEGACY = "unfenced_legacy"
    UNKNOWN = "unknown"


class MailboxHandoffState(StrEnum):
    """Track mutable delivery state without changing immutable evidence."""

    PENDING = "pending"
    CLAIMED = "claimed"
    SETTLED = "settled"
    BLOCKED = "blocked"


class MailboxHandoffNotifier(Protocol):
    """Advisory post-commit hint sink for one exact fenced mailbox row.

    A notifier is deliberately not a handoff target. Producers may use it to
    reduce pull-delivery latency after their transaction commits, but a future
    dispatcher must still read, claim, and validate the durable sidecar before
    it presents work to an Actor.
    """

    def notify(self, mailbox_id: int) -> Awaitable[None] | None:
        """Hint that a durable fenced handoff is available for later delivery."""


@dataclass(slots=True, frozen=True)
class MailboxHandoffIdentity:
    """Immutable mailbox identity copied into a durable handoff sidecar."""

    mailbox_id: int
    event_id: str
    key: SessionKey
    ownership_generation: int

    def __post_init__(self) -> None:
        """Reject an identity that cannot name one exact mailbox event."""

        if isinstance(self.mailbox_id, bool) or not isinstance(self.mailbox_id, int):
            raise ValueError("mailbox_id must be an integer")
        if self.mailbox_id < 1:
            raise ValueError("mailbox_id must be positive")
        event_id = str(self.event_id or "").strip()
        if not event_id:
            raise ValueError("event_id must not be empty")
        if not isinstance(self.key, SessionKey):
            raise TypeError("key must be a SessionKey")
        if isinstance(self.ownership_generation, bool) or not isinstance(
            self.ownership_generation,
            int,
        ):
            raise ValueError("ownership_generation must be an integer")
        if self.ownership_generation < 0:
            raise ValueError("ownership_generation must not be negative")
        object.__setattr__(self, "event_id", event_id)


@dataclass(slots=True, frozen=True)
class MailboxHandoffEvidence:
    """Immutable evidence that may, and only may, project a fenced wake request."""

    identity: MailboxHandoffIdentity
    state: MailboxHandoffEvidenceState
    admission_fence_id: str = ""
    admission_fence_generation: int = 0

    def __post_init__(self) -> None:
        """Require fenced and non-fenced evidence to use disjoint encodings."""

        if not isinstance(self.identity, MailboxHandoffIdentity):
            raise TypeError("identity must be a MailboxHandoffIdentity")
        state = MailboxHandoffEvidenceState(self.state)
        fence_id = str(self.admission_fence_id or "").strip()
        fence_generation = self.admission_fence_generation
        if isinstance(fence_generation, bool) or not isinstance(fence_generation, int):
            raise ValueError("admission_fence_generation must be an integer")
        if fence_generation < 0:
            raise ValueError("admission_fence_generation must not be negative")
        if state is MailboxHandoffEvidenceState.FENCED:
            if not fence_id or fence_generation < 1:
                raise ValueError("fenced handoff evidence requires a complete admission fence")
            if self.identity.ownership_generation < 1:
                raise ValueError("fenced handoff evidence requires positive ownership generation")
        elif fence_id or fence_generation:
            raise ValueError("legacy and unknown handoff evidence must not carry a fence")
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "admission_fence_id", fence_id)

    @property
    def is_fenced(self) -> bool:
        """Return whether this is canonical evidence for a typed wake request."""

        return self.state is MailboxHandoffEvidenceState.FENCED

    def as_fenced_wake_request(self) -> FencedMailboxWakeRequest:
        """Project exact fenced evidence without consulting current ownership.

        Unknown and legacy rows deliberately cannot be promoted by looking at a
        current owner.  A future target must validate this request at its own
        durable boundary before any Actor wake is attempted.
        """

        if not self.is_fenced:
            raise ValueError("non-fenced mailbox handoff evidence cannot create a wake request")
        return FencedMailboxWakeRequest(
            key=self.identity.key,
            ownership_generation=self.identity.ownership_generation,
            admission_fence_id=self.admission_fence_id,
            admission_fence_generation=self.admission_fence_generation,
        )


@dataclass(slots=True, frozen=True)
class MailboxHandoffTarget:
    """Identify one mutable wake-consumer incarnation that holds a handoff lease."""

    target_id: str
    incarnation_id: str

    def __post_init__(self) -> None:
        """Normalize both target identifiers before they enter durable state."""

        target_id = str(self.target_id or "").strip()
        incarnation_id = str(self.incarnation_id or "").strip()
        if not target_id:
            raise ValueError("target_id must not be empty")
        if not incarnation_id:
            raise ValueError("incarnation_id must not be empty")
        object.__setattr__(self, "target_id", target_id)
        object.__setattr__(self, "incarnation_id", incarnation_id)


@dataclass(slots=True, frozen=True)
class FencedMailboxHandoffClaim:
    """Lease-bound handoff work that is safe to present to a future wake target."""

    handoff_id: str
    evidence: MailboxHandoffEvidence
    claim_id: str
    worker_id: str
    target: MailboxHandoffTarget
    attempt_count: int
    claimed_at: float
    lease_expires_at: float

    def __post_init__(self) -> None:
        """Reject a claim that could outlive or change its exact evidence."""

        handoff_id = str(self.handoff_id or "").strip()
        claim_id = str(self.claim_id or "").strip()
        worker_id = str(self.worker_id or "").strip()
        if not handoff_id or not claim_id or not worker_id:
            raise ValueError("handoff_id, claim_id, and worker_id must not be empty")
        if not isinstance(self.evidence, MailboxHandoffEvidence) or not self.evidence.is_fenced:
            raise ValueError("a fenced handoff claim requires fenced evidence")
        if not isinstance(self.target, MailboxHandoffTarget):
            raise TypeError("target must be a MailboxHandoffTarget")
        if isinstance(self.attempt_count, bool) or not isinstance(self.attempt_count, int):
            raise ValueError("attempt_count must be an integer")
        if self.attempt_count < 1:
            raise ValueError("attempt_count must be positive")
        claimed_at = _finite_timestamp(self.claimed_at, "claimed_at")
        lease_expires_at = _finite_timestamp(self.lease_expires_at, "lease_expires_at")
        if lease_expires_at <= claimed_at:
            raise ValueError("lease_expires_at must be after claimed_at")
        object.__setattr__(self, "handoff_id", handoff_id)
        object.__setattr__(self, "claim_id", claim_id)
        object.__setattr__(self, "worker_id", worker_id)
        object.__setattr__(self, "claimed_at", claimed_at)
        object.__setattr__(self, "lease_expires_at", lease_expires_at)

    @property
    def request(self) -> FencedMailboxWakeRequest:
        """Return the ownership subrequest that the future target must validate.

        This request is not the handoff boundary: it intentionally lacks the
        mailbox event, handoff, and target identities retained by this claim.
        Future dispatchers must send the complete claim through
        :class:`FencedMailboxHandoffPort`.
        """

        return self.evidence.as_fenced_wake_request()

    @property
    def identity(self) -> MailboxHandoffIdentity:
        """Return the exact mailbox event identity retained by this claim."""

        return self.evidence.identity


@dataclass(slots=True, frozen=True)
class FencedMailboxHandoffReceipt:
    """Bind a target's typed wake receipt to the specific leased handoff.

    A target may return ``DEFERRED`` only before it accepts the mailbox work or
    emits a side effect derived from it. The dispatcher is then allowed to
    release the exact claim for a later target incarnation.
    """

    claim: FencedMailboxHandoffClaim
    wake_receipt: FencedMailboxWakeReceipt

    def __post_init__(self) -> None:
        """Reject a receipt from another mailbox, fence, or target claim."""

        if not isinstance(self.claim, FencedMailboxHandoffClaim):
            raise TypeError("claim must be a FencedMailboxHandoffClaim")
        if not isinstance(self.wake_receipt, FencedMailboxWakeReceipt):
            raise TypeError("wake_receipt must be a FencedMailboxWakeReceipt")
        if self.wake_receipt.request != self.claim.request:
            raise ValueError("wake receipt request does not match its handoff claim")

    @property
    def disposition(self) -> FencedMailboxWakeDisposition:
        """Return the future target's typed delivery result.

        ``DEFERRED`` is non-terminal: the dispatcher must preserve the exact
        durable handoff for a later target instead of settling it.
        """

        return self.wake_receipt.disposition


class FencedMailboxHandoffPort(Protocol):
    """Future target boundary for one mailbox event and consumer incarnation.

    Unlike ``FencedMailboxWakePort``, this protocol never collapses work to a
    session-level request.  Implementations must validate the claim's mailbox,
    handoff, ownership, admission fence, and target incarnation at their own
    durable boundary before returning the same claim in a typed receipt.
    """

    def wake_handoff(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> Awaitable[FencedMailboxHandoffReceipt] | FencedMailboxHandoffReceipt:
        """Wake one exact mailbox handoff or return a typed delivery result."""


def _finite_timestamp(value: object, field_name: str) -> float:
    """Validate one finite timestamp carried across a durable handoff boundary."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field_name} must be finite")
    return numeric


__all__ = [
    "FencedMailboxHandoffClaim",
    "FencedMailboxHandoffPort",
    "FencedMailboxHandoffReceipt",
    "MailboxHandoffEvidence",
    "MailboxHandoffEvidenceState",
    "MailboxHandoffIdentity",
    "MailboxHandoffNotifier",
    "MailboxHandoffState",
    "MailboxHandoffTarget",
]
