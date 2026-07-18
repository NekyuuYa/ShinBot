"""Unit coverage for immutable mailbox handoff contracts."""

from __future__ import annotations

import pytest

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
)
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffClaim,
    FencedMailboxHandoffReceipt,
    MailboxHandoffEvidence,
    MailboxHandoffEvidenceState,
    MailboxHandoffIdentity,
    MailboxHandoffTarget,
)


def _identity() -> MailboxHandoffIdentity:
    """Build one exact mailbox identity for typed-contract tests."""

    return MailboxHandoffIdentity(
        mailbox_id=7,
        event_id="event-a",
        key=SessionKey("profile-a", "profile-a:group:room"),
        ownership_generation=3,
    )


def test_only_fenced_evidence_can_project_a_wake_request() -> None:
    """Unknown historical evidence cannot become fenced through a type conversion."""

    unknown = MailboxHandoffEvidence(
        identity=_identity(),
        state=MailboxHandoffEvidenceState.UNKNOWN,
    )
    fenced = MailboxHandoffEvidence(
        identity=_identity(),
        state=MailboxHandoffEvidenceState.FENCED,
        admission_fence_id="fence-a",
        admission_fence_generation=2,
    )

    with pytest.raises(ValueError, match="cannot create a wake request"):
        unknown.as_fenced_wake_request()
    assert fenced.as_fenced_wake_request().admission_fence_id == "fence-a"


def test_legacy_and_unknown_evidence_reject_fence_fields() -> None:
    """A non-fenced state has one unambiguous blank-fence storage encoding."""

    with pytest.raises(ValueError, match="must not carry a fence"):
        MailboxHandoffEvidence(
            identity=_identity(),
            state=MailboxHandoffEvidenceState.UNFENCED_LEGACY,
            admission_fence_id="fence-a",
            admission_fence_generation=2,
        )


def test_target_receipt_cannot_be_bound_to_another_fenced_request() -> None:
    """Settlement receipts retain the lease's exact ownership and fence request."""

    evidence = MailboxHandoffEvidence(
        identity=_identity(),
        state=MailboxHandoffEvidenceState.FENCED,
        admission_fence_id="fence-a",
        admission_fence_generation=2,
    )
    claim = FencedMailboxHandoffClaim(
        handoff_id="handoff-a",
        evidence=evidence,
        claim_id="claim-a",
        worker_id="worker-a",
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
        attempt_count=1,
        claimed_at=10.0,
        lease_expires_at=20.0,
    )
    assert claim.identity.mailbox_id == 7
    assert claim.identity.event_id == "event-a"
    assert claim.target == MailboxHandoffTarget("target-a", "incarnation-a")
    wrong_request = evidence.as_fenced_wake_request()
    wrong_request = type(wrong_request)(
        key=wrong_request.key,
        ownership_generation=wrong_request.ownership_generation,
        admission_fence_id="fence-b",
        admission_fence_generation=2,
    )

    with pytest.raises(ValueError, match="does not match"):
        FencedMailboxHandoffReceipt(
            claim=claim,
            wake_receipt=FencedMailboxWakeReceipt(
                request=wrong_request,
                disposition=FencedMailboxWakeDisposition.STALE,
            ),
        )
