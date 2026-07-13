"""Unit tests for pure commit-time recovery protocol contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest

from shinbot.agent.runtime.session_actor.recovery import (
    MAX_RECOVERY_TEXT_BYTES,
    RecoveryAggregateFence,
    RecoveryDecision,
    RecoveryDecisionKind,
    RecoveryDeliveryEnvelopeIdentity,
    RecoveryDeliveryPayload,
    RecoveryGraphNode,
    RecoverySubject,
    build_recovery_certificate,
)
from shinbot.agent.runtime.session_actor.recovery_commit import (
    RecoveryCommitIntent,
    RecoveryCommitIntentMismatch,
    RecoveryMaterializationBlocked,
)


def _payload(*, delivery_cycle: int = 0) -> RecoveryDeliveryPayload:
    subject = RecoverySubject(
        profile_id="profile-a",
        session_id="bot:group:room",
        ownership_generation=3,
    )
    certificate = build_recovery_certificate(
        subject=subject,
        aggregate_fence=RecoveryAggregateFence(
            state="review",
            state_revision=7,
            event_sequence=11,
            activity_generation=5,
            active_epoch=2,
            current_plan_id="plan-4",
            review_plan_revision=4,
        ),
        nodes=(
            RecoveryGraphNode(
                identity="operation:review-1",
                kind="operation",
                authority="agent_session_operations",
                status="pending",
                facts={"operation_id": "review-1"},
            ),
        ),
        edges=(),
        invariants=(),
        decision=RecoveryDecision(
            kind=RecoveryDecisionKind.RECOVER_ORPHANED_WORK,
            reason_codes=("orphaned_work_without_live_completion",),
            target_node_identities=("operation:review-1",),
        ),
    )
    return RecoveryDeliveryPayload(
        certificate=certificate,
        delivery_cycle=delivery_cycle,
    )


def _envelope(payload: RecoveryDeliveryPayload) -> RecoveryDeliveryEnvelopeIdentity:
    subject = payload.certificate.subject
    return RecoveryDeliveryEnvelopeIdentity(
        event_id=payload.event_id,
        profile_id=subject.profile_id,
        session_id=subject.session_id,
        ownership_generation=subject.ownership_generation,
    )


def test_intent_keeps_only_compact_delivery_expectation() -> None:
    payload = _payload()
    intent = RecoveryCommitIntent.from_delivery(
        envelope=_envelope(payload),
        payload=payload,
    )

    assert intent.case_id == payload.case_id
    assert intent.delivery_cycle == payload.delivery_cycle
    assert intent.certificate_digest == payload.certificate.certificate_digest
    assert not hasattr(intent, "certificate")
    intent.validate_delivery(payload)


def test_intent_rejects_delivery_cycle_drift() -> None:
    original = _payload(delivery_cycle=0)
    intent = RecoveryCommitIntent.from_delivery(
        envelope=_envelope(original),
        payload=original,
    )

    with pytest.raises(RecoveryCommitIntentMismatch) as raised:
        intent.validate_delivery(_payload(delivery_cycle=1))
    assert raised.value.code == "recovery_delivery_cycle_changed"


def test_intent_rejects_noncanonical_digest_and_deep_freezes_block_details() -> None:
    payload = _payload()
    with pytest.raises(ValueError, match="SHA-256"):
        RecoveryCommitIntent(
            envelope=_envelope(payload),
            case_id=payload.case_id,
            delivery_cycle=0,
            certificate_digest="A" * 64,
        )

    source = {"state": ["review"]}
    blocked = RecoveryMaterializationBlocked(
        code="review_materializer_missing",
        details=source,
    )
    source["state"].append("active_chat")

    assert blocked.details == {"state": ("review",)}
    with pytest.raises(TypeError):
        cast(dict[str, object], blocked.details)["state"] = "active_chat"
    assert blocked.to_record() == {
        "code": "review_materializer_missing",
        "details": {"state": ["review"]},
    }
    with pytest.raises(TypeError, match="must be a mapping"):
        RecoveryMaterializationBlocked(
            code="invalid_details",
            details=cast(Mapping[str, object], [("not", "a mapping")]),
        )
    with pytest.raises(ValueError, match="maximum recovery text byte size"):
        RecoveryMaterializationBlocked(code="x" * (MAX_RECOVERY_TEXT_BYTES + 1))


def test_intent_rejects_a_directly_constructed_inconsistent_envelope() -> None:
    payload = _payload()
    with pytest.raises(ValueError, match="event_id does not match"):
        RecoveryCommitIntent(
            envelope=RecoveryDeliveryEnvelopeIdentity(
                event_id="different-recovery-event",
                profile_id=payload.certificate.subject.profile_id,
                session_id=payload.certificate.subject.session_id,
                ownership_generation=payload.certificate.subject.ownership_generation,
            ),
            case_id=payload.case_id,
            delivery_cycle=payload.delivery_cycle,
            certificate_digest=payload.certificate.certificate_digest,
        )
