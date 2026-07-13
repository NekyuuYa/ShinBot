"""Unit tests for the pure typed recovery-request reducer branch."""

from __future__ import annotations

import pytest

from shinbot.agent.runtime.session_actor.aggregate import AgentSessionAggregate, SessionKey
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_SOURCE,
    RecoveryAggregateFence,
    RecoveryDecision,
    RecoveryDecisionKind,
    RecoveryDeliveryPayload,
    RecoveryGraphNode,
    RecoverySubject,
    build_recovery_certificate,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
)


def _aggregate() -> AgentSessionAggregate:
    return AgentSessionAggregate(
        key=SessionKey("profile-a", "bot:group:room"),
        ownership_generation=3,
        state="review",
        state_revision=7,
        event_sequence=11,
    )


def _delivery() -> RecoveryDeliveryPayload:
    aggregate = _aggregate()
    certificate = build_recovery_certificate(
        subject=RecoverySubject(
            profile_id=aggregate.profile_id,
            session_id=aggregate.session_id,
            ownership_generation=aggregate.ownership_generation,
        ),
        aggregate_fence=RecoveryAggregateFence(
            state=aggregate.state,
            state_revision=aggregate.state_revision,
            event_sequence=aggregate.event_sequence,
            activity_generation=aggregate.activity_generation,
            active_epoch=aggregate.active_epoch,
        ),
        nodes=(
            RecoveryGraphNode(
                identity="operation:review-1",
                kind="operation",
                authority="agent_session_operations",
                status="pending",
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
    return RecoveryDeliveryPayload(certificate=certificate, delivery_cycle=0)


def _event(
    delivery: RecoveryDeliveryPayload,
    *,
    source: str = RECOVERY_DELIVERY_EVENT_SOURCE,
    payload: dict[str, object] | None = None,
) -> SessionEventEnvelope:
    aggregate = _aggregate()
    return SessionEventEnvelope(
        event_id=delivery.event_id,
        key=aggregate.key,
        kind=AgentSessionEventKind.RECOVERY_REQUESTED,
        ownership_generation=aggregate.ownership_generation,
        source=source,
        payload=delivery.to_record() if payload is None else payload,
    )


def test_typed_recovery_requested_produces_only_a_compact_commit_intent() -> None:
    aggregate = _aggregate()
    delivery = _delivery()

    transition = AgentSessionReducer().reduce(aggregate, _event(delivery))

    assert transition.disposition == "recovery_commit_pending"
    assert transition.result == {}
    assert transition.recovery_commit_intent is not None
    assert transition.recovery_commit_intent.case_id == delivery.case_id
    assert transition.recovery_commit_intent.delivery_cycle == 0
    assert (
        transition.recovery_commit_intent.certificate_digest
        == delivery.certificate.certificate_digest
    )
    assert not hasattr(transition.recovery_commit_intent, "certificate")
    assert transition.aggregate.state == aggregate.state
    assert transition.aggregate.state_revision == aggregate.state_revision
    assert transition.aggregate.event_sequence == aggregate.event_sequence + 1
    assert not transition.effects
    assert not transition.operations


def test_legacy_recovery_requested_cannot_produce_a_commit_intent() -> None:
    aggregate = _aggregate()
    delivery = _delivery()

    transition = AgentSessionReducer().reduce(
        aggregate,
        _event(delivery, source="session_actor_recovery"),
    )

    assert transition.disposition == "ignored_legacy_recovery_event"
    assert transition.recovery_commit_intent is None
    assert transition.aggregate.event_sequence == aggregate.event_sequence + 1


def test_typed_recovery_requested_rejects_an_invalid_payload() -> None:
    delivery = _delivery()

    with pytest.raises(ValueError, match="recovery"):
        AgentSessionReducer().reduce(
            _aggregate(),
            _event(delivery, payload={}),
        )
