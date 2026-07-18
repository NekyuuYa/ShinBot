"""Unit coverage for no-replay session-actor recovery materializers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RecoveryAggregateFence,
    RecoveryCertificate,
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
    RecoveryMaterializationBlocked,
    RecoveryMaterializer,
)
from shinbot.agent.runtime.session_actor.recovery_materializers import (
    ActiveChatRecoveryMaterializer,
    ActiveChatSettlingRecoveryMaterializer,
    ActiveReplyRecoveryMaterializer,
    ReviewRecoveryMaterializer,
)
from shinbot.agent.runtime.session_actor.transition_validation import (
    validate_session_transition,
)


@dataclass(frozen=True, slots=True)
class _RecoveryCase:
    """State-specific aggregate and materializer inputs for one unit test."""

    aggregate: AgentSessionAggregate
    operation_id: str
    materializer: RecoveryMaterializer


def _operation_fence(
    *,
    operation_id: str,
    operation_kind: str,
    plan_id: str,
) -> dict[str, object]:
    """Build the complete immutable fence required by no-replay recovery."""

    return {
        "operation_id": operation_id,
        "operation_kind": operation_kind,
        "ownership_generation": 1,
        "plan_id": plan_id,
        "active_epoch": 2,
        "activity_generation": 3,
        "input_watermark": 41,
        "input_ledger_sequence": 7,
    }


def _case(state: str) -> _RecoveryCase:
    """Return one safely materializable aggregate for each non-idle state."""

    key = SessionKey("profile-a", "bot:group:room")
    operation_id = f"{state}-operation"
    plan_id = "current-plan"
    data: dict[str, Any] = {
        "message_watermark": 99,
        "pending_high_priority_message_log_ids": [31],
    }
    fields: dict[str, Any] = {
        "review_operation_id": "",
        "active_reply_operation_id": "",
        "active_chat_round_operation_id": "",
        "idle_planning_operation_id": "",
        "active_chat_state": {},
    }
    materializer: RecoveryMaterializer
    operation_kind: str
    if state == "review":
        fields["review_operation_id"] = operation_id
        operation_kind = "review"
        materializer = ReviewRecoveryMaterializer(delay_seconds=12.0)
    elif state == "active_reply":
        fields["active_reply_operation_id"] = operation_id
        operation_kind = "active_reply"
        materializer = ActiveReplyRecoveryMaterializer(delay_seconds=12.0)
    elif state == "active_chat":
        fields["active_chat_state"] = {
            "bootstrap_operation_id": operation_id,
            "bootstrap_status": "pending",
            "pending_message_log_ids": [31],
        }
        operation_kind = "active_chat_bootstrap"
        materializer = ActiveChatRecoveryMaterializer(delay_seconds=12.0)
    elif state == "active_chat_settling":
        fields["idle_planning_operation_id"] = operation_id
        operation_kind = "idle_review_planning"
        successor_plan_id = "settling-successor-plan"
        data["idle_exit"] = {
            "operation_id": operation_id,
            "plan_id": successor_plan_id,
            "ownership_generation": 1,
            "active_epoch": 2,
            "activity_generation": 3,
        }
        materializer = ActiveChatSettlingRecoveryMaterializer(delay_seconds=12.0)
    else:
        raise AssertionError(f"unknown state: {state}")
    data["operation_fences"] = {
        operation_id: _operation_fence(
            operation_id=operation_id,
            operation_kind=operation_kind,
            plan_id=(
                "settling-successor-plan"
                if state == "active_chat_settling"
                else plan_id
            ),
        )
    }
    aggregate = AgentSessionAggregate(
        key=key,
        ownership_generation=1,
        state=state,
        state_revision=4,
        event_sequence=9,
        activity_generation=3,
        active_epoch=2,
        current_plan_id=plan_id,
        review_plan_revision=3,
        review_plan={"plan_id": plan_id, "plan_revision": 3},
        data=data,
        **fields,
    )
    return _RecoveryCase(
        aggregate=aggregate,
        operation_id=operation_id,
        materializer=materializer,
    )


def _intent(aggregate: AgentSessionAggregate, *, operation_id: str) -> RecoveryCommitIntent:
    """Build one scanner-equivalent compact intent for an aggregate fixture."""

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
            current_plan_id=aggregate.current_plan_id,
            review_plan_revision=aggregate.review_plan_revision,
        ),
        nodes=(
            RecoveryGraphNode(
                identity=f"operation:{operation_id}",
                kind="operation",
                authority="agent_session_operations",
                status="pending",
                facts={"operation_id": operation_id},
            ),
        ),
        edges=(),
        invariants=(),
        decision=RecoveryDecision(
            kind=RecoveryDecisionKind.RECOVER_ORPHANED_WORK,
            reason_codes=("orphaned_work_without_live_completion",),
            target_node_identities=(f"operation:{operation_id}",),
        ),
    )
    payload = RecoveryDeliveryPayload(certificate=certificate, delivery_cycle=0)
    envelope = RecoveryDeliveryEnvelopeIdentity(
        event_id=payload.event_id,
        profile_id=aggregate.profile_id,
        session_id=aggregate.session_id,
        ownership_generation=aggregate.ownership_generation,
    )
    return RecoveryCommitIntent.from_delivery(envelope=envelope, payload=payload)


@pytest.mark.parametrize(
    "state",
    ("review", "active_reply", "active_chat", "active_chat_settling"),
)
def test_materializer_settles_each_non_idle_state_without_replay(state: str) -> None:
    """Every supported orphan shape becomes idle with a fresh defaulted plan."""

    case = _case(state)
    intent = _intent(case.aggregate, operation_id=case.operation_id)
    materializer = case.materializer
    transition = materializer.materialize(
        aggregate=case.aggregate,
        intent=intent,
        certificate=_certificate_from_intent(case.aggregate, case.operation_id),
    )

    assert not isinstance(transition, RecoveryMaterializationBlocked)
    assert transition.aggregate.state == "idle"
    assert transition.aggregate.event_sequence == case.aggregate.event_sequence + 1
    assert transition.aggregate.state_revision == case.aggregate.state_revision + 1
    assert transition.aggregate.current_plan_id != case.aggregate.current_plan_id
    assert transition.aggregate.review_plan_revision == case.aggregate.review_plan_revision + 1
    assert transition.aggregate.review_operation_id == ""
    assert transition.aggregate.active_reply_operation_id == ""
    assert transition.aggregate.active_chat_round_operation_id == ""
    assert transition.aggregate.idle_planning_operation_id == ""
    assert transition.aggregate.active_chat_state == {}
    assert transition.aggregate.data["message_watermark"] == 99
    assert transition.aggregate.data["pending_high_priority_message_log_ids"] == [31]
    assert "operation_fences" not in transition.aggregate.data
    assert "idle_exit" not in transition.aggregate.data
    assert not transition.effects
    assert not transition.message_ledger_mutations
    assert len(transition.operations) == 1
    assert transition.operations[0].operation_id == case.operation_id
    assert transition.operations[0].status.value == "failed"
    assert transition.operations[0].finished_at is None
    assert len(transition.review_schedules) == 1
    assert transition.review_schedules[0].applied_delay_seconds == 12.0
    assert len(transition.review_schedule_events) == 1
    validate_session_transition(
        case.aggregate,
        transition,
        effect_contract_authority=builtin_effect_contract_authority(),
    )


def test_materializer_rejects_incomplete_operation_fence() -> None:
    """Old aggregate shapes cannot gain recovery authority through a fake fence."""

    case = _case("review")
    data = dict(case.aggregate.data)
    data["operation_fences"] = {
        case.operation_id: {
            "operation_id": case.operation_id,
            "ownership_generation": 1,
        }
    }
    aggregate = case.aggregate.advance(data=data)
    intent = _intent(aggregate, operation_id=case.operation_id)
    result = ReviewRecoveryMaterializer().materialize(
        aggregate=aggregate,
        intent=intent,
        certificate=_certificate_from_intent(aggregate, case.operation_id),
    )

    assert isinstance(result, RecoveryMaterializationBlocked)
    assert result.code == "recovery_operation_fence_incomplete"


def test_active_chat_materializer_rejects_multiple_orphaned_operations() -> None:
    """A bootstrap/round overlap cannot be guessed into one recovery action."""

    case = _case("active_chat")
    data = dict(case.aggregate.data)
    round_operation_id = "active-chat-round-operation"
    data["operation_fences"] = {
        case.operation_id: data["operation_fences"][case.operation_id],
        round_operation_id: _operation_fence(
            operation_id=round_operation_id,
            operation_kind="active_chat_round",
            plan_id="current-plan",
        ),
    }
    aggregate = case.aggregate.advance(
        active_chat_round_operation_id=round_operation_id,
        data=data,
    )
    intent = _intent(aggregate, operation_id=case.operation_id)
    result = ActiveChatRecoveryMaterializer().materialize(
        aggregate=aggregate,
        intent=intent,
        certificate=_certificate_from_intent(aggregate, case.operation_id),
    )

    assert isinstance(result, RecoveryMaterializationBlocked)
    assert result.code == "recovery_active_chat_multiple_operations"


def test_settling_materializer_rejects_unfinished_exit_control() -> None:
    """Only a completed local exit control can be cleared by recovery."""

    case = _case("active_chat_settling")
    data = dict(case.aggregate.data)
    data["effect_control_intents"] = {
        "enqueue_active_chat_exit_request": {
            "status": "requested",
            "effect_kind": "enqueue_active_chat_exit_request",
            "operation_id": "",
            "plan_id": "current-plan",
            "ownership_generation": 1,
            "active_epoch": 2,
            "activity_generation": 3,
        }
    }
    aggregate = case.aggregate.advance(data=data)
    intent = _intent(aggregate, operation_id=case.operation_id)
    result = ActiveChatSettlingRecoveryMaterializer().materialize(
        aggregate=aggregate,
        intent=intent,
        certificate=_certificate_from_intent(aggregate, case.operation_id),
    )

    assert isinstance(result, RecoveryMaterializationBlocked)
    assert result.code == "recovery_settling_exit_control_fence_changed"


def _certificate_from_intent(
    aggregate: AgentSessionAggregate,
    operation_id: str,
) -> RecoveryCertificate:
    """Return the full certificate matching :func:`_intent` for materialization."""

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
            current_plan_id=aggregate.current_plan_id,
            review_plan_revision=aggregate.review_plan_revision,
        ),
        nodes=(
            RecoveryGraphNode(
                identity=f"operation:{operation_id}",
                kind="operation",
                authority="agent_session_operations",
                status="pending",
                facts={"operation_id": operation_id},
            ),
        ),
        edges=(),
        invariants=(),
        decision=RecoveryDecision(
            kind=RecoveryDecisionKind.RECOVER_ORPHANED_WORK,
            reason_codes=("orphaned_work_without_live_completion",),
            target_node_identities=(f"operation:{operation_id}",),
        ),
    )
    return certificate
