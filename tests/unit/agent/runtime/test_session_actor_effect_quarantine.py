"""Reducer regressions for store-owned terminal effect quarantine events."""

from __future__ import annotations

from dataclasses import replace

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectEnvelope,
    quarantined_event_id,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionOperationStatus,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


def _active_chat() -> AgentSessionAggregate:
    """Return a completed Actor v3 Active Chat session with no pending input."""

    return AgentSessionAggregate(
        key=_KEY,
        ownership_generation=1,
        state=AgentSessionState.ACTIVE_CHAT,
        state_revision=4,
        event_sequence=8,
        active_epoch=3,
        activity_generation=7,
        current_plan_id="review-plan-a",
        review_plan_revision=1,
        review_plan={"plan_id": "review-plan-a", "plan_revision": 1},
        active_chat_state={
            "active_epoch": 3,
            "actor_workflow_contract_version": 3,
            "interest_value": 20.0,
            "decay_half_life_seconds": 20.0,
            "entered_at": 10.0,
            "updated_at": 10.0,
            "tick_count": 0,
            "pending_message_log_ids": [],
            "bootstrap_status": "completed",
            "bootstrap_operation_id": "",
            "round_schedule_revision": 0,
            "round_schedule_id": "",
            "round_due_at": None,
        },
        data={
            "message_watermark": 20,
            "delivery_context": {
                "instance_id": "instance-a",
                "target_session_id": "instance-a:group:room-a",
            },
        },
        updated_at=10.0,
    )


def _message_event(*, event_id: str, message_log_id: int) -> SessionEventEnvelope:
    """Build one trusted message ingress event for the actor reducer."""

    return SessionEventEnvelope(
        event_id=event_id,
        key=_KEY,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=1,
        source="agent_route_relay",
        occurred_at=float(message_log_id),
        created_at=float(message_log_id),
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": _KEY.profile_id,
                "session_id": _KEY.session_id,
            },
            "bot_id": "bot-a",
            "bot_binding_id": "binding-a",
            "base_session_id": "instance-a:group:room-a",
            "bot_session_id": _KEY.session_id,
            "message_log_id": message_log_id,
            "sender_id": "user-a",
            "instance_id": "instance-a",
            "platform": "test",
            "self_id": "bot-a",
            "is_private": False,
            "is_mentioned": False,
            "is_mention_to_other": False,
            "is_reply_to_bot": False,
            "is_poke_to_bot": False,
            "is_poke_to_other": False,
            "already_handled": False,
            "is_stopped": False,
            "trace_id": f"trace:{event_id}",
            "observed_at": float(message_log_id),
            "event_type": "message-created",
        },
        trace_id=f"trace:{event_id}",
    )


def _round_due_event(aggregate: AgentSessionAggregate) -> SessionEventEnvelope:
    """Build the authenticated control completion that starts a round."""

    state = aggregate.active_chat_state
    intent = aggregate.data["effect_control_intents"][
        "enqueue_active_chat_round_due"
    ]
    contract = builtin_effect_contract("enqueue_active_chat_round_due")
    return SessionEventEnvelope(
        event_id=str(state["round_due_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.ACTIVE_CHAT_ROUND_DUE,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=50.0,
        causation_id=str(state["round_schedule_source_event_id"]),
        payload={
            "effect_id": state["round_schedule_effect_id"],
            "effect_kind": contract.effect_kind,
            "idempotency_key": state["round_schedule_effect_id"],
            "operation_id": "",
            "plan_id": intent["plan_id"],
            "schedule_id": state["round_schedule_id"],
            "schedule_revision": state["round_schedule_revision"],
            "active_epoch": 3,
            "activity_generation": intent["activity_generation"],
            "input_watermark": state["round_schedule_input_watermark"],
            "input_ledger_sequence": None,
            "attempt_count": 1,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
        },
    )


def _stamp_operation_sequence(
    aggregate: AgentSessionAggregate,
    operation_id: str,
    sequence: int,
) -> AgentSessionAggregate:
    """Model the ledger sequence persisted with a freshly started operation."""

    data = dict(aggregate.data)
    fences = dict(data["operation_fences"])
    fence = dict(fences[operation_id])
    fence["input_ledger_sequence"] = sequence
    fences[operation_id] = fence
    data["operation_fences"] = fences
    return replace(aggregate, data=data)


def _running_v3_round(
    *,
    config: IdleExitReducerConfig,
) -> tuple[AgentSessionReducer, AgentSessionAggregate]:
    """Start a genuine Actor v3 round through message and timer reduction."""

    reducer = AgentSessionReducer(config=config)
    buffered = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    running = reducer.reduce(
        buffered.aggregate,
        _round_due_event(buffered.aggregate),
    ).aggregate
    operation_id = running.active_chat_round_operation_id
    fence = running.data["operation_fences"][operation_id]
    assert fence["effect_kind"] == "run_active_chat_round"
    assert fence["contract_version"] == 3
    return reducer, _stamp_operation_sequence(running, operation_id, sequence=1)


def _started_review() -> AgentSessionAggregate:
    """Return a real review operation which can directly enter Active Chat."""

    operation_id = "review-operation-a"
    effect_id = "review-effect-a"
    contract = builtin_effect_contract("run_review_workflow")
    return AgentSessionAggregate(
        key=_KEY,
        ownership_generation=1,
        state=AgentSessionState.REVIEW,
        state_revision=4,
        event_sequence=8,
        current_plan_id="review-plan-a",
        review_plan_revision=1,
        review_plan={
            "plan_id": "review-plan-a",
            "plan_revision": 1,
            "kind": "planned",
            "applied_delay_seconds": 60.0,
            "reason": "scheduled review",
            "source": "unit-test",
        },
        review_operation_id=operation_id,
        data={
            "message_watermark": 20,
            "delivery_context": {
                "instance_id": "instance-a",
                "target_session_id": "instance-a:group:room-a",
            },
            "operation_fences": {
                operation_id: {
                    "operation_id": operation_id,
                    "operation_kind": "review",
                    "source_event_id": "review-due-a",
                    "effect_id": effect_id,
                    "effect_kind": "run_review_workflow",
                    "idempotency_key": effect_id,
                    "completion_event_id": "review-completion-a",
                    "failure_event_id": "review-failure-a",
                    "ownership_generation": 1,
                    "plan_id": "review-plan-a",
                    "plan_revision": 1,
                    "active_epoch": 0,
                    "activity_generation": 0,
                    "input_watermark": 20,
                    "input_ledger_sequence": 2,
                    "contract_version": contract.version,
                    "contract_signature": contract.signature,
                    "instance_id": "instance-a",
                    "target_session_id": "instance-a:group:room-a",
                }
            },
        },
        updated_at=100.0,
    )


def _review_completion(aggregate: AgentSessionAggregate) -> SessionEventEnvelope:
    """Complete a review into native Active Chat without outbound actions."""

    operation_id = aggregate.review_operation_id
    fence = aggregate.data["operation_fences"][operation_id]
    contract = builtin_effect_contract(
        "run_review_workflow",
        version=int(fence["contract_version"]),
    )
    return SessionEventEnvelope(
        event_id=str(fence["completion_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.REVIEW_COMPLETED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=200.0,
        causation_id=str(fence["source_event_id"]),
        trace_id="trace:review-completion",
        payload={
            "effect_id": fence["effect_id"],
            "effect_kind": contract.effect_kind,
            "idempotency_key": fence["idempotency_key"],
            "operation_id": operation_id,
            "plan_id": fence["plan_id"],
            "active_epoch": fence["active_epoch"],
            "activity_generation": fence["activity_generation"],
            "input_watermark": fence["input_watermark"],
            "input_ledger_sequence": fence["input_ledger_sequence"],
            "attempt_count": 1,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
            "workflow_result": {
                "schema_version": 1,
                "completion_type": "review",
                "consumed_message_log_ids": [10, 20],
                "external_actions": {"schema_version": 1, "intents": []},
                "enter_active_chat": True,
                "next_review_outcome": None,
            },
        },
    )


def _pending_v3_bootstrap() -> tuple[AgentSessionReducer, AgentSessionAggregate]:
    """Enter Active Chat through review completion and retain one new message."""

    reducer = AgentSessionReducer()
    review = _started_review()
    entered = reducer.reduce(review, _review_completion(review))
    bootstrap = next(
        effect
        for effect in entered.effects
        if effect.kind == "run_active_chat_bootstrap"
    )
    assert bootstrap.contract_version == 3
    buffered = reducer.reduce(
        entered.aggregate,
        _message_event(event_id="message:21", message_log_id=21),
    )
    assert buffered.aggregate.active_chat_state["bootstrap_status"] == "pending"
    assert buffered.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    return reducer, buffered.aggregate


def _effect_quarantined(
    aggregate: AgentSessionAggregate,
    *,
    operation_id: str,
    source: str = "effect_store",
    effect_id: str | None = None,
) -> SessionEventEnvelope:
    """Build the exact store-owned terminal quarantine shape for one effect."""

    fence = aggregate.data["operation_fences"][operation_id]
    canonical_effect_id = effect_id or str(fence["effect_id"])
    effect = DurableEffectEnvelope(
        effect_id=canonical_effect_id,
        key=_KEY,
        kind=str(fence["effect_kind"]),
        idempotency_key=str(fence["idempotency_key"]),
        ownership_generation=1,
        contract_version=int(fence["contract_version"]),
        contract_signature=str(fence["contract_signature"]),
        payload=dict(fence),
        source_event_id=str(fence["source_event_id"]),
        operation_id=operation_id,
        trace_id="trace:quarantine",
    )
    event_id = quarantined_event_id(effect)
    return SessionEventEnvelope(
        event_id=event_id,
        key=_KEY,
        kind="EffectQuarantined",
        ownership_generation=1,
        source=source,
        occurred_at=75.0,
        causation_id=effect.source_event_id,
        correlation_id=operation_id,
        trace_id=effect.trace_id,
        payload={
            "attempt_count": 3,
            "contract_signature": effect.contract_signature,
            "contract_version": effect.contract_version,
            "effect_id": canonical_effect_id,
            "effect_kind": effect.kind,
            "failure_code": "unsupported_contract",
            "failure_message": "no handler accepted the persisted contract",
            "idempotency_key": effect.idempotency_key,
            "operation_id": operation_id,
            "reason_code": "unsupported_contract",
            "reason_message": "no handler accepted the persisted contract",
        },
    )


@pytest.mark.parametrize(
    ("max_attempts", "expected_disposition", "expected_effect_kind"),
    (
        (
            2,
            "active_chat_round_effect_failed_retry_scheduled",
            "enqueue_active_chat_round_due",
        ),
        (
            1,
            "active_chat_round_retries_exhausted_exit_requested",
            "enqueue_active_chat_exit_request",
        ),
    ),
)
def test_effect_store_quarantine_uses_v3_round_terminal_failure_policy(
    max_attempts: int,
    expected_disposition: str,
    expected_effect_kind: str,
) -> None:
    """A quarantined v3 round must release its slot through bounded recovery."""

    reducer, running = _running_v3_round(
        config=IdleExitReducerConfig(active_chat_round_max_attempts=max_attempts)
    )
    operation_id = running.active_chat_round_operation_id
    quarantined = _effect_quarantined(running, operation_id=operation_id)

    transition = reducer.reduce(running, quarantined)

    assert transition.disposition == expected_disposition
    assert [effect.kind for effect in transition.effects] == [expected_effect_kind]
    assert transition.message_ledger_mutations == ()
    assert transition.aggregate.active_chat_round_operation_id == ""
    assert transition.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert operation_id not in transition.aggregate.data.get("operation_fences", {})
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.operations[0].failure_code == "unsupported_contract"
    assert transition.operations[0].metadata["failure_event_id"] == quarantined.event_id
    assert transition.operations[0].metadata["effect_failure"]["source"] == "effect_store"
    retry_chain = transition.aggregate.active_chat_state["round_retry_chain"]
    assert retry_chain["active_epoch"] == 3
    assert retry_chain["message_log_ids"] == [21]
    assert retry_chain["attempt_count"] == 1
    if max_attempts == 1:
        assert transition.aggregate.active_chat_state["exit_requested"] is True
        assert transition.aggregate.active_chat_state["round_retry_blocker"][
            "max_attempts"
        ] == 1
    else:
        assert "round_retry_blocker" not in transition.aggregate.active_chat_state
        assert transition.aggregate.active_chat_state["round_schedule_id"]


def test_effect_store_quarantine_fails_pending_v3_bootstrap_closed() -> None:
    """A quarantined bootstrap exits without consuming buffered post-handoff input."""

    reducer, pending = _pending_v3_bootstrap()
    operation_id = str(pending.active_chat_state["bootstrap_operation_id"])
    quarantined = _effect_quarantined(pending, operation_id=operation_id)

    transition = reducer.reduce(pending, quarantined)

    assert transition.disposition == "active_chat_bootstrap_effect_failed_exit_requested"
    assert [effect.kind for effect in transition.effects] == [
        "enqueue_active_chat_exit_request"
    ]
    assert transition.message_ledger_mutations == ()
    assert transition.aggregate.active_chat_state["bootstrap_status"] == "exit_requested"
    assert transition.aggregate.active_chat_state["bootstrap_operation_id"] == ""
    assert transition.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert operation_id not in transition.aggregate.data.get("operation_fences", {})
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.operations[0].failure_code == "unsupported_contract"
    assert transition.operations[0].metadata["failure_event_id"] == quarantined.event_id
    assert transition.operations[0].metadata["effect_failure"]["source"] == "effect_store"


@pytest.mark.parametrize(
    ("source", "effect_id"),
    (
        ("untrusted_effect_store", None),
        ("effect_store", "forged-round-effect"),
    ),
)
def test_effect_store_quarantine_requires_exact_source_and_effect_identity(
    source: str,
    effect_id: str | None,
) -> None:
    """Forged quarantine evidence cannot release a live v3 round operation."""

    reducer, running = _running_v3_round(config=IdleExitReducerConfig())
    operation_id = running.active_chat_round_operation_id
    quarantined = _effect_quarantined(
        running,
        operation_id=operation_id,
        source=source,
        effect_id=effect_id,
    )

    transition = reducer.reduce(running, quarantined)

    assert "ignored" in transition.disposition or "stale" in transition.disposition
    assert transition.effects == ()
    assert transition.operations == ()
    assert transition.message_ledger_mutations == ()
    assert transition.aggregate.active_chat_round_operation_id == operation_id
    assert transition.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert operation_id in transition.aggregate.data["operation_fences"]
