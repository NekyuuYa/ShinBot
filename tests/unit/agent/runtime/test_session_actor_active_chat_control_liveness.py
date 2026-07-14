"""Regression coverage for Active Chat control-effect liveness."""

from __future__ import annotations

from dataclasses import replace

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope
from shinbot.agent.runtime.session_actor.message_ledger import (
    ConsumeMessageLedgerEntries,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEffectKind,
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)
from shinbot.agent.runtime.session_actor.review_due_identity import (
    REVIEW_DUE_EVENT_SOURCE,
    review_due_event_id,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


def _active_chat() -> AgentSessionAggregate:
    """Build one completed Active Chat aggregate with a durable review plan."""

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
    """Build one normal-priority message delivery for the active session."""

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


def _control_effect_event(
    aggregate: AgentSessionAggregate,
    *,
    effect_kind: AgentSessionEffectKind,
    failed: bool,
    occurred_at: float,
) -> SessionEventEnvelope:
    """Build a fenced completion or failure for one Active Chat control effect."""

    intent = aggregate.data["effect_control_intents"][effect_kind]
    contract = builtin_effect_contract(
        effect_kind,
        version=int(intent["contract_version"]),
    )
    payload: dict[str, object] = {
        "effect_id": intent["effect_id"],
        "effect_kind": effect_kind,
        "idempotency_key": intent["idempotency_key"],
        "operation_id": intent["operation_id"],
        "plan_id": intent["plan_id"],
        "active_epoch": intent["active_epoch"],
        "activity_generation": intent["activity_generation"],
        "input_watermark": intent["input_watermark"],
        "input_ledger_sequence": intent["input_ledger_sequence"],
        "attempt_count": 1,
        "contract_version": contract.version,
        "contract_signature": contract.signature,
    }
    if effect_kind == AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST:
        payload.update(
            {
                "trigger": intent["trigger"],
                "expected_active_epoch": intent["expected_active_epoch"],
                "expected_message_watermark": intent[
                    "expected_message_watermark"
                ],
            }
        )
    else:
        payload.update(
            {
                "schedule_id": intent["schedule_id"],
                "schedule_revision": intent["schedule_revision"],
            }
        )
    if failed:
        payload.update(
            {
                "failure_code": "SyntheticControlFailure",
                "failure_message": "control effect retries exhausted",
            }
        )
    return SessionEventEnvelope(
        event_id=str(
            intent["failure_event_id" if failed else "completion_event_id"]
        ),
        key=_KEY,
        kind=(
            AgentSessionEventKind.EFFECT_FAILED
            if failed
            else contract.completion_event_kind
        ),
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=occurred_at,
        causation_id=str(intent["causation_id"]),
        correlation_id=str(intent["operation_id"] or intent["effect_id"]),
        payload=payload,
    )


def _exit_tick() -> SessionEventEnvelope:
    """Build an idle-interest tick that asks the active chat to exit."""

    return SessionEventEnvelope(
        event_id="tick:exit",
        key=_KEY,
        kind=AgentSessionEventKind.ACTIVE_CHAT_TICK,
        ownership_generation=1,
        source="active_chat_timer",
        occurred_at=20.0,
        payload={
            "active_epoch": 3,
            "expected_message_watermark": 20,
            "ownership_generation": 1,
        },
    )


def _idle_planner_completion(
    aggregate: AgentSessionAggregate,
) -> SessionEventEnvelope:
    """Complete the idle planner with an immediately due next-review plan."""

    pending = aggregate.data["idle_exit"]
    operation_id = str(pending["operation_id"])
    fence = aggregate.data["operation_fences"][operation_id]
    effect_kind = AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING
    contract = builtin_effect_contract(effect_kind)
    return SessionEventEnvelope(
        event_id=str(pending["completion_event_id"]),
        key=_KEY,
        kind=contract.completion_event_kind,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=70.0,
        causation_id=str(pending["requested_by_event_id"]),
        payload={
            "effect_id": pending["planner_effect_id"],
            "effect_kind": effect_kind,
            "idempotency_key": pending["planner_idempotency_key"],
            "operation_id": operation_id,
            "plan_id": pending["plan_id"],
            "active_epoch": pending["active_epoch"],
            "activity_generation": pending["activity_generation"],
            "input_watermark": fence["input_watermark"],
            "input_ledger_sequence": fence["input_ledger_sequence"],
            "attempt_count": 1,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
            "outcome": {
                "kind": "planned",
                "requested_delay_seconds": 0.0,
                "reason": "recover_pending_active_chat_input",
            },
        },
    )


def _review_due_event(aggregate: AgentSessionAggregate) -> SessionEventEnvelope:
    """Build the authoritative due delivery for the current review plan."""

    event_id = review_due_event_id(
        key=aggregate.key,
        plan_id=aggregate.current_plan_id,
        plan_revision=aggregate.review_plan_revision,
        ownership_generation=aggregate.ownership_generation,
    )
    return SessionEventEnvelope(
        event_id=event_id,
        key=aggregate.key,
        kind=AgentSessionEventKind.REVIEW_DUE,
        ownership_generation=aggregate.ownership_generation,
        source=REVIEW_DUE_EVENT_SOURCE,
        occurred_at=80.0,
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": aggregate.key.profile_id,
                "session_id": aggregate.key.session_id,
            },
            "plan_id": aggregate.current_plan_id,
            "plan_revision": aggregate.review_plan_revision,
            "ownership_generation": aggregate.ownership_generation,
            "attempt_count": 0,
        },
    )


def test_round_due_control_exhaustion_requests_exit_and_preserves_pending_input() -> None:
    """A dead round timer must hand pending input to the existing exit path."""

    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(control_reconciliation_max_cycles=2)
    )
    scheduled = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    retried = reducer.reduce(
        scheduled.aggregate,
        _control_effect_event(
            scheduled.aggregate,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
            failed=True,
            occurred_at=40.0,
        ),
    )

    exhausted = reducer.reduce(
        retried.aggregate,
        _control_effect_event(
            retried.aggregate,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
            failed=True,
            occurred_at=50.0,
        ),
    )

    assert exhausted.reason == "active_chat_round_due_retries_exhausted"
    assert exhausted.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert exhausted.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert exhausted.aggregate.active_chat_state["exit_requested"] is True
    assert [effect.kind for effect in exhausted.effects] == [
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
    ]
    assert exhausted.message_ledger_mutations == ()


def test_exit_request_control_exhaustion_fails_over_to_settling_and_idle_planning() -> None:
    """A dead exit enqueue must not leave Active Chat permanently blocked."""

    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(control_reconciliation_max_cycles=2)
    )
    active = _active_chat()
    low_interest = replace(
        active,
        active_chat_state={
            **active.active_chat_state,
            "interest_value": 1.0,
            "updated_at": 10.0,
        },
    )
    requested = reducer.reduce(low_interest, _exit_tick())
    retried = reducer.reduce(
        requested.aggregate,
        _control_effect_event(
            requested.aggregate,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
            failed=True,
            occurred_at=40.0,
        ),
    )

    exhausted = reducer.reduce(
        retried.aggregate,
        _control_effect_event(
            retried.aggregate,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
            failed=True,
            occurred_at=50.0,
        ),
    )

    assert exhausted.reason == "active_chat_exit_request_retries_exhausted"
    assert exhausted.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert exhausted.aggregate.idle_planning_operation_id
    assert "exit_blocker" not in exhausted.aggregate.active_chat_state
    assert {effect.kind for effect in exhausted.effects} == {
        AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
        AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
    }
    assert exhausted.message_ledger_mutations == ()


def test_round_due_failure_path_leaves_pending_ledger_for_the_next_review() -> None:
    """Exit recovery must not consume input before the successor review owns it."""

    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(control_reconciliation_max_cycles=2)
    )
    scheduled = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    retried = reducer.reduce(
        scheduled.aggregate,
        _control_effect_event(
            scheduled.aggregate,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
            failed=True,
            occurred_at=40.0,
        ),
    )
    exit_requested = reducer.reduce(
        retried.aggregate,
        _control_effect_event(
            retried.aggregate,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
            failed=True,
            occurred_at=50.0,
        ),
    )
    settling = reducer.reduce(
        exit_requested.aggregate,
        _control_effect_event(
            exit_requested.aggregate,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
            failed=False,
            occurred_at=60.0,
        ),
    )
    idle = reducer.reduce(
        settling.aggregate,
        _idle_planner_completion(settling.aggregate),
    )
    next_review = reducer.reduce(idle.aggregate, _review_due_event(idle.aggregate))

    transitions = (scheduled, retried, exit_requested, settling, idle, next_review)
    assert all(
        not isinstance(mutation, ConsumeMessageLedgerEntries)
        for transition in transitions
        for mutation in transition.message_ledger_mutations
    )
    assert next_review.disposition == "review_started"
    assert next_review.effects[0].kind == AgentSessionEffectKind.RUN_REVIEW_WORKFLOW
    assert next_review.effects[0].payload["input_watermark"] == 21
    assert next_review.operations[0].input_watermark == 21
