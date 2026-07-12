"""Actor-owned active-chat bootstrap, round, and tick reducer coverage."""

from __future__ import annotations

from dataclasses import replace

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    derived_effect_event_id,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionEffect,
    SessionEventEnvelope,
    SessionOperationStatus,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionReceiptStatus,
    builtin_external_action_effect_contract,
)
from shinbot.agent.runtime.session_actor.message_ledger import (
    ConsumeMessageLedgerEntries,
    MessageLedgerConsumptionKind,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


def _active_chat(*, bootstrap_status: str = "completed") -> AgentSessionAggregate:
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
            "interest_value": 20.0,
            "decay_half_life_seconds": 20.0,
            "entered_at": 10.0,
            "updated_at": 10.0,
            "tick_count": 0,
            "pending_message_log_ids": [],
            "bootstrap_status": bootstrap_status,
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


def _control_effect_event(
    aggregate: AgentSessionAggregate,
    *,
    effect_kind: str,
    failed: bool = False,
) -> SessionEventEnvelope:
    """Build the authoritative executor envelope for one active-chat control."""

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
    if effect_kind == "enqueue_active_chat_exit_request":
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
        occurred_at=80.0,
        causation_id=str(intent["causation_id"]),
        correlation_id=str(intent["operation_id"] or intent["effect_id"]),
        payload=payload,
    )


def _stamp_operation_sequence(
    aggregate: AgentSessionAggregate,
    operation_id: str,
    sequence: int,
) -> AgentSessionAggregate:
    data = dict(aggregate.data)
    registry = dict(data["operation_fences"])
    fence = dict(registry[operation_id])
    fence["input_ledger_sequence"] = sequence
    registry[operation_id] = fence
    data["operation_fences"] = registry
    return replace(aggregate, data=data)


def _round_completion(
    aggregate: AgentSessionAggregate,
    *,
    outcome: str,
    consumed_ids: list[int],
    interest_delta: float = 0.0,
    intents: list[dict[str, object]] | None = None,
) -> SessionEventEnvelope:
    operation_id = aggregate.active_chat_round_operation_id
    fence = aggregate.data["operation_fences"][operation_id]
    contract = builtin_effect_contract("run_active_chat_round")
    wire_intents = [
        {
            "action_ordinal": item["action_ordinal"],
            "kind": item["kind"],
            "payload": item["payload"],
            "proposal_id": item["tool_call_id"],
        }
        for item in (intents or [])
    ]
    return SessionEventEnvelope(
        event_id=str(fence["completion_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.ACTIVE_CHAT_ROUND_COMPLETED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=60.0,
        causation_id=str(fence["source_event_id"]),
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
                "completion_type": "active_chat_round",
                "consumed_message_log_ids": consumed_ids,
                "external_actions": {"schema_version": 1, "intents": wire_intents},
                "outcome": outcome,
                "interest_delta": interest_delta,
                "reason": "round completed",
            },
        },
    )


def _external_action_completion(
    aggregate: AgentSessionAggregate,
    effect: SessionEffect,
    *,
    receipt_status: ExternalActionReceiptStatus,
) -> SessionEventEnvelope:
    pending = aggregate.data["pending_outbound_actions"][effect.effect_id]
    contract = builtin_external_action_effect_contract(effect.kind)
    return SessionEventEnvelope(
        event_id=derived_effect_event_id(
            key=_KEY,
            effect_id=effect.effect_id,
            outcome="completed",
        ),
        key=_KEY,
        kind=AgentSessionEventKind.EXTERNAL_ACTION_COMPLETED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=70.0,
        causation_id=str(pending["source_event_id"]),
        correlation_id=effect.operation_id or effect.effect_id,
        payload={
            "action_ordinal": pending["action_ordinal"],
            "attempt_count": 1,
            "contract_signature": contract.signature,
            "contract_version": contract.version,
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "idempotency_key": effect.idempotency_key,
            "operation_id": effect.operation_id,
            "receipt_status": receipt_status.value,
            "request_digest": pending["request_digest"],
        },
    )


def _pending_bootstrap() -> AgentSessionAggregate:
    aggregate = _active_chat(bootstrap_status="pending")
    operation_id = "bootstrap-operation-a"
    effect_id = "bootstrap-effect-a"
    state = dict(aggregate.active_chat_state)
    state["bootstrap_operation_id"] = operation_id
    data = dict(aggregate.data)
    data["operation_fences"] = {
        operation_id: {
            "operation_id": operation_id,
            "operation_kind": "active_chat_bootstrap",
            "source_event_id": "review-completion-a",
            "effect_id": effect_id,
            "effect_kind": "run_active_chat_bootstrap",
            "idempotency_key": effect_id,
            "completion_event_id": "bootstrap-completion-a",
            "failure_event_id": "bootstrap-failure-a",
            "ownership_generation": 1,
            "plan_id": "review-plan-a",
            "active_epoch": 3,
            "activity_generation": 7,
            "input_watermark": 20,
            "input_ledger_sequence": 1,
            "instance_id": "instance-a",
            "target_session_id": "instance-a:group:room-a",
        }
    }
    return replace(aggregate, active_chat_state=state, data=data)


def _bootstrap_completion(
    aggregate: AgentSessionAggregate,
    *,
    disposition: str,
) -> SessionEventEnvelope:
    operation_id = str(aggregate.active_chat_state["bootstrap_operation_id"])
    fence = aggregate.data["operation_fences"][operation_id]
    contract = builtin_effect_contract("run_active_chat_bootstrap")
    return SessionEventEnvelope(
        event_id=str(fence["completion_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.ACTIVE_CHAT_BOOTSTRAP_COMPLETED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=45.0,
        causation_id=str(fence["source_event_id"]),
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
                "completion_type": "active_chat_bootstrap",
                "disposition": disposition,
                "reason": "bootstrap complete",
            },
        },
    )


def _workflow_effect_failure(
    aggregate: AgentSessionAggregate,
    *,
    operation_id: str,
    event_id: str | None = None,
) -> SessionEventEnvelope:
    fence = aggregate.data["operation_fences"][operation_id]
    contract = builtin_effect_contract(str(fence["effect_kind"]))
    return SessionEventEnvelope(
        event_id=event_id or str(fence["failure_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.EFFECT_FAILED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=65.0,
        causation_id=str(fence["source_event_id"]),
        correlation_id=operation_id,
        trace_id="trace:workflow-failure",
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
            "attempt_count": 3,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
            "failure_code": "SyntheticWorkflowFailure",
            "failure_message": "workflow retries exhausted",
        },
    )


def test_message_debounces_then_round_due_freezes_one_input_snapshot() -> None:
    reducer = AgentSessionReducer()
    buffered = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )

    assert buffered.disposition == "active_chat_message_buffered"
    assert buffered.aggregate.active_chat_round_operation_id == ""
    assert len(buffered.effects) == 1
    assert buffered.effects[0].kind == "enqueue_active_chat_round_due"
    due = reducer.reduce(buffered.aggregate, _round_due_event(buffered.aggregate))

    assert due.disposition == "active_chat_round_started"
    assert due.aggregate.active_chat_round_operation_id
    assert due.aggregate.active_chat_state["round_input_message_log_ids"] == [21]
    assert due.effects[0].kind == "run_active_chat_round"


def test_later_message_stays_beyond_running_round_snapshot() -> None:
    reducer = AgentSessionReducer()
    buffered = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    running = reducer.reduce(
        buffered.aggregate,
        _round_due_event(buffered.aggregate),
    ).aggregate
    running = _stamp_operation_sequence(
        running,
        running.active_chat_round_operation_id,
        1,
    )
    later = reducer.reduce(
        running,
        _message_event(event_id="message:22", message_log_id=22),
    ).aggregate

    completed = reducer.reduce(
        later,
        _round_completion(
            later,
            outcome="continue",
            consumed_ids=[21],
            interest_delta=2.0,
        ),
    )

    assert completed.disposition == "active_chat_round_completed"
    assert completed.aggregate.active_chat_round_operation_id == ""
    assert completed.aggregate.active_chat_state["pending_message_log_ids"] == [22]
    assert completed.operations[0].status is SessionOperationStatus.COMPLETED
    consumption = completed.message_ledger_mutations[0]
    assert isinstance(consumption, ConsumeMessageLedgerEntries)
    assert consumption.kind is MessageLedgerConsumptionKind.CHAT
    assert consumption.explicit_message_log_ids == (21,)
    assert consumption.input_ledger_sequence == 1
    assert any(effect.kind == "enqueue_active_chat_round_due" for effect in completed.effects)


def test_round_exit_with_no_new_input_only_enqueues_fenced_exit_request() -> None:
    reducer = AgentSessionReducer()
    buffered = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    running = reducer.reduce(
        buffered.aggregate,
        _round_due_event(buffered.aggregate),
    ).aggregate
    running = _stamp_operation_sequence(
        running,
        running.active_chat_round_operation_id,
        1,
    )

    completed = reducer.reduce(
        running,
        _round_completion(running, outcome="exit", consumed_ids=[21]),
    )

    assert completed.disposition == "active_chat_round_exit_requested"
    assert completed.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert completed.aggregate.active_chat_state["exit_requested"] is True
    assert completed.effects[-1].kind == "enqueue_active_chat_exit_request"


def test_round_effect_failure_keeps_input_and_retries_through_round_due() -> None:
    reducer = AgentSessionReducer()
    buffered = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    running = reducer.reduce(
        buffered.aggregate,
        _round_due_event(buffered.aggregate),
    ).aggregate
    operation_id = running.active_chat_round_operation_id
    running = _stamp_operation_sequence(running, operation_id, 1)

    failed = reducer.reduce(
        running,
        _workflow_effect_failure(running, operation_id=operation_id),
    )

    assert failed.disposition == "active_chat_round_effect_failed_retry_scheduled"
    assert failed.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert failed.aggregate.active_chat_round_operation_id == ""
    assert operation_id not in failed.aggregate.data.get("operation_fences", {})
    assert failed.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert failed.aggregate.active_chat_state["round_schedule_id"]
    assert [effect.kind for effect in failed.effects] == [
        "enqueue_active_chat_round_due"
    ]
    assert failed.message_ledger_mutations == ()
    operation = failed.operations[0]
    assert operation.status is SessionOperationStatus.FAILED
    assert operation.metadata["failure_event_id"] == (
        running.data["operation_fences"][operation_id]["failure_event_id"]
    )
    assert operation.metadata["effect_failure"]["effect_kind"] == (
        "run_active_chat_round"
    )

    retry = reducer.reduce(failed.aggregate, _round_due_event(failed.aggregate))

    assert retry.disposition == "active_chat_round_started"
    assert [effect.kind for effect in retry.effects] == ["run_active_chat_round"]


def test_round_effect_failure_requires_its_failure_event_id() -> None:
    reducer = AgentSessionReducer()
    buffered = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    running = reducer.reduce(
        buffered.aggregate,
        _round_due_event(buffered.aggregate),
    ).aggregate
    operation_id = running.active_chat_round_operation_id
    running = _stamp_operation_sequence(running, operation_id, 1)

    stale = reducer.reduce(
        running,
        _workflow_effect_failure(
            running,
            operation_id=operation_id,
            event_id="wrong-round-failure-event",
        ),
    )

    assert stale.disposition == "active_chat_round_effect_failure_stale"
    assert "event_id_changed" in stale.reason
    assert stale.aggregate.active_chat_round_operation_id == operation_id
    assert operation_id in stale.aggregate.data["operation_fences"]


def test_round_action_delays_exit_and_next_round_until_receipt_succeeds() -> None:
    reducer = AgentSessionReducer()
    buffered = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    running = reducer.reduce(
        buffered.aggregate,
        _round_due_event(buffered.aggregate),
    ).aggregate
    running = _stamp_operation_sequence(
        running,
        running.active_chat_round_operation_id,
        1,
    )
    waiting = reducer.reduce(
        running,
        _round_completion(
            running,
            outcome="exit",
            consumed_ids=[21],
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "round-tool-a",
                    "action_ordinal": 0,
                    "payload": {"text": "one final reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")

    assert waiting.disposition == "active_chat_round_waiting_outbound"
    assert waiting.aggregate.active_chat_round_operation_id == ""
    assert waiting.aggregate.data["outbound_continuation"]["kind"] == (
        "active_chat_round"
    )
    assert all(
        effect.kind != "enqueue_active_chat_exit_request" for effect in waiting.effects
    )

    buffered_later = reducer.reduce(
        waiting.aggregate,
        _message_event(event_id="message:22", message_log_id=22),
    )

    assert buffered_later.effects == ()
    assert buffered_later.aggregate.active_chat_state["pending_message_log_ids"] == [
        22
    ]

    released = reducer.reduce(
        buffered_later.aggregate,
        _external_action_completion(
            buffered_later.aggregate,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
        ),
    )

    assert released.disposition == "external_actions_completed_active_chat_round_scheduled"
    assert "pending_outbound_actions" not in released.aggregate.data
    assert "outbound_continuation" not in released.aggregate.data
    assert released.aggregate.active_chat_state["round_schedule_id"]
    assert [effect.kind for effect in released.effects] == [
        "enqueue_active_chat_round_due"
    ]


def test_tick_uses_message_watermark_fence_before_requesting_exit() -> None:
    reducer = AgentSessionReducer()
    aggregate = _active_chat()
    state = dict(aggregate.active_chat_state)
    state.update({"interest_value": 1.0, "updated_at": 10.0})
    aggregate = replace(aggregate, active_chat_state=state)
    tick = SessionEventEnvelope(
        event_id="tick:1",
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

    transition = reducer.reduce(aggregate, tick)

    assert transition.disposition == "active_chat_tick_exit_requested"
    assert transition.effects[0].kind == "enqueue_active_chat_exit_request"
    stale = reducer.reduce(
        replace(aggregate, data={**aggregate.data, "message_watermark": 21}),
        tick,
    )
    assert stale.disposition == "active_chat_tick_stale"
    assert "message_watermark_changed" in stale.reason


def test_exit_control_completion_enters_the_single_settling_path() -> None:
    reducer = AgentSessionReducer()
    aggregate = _active_chat()
    state = dict(aggregate.active_chat_state)
    state.update({"interest_value": 1.0, "updated_at": 10.0})
    aggregate = replace(aggregate, active_chat_state=state)
    tick = SessionEventEnvelope(
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
    requested = reducer.reduce(aggregate, tick)

    settled = reducer.reduce(
        requested.aggregate,
        _control_effect_event(
            requested.aggregate,
            effect_kind="enqueue_active_chat_exit_request",
        ),
    )

    intent = settled.aggregate.data["effect_control_intents"][
        "enqueue_active_chat_exit_request"
    ]
    assert settled.disposition == "active_chat_exit_settling"
    assert settled.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert intent["status"] == "completed"
    assert intent["completion"]["effect_id"] == requested.effects[0].effect_id


def test_exit_control_failure_is_bounded_then_requires_new_activity() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(control_reconciliation_max_cycles=2)
    )
    aggregate = _active_chat()
    state = dict(aggregate.active_chat_state)
    state.update({"interest_value": 1.0, "updated_at": 10.0})
    aggregate = replace(aggregate, active_chat_state=state)
    tick = SessionEventEnvelope(
        event_id="tick:exit-failure",
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
    requested = reducer.reduce(aggregate, tick)
    retried = reducer.reduce(
        requested.aggregate,
        _control_effect_event(
            requested.aggregate,
            effect_kind="enqueue_active_chat_exit_request",
            failed=True,
        ),
    )

    assert retried.disposition == "active_chat_exit_request_retry_scheduled"
    assert retried.effects[0].kind == "enqueue_active_chat_exit_request"
    assert retried.aggregate.data["effect_control_intents"][
        "enqueue_active_chat_exit_request"
    ]["retry_cycle"] == 1

    blocked = reducer.reduce(
        retried.aggregate,
        _control_effect_event(
            retried.aggregate,
            effect_kind="enqueue_active_chat_exit_request",
            failed=True,
        ),
    )

    assert blocked.disposition == "active_chat_exit_request_failed_blocked"
    assert blocked.effects == ()
    assert blocked.aggregate.data["effect_control_intents"][
        "enqueue_active_chat_exit_request"
    ]["status"] == "failed"
    assert reducer.reduce(blocked.aggregate, tick).effects == ()

    resumed = reducer.reduce(
        blocked.aggregate,
        _message_event(event_id="message:21", message_log_id=21),
    )
    assert resumed.aggregate.data["effect_control_intents"][
        "enqueue_active_chat_exit_request"
    ]["status"] == "superseded"
    assert [effect.kind for effect in resumed.effects] == [
        "enqueue_active_chat_round_due"
    ]


def test_round_due_control_failure_retries_then_preserves_pending_input() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(control_reconciliation_max_cycles=2)
    )
    requested = reducer.reduce(
        _active_chat(),
        _message_event(event_id="message:21", message_log_id=21),
    )
    retried = reducer.reduce(
        requested.aggregate,
        _control_effect_event(
            requested.aggregate,
            effect_kind="enqueue_active_chat_round_due",
            failed=True,
        ),
    )

    assert retried.disposition == "active_chat_round_due_retry_scheduled"
    assert retried.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert retried.effects[0].kind == "enqueue_active_chat_round_due"

    blocked = reducer.reduce(
        retried.aggregate,
        _control_effect_event(
            retried.aggregate,
            effect_kind="enqueue_active_chat_round_due",
            failed=True,
        ),
    )

    assert blocked.disposition == "active_chat_round_due_failed_blocked"
    assert blocked.effects == ()
    assert blocked.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert blocked.aggregate.data["effect_control_intents"][
        "enqueue_active_chat_round_due"
    ]["status"] == "failed"

    resumed = reducer.reduce(
        blocked.aggregate,
        _message_event(event_id="message:22", message_log_id=22),
    )
    assert [effect.kind for effect in resumed.effects] == [
        "enqueue_active_chat_round_due"
    ]
    assert resumed.aggregate.data["effect_control_intents"][
        "enqueue_active_chat_round_due"
    ]["status"] == "requested"


def test_bootstrap_completion_releases_buffered_messages_to_durable_round_timer() -> None:
    reducer = AgentSessionReducer()
    pending = _pending_bootstrap()
    buffered = reducer.reduce(
        pending,
        _message_event(event_id="message:21", message_log_id=21),
    )
    assert buffered.effects == ()

    completed = reducer.reduce(
        buffered.aggregate,
        _bootstrap_completion(buffered.aggregate, disposition="watch"),
    )

    assert completed.disposition == "active_chat_bootstrap_completed"
    assert completed.aggregate.active_chat_state["bootstrap_status"] == "completed"
    assert completed.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert completed.operations[0].status is SessionOperationStatus.COMPLETED
    assert completed.effects[0].kind == "enqueue_active_chat_round_due"


def test_bootstrap_effect_failure_terminalizes_then_uses_fenced_exit() -> None:
    reducer = AgentSessionReducer()
    pending = _pending_bootstrap()
    buffered = reducer.reduce(
        pending,
        _message_event(event_id="message:21", message_log_id=21),
    )
    operation_id = str(buffered.aggregate.active_chat_state["bootstrap_operation_id"])

    failed = reducer.reduce(
        buffered.aggregate,
        _workflow_effect_failure(
            buffered.aggregate,
            operation_id=operation_id,
        ),
    )

    assert failed.disposition == "active_chat_bootstrap_effect_failed_exit_requested"
    assert failed.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert failed.aggregate.active_chat_state["bootstrap_status"] == "exit_requested"
    assert failed.aggregate.active_chat_state["bootstrap_operation_id"] == ""
    assert failed.aggregate.active_chat_state["pending_message_log_ids"] == [21]
    assert operation_id not in failed.aggregate.data.get("operation_fences", {})
    assert [effect.kind for effect in failed.effects] == [
        "enqueue_active_chat_exit_request"
    ]
    assert failed.message_ledger_mutations == ()
    operation = failed.operations[0]
    assert operation.status is SessionOperationStatus.FAILED
    assert operation.metadata["failure_event_id"] == "bootstrap-failure-a"
    assert operation.metadata["effect_failure"]["causation_id"] == (
        "review-completion-a"
    )


def test_bootstrap_effect_failure_requires_its_failure_event_id() -> None:
    reducer = AgentSessionReducer()
    pending = _pending_bootstrap()
    operation_id = str(pending.active_chat_state["bootstrap_operation_id"])

    stale = reducer.reduce(
        pending,
        _workflow_effect_failure(
            pending,
            operation_id=operation_id,
            event_id="wrong-bootstrap-failure-event",
        ),
    )

    assert stale.disposition == "active_chat_bootstrap_effect_failure_stale"
    assert "event_id_changed" in stale.reason
    assert stale.aggregate.active_chat_state["bootstrap_operation_id"] == operation_id
    assert operation_id in stale.aggregate.data["operation_fences"]


def test_bootstrap_exit_uses_same_fenced_idle_exit_entry_path() -> None:
    reducer = AgentSessionReducer()
    completed = reducer.reduce(
        _pending_bootstrap(),
        _bootstrap_completion(_pending_bootstrap(), disposition="exit_soon"),
    )

    assert completed.disposition == "active_chat_bootstrap_completed"
    assert completed.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert completed.aggregate.active_chat_state["bootstrap_status"] == "exit_requested"
    assert completed.effects[0].kind == "enqueue_active_chat_exit_request"
