"""Workflow-completion tests for the durable Agent session reducer."""

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
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


def _message_event(
    *,
    event_id: str,
    message_log_id: int,
    mentioned: bool = True,
) -> SessionEventEnvelope:
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
            "is_mentioned": mentioned,
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


def _active_reply_completion(
    aggregate: AgentSessionAggregate,
    *,
    consumed_ids: list[int],
    sequence: int,
    intents: list[dict[str, object]] | None = None,
) -> SessionEventEnvelope:
    operation_id = aggregate.active_reply_operation_id
    fence = aggregate.data["operation_fences"][operation_id]
    contract = builtin_effect_contract("run_active_reply_workflow")
    wire_intents = [
        {
            "proposal_id": item["tool_call_id"],
            "action_ordinal": item["action_ordinal"],
            "kind": item["kind"],
            "payload": item["payload"],
        }
        for item in (intents or [])
    ]
    return SessionEventEnvelope(
        event_id=str(fence["completion_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.ACTIVE_REPLY_COMPLETED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=200.0,
        causation_id=str(fence["source_event_id"]),
        trace_id="trace:completion",
        payload={
            "effect_id": fence["effect_id"],
            "effect_kind": contract.effect_kind,
            "idempotency_key": fence["idempotency_key"],
            "operation_id": operation_id,
            "plan_id": fence["plan_id"],
            "active_epoch": fence["active_epoch"],
            "activity_generation": fence["activity_generation"],
            "input_watermark": fence["input_watermark"],
            "input_ledger_sequence": sequence,
            "attempt_count": 1,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
            "workflow_result": {
                "schema_version": 1,
                "completion_type": "active_reply",
                "consumed_message_log_ids": consumed_ids,
                "external_actions": {
                    "schema_version": 1,
                    "intents": wire_intents,
                },
            },
        },
    )


def _started_active_reply() -> tuple[AgentSessionReducer, AgentSessionAggregate]:
    reducer = AgentSessionReducer()
    initial = AgentSessionAggregate(
        key=_KEY,
        ownership_generation=1,
        updated_at=1.0,
    )
    started = reducer.reduce(
        initial,
        _message_event(event_id="message:10", message_log_id=10),
    ).aggregate
    return reducer, _stamp_operation_sequence(
        started,
        started.active_reply_operation_id,
        1,
    )


def _started_review() -> tuple[AgentSessionReducer, AgentSessionAggregate]:
    reducer = AgentSessionReducer()
    operation_id = "review-operation-a"
    effect_id = "review-effect-a"
    aggregate = AgentSessionAggregate(
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
                    "instance_id": "instance-a",
                    "target_session_id": "instance-a:group:room-a",
                }
            },
        },
        updated_at=100.0,
    )
    return reducer, aggregate


def _review_completion(
    aggregate: AgentSessionAggregate,
    *,
    enter_active_chat: object,
    consumed_ids: list[int],
    intents: list[dict[str, object]] | None = None,
    next_review_outcome: dict[str, object] | None = None,
    extra_payload: dict[str, object] | None = None,
) -> SessionEventEnvelope:
    operation_id = aggregate.review_operation_id
    fence = aggregate.data["operation_fences"][operation_id]
    contract = builtin_effect_contract("run_review_workflow")
    wire_intents = [
        {
            "proposal_id": item["tool_call_id"],
            "action_ordinal": item["action_ordinal"],
            "kind": item["kind"],
            "payload": item["payload"],
        }
        for item in (intents or [])
    ]
    wire_outcome: dict[str, object] | None = None
    if next_review_outcome is not None:
        wire_outcome = {
            "kind": next_review_outcome["kind"],
            "applied_delay_seconds": next_review_outcome.get(
                "applied_delay_seconds",
                next_review_outcome.get("requested_delay_seconds", 900.0),
            ),
            "requested_delay_seconds": next_review_outcome.get(
                "requested_delay_seconds"
            ),
            "reason": next_review_outcome["reason"],
            "fallback_reason": next_review_outcome.get("fallback_reason", ""),
        }
    payload: dict[str, object] = {
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
            "consumed_message_log_ids": consumed_ids,
            "external_actions": {
                "schema_version": 1,
                "intents": wire_intents,
            },
            "enter_active_chat": enter_active_chat,
            "next_review_outcome": wire_outcome,
        },
    }
    payload.update(extra_payload or {})
    return SessionEventEnvelope(
        event_id=str(fence["completion_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.REVIEW_COMPLETED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=200.0,
        causation_id=str(fence["source_event_id"]),
        trace_id="trace:review-completion",
        payload=payload,
    )


def _review_due_event(*, event_id: str = "review-due-a") -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id,
        key=_KEY,
        kind=AgentSessionEventKind.REVIEW_DUE,
        ownership_generation=1,
        source="review_due_scanner",
        occurred_at=100.0,
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": _KEY.profile_id,
                "session_id": _KEY.session_id,
            },
            "plan_id": "review-plan-a",
            "plan_revision": 1,
            "ownership_generation": 1,
            "attempt_count": 0,
        },
    )


def _external_action_completion(
    aggregate: AgentSessionAggregate,
    effect: SessionEffect,
    *,
    receipt_status: ExternalActionReceiptStatus,
    request_digest: str | None = None,
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
        occurred_at=210.0,
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
            "request_digest": request_digest or pending["request_digest"],
        },
    )


def _external_action_failure(
    aggregate: AgentSessionAggregate,
    effect: SessionEffect,
    *,
    event_id: str | None = None,
    request_digest: str | None = None,
) -> SessionEventEnvelope:
    pending = aggregate.data["pending_outbound_actions"][effect.effect_id]
    contract = builtin_external_action_effect_contract(effect.kind)
    return SessionEventEnvelope(
        event_id=event_id
        or derived_effect_event_id(
            key=_KEY,
            effect_id=effect.effect_id,
            outcome="failed",
        ),
        key=_KEY,
        kind=AgentSessionEventKind.EFFECT_FAILED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=210.0,
        causation_id=str(pending["source_event_id"]),
        correlation_id=effect.operation_id or effect.effect_id,
        trace_id="trace:external-action-failure",
        payload={
            "action_ordinal": pending["action_ordinal"],
            "attempt_count": 5,
            "contract_signature": contract.signature,
            "contract_version": contract.version,
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "failure_code": "ExternalActionRetryRequired",
            "failure_message": "adapter remained unavailable before dispatch",
            "idempotency_key": effect.idempotency_key,
            "operation_id": effect.operation_id,
            "request_digest": request_digest or pending["request_digest"],
        },
    )


def _workflow_effect_failure(
    aggregate: AgentSessionAggregate,
    *,
    operation_id: str,
    event_id: str | None = None,
    failure_code: str = "SyntheticWorkflowFailure",
    failure_message: str = "workflow retries exhausted",
) -> SessionEventEnvelope:
    fence = aggregate.data["operation_fences"][operation_id]
    contract = builtin_effect_contract(str(fence["effect_kind"]))
    return SessionEventEnvelope(
        event_id=event_id or str(fence["failure_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.EFFECT_FAILED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=220.0,
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
            "failure_code": failure_code,
            "failure_message": failure_message,
        },
    )


def _review_cancellation_completion(
    aggregate: AgentSessionAggregate,
    *,
    payload_update: dict[str, object] | None = None,
) -> SessionEventEnvelope:
    intent = aggregate.data["effect_control_intents"]["cancel_review_workflow"]
    contract = builtin_effect_contract("cancel_review_workflow")
    payload: dict[str, object] = {
        "effect_id": intent["effect_id"],
        "effect_kind": contract.effect_kind,
        "idempotency_key": intent["idempotency_key"],
        "operation_id": intent["operation_id"],
        "plan_id": intent["plan_id"],
        "active_epoch": intent["active_epoch"],
        "activity_generation": intent["activity_generation"],
        "input_watermark": intent["input_watermark"],
        "input_ledger_sequence": intent["input_ledger_sequence"],
        "attempt_count": 1,
        "contract_version": intent["contract_version"],
        "contract_signature": intent["contract_signature"],
    }
    payload.update(payload_update or {})
    return SessionEventEnvelope(
        event_id=str(intent["completion_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.REVIEW_CANCELLATION_COMPLETED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=210.0,
        causation_id=str(intent["causation_id"]),
        correlation_id=str(intent["operation_id"]),
        trace_id="trace:review-cancellation-completion",
        payload=payload,
    )


def _review_cancellation_failure(
    aggregate: AgentSessionAggregate,
    *,
    payload_update: dict[str, object] | None = None,
) -> SessionEventEnvelope:
    intent = aggregate.data["effect_control_intents"]["cancel_review_workflow"]
    contract = builtin_effect_contract("cancel_review_workflow")
    payload: dict[str, object] = {
        "effect_id": intent["effect_id"],
        "effect_kind": contract.effect_kind,
        "idempotency_key": intent["idempotency_key"],
        "operation_id": intent["operation_id"],
        "plan_id": intent["plan_id"],
        "active_epoch": intent["active_epoch"],
        "activity_generation": intent["activity_generation"],
        "input_watermark": intent["input_watermark"],
        "input_ledger_sequence": intent["input_ledger_sequence"],
        "attempt_count": 5,
        "contract_version": intent["contract_version"],
        "contract_signature": intent["contract_signature"],
        "failure_code": "ReviewCancellationExhausted",
        "failure_message": "review task did not acknowledge cancellation",
    }
    payload.update(payload_update or {})
    return SessionEventEnvelope(
        event_id=str(intent["failure_event_id"]),
        key=_KEY,
        kind=AgentSessionEventKind.EFFECT_FAILED,
        ownership_generation=1,
        source=contract.completion_source,
        occurred_at=210.0,
        causation_id=str(intent["causation_id"]),
        correlation_id=str(intent["operation_id"]),
        trace_id="trace:review-cancellation-failure",
        payload=payload,
    )


def _interrupted_review_waiting_for_cancellation(
) -> tuple[AgentSessionReducer, AgentSessionAggregate]:
    reducer, reviewing = _started_review()
    interrupted = reducer.reduce(
        reviewing,
        _message_event(event_id="message:21", message_log_id=21),
    )
    return reducer, interrupted.aggregate


def _idle_with_plan(*, pending_priority: bool) -> AgentSessionAggregate:
    data: dict[str, object] = {
        "message_watermark": 10,
        "delivery_context": {
            "instance_id": "instance-a",
            "target_session_id": "instance-a:group:room-a",
        },
    }
    if pending_priority:
        data["pending_high_priority_message_log_ids"] = [10]
    return AgentSessionAggregate(
        key=_KEY,
        ownership_generation=1,
        current_plan_id="review-plan-a",
        review_plan_revision=1,
        review_plan={
            "plan_id": "review-plan-a",
            "plan_revision": 1,
            "kind": "planned",
            "applied_delay_seconds": 60.0,
        },
        data=data,
        updated_at=50.0,
    )


def test_active_reply_completion_consumes_snapshot_and_materializes_intent() -> None:
    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[10],
        sequence=1,
        intents=[
            {
                "kind": "send_reply",
                "tool_call_id": "tool-call-a",
                "action_ordinal": 0,
                "payload": {"text": "accepted reply"},
            }
        ],
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "active_reply_completed_waiting_outbound"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.active_reply_operation_id == ""
    assert "operation_fences" not in transition.aggregate.data
    assert "pending_high_priority_message_log_ids" not in transition.aggregate.data
    assert len(transition.operations) == 1
    assert transition.operations[0].status is SessionOperationStatus.COMPLETED
    consumptions = [
        item
        for item in transition.message_ledger_mutations
        if isinstance(item, ConsumeMessageLedgerEntries)
    ]
    assert {item.kind for item in consumptions} == {
        MessageLedgerConsumptionKind.CHAT,
        MessageLedgerConsumptionKind.HIGH_PRIORITY,
    }
    assert all(item.input_ledger_sequence == 1 for item in consumptions)
    assert all(item.explicit_message_log_ids == (10,) for item in consumptions)
    assert len(transition.effects) == 1
    action = transition.effects[0]
    assert action.kind == "send_reply"
    assert action.payload["instance_id"] == "instance-a"
    assert action.payload["target_session_id"] == "instance-a:group:room-a"
    assert action.payload["payload"] == {"text": "accepted reply"}
    assert "ownership_generation" not in action.payload
    assert transition.aggregate.data["pending_outbound_actions"][action.effect_id][
        "status"
    ] == "pending"


def test_pending_outbound_message_waits_then_starts_queued_active_reply() -> None:
    reducer, started = _started_active_reply()
    completed = reducer.reduce(
        started,
        _active_reply_completion(
            started,
            consumed_ids=[10],
            sequence=1,
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "tool-call-a",
                    "action_ordinal": 0,
                    "payload": {"text": "accepted reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in completed.effects if effect.kind == "send_reply")

    deferred_message = reducer.reduce(
        completed.aggregate,
        _message_event(event_id="message:11", message_log_id=11),
    )

    assert deferred_message.disposition == "message_recorded_waiting_outbound"
    assert deferred_message.aggregate.state == AgentSessionState.IDLE
    assert deferred_message.effects == ()
    assert deferred_message.aggregate.data["pending_high_priority_message_log_ids"] == [
        11
    ]

    released = reducer.reduce(
        deferred_message.aggregate,
        _external_action_completion(
            deferred_message.aggregate,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
        ),
    )

    assert released.disposition == "external_actions_completed_active_reply_started"
    assert released.aggregate.state == AgentSessionState.ACTIVE_REPLY
    assert released.aggregate.active_reply_operation_id
    assert [effect.kind for effect in released.effects] == [
        "run_active_reply_workflow"
    ]


def test_active_reply_review_resume_waits_for_its_visible_action() -> None:
    reducer = AgentSessionReducer()
    due = reducer.reduce(
        _idle_with_plan(pending_priority=True),
        _review_due_event(),
    )
    started = _stamp_operation_sequence(
        due.aggregate,
        due.aggregate.active_reply_operation_id,
        1,
    )
    waiting = reducer.reduce(
        started,
        _active_reply_completion(
            started,
            consumed_ids=[10],
            sequence=1,
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "due-tool-a",
                    "action_ordinal": 0,
                    "payload": {"text": "priority reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")

    assert waiting.disposition == "active_reply_completed_waiting_outbound"
    assert waiting.aggregate.state == AgentSessionState.IDLE
    assert waiting.aggregate.active_reply_resume["kind"] == "resume_due_review"
    assert all(effect.kind != "run_review_workflow" for effect in waiting.effects)

    released = reducer.reduce(
        waiting.aggregate,
        _external_action_completion(
            waiting.aggregate,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
        ),
    )

    assert released.disposition == "external_actions_completed_review_resumed"
    assert released.aggregate.state == AgentSessionState.REVIEW
    assert released.aggregate.review_operation_id
    assert released.aggregate.active_reply_resume == {}
    assert [effect.kind for effect in released.effects] == ["run_review_workflow"]


def test_active_reply_completion_with_wrong_ledger_sequence_is_stale() -> None:
    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[10],
        sequence=2,
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "active_reply_completion_stale"
    assert "input_ledger_sequence_changed" in transition.reason
    assert transition.aggregate.state == AgentSessionState.ACTIVE_REPLY
    assert transition.effects == ()
    assert transition.operations == ()
    assert transition.message_ledger_mutations == ()


def test_invalid_active_reply_intent_fails_closed_without_consumption() -> None:
    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[10],
        sequence=1,
        intents=[
            {
                "kind": "send_reply",
                "tool_call_id": "tool-call-a",
                "action_ordinal": 0,
                "payload": {
                    "text": "must not be sent",
                    "idempotency_key": "model-controlled",
                },
            }
        ],
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "active_reply_completion_rejected"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.effects == ()
    assert transition.message_ledger_mutations == ()
    assert "pending_high_priority_message_log_ids" in transition.aggregate.data


def test_active_reply_effect_failure_terminalizes_without_consuming_input() -> None:
    reducer, started = _started_active_reply()
    operation_id = started.active_reply_operation_id

    transition = reducer.reduce(
        started,
        _workflow_effect_failure(started, operation_id=operation_id),
    )

    assert transition.disposition == "active_reply_effect_failed"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.active_reply_operation_id == ""
    assert "operation_fences" not in transition.aggregate.data
    assert transition.aggregate.data["pending_high_priority_message_log_ids"] == [10]
    assert transition.effects == ()
    assert transition.message_ledger_mutations == ()
    operation = transition.operations[0]
    assert operation.status is SessionOperationStatus.FAILED
    assert operation.failure_code == "SyntheticWorkflowFailure"
    assert operation.metadata["failure_event_id"] == started.data[
        "operation_fences"
    ][operation_id]["failure_event_id"]
    failure = operation.metadata["effect_failure"]
    assert failure["event_id"] == started.data["operation_fences"][operation_id][
        "failure_event_id"
    ]
    assert failure["effect_id"] == started.data["operation_fences"][operation_id][
        "effect_id"
    ]


def test_active_reply_effect_failure_defers_interrupted_review_without_launching_it() -> None:
    reducer = AgentSessionReducer()
    due = reducer.reduce(
        _idle_with_plan(pending_priority=True),
        _review_due_event(),
    )
    started = _stamp_operation_sequence(
        due.aggregate,
        due.aggregate.active_reply_operation_id,
        1,
    )

    transition = reducer.reduce(
        started,
        _workflow_effect_failure(
            started,
            operation_id=started.active_reply_operation_id,
        ),
    )

    assert transition.disposition == "active_reply_effect_failed_review_deferred"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.review_operation_id == ""
    assert transition.aggregate.active_reply_operation_id == ""
    assert transition.aggregate.active_reply_resume["kind"] == "resume_due_review"
    assert transition.aggregate.active_reply_resume["status"] == "deferred"
    assert transition.aggregate.data["pending_high_priority_message_log_ids"] == [10]
    assert transition.effects == ()
    assert len(transition.review_schedules) == 1
    assert transition.review_schedules[0].status.value == "scheduled"
    assert transition.review_schedules[0].available_at == 250.0
    assert transition.review_schedule_events[0].event_type == "deferred"
    assert transition.review_schedule_events[0].operation_id == (
        started.active_reply_operation_id
    )


def test_active_reply_effect_failure_requires_its_failure_event_id() -> None:
    reducer, started = _started_active_reply()
    operation_id = started.active_reply_operation_id

    transition = reducer.reduce(
        started,
        _workflow_effect_failure(
            started,
            operation_id=operation_id,
            event_id="wrong-active-reply-failure-event",
        ),
    )

    assert transition.disposition == "active_reply_effect_failure_stale"
    assert "event_id_changed" in transition.reason
    assert transition.aggregate.state == AgentSessionState.ACTIVE_REPLY
    assert transition.aggregate.active_reply_operation_id == operation_id
    assert operation_id in transition.aggregate.data["operation_fences"]
    assert transition.operations == ()


def test_review_interruption_releases_active_reply_only_after_cancellation() -> None:
    reducer, reviewing = _started_review()
    interrupted = reducer.reduce(
        reviewing,
        _message_event(event_id="message:21", message_log_id=21),
    )

    assert interrupted.disposition == (
        "review_interrupted_active_reply_waiting_cancellation"
    )
    assert interrupted.aggregate.state == AgentSessionState.ACTIVE_REPLY
    assert interrupted.aggregate.review_operation_id == ""
    assert interrupted.aggregate.active_reply_operation_id
    assert [effect.kind for effect in interrupted.effects] == [
        "cancel_review_workflow"
    ]
    assert [operation.status for operation in interrupted.operations] == [
        SessionOperationStatus.SUPERSEDED,
        SessionOperationStatus.PENDING,
    ]
    intent = interrupted.aggregate.data["effect_control_intents"][
        "cancel_review_workflow"
    ]
    pending = intent["pending_active_reply"]
    assert intent["status"] == "requested"
    assert pending["operation_id"] == interrupted.aggregate.active_reply_operation_id
    assert pending["message_log_ids"] == [21]
    active_reply_fence = interrupted.aggregate.data["operation_fences"][
        interrupted.aggregate.active_reply_operation_id
    ]
    assert active_reply_fence["source_event_id"] == intent["completion_event_id"]

    waiting = _stamp_operation_sequence(
        interrupted.aggregate,
        interrupted.aggregate.active_reply_operation_id,
        3,
    )
    released = reducer.reduce(
        waiting,
        _review_cancellation_completion(waiting),
    )

    assert released.disposition == (
        "review_cancellation_completed_active_reply_released"
    )
    assert released.aggregate.state == AgentSessionState.ACTIVE_REPLY
    assert released.aggregate.active_reply_operation_id == pending["operation_id"]
    assert released.aggregate.data["effect_control_intents"][
        "cancel_review_workflow"
    ]["status"] == "completed"
    assert [operation.status for operation in released.operations] == [
        SessionOperationStatus.PENDING
    ]
    assert [effect.kind for effect in released.effects] == [
        "run_active_reply_workflow"
    ]
    effect = released.effects[0]
    assert effect.operation_id == pending["operation_id"]
    assert effect.payload["message_log_ids"] == [21]
    assert effect.payload["input_ledger_sequence"] == 3
    assert effect.payload["source_event_id"] == intent["completion_event_id"]

    completed = reducer.reduce(
        released.aggregate,
        _active_reply_completion(
            released.aggregate,
            consumed_ids=[21],
            sequence=3,
        ),
    )

    assert completed.disposition == "active_reply_completed_review_resumed"
    assert completed.aggregate.state == AgentSessionState.REVIEW
    assert completed.aggregate.active_reply_operation_id == ""
    assert completed.aggregate.review_operation_id
    assert [effect.kind for effect in completed.effects] == [
        "run_review_workflow"
    ]


def test_review_cancellation_completion_requires_exact_provenance() -> None:
    reducer, waiting = _interrupted_review_waiting_for_cancellation()
    waiting = _stamp_operation_sequence(
        waiting,
        waiting.active_reply_operation_id,
        3,
    )

    stale = reducer.reduce(
        waiting,
        _review_cancellation_completion(
            waiting,
            payload_update={"input_watermark": 999},
        ),
    )

    assert stale.disposition == "review_cancellation_completion_stale"
    assert "input_watermark_changed" in stale.reason
    assert stale.aggregate.active_reply_operation_id == waiting.active_reply_operation_id
    assert stale.aggregate.data["effect_control_intents"][
        "cancel_review_workflow"
    ]["status"] == "requested"
    assert stale.effects == ()


def test_review_cancellation_effect_failure_blocks_reply_and_reschedules_review() -> None:
    reducer, waiting = _interrupted_review_waiting_for_cancellation()
    waiting = _stamp_operation_sequence(
        waiting,
        waiting.active_reply_operation_id,
        3,
    )
    active_reply_operation_id = waiting.active_reply_operation_id
    cancellation_intent = waiting.data["effect_control_intents"][
        "cancel_review_workflow"
    ]

    blocked = reducer.reduce(
        waiting,
        _review_cancellation_failure(waiting),
    )

    assert blocked.disposition == "review_cancellation_effect_failed_blocked"
    assert blocked.aggregate.state == AgentSessionState.IDLE
    assert blocked.aggregate.active_reply_operation_id == ""
    assert active_reply_operation_id not in blocked.aggregate.data.get(
        "operation_fences", {}
    )
    assert blocked.aggregate.data["pending_high_priority_message_log_ids"] == [21]
    assert blocked.operations[0].operation_id == active_reply_operation_id
    assert blocked.operations[0].status is SessionOperationStatus.FAILED
    assert blocked.operations[0].failure_code == "ReviewCancellationExhausted"
    intent = blocked.aggregate.data["effect_control_intents"][
        "cancel_review_workflow"
    ]
    assert intent["status"] == "failed"
    assert intent["last_failure"]["event_id"] == cancellation_intent[
        "failure_event_id"
    ]
    assert blocked.aggregate.data["outbound_blocked"] == {
        "effect_id": cancellation_intent["effect_id"],
        "failure_code": "ReviewCancellationExhausted",
        "failure_event_id": cancellation_intent["failure_event_id"],
        "kind": "effect_failed",
        "operation_id": active_reply_operation_id,
    }
    assert len(blocked.review_schedules) == 1
    assert blocked.review_schedules[0].status.value == "scheduled"
    assert blocked.review_schedule_events[0].event_type == "deferred"
    assert blocked.result["review_retry_scheduled"] is True

    recorded = reducer.reduce(
        blocked.aggregate,
        _message_event(event_id="message:22", message_log_id=22),
    )

    assert recorded.disposition == "message_recorded_active_reply_blocked"
    assert recorded.effects == ()
    assert recorded.aggregate.data["pending_high_priority_message_log_ids"] == [21, 22]

    retry_due = replace(
        _review_due_event(event_id="review-due-retry"),
        occurred_at=250.0,
    )
    resumed_review = reducer.reduce(recorded.aggregate, retry_due)

    assert resumed_review.disposition == "review_started"
    assert resumed_review.aggregate.state == AgentSessionState.REVIEW
    assert resumed_review.aggregate.active_reply_operation_id == ""
    assert [effect.kind for effect in resumed_review.effects] == [
        "run_review_workflow"
    ]


def test_review_completion_enters_active_chat_and_uses_trusted_action_target() -> None:
    reducer, started = _started_review()
    completion = _review_completion(
        started,
        enter_active_chat=True,
        consumed_ids=[10, 20],
        intents=[
            {
                "kind": "send_reply",
                "tool_call_id": "review-tool-a",
                "action_ordinal": 0,
                "payload": {"text": "review reply"},
            }
        ],
        extra_payload={
            "instance_id": "attacker-instance",
            "target_session_id": "attacker-session",
        },
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "review_completed_active_chat_waiting_outbound"
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert transition.aggregate.active_epoch == 1
    assert transition.aggregate.review_operation_id == ""
    assert transition.aggregate.active_chat_state["bootstrap_status"] == "waiting_outbound"
    assert transition.operations[0].status is SessionOperationStatus.COMPLETED
    assert len(transition.message_ledger_mutations) == 1
    consumption = transition.message_ledger_mutations[0]
    assert isinstance(consumption, ConsumeMessageLedgerEntries)
    assert consumption.kind is MessageLedgerConsumptionKind.REVIEW
    assert consumption.explicit_message_log_ids == (10, 20)
    assert consumption.input_ledger_sequence == 2
    assert transition.review_schedules[0].status.value == "completed"
    action_effects = [
        effect for effect in transition.effects if effect.kind == "send_reply"
    ]
    assert len(action_effects) == 1
    assert action_effects[0].payload["instance_id"] == "instance-a"
    assert action_effects[0].payload["target_session_id"] == "instance-a:group:room-a"
    pending = transition.aggregate.data["pending_outbound_actions"]
    assert pending[action_effects[0].effect_id]["status"] == "pending"


def test_external_action_success_starts_bootstrap_after_exact_receipt() -> None:
    reducer, started = _started_review()
    waiting = reducer.reduce(
        started,
        _review_completion(
            started,
            enter_active_chat=True,
            consumed_ids=[10, 20],
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "review-tool-a",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")

    released = reducer.reduce(
        waiting.aggregate,
        _external_action_completion(
            waiting.aggregate,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
        ),
    )

    assert released.disposition == "external_actions_completed_bootstrap_started"
    assert "pending_outbound_actions" not in released.aggregate.data
    assert released.aggregate.active_chat_state["bootstrap_status"] == "pending"
    assert released.aggregate.active_chat_state["outbound_gate_completed_effect_id"] == (
        action.effect_id
    )
    assert [effect.kind for effect in released.effects] == [
        "run_active_chat_bootstrap"
    ]


def test_external_action_unknown_blocks_bootstrap_without_automatic_release() -> None:
    reducer, started = _started_review()
    waiting = reducer.reduce(
        started,
        _review_completion(
            started,
            enter_active_chat=True,
            consumed_ids=[10, 20],
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "review-tool-a",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")

    blocked = reducer.reduce(
        waiting.aggregate,
        _external_action_completion(
            waiting.aggregate,
            action,
            receipt_status=ExternalActionReceiptStatus.UNKNOWN,
        ),
    )

    assert blocked.disposition == "external_action_terminal_blocked"
    assert blocked.effects == ()
    assert blocked.aggregate.active_chat_state["bootstrap_status"] == "waiting_outbound"
    pending = blocked.aggregate.data["pending_outbound_actions"]
    assert pending[action.effect_id]["status"] == "unknown"
    assert blocked.aggregate.data["outbound_blocked_reason"] == "unknown"


def test_terminal_external_action_effect_failure_blocks_bootstrap_and_late_receipt() -> None:
    reducer, started = _started_review()
    waiting = reducer.reduce(
        started,
        _review_completion(
            started,
            enter_active_chat=True,
            consumed_ids=[10, 20],
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "review-tool-a",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")

    blocked = reducer.reduce(
        waiting.aggregate,
        _external_action_failure(waiting.aggregate, action),
    )

    assert blocked.disposition == "external_action_effect_failed_blocked"
    assert blocked.effects == ()
    assert blocked.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert blocked.aggregate.active_chat_state["bootstrap_status"] == "waiting_outbound"
    pending = blocked.aggregate.data["pending_outbound_actions"]
    assert pending[action.effect_id]["status"] == "effect_failed"
    assert pending[action.effect_id]["effect_failure"] == {
        "action_ordinal": 0,
        "attempt_count": 5,
        "causation_id": waiting.aggregate.data["pending_outbound_actions"][
            action.effect_id
        ]["source_event_id"],
        "contract_signature": action.contract_signature,
        "contract_version": action.contract_version,
        "effect_id": action.effect_id,
        "effect_kind": action.kind,
        "event_id": derived_effect_event_id(
            key=_KEY,
            effect_id=action.effect_id,
            outcome="failed",
        ),
        "failure_code": "ExternalActionRetryRequired",
        "failure_message": "adapter remained unavailable before dispatch",
        "idempotency_key": action.idempotency_key,
        "occurred_at": 210.0,
        "operation_id": action.operation_id,
        "ownership_generation": 1,
        "request_digest": waiting.aggregate.data["pending_outbound_actions"][
            action.effect_id
        ]["request_digest"],
        "source": "effect_executor",
    }
    assert blocked.aggregate.data["outbound_blocked"] == {
        "effect_id": action.effect_id,
        "failure_code": "ExternalActionRetryRequired",
        "failure_event_id": derived_effect_event_id(
            key=_KEY,
            effect_id=action.effect_id,
            outcome="failed",
        ),
        "kind": "effect_failed",
        "operation_id": action.operation_id,
    }

    late = reducer.reduce(
        blocked.aggregate,
        _external_action_completion(
            blocked.aggregate,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
        ),
    )

    assert late.disposition == "external_action_completion_stale"
    assert "effect_failure_recorded" in late.reason
    assert late.effects == ()
    assert late.aggregate.active_chat_state["bootstrap_status"] == "waiting_outbound"
    assert late.aggregate.data["pending_outbound_actions"][action.effect_id][
        "status"
    ] == "effect_failed"


def test_external_action_effect_failure_with_changed_request_digest_is_stale() -> None:
    reducer, started = _started_review()
    waiting = reducer.reduce(
        started,
        _review_completion(
            started,
            enter_active_chat=True,
            consumed_ids=[10, 20],
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "review-tool-a",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")

    stale = reducer.reduce(
        waiting.aggregate,
        _external_action_failure(
            waiting.aggregate,
            action,
            request_digest="0" * 64,
        ),
    )

    assert stale.disposition == "external_action_effect_failure_stale"
    assert "request_digest_changed" in stale.reason
    pending = stale.aggregate.data["pending_outbound_actions"]
    assert pending[action.effect_id]["status"] == "pending"
    assert "effect_failure" not in pending[action.effect_id]


def test_external_action_completion_with_changed_request_digest_is_stale() -> None:
    reducer, started = _started_review()
    waiting = reducer.reduce(
        started,
        _review_completion(
            started,
            enter_active_chat=True,
            consumed_ids=[10, 20],
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "review-tool-a",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")

    stale = reducer.reduce(
        waiting.aggregate,
        _external_action_completion(
            waiting.aggregate,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
            request_digest="0" * 64,
        ),
    )

    assert stale.disposition == "external_action_completion_stale"
    assert "request_digest_changed" in stale.reason
    assert stale.effects == ()
    assert stale.aggregate.active_chat_state["bootstrap_status"] == "waiting_outbound"
    pending = stale.aggregate.data["pending_outbound_actions"]
    assert pending[action.effect_id]["status"] == "pending"


def test_review_completion_returns_idle_with_typed_next_schedule() -> None:
    reducer, started = _started_review()
    completion = _review_completion(
        started,
        enter_active_chat=False,
        consumed_ids=[10, 20],
        next_review_outcome={
            "kind": "planned",
            "requested_delay_seconds": 42.0,
            "reason": "review complete",
        },
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "review_completed_idle_scheduled"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.current_plan_id != "review-plan-a"
    assert transition.aggregate.review_plan_revision == 2
    assert transition.review_schedules[0].applied_delay_seconds == 42.0
    assert transition.review_schedules[0].status.value == "scheduled"
    assert transition.review_schedule_events[0].previous_plan_id == "review-plan-a"


def test_review_effect_failure_terminalizes_with_default_fallback_schedule() -> None:
    reducer, started = _started_review()
    operation_id = started.review_operation_id

    transition = reducer.reduce(
        started,
        _workflow_effect_failure(started, operation_id=operation_id),
    )

    assert transition.disposition == "review_effect_failed_idle_scheduled"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.review_operation_id == ""
    assert operation_id not in transition.aggregate.data.get("operation_fences", {})
    assert transition.effects == ()
    assert transition.message_ledger_mutations == ()
    operation = transition.operations[0]
    assert operation.status is SessionOperationStatus.FAILED
    assert operation.failure_code == "SyntheticWorkflowFailure"
    assert operation.metadata["failure_event_id"] == "review-failure-a"
    assert operation.metadata["effect_failure"]["causation_id"] == "review-due-a"
    schedule = transition.review_schedules[0]
    assert schedule.status.value == "scheduled"
    assert schedule.outcome == "failed"
    assert schedule.applied_delay_seconds == 900.0
    assert schedule.reason == "review_workflow_effect_failed"


def test_review_due_defers_while_prior_visible_action_is_unsettled() -> None:
    reducer, started = _started_review()
    waiting = reducer.reduce(
        started,
        _review_completion(
            started,
            enter_active_chat=False,
            consumed_ids=[10, 20],
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "review-tool-a",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
            next_review_outcome={
                "kind": "planned",
                "requested_delay_seconds": 42.0,
                "reason": "review complete",
            },
        ),
    )
    payload = dict(_review_due_event(event_id="review-due-outbound").payload)
    payload.update(
        {
            "event_id": "review-due-outbound",
            "plan_id": waiting.aggregate.current_plan_id,
            "plan_revision": waiting.aggregate.review_plan_revision,
        }
    )
    due = replace(
        _review_due_event(event_id="review-due-outbound"),
        payload=payload,
    )

    deferred = reducer.reduce(waiting.aggregate, due)

    assert waiting.disposition == "review_completed_idle_waiting_outbound"
    assert deferred.disposition == "review_due_deferred"
    assert deferred.effects == ()
    assert deferred.reason == "outbound_actions_pending"
    assert deferred.review_schedule_events[0].operation_id == (
        next(iter(waiting.aggregate.data["pending_outbound_actions"].values()))[
            "operation_id"
        ]
    )


def test_invalid_review_completion_fails_operation_and_schedules_fallback() -> None:
    reducer, started = _started_review()
    completion = _review_completion(
        started,
        enter_active_chat="yes",
        consumed_ids=[10, 20],
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "review_completed_idle_scheduled"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.operations[0].failure_code == "invalid_review_completion"
    assert transition.effects == ()
    assert transition.message_ledger_mutations == ()
    assert transition.review_schedules[0].outcome == "failed"


def test_review_due_runs_pending_priority_then_resumes_review() -> None:
    reducer = AgentSessionReducer()
    due = reducer.reduce(
        _idle_with_plan(pending_priority=True),
        _review_due_event(),
    )
    assert due.disposition == "review_due_active_reply_started"
    assert due.aggregate.state == AgentSessionState.ACTIVE_REPLY
    assert due.aggregate.active_reply_resume["kind"] == "resume_due_review"
    started = _stamp_operation_sequence(
        due.aggregate,
        due.aggregate.active_reply_operation_id,
        1,
    )

    completed = reducer.reduce(
        started,
        _active_reply_completion(
            started,
            consumed_ids=[10],
            sequence=1,
        ),
    )

    assert completed.disposition == "active_reply_completed_review_resumed"
    assert completed.aggregate.state == AgentSessionState.REVIEW
    assert completed.aggregate.active_reply_operation_id == ""
    assert completed.aggregate.review_operation_id
    assert completed.aggregate.active_reply_resume == {}
    assert len(completed.operations) == 2
    assert completed.operations[0].status is SessionOperationStatus.COMPLETED
    assert completed.operations[1].status is SessionOperationStatus.PENDING
    assert completed.effects[-1].kind == "run_review_workflow"


def test_review_due_without_priority_starts_single_review() -> None:
    transition = AgentSessionReducer().reduce(
        _idle_with_plan(pending_priority=False),
        _review_due_event(),
    )

    assert transition.disposition == "review_started"
    assert transition.aggregate.state == AgentSessionState.REVIEW
    assert len(transition.operations) == 1
    assert transition.operations[0].kind == "review"
    assert len(transition.effects) == 1
