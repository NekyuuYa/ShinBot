"""Workflow-completion tests for the durable Agent session reducer."""

from __future__ import annotations

from dataclasses import replace

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectExecutionContract,
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
from shinbot.agent.runtime.session_actor.review_due_identity import (
    REVIEW_DUE_EVENT_SOURCE,
    review_due_event_id,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


def _persisted_effect_contract(
    effect_kind: str,
    snapshot: dict[str, object],
) -> EffectExecutionContract:
    version = snapshot.get("contract_version")
    return builtin_effect_contract(
        effect_kind,
        version=(
            int(version)
            if isinstance(version, int) and not isinstance(version, bool)
            else 1
        ),
    )


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


def _replace_operation_fence(
    aggregate: AgentSessionAggregate,
    operation_id: str,
    updates: dict[str, object],
) -> AgentSessionAggregate:
    data = dict(aggregate.data)
    registry = dict(data["operation_fences"])
    fence = dict(registry[operation_id])
    fence.update(updates)
    registry[operation_id] = fence
    data["operation_fences"] = registry
    return replace(aggregate, data=data)


def _without_operation_contract_snapshot(
    aggregate: AgentSessionAggregate,
    operation_id: str,
) -> AgentSessionAggregate:
    data = dict(aggregate.data)
    registry = dict(data["operation_fences"])
    fence = dict(registry[operation_id])
    fence.pop("contract_version", None)
    fence.pop("contract_signature", None)
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
    contract = _persisted_effect_contract("run_active_reply_workflow", fence)
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
    contract = _persisted_effect_contract("run_review_workflow", fence)
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


def _review_due_event(
    *,
    key: SessionKey = _KEY,
    plan_id: str = "review-plan-a",
    plan_revision: int = 1,
    delivery_cycle: int = 0,
) -> SessionEventEnvelope:
    event_id = review_due_event_id(
        key=key,
        plan_id=plan_id,
        plan_revision=plan_revision,
        ownership_generation=1,
        delivery_cycle=delivery_cycle,
    )
    payload: dict[str, object] = {
        "version": 1 if delivery_cycle == 0 else 2,
        "event_id": event_id,
        "session_key": {
            "profile_id": key.profile_id,
            "session_id": key.session_id,
        },
        "plan_id": plan_id,
        "plan_revision": plan_revision,
        "ownership_generation": 1,
        "attempt_count": 0,
    }
    if delivery_cycle > 0:
        payload["delivery_cycle"] = delivery_cycle
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind=AgentSessionEventKind.REVIEW_DUE,
        ownership_generation=1,
        source=REVIEW_DUE_EVENT_SOURCE,
        occurred_at=100.0,
        payload=payload,
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
            "receipt_idempotency_key": effect.idempotency_key,
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
    contract = _persisted_effect_contract(str(fence["effect_kind"]), fence)
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


@pytest.mark.parametrize("effect_kind", ["run_active_reply_workflow", "run_review_workflow"])
@pytest.mark.parametrize("failed", [False, True])
def test_legacy_workflow_fence_without_snapshot_accepts_only_v1(
    effect_kind: str,
    failed: bool,
) -> None:
    if effect_kind == "run_active_reply_workflow":
        reducer, aggregate = _started_active_reply()
        operation_id = aggregate.active_reply_operation_id
        aggregate = _without_operation_contract_snapshot(aggregate, operation_id)
        completion = _active_reply_completion(
            aggregate,
            consumed_ids=[10],
            sequence=1,
        )
        stale_disposition = (
            "active_reply_effect_failure_stale"
            if failed
            else "active_reply_completion_stale"
        )
    else:
        reducer, aggregate = _started_review()
        operation_id = aggregate.review_operation_id
        completion = _review_completion(
            aggregate,
            enter_active_chat=False,
            consumed_ids=[10, 20],
            next_review_outcome={
                "kind": "planned",
                "requested_delay_seconds": 42.0,
                "reason": "legacy review complete",
            },
        )
        stale_disposition = (
            "review_effect_failure_stale"
            if failed
            else "review_completion_stale"
        )
    legacy_event = (
        _workflow_effect_failure(aggregate, operation_id=operation_id)
        if failed
        else completion
    )
    legacy_contract = builtin_effect_contract(effect_kind, version=1)
    assert legacy_event.payload["contract_version"] == legacy_contract.version
    assert legacy_event.payload["contract_signature"] == legacy_contract.signature
    current_contract = builtin_effect_contract(effect_kind)
    current_event = replace(
        legacy_event,
        payload={
            **legacy_event.payload,
            "contract_version": current_contract.version,
            "contract_signature": current_contract.signature,
        },
    )

    accepted = reducer.reduce(aggregate, legacy_event)
    rejected = reducer.reduce(aggregate, current_event)

    assert accepted.disposition != stale_disposition
    assert rejected.disposition == stale_disposition
    assert "contract_version_changed" in rejected.reason
    assert "contract_signature_changed" in rejected.reason


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
                "payload": {
                    "text": "accepted reply",
                    "quote_message_log_id": 10,
                },
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
    assert action.payload["payload"] == {
        "text": "accepted reply",
        "quote_message_log_id": 10,
    }
    assert "ownership_generation" not in action.payload
    assert transition.aggregate.data["pending_outbound_actions"][action.effect_id][
        "status"
    ] == "pending"


def test_active_reply_completion_rejects_consumption_outside_its_selection() -> None:
    """A matching completion cannot consume another message below its watermark."""

    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[9],
        sequence=1,
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "active_reply_completion_rejected"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.message_ledger_mutations == ()
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.operations[0].failure_code == "invalid_active_reply_completion"


def test_active_reply_completion_rejects_raw_platform_action_target() -> None:
    """The reducer repeats the durable target fence before materializing I/O."""

    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[10],
        sequence=1,
        intents=[
            {
                "kind": "send_reply",
                "tool_call_id": "raw-platform-target",
                "action_ordinal": 0,
                "payload": {
                    "text": "must not send",
                    "quote_message_id": "platform-elsewhere",
                },
            }
        ],
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "active_reply_completion_rejected"
    assert transition.effects == ()
    assert transition.message_ledger_mutations == ()


def test_active_reply_completion_rejects_a_second_bound_reply() -> None:
    """The reducer repeats the active-reply slice's single-reply action cap."""

    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[10],
        sequence=1,
        intents=[
            {
                "kind": "send_reply",
                "tool_call_id": "first-reply",
                "action_ordinal": 0,
                "payload": {
                    "text": "first",
                    "quote_message_log_id": 10,
                },
            },
            {
                "kind": "send_reply",
                "tool_call_id": "second-reply",
                "action_ordinal": 1,
                "payload": {
                    "text": "second",
                    "quote_message_log_id": 10,
                },
            },
        ],
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "active_reply_completion_rejected"
    assert transition.effects == ()
    assert transition.message_ledger_mutations == ()


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
                    "payload": {
                        "text": "accepted reply",
                        "quote_message_log_id": 10,
                    },
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
    queued_operation_id = released.aggregate.active_reply_operation_id
    queued_fence = released.aggregate.data["operation_fences"][queued_operation_id]
    assert queued_fence["message_log_ids"] == [11]

    queued = _stamp_operation_sequence(released.aggregate, queued_operation_id, 2)
    malformed_completion = _active_reply_completion(
        queued,
        consumed_ids=[10],
        sequence=2,
    )
    rejected = reducer.reduce(queued, malformed_completion)

    assert rejected.disposition == "active_reply_completion_rejected"
    assert rejected.message_ledger_mutations == ()
    assert rejected.aggregate.data["pending_high_priority_message_log_ids"] == [11]


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
                    "payload": {
                        "text": "priority reply",
                        "quote_message_log_id": 10,
                    },
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


@pytest.mark.parametrize(
    ("field_name", "malformed_value", "expected_mismatch"),
    (
        ("contract_version", "2", "contract_version_changed"),
        ("contract_version", 2.9, "contract_version_changed"),
        ("contract_version", True, "contract_version_changed"),
        ("active_epoch", "0", "active_epoch_changed"),
        ("active_epoch", 0.9, "active_epoch_changed"),
        ("activity_generation", False, "activity_generation_changed"),
        ("input_watermark", "10", "input_watermark_changed"),
        ("input_watermark", 10.9, "input_watermark_changed"),
        ("input_ledger_sequence", "1", "input_ledger_sequence_changed"),
        ("input_ledger_sequence", 1.9, "input_ledger_sequence_changed"),
        ("attempt_count", "1", "attempt_count_invalid"),
        ("attempt_count", 1.9, "attempt_count_invalid"),
    ),
)
def test_workflow_completion_integer_fences_reject_type_confusion(
    field_name: str,
    malformed_value: object,
    expected_mismatch: str,
) -> None:
    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[10],
        sequence=1,
    )
    completion = replace(
        completion,
        payload={**completion.payload, field_name: malformed_value},
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "active_reply_completion_stale"
    assert expected_mismatch in transition.reason
    assert transition.aggregate.state == AgentSessionState.ACTIVE_REPLY
    assert transition.aggregate.active_reply_operation_id == started.active_reply_operation_id
    assert transition.effects == ()
    assert transition.operations == ()
    assert transition.message_ledger_mutations == ()


@pytest.mark.parametrize(
    "field_name",
    [
        "effect_id",
        "effect_kind",
        "idempotency_key",
        "operation_id",
        "plan_id",
        "contract_signature",
    ],
)
def test_workflow_completion_text_fences_reject_whitespace_aliases(
    field_name: str,
) -> None:
    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[10],
        sequence=1,
    )
    canonical_value = completion.payload[field_name]
    assert isinstance(canonical_value, str)
    completion = replace(
        completion,
        payload={
            **completion.payload,
            field_name: f" {canonical_value} ",
        },
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "active_reply_completion_stale"
    assert f"{field_name}_changed" in transition.reason
    assert transition.aggregate.active_reply_operation_id == (
        started.active_reply_operation_id
    )
    assert transition.effects == ()
    assert transition.operations == ()


@pytest.mark.parametrize(
    ("field_name", "malformed_value", "supplied_value", "expected_mismatch"),
    (
        ("active_epoch", "0", 0, "expected_active_epoch_invalid"),
        (
            "activity_generation",
            0.0,
            0,
            "expected_activity_generation_invalid",
        ),
        (
            "ownership_generation",
            True,
            None,
            "expected_ownership_generation_invalid",
        ),
        ("effect_id", 1, "1", "expected_effect_id_invalid"),
    ),
)
def test_workflow_completion_rejects_malformed_persisted_authority(
    field_name: str,
    malformed_value: object,
    supplied_value: object,
    expected_mismatch: str,
) -> None:
    reducer, started = _started_active_reply()
    completion = _active_reply_completion(
        started,
        consumed_ids=[10],
        sequence=1,
    )
    operation_id = started.active_reply_operation_id
    malformed = _replace_operation_fence(
        started,
        operation_id,
        {field_name: malformed_value},
    )
    if supplied_value is not None:
        completion = replace(
            completion,
            payload={**completion.payload, field_name: supplied_value},
        )

    transition = reducer.reduce(malformed, completion)

    assert transition.disposition == "active_reply_completion_stale"
    assert expected_mismatch in transition.reason
    assert transition.aggregate.active_reply_operation_id == operation_id
    assert transition.effects == ()
    assert transition.operations == ()


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


def test_legacy_held_active_reply_is_atomically_upgraded_when_released() -> None:
    reducer, waiting = _interrupted_review_waiting_for_cancellation()
    operation_id = waiting.active_reply_operation_id
    waiting = _stamp_operation_sequence(waiting, operation_id, 3)
    data = dict(waiting.data)
    fences = dict(data["operation_fences"])
    legacy_fence = dict(fences[operation_id])
    legacy_fence.pop("contract_version")
    legacy_fence.pop("contract_signature")
    fences[operation_id] = legacy_fence
    data["operation_fences"] = fences
    waiting = replace(waiting, data=data)

    released = reducer.reduce(
        waiting,
        _review_cancellation_completion(waiting),
    )

    contract = builtin_effect_contract("run_active_reply_workflow")
    assert released.effects[0].contract_version == contract.version
    assert released.effects[0].contract_signature == contract.signature
    upgraded_fence = released.aggregate.data["operation_fences"][operation_id]
    assert upgraded_fence["contract_version"] == contract.version
    assert upgraded_fence["contract_signature"] == contract.signature

    completed = reducer.reduce(
        released.aggregate,
        _active_reply_completion(
            released.aggregate,
            consumed_ids=[21],
            sequence=3,
        ),
    )
    assert completed.disposition == "active_reply_completed_review_resumed"


def test_explicit_v1_held_active_reply_keeps_its_contract_when_released() -> None:
    reducer, waiting = _interrupted_review_waiting_for_cancellation()
    operation_id = waiting.active_reply_operation_id
    waiting = _stamp_operation_sequence(waiting, operation_id, 3)
    data = dict(waiting.data)
    fences = dict(data["operation_fences"])
    legacy_fence = dict(fences[operation_id])
    legacy_contract = builtin_effect_contract(
        "run_active_reply_workflow",
        version=1,
    )
    legacy_fence.update(
        {
            "contract_version": legacy_contract.version,
            "contract_signature": legacy_contract.signature,
        }
    )
    fences[operation_id] = legacy_fence
    data["operation_fences"] = fences
    waiting = replace(waiting, data=data)

    released = reducer.reduce(
        waiting,
        _review_cancellation_completion(waiting),
    )

    assert released.disposition == (
        "review_cancellation_completed_active_reply_released"
    )
    assert released.effects[0].contract_version == legacy_contract.version
    assert released.effects[0].contract_signature == legacy_contract.signature
    released_fence = released.aggregate.data["operation_fences"][operation_id]
    assert released_fence["contract_version"] == legacy_contract.version
    assert released_fence["contract_signature"] == legacy_contract.signature

    completed = reducer.reduce(
        released.aggregate,
        _active_reply_completion(
            released.aggregate,
            consumed_ids=[21],
            sequence=3,
        ),
    )
    assert completed.disposition == "active_reply_completed_review_resumed"


@pytest.mark.parametrize(
    ("contract_version", "contract_signature", "expected_mismatch"),
    (
        (1, None, "contract_snapshot_incomplete"),
        (99, "unknown", "contract_version_unknown"),
        (1, "changed", "contract_signature_changed"),
    ),
)
def test_held_active_reply_rejects_invalid_persisted_contract_snapshot(
    contract_version: int,
    contract_signature: str | None,
    expected_mismatch: str,
) -> None:
    reducer, waiting = _interrupted_review_waiting_for_cancellation()
    operation_id = waiting.active_reply_operation_id
    waiting = _stamp_operation_sequence(waiting, operation_id, 3)
    data = dict(waiting.data)
    fences = dict(data["operation_fences"])
    invalid_fence = dict(fences[operation_id])
    invalid_fence["contract_version"] = contract_version
    if contract_signature is None:
        invalid_fence.pop("contract_signature", None)
    else:
        invalid_fence["contract_signature"] = contract_signature
    fences[operation_id] = invalid_fence
    data["operation_fences"] = fences
    waiting = replace(waiting, data=data)

    blocked = reducer.reduce(
        waiting,
        _review_cancellation_completion(waiting),
    )

    assert blocked.disposition == (
        "review_cancellation_completed_active_reply_blocked"
    )
    assert blocked.effects == ()
    assert expected_mismatch in blocked.result["failure"]["failure_message"]


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
        _review_due_event(delivery_cycle=1),
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


@pytest.mark.parametrize("legacy_marker", [None, 99])
def test_legacy_waiting_outbound_state_pins_bootstrap_to_v2(
    legacy_marker: int | None,
) -> None:
    """A persisted pre-v3 receipt gate must not mint a native bootstrap."""

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
                    "tool_call_id": "legacy-waiting-outbound-reply",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")
    active_state = dict(waiting.aggregate.active_chat_state)
    if legacy_marker is None:
        active_state.pop("actor_workflow_contract_version")
    else:
        active_state["actor_workflow_contract_version"] = legacy_marker
    legacy_waiting = replace(waiting.aggregate, active_chat_state=active_state)

    released = reducer.reduce(
        legacy_waiting,
        _external_action_completion(
            legacy_waiting,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
        ),
    )

    bootstrap = next(
        effect
        for effect in released.effects
        if effect.kind == "run_active_chat_bootstrap"
    )
    assert bootstrap.contract_version == 2
    assert bootstrap.contract_signature == builtin_effect_contract(
        "run_active_chat_bootstrap",
        version=2,
    ).signature
    assert "handoff_operation_id" not in bootstrap.payload
    assert "handoff_message_log_ids" not in bootstrap.payload
    assert (
        released.aggregate.active_chat_state["actor_workflow_contract_version"]
        == 2
    )


def test_v3_waiting_outbound_without_a_provable_handoff_downgrades_to_v2() -> None:
    """A partial v3 state must not resume as mixed-version bootstrap/round work."""

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
                    "tool_call_id": "partial-v3-waiting-outbound-reply",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")
    active_state = dict(waiting.aggregate.active_chat_state)
    active_state.pop("bootstrap_handoff_message_log_ids")
    partial_v3_waiting = replace(waiting.aggregate, active_chat_state=active_state)

    released = reducer.reduce(
        partial_v3_waiting,
        _external_action_completion(
            partial_v3_waiting,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
        ),
    )

    bootstrap = next(
        effect
        for effect in released.effects
        if effect.kind == "run_active_chat_bootstrap"
    )
    assert bootstrap.contract_version == 2
    assert "handoff_operation_id" not in bootstrap.payload
    assert "handoff_message_log_ids" not in bootstrap.payload
    assert released.aggregate.active_chat_state["actor_workflow_contract_version"] == 2


def test_bootstrap_handoff_is_equivalent_with_or_without_review_reply_receipt() -> None:
    """A receipt delay cannot widen or otherwise rewrite bootstrap model input."""

    direct_reducer, direct_started = _started_review()
    direct = direct_reducer.reduce(
        direct_started,
        _review_completion(
            direct_started,
            enter_active_chat=True,
            consumed_ids=[10, 20],
        ),
    )
    direct_effect = next(
        effect
        for effect in direct.effects
        if effect.kind == "run_active_chat_bootstrap"
    )

    receipt_reducer, receipt_started = _started_review()
    waiting = receipt_reducer.reduce(
        receipt_started,
        _review_completion(
            receipt_started,
            enter_active_chat=True,
            consumed_ids=[10, 20],
            intents=[
                {
                    "kind": "send_reply",
                    "tool_call_id": "review-handoff-reply",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")
    receipt = receipt_reducer.reduce(
        waiting.aggregate,
        _external_action_completion(
            waiting.aggregate,
            action,
            receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
        ),
    )
    receipt_effect = next(
        effect
        for effect in receipt.effects
        if effect.kind == "run_active_chat_bootstrap"
    )

    fields = {
        "contract_version",
        "contract_signature",
        "handoff_operation_id",
        "handoff_message_log_ids",
        "input_watermark",
        "input_ledger_sequence",
        "active_epoch",
        "activity_generation",
        "instance_id",
        "target_session_id",
    }
    assert {field: direct_effect.payload[field] for field in fields} == {
        field: receipt_effect.payload[field] for field in fields
    }
    assert direct_effect.contract_version == 3
    assert receipt_effect.contract_version == 3
    assert "review_completion" not in direct_effect.payload
    assert "review_completion" not in receipt_effect.payload


def test_legacy_pending_external_action_without_snapshot_accepts_only_v1() -> None:
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
    data = dict(waiting.aggregate.data)
    pending = dict(data["pending_outbound_actions"])
    entry = dict(pending[action.effect_id])
    entry.pop("contract_version")
    entry.pop("contract_signature")
    pending[action.effect_id] = entry
    data["pending_outbound_actions"] = pending
    legacy_aggregate = replace(waiting.aggregate, data=data)
    current_event = _external_action_completion(
        legacy_aggregate,
        action,
        receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
    )
    legacy_contract = builtin_external_action_effect_contract(
        action.kind,
        version=1,
    )
    legacy_event = replace(
        current_event,
        payload={
            **current_event.payload,
            "contract_version": legacy_contract.version,
            "contract_signature": legacy_contract.signature,
        },
    )

    accepted = reducer.reduce(legacy_aggregate, legacy_event)
    rejected = reducer.reduce(legacy_aggregate, current_event)

    assert accepted.disposition == "external_actions_completed_bootstrap_started"
    assert "pending_outbound_actions" not in accepted.aggregate.data
    assert rejected.disposition == "external_action_completion_stale"
    assert "contract_version_changed" in rejected.reason
    assert "contract_signature_changed" in rejected.reason


@pytest.mark.parametrize(
    ("field_name", "expected_mismatch"),
    (
        ("effect_id", "pending_external_action_missing"),
        ("effect_kind", "effect_kind_changed"),
        ("idempotency_key", "idempotency_key_changed"),
        ("operation_id", "operation_id_changed"),
        ("request_digest", "request_digest_changed"),
        ("receipt_idempotency_key", "receipt_idempotency_key_changed"),
        ("contract_signature", "contract_signature_changed"),
        ("receipt_status", "receipt_status_invalid"),
    ),
)
def test_external_action_completion_text_fences_reject_whitespace_aliases(
    field_name: str,
    expected_mismatch: str,
) -> None:
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
                    "tool_call_id": "review-tool-exact-text",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")
    completion = _external_action_completion(
        waiting.aggregate,
        action,
        receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
    )
    canonical_value = completion.payload[field_name]
    assert isinstance(canonical_value, str)
    completion = replace(
        completion,
        payload={
            **completion.payload,
            field_name: f" {canonical_value} ",
        },
    )

    transition = reducer.reduce(waiting.aggregate, completion)

    assert transition.disposition == "external_action_completion_stale"
    assert expected_mismatch in transition.reason
    assert transition.effects == ()
    pending = transition.aggregate.data["pending_outbound_actions"]
    assert pending[action.effect_id]["status"] == "pending"


@pytest.mark.parametrize(
    "malformed_digest",
    [int("1" * 64), 1.0, True],
)
def test_external_action_completion_rejects_malformed_persisted_text_authority(
    malformed_digest: object,
) -> None:
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
                    "tool_call_id": "review-tool-malformed-authority",
                    "action_ordinal": 0,
                    "payload": {"text": "review reply"},
                }
            ],
        ),
    )
    action = next(effect for effect in waiting.effects if effect.kind == "send_reply")
    data = dict(waiting.aggregate.data)
    pending = dict(data["pending_outbound_actions"])
    entry = dict(pending[action.effect_id])
    entry["request_digest"] = malformed_digest
    pending[action.effect_id] = entry
    data["pending_outbound_actions"] = pending
    malformed = replace(waiting.aggregate, data=data)
    completion = _external_action_completion(
        malformed,
        action,
        receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
    )
    completion = replace(
        completion,
        payload={
            **completion.payload,
            "request_digest": str(malformed_digest),
        },
    )

    transition = reducer.reduce(malformed, completion)

    assert transition.disposition == "external_action_completion_stale"
    assert "pending_external_actions_invalid" in transition.reason
    assert transition.effects == ()
    assert transition.aggregate.data["pending_outbound_actions"][action.effect_id][
        "status"
    ] == "pending"


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


def test_v2_external_action_completion_requires_receipt_idempotency_key() -> None:
    """A v2 receipt result cannot settle an action without its receipt identity."""

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
    completion = _external_action_completion(
        waiting.aggregate,
        action,
        receipt_status=ExternalActionReceiptStatus.SUCCEEDED,
    )
    payload = dict(completion.payload)
    payload.pop("receipt_idempotency_key")

    stale = reducer.reduce(
        waiting.aggregate,
        replace(completion, payload=payload),
    )

    assert stale.disposition == "external_action_completion_stale"
    assert "receipt_idempotency_key_missing" in stale.reason
    assert stale.effects == ()
    assert stale.aggregate.data["pending_outbound_actions"][action.effect_id][
        "status"
    ] == "pending"


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
        extra_payload={
            "model_execution_id": "model-execution-review-a",
            "prompt_signature": "prompt-signature-review-a",
        },
    )

    transition = reducer.reduce(started, completion)

    assert transition.disposition == "review_completed_idle_scheduled"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.current_plan_id != "review-plan-a"
    assert transition.aggregate.review_plan_revision == 2
    assert transition.review_schedules[0].applied_delay_seconds == 42.0
    assert transition.review_schedules[0].model_execution_id == (
        "model-execution-review-a"
    )
    assert transition.review_schedules[0].prompt_signature == (
        "prompt-signature-review-a"
    )
    assert transition.review_schedules[0].status.value == "scheduled"
    assert transition.review_schedule_events[0].previous_plan_id == "review-plan-a"


def test_bypassed_review_schedule_preserves_model_provenance() -> None:
    reducer, started = _started_review()
    completion = _review_completion(
        started,
        enter_active_chat=False,
        consumed_ids=[10, 20],
        next_review_outcome={
            "kind": "bypassed",
            "applied_delay_seconds": 30.0,
            "reason": "trusted bypass",
            "fallback_reason": "review_policy_bypass",
        },
        extra_payload={
            "model_execution_id": "model-execution-bypass",
            "prompt_signature": "prompt-signature-bypass",
        },
    )

    transition = reducer.reduce(started, completion)

    schedule = transition.review_schedules[0]
    assert schedule.outcome == "bypassed"
    assert schedule.model_execution_id == "model-execution-bypass"
    assert schedule.prompt_signature == "prompt-signature-bypass"


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
    due = _review_due_event(
        plan_id=waiting.aggregate.current_plan_id,
        plan_revision=waiting.aggregate.review_plan_revision,
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


@pytest.mark.parametrize(
    ("field_name", "malformed_value", "expected_mismatch"),
    (
        ("ownership_generation", "1", "ownership_generation_changed"),
        ("ownership_generation", 1.0, "ownership_generation_changed"),
        ("ownership_generation", True, "ownership_generation_changed"),
        ("plan_revision", "1", "plan_revision_missing"),
        ("plan_revision", 1.0, "plan_revision_missing"),
        ("plan_revision", True, "plan_revision_missing"),
    ),
)
def test_review_due_integer_fences_reject_type_confusion(
    field_name: str,
    malformed_value: object,
    expected_mismatch: str,
) -> None:
    aggregate = _idle_with_plan(pending_priority=False)
    event = _review_due_event()
    event = replace(
        event,
        payload={**event.payload, field_name: malformed_value},
    )

    transition = AgentSessionReducer().reduce(aggregate, event)

    assert transition.disposition == "review_due_superseded"
    assert expected_mismatch in transition.reason
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.effects == ()
    assert transition.operations == ()


@pytest.mark.parametrize(
    "field_name",
    ["event_id", "profile_id", "session_id", "plan_id"],
)
def test_review_due_text_identity_rejects_whitespace_aliases(
    field_name: str,
) -> None:
    aggregate = _idle_with_plan(pending_priority=False)
    event = _review_due_event()
    payload = dict(event.payload)
    if field_name in {"profile_id", "session_id"}:
        session_key = dict(payload["session_key"])
        canonical_value = session_key[field_name]
        assert isinstance(canonical_value, str)
        session_key[field_name] = f" {canonical_value} "
        payload["session_key"] = session_key
    else:
        canonical_value = payload[field_name]
        assert isinstance(canonical_value, str)
        payload[field_name] = f" {canonical_value} "
    event = replace(event, payload=payload)

    transition = AgentSessionReducer().reduce(aggregate, event)

    assert transition.disposition == "review_due_superseded"
    assert f"{field_name}_changed" in transition.reason
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.effects == ()
    assert transition.operations == ()


@pytest.mark.parametrize(
    ("field_name", "expected_mismatch"),
    (
        ("profile_id", "profile_id_changed"),
        ("session_id", "session_id_changed"),
        ("plan_id", "plan_id_missing"),
    ),
)
def test_review_due_identity_rejects_numeric_text_aliases(
    field_name: str,
    expected_mismatch: str,
) -> None:
    key = SessionKey("1", "2")
    plan_id = "3"
    base = _idle_with_plan(pending_priority=False)
    aggregate = replace(
        base,
        key=key,
        current_plan_id=plan_id,
        review_plan={
            **base.review_plan,
            "plan_id": plan_id,
        },
    )
    event = _review_due_event(key=key, plan_id=plan_id)
    payload = dict(event.payload)
    if field_name in {"profile_id", "session_id"}:
        session_key = dict(payload["session_key"])
        session_key[field_name] = int(session_key[field_name])
        payload["session_key"] = session_key
    else:
        payload[field_name] = int(plan_id)
    event = replace(event, payload=payload)

    transition = AgentSessionReducer().reduce(aggregate, event)

    assert transition.disposition == "review_due_superseded"
    assert expected_mismatch in transition.reason
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.effects == ()
    assert transition.operations == ()
