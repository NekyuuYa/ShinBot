"""Tests for the pure active-chat exit session reducer slice."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from dataclasses import replace

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionOperationStatus,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEffectKind,
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    Bypassed,
    Defaulted,
    Failed,
    IdleExitReducerConfig,
    Planned,
)

_KEY = SessionKey("profile-a", "session-a")


def _active_chat() -> AgentSessionAggregate:
    return AgentSessionAggregate(
        key=_KEY,
        ownership_generation=1,
        state=AgentSessionState.ACTIVE_CHAT,
        state_revision=4,
        event_sequence=9,
        active_epoch=3,
        activity_generation=7,
        active_chat_state={"attention": 0.75, "trace": [101, 102]},
        data={
            "message_watermark": 102,
            "unrelated": {"kept": True},
        },
        updated_at=50.0,
    )


def _event(
    kind: AgentSessionEventKind | str,
    *,
    event_id: str,
    payload: dict[str, object] | None = None,
    occurred_at: float = 100.0,
    causation_id: str = "",
    source: str | None = None,
) -> SessionEventEnvelope:
    event_payload = dict(payload or {})
    if str(kind) == AgentSessionEventKind.MESSAGE_RECEIVED:
        message_log_id = event_payload.get("message_log_id", 103)
        canonical_payload: dict[str, object] = {
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": _KEY.profile_id,
                "session_id": _KEY.session_id,
            },
            "bot_id": "profile-a",
            "bot_binding_id": "binding-a",
            "base_session_id": "base-session-a",
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
            "trace_id": "trace-a",
            "observed_at": occurred_at,
            "event_type": "message-created",
        }
        canonical_payload.update(event_payload)
        event_payload = canonical_payload
    effect_event_kinds = {
        AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_RECONCILED,
        AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_STOPPED,
        AgentSessionEventKind.EFFECT_FAILED,
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_CANCELLATION_COMPLETED,
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILED,
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_DEADLINE_REACHED,
    }
    return SessionEventEnvelope(
        event_id=event_id,
        key=_KEY,
        kind=str(kind),
        ownership_generation=1,
        payload=event_payload,
        source=(
            source
            if source is not None
            else (
                "effect_executor"
                if str(kind) in {str(item) for item in effect_event_kinds}
                else "unit-test"
            )
        ),
        occurred_at=occurred_at,
        causation_id=causation_id,
        correlation_id="correlation-a",
        trace_id="trace-a",
        created_at=occurred_at,
    )


def _exit_event() -> SessionEventEnvelope:
    return _event(
        AgentSessionEventKind.EXIT_REQUESTED,
        event_id="exit-1",
        payload={
            "operation_id": "idle-operation-1",
            "plan_id": "review-plan-1",
            "planner_effect_id": "planner-effect-1",
            "planner_failure_event_id": "planner-failure-event-1",
            "deadline_effect_id": "deadline-effect-1",
            "deadline_failure_event_id": "deadline-failure-event-1",
            "completion_event_id": "completion-event-1",
            "deadline_event_id": "deadline-event-1",
            # These untrusted values must not override reducer policy.
            "deadline_at": 999.0,
            "deadline_after_seconds": 777.0,
            "trigger": "attention_decay",
            "planning_input": {"messages": [101, 102]},
            "input_watermark": 102,
        },
    )


def _settling(
    reducer: AgentSessionReducer,
) -> AgentSessionAggregate:
    return reducer.reduce(_active_chat(), _exit_event()).aggregate


def _executor_provenance(
    effect_kind: AgentSessionEffectKind,
    *,
    effect_id: str,
    operation_id: str = "idle-operation-1",
    idempotency_key: str | None = None,
    attempt_count: int = 1,
    contract_version: int | None = None,
) -> dict[str, object]:
    contract = builtin_effect_contract(effect_kind, version=contract_version)
    return {
        "operation_id": operation_id,
        "plan_id": "review-plan-1",
        "active_epoch": 3,
        "activity_generation": 7,
        "input_watermark": 102,
        "input_ledger_sequence": None,
        "effect_id": effect_id,
        "effect_kind": effect_kind,
        "idempotency_key": idempotency_key or effect_id,
        "attempt_count": attempt_count,
        "contract_version": contract.version,
        "contract_signature": contract.signature,
    }


def _completion_payload(
    outcome: dict[str, object],
    **trusted: object,
) -> dict[str, object]:
    payload: dict[str, object] = {
        **_executor_provenance(
            AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
            effect_id="planner-effect-1",
        ),
        "outcome": outcome,
    }
    payload.update(trusted)
    return payload


def _effect_failure_payload(
    effect_kind: AgentSessionEffectKind,
    *,
    effect_id: str,
    idempotency_key: str | None = None,
    contract_version: int | None = None,
) -> dict[str, object]:
    return {
        **_executor_provenance(
            effect_kind,
            effect_id=effect_id,
            idempotency_key=idempotency_key,
            attempt_count=3,
            contract_version=contract_version,
        ),
        "failure_code": "retry_exhausted",
        "failure_message": "durable effect remained unavailable",
    }


def _settled_with_stop_intent(
    reducer: AgentSessionReducer,
) -> AgentSessionAggregate:
    completion = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload(
            {"kind": "planned", "requested_delay_seconds": 30.0},
            stop_effect_id="stop-effect-1",
            stop_completion_event_id="stop-completion-event-1",
            stop_failure_event_id="stop-failure-event-1",
        ),
        occurred_at=120.0,
        causation_id="exit-1",
    )
    return reducer.reduce(_settling(reducer), completion).aggregate


def _cancelled_with_intent(
    reducer: AgentSessionReducer,
) -> AgentSessionAggregate:
    return reducer.reduce(
        _settling(reducer),
        _event(
            AgentSessionEventKind.MESSAGE_RECEIVED,
            event_id="message-103",
            payload={
                "message_log_id": 103,
                "cancel_effect_id": "cancel-effect-1",
                "cancel_completion_event_id": "cancel-completion-event-1",
                "cancel_failure_event_id": "cancel-failure-event-1",
            },
            occurred_at=110.0,
        ),
    ).aggregate


def _failed_stop_reconciliation(
    reducer: AgentSessionReducer,
) -> SessionTransition:
    failure = _event(
        AgentSessionEventKind.EFFECT_FAILED,
        event_id="stop-failure-event-1",
        payload=_effect_failure_payload(
            AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
            effect_id="stop-effect-1",
        ),
        occurred_at=125.0,
        causation_id="completion-event-1",
    )
    return reducer.reduce(_settled_with_stop_intent(reducer), failure)


def _failed_control_reconciliation(
    reducer: AgentSessionReducer,
    control_effect_kind: AgentSessionEffectKind,
) -> SessionTransition:
    if control_effect_kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME:
        return _failed_stop_reconciliation(reducer)
    cancelled = _cancelled_with_intent(reducer)
    return reducer.reduce(
        cancelled,
        _control_effect_event(
            cancelled,
            AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
            failed=True,
        ),
    )


def _reconciliation_event(
    intent: Mapping[str, object],
    *,
    failed: bool = False,
    occurred_at: float = 130.0,
    contract_version: int | None = None,
) -> SessionEventEnvelope:
    effect_kind = AgentSessionEffectKind(str(intent["reconciliation_kind"]))
    persisted_version = intent.get("reconciliation_contract_version")
    resolved_contract_version = contract_version or (
        int(persisted_version)
        if isinstance(persisted_version, int)
        and not isinstance(persisted_version, bool)
        else 1
    )
    payload = _executor_provenance(
        effect_kind,
        effect_id=str(intent["reconciliation_effect_id"]),
        operation_id=str(intent["reconciliation_operation_id"]),
        idempotency_key=str(intent["reconciliation_idempotency_key"]),
        attempt_count=8 if failed else 1,
        contract_version=resolved_contract_version,
    )
    payload.update(
        {
            "plan_id": intent["plan_id"],
            "active_epoch": intent["active_epoch"],
            "activity_generation": intent["activity_generation"],
            "input_watermark": intent["input_watermark"],
            "input_ledger_sequence": intent["input_ledger_sequence"],
        }
    )
    if failed:
        payload.update(
            {
                "failure_code": "reconciliation_unavailable",
                "failure_message": "runtime did not converge",
            }
        )
    return _event(
        (
            AgentSessionEventKind.EFFECT_FAILED
            if failed
            else (
                AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_RECONCILED
                if effect_kind
                == AgentSessionEffectKind.ACTIVE_CHAT_RUNTIME_RECONCILIATION
                else AgentSessionEventKind.IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILED
            )
        ),
        event_id=str(
            intent[
                "reconciliation_failure_event_id"
                if failed
                else "reconciliation_completion_event_id"
            ]
        ),
        payload=payload,
        occurred_at=occurred_at,
        causation_id=str(intent["reconciliation_causation_id"]),
    )


def _control_effect_event(
    aggregate: AgentSessionAggregate,
    effect_kind: AgentSessionEffectKind,
    *,
    failed: bool = False,
    contract_version: int | None = None,
) -> SessionEventEnvelope:
    intent = aggregate.data["effect_control_intents"][effect_kind]
    persisted_version = intent.get("contract_version")
    resolved_version = contract_version or (
        int(persisted_version)
        if isinstance(persisted_version, int)
        and not isinstance(persisted_version, bool)
        else 1
    )
    contract = builtin_effect_contract(effect_kind, version=resolved_version)
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
        "attempt_count": 5 if failed else 1,
        "contract_version": contract.version,
        "contract_signature": contract.signature,
    }
    if failed:
        payload.update(
            {
                "failure_code": "control_effect_failed",
                "failure_message": "control runtime did not converge",
            }
        )
    completion_kind = (
        AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_STOPPED
        if effect_kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
        else AgentSessionEventKind.IDLE_REVIEW_PLANNING_CANCELLATION_COMPLETED
    )
    return _event(
        AgentSessionEventKind.EFFECT_FAILED if failed else completion_kind,
        event_id=str(
            intent["failure_event_id" if failed else "completion_event_id"]
        ),
        payload=payload,
        causation_id=str(intent["causation_id"]),
    )


def _replace_control_intent(
    aggregate: AgentSessionAggregate,
    effect_kind: AgentSessionEffectKind,
    intent: Mapping[str, object],
) -> AgentSessionAggregate:
    data = dict(aggregate.data)
    intents = dict(data["effect_control_intents"])
    intents[effect_kind] = dict(intent)
    data["effect_control_intents"] = intents
    return replace(aggregate, data=data)


def test_exit_request_creates_fenced_operation_and_durable_effects() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(planning_deadline_seconds=25.0)
    )
    aggregate = _active_chat()
    event = _exit_event()

    transition = reducer.reduce(aggregate, event)

    assert not inspect.isawaitable(transition)
    assert transition.disposition == "active_chat_exit_settling"
    assert transition.caused_operation_id == "idle-operation-1"
    assert transition.caused_plan_id == "review-plan-1"
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert transition.aggregate.state_revision == aggregate.state_revision + 1
    assert transition.aggregate.event_sequence == aggregate.event_sequence + 1
    assert transition.aggregate.active_epoch == 3
    assert transition.aggregate.activity_generation == 7
    assert transition.aggregate.active_chat_state == aggregate.active_chat_state
    assert transition.aggregate.data["unrelated"] == {"kept": True}
    assert transition.aggregate.data["idle_exit"] == {
        "operation_id": "idle-operation-1",
        "plan_id": "review-plan-1",
        "active_epoch": 3,
        "activity_generation": 7,
        "ownership_generation": 1,
        "requested_at": 100.0,
        "deadline_delay_seconds": 25.0,
        "trigger": "attention_decay",
        "source": "unit-test",
        "requested_by_event_id": "exit-1",
        "planner_effect_id": "planner-effect-1",
        "planner_idempotency_key": "planner-effect-1",
        "planner_failure_event_id": "planner-failure-event-1",
        "deadline_effect_id": "deadline-effect-1",
        "deadline_idempotency_key": "deadline-effect-1",
        "deadline_failure_event_id": "deadline-failure-event-1",
        "completion_event_id": "completion-event-1",
        "deadline_event_id": "deadline-event-1",
        "input_watermark": 102,
        "planner_contract_version": builtin_effect_contract(
            AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING
        ).version,
        "planner_contract_signature": builtin_effect_contract(
            AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING
        ).signature,
        "deadline_contract_version": builtin_effect_contract(
            AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE
        ).version,
        "deadline_contract_signature": builtin_effect_contract(
            AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE
        ).signature,
    }

    assert len(transition.operations) == 1
    operation = transition.operations[0]
    assert operation.operation_id == "idle-operation-1"
    assert operation.status is SessionOperationStatus.PENDING
    assert operation.active_epoch == 3
    assert operation.activity_generation == 7
    assert operation.input_watermark == 102

    effects = {effect.kind: effect for effect in transition.effects}
    assert set(effects) == {
        AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
        AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
    }
    planner = effects[AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING]
    assert planner.effect_id == "planner-effect-1"
    assert planner.idempotency_key == "planner-effect-1"
    assert planner.operation_id == "idle-operation-1"
    assert planner.available_after_seconds == 0.0
    assert planner.payload["active_epoch"] == 3
    assert planner.payload["activity_generation"] == 7
    assert planner.payload["planning_input"] == {"messages": [101, 102]}
    deadline = effects[
        AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE
    ]
    assert deadline.effect_id == "deadline-effect-1"
    assert deadline.idempotency_key == "deadline-effect-1"
    assert deadline.available_after_seconds == 25.0
    assert deadline.payload["deadline_event_id"] == "deadline-event-1"
    assert deadline.payload["enqueue_only_if_operation_status"] == [
        "pending",
        "running",
    ]
    assert deadline.payload["terminal_operation_disposition"] == "skip"
    assert transition.result["deadline_effect_contract"] == (
        "skip_when_operation_terminal"
    )
    assert transition.review_schedules == ()

    assert reducer.reduce(aggregate, event) == transition


@pytest.mark.parametrize("tampered_prefix", ["planner", "deadline"])
def test_idle_planner_and_deadline_contract_snapshots_are_independent(
    tampered_prefix: str,
) -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    data = dict(settling.data)
    pending = dict(data["idle_exit"])
    pending[f"{tampered_prefix}_contract_signature"] = "0" * 64
    data["idle_exit"] = pending
    tampered = replace(settling, data=data)
    planner_event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload(
            {"kind": "planned", "requested_delay_seconds": 30.0}
        ),
        causation_id="exit-1",
    )
    deadline_event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_DEADLINE_REACHED,
        event_id="deadline-event-1",
        payload=_executor_provenance(
            AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
            effect_id="deadline-effect-1",
        ),
        causation_id="exit-1",
    )
    rejected_event = planner_event if tampered_prefix == "planner" else deadline_event
    accepted_event = deadline_event if tampered_prefix == "planner" else planner_event

    rejected = reducer.reduce(tampered, rejected_event)
    accepted = reducer.reduce(tampered, accepted_event)

    assert rejected.disposition == "superseded"
    assert "expected_contract_signature_changed" in rejected.reason
    assert accepted.disposition == "active_chat_exit_committed"


def test_legacy_idle_exit_without_contract_snapshots_accepts_only_v1() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    data = dict(settling.data)
    pending = dict(data["idle_exit"])
    for prefix in ("planner", "deadline"):
        pending.pop(f"{prefix}_contract_version")
        pending.pop(f"{prefix}_contract_signature")
    data["idle_exit"] = pending
    legacy = replace(settling, data=data)
    legacy_contract = builtin_effect_contract(
        AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
        version=1,
    )
    legacy_payload = _completion_payload(
        {"kind": "planned", "requested_delay_seconds": 30.0}
    )
    legacy_payload.update(
        {
            "contract_version": legacy_contract.version,
            "contract_signature": legacy_contract.signature,
        }
    )
    legacy_event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=legacy_payload,
        causation_id="exit-1",
    )
    current_event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload(
            {"kind": "planned", "requested_delay_seconds": 30.0}
        ),
        causation_id="exit-1",
    )

    accepted = reducer.reduce(legacy, legacy_event)
    rejected = reducer.reduce(legacy, current_event)

    assert accepted.disposition == "active_chat_exit_committed"
    assert rejected.disposition == "superseded"
    assert "contract_version_changed" in rejected.reason
    assert "contract_signature_changed" in rejected.reason


@pytest.mark.parametrize(
    ("serialized", "outcome_type", "outcome_name", "delay", "operation_status"),
    [
        (
            {
                "kind": "planned",
                "requested_delay_seconds": 45.0,
                "applied_delay_seconds": 1.0,
                "reason": "conversation_is_quiet",
            },
            Planned,
            "planned",
            45.0,
            SessionOperationStatus.COMPLETED,
        ),
        (
            {
                "kind": "defaulted",
                "applied_delay_seconds": 1.0,
                "reason": "use_default",
            },
            Defaulted,
            "defaulted",
            75.0,
            SessionOperationStatus.COMPLETED,
        ),
        (
            {
                "kind": "failed",
                "applied_delay_seconds": 1.0,
                "reason": "model_failed",
                "failure_code": "provider_error",
            },
            Failed,
            "failed",
            75.0,
            SessionOperationStatus.FAILED,
        ),
        (
            {
                "kind": "bypassed",
                "applied_delay_seconds": 1.0,
                "reason": "recovery_bypass",
            },
            Bypassed,
            "bypassed",
            75.0,
            SessionOperationStatus.COMPLETED,
        ),
    ],
)
def test_valid_completion_commits_exactly_one_typed_schedule_outcome(
    serialized: dict[str, object],
    outcome_type: type[Planned | Defaulted | Failed | Bypassed],
    outcome_name: str,
    delay: float,
    operation_status: SessionOperationStatus,
) -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(
            default_review_delay_seconds=75.0,
            minimum_review_delay_seconds=5.0,
            maximum_review_delay_seconds=100.0,
        )
    )
    settling = _settling(reducer)
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload(serialized),
        occurred_at=120.0,
        causation_id="exit-1",
    )

    resolved = reducer.resolve_schedule_outcome(event)
    transition = reducer.reduce(settling, event)

    assert isinstance(resolved, outcome_type)
    assert transition.disposition == "active_chat_exit_committed"
    assert transition.caused_operation_id == "idle-operation-1"
    assert transition.caused_plan_id == "review-plan-1"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.idle_planning_operation_id == ""
    assert transition.aggregate.active_chat_state == {}
    assert "idle_exit" not in transition.aggregate.data
    assert transition.aggregate.current_plan_id == "review-plan-1"
    assert transition.aggregate.review_plan_revision == 1
    assert "scheduled_from" not in transition.aggregate.review_plan
    assert "next_review_at" not in transition.aggregate.review_plan

    assert len(transition.review_schedules) == 1
    schedule = transition.review_schedules[0]
    assert schedule.outcome == outcome_name
    assert schedule.applied_delay_seconds == delay
    assert schedule.scheduled_from is None
    assert schedule.next_review_at is None
    assert schedule.expected_active_epoch == 3
    assert schedule.expected_activity_generation == 7
    assert schedule.committed_state_revision == transition.aggregate.state_revision
    assert len(transition.review_schedule_events) == 1
    assert transition.review_schedule_events[0].outcome == outcome_name
    assert transition.review_schedule_events[0].plan_id == "review-plan-1"
    assert len(transition.operations) == 1
    assert transition.operations[0].status is operation_status
    assert transition.operations[0].metadata["idle_exit"]["plan_id"] == (
        "review-plan-1"
    )
    assert transition.operations[0].metadata["deadline_effect_contract"] == (
        "skip_when_operation_terminal"
    )
    assert len(transition.effects) == 1
    assert (
        transition.effects[0].kind
        == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    )


def test_planned_delay_is_bounded_before_the_schedule_commit() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(
            default_review_delay_seconds=20.0,
            minimum_review_delay_seconds=5.0,
            maximum_review_delay_seconds=60.0,
        )
    )
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload(
            {
                "kind": "planned",
                "requested_delay_seconds": 900.0,
                "applied_delay_seconds": 1.0,
                "reason": "model_requested_long_wait",
            }
        ),
        causation_id="exit-1",
    )

    transition = reducer.reduce(_settling(reducer), event)

    assert transition.review_schedules[0].requested_delay_seconds == 900.0
    assert transition.review_schedules[0].applied_delay_seconds == 60.0


def test_only_trusted_bypass_completion_can_supply_an_explicit_delay() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(default_review_delay_seconds=75.0)
    )
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload(
            {
                "kind": "bypassed",
                "applied_delay_seconds": 1.0,
                "reason": "manual_recovery",
            },
            bypass_delay_seconds=15.0,
        ),
        causation_id="exit-1",
    )

    transition = reducer.reduce(_settling(reducer), event)

    assert transition.review_schedules[0].outcome == "bypassed"
    assert transition.review_schedules[0].applied_delay_seconds == 15.0


def test_nested_model_outcome_cannot_override_execution_provenance() -> None:
    reducer = AgentSessionReducer()
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload(
            {
                "kind": "planned",
                "requested_delay_seconds": 20.0,
                "reason": "model_reason_is_allowed",
                "model_execution_id": "forged-execution",
                "prompt_signature": "forged-signature",
                "source": "forged-source",
            },
            model_execution_id="trusted-execution",
            prompt_signature="trusted-signature",
            source="trusted-effect-runner",
        ),
        causation_id="exit-1",
    )

    transition = reducer.reduce(_settling(reducer), event)
    schedule = transition.review_schedules[0]

    assert schedule.reason == "model_reason_is_allowed"
    assert schedule.model_execution_id == "trusted-execution"
    assert schedule.prompt_signature == "trusted-signature"
    assert schedule.source == "trusted-effect-runner"


def test_deadline_settles_with_explicit_failed_fallback_and_stops_runtime() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(default_review_delay_seconds=80.0)
    )
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_DEADLINE_REACHED,
        event_id="deadline-event-1",
        payload=_executor_provenance(
            AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
            effect_id="deadline-effect-1",
        ),
        occurred_at=125.0,
        causation_id="exit-1",
    )

    transition = reducer.reduce(_settling(reducer), event)

    assert transition.aggregate.state == AgentSessionState.IDLE
    assert len(transition.review_schedules) == 1
    schedule = transition.review_schedules[0]
    assert schedule.outcome == "failed"
    assert schedule.applied_delay_seconds == 80.0
    assert schedule.fallback_reason == "planner_deadline_reached"
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.operations[0].failure_code == "planner_deadline_reached"
    assert transition.effects[0].kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME


@pytest.mark.parametrize(
    ("event_id", "causation_id", "expected_reason"),
    [
        ("other-deadline", "exit-1", "event_id_changed"),
        ("deadline-event-1", "other-exit", "causation_id_changed"),
    ],
)
def test_deadline_requires_expected_event_provenance(
    event_id: str,
    causation_id: str,
    expected_reason: str,
) -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_DEADLINE_REACHED,
        event_id=event_id,
        payload=_executor_provenance(
            AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
            effect_id="deadline-effect-1",
        ),
        occurred_at=125.0,
        causation_id=causation_id,
    )

    transition = reducer.reduce(settling, event)

    assert transition.disposition == "superseded"
    assert expected_reason in transition.reason
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert transition.review_schedules == ()


@pytest.mark.parametrize(
    ("payload_update", "expected_reason"),
    [
        ({"operation_id": "other-operation"}, "operation_id_changed"),
        ({"active_epoch": 99}, "active_epoch_changed"),
        ({"activity_generation": 99}, "activity_generation_changed"),
        ({"plan_id": "other-plan"}, "plan_id_changed"),
        ({"plan_id": ""}, "plan_id_changed"),
    ],
)
def test_stale_completion_only_appends_superseded_journal(
    payload_update: dict[str, object],
    expected_reason: str,
) -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    payload = _completion_payload(
        {
            "kind": "planned",
            "requested_delay_seconds": 30.0,
            "reason": "stale",
        }
    )
    payload.update(payload_update)
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=payload,
        occurred_at=settling.updated_at,
        causation_id="exit-1",
    )

    transition = reducer.reduce(settling, event)

    assert transition.disposition == "superseded"
    assert expected_reason in transition.reason
    assert transition.aggregate.state == settling.state
    assert transition.aggregate.state_revision == settling.state_revision
    assert transition.aggregate.event_sequence == settling.event_sequence + 1
    assert transition.aggregate.idle_planning_operation_id == (
        settling.idle_planning_operation_id
    )
    assert transition.effects == ()
    assert transition.operations == ()
    assert transition.review_schedules == ()
    assert len(transition.review_schedule_events) == 1
    journal = transition.review_schedule_events[0]
    assert journal.event_type == "superseded"
    assert journal.outcome == "superseded"


@pytest.mark.parametrize(
    (
        "authority_location",
        "field_name",
        "malformed_value",
        "supplied_value",
        "expected_reason",
    ),
    (
        (
            "idle_exit",
            "active_epoch",
            "3",
            3,
            "expected_active_epoch_invalid",
        ),
        (
            "idle_exit",
            "activity_generation",
            7.0,
            7,
            "expected_activity_generation_invalid",
        ),
        (
            "operation_fence",
            "input_watermark",
            True,
            None,
            "expected_input_watermark_invalid",
        ),
        (
            "idle_exit",
            "planner_effect_id",
            1,
            "1",
            "expected_effect_id_invalid",
        ),
    ),
)
def test_idle_planning_completion_rejects_malformed_persisted_authority(
    authority_location: str,
    field_name: str,
    malformed_value: object,
    supplied_value: object,
    expected_reason: str,
) -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    data = dict(settling.data)
    if authority_location == "idle_exit":
        pending = dict(data["idle_exit"])
        pending[field_name] = malformed_value
        data["idle_exit"] = pending
    else:
        registry = dict(data["operation_fences"])
        fence = dict(registry[settling.idle_planning_operation_id])
        fence[field_name] = malformed_value
        registry[settling.idle_planning_operation_id] = fence
        data["operation_fences"] = registry
    malformed = replace(settling, data=data)
    payload = _completion_payload(
        {"kind": "planned", "requested_delay_seconds": 30.0}
    )
    completion_field = (
        "effect_id" if field_name == "planner_effect_id" else field_name
    )
    payload[completion_field] = supplied_value
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=payload,
        occurred_at=120.0,
        causation_id="exit-1",
    )

    transition = reducer.reduce(malformed, event)

    assert transition.disposition == "superseded"
    assert expected_reason in transition.reason
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert transition.effects == ()
    assert transition.review_schedules == ()


@pytest.mark.parametrize(
    ("event_id", "causation_id", "expected_reason"),
    [
        ("forged-completion", "exit-1", "event_id_changed"),
        ("completion-event-1", "other-exit", "causation_id_changed"),
        ("completion-event-1", "", "causation_id_missing"),
    ],
)
def test_completion_requires_expected_event_provenance(
    event_id: str,
    causation_id: str,
    expected_reason: str,
) -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id=event_id,
        payload=_completion_payload(
            {
                "kind": "planned",
                "requested_delay_seconds": 30.0,
            }
        ),
        occurred_at=120.0,
        causation_id=causation_id,
    )

    transition = reducer.reduce(settling, event)

    assert transition.disposition == "superseded"
    assert expected_reason in transition.reason
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert transition.review_schedules == ()


@pytest.mark.parametrize(
    "missing_field",
    [
        "effect_id",
        "effect_kind",
        "idempotency_key",
        "operation_id",
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "attempt_count",
        "contract_version",
        "contract_signature",
    ],
)
def test_completion_missing_required_provenance_is_superseded(
    missing_field: str,
) -> None:
    reducer = AgentSessionReducer()
    payload = _completion_payload(
        {"kind": "planned", "requested_delay_seconds": 30.0}
    )
    payload.pop(missing_field)
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=payload,
        occurred_at=120.0,
        causation_id="exit-1",
    )

    transition = reducer.reduce(_settling(reducer), event)

    assert transition.disposition == "superseded"
    assert f"{missing_field}_missing" in transition.reason
    assert transition.review_schedules == ()


def test_completion_requires_executor_source_and_ownership_generation() -> None:
    reducer = AgentSessionReducer()
    payload = _completion_payload(
        {"kind": "planned", "requested_delay_seconds": 30.0}
    )
    wrong_source = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=payload,
        causation_id="exit-1",
        source="untrusted-workflow",
    )
    wrong_generation = SessionEventEnvelope(
        event_id="completion-event-1",
        key=_KEY,
        kind=AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        ownership_generation=2,
        payload=payload,
        source="effect_executor",
        occurred_at=120.0,
        causation_id="exit-1",
    )

    source_transition = reducer.reduce(_settling(reducer), wrong_source)
    generation_transition = reducer.reduce(_settling(reducer), wrong_generation)

    assert "source_changed" in source_transition.reason
    assert "ownership_generation_changed" in generation_transition.reason
    assert source_transition.review_schedules == ()
    assert generation_transition.review_schedules == ()


def test_message_while_settling_cancels_exit_and_supersedes_operation() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    event = _event(
        AgentSessionEventKind.MESSAGE_RECEIVED,
        event_id="message-103",
        payload={"message_log_id": 103, "cancel_effect_id": "cancel-effect-1"},
        occurred_at=110.0,
    )

    transition = reducer.reduce(settling, event)

    assert transition.disposition == "active_chat_exit_cancelled"
    assert transition.caused_operation_id == "idle-operation-1"
    assert transition.caused_plan_id == "review-plan-1"
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert transition.aggregate.activity_generation == 8
    assert transition.aggregate.active_epoch == 3
    assert transition.aggregate.active_chat_state == settling.active_chat_state
    assert transition.aggregate.idle_planning_operation_id == ""
    assert "idle_exit" not in transition.aggregate.data
    cancellation_intent = transition.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
    ]
    assert cancellation_intent["desired_state"] == "cancelled"
    assert cancellation_intent["status"] == "requested"
    assert cancellation_intent["effect_id"] == "cancel-effect-1"
    assert cancellation_intent["causation_id"] == "message-103"
    cancel_contract = builtin_effect_contract(
        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
    )
    assert cancellation_intent["contract_version"] == cancel_contract.version
    assert cancellation_intent["contract_signature"] == cancel_contract.signature
    assert len(transition.operations) == 1
    assert transition.operations[0].status is SessionOperationStatus.SUPERSEDED
    assert transition.operations[0].superseded_at == 110.0
    assert (
        transition.operations[0].metadata["idle_exit"]["deadline_delay_seconds"]
        == 30.0
    )
    assert len(transition.effects) == 1
    assert transition.effects[0].effect_id == "cancel-effect-1"
    assert transition.effects[0].idempotency_key == "cancel-effect-1"
    assert (
        transition.effects[0].kind
        == AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
    )
    assert transition.review_schedules == ()
    assert len(transition.review_schedule_events) == 1
    assert transition.review_schedule_events[0].outcome == "superseded"
    assert len(transition.message_ledger_mutations) == 1
    assert transition.message_ledger_mutations[0].message_log_id == 103


def test_late_old_message_is_recorded_without_cancelling_settlement() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    event = _event(
        AgentSessionEventKind.MESSAGE_RECEIVED,
        event_id="message-101-late",
        payload={"message_log_id": 101},
        occurred_at=110.0,
    )

    transition = reducer.reduce(settling, event)

    assert transition.disposition == "late_snapshot_message_recorded"
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert transition.aggregate.activity_generation == settling.activity_generation
    assert transition.aggregate.idle_planning_operation_id == "idle-operation-1"
    assert len(transition.message_ledger_mutations) == 1
    assert transition.message_ledger_mutations[0].message_log_id == 101


def test_self_message_is_suppressed_without_cancelling_settlement() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    event = _event(
        AgentSessionEventKind.MESSAGE_RECEIVED,
        event_id="message-self",
        payload={"message_log_id": 104, "sender_id": "bot-a"},
        occurred_at=110.0,
    )

    transition = reducer.reduce(settling, event)

    assert transition.disposition == "message_recorded_suppressed"
    assert transition.reason == "self_message"
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert transition.aggregate.activity_generation == settling.activity_generation
    assert transition.aggregate.idle_planning_operation_id == "idle-operation-1"
    appended = transition.message_ledger_mutations[0]
    assert appended.is_self_message is True
    assert appended.eligible_for_work is False
    assert appended.suppression_reason == "self_message"


def test_completion_after_message_cancellation_cannot_return_session_to_idle() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    cancelled = reducer.reduce(
        settling,
        _event(
            AgentSessionEventKind.MESSAGE_RECEIVED,
            event_id="message-103",
            occurred_at=110.0,
        ),
    ).aggregate
    completion = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="late-completion",
        payload=_completion_payload(
            {
                "kind": "planned",
                "requested_delay_seconds": 30.0,
                "reason": "too_late",
            }
        ),
        occurred_at=120.0,
    )

    transition = reducer.reduce(cancelled, completion)

    assert transition.disposition == "superseded"
    assert "state_changed" in transition.reason
    assert "operation_id_changed" in transition.reason
    assert "activity_generation_changed" in transition.reason
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert transition.aggregate.activity_generation == 8
    assert transition.aggregate.state_revision == cancelled.state_revision
    assert transition.review_schedules == ()
    assert transition.review_schedule_events[0].outcome == "superseded"


def test_deadline_winner_fences_late_planner_completion() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    deadline = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_DEADLINE_REACHED,
        event_id="deadline-event-1",
        payload=_executor_provenance(
            AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
            effect_id="deadline-effect-1",
        ),
        occurred_at=125.0,
        causation_id="exit-1",
    )
    committed = reducer.reduce(settling, deadline)
    late_completion = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="late-planner-completion",
        payload=_completion_payload(
            {
                "kind": "planned",
                "requested_delay_seconds": 10.0,
                "reason": "late_model_result",
            }
        ),
        occurred_at=130.0,
    )

    stale = reducer.reduce(committed.aggregate, late_completion)

    assert len(committed.review_schedules) == 1
    assert committed.review_schedules[0].outcome == "failed"
    assert stale.disposition == "superseded"
    assert stale.review_schedules == ()
    assert stale.aggregate.current_plan_id == committed.aggregate.current_plan_id
    assert stale.aggregate.review_plan_revision == 1
    assert stale.aggregate.state_revision == committed.aggregate.state_revision


def test_malformed_planner_result_fails_closed_but_still_settles_exit() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(default_review_delay_seconds=55.0)
    )
    event = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload({"kind": "unknown-model-answer"}),
        causation_id="exit-1",
    )

    transition = reducer.reduce(_settling(reducer), event)

    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.review_schedules[0].outcome == "failed"
    assert transition.review_schedules[0].applied_delay_seconds == 55.0
    assert transition.review_schedules[0].fallback_reason == "invalid_planner_outcome"
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.operations[0].failure_code == "invalid_planner_outcome"


def test_idle_planner_effect_failure_settles_with_failed_default_schedule() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(default_review_delay_seconds=65.0)
    )
    event = _event(
        AgentSessionEventKind.EFFECT_FAILED,
        event_id="planner-failure-event-1",
        payload={
            **_effect_failure_payload(
                AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
                effect_id="planner-effect-1",
            ),
            "failure_message": "model route remained unavailable",
        },
        occurred_at=121.0,
        causation_id="exit-1",
    )

    transition = reducer.reduce(_settling(reducer), event)

    assert transition.disposition == "active_chat_exit_committed"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert len(transition.review_schedules) == 1
    schedule = transition.review_schedules[0]
    assert schedule.outcome == "failed"
    assert schedule.applied_delay_seconds == 65.0
    assert schedule.reason == "idle_review_planning_effect_failed"
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.operations[0].failure_code == "retry_exhausted"
    assert transition.operations[0].failure_message == (
        "model route remained unavailable"
    )
    assert transition.effects[0].kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME


@pytest.mark.parametrize(
    ("payload_update", "expected_reason"),
    [
        ({"effect_id": "other-planner-effect"}, "effect_id_changed"),
        ({"idempotency_key": "other-idempotency-key"}, "idempotency_key_changed"),
    ],
)
def test_planner_effect_failure_requires_exact_effect_identity(
    payload_update: dict[str, object],
    expected_reason: str,
) -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    payload = _effect_failure_payload(
        AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
        effect_id="planner-effect-1",
    )
    payload.update(payload_update)
    event = _event(
        AgentSessionEventKind.EFFECT_FAILED,
        event_id="planner-failure-event-1",
        payload=payload,
        occurred_at=121.0,
        causation_id="exit-1",
    )

    transition = reducer.reduce(settling, event)

    assert transition.disposition == "superseded"
    assert expected_reason in transition.reason
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert transition.review_schedules == ()


def test_deadline_effect_failure_settles_with_failed_fallback_schedule() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(default_review_delay_seconds=85.0)
    )
    event = _event(
        AgentSessionEventKind.EFFECT_FAILED,
        event_id="deadline-failure-event-1",
        payload=_effect_failure_payload(
            AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
            effect_id="deadline-effect-1",
        ),
        occurred_at=121.0,
        causation_id="exit-1",
    )

    transition = reducer.reduce(_settling(reducer), event)

    assert transition.disposition == "active_chat_exit_committed"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert len(transition.review_schedules) == 1
    schedule = transition.review_schedules[0]
    assert schedule.outcome == "failed"
    assert schedule.applied_delay_seconds == 85.0
    assert schedule.reason == "idle_review_planning_deadline_effect_failed"
    assert schedule.fallback_reason == "planner_deadline_effect_failed"
    assert transition.operations[0].status is SessionOperationStatus.FAILED
    assert transition.operations[0].failure_code == "retry_exhausted"
    assert transition.effects[0].kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME


def test_late_idle_planner_effect_failure_is_superseded_after_message_cancel() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    cancelled = reducer.reduce(
        settling,
        _event(
            AgentSessionEventKind.MESSAGE_RECEIVED,
            event_id="message-cancel-before-effect-failure",
            occurred_at=115.0,
        ),
    ).aggregate
    event = _event(
        AgentSessionEventKind.EFFECT_FAILED,
        event_id="planner-failure-event-1",
        payload={
            **_effect_failure_payload(
                AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
                effect_id="planner-effect-1",
            ),
            "failure_message": "late failure",
        },
        occurred_at=120.0,
        causation_id="exit-1",
    )

    transition = reducer.reduce(cancelled, event)

    assert transition.disposition == "superseded"
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert transition.aggregate.state_revision == cancelled.state_revision
    assert transition.review_schedules == ()
    assert transition.review_schedule_events[0].outcome == "superseded"


def test_stop_effect_failure_records_idle_runtime_reconciliation_intent() -> None:
    reducer = AgentSessionReducer()
    completion = _event(
        AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        event_id="completion-event-1",
        payload=_completion_payload(
            {
                "kind": "planned",
                "requested_delay_seconds": 30.0,
            },
            stop_effect_id="stop-effect-1",
            stop_completion_event_id="stop-completion-event-1",
            stop_failure_event_id="stop-failure-event-1",
        ),
        occurred_at=120.0,
        causation_id="exit-1",
    )
    settled = reducer.reduce(_settling(reducer), completion).aggregate
    failure = _event(
        AgentSessionEventKind.EFFECT_FAILED,
        event_id="stop-failure-event-1",
        payload=_effect_failure_payload(
            AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
            effect_id="stop-effect-1",
        ),
        occurred_at=125.0,
        causation_id="completion-event-1",
    )

    transition = reducer.reduce(settled, failure)

    assert transition.disposition == "active_chat_runtime_reconciliation_required"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.current_plan_id == "review-plan-1"
    assert transition.aggregate.review_plan_revision == 1
    assert transition.review_schedules == ()
    assert transition.review_schedule_events == ()
    assert len(transition.operations) == 1
    reconciliation = transition.operations[0]
    assert reconciliation.kind == "active_chat_runtime_reconciliation"
    assert reconciliation.status is SessionOperationStatus.PENDING
    intent = transition.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]
    assert intent["desired_state"] == "stopped"
    assert intent["status"] == "reconciliation_requested"
    assert intent["reconciliation_operation_id"] == reconciliation.operation_id
    assert intent["last_failure"]["effect_id"] == "stop-effect-1"
    assert len(transition.effects) == 1
    assert (
        transition.effects[0].kind
        == AgentSessionEffectKind.ACTIVE_CHAT_RUNTIME_RECONCILIATION
    )
    assert intent["reconciliation_effect_id"] == transition.effects[0].effect_id
    reconciliation_contract = builtin_effect_contract(
        AgentSessionEffectKind.ACTIVE_CHAT_RUNTIME_RECONCILIATION
    )
    assert intent["reconciliation_contract_version"] == (
        reconciliation_contract.version
    )
    assert intent["reconciliation_contract_signature"] == (
        reconciliation_contract.signature
    )
    assert reconciliation.operation_id in transition.aggregate.data[
        "operation_fences"
    ]
    assert reducer.reduce(settled, failure) == transition


def test_cancel_effect_failure_records_reconciliation_without_settling_idle() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    message = _event(
        AgentSessionEventKind.MESSAGE_RECEIVED,
        event_id="message-103",
        payload={
            "message_log_id": 103,
            "cancel_effect_id": "cancel-effect-1",
            "cancel_completion_event_id": "cancel-completion-event-1",
            "cancel_failure_event_id": "cancel-failure-event-1",
        },
        occurred_at=110.0,
    )
    cancelled = reducer.reduce(settling, message).aggregate
    failure = _event(
        AgentSessionEventKind.EFFECT_FAILED,
        event_id="cancel-failure-event-1",
        payload=_effect_failure_payload(
            AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
            effect_id="cancel-effect-1",
        ),
        occurred_at=120.0,
        causation_id="message-103",
    )

    transition = reducer.reduce(cancelled, failure)

    assert transition.disposition == (
        "idle_planning_cancellation_reconciliation_required"
    )
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT
    assert transition.aggregate.activity_generation == 8
    assert transition.aggregate.current_plan_id == ""
    assert transition.review_schedules == ()
    assert transition.review_schedule_events == ()
    assert len(transition.operations) == 1
    reconciliation = transition.operations[0]
    assert reconciliation.kind == (
        "idle_review_planning_cancellation_reconciliation"
    )
    assert reconciliation.status is SessionOperationStatus.PENDING
    intent = transition.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
    ]
    assert intent["desired_state"] == "cancelled"
    assert intent["status"] == "reconciliation_requested"
    assert intent["reconciliation_operation_id"] == reconciliation.operation_id
    assert intent["last_failure"]["effect_id"] == "cancel-effect-1"
    assert len(transition.effects) == 1
    assert (
        transition.effects[0].kind
        == AgentSessionEffectKind.IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILIATION
    )
    reconciliation_contract = builtin_effect_contract(
        AgentSessionEffectKind.IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILIATION
    )
    assert intent["reconciliation_contract_version"] == (
        reconciliation_contract.version
    )
    assert intent["reconciliation_contract_signature"] == (
        reconciliation_contract.signature
    )
    assert reconciliation.operation_id in transition.aggregate.data[
        "operation_fences"
    ]


def test_control_completion_closes_requested_intent() -> None:
    reducer = AgentSessionReducer()
    settled = _settled_with_stop_intent(reducer)
    event = _event(
        AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_STOPPED,
        event_id="stop-completion-event-1",
        payload=_executor_provenance(
            AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
            effect_id="stop-effect-1",
        ),
        occurred_at=125.0,
        causation_id="completion-event-1",
    )

    transition = reducer.reduce(settled, event)

    assert transition.disposition == "active_chat_runtime_stopped"
    intent = transition.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]
    assert intent["status"] == "completed"
    assert intent["completion"]["effect_id"] == "stop-effect-1"
    assert transition.effects == ()


def test_idle_planning_cancellation_completion_closes_requested_intent() -> None:
    reducer = AgentSessionReducer()
    cancelled = _cancelled_with_intent(reducer)

    transition = reducer.reduce(
        cancelled,
        _control_effect_event(
            cancelled,
            AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
        ),
    )

    assert transition.disposition == "idle_planning_cancellation_completed"
    intent = transition.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
    ]
    assert intent["status"] == "completed"
    assert intent["completion"]["effect_id"] == "cancel-effect-1"


@pytest.mark.parametrize(
    "effect_kind",
    [
        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
    ],
)
@pytest.mark.parametrize("failed", [False, True])
def test_legacy_control_intent_without_snapshot_accepts_only_v1(
    effect_kind: AgentSessionEffectKind,
    failed: bool,
) -> None:
    reducer = AgentSessionReducer()
    aggregate = (
        _settled_with_stop_intent(reducer)
        if effect_kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
        else _cancelled_with_intent(reducer)
    )
    intent = dict(aggregate.data["effect_control_intents"][effect_kind])
    intent.pop("contract_version")
    intent.pop("contract_signature")
    legacy = _replace_control_intent(aggregate, effect_kind, intent)

    accepted = reducer.reduce(
        legacy,
        _control_effect_event(legacy, effect_kind, failed=failed),
    )
    rejected = reducer.reduce(
        legacy,
        _control_effect_event(
            legacy,
            effect_kind,
            failed=failed,
            contract_version=2,
        ),
    )

    if failed:
        assert accepted.disposition in {
            "active_chat_runtime_reconciliation_required",
            "idle_planning_cancellation_reconciliation_required",
        }
        assert rejected.disposition == "ignored_unrelated_control_effect_failure"
    else:
        assert accepted.disposition in {
            "active_chat_runtime_stopped",
            "idle_planning_cancellation_completed",
        }
        assert rejected.disposition == "ignored_unrelated_control_effect_completion"
    assert "contract_version_changed" in rejected.reason
    assert "contract_signature_changed" in rejected.reason


@pytest.mark.parametrize(
    "effect_kind",
    [
        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
    ],
)
@pytest.mark.parametrize("failed", [False, True])
def test_control_intent_rejects_a_mutated_persisted_contract_snapshot(
    effect_kind: AgentSessionEffectKind,
    failed: bool,
) -> None:
    reducer = AgentSessionReducer()
    aggregate = (
        _settled_with_stop_intent(reducer)
        if effect_kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
        else _cancelled_with_intent(reducer)
    )
    intent = dict(aggregate.data["effect_control_intents"][effect_kind])
    intent["contract_signature"] = "0" * 64
    tampered = _replace_control_intent(aggregate, effect_kind, intent)

    transition = reducer.reduce(
        tampered,
        _control_effect_event(tampered, effect_kind, failed=failed),
    )

    assert transition.disposition == (
        "ignored_unrelated_control_effect_failure"
        if failed
        else "ignored_unrelated_control_effect_completion"
    )
    assert "expected_contract_signature_changed" in transition.reason


def test_reconciliation_completion_is_fenced_and_completes_operation() -> None:
    reducer = AgentSessionReducer()
    requested = _failed_stop_reconciliation(reducer)
    intent = requested.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]

    transition = reducer.reduce(
        requested.aggregate,
        _reconciliation_event(intent),
    )

    assert transition.disposition == "active_chat_runtime_reconciled"
    assert transition.operations[0].status is SessionOperationStatus.COMPLETED
    assert transition.operations[0].operation_id == intent[
        "reconciliation_operation_id"
    ]
    reconciled = transition.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]
    assert reconciled["status"] == "reconciled"
    assert reconciled["reconciliation_completion"]["effect_id"] == intent[
        "reconciliation_effect_id"
    ]
    assert intent["reconciliation_operation_id"] not in transition.aggregate.data.get(
        "operation_fences",
        {},
    )


@pytest.mark.parametrize(
    "control_effect_kind",
    [
        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
    ],
)
@pytest.mark.parametrize("failed", [False, True])
def test_legacy_reconciliation_without_snapshot_accepts_only_v1(
    control_effect_kind: AgentSessionEffectKind,
    failed: bool,
) -> None:
    reducer = AgentSessionReducer()
    requested = _failed_control_reconciliation(reducer, control_effect_kind)
    intent = dict(
        requested.aggregate.data["effect_control_intents"][control_effect_kind]
    )
    intent.pop("reconciliation_contract_version")
    intent.pop("reconciliation_contract_signature")
    legacy = _replace_control_intent(
        requested.aggregate,
        control_effect_kind,
        intent,
    )

    accepted = reducer.reduce(
        legacy,
        _reconciliation_event(intent, failed=failed),
    )
    rejected = reducer.reduce(
        legacy,
        _reconciliation_event(intent, failed=failed, contract_version=2),
    )

    if failed:
        assert accepted.disposition in {
            "active_chat_runtime_reconciliation_required",
            "idle_planning_cancellation_reconciliation_required",
        }
        assert rejected.disposition == "ignored_unrelated_reconciliation_failure"
    else:
        assert accepted.disposition in {
            "active_chat_runtime_reconciled",
            "idle_planning_cancellation_reconciled",
        }
        assert rejected.disposition == "ignored_unrelated_control_reconciliation"
    assert "contract_version_changed" in rejected.reason
    assert "contract_signature_changed" in rejected.reason


@pytest.mark.parametrize(
    "control_effect_kind",
    [
        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
    ],
)
@pytest.mark.parametrize("failed", [False, True])
def test_reconciliation_rejects_a_mutated_persisted_contract_snapshot(
    control_effect_kind: AgentSessionEffectKind,
    failed: bool,
) -> None:
    reducer = AgentSessionReducer()
    requested = _failed_control_reconciliation(reducer, control_effect_kind)
    intent = dict(
        requested.aggregate.data["effect_control_intents"][control_effect_kind]
    )
    intent["reconciliation_contract_signature"] = "0" * 64
    tampered = _replace_control_intent(
        requested.aggregate,
        control_effect_kind,
        intent,
    )

    transition = reducer.reduce(
        tampered,
        _reconciliation_event(intent, failed=failed),
    )

    assert transition.disposition == (
        "ignored_unrelated_reconciliation_failure"
        if failed
        else "ignored_unrelated_control_reconciliation"
    )
    assert "expected_contract_signature_changed" in transition.reason


def test_reconciliation_failure_retries_with_new_fenced_effect_then_exhausts() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(control_reconciliation_max_cycles=2)
    )
    first = _failed_stop_reconciliation(reducer)
    first_intent = first.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]

    second = reducer.reduce(
        first.aggregate,
        _reconciliation_event(first_intent, failed=True),
    )

    assert second.disposition == "active_chat_runtime_reconciliation_required"
    assert [operation.status for operation in second.operations] == [
        SessionOperationStatus.FAILED,
        SessionOperationStatus.PENDING,
    ]
    second_intent = second.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]
    assert second_intent["reconciliation_cycle"] == 2
    assert second.effects[0].effect_id != first.effects[0].effect_id
    assert first_intent["reconciliation_operation_id"] not in second.aggregate.data[
        "operation_fences"
    ]
    assert second_intent["reconciliation_operation_id"] in second.aggregate.data[
        "operation_fences"
    ]

    exhausted = reducer.reduce(
        second.aggregate,
        _reconciliation_event(
            second_intent,
            failed=True,
            occurred_at=140.0,
        ),
    )

    assert exhausted.disposition == "control_reconciliation_exhausted"
    assert exhausted.effects == ()
    assert exhausted.operations[0].status is SessionOperationStatus.FAILED
    exhausted_intent = exhausted.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]
    assert exhausted_intent["status"] == "reconciliation_failed"
    assert len(exhausted_intent["reconciliation_failures"]) == 2
    assert second_intent[
        "reconciliation_operation_id"
    ] not in exhausted.aggregate.data.get("operation_fences", {})


def test_reconciliation_completion_missing_provenance_is_ignored() -> None:
    reducer = AgentSessionReducer()
    requested = _failed_stop_reconciliation(reducer)
    intent = requested.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]
    event = _reconciliation_event(intent)
    payload = dict(event.payload)
    payload.pop("input_watermark")

    transition = reducer.reduce(
        requested.aggregate,
        SessionEventEnvelope(
            event_id=event.event_id,
            key=event.key,
            kind=event.kind,
            ownership_generation=event.ownership_generation,
            payload=payload,
            source=event.source,
            occurred_at=event.occurred_at,
            causation_id=event.causation_id,
        ),
    )

    assert transition.disposition == "ignored_unrelated_control_reconciliation"
    assert "input_watermark_missing" in transition.reason
    unchanged_intent = transition.aggregate.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]
    assert unchanged_intent["status"] == "reconciliation_requested"


def test_unrelated_effect_failure_does_not_settle_idle_planning_operation() -> None:
    reducer = AgentSessionReducer()
    settling = _settling(reducer)
    event = _event(
        AgentSessionEventKind.EFFECT_FAILED,
        event_id="unrelated-effect-failed",
        payload={
            "effect_id": "other-effect",
            "effect_kind": AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
            "operation_id": "other-operation",
            "failure_code": "retry_exhausted",
        },
        occurred_at=120.0,
    )

    transition = reducer.reduce(settling, event)

    assert transition.disposition == "ignored_unrelated_control_effect_failure"
    assert transition.aggregate.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert transition.aggregate.state_revision == settling.state_revision
    assert transition.review_schedules == ()
    assert transition.operations == ()


def test_reducer_rejects_cross_session_event() -> None:
    reducer = AgentSessionReducer()
    event = SessionEventEnvelope(
        event_id="wrong-session",
        key=SessionKey("profile-b", "session-b"),
        kind=AgentSessionEventKind.EXIT_REQUESTED,
    )

    with pytest.raises(ValueError, match="does not match aggregate ownership"):
        reducer.reduce(_active_chat(), event)


def test_event_envelope_rejects_non_finite_time_before_reduction() -> None:
    with pytest.raises(ValueError, match="occurred_at must be finite"):
        _event(
            AgentSessionEventKind.EXIT_REQUESTED,
            event_id="invalid-time",
            occurred_at=float("nan"),
        )
