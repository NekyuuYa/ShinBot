"""SQLite contract test for the pure active-chat exit reducer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
    resolved_outcome_fence_fields,
)
from shinbot.agent.runtime.session_actor.effect_store import (
    SQLiteDurableEffectStore,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionReviewSchedule,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEffectKind,
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager


@pytest.mark.asyncio
async def test_exit_completion_commits_fences_metadata_and_schedule_clock(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    store = SQLiteSessionActorStore(
        database,
        lease_seconds=30.0,
        retry_delay_seconds=0.0,
        clock=lambda: now[0],
    )
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", "profile-a:group:room")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="idle exit store test",
    ).ownership

    bootstrap = SessionEventEnvelope(
        event_id="bootstrap-active-chat",
        key=key,
        kind="TestBootstrapActiveChat",
        ownership_generation=ownership.generation,
        occurred_at=100.0,
    )
    await store.enqueue(bootstrap)
    bootstrap_claim = await store.claim_next(key, worker_id="worker")
    assert bootstrap_claim is not None
    initial = await store.load(key)
    await store.commit(
        bootstrap_claim,
        SessionTransition(
            aggregate=initial.advance(
                state=AgentSessionState.ACTIVE_CHAT.value,
                active_epoch=4,
                activity_generation=6,
                current_plan_id="previous-review-plan",
                review_plan_revision=1,
                review_plan={
                    "plan_id": "previous-review-plan",
                    "plan_revision": 1,
                },
                active_chat_state={"attention": 0.5, "trace": [98, 99]},
                data={"message_watermark": 99},
                updated_at=100.0,
            ),
            disposition="test_active_chat_started",
            caused_plan_id="previous-review-plan",
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="previous-review-plan",
                    plan_revision=1,
                    applied_delay_seconds=60.0,
                    trigger="previous_idle_review",
                    outcome="planned",
                    source="integration-test",
                ),
            ),
            reason="test_bootstrap",
        ),
        expected_revision=initial.state_revision,
    )

    now[0] = 120.0
    exit_event = SessionEventEnvelope(
        event_id="exit-requested",
        key=key,
        kind=AgentSessionEventKind.EXIT_REQUESTED,
        ownership_generation=ownership.generation,
        source="integration-test",
        occurred_at=110.0,
        payload={
            "operation_id": "idle-operation-sqlite",
            "plan_id": "review-plan-sqlite",
            "input_watermark": 99,
            "trigger": "attention_decay",
        },
    )
    await store.enqueue(exit_event)
    exit_claim = await store.claim_next(key, worker_id="worker")
    assert exit_claim is not None
    active = await store.load(key)
    exit_transition = reducer.reduce(active, exit_event)
    settling = await store.commit(
        exit_claim,
        exit_transition,
        expected_revision=active.state_revision,
    )
    assert settling.state == AgentSessionState.ACTIVE_CHAT_SETTLING
    assert settling.state_revision == 2
    assert settling.event_sequence == 2
    pending_exit = settling.data["idle_exit"]
    assert pending_exit["deadline_scheduled_from"] == 120.0
    assert pending_exit["deadline_at"] == 150.0

    with database.connect() as conn:
        pending_operation = conn.execute(
            """
            SELECT status, launched_by_event_id, state_revision, active_epoch,
                   activity_generation, input_watermark, started_at
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            ("idle-operation-sqlite",),
        ).fetchone()
    assert pending_operation is not None
    assert tuple(pending_operation) == (
        "pending",
        "exit-requested",
        settling.state_revision,
        4,
        6,
        99,
        110.0,
    )

    now[0] = 200.0
    planner_contract = builtin_effect_contract("run_idle_review_planning")
    completion = SessionEventEnvelope(
        event_id=pending_exit["completion_event_id"],
        key=key,
        kind=AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
        ownership_generation=ownership.generation,
        source="effect_executor",
        occurred_at=130.0,
        payload={
            "operation_id": "idle-operation-sqlite",
            "plan_id": "review-plan-sqlite",
            "active_epoch": 4,
            "activity_generation": 6,
            "input_watermark": 99,
            "input_ledger_sequence": 0,
            "effect_id": pending_exit["planner_effect_id"],
            "effect_kind": "run_idle_review_planning",
            "idempotency_key": pending_exit["planner_idempotency_key"],
            "attempt_count": 1,
            "contract_version": planner_contract.version,
            "contract_signature": planner_contract.signature,
            "model_execution_id": "execution-1",
            "prompt_signature": "prompt-1",
            "outcome": {
                "kind": "planned",
                "requested_delay_seconds": 30.0,
                "reason": "quiet_after_active_chat",
            },
        },
        causation_id="exit-requested",
    )
    await store.enqueue(completion)
    completion_claim = await store.claim_next(key, worker_id="worker")
    assert completion_claim is not None
    loaded_settling = await store.load(key)
    committed = await store.commit(
        completion_claim,
        reducer.reduce(loaded_settling, completion),
        expected_revision=loaded_settling.state_revision,
    )

    assert committed.state == AgentSessionState.IDLE
    assert committed.current_plan_id == "review-plan-sqlite"
    assert committed.review_plan_revision == 2
    assert committed.state_revision == 3
    assert committed.event_sequence == 3
    assert committed.review_plan["scheduled_from"] == 200.0
    assert committed.review_plan["next_review_at"] == 230.0
    assert committed.review_plan["applied_delay_seconds"] == 30.0
    assert committed.updated_at == 200.0

    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT status, launched_by_event_id, state_revision, active_epoch,
                   activity_generation, input_watermark, started_at,
                   finished_at, metadata_json
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            ("idle-operation-sqlite",),
        ).fetchone()
        schedule = conn.execute(
            """
            SELECT plan_revision, outcome, requested_delay_seconds,
                   applied_delay_seconds, scheduled_from, next_review_at,
                   committed_state_revision, model_execution_id,
                   prompt_signature
            FROM agent_review_schedules
            WHERE plan_id = ?
            """,
            ("review-plan-sqlite",),
        ).fetchone()
        schedules = conn.execute(
            """
            SELECT plan_id, plan_revision, status, scheduled_from,
                   next_review_at
            FROM agent_review_schedules
            WHERE profile_id = ? AND session_id = ?
            ORDER BY plan_revision
            """,
            (key.profile_id, key.session_id),
        ).fetchall()
        transitions = conn.execute(
            """
            SELECT event_id, from_state, to_state, disposition,
                   state_revision, event_sequence, operation_id, plan_id,
                   created_at
            FROM agent_state_transitions
            WHERE profile_id = ? AND session_id = ?
            ORDER BY event_sequence
            """,
            (key.profile_id, key.session_id),
        ).fetchall()
        superseded_event = conn.execute(
            """
            SELECT plan_id, previous_plan_id, event_type, outcome,
                   committed_state_revision, metadata_json
            FROM agent_review_schedule_events
            WHERE profile_id = ? AND session_id = ? AND event_type = 'superseded'
            """,
            (key.profile_id, key.session_id),
        ).fetchone()

    assert operation is not None
    assert operation["status"] == "completed"
    assert operation["launched_by_event_id"] == "exit-requested"
    assert operation["state_revision"] == settling.state_revision
    assert operation["active_epoch"] == 4
    assert operation["activity_generation"] == 6
    assert operation["input_watermark"] == 99
    assert operation["started_at"] == 110.0
    assert operation["finished_at"] == 130.0
    operation_metadata = json.loads(operation["metadata_json"])
    assert operation_metadata["idle_exit"]["operation_id"] == (
        "idle-operation-sqlite"
    )
    assert operation_metadata["idle_exit"]["deadline_at"] == 150.0
    assert operation_metadata["schedule_outcome"]["kind"] == "planned"

    assert schedule is not None
    assert schedule["plan_revision"] == 2
    assert schedule["outcome"] == "planned"
    assert schedule["requested_delay_seconds"] == 30.0
    assert schedule["applied_delay_seconds"] == 30.0
    assert schedule["scheduled_from"] == 200.0
    assert schedule["next_review_at"] == 230.0
    assert schedule["committed_state_revision"] == committed.state_revision
    assert schedule["model_execution_id"] == "execution-1"
    assert schedule["prompt_signature"] == "prompt-1"

    assert [tuple(row) for row in schedules] == [
        ("previous-review-plan", 1, "superseded", 100.0, 160.0),
        ("review-plan-sqlite", 2, "scheduled", 200.0, 230.0),
    ]
    assert [tuple(row) for row in transitions] == [
        (
            "bootstrap-active-chat",
            AgentSessionState.IDLE.value,
            AgentSessionState.ACTIVE_CHAT.value,
            "test_active_chat_started",
            1,
            1,
            "",
            "previous-review-plan",
            100.0,
        ),
        (
            "exit-requested",
            AgentSessionState.ACTIVE_CHAT.value,
            AgentSessionState.ACTIVE_CHAT_SETTLING.value,
            "active_chat_exit_settling",
            2,
            2,
            "idle-operation-sqlite",
            "review-plan-sqlite",
            120.0,
        ),
        (
            pending_exit["completion_event_id"],
            AgentSessionState.ACTIVE_CHAT_SETTLING.value,
            AgentSessionState.IDLE.value,
            "active_chat_exit_committed",
            3,
            3,
            "idle-operation-sqlite",
            "review-plan-sqlite",
            200.0,
        ),
    ]
    assert superseded_event is not None
    assert tuple(superseded_event) == (
        "previous-review-plan",
        "previous-review-plan",
        "superseded",
        "superseded",
        committed.state_revision,
        '{"superseded_by_plan_id":"review-plan-sqlite"}',
    )

    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=30.0,
        clock=lambda: now[0],
    )
    stop_contract = builtin_effect_contract(
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    )
    stop_claim = await effect_store.claim_next(
        worker_id="control-worker",
        effect_contracts=(stop_contract.ref,),
    )
    assert stop_claim is not None
    now[0] = 205.0
    stop_failure = SessionEventEnvelope(
        event_id=committed.data["effect_control_intents"][
            AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
        ]["failure_event_id"],
        key=key,
        kind=AgentSessionEventKind.EFFECT_FAILED,
        ownership_generation=ownership.generation,
        source="effect_executor",
        occurred_at=now[0],
        causation_id=stop_claim.effect.source_event_id,
        correlation_id=(
            stop_claim.effect.operation_id or stop_claim.effect.effect_id
        ),
        trace_id=stop_claim.effect.trace_id,
        payload={
            **stop_claim.effect.outcome_fence_payload(
                resolved_outcome_fence_fields(stop_contract)
            ),
            "effect_id": stop_claim.effect.effect_id,
            "effect_kind": stop_claim.effect.kind,
            "operation_id": stop_claim.effect.operation_id,
            "idempotency_key": stop_claim.effect.idempotency_key,
            "attempt_count": stop_claim.attempt_count,
            "contract_version": stop_claim.effect.contract_version,
            "contract_signature": stop_claim.effect.contract_signature,
            "failure_code": "runtime_stop_failed",
            "failure_message": "active chat worker did not stop",
        },
    )
    await effect_store.fail_with_event(
        stop_claim,
        stop_failure,
        error="active chat worker did not stop",
    )

    failure_claim = await store.claim_next(key, worker_id="actor-worker")
    assert failure_claim is not None
    before_reconciliation = await store.load(key)
    reconciliation_started = await store.commit(
        failure_claim,
        reducer.reduce(before_reconciliation, stop_failure),
        expected_revision=before_reconciliation.state_revision,
    )
    reconciliation_intent = reconciliation_started.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]
    reconciliation_contract = builtin_effect_contract(
        AgentSessionEffectKind.ACTIVE_CHAT_RUNTIME_RECONCILIATION
    )
    with database.connect() as conn:
        durable_reconciliation = conn.execute(
            """
            SELECT operation.status, effect.status, effect.kind
            FROM agent_session_operations AS operation
            JOIN agent_effect_outbox AS effect
              ON effect.operation_id = operation.operation_id
            WHERE operation.operation_id = ?
            """,
            (reconciliation_intent["reconciliation_operation_id"],),
        ).fetchone()
    assert durable_reconciliation is not None
    assert tuple(durable_reconciliation) == (
        "pending",
        "pending",
        AgentSessionEffectKind.ACTIVE_CHAT_RUNTIME_RECONCILIATION,
    )

    reconciliation_claim = await effect_store.claim_next(
        worker_id="control-worker",
        effect_contracts=(reconciliation_contract.ref,),
    )
    assert reconciliation_claim is not None
    now[0] = 210.0
    reconciliation_completion = SessionEventEnvelope(
        event_id=reconciliation_intent["reconciliation_completion_event_id"],
        key=key,
        kind=AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_RECONCILED,
        ownership_generation=ownership.generation,
        source=reconciliation_contract.completion_source,
        occurred_at=now[0],
        causation_id=reconciliation_claim.effect.source_event_id,
        payload={
            **reconciliation_claim.effect.outcome_fence_payload(
                resolved_outcome_fence_fields(reconciliation_contract)
            ),
            "effect_id": reconciliation_claim.effect.effect_id,
            "effect_kind": reconciliation_claim.effect.kind,
            "operation_id": reconciliation_claim.effect.operation_id,
            "idempotency_key": reconciliation_claim.effect.idempotency_key,
            "attempt_count": reconciliation_claim.attempt_count,
            "contract_version": reconciliation_claim.effect.contract_version,
            "contract_signature": reconciliation_claim.effect.contract_signature,
        },
    )
    await effect_store.complete_with_event(
        reconciliation_claim,
        reconciliation_completion,
    )

    completion_claim = await store.claim_next(key, worker_id="actor-worker")
    assert completion_claim is not None
    before_completion = await store.load(key)
    reconciled = await store.commit(
        completion_claim,
        reducer.reduce(before_completion, reconciliation_completion),
        expected_revision=before_completion.state_revision,
    )
    assert reconciled.data["effect_control_intents"][
        AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    ]["status"] == "reconciled"
    with database.connect() as conn:
        statuses = conn.execute(
            """
            SELECT operation.status, effect.status
            FROM agent_session_operations AS operation
            JOIN agent_effect_outbox AS effect
              ON effect.operation_id = operation.operation_id
            WHERE operation.operation_id = ?
            """,
            (reconciliation_intent["reconciliation_operation_id"],),
        ).fetchone()
    assert statuses is not None
    assert tuple(statuses) == ("completed", "completed")
