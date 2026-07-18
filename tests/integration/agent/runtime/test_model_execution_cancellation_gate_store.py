"""Integration coverage for the Actor-native v3 model cancellation gate."""

from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import builtin_effect_contract
from shinbot.agent.runtime.session_actor.events import (
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionOperationStatus,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.model_execution_cancellation_gate import (
    ModelExecutionCancellationGateRequest,
    ModelExecutionCancellationGateStatus,
    SQLiteModelExecutionCancellationGateStore,
)
from shinbot.agent.runtime.session_actor.model_execution_witness import (
    ModelExecutionClaim,
    ModelExecutionPermitDisposition,
    SQLiteModelExecutionWitnessStore,
    mark_expired_model_execution_unknown,
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
from shinbot.persistence.records import MessageLogRecord


def _database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


async def _prepare_settling_target(
    tmp_path: Path,
    *,
    target_status: str,
) -> tuple[
    DatabaseManager,
    SQLiteSessionActorStore,
    SessionKey,
    int,
    AgentSessionReducer,
    str,
    str,
]:
    """Create one exact v3 idle-planning target owned by a settling actor."""

    database = _database(tmp_path)
    now = [100.0]
    store = SQLiteSessionActorStore(database, clock=lambda: now[0])
    key = SessionKey("profile-model-gate", f"session:{target_status}")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="model cancellation gate integration test",
        legacy_session_id=f"legacy:{key.session_id}",
    ).ownership
    generation = ownership.generation
    await store.ensure(key, ownership_generation=generation)
    reducer = AgentSessionReducer()
    setup_event = SessionEventEnvelope(
        event_id="setup:model-gate",
        key=key,
        kind="SetupModelGate",
        ownership_generation=generation,
        source="integration-test",
        occurred_at=now[0],
        available_at=now[0],
        created_at=now[0],
    )
    await store.enqueue(setup_event)
    setup_claim = await store.claim_next(key, worker_id="setup-worker")
    assert setup_claim is not None
    idle = await store.load(key)
    operation_id = "idle-planning-operation:model-gate"
    effect_id = "idle-planning-effect:model-gate"
    contract = builtin_effect_contract("run_idle_review_planning", version=3)
    fence = {
        "operation_id": operation_id,
        "operation_kind": "idle_review_planning",
        "source_event_id": setup_event.event_id,
        "effect_id": effect_id,
        "effect_kind": contract.effect_kind,
        "contract_version": contract.version,
        "contract_signature": contract.signature,
        "idempotency_key": effect_id,
        "completion_event_id": "idle-planning-complete:model-gate",
        "failure_event_id": "idle-planning-failed:model-gate",
        "ownership_generation": generation,
        "plan_id": "plan:model-gate",
        "active_epoch": idle.active_epoch,
        "activity_generation": idle.activity_generation,
        "input_watermark": 0,
        "input_ledger_sequence": None,
    }
    data = dict(idle.data)
    data["operation_fences"] = {operation_id: fence}
    data["idle_exit"] = {
        "operation_id": operation_id,
        "plan_id": "plan:model-gate",
        "planner_effect_id": effect_id,
        "planner_idempotency_key": effect_id,
        "planner_contract_version": contract.version,
        "planner_contract_signature": contract.signature,
        "active_epoch": idle.active_epoch,
        "activity_generation": idle.activity_generation,
        "input_watermark": 0,
        "input_ledger_sequence": None,
    }
    settling = idle.advance(
        state=AgentSessionState.ACTIVE_CHAT_SETTLING.value,
        idle_planning_operation_id=operation_id,
        data=data,
        updated_at=now[0],
    )
    await store.commit(
        setup_claim,
        SessionTransition(
            aggregate=settling,
            disposition="setup_model_execution_cancellation_target",
            operations=(
                SessionOperation(
                    operation_id=operation_id,
                    kind="idle_review_planning",
                    status=SessionOperationStatus.PENDING,
                    launched_by_event_id=setup_event.event_id,
                    state_revision=settling.state_revision,
                    active_epoch=idle.active_epoch,
                    activity_generation=idle.activity_generation,
                    input_watermark=0,
                    started_at=now[0],
                ),
            ),
            effects=(
                SessionEffect(
                    effect_id=effect_id,
                    kind=contract.effect_kind,
                    contract_version=contract.version,
                    contract_signature=contract.signature,
                    idempotency_key=effect_id,
                    operation_id=operation_id,
                    payload={
                        **fence,
                        "planning_input": {
                            "version": 1,
                            "active_epoch": idle.active_epoch,
                            "activity_generation": idle.activity_generation,
                            "input_watermark": 0,
                            "input_ledger_sequence": None,
                            "trigger": "active_chat_exit",
                        },
                        "trigger": "active_chat_exit",
                        "source": "integration-test",
                    },
                ),
            ),
        ),
        expected_revision=idle.state_revision,
    )
    if target_status == "processing":
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = 'processing', attempt_count = 1,
                    claim_id = 'target-claim', lease_owner = 'target-worker',
                    lease_until = 200.0, updated_at = 101.0
                WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                """,
                (key.profile_id, key.session_id, effect_id),
            )
    return database, store, key, generation, reducer, operation_id, effect_id


def _message_event(
    *,
    key: SessionKey,
    generation: int,
    message_log_id: int,
) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id="message:model-gate",
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=generation,
        payload={
            "version": 1,
            "event_id": "message:model-gate",
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "bot_id": key.profile_id,
            "bot_binding_id": "binding-model-gate",
            "base_session_id": "instance-model-gate:base-session",
            "bot_session_id": key.session_id,
            "message_log_id": message_log_id,
            "sender_id": "user-a",
            "instance_id": "instance-model-gate",
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
            "trace_id": "trace:model-gate",
            "observed_at": 100.0,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
        source="agent_route_outbox",
        occurred_at=100.0,
        causation_id="route:model-gate",
        correlation_id="correlation:model-gate",
        trace_id="trace:model-gate",
        available_at=100.0,
        created_at=100.0,
    )


async def _commit_new_message(
    database: DatabaseManager,
    store: SQLiteSessionActorStore,
    key: SessionKey,
    generation: int,
    reducer: AgentSessionReducer,
) -> tuple[SessionTransition, str]:
    message_log_id = database.message_logs.insert(
        MessageLogRecord(
            session_id="base-session",
            platform_msg_id="platform:model-gate",
            sender_id="user-a",
            sender_name="User A",
            raw_text="new message",
            content_json="[]",
            role="user",
            created_at=100.0,
        )
    )
    event = _message_event(
        key=key,
        generation=generation,
        message_log_id=message_log_id,
    )
    await store.enqueue(event)
    claim = await store.claim_next(key, worker_id="message-worker")
    assert claim is not None
    settling = await store.load(key)
    transition = reducer.reduce(settling, event)
    await store.commit(claim, transition, expected_revision=settling.state_revision)
    return transition, event.event_id


def _gate_row(database: DatabaseManager, key: SessionKey) -> dict[str, object]:
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM agent_model_execution_cancellation_gates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert row is not None
    return dict(row)


@pytest.mark.asyncio
async def test_pending_v3_model_target_is_cancelled_in_the_actor_commit(
    tmp_path: Path,
) -> None:
    """A never-claimed target cannot race a new model task after supersession."""

    database, store, key, generation, reducer, _operation_id, effect_id = (
        await _prepare_settling_target(tmp_path, target_status="pending")
    )

    transition, _event_id = await _commit_new_message(
        database,
        store,
        key,
        generation,
        reducer,
    )

    assert len(transition.model_execution_cancellation_gate_requests) == 1
    assert transition.effects[0].kind == AgentSessionEffectKind.CANCEL_MODEL_EXECUTION
    gate = _gate_row(database, key)
    assert gate["gate_status"] == "terminal"
    assert gate["target_effect_status"] == "cancelled"
    assert gate["target_execution_status"] == "none"
    with database.connect() as conn:
        target = conn.execute(
            "SELECT status, claim_id, lease_owner FROM agent_effect_outbox WHERE effect_id = ?",
            (effect_id,),
        ).fetchone()
    assert tuple(target) == ("cancelled", "", "")


@pytest.mark.asyncio
async def test_gate_cancels_claimed_target_before_its_handler_task_starts(
    tmp_path: Path,
) -> None:
    """The witness start boundary sees the committed gate before task creation."""

    database, store, key, generation, reducer, operation_id, effect_id = (
        await _prepare_settling_target(tmp_path, target_status="processing")
    )
    transition, _event_id = await _commit_new_message(
        database,
        store,
        key,
        generation,
        reducer,
    )
    request = transition.model_execution_cancellation_gate_requests[0]
    contract = builtin_effect_contract("run_idle_review_planning", version=3)
    witness = SQLiteModelExecutionWitnessStore(database, clock=lambda: 103.0)

    permit = await witness.begin_execution(
        ModelExecutionClaim(
            key=key,
            ownership_generation=generation,
            effect_id=effect_id,
            operation_id=operation_id,
            effect_kind="run_idle_review_planning",
            contract_version=contract.version,
            contract_signature=contract.signature,
            claim_id="target-claim",
            worker_id="target-worker",
        )
    )

    assert permit.disposition is ModelExecutionPermitDisposition.CANCELLED
    assert permit.cancellation_effect_id == request.cancellation_effect_id
    gate = _gate_row(database, key)
    assert gate["gate_status"] == "terminal"
    assert gate["target_execution_status"] == "none"


@pytest.mark.asyncio
async def test_running_target_waits_for_real_task_exit_before_confirmation(
    tmp_path: Path,
) -> None:
    """A running witness is a wait, then becomes terminal only after finish."""

    database, store, key, generation, reducer, operation_id, effect_id = (
        await _prepare_settling_target(tmp_path, target_status="processing")
    )
    contract = builtin_effect_contract("run_idle_review_planning", version=3)
    claim = ModelExecutionClaim(
        key=key,
        ownership_generation=generation,
        effect_id=effect_id,
        operation_id=operation_id,
        effect_kind="run_idle_review_planning",
        contract_version=contract.version,
        contract_signature=contract.signature,
        claim_id="target-claim",
        worker_id="target-worker",
    )
    witness = SQLiteModelExecutionWitnessStore(database, clock=lambda: 103.0)
    started = await witness.begin_execution(claim)
    assert started.disposition is ModelExecutionPermitDisposition.STARTED
    transition, event_id = await _commit_new_message(
        database,
        store,
        key,
        generation,
        reducer,
    )
    request = transition.model_execution_cancellation_gate_requests[0]
    gate_store = SQLiteModelExecutionCancellationGateStore(database, clock=lambda: 104.0)
    pending = await gate_store.ensure_model_execution_cancelled(
        ModelExecutionCancellationGateRequest(
            key=key,
            ownership_generation=generation,
            cancellation_effect_id=request.cancellation_effect_id,
            request_event_id=event_id,
            target_operation_id=operation_id,
            target_effect_id=effect_id,
            target_effect_kind=claim.effect_kind,
            target_contract_version=claim.contract_version,
            target_contract_signature=claim.contract_signature,
        )
    )
    assert pending.status is ModelExecutionCancellationGateStatus.PENDING
    assert pending.durable_running_count == 1

    finished = await witness.finish_execution(claim)
    confirmed = await gate_store.ensure_model_execution_cancelled(
        ModelExecutionCancellationGateRequest(
            key=key,
            ownership_generation=generation,
            cancellation_effect_id=request.cancellation_effect_id,
            request_event_id=event_id,
            target_operation_id=operation_id,
            target_effect_id=effect_id,
            target_effect_kind=claim.effect_kind,
            target_contract_version=claim.contract_version,
            target_contract_signature=claim.contract_signature,
        )
    )

    assert finished.disposition is ModelExecutionPermitDisposition.CANCELLED
    assert confirmed.status is ModelExecutionCancellationGateStatus.CONFIRMED
    assert _gate_row(database, key)["gate_status"] == "terminal"


@pytest.mark.asyncio
async def test_terminal_target_snapshots_finished_witness_before_control_completion(
    tmp_path: Path,
) -> None:
    """A pre-settled target keeps its finished witness fence in the v3 gate."""

    database, store, key, generation, reducer, operation_id, effect_id = (
        await _prepare_settling_target(tmp_path, target_status="pending")
    )
    contract = builtin_effect_contract("run_idle_review_planning", version=3)
    claim = ModelExecutionClaim(
        key=key,
        ownership_generation=generation,
        effect_id=effect_id,
        operation_id=operation_id,
        effect_kind=contract.effect_kind,
        contract_version=contract.version,
        contract_signature=contract.signature,
        claim_id="target-claim",
        worker_id="target-worker",
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET status = 'completed', attempt_count = 1,
                claim_id = ?, lease_owner = '', lease_until = NULL,
                completed_at = 103.0, updated_at = 103.0
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (claim.claim_id, key.profile_id, key.session_id, effect_id),
        )
        conn.execute(
            """
            INSERT INTO agent_model_execution_runs (
                profile_id, session_id, ownership_generation,
                effect_id, operation_id, effect_kind,
                contract_version, contract_signature, claim_id, worker_id,
                execution_status, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'finished', 101.0, 103.0)
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                claim.effect_id,
                claim.operation_id,
                claim.effect_kind,
                claim.contract_version,
                claim.contract_signature,
                claim.claim_id,
                claim.worker_id,
            ),
        )

    transition, event_id = await _commit_new_message(
        database,
        store,
        key,
        generation,
        reducer,
    )
    request = transition.model_execution_cancellation_gate_requests[0]
    gate = _gate_row(database, key)

    assert gate["gate_status"] == "terminal"
    assert gate["target_execution_status"] == "finished"
    assert gate["target_claim_id"] == claim.claim_id
    assert gate["target_worker_id"] == claim.worker_id
    observation = await SQLiteModelExecutionCancellationGateStore(
        database,
        clock=lambda: 104.0,
    ).ensure_model_execution_cancelled(
        ModelExecutionCancellationGateRequest(
            key=key,
            ownership_generation=generation,
            cancellation_effect_id=request.cancellation_effect_id,
            request_event_id=event_id,
            target_operation_id=operation_id,
            target_effect_id=effect_id,
            target_effect_kind=claim.effect_kind,
            target_contract_version=claim.contract_version,
            target_contract_signature=claim.contract_signature,
        )
    )

    assert observation.status is ModelExecutionCancellationGateStatus.CONFIRMED


@pytest.mark.asyncio
async def test_unknown_target_becomes_a_durable_blocker_not_a_cancellation(
    tmp_path: Path,
) -> None:
    """Unknown model evidence must survive supersession as an explicit blocker."""

    database, store, key, generation, reducer, operation_id, effect_id = (
        await _prepare_settling_target(tmp_path, target_status="processing")
    )
    contract = builtin_effect_contract("run_idle_review_planning", version=3)
    claim = ModelExecutionClaim(
        key=key,
        ownership_generation=generation,
        effect_id=effect_id,
        operation_id=operation_id,
        effect_kind="run_idle_review_planning",
        contract_version=contract.version,
        contract_signature=contract.signature,
        claim_id="target-claim",
        worker_id="target-worker",
    )
    witness = SQLiteModelExecutionWitnessStore(database, clock=lambda: 103.0)
    assert (await witness.begin_execution(claim)).disposition is ModelExecutionPermitDisposition.STARTED
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        assert mark_expired_model_execution_unknown(
            conn,
            key=key,
            ownership_generation=generation,
            effect_id=effect_id,
            claim_id=claim.claim_id,
            worker_id=claim.worker_id,
            now=104.0,
            reason="test_unknown_model_execution",
        )

    transition, event_id = await _commit_new_message(
        database,
        store,
        key,
        generation,
        reducer,
    )
    request = transition.model_execution_cancellation_gate_requests[0]
    observation = await SQLiteModelExecutionCancellationGateStore(
        database,
        clock=lambda: 105.0,
    ).ensure_model_execution_cancelled(
        ModelExecutionCancellationGateRequest(
            key=key,
            ownership_generation=generation,
            cancellation_effect_id=request.cancellation_effect_id,
            request_event_id=event_id,
            target_operation_id=operation_id,
            target_effect_id=effect_id,
            target_effect_kind=claim.effect_kind,
            target_contract_version=claim.contract_version,
            target_contract_signature=claim.contract_signature,
        )
    )

    assert observation.status is ModelExecutionCancellationGateStatus.BLOCKED
    assert observation.durable_unknown_count == 1
    gate = _gate_row(database, key)
    assert gate["gate_status"] == "blocked"
    assert gate["target_effect_status"] == "processing"
    assert gate["target_execution_status"] == "unknown"

    control_effect = transition.effects[0]
    control_contract = builtin_effect_contract("cancel_model_execution", version=3)
    aggregate = await store.load(key)
    completion = SessionEventEnvelope(
        event_id=control_effect.payload["completion_event_id"],
        key=key,
        kind=AgentSessionEventKind.MODEL_EXECUTION_CANCELLATION_COMPLETED,
        ownership_generation=generation,
        payload={
            **{
                field_name: control_effect.payload[field_name]
                for field_name in control_contract.outcome_fence_fields or ()
            },
            "effect_id": control_effect.effect_id,
            "effect_kind": control_effect.kind,
            "idempotency_key": control_effect.idempotency_key,
            "operation_id": control_effect.operation_id,
            "contract_version": control_effect.contract_version,
            "contract_signature": control_effect.contract_signature,
            "attempt_count": 1,
            "model_execution_cancellation": observation.to_payload(),
        },
        source="effect_executor",
        occurred_at=105.0,
        causation_id=event_id,
        correlation_id=operation_id,
        available_at=105.0,
        created_at=105.0,
    )

    blocked = reducer.reduce(aggregate, completion)

    assert blocked.disposition == "model_execution_cancellation_blocked"
    assert blocked.aggregate.data["model_execution_blocked"] == {
        "kind": "execution_unknown",
        "source": "model_execution_cancellation_gate",
        "event_id": completion.event_id,
        "effect_id": effect_id,
        "effect_kind": "run_idle_review_planning",
        "operation_id": operation_id,
        "claim_id": "target-claim",
        "worker_id": "target-worker",
        "unknown_reason": "model_execution_witness_unknown",
        "durable_unknown_count": 1,
    }
