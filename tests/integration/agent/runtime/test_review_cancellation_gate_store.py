"""Atomic persistence coverage for Actor v2 review-cancellation gates."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectExecutor,
    EffectHandlerRegistry,
    EffectHandlerResult,
    EffectLane,
    EffectRunStatus,
    EffectSettlementStatus,
)
from shinbot.agent.runtime.session_actor.effect_store import (
    EffectStoreConflict,
    SQLiteDurableEffectStore,
)
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionOperationStatus,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
)
from shinbot.agent.runtime.session_actor.review_execution_gate import (
    REVIEW_EXECUTION_UNKNOWN_EVENT_KIND,
    REVIEW_EXECUTION_UNKNOWN_EVENT_SOURCE,
    ReviewExecutionClaim,
    ReviewExecutionGateError,
    ReviewExecutionPermitDisposition,
    ReviewExecutionUnknownNotice,
    SQLiteReviewExecutionGateStore,
)
from shinbot.agent.runtime.session_actor.store import (
    DurableRecordConflict,
    SQLiteSessionActorStore,
)
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


class _WakeTarget:
    """Record unexpected mailbox wakeups from cancellation-only test paths."""

    def __init__(self) -> None:
        self.woken: list[SessionKey] = []

    async def wake(self, key: SessionKey) -> None:
        """Record one durable wake request."""

        self.woken.append(key)

    async def recover(self) -> int:
        """No mailbox recovery is needed in these isolated integration tests."""

        return 0


def _execution_claim(effect_claim: ClaimedEffect) -> ReviewExecutionClaim:
    """Project an executor claim into the exact review-witness identity."""

    effect = effect_claim.effect
    return ReviewExecutionClaim(
        key=effect_claim.key,
        ownership_generation=effect.ownership_generation,
        review_effect_id=effect.effect_id,
        review_operation_id=effect.operation_id,
        review_effect_kind=effect.kind,
        review_contract_version=effect.contract_version,
        review_contract_signature=effect.contract_signature,
        claim_id=effect_claim.claim_id,
        worker_id=effect_claim.worker_id,
    )


def _make_store(tmp_path: Path) -> tuple[DatabaseManager, SQLiteSessionActorStore]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database, SQLiteSessionActorStore(database, clock=lambda: 100.0)


async def _activate(
    database: DatabaseManager,
    store: SQLiteSessionActorStore,
    key: SessionKey,
    *,
    admission_grant: ActorV2AdmissionGrant | None = None,
) -> int:
    claim = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="review cancellation gate integration test",
        legacy_session_id=f"legacy:{key.profile_id}:{key.session_id}",
        admission_grant=admission_grant,
    )
    generation = claim.ownership.generation
    await store.ensure(key, ownership_generation=generation)
    return generation


def _execution_binding(
    database: DatabaseManager,
    *,
    key: SessionKey,
    ownership_generation: int,
    admission_grant: ActorV2AdmissionGrant,
    target_incarnation_id: str,
) -> FencedActorExecutionBinding:
    """Acquire one exact target capability for a review witness test."""

    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership_generation,
        admission_fence_id=admission_grant.fence.fence_id,
        admission_fence_generation=admission_grant.fence.generation,
    )
    target_lease = database.actor_v2_fenced_wake_target_leases.acquire(
        request,
        target=MailboxHandoffTarget(
            "review-witness-test-target",
            target_incarnation_id,
        ),
        ttl_seconds=60.0,
    )
    return FencedActorExecutionBinding(request=request, target_lease=target_lease)


def _insert_message(
    database: DatabaseManager,
    *,
    token: str,
    created_at: float,
) -> int:
    return database.message_logs.insert(
        MessageLogRecord(
            session_id="base-session",
            platform_msg_id=f"platform:{token}",
            sender_id="user-a",
            sender_name="User A",
            raw_text=token,
            content_json="[]",
            role="user",
            created_at=created_at,
        )
    )


def _message_event(
    *,
    event_id: str,
    key: SessionKey,
    generation: int,
    message_log_id: int,
    observed_at: float,
    mentioned: bool,
) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=generation,
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "bot_id": key.profile_id,
            "bot_binding_id": "binding-a",
            "base_session_id": "instance-a:base-session",
            "bot_session_id": key.session_id,
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
            "observed_at": observed_at,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
        source="agent_route_outbox",
        occurred_at=observed_at,
        causation_id=f"route:{event_id}",
        correlation_id=f"correlation:{event_id}",
        trace_id=f"trace:{event_id}",
        available_at=observed_at,
        created_at=observed_at,
    )


async def _commit_message(
    store: SQLiteSessionActorStore,
    reducer: AgentSessionReducer,
    event: SessionEventEnvelope,
) -> None:
    await store.enqueue(event)
    claim = await store.claim_next(event.key, worker_id="message-worker")
    assert claim is not None
    aggregate = await store.load(event.key)
    await store.commit(
        claim,
        reducer.reduce(aggregate, event),
        expected_revision=aggregate.state_revision,
    )


async def _prepare_interruption(
    tmp_path: Path,
    *,
    target_status: str,
    admission_grants: list[ActorV2AdmissionGrant] | None = None,
) -> tuple[
    DatabaseManager,
    SQLiteSessionActorStore,
    SessionKey,
    int,
    ClaimedSessionEvent,
    SessionTransition,
    str,
    str,
    int,
]:
    database, store = _make_store(tmp_path)
    reducer = AgentSessionReducer()
    key = SessionKey("profile-a", f"review-gate:{target_status}")
    admission_grant = None
    if admission_grants is not None:
        admission_grant = database.actor_v2_admission_fences.reserve(
            key,
            holder_id="fenced-review-expiry-test",
            ttl_seconds=3600.0,
        )
        admission_grants.append(admission_grant)
    generation = await _activate(
        database,
        store,
        key,
        admission_grant=admission_grant,
    )

    baseline_message_id = _insert_message(
        database,
        token="baseline",
        created_at=10.0,
    )
    await _commit_message(
        store,
        reducer,
        _message_event(
            event_id="message:baseline",
            key=key,
            generation=generation,
            message_log_id=baseline_message_id,
            observed_at=10.0,
            mentioned=False,
        ),
    )

    review_setup = SessionEventEnvelope(
        event_id="setup:review",
        key=key,
        kind="SetupReview",
        ownership_generation=generation,
        source="integration-test",
        occurred_at=15.0,
        available_at=15.0,
        created_at=15.0,
    )
    await store.enqueue(review_setup)
    setup_claim = await store.claim_next(key, worker_id="review-setup-worker")
    assert setup_claim is not None
    idle = await store.load(key)
    review_operation_id = "review-operation:gate"
    review_effect_id = "review-effect:gate"
    review_contract = builtin_effect_contract("run_review_workflow")
    review_fence = {
        "operation_id": review_operation_id,
        "operation_kind": "review",
        "source_event_id": review_setup.event_id,
        "effect_id": review_effect_id,
        "effect_kind": review_contract.effect_kind,
        "contract_version": review_contract.version,
        "contract_signature": review_contract.signature,
        "idempotency_key": review_effect_id,
        "completion_event_id": "review-completed:gate",
        "failure_event_id": "review-failed:gate",
        "ownership_generation": generation,
        "plan_id": idle.current_plan_id,
        "plan_revision": idle.review_plan_revision,
        "active_epoch": idle.active_epoch,
        "activity_generation": idle.activity_generation,
        "input_watermark": baseline_message_id,
        "input_ledger_sequence": None,
        "instance_id": "instance-a",
        "target_session_id": "instance-a:base-session",
    }
    review_data = dict(idle.data)
    review_data["operation_fences"] = {review_operation_id: review_fence}
    reviewing_target = idle.advance(
        state=AgentSessionState.REVIEW.value,
        review_operation_id=review_operation_id,
        data=review_data,
        updated_at=idle.updated_at,
    )
    await store.commit(
        setup_claim,
        SessionTransition(
            aggregate=reviewing_target,
            disposition="setup_review_for_cancellation_gate",
            operations=(
                SessionOperation(
                    operation_id=review_operation_id,
                    kind="review",
                    status=SessionOperationStatus.PENDING,
                    launched_by_event_id=review_setup.event_id,
                    state_revision=reviewing_target.state_revision,
                    active_epoch=idle.active_epoch,
                    activity_generation=idle.activity_generation,
                    input_watermark=baseline_message_id,
                    started_at=review_setup.occurred_at,
                ),
            ),
            effects=(
                SessionEffect(
                    effect_id=review_effect_id,
                    kind=review_contract.effect_kind,
                    contract_version=review_contract.version,
                    contract_signature=review_contract.signature,
                    idempotency_key=review_effect_id,
                    operation_id=review_operation_id,
                    payload={
                        **review_fence,
                        "review_plan": dict(reviewing_target.review_plan),
                    },
                ),
            ),
        ),
        expected_revision=idle.state_revision,
    )

    if target_status != "pending":
        with database.connect() as conn:
            if target_status == "processing":
                conn.execute(
                    """
                    UPDATE agent_effect_outbox
                    SET status = 'processing',
                        attempt_count = 1,
                        claim_id = 'review-worker-claim',
                        lease_owner = 'review-worker',
                        lease_until = 200.0,
                        updated_at = 20.0
                    WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                    """,
                    (key.profile_id, key.session_id, review_effect_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE agent_effect_outbox
                    SET status = ?,
                        attempt_count = 3,
                        claim_id = '',
                        lease_owner = '',
                        lease_until = NULL,
                        completed_at = 50.0,
                        updated_at = 50.0,
                        last_error = 'already terminal'
                    WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                    """,
                    (
                        target_status,
                        key.profile_id,
                        key.session_id,
                        review_effect_id,
                    ),
                )

    priority_message_id = _insert_message(
        database,
        token="priority",
        created_at=20.0,
    )
    priority_event = _message_event(
        event_id="message:priority",
        key=key,
        generation=generation,
        message_log_id=priority_message_id,
        observed_at=20.0,
        mentioned=True,
    )
    await store.enqueue(priority_event)
    priority_claim = await store.claim_next(key, worker_id="priority-worker")
    assert priority_claim is not None
    reviewing = await store.load(key)
    transition = reducer.reduce(reviewing, priority_event)
    return (
        database,
        store,
        key,
        generation,
        priority_claim,
        transition,
        review_effect_id,
        review_operation_id,
        _mailbox_count(database, key),
    )


def _mailbox_count(database: DatabaseManager, key: SessionKey) -> int:
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert row is not None
    return int(row["count"])


def _outbox_row(
    database: DatabaseManager,
    key: SessionKey,
    effect_id: str,
) -> dict[str, object]:
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, effect_id),
        ).fetchone()
    assert row is not None
    return dict(row)


def _gate_row(
    database: DatabaseManager,
    key: SessionKey,
) -> dict[str, object]:
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM agent_review_cancellation_gates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert row is not None
    return dict(row)


@pytest.mark.asyncio
async def test_pending_review_is_cancelled_with_gate_in_message_commit(
    tmp_path: Path,
) -> None:
    """A pending review becomes terminal without emitting a control mailbox."""

    (
        database,
        store,
        key,
        _generation,
        claim,
        transition,
        review_effect_id,
        review_operation_id,
        mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")

    assert len(transition.review_cancellation_gate_requests) == 1
    request = transition.review_cancellation_gate_requests[0]
    assert request.review_effect_id == review_effect_id
    assert request.review_effect_kind == "run_review_workflow"
    assert request.cancellation_effect_id == transition.effects[0].effect_id

    reviewing = await store.load(key)
    waiting = await store.commit(
        claim,
        transition,
        expected_revision=reviewing.state_revision,
    )

    assert waiting.state == AgentSessionState.ACTIVE_REPLY
    target = _outbox_row(database, key, review_effect_id)
    assert target["status"] == "cancelled"
    assert target["claim_id"] == ""
    assert target["lease_owner"] == ""
    assert target["lease_until"] is None
    gate = _gate_row(database, key)
    assert gate["gate_status"] == "terminal"
    assert gate["target_effect_status"] == "cancelled"
    assert gate["review_effect_id"] == review_effect_id
    assert gate["request_event_id"] == claim.envelope.event_id
    assert _mailbox_count(database, key) == mailbox_count_before
    assert _outbox_row(database, key, request.cancellation_effect_id)["status"] == "pending"


@pytest.mark.asyncio
async def test_gate_identity_mutation_rolls_back_the_entire_message_commit(
    tmp_path: Path,
) -> None:
    """A changed target cannot partially supersede the review or write a gate."""

    (
        database,
        store,
        key,
        _generation,
        claim,
        transition,
        review_effect_id,
        review_operation_id,
        mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    request = transition.review_cancellation_gate_requests[0]
    tampered = replace(request, review_effect_id="review-effect:substituted")
    malformed = replace(
        transition,
        review_cancellation_gate_requests=(tampered,),
    )
    reviewing = await store.load(key)

    with pytest.raises(DurableRecordConflict, match="review operation fence"):
        await store.commit(
            claim,
            malformed,
            expected_revision=reviewing.state_revision,
        )

    unchanged = await store.load(key)
    assert unchanged.state == AgentSessionState.REVIEW
    assert unchanged.review_operation_id == review_operation_id
    assert _outbox_row(database, key, review_effect_id)["status"] == "pending"
    with database.connect() as conn:
        gate_count = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM agent_review_cancellation_gates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        transition_count = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM agent_state_transitions
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, claim.envelope.event_id),
        ).fetchone()
    assert gate_count is not None and int(gate_count["count"]) == 0
    assert transition_count is not None and int(transition_count["count"]) == 0
    assert _mailbox_count(database, key) == mailbox_count_before


@pytest.mark.asyncio
async def test_target_outbox_identity_mutation_rejects_the_gate_commit(
    tmp_path: Path,
) -> None:
    """A target row cannot swap its sealed review contract after reduction."""

    (
        database,
        store,
        key,
        _generation,
        claim,
        transition,
        review_effect_id,
        _review_operation_id,
        mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET contract_signature = 'tampered-review-contract'
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        )
    reviewing = await store.load(key)

    with pytest.raises(DurableRecordConflict, match="target outbox identity"):
        await store.commit(
            claim,
            transition,
            expected_revision=reviewing.state_revision,
        )

    unchanged = await store.load(key)
    assert unchanged.state == AgentSessionState.REVIEW
    assert _outbox_row(database, key, review_effect_id)["status"] == "pending"
    with database.connect() as conn:
        gate_count = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM agent_review_cancellation_gates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert gate_count is not None and int(gate_count["count"]) == 0
    assert _mailbox_count(database, key) == mailbox_count_before


@pytest.mark.asyncio
async def test_processing_review_only_records_a_requested_gate(
    tmp_path: Path,
) -> None:
    """Actor commit never rewrites a review already claimed by a worker."""

    (
        database,
        store,
        key,
        _generation,
        claim,
        transition,
        review_effect_id,
        _review_operation_id,
        mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="processing")
    reviewing = await store.load(key)
    await store.commit(
        claim,
        transition,
        expected_revision=reviewing.state_revision,
    )

    target = _outbox_row(database, key, review_effect_id)
    assert target["status"] == "processing"
    assert target["claim_id"] == "review-worker-claim"
    gate = _gate_row(database, key)
    assert gate["gate_status"] == "requested"
    assert gate["target_effect_status"] == "processing"
    assert gate["target_effect_claim_id"] == "review-worker-claim"
    assert _mailbox_count(database, key) == mailbox_count_before


@pytest.mark.asyncio
@pytest.mark.parametrize("target_status", ("completed", "failed", "cancelled"))
async def test_terminal_review_status_is_retained_as_gate_evidence(
    tmp_path: Path,
    target_status: str,
) -> None:
    """An already terminal review remains observable instead of being rewritten."""

    (
        database,
        store,
        key,
        _generation,
        claim,
        transition,
        review_effect_id,
        _review_operation_id,
        mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status=target_status)
    reviewing = await store.load(key)
    await store.commit(
        claim,
        transition,
        expected_revision=reviewing.state_revision,
    )

    target = _outbox_row(database, key, review_effect_id)
    assert target["status"] == target_status
    assert target["completed_at"] == 50.0
    gate = _gate_row(database, key)
    assert gate["gate_status"] == "terminal"
    assert gate["target_effect_status"] == target_status
    assert gate["target_effect_terminal_at"] == 50.0
    assert _mailbox_count(database, key) == mailbox_count_before


@pytest.mark.asyncio
@pytest.mark.parametrize("target_status", ("completed", "failed"))
async def test_settled_review_claim_is_not_mistaken_for_a_live_lease(
    tmp_path: Path,
    target_status: str,
) -> None:
    """Terminal complete/fail effects retain their claim as idempotency evidence."""

    (
        database,
        store,
        key,
        _generation,
        claim,
        transition,
        review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status=target_status)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET claim_id = 'settled-review-claim'
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        )

    reviewing = await store.load(key)
    await store.commit(
        claim,
        transition,
        expected_revision=reviewing.state_revision,
    )

    gate = _gate_row(database, key)
    assert gate["gate_status"] == "terminal"
    assert gate["target_effect_claim_id"] == "settled-review-claim"


def test_legacy_running_review_execution_witness_migrates_to_unknown(
    tmp_path: Path,
) -> None:
    """A pre-lease running witness must never be mistaken for a finished model call."""

    database, _store = _make_store(tmp_path)
    key = SessionKey("profile-a", "legacy-review-execution-migration")
    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="seed legacy review execution witness",
    ).ownership.generation
    review_contract = builtin_effect_contract("run_review_workflow")
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 10.0, 10.0)
            """,
            (key.profile_id, key.session_id, generation),
        )
        conn.execute("DROP TABLE agent_review_execution_runs")
        conn.execute(
            """
            CREATE TABLE agent_review_execution_runs (
                run_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                ownership_generation INTEGER NOT NULL,
                review_effect_id TEXT NOT NULL,
                review_operation_id TEXT NOT NULL,
                review_effect_kind TEXT NOT NULL,
                review_contract_version INTEGER NOT NULL,
                review_contract_signature TEXT NOT NULL,
                claim_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                execution_status TEXT NOT NULL,
                started_at REAL NOT NULL,
                finished_at REAL,
                UNIQUE(
                    profile_id, session_id, ownership_generation,
                    review_effect_id, claim_id
                ),
                CHECK(execution_status IN ('running', 'finished', 'cancelled'))
            )
            """
        )
        conn.execute(
            """
            INSERT INTO agent_review_execution_runs (
                run_seq, profile_id, session_id, ownership_generation,
                review_effect_id, review_operation_id, review_effect_kind,
                review_contract_version, review_contract_signature, claim_id,
                worker_id, execution_status, started_at, finished_at
            ) VALUES (7, ?, ?, ?, 'legacy-review-effect',
                      'legacy-review-operation', 'run_review_workflow', ?, ?,
                      'legacy-review-claim', 'legacy-review-worker', 'running',
                      10.0, NULL)
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                review_contract.version,
                review_contract.signature,
            ),
        )

    database.initialize()
    database.initialize()

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT run_seq, execution_status, started_at, finished_at,
                   unknown_at, unknown_reason
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        columns = {
            str(column["name"])
            for column in conn.execute("PRAGMA table_info(agent_review_execution_runs)")
        }
        legacy_table = conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table'
              AND name = 'agent_review_execution_runs_legacy'
            """
        ).fetchone()

    assert row is not None
    assert tuple(row) == (
        7,
        "unknown",
        10.0,
        None,
        10.0,
        "legacy_execution_witness_without_expiry",
    )
    assert {"unknown_at", "unknown_reason"} <= columns
    assert legacy_table is None


@pytest.mark.asyncio
async def test_expired_review_witness_becomes_unknown_without_replaying_or_cancelling(
    tmp_path: Path,
) -> None:
    """An expired review lease is a hard unknown blocker, never replay proof."""

    (
        database,
        store,
        key,
        _generation,
        priority_claim,
        transition,
        review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    authority = builtin_effect_contract_authority()
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: now[0],
        contract_authority=authority,
    )
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])

    effect_claim = await effect_store.claim_next(worker_id="review-worker")
    assert effect_claim is not None
    witness_claim = _execution_claim(effect_claim)
    started = await gate_store.begin_execution(witness_claim)
    assert started.disposition is ReviewExecutionPermitDisposition.STARTED

    now[0] = 200.0
    assert await effect_store.recover_expired(worker_id="recovery-worker") == 0
    assert await effect_store.claim_next(worker_id="second-worker") is None
    assert await effect_store.next_available_at() is None
    with database.connect() as conn:
        unknown_event = conn.execute(
            """
            SELECT event_id, kind, source, payload_json, causation_id,
                   correlation_id, status
            FROM agent_session_mailbox
            WHERE profile_id = ?
              AND session_id = ?
              AND kind = ?
            """,
            (key.profile_id, key.session_id, REVIEW_EXECUTION_UNKNOWN_EVENT_KIND),
        ).fetchone()
    assert unknown_event is not None
    assert tuple(unknown_event[1:3]) == (
        REVIEW_EXECUTION_UNKNOWN_EVENT_KIND,
        REVIEW_EXECUTION_UNKNOWN_EVENT_SOURCE,
    )
    assert unknown_event["causation_id"] == effect_claim.effect.source_event_id
    assert unknown_event["correlation_id"] == effect_claim.effect.operation_id
    assert unknown_event["status"] == "pending"
    notice = ReviewExecutionUnknownNotice.from_payload(
        json.loads(str(unknown_event["payload_json"])),
        event_id=str(unknown_event["event_id"]),
        key=key,
        ownership_generation=effect_claim.effect.ownership_generation,
    )
    assert notice.claim == witness_claim
    assert notice.unknown_at == 200.0
    assert notice.unknown_reason == ("review_execution_lease_expired_before_handler_terminal")

    reviewing = await store.load(key)
    await store.commit(
        priority_claim,
        transition,
        expected_revision=reviewing.state_revision,
    )
    request = transition.review_cancellation_gate_requests[0]
    pending = await gate_store.ensure_review_cancelled(request)
    assert pending.confirmed is False
    assert pending.durable_running_count == 0
    assert pending.durable_unknown_count == 1
    assert pending.blocker_code == "review_execution_witness_unknown"
    mailbox_count = _mailbox_count(database, key)

    with pytest.raises(ReviewExecutionGateError, match="already terminal"):
        await gate_store.finish_execution(witness_claim)
    assert _outbox_row(database, key, review_effect_id)["status"] == "processing"
    assert _mailbox_count(database, key) == mailbox_count
    with database.connect() as conn:
        run = conn.execute(
            """
            SELECT execution_status, unknown_at, unknown_reason
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert run is not None
    assert tuple(run) == (
        "unknown",
        200.0,
        "review_execution_lease_expired_before_handler_terminal",
    )

    late_release = await effect_store.release_for_retry(
        effect_claim,
        error="late executor cleanup after lease expiry",
        available_at=201.0,
    )

    assert late_release is not None
    assert late_release.status is EffectSettlementStatus.CANCELLED
    assert late_release.wake_request is None
    assert _outbox_row(database, key, review_effect_id)["status"] == "cancelled"
    assert _gate_row(database, key)["gate_status"] == "cancelled"
    blocked = await gate_store.ensure_review_cancelled(request)
    assert blocked.status.value == "blocked"
    assert blocked.durable_unknown_count == 1
    assert _mailbox_count(database, key) == mailbox_count


@pytest.mark.asyncio
async def test_fenced_review_witness_rejects_lost_target_before_start_and_finish(
    tmp_path: Path,
) -> None:
    """A stale target cannot create or finalize review execution evidence."""

    admission_grants: list[ActorV2AdmissionGrant] = []
    (
        database,
        _store,
        key,
        generation,
        _priority_claim,
        _transition,
        review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(
        tmp_path,
        target_status="pending",
        admission_grants=admission_grants,
    )
    now = [100.0]
    authority = builtin_effect_contract_authority()
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: now[0],
        contract_authority=authority,
    )
    review_contract = builtin_effect_contract("run_review_workflow")
    first_binding = _execution_binding(
        database,
        key=key,
        ownership_generation=generation,
        admission_grant=admission_grants[0],
        target_incarnation_id="review-witness-incarnation-a",
    )
    effect_claim = await effect_store.claim_next(
        worker_id="fenced-review-worker",
        effect_contracts=(review_contract.ref,),
        execution_binding=first_binding,
    )
    assert effect_claim is not None
    assert effect_claim.effect.effect_id == review_effect_id
    execution_claim = _execution_claim(effect_claim)
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    database.actor_v2_fenced_wake_target_leases.release(first_binding.target_lease)

    with pytest.raises(FencedWakeTargetLeaseError):
        await gate_store.begin_execution(
            execution_claim,
            execution_binding=first_binding,
        )

    second_binding = _execution_binding(
        database,
        key=key,
        ownership_generation=generation,
        admission_grant=admission_grants[0],
        target_incarnation_id="review-witness-incarnation-b",
    )
    started = await gate_store.begin_execution(
        execution_claim,
        execution_binding=second_binding,
    )
    assert started.disposition is ReviewExecutionPermitDisposition.STARTED
    database.actor_v2_fenced_wake_target_leases.release(second_binding.target_lease)

    with pytest.raises(FencedWakeTargetLeaseError):
        await gate_store.finish_execution(
            execution_claim,
            execution_binding=second_binding,
        )
    with database.connect() as conn:
        status = conn.execute(
            """
            SELECT execution_status FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()["execution_status"]
    assert status == "running"


@pytest.mark.asyncio
async def test_expired_review_notice_returns_exact_fenced_wake_request(
    tmp_path: Path,
) -> None:
    """Review-expiry diagnostics retain the full final admission identity."""

    admission_grants: list[ActorV2AdmissionGrant] = []
    (
        database,
        _store,
        key,
        generation,
        _priority_claim,
        _transition,
        _review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(
        tmp_path,
        target_status="pending",
        admission_grants=admission_grants,
    )
    assert len(admission_grants) == 1
    grant = admission_grants[0]
    now = [100.0]
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: now[0],
        contract_authority=builtin_effect_contract_authority(),
    )
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    effect_claim = await effect_store.claim_next(worker_id="fenced-review-worker")
    assert effect_claim is not None
    assert (
        await gate_store.begin_execution(_execution_claim(effect_claim))
    ).disposition is ReviewExecutionPermitDisposition.STARTED

    now[0] = 200.0
    assert await effect_store.recover_expired(worker_id="fenced-review-recovery") == 0

    notifications = await effect_store.drain_quarantine_notifications()
    assert len(notifications) == 1
    assert notifications[0].status is EffectSettlementStatus.COMMITTED
    assert notifications[0].effect_id == effect_claim.effect.effect_id
    assert notifications[0].wake_request == FencedMailboxWakeRequest(
        key=key,
        ownership_generation=generation,
        admission_fence_id=grant.fence.fence_id,
        admission_fence_generation=grant.fence.generation,
    )


@pytest.mark.asyncio
async def test_executor_iteration_recovers_expired_review_and_wakes_actor(
    tmp_path: Path,
) -> None:
    """A running worker performs expiry maintenance without requiring restart."""

    (
        database,
        _store,
        key,
        _generation,
        priority_claim,
        _transition,
        review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    authority = builtin_effect_contract_authority()
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: now[0],
        contract_authority=authority,
    )
    effect_claim = await effect_store.claim_next(worker_id="review-worker")
    assert effect_claim is not None
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    assert (
        await gate_store.begin_execution(_execution_claim(effect_claim))
    ).disposition is ReviewExecutionPermitDisposition.STARTED

    handler_calls = 0

    async def unexpected_handler(_context: object) -> EffectHandlerResult:
        nonlocal handler_calls
        handler_calls += 1
        return EffectHandlerResult(payload={})

    review_contract = builtin_effect_contract("run_review_workflow")
    handlers = EffectHandlerRegistry(contract_authority=authority)
    handlers.register(
        review_contract.effect_kind,
        unexpected_handler,
        contract=review_contract,
    )
    wake_target = _WakeTarget()
    executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        session_registry=wake_target,
        renew_interval_seconds=10.0,
        review_execution_gate_store=gate_store,
        clock=lambda: now[0],
    )

    now[0] = 200.0
    result = await executor.run_once(lane=EffectLane.PLANNER)

    assert result.status is EffectRunStatus.EMPTY
    assert handler_calls == 0
    assert wake_target.woken == [key]
    assert _outbox_row(database, key, review_effect_id)["status"] == "processing"
    with database.connect() as conn:
        run = conn.execute(
            """
            SELECT execution_status, unknown_reason
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT status
            FROM agent_session_mailbox
            WHERE profile_id = ?
              AND session_id = ?
              AND kind = ?
            """,
            (key.profile_id, key.session_id, REVIEW_EXECUTION_UNKNOWN_EVENT_KIND),
        ).fetchone()
    assert run is not None
    assert tuple(run) == (
        "unknown",
        "review_execution_lease_expired_before_handler_terminal",
    )
    assert mailbox is not None and mailbox["status"] == "pending"

    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET status = 'completed',
                claim_id = '',
                lease_owner = '',
                lease_until = NULL,
                handled_at = 200.0
            WHERE profile_id = ?
              AND session_id = ?
              AND event_id = ?
            """,
            (key.profile_id, key.session_id, priority_claim.envelope.event_id),
        )
    actor_store = SQLiteSessionActorStore(database, clock=lambda: now[0])
    unknown_claim = await actor_store.claim_next(key, worker_id="actor-worker")
    assert unknown_claim is not None
    assert unknown_claim.envelope.kind == AgentSessionEventKind.REVIEW_EXECUTION_UNKNOWN
    reviewing = await actor_store.load(key)
    blocked = await actor_store.commit(
        unknown_claim,
        AgentSessionReducer().reduce(reviewing, unknown_claim.envelope),
        expected_revision=reviewing.state_revision,
    )
    assert blocked.state == AgentSessionState.REVIEW
    assert blocked.data["review_cancellation_blocked"]["kind"] == "execution_unknown"


@pytest.mark.asyncio
async def test_unknown_review_witness_blocks_every_model_effect_claim(
    tmp_path: Path,
) -> None:
    """The outbox fence closes the gap before the actor consumes its mailbox."""

    (
        database,
        _store,
        key,
        generation,
        _priority_claim,
        _transition,
        _review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    authority = builtin_effect_contract_authority()
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: now[0],
        contract_authority=authority,
    )
    review_claim = await effect_store.claim_next(worker_id="review-worker")
    assert review_claim is not None
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    assert (
        await gate_store.begin_execution(_execution_claim(review_claim))
    ).disposition is ReviewExecutionPermitDisposition.STARTED

    now[0] = 200.0
    assert await effect_store.recover_expired(worker_id="recovery-worker") == 0
    active_reply_contract = builtin_effect_contract("run_active_reply_workflow")
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, created_at, updated_at
            ) VALUES ('blocked-active-reply-effect', 'blocked-active-reply-effect',
                      ?, ?, ?, 'blocked-active-reply-source',
                      'blocked-active-reply-operation', ?, ?, ?, '{}', 'pending',
                      0, 200.0, 200.0, 200.0)
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                active_reply_contract.effect_kind,
                active_reply_contract.version,
                active_reply_contract.signature,
            ),
        )

    contract_filter = ((active_reply_contract.effect_kind, active_reply_contract.version),)
    assert (
        await effect_store.claim_next(
            worker_id="active-reply-worker",
            effect_contracts=contract_filter,
        )
        is None
    )
    assert await effect_store.next_available_at(effect_contracts=contract_filter) is None
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, attempt_count
            FROM agent_effect_outbox
            WHERE effect_id = 'blocked-active-reply-effect'
            """
        ).fetchone()
    assert row is not None and tuple(row) == ("pending", 0)


@pytest.mark.asyncio
async def test_gate_between_claim_and_task_start_cancels_without_execution_run(
    tmp_path: Path,
) -> None:
    """A worker that claimed just before the gate must not create a model task."""

    (
        database,
        store,
        key,
        _generation,
        priority_claim,
        transition,
        review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    effect_claim = await SQLiteDurableEffectStore(
        database,
        clock=lambda: now[0],
        contract_authority=builtin_effect_contract_authority(),
    ).claim_next(worker_id="review-worker")
    assert effect_claim is not None

    reviewing = await store.load(key)
    await store.commit(
        priority_claim,
        transition,
        expected_revision=reviewing.state_revision,
    )
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    permit = await gate_store.begin_execution(_execution_claim(effect_claim))

    assert permit.disposition is ReviewExecutionPermitDisposition.CANCELLED
    assert _outbox_row(database, key, review_effect_id)["status"] == "cancelled"
    assert _gate_row(database, key)["gate_status"] == "terminal"
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert row is not None and int(row["count"]) == 0


@pytest.mark.asyncio
async def test_gate_defers_a_repeat_start_while_a_durable_witness_is_running(
    tmp_path: Path,
) -> None:
    """A gate cannot terminalize while even the same claim still has a run witness."""

    (
        database,
        store,
        key,
        _generation,
        priority_claim,
        transition,
        _review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    effect_store = SQLiteDurableEffectStore(
        database,
        clock=lambda: now[0],
        contract_authority=builtin_effect_contract_authority(),
    )
    effect_claim = await effect_store.claim_next(worker_id="review-worker")
    assert effect_claim is not None
    execution_claim = _execution_claim(effect_claim)
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    assert (
        await gate_store.begin_execution(execution_claim)
    ).disposition is ReviewExecutionPermitDisposition.STARTED
    without_gate = await gate_store.begin_execution(execution_claim)
    assert without_gate.disposition is ReviewExecutionPermitDisposition.DEFERRED
    assert without_gate.blocker_code == "review_execution_witness_running"

    reviewing = await store.load(key)
    await store.commit(
        priority_claim,
        transition,
        expected_revision=reviewing.state_revision,
    )
    repeated = await gate_store.begin_execution(execution_claim)

    assert repeated.disposition is ReviewExecutionPermitDisposition.DEFERRED
    assert repeated.blocker_code == "review_execution_witness_running"
    assert _gate_row(database, key)["gate_status"] == "requested"


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate_status", ("running", "unknown"))
async def test_finishing_one_duplicate_witness_does_not_terminalize_the_gate(
    tmp_path: Path,
    duplicate_status: str,
) -> None:
    """A stale live or unknown run remains a blocker after this task exits."""

    (
        database,
        store,
        key,
        _generation,
        priority_claim,
        transition,
        review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    effect_store = SQLiteDurableEffectStore(
        database,
        clock=lambda: now[0],
        contract_authority=builtin_effect_contract_authority(),
    )
    effect_claim = await effect_store.claim_next(worker_id="review-worker")
    assert effect_claim is not None
    execution_claim = _execution_claim(effect_claim)
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    assert (
        await gate_store.begin_execution(execution_claim)
    ).disposition is ReviewExecutionPermitDisposition.STARTED

    reviewing = await store.load(key)
    await store.commit(
        priority_claim,
        transition,
        expected_revision=reviewing.state_revision,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_review_execution_runs (
                profile_id, session_id, ownership_generation,
                review_effect_id, review_operation_id, review_effect_kind,
                review_contract_version, review_contract_signature,
                claim_id, worker_id, execution_status, started_at,
                unknown_at, unknown_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'duplicate-claim',
                      'duplicate-worker', ?, 100, ?, ?)
            """,
            (
                key.profile_id,
                key.session_id,
                execution_claim.ownership_generation,
                review_effect_id,
                execution_claim.review_operation_id,
                execution_claim.review_effect_kind,
                execution_claim.review_contract_version,
                execution_claim.review_contract_signature,
                duplicate_status,
                100.0 if duplicate_status == "unknown" else None,
                (
                    "review_execution_lease_expired_before_handler_terminal"
                    if duplicate_status == "unknown"
                    else ""
                ),
            ),
        )

    finished = await gate_store.finish_execution(execution_claim)

    assert finished.disposition is ReviewExecutionPermitDisposition.CANCELLED
    assert _gate_row(database, key)["gate_status"] == "cancelled"
    with database.connect() as conn:
        remaining = conn.execute(
            """
            SELECT execution_status
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
              AND claim_id = 'duplicate-claim'
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert remaining is not None and remaining["execution_status"] == duplicate_status


@pytest.mark.asyncio
async def test_stale_effect_store_fence_cannot_regress_a_terminal_gate(
    tmp_path: Path,
) -> None:
    """A late release observes terminal cancellation without reopening its gate."""

    (
        database,
        store,
        key,
        _generation,
        priority_claim,
        transition,
        _review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    effect_store = SQLiteDurableEffectStore(
        database,
        clock=lambda: now[0],
        contract_authority=builtin_effect_contract_authority(),
    )
    effect_claim = await effect_store.claim_next(worker_id="review-worker")
    assert effect_claim is not None
    reviewing = await store.load(key)
    await store.commit(
        priority_claim,
        transition,
        expected_revision=reviewing.state_revision,
    )
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    assert (
        await gate_store.begin_execution(_execution_claim(effect_claim))
    ).disposition is ReviewExecutionPermitDisposition.CANCELLED
    assert _gate_row(database, key)["gate_status"] == "terminal"

    late_release = await effect_store.release_for_retry(
        effect_claim,
        error="late executor cleanup",
        available_at=101.0,
    )

    assert late_release is not None
    assert late_release.status is EffectSettlementStatus.CANCELLED
    assert _gate_row(database, key)["gate_status"] == "terminal"


@pytest.mark.asyncio
async def test_stale_effect_store_fence_rejects_terminal_gate_with_unknown_witness(
    tmp_path: Path,
) -> None:
    """A corrupt terminal gate cannot hide an unresolved model execution."""

    (
        database,
        store,
        key,
        _generation,
        priority_claim,
        transition,
        review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    effect_store = SQLiteDurableEffectStore(
        database,
        clock=lambda: now[0],
        contract_authority=builtin_effect_contract_authority(),
    )
    effect_claim = await effect_store.claim_next(worker_id="review-worker")
    assert effect_claim is not None
    execution_claim = _execution_claim(effect_claim)
    reviewing = await store.load(key)
    await store.commit(
        priority_claim,
        transition,
        expected_revision=reviewing.state_revision,
    )
    gate_store = SQLiteReviewExecutionGateStore(database, clock=lambda: now[0])
    assert (
        await gate_store.begin_execution(execution_claim)
    ).disposition is ReviewExecutionPermitDisposition.CANCELLED
    assert _gate_row(database, key)["gate_status"] == "terminal"
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_review_execution_runs (
                profile_id, session_id, ownership_generation,
                review_effect_id, review_operation_id, review_effect_kind,
                review_contract_version, review_contract_signature,
                claim_id, worker_id, execution_status, started_at,
                unknown_at, unknown_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'unknown-review-claim',
                      'unknown-review-worker', 'unknown', 100.0, 100.0,
                      'review_execution_lease_expired_before_handler_terminal')
            """,
            (
                key.profile_id,
                key.session_id,
                execution_claim.ownership_generation,
                review_effect_id,
                execution_claim.review_operation_id,
                execution_claim.review_effect_kind,
                execution_claim.review_contract_version,
                execution_claim.review_contract_signature,
            ),
        )

    with pytest.raises(EffectStoreConflict, match="gate changed"):
        await effect_store.release_for_retry(
            effect_claim,
            error="late executor cleanup",
            available_at=101.0,
        )

    assert _gate_row(database, key)["gate_status"] == "terminal"


@pytest.mark.asyncio
async def test_executor_cancels_after_real_review_task_exit_without_mailbox_outcome(
    tmp_path: Path,
) -> None:
    """A gate raised during model work wins over its later completion result."""

    (
        database,
        store,
        key,
        _generation,
        priority_claim,
        transition,
        review_effect_id,
        _review_operation_id,
        _mailbox_count_before,
    ) = await _prepare_interruption(tmp_path, target_status="pending")
    now = [100.0]
    authority = builtin_effect_contract_authority()
    effect_store = SQLiteDurableEffectStore(
        database,
        clock=lambda: now[0],
        contract_authority=authority,
    )
    handler_started = asyncio.Event()
    allow_handler_exit = asyncio.Event()

    async def review_handler(_context: object) -> EffectHandlerResult:
        handler_started.set()
        await allow_handler_exit.wait()
        return EffectHandlerResult(payload={})

    review_contract = builtin_effect_contract("run_review_workflow")
    handlers = EffectHandlerRegistry(contract_authority=authority)
    handlers.register(
        review_contract.effect_kind,
        review_handler,
        contract=review_contract,
    )
    wake_target = _WakeTarget()
    executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        session_registry=wake_target,
        renew_interval_seconds=10.0,
        review_execution_gate_store=SQLiteReviewExecutionGateStore(
            database,
            clock=lambda: now[0],
        ),
        clock=lambda: now[0],
    )

    run_task = asyncio.create_task(executor.run_once(lane=EffectLane.PLANNER))
    await asyncio.wait_for(handler_started.wait(), timeout=1.0)
    reviewing = await store.load(key)
    await store.commit(
        priority_claim,
        transition,
        expected_revision=reviewing.state_revision,
    )
    mailbox_count = _mailbox_count(database, key)

    allow_handler_exit.set()
    result = await asyncio.wait_for(run_task, timeout=1.0)

    assert result.status is EffectRunStatus.CANCELLED
    assert _outbox_row(database, key, review_effect_id)["status"] == "cancelled"
    assert _gate_row(database, key)["gate_status"] == "terminal"
    assert _mailbox_count(database, key) == mailbox_count
    assert wake_target.woken == []
    with database.connect() as conn:
        run = conn.execute(
            """
            SELECT execution_status
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert run is not None and run["execution_status"] == "cancelled"
