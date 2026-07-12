from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.events import (
    EventEnqueueResult,
    MailboxEventStatus,
    ReviewScheduleStatus,
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionOperationStatus,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.store import (
    AggregateVersionConflict,
    DurableRecordConflict,
    MailboxEventConflict,
    MailboxLeaseConflict,
    SQLiteSessionActorStore,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager


class _OwnershipTestStore(SQLiteSessionActorStore):
    """Keep pre-ownership store cases focused on their original contract."""

    async def enqueue(
        self,
        envelope: SessionEventEnvelope,
    ) -> EventEnqueueResult:
        ownership = self._database.agent_runtime_ownership.get(envelope.key)
        if ownership is None:
            ownership = self._database.agent_runtime_ownership.claim(
                envelope.key,
                AgentRuntimeOwnershipMode.ACTOR_V2,
                reason="session actor store integration test",
                legacy_session_id=(
                    f"legacy:{envelope.key.profile_id}:{envelope.key.session_id}"
                ),
            ).ownership
        return await super().enqueue(
            replace(envelope, ownership_generation=ownership.generation)
        )


def _make_store(tmp_path: Path) -> tuple[DatabaseManager, SQLiteSessionActorStore]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database, _OwnershipTestStore(
        database,
        lease_seconds=30.0,
        retry_delay_seconds=0.0,
        clock=lambda: 100.0,
    )


def _event(event_id: str, key: SessionKey) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="message_received",
        source="test",
        payload={"event_id": event_id},
        trace_id=f"trace:{event_id}",
    )


@pytest.mark.asyncio
async def test_sqlite_session_store_isolates_same_session_across_profiles(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key_a = SessionKey("profile-a", "instance:group:room")
    key_b = SessionKey("profile-b", "instance:group:room")
    await store.enqueue(_event("same-event", key_a))
    await store.enqueue(_event("same-event", key_b))

    claim_a = await store.claim_next(key_a, worker_id="worker-a")
    claim_b = await store.claim_next(key_b, worker_id="worker-b")
    assert claim_a is not None
    assert claim_b is not None
    aggregate_a = await store.load(key_a)
    aggregate_b = await store.load(key_b)
    await store.commit(
        claim_a,
        SessionTransition(
            aggregate=aggregate_a.advance(data={"owner": "profile-a"}),
            disposition="profile_a_applied",
            reason="test_a",
        ),
        expected_revision=aggregate_a.state_revision,
    )
    await store.commit(
        claim_b,
        SessionTransition(
            aggregate=aggregate_b.advance(data={"owner": "profile-b"}),
            disposition="profile_b_applied",
            reason="test_b",
        ),
        expected_revision=aggregate_b.state_revision,
    )

    assert (await store.load(key_a)).data == {"owner": "profile-a"}
    assert (await store.load(key_b)).data == {"owner": "profile-b"}
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT profile_id, session_id, state_revision, event_sequence
            FROM agent_session_aggregates
            ORDER BY profile_id
            """
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("profile-a", "instance:group:room", 1, 1),
        ("profile-b", "instance:group:room", 1, 1),
    ]


@pytest.mark.asyncio
async def test_sqlite_session_store_enqueue_is_durably_idempotent(
    tmp_path: Path,
) -> None:
    _database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    envelope = _event("same-event", key)

    first = await store.enqueue(envelope)
    duplicate = await store.enqueue(envelope)
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    await store.commit(
        claim,
        SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition="duplicate_test_applied",
        ),
        expected_revision=aggregate.state_revision,
    )
    completed_duplicate = await store.enqueue(envelope)

    assert first.inserted is True
    assert duplicate.inserted is False
    assert duplicate.status == MailboxEventStatus.PENDING
    assert completed_duplicate.inserted is False
    assert completed_duplicate.status == MailboxEventStatus.COMPLETED
    with pytest.raises(MailboxEventConflict):
        await store.enqueue(
            SessionEventEnvelope(
                event_id="same-event",
                key=key,
                kind="message_received",
                payload={"event_id": "different-payload"},
            )
        )
    with pytest.raises(MailboxEventConflict):
        await store.enqueue(replace(envelope, source="different-source"))
    other_profile = await store.enqueue(
        _event("same-event", SessionKey("profile-b", key.session_id))
    )
    assert other_profile.inserted is True


@pytest.mark.asyncio
async def test_sqlite_session_store_recovers_only_expired_claims(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    now = [100.0]
    store = _OwnershipTestStore(
        database,
        lease_seconds=10.0,
        retry_delay_seconds=0.0,
        clock=lambda: now[0],
    )
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("leased-event", key))
    first_claim = await store.claim_next(key, worker_id="worker-a")
    assert first_claim is not None

    assert await store.recover(key, worker_id="worker-b") == 0
    assert await store.claim_next(key, worker_id="worker-b") is None

    now[0] = 111.0
    assert await store.recover(key, worker_id="worker-b") == 1
    second_claim = await store.claim_next(key, worker_id="worker-b")
    assert second_claim is not None
    assert second_claim.attempt_count == 2


@pytest.mark.parametrize("record_kind", ["operation", "schedule"])
@pytest.mark.asyncio
async def test_sqlite_session_store_rejects_durable_id_owner_changes(
    tmp_path: Path,
    record_kind: str,
) -> None:
    _database, store = _make_store(tmp_path)
    key_a = SessionKey("profile-a", "instance:group:room")
    key_b = SessionKey("profile-b", "instance:group:room")
    await store.enqueue(_event("same-event", key_a))
    await store.enqueue(_event("same-event", key_b))
    claim_a = await store.claim_next(key_a, worker_id="worker-a")
    claim_b = await store.claim_next(key_b, worker_id="worker-b")
    assert claim_a is not None
    assert claim_b is not None
    aggregate_a = await store.load(key_a)
    aggregate_b = await store.load(key_b)
    operations: tuple[SessionOperation, ...] = ()
    schedules: tuple[SessionReviewSchedule, ...] = ()
    if record_kind == "operation":
        operations = (
            SessionOperation(operation_id="shared-id", kind="review"),
        )
    else:
        schedules = (
            SessionReviewSchedule(
                plan_id="shared-id",
                plan_revision=1,
                applied_delay_seconds=120.0,
            ),
        )
    target_a = aggregate_a.advance(state_changed=False)
    target_b = aggregate_b.advance(state_changed=False)
    caused_plan_id = ""
    if schedules:
        target_a = aggregate_a.advance(
            current_plan_id="shared-id",
            review_plan_revision=1,
            review_plan={"plan_id": "shared-id"},
        )
        target_b = aggregate_b.advance(
            current_plan_id="shared-id",
            review_plan_revision=1,
            review_plan={"plan_id": "shared-id"},
        )
        caused_plan_id = "shared-id"
    await store.commit(
        claim_a,
        SessionTransition(
            aggregate=target_a,
            disposition="durable_record_created",
            caused_plan_id=caused_plan_id,
            operations=operations,
            review_schedules=schedules,
        ),
        expected_revision=aggregate_a.state_revision,
    )

    with pytest.raises(DurableRecordConflict):
        await store.commit(
            claim_b,
            SessionTransition(
                aggregate=target_b,
                disposition="durable_record_created",
                caused_plan_id=caused_plan_id,
                operations=operations,
                review_schedules=schedules,
            ),
            expected_revision=aggregate_b.state_revision,
        )

    restored_b = await store.load(key_b)
    assert restored_b.state_revision == 0
    assert restored_b.event_sequence == 0


@pytest.mark.asyncio
async def test_sqlite_session_store_rejects_stale_aggregate_revision(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("stale-event", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    transition = SessionTransition(
        aggregate=aggregate.advance(state="review"),
        disposition="entered_review",
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state_revision = state_revision + 1
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )

    with pytest.raises(AggregateVersionConflict):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

    with database.connect() as conn:
        mailbox = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = ?",
            (claim.envelope.event_id,),
        ).fetchone()
        transitions = conn.execute("SELECT COUNT(*) FROM agent_state_transitions").fetchone()
    assert tuple(mailbox) == ("processing", claim.claim_id)
    assert transitions[0] == 0


@pytest.mark.asyncio
async def test_sqlite_session_store_rolls_back_all_atomic_commit_records(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("rollback-event", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    transition = SessionTransition(
        aggregate=aggregate.advance(state="active_chat"),
        disposition="review_completed",
        caused_operation_id="operation:rollback",
        effects=(
            SessionEffect(
                effect_id="effect:rollback",
                kind="start_workflow",
                contract_signature="test-start-workflow-v1",
            ),
        ),
        operations=(
            SessionOperation(
                operation_id="operation:rollback",
                kind="review",
                status=SessionOperationStatus.RUNNING,
            ),
        ),
        review_schedule_events=(
            # The frozen dataclass validates required fields, so force the SQL
            # rollback after earlier records via a duplicate journal identity.
            SessionReviewScheduleEvent(
                schedule_event_id="schedule-event:rollback",
                event_type="scheduled",
            ),
            SessionReviewScheduleEvent(
                schedule_event_id="schedule-event:rollback",
                event_type="scheduled",
            ),
        ),
        reason="review_complete",
    )

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

    restored = await store.load(key)
    assert restored.state == "idle"
    assert restored.state_revision == 0
    assert restored.event_sequence == 0
    with database.connect() as conn:
        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "agent_session_operations",
                "agent_state_transitions",
                "agent_review_schedule_events",
                "agent_effect_outbox",
            )
        }
        mailbox_status = conn.execute(
            "SELECT status FROM agent_session_mailbox WHERE event_id = 'rollback-event'"
        ).fetchone()[0]
    assert counts == {
        "agent_session_operations": 0,
        "agent_state_transitions": 0,
        "agent_review_schedule_events": 0,
        "agent_effect_outbox": 0,
    }
    assert mailbox_status == "processing"


@pytest.mark.asyncio
async def test_sqlite_session_store_commits_operation_schedule_journals_and_effect(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("commit-event", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    target = aggregate.advance(
        state="idle",
        current_plan_id="plan-1",
        review_plan_revision=1,
        review_plan={"plan_id": "plan-1", "next_review_at": 999.0},
        idle_planning_operation_id="operation-1",
    )

    committed = await store.commit(
        claim,
        SessionTransition(
            aggregate=target,
            disposition="idle_review_planned",
            caused_operation_id="operation-1",
            caused_plan_id="plan-1",
            effects=(
                SessionEffect(
                    effect_id="effect-1",
                    kind="schedule_review_timer",
                    contract_signature="test-schedule-review-v1",
                    operation_id="operation-1",
                ),
            ),
            operations=(
                SessionOperation(
                    operation_id="operation-1",
                    kind="idle_review_planning",
                    status=SessionOperationStatus.COMPLETED,
                    finished_at=100.0,
                ),
            ),
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="plan-1",
                    plan_revision=1,
                    trigger="active_chat_decay_exit",
                    outcome="planned",
                    source="llm",
                    requested_delay_seconds=120.0,
                    applied_delay_seconds=120.0,
                    reason="topic_settled",
                    model_execution_id="execution-1",
                ),
            ),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id="schedule-event-1",
                    event_type="scheduled",
                    plan_id="plan-1",
                    trigger="active_chat_decay_exit",
                    outcome="planned",
                    source="llm",
                    applied_delay_seconds=120.0,
                    scheduled_from=1.0,
                    next_review_at=2.0,
                    model_execution_id="execution-1",
                    operation_id="operation-1",
                ),
            ),
            result={"planning": "accepted"},
            reason="active_chat_decay_exit",
        ),
        expected_revision=aggregate.state_revision,
    )

    assert committed.state_revision == 1
    assert committed.event_sequence == 1
    assert committed.review_plan["scheduled_from"] == 100.0
    assert committed.review_plan["next_review_at"] == 220.0
    assert committed.review_plan["applied_delay_seconds"] == 120.0
    with database.connect() as conn:
        mailbox = conn.execute(
            "SELECT status, claim_id FROM agent_session_mailbox WHERE event_id = 'commit-event'"
        ).fetchone()
        operation = conn.execute(
            "SELECT kind, status FROM agent_session_operations WHERE operation_id = 'operation-1'"
        ).fetchone()
        schedule = conn.execute(
            "SELECT source, next_review_at FROM agent_review_schedules WHERE plan_id = 'plan-1'"
        ).fetchone()
        transition = conn.execute(
            """
            SELECT trigger, disposition, operation_id, plan_id
            FROM agent_state_transitions
            """
        ).fetchone()
        schedule_event = conn.execute(
            """
            SELECT event_type, model_execution_id, scheduled_from, next_review_at
            FROM agent_review_schedule_events
            """
        ).fetchone()
        effect = conn.execute(
            "SELECT kind, status FROM agent_effect_outbox WHERE effect_id = 'effect-1'"
        ).fetchone()
    assert tuple(mailbox) == ("completed", "")
    assert tuple(operation) == ("idle_review_planning", "completed")
    assert tuple(schedule) == ("llm", 220.0)
    assert tuple(transition) == (
        "active_chat_decay_exit",
        "idle_review_planned",
        "operation-1",
        "plan-1",
    )
    assert tuple(schedule_event) == ("scheduled", "execution-1", 100.0, 220.0)
    assert tuple(effect) == ("schedule_review_timer", "pending")


@pytest.mark.asyncio
async def test_sqlite_session_store_fails_only_the_owned_mailbox_event(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key_a = SessionKey("profile-a", "instance:group:room")
    key_b = SessionKey("profile-b", "instance:group:room")
    await store.enqueue(_event("same-event", key_a))
    await store.enqueue(_event("same-event", key_b))
    claim_a = await store.claim_next(key_a, worker_id="worker-a")
    claim_b = await store.claim_next(key_b, worker_id="worker-b")
    assert claim_a is not None
    assert claim_b is not None

    await store.fail(claim_a, error="invalid_transition")

    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT profile_id, status, claim_id, lease_owner, last_error
            FROM agent_session_mailbox
            WHERE session_id = ? AND event_id = ?
            ORDER BY profile_id
            """,
            (key_a.session_id, claim_a.envelope.event_id),
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("profile-a", "failed", "", "", "invalid_transition"),
        ("profile-b", "processing", claim_b.claim_id, "worker-b", ""),
    ]
    assert await store.claim_next(key_a, worker_id="worker-c") is None
    with pytest.raises(MailboxLeaseConflict, match="not owned"):
        await store.fail(claim_a, error="duplicate")

    failed_aggregate = await store.load(key_a)
    assert failed_aggregate.state_revision == 0
    assert failed_aggregate.event_sequence == 1
    await store.enqueue(_event("healthy-event", key_a))
    healthy_claim = await store.claim_next(key_a, worker_id="worker-c")
    assert healthy_claim is not None
    await store.commit(
        healthy_claim,
        SessionTransition(
            aggregate=failed_aggregate.advance(state_changed=False),
            disposition="healthy_noop",
        ),
        expected_revision=failed_aggregate.state_revision,
    )
    with database.connect() as conn:
        journal = conn.execute(
            """
            SELECT event_id, disposition, state_revision, event_sequence
            FROM agent_state_transitions
            WHERE profile_id = ? AND session_id = ?
            ORDER BY event_sequence
            """,
            (key_a.profile_id, key_a.session_id),
        ).fetchall()
    assert [tuple(row) for row in journal] == [
        ("same-event", "failed", 0, 1),
        ("healthy-event", "healthy_noop", 0, 2),
    ]


@pytest.mark.asyncio
async def test_sqlite_session_store_preserves_zero_operation_fences_and_rejects_regression(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("operation-start", key))
    first_claim = await store.claim_next(key, worker_id="worker")
    assert first_claim is not None
    aggregate = await store.load(key)
    await store.commit(
        first_claim,
        SessionTransition(
            aggregate=aggregate.advance(active_epoch=3, activity_generation=4),
            disposition="operation_started",
            caused_operation_id="operation-1",
            operations=(
                SessionOperation(
                    operation_id="operation-1",
                    kind="review",
                    status=SessionOperationStatus.RUNNING,
                    state_revision=0,
                    active_epoch=0,
                    activity_generation=0,
                    started_at=0.0,
                ),
            ),
        ),
        expected_revision=aggregate.state_revision,
    )
    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT status, state_revision, active_epoch, activity_generation, started_at
            FROM agent_session_operations WHERE operation_id = 'operation-1'
            """
        ).fetchone()
    assert tuple(operation) == ("running", 0, 0, 0, 0.0)

    await store.enqueue(_event("operation-regression", key))
    second_claim = await store.claim_next(key, worker_id="worker")
    assert second_claim is not None
    current = await store.load(key)
    with pytest.raises(DurableRecordConflict, match="cannot move backwards"):
        await store.commit(
            second_claim,
            SessionTransition(
                aggregate=current.advance(state_changed=False),
                disposition="operation_regressed",
                caused_operation_id="operation-1",
                operations=(
                    SessionOperation(
                        operation_id="operation-1",
                        kind="review",
                        status=SessionOperationStatus.PENDING,
                    ),
                ),
            ),
            expected_revision=current.state_revision,
        )
    restored = await store.load(key)
    assert restored.state_revision == current.state_revision
    assert restored.event_sequence == current.event_sequence


@pytest.mark.parametrize("revision_delta", [0, 1])
@pytest.mark.asyncio
async def test_sqlite_session_store_enforces_canonical_aggregate_diff(
    tmp_path: Path,
    revision_delta: int,
) -> None:
    _database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event(f"canonical-{revision_delta}", key))
    claim = await store.claim_next(key, worker_id="worker")
    assert claim is not None
    aggregate = await store.load(key)
    target = replace(
        aggregate,
        state="review" if revision_delta == 0 else aggregate.state,
        state_revision=aggregate.state_revision + revision_delta,
        event_sequence=aggregate.event_sequence + 1,
    )

    with pytest.raises(ValueError, match="canonical aggregate diff"):
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="malformed_transition",
            ),
            expected_revision=aggregate.state_revision,
        )


@pytest.mark.asyncio
async def test_sqlite_session_store_supersedes_plans_and_keeps_terminal_status(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")

    async def commit_plan(plan_id: str, plan_revision: int, event_id: str) -> None:
        await store.enqueue(_event(event_id, key))
        claim = await store.claim_next(key, worker_id="worker")
        assert claim is not None
        current = await store.load(key)
        target = current.advance(
            current_plan_id=plan_id,
            review_plan_revision=plan_revision,
            review_plan={"plan_id": plan_id},
        )
        await store.commit(
            claim,
            SessionTransition(
                aggregate=target,
                disposition="review_planned",
                caused_plan_id=plan_id,
                review_schedules=(
                    SessionReviewSchedule(
                        plan_id=plan_id,
                        plan_revision=plan_revision,
                        applied_delay_seconds=30.0,
                    ),
                ),
            ),
            expected_revision=current.state_revision,
        )

    await commit_plan("plan-1", 1, "plan-one")
    await commit_plan("plan-2", 2, "plan-two")
    current = await store.load(key)
    assert current.current_plan_id == "plan-2"
    assert current.review_plan_revision == 2

    await store.enqueue(_event("plan-completed", key))
    complete_claim = await store.claim_next(key, worker_id="worker")
    assert complete_claim is not None
    await store.commit(
        complete_claim,
        SessionTransition(
            aggregate=current.advance(state_changed=False),
            disposition="review_completed",
            caused_plan_id="plan-2",
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="plan-2",
                    plan_revision=2,
                    applied_delay_seconds=30.0,
                    status=ReviewScheduleStatus.COMPLETED,
                ),
            ),
        ),
        expected_revision=current.state_revision,
    )

    terminal = await store.load(key)
    await store.enqueue(_event("plan-regression", key))
    regression_claim = await store.claim_next(key, worker_id="worker")
    assert regression_claim is not None
    with pytest.raises(DurableRecordConflict, match="terminal state"):
        await store.commit(
            regression_claim,
            SessionTransition(
                aggregate=terminal.advance(state_changed=False),
                disposition="review_rescheduled",
                caused_plan_id="plan-2",
                review_schedules=(
                    SessionReviewSchedule(
                        plan_id="plan-2",
                        plan_revision=2,
                        applied_delay_seconds=30.0,
                    ),
                ),
            ),
            expected_revision=terminal.state_revision,
        )

    with database.connect() as conn:
        schedules = conn.execute(
            """
            SELECT plan_id, plan_revision, status
            FROM agent_review_schedules
            ORDER BY plan_revision
            """
        ).fetchall()
        superseded = conn.execute(
            """
            SELECT plan_id, event_type, metadata_json
            FROM agent_review_schedule_events
            WHERE event_type = 'superseded'
            """
        ).fetchone()
    assert [tuple(row) for row in schedules] == [
        ("plan-1", 1, "superseded"),
        ("plan-2", 2, "completed"),
    ]
    assert tuple(superseded) == (
        "plan-1",
        "superseded",
        '{"superseded_by_plan_id":"plan-2"}',
    )


@pytest.mark.asyncio
async def test_sqlite_session_store_preserves_operation_provenance_on_completion(
    tmp_path: Path,
) -> None:
    database, store = _make_store(tmp_path)
    key = SessionKey("profile-a", "instance:group:room")
    await store.enqueue(_event("operation-created", key))
    created_claim = await store.claim_next(key, worker_id="worker")
    assert created_claim is not None
    aggregate = await store.load(key)
    await store.commit(
        created_claim,
        SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition="operation_created",
            caused_operation_id="operation-audit",
            operations=(
                SessionOperation(
                    operation_id="operation-audit",
                    kind="idle_review_planning",
                    status=SessionOperationStatus.RUNNING,
                    metadata={"deadline_at": 130.0, "planning_input": "tail-v1"},
                ),
            ),
        ),
        expected_revision=aggregate.state_revision,
    )

    await store.enqueue(_event("operation-completed", key))
    completed_claim = await store.claim_next(key, worker_id="worker")
    assert completed_claim is not None
    current = await store.load(key)
    await store.commit(
        completed_claim,
        SessionTransition(
            aggregate=current.advance(state_changed=False),
            disposition="operation_completed",
            caused_operation_id="operation-audit",
            operations=(
                SessionOperation(
                    operation_id="operation-audit",
                    kind="idle_review_planning",
                    status=SessionOperationStatus.COMPLETED,
                    metadata={"result": "planned"},
                ),
            ),
        ),
        expected_revision=current.state_revision,
    )

    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT status, launched_by_event_id, metadata_json
            FROM agent_session_operations
            WHERE operation_id = 'operation-audit'
            """
        ).fetchone()
    assert tuple(operation) == (
        "completed",
        "operation-created",
        '{"deadline_at":130.0,"planning_input":"tail-v1","result":"planned"}',
    )

    await store.enqueue(_event("operation-tampered", key))
    tampered_claim = await store.claim_next(key, worker_id="worker")
    assert tampered_claim is not None
    terminal = await store.load(key)
    with pytest.raises(DurableRecordConflict, match="immutable fences"):
        await store.commit(
            tampered_claim,
            SessionTransition(
                aggregate=terminal.advance(state_changed=False),
                disposition="operation_tampered",
                caused_operation_id="operation-audit",
                operations=(
                    SessionOperation(
                        operation_id="operation-audit",
                        kind="idle_review_planning",
                        status=SessionOperationStatus.COMPLETED,
                        active_epoch=99,
                    ),
                ),
            ),
            expected_revision=terminal.state_revision,
        )
