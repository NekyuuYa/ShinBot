"""Integration tests for durable actor ReviewDue scanning and wake debt."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionReviewSchedule,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.review_due_scanner import (
    GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID,
    REVIEW_DUE_EVENT_KIND,
    REVIEW_DUE_EVENT_SOURCE,
    DurableReviewDueRepository,
    DurableReviewDueScannerService,
    ReviewDueDisposition,
    ReviewDueWakeError,
    review_due_event_id,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager


@dataclass(slots=True)
class _WakeTarget:
    failures_remaining: int = 0
    calls: list[SessionKey] = field(default_factory=list)

    async def wake(self, key: SessionKey) -> None:
        self.calls.append(key)
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError("synthetic wake failure")


def _make_runtime(
    tmp_path: Path,
    now: list[float],
) -> tuple[DatabaseManager, SQLiteSessionActorStore]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database, SQLiteSessionActorStore(database, clock=lambda: now[0])


async def _seed_due_schedule(
    database: DatabaseManager,
    store: SQLiteSessionActorStore,
    *,
    key: SessionKey,
    plan_id: str,
) -> int:
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="review due scanner test",
        legacy_session_id=f"legacy:{key.profile_id}:{key.session_id}",
    ).ownership
    generation = ownership.generation
    await store.ensure(key, ownership_generation=generation)
    event = SessionEventEnvelope(
        event_id=f"seed-schedule:{plan_id}",
        key=key,
        kind="SeedReviewSchedule",
        ownership_generation=generation,
        source="integration-test",
        occurred_at=1.0,
        trace_id=f"trace:seed:{plan_id}",
        available_at=1.0,
        created_at=1.0,
    )
    await store.enqueue(event)
    claim = await store.claim_next(key, worker_id="schedule-seeder")
    assert claim is not None
    aggregate = await store.load(key)
    target = aggregate.advance(
        current_plan_id=plan_id,
        review_plan_revision=aggregate.review_plan_revision + 1,
        review_plan={"plan_id": plan_id},
    )
    await store.commit(
        claim,
        SessionTransition(
            aggregate=target,
            disposition="review_schedule_seeded",
            caused_plan_id=plan_id,
            review_schedules=(
                SessionReviewSchedule(
                    plan_id=plan_id,
                    plan_revision=target.review_plan_revision,
                    applied_delay_seconds=0.0,
                    trigger="test_due",
                    outcome="planned",
                    source="integration-test",
                    reason="test_due",
                ),
            ),
        ),
        expected_revision=aggregate.state_revision,
    )
    return generation


def _repository(
    database: DatabaseManager,
    now: list[float],
    *,
    profile_id: str | None = None,
) -> DurableReviewDueRepository:
    return DurableReviewDueRepository(
        database,
        retry_base_seconds=5.0,
        retry_max_seconds=20.0,
        clock=lambda: now[0],
        profile_id=profile_id,
    )


def _mailbox_rows(database: DatabaseManager) -> list[object]:
    with database.connect() as conn:
        return list(
            conn.execute(
                """
                SELECT * FROM agent_session_mailbox
                WHERE kind = 'ReviewDue'
                ORDER BY mailbox_id
                """
            ).fetchall()
        )


def _service_health_row(database: DatabaseManager) -> sqlite3.Row:
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM agent_runtime_service_health
            WHERE profile_id = ?
              AND service_name = 'durable_review_due_scanner'
            """,
            (GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID,),
        ).fetchone()
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_double_scanner_and_restart_dispatch_one_exact_event(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "external-session")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="plan-a",
    )
    first = _repository(database, now)
    second = _repository(database, now)

    summaries = await asyncio.gather(
        asyncio.to_thread(partial(first.dispatch_due, limit=1)),
        asyncio.to_thread(partial(second.dispatch_due, limit=1)),
    )

    assert sum(summary.dispatched_count for summary in summaries) == 1
    assert sum(summary.attempted_count for summary in summaries) == 1
    rows = _mailbox_rows(database)
    assert len(rows) == 1
    row = rows[0]
    assert str(row["event_id"]) == review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=generation,
    )
    payload = json.loads(str(row["payload_json"]))
    assert payload["plan_id"] == "plan-a"
    assert payload["plan_revision"] == 1
    assert payload["ownership_generation"] == generation
    assert payload["attempt_count"] == 0
    restarted = _repository(database, now)
    assert restarted.dispatch_due(limit=10).attempted_count == 0
    assert restarted.pending_review_due_keys() == (key,)
    with database.connect() as conn:
        schedule = conn.execute(
            "SELECT status FROM agent_review_schedules WHERE plan_id = 'plan-a'"
        ).fetchone()
    assert schedule is not None
    assert str(schedule["status"]) == "claimed"


@pytest.mark.asyncio
async def test_stale_plan_is_superseded_without_review_due_mailbox(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "session-a")
    await _seed_due_schedule(database, store, key=key, plan_id="old-plan")
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET current_plan_id = 'new-plan', review_plan_revision = 2,
                review_plan_json = '{"plan_id":"new-plan"}',
                state_revision = state_revision + 1
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.superseded_count == 1
    assert summary.results[0].reason == "aggregate_current_plan_mismatch"
    assert _mailbox_rows(database) == []
    with database.connect() as conn:
        schedule = conn.execute(
            "SELECT status, last_error FROM agent_review_schedules"
        ).fetchone()
        event = conn.execute(
            """
            SELECT event_type, outcome, reason
            FROM agent_review_schedule_events
            WHERE source = 'durable_review_due_scanner'
            """
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == ("superseded", "aggregate_current_plan_mismatch")
    assert event is not None
    assert tuple(event) == (
        "superseded",
        "superseded",
        "aggregate_current_plan_mismatch",
    )


@pytest.mark.asyncio
async def test_migration_freezes_schedule_then_abort_dispatches_new_generation(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "session-a")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="plan-a",
    )
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=generation,
        reason="test migration race",
    )
    with database.connect() as conn:
        frozen_before = conn.execute(
            """
            SELECT status, ownership_generation, available_at,
                   attempt_count, last_error, claim_owner, claim_until,
                   updated_at
            FROM agent_review_schedules
            """
        ).fetchone()
    assert frozen_before is not None

    skipped = _repository(database, now).dispatch_due(limit=1)

    assert skipped.fence_skipped_count == 1
    assert skipped.results[0].disposition is ReviewDueDisposition.FENCE_SKIPPED
    assert skipped.results[0].reason == "ownership_migrating"
    assert skipped.results[0].retry_at is None
    with database.connect() as conn:
        frozen_after = conn.execute(
            """
            SELECT status, ownership_generation, available_at,
                   attempt_count, last_error, claim_owner, claim_until,
                   updated_at
            FROM agent_review_schedules
            """
        ).fetchone()
    assert frozen_after is not None
    assert tuple(frozen_after) == tuple(frozen_before)
    aborted = database.agent_runtime_ownership.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="resume actor after test",
    )

    dispatched = _repository(database, now).dispatch_due(limit=1)

    assert dispatched.dispatched_count == 1
    assert dispatched.results[0].ownership_generation == aborted.generation
    payload = json.loads(str(_mailbox_rows(database)[0]["payload_json"]))
    assert payload["ownership_generation"] == aborted.generation


@pytest.mark.asyncio
async def test_refenced_old_due_event_is_failed_before_new_generation_dispatch(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "session-a")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="plan-a",
    )
    first = _repository(database, now).dispatch_due(limit=1)
    assert first.dispatched_count == 1
    old_event_id = first.results[0].event_id
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=generation,
        reason="fence claimed due event",
    )
    aborted = database.agent_runtime_ownership.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="return to actor runtime",
    )

    second = _repository(database, now).dispatch_due(limit=1)

    assert second.dispatched_count == 1
    assert second.results[0].ownership_generation == aborted.generation
    assert second.results[0].event_id != old_event_id
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id, ownership_generation, status, last_error,
                   payload_json
            FROM agent_session_mailbox
            WHERE kind = 'ReviewDue'
            ORDER BY mailbox_id
            """
        ).fetchall()
    assert len(rows) == 2
    assert str(rows[0]["event_id"]) == old_event_id
    assert str(rows[0]["status"]) == "failed"
    assert str(rows[0]["last_error"]) == (
        "review_due_exact_plan_fence_superseded"
    )
    assert str(rows[1]["status"]) == "pending"
    assert int(rows[1]["ownership_generation"]) == aborted.generation
    assert json.loads(str(rows[1]["payload_json"]))["ownership_generation"] == (
        aborted.generation
    )


@pytest.mark.asyncio
async def test_unavailable_first_row_is_retried_without_blocking_next_profile(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    missing_key = SessionKey("a-profile", "same-session")
    valid_key = SessionKey("b-profile", "same-session")
    await _seed_due_schedule(database, store, key=missing_key, plan_id="plan-a")
    await _seed_due_schedule(database, store, key=valid_key, plan_id="plan-b")
    with database.connect() as conn:
        conn.execute(
            """
            DELETE FROM agent_session_runtime_ownership
            WHERE profile_id = ? AND session_id = ?
            """,
            (missing_key.profile_id, missing_key.session_id),
        )
        missing_before = conn.execute(
            """
            SELECT ownership_generation, available_at, attempt_count,
                   last_error, updated_at
            FROM agent_review_schedules
            WHERE profile_id = ? AND session_id = ?
            """,
            (missing_key.profile_id, missing_key.session_id),
        ).fetchone()
    assert missing_before is not None

    summary = _repository(database, now).dispatch_due(limit=2)

    assert [result.disposition for result in summary.results] == [
        ReviewDueDisposition.FENCE_SKIPPED,
        ReviewDueDisposition.DISPATCHED,
    ]
    assert summary.results[0].reason == "ownership_missing"
    assert summary.results[1].key == valid_key
    rows = _mailbox_rows(database)
    assert len(rows) == 1
    assert str(rows[0]["profile_id"]) == valid_key.profile_id
    with database.connect() as conn:
        missing_after = conn.execute(
            """
            SELECT ownership_generation, available_at, attempt_count,
                   last_error, updated_at
            FROM agent_review_schedules
            WHERE profile_id = ? AND session_id = ?
            """,
            (missing_key.profile_id, missing_key.session_id),
        ).fetchone()
    assert missing_after is not None
    assert tuple(missing_after) == tuple(missing_before)


@pytest.mark.asyncio
async def test_unavailable_registry_and_wake_failure_leave_recoverable_debt(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "session-a")
    await _seed_due_schedule(database, store, key=key, plan_id="plan-a")
    repository = _repository(database, now)
    no_registry = DurableReviewDueScannerService(repository, wake_target=None)

    summary = await no_registry.run_once()

    assert summary.dispatched_count == 1
    assert no_registry.health_snapshot().success_count == 1
    assert repository.pending_review_due_keys() == (key,)
    durable_health = _service_health_row(database)
    assert str(durable_health["status"]) == "running"
    assert int(durable_health["scan_count"]) == 1
    assert int(durable_health["due_seen_count"]) == 1
    assert int(durable_health["dispatch_count"]) == 1
    assert int(durable_health["skip_count"]) == 0

    wake_target = _WakeTarget(failures_remaining=1)
    restarted = DurableReviewDueScannerService(
        _repository(database, now),
        wake_target=wake_target,
    )
    with pytest.raises(ReviewDueWakeError) as exc_info:
        await restarted.run_once()
    assert exc_info.value.keys == (key,)
    assert restarted.health_snapshot().consecutive_failures == 1
    assert len(_mailbox_rows(database)) == 1
    durable_health = _service_health_row(database)
    assert str(durable_health["status"]) == "degraded"
    assert int(durable_health["scan_count"]) == 2
    assert int(durable_health["due_seen_count"]) == 1
    assert int(durable_health["dispatch_count"]) == 1
    assert int(durable_health["consecutive_failures"]) == 1
    assert str(durable_health["last_error_code"]) == "ReviewDueWakeError"

    recovered = await restarted.run_once()

    assert recovered.dispatched_count == 0
    assert wake_target.calls == [key, key]
    assert restarted.health_snapshot().consecutive_failures == 0
    assert len(_mailbox_rows(database)) == 1
    durable_health = _service_health_row(database)
    assert str(durable_health["status"]) == "running"
    assert int(durable_health["scan_count"]) == 3
    assert int(durable_health["due_seen_count"]) == 1
    assert int(durable_health["dispatch_count"]) == 1
    assert int(durable_health["consecutive_failures"]) == 0


@pytest.mark.asyncio
async def test_same_external_session_profiles_dispatch_independently(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key_a = SessionKey("profile-a", "same-session")
    key_b = SessionKey("profile-b", "same-session")
    await _seed_due_schedule(database, store, key=key_a, plan_id="plan-a")
    await _seed_due_schedule(database, store, key=key_b, plan_id="plan-b")

    summary = _repository(database, now).dispatch_due(limit=2)

    assert summary.dispatched_count == 2
    assert set(summary.dispatched_keys) == {key_a, key_b}
    rows = _mailbox_rows(database)
    assert len(rows) == 2
    assert len({str(row["event_id"]) for row in rows}) == 2
    assert {
        (str(row["profile_id"]), str(row["session_id"])) for row in rows
    } == {
        (key_a.profile_id, key_a.session_id),
        (key_b.profile_id, key_b.session_id),
    }


@pytest.mark.asyncio
async def test_deterministic_mailbox_payload_conflict_fails_closed(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "session-a")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="plan-a",
    )
    event_id = review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=generation,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json, causation_id,
                correlation_id, trace_id, status, attempt_count, available_at,
                claim_id, lease_owner, lease_until, created_at, handled_at,
                last_error
            ) VALUES (?, ?, ?, ?, ?, 'conflicting-source', 100, '{}',
                      'plan-a', 'plan-a', ?, 'pending', 0, 100,
                      '', '', NULL, 100, NULL, '')
            """,
            (
                event_id,
                key.profile_id,
                key.session_id,
                generation,
                REVIEW_DUE_EVENT_KIND,
                event_id,
            ),
        )

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.deferred_count == 1
    assert summary.results[0].reason == "mailbox_identity_conflict"
    assert summary.results[0].retry_at == 105.0

    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, available_at, attempt_count, last_error
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
        events = conn.execute(
            """
            SELECT COUNT(*) AS count FROM agent_review_schedule_events
            WHERE source = 'durable_review_due_scanner'
            """
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == (
        "scheduled",
        105.0,
        1,
        "mailbox_identity_conflict",
    )
    assert events is not None
    assert int(events["count"]) == 0


@pytest.mark.asyncio
async def test_mailbox_insert_does_not_ignore_other_unique_constraints(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "session-a")
    blocker_key = SessionKey("profile-b", "session-b")
    await _seed_due_schedule(database, store, key=key, plan_id="plan-a")
    blocker_generation = await _seed_due_schedule(
        database,
        store,
        key=blocker_key,
        plan_id="plan-b",
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json, causation_id,
                correlation_id, trace_id, status, attempt_count, available_at,
                claim_id, lease_owner, lease_until, created_at, handled_at,
                last_error
            ) VALUES ('blocker', ?, ?, ?, ?, 'integration-test', 100, '{}',
                      '', '', 'blocker', 'pending', 0, 100,
                      '', '', NULL, 100, NULL, '')
            """,
            (
                blocker_key.profile_id,
                blocker_key.session_id,
                blocker_generation,
                REVIEW_DUE_EVENT_KIND,
            ),
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX test_mailbox_kind_unique
            ON agent_session_mailbox(kind)
            WHERE kind = 'ReviewDue'
            """
        )
        before = conn.execute(
            """
            SELECT status, available_at, attempt_count, last_error, updated_at
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert before is not None

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        _repository(database, now, profile_id=key.profile_id).dispatch_due(limit=1)

    with database.connect() as conn:
        after = conn.execute(
            """
            SELECT status, available_at, attempt_count, last_error, updated_at
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert after is not None
    assert tuple(after) == tuple(before)
    assert len(_mailbox_rows(database)) == 1


@pytest.mark.asyncio
async def test_schedule_event_insert_does_not_ignore_other_unique_constraints(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "session-a")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="plan-a",
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_review_schedule_events (
                schedule_event_id, profile_id, session_id,
                ownership_generation, event_id, event_type, source, created_at
            ) VALUES ('blocker', ?, ?, ?, 'blocker', 'blocker', ?, 100)
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                REVIEW_DUE_EVENT_SOURCE,
            ),
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX test_schedule_event_source_unique
            ON agent_review_schedule_events(source)
            """
        )
        before = conn.execute(
            """
            SELECT status, available_at, attempt_count, last_error, updated_at
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert before is not None

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        _repository(database, now).dispatch_due(limit=1)

    with database.connect() as conn:
        after = conn.execute(
            """
            SELECT status, available_at, attempt_count, last_error, updated_at
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert after is not None
    assert tuple(after) == tuple(before)
    assert _mailbox_rows(database) == []
