"""Integration tests for durable actor ReviewDue scanning and wake debt."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from functools import partial
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.manual_review import (
    MANUAL_REVIEW_EVENT_KIND,
    MANUAL_REVIEW_EVENT_SOURCE,
    ManualReviewAdmissionRequiredError,
    ManualReviewRequest,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)
from shinbot.agent.runtime.session_actor.review_due_scanner import (
    GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID,
    REVIEW_DUE_EVENT_KIND,
    REVIEW_DUE_EVENT_SOURCE,
    DurableReviewDueRepository,
    DurableReviewDueScannerService,
    ManualReviewAdmissionDisposition,
    ManualReviewAdmissionError,
    ManualReviewAdmissionService,
    ReviewDueConflict,
    ReviewDueDisposition,
    review_due_event_id,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMigrationConflict,
    AgentRuntimeOwnershipMode,
)
from shinbot.core.dispatch.mailbox_handoff import (
    MailboxHandoffEvidenceState,
    MailboxHandoffState,
)
from shinbot.persistence import DatabaseManager

_MAILBOX_RAW_LOGICAL_KEY_INDEX = "idx_agent_session_mailbox_raw_logical_key"
_SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX = (
    "idx_agent_review_schedule_events_raw_logical_key"
)
_MANUAL_REVIEW_REQUEST_INDEX = "idx_agent_session_mailbox_manual_review_request"
_MANUAL_REVIEW_REQUEST_UNIQUE_INDEX = (
    "idx_agent_session_mailbox_manual_review_request_unique"
)


class _MailboxHandoffNotifier:
    """Capture advisory mailbox ids without implementing a wake target."""

    def __init__(self, *, fail: bool = False) -> None:
        self.mailbox_ids: list[int] = []
        self._fail = fail

    async def notify(self, mailbox_id: int) -> None:
        self.mailbox_ids.append(mailbox_id)
        if self._fail:
            raise RuntimeError("synthetic mailbox handoff notifier failure")


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
    admission_grant: ActorV2AdmissionGrant | None = None,
) -> int:
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="review due scanner test",
        legacy_session_id=f"legacy:{key.profile_id}:{key.session_id}",
        admission_grant=admission_grant,
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
    plan_revision = aggregate.review_plan_revision + 1
    target = aggregate.advance(
        current_plan_id=plan_id,
        review_plan_revision=plan_revision,
        review_plan={
            "plan_id": plan_id,
            "plan_revision": plan_revision,
            "applied_delay_seconds": 0.0,
            "trigger": "test_due",
            "kind": "planned",
            "source": "integration-test",
            "reason": "test_due",
        },
    )
    schedule = SessionReviewSchedule(
        plan_id=plan_id,
        plan_revision=target.review_plan_revision,
        applied_delay_seconds=0.0,
        trigger="test_due",
        outcome="planned",
        source="integration-test",
        reason="test_due",
    )
    await store.commit(
        claim,
        SessionTransition(
            aggregate=target,
            disposition="review_schedule_seeded",
            caused_plan_id=plan_id,
            review_schedules=(schedule,),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id=f"seed-scheduled:{plan_id}",
                    event_type="scheduled",
                    plan_id=plan_id,
                    trigger=schedule.trigger,
                    outcome=schedule.outcome,
                    source=schedule.source,
                    applied_delay_seconds=schedule.applied_delay_seconds,
                    reason=schedule.reason,
                    metadata={
                        "plan_revision": schedule.plan_revision,
                        "schedule_outcome": {
                            "active_reply_threshold": {},
                            "applied_delay_seconds": schedule.applied_delay_seconds,
                            "fallback_reason": "",
                            "kind": schedule.outcome,
                            "mention_sensitivity": "normal",
                            "model_execution_id": "",
                            "prompt_signature": "",
                            "reason": schedule.reason,
                            "requested_delay_seconds": None,
                            "source": schedule.source,
                        },
                    },
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


def _assert_raw_logical_key_indexes(database: DatabaseManager) -> None:
    """Assert the storage-aware ReviewDue preflights stay indexed."""

    expected_sql = {
        _MAILBOX_RAW_LOGICAL_KEY_INDEX: (
            "CREATE INDEX idx_agent_session_mailbox_raw_logical_key "
            "ON agent_session_mailbox("
            "CAST(profile_id AS BLOB),"
            "CAST(session_id AS BLOB),"
            "CAST(event_id AS BLOB)"
            ")"
        ),
        _SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX: (
            "CREATE INDEX idx_agent_review_schedule_events_raw_logical_key "
            "ON agent_review_schedule_events(CAST(schedule_event_id AS BLOB))"
        ),
    }
    expected_tables = {
        _MAILBOX_RAW_LOGICAL_KEY_INDEX: "agent_session_mailbox",
        _SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX: "agent_review_schedule_events",
    }
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT name, tbl_name, sql
            FROM sqlite_master
            WHERE type = 'index'
              AND name IN (?, ?)
            """,
            (
                _MAILBOX_RAW_LOGICAL_KEY_INDEX,
                _SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX,
            ),
        ).fetchall()
        actual = {str(row["name"]): row for row in rows}
        mailbox_index_flags = {
            str(row["name"]): int(row["unique"])
            for row in conn.execute("PRAGMA index_list('agent_session_mailbox')")
        }
        schedule_event_index_flags = {
            str(row["name"]): int(row["unique"])
            for row in conn.execute(
                "PRAGMA index_list('agent_review_schedule_events')"
            )
        }
        mailbox_plan = conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(event_id AS BLOB) = ?
            ORDER BY mailbox_id
            """,
            (b"profile-a", b"session-a", b"event-a"),
        ).fetchall()
        schedule_event_plan = conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM agent_review_schedule_events
            WHERE CAST(schedule_event_id AS BLOB) = ?
            ORDER BY schedule_event_seq
            """,
            (b"schedule-event-a",),
        ).fetchall()

    assert set(actual) == set(expected_sql)
    for index_name, expected in expected_sql.items():
        assert str(actual[index_name]["tbl_name"]) == expected_tables[index_name]
        assert actual[index_name]["sql"] is not None
        assert "".join(str(actual[index_name]["sql"]).upper().split()) == "".join(
            expected.upper().split()
        )
    assert mailbox_index_flags[_MAILBOX_RAW_LOGICAL_KEY_INDEX] == 0
    assert schedule_event_index_flags[_SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX] == 0
    _assert_plan_uses_index(
        mailbox_plan,
        table_name="agent_session_mailbox",
        index_name=_MAILBOX_RAW_LOGICAL_KEY_INDEX,
    )
    _assert_plan_uses_index(
        schedule_event_plan,
        table_name="agent_review_schedule_events",
        index_name=_SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX,
    )


def test_manual_review_request_lookup_is_indexed(tmp_path: Path) -> None:
    """Manual request-id replays stay targeted for long-lived sessions."""

    now = [100.0]
    database, _store = _make_runtime(tmp_path, now)
    with database.connect() as conn:
        index_row = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'index' AND name = ?
            """,
            (_MANUAL_REVIEW_REQUEST_INDEX,),
        ).fetchone()
        query_plan = conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT event_id
            FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(kind AS BLOB) = ?
              AND CAST(source AS BLOB) = ?
              AND CAST(causation_id AS BLOB) = ?
            """,
            (
                b"profile-a",
                b"session-a",
                b"ManualReviewRequested",
                b"manual_review_admission",
                b"operator-request-a",
            ),
        ).fetchall()
    assert index_row is not None
    assert index_row["sql"] is not None
    _assert_plan_uses_index(
        query_plan,
        table_name="agent_session_mailbox",
        index_name=_MANUAL_REVIEW_REQUEST_INDEX,
    )


def _assert_plan_uses_index(
    plan: list[sqlite3.Row],
    *,
    table_name: str,
    index_name: str,
) -> None:
    """Assert a raw-key query is targeted without planner-specific wording."""

    details = tuple(str(row["detail"]) for row in plan)
    assert any(
        detail.startswith(f"SEARCH {table_name} ") and index_name in detail
        for detail in details
    ), details
    assert all(f"SCAN {table_name}" not in detail for detail in details), details
    assert all("USE TEMP B-TREE" not in detail for detail in details), details


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


def _mailbox_handoff_row(
    database: DatabaseManager,
    *,
    event_id: str,
) -> sqlite3.Row | None:
    """Return immutable handoff evidence for one exact mailbox event."""

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT handoff.mailbox_id, handoff.event_id,
                   handoff.ownership_generation,
                   handoff.evidence_state, handoff.admission_fence_id,
                   handoff.admission_fence_generation, handoff.state
            FROM agent_session_mailbox AS mailbox
            LEFT JOIN agent_session_mailbox_handoffs AS handoff
              ON handoff.mailbox_id = mailbox.mailbox_id
            WHERE mailbox.event_id = ?
            """,
            (event_id,),
        ).fetchone()
    return row


@pytest.mark.asyncio
async def test_manual_review_admission_notifies_exact_fenced_mailbox_id(
    tmp_path: Path,
) -> None:
    """Manual admission publishes only the committed mailbox identity."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "manual-notifier")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="manual-notifier",
        ttl_seconds=3600.0,
    )
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-notifier-plan",
        admission_grant=grant,
    )
    notifier = _MailboxHandoffNotifier()
    admission = ManualReviewAdmissionService(
        _repository(database, now),
        mailbox_handoff_notifier=notifier,
    )

    result = await admission.request(
        key,
        request_id="manual-notifier-request",
        requested_by="operator-a",
    )

    assert result.disposition is ManualReviewAdmissionDisposition.ADMITTED
    assert result.mailbox_id is not None
    assert notifier.mailbox_ids == [result.mailbox_id]
    handoff = database.actor_v2_mailbox_handoffs.read(result.mailbox_id)
    assert handoff is not None
    assert handoff.state is MailboxHandoffState.PENDING
    assert handoff.evidence.state is MailboxHandoffEvidenceState.FENCED
    assert handoff.evidence.identity.key == key
    assert handoff.evidence.identity.ownership_generation == generation
    assert handoff.evidence.admission_fence_id == grant.fence.fence_id
    assert handoff.evidence.admission_fence_generation == grant.fence.generation


@pytest.mark.asyncio
async def test_manual_review_notifier_failure_keeps_durable_handoff(
    tmp_path: Path,
) -> None:
    """A failed manual hint does not fail admission or fall back to key wake."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "manual-notifier-failure")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="manual-notifier-failure",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-notifier-failure-plan",
        admission_grant=grant,
    )
    notifier = _MailboxHandoffNotifier(fail=True)
    admission = ManualReviewAdmissionService(
        _repository(database, now),
        mailbox_handoff_notifier=notifier,
    )

    result = await admission.request(
        key,
        request_id="manual-notifier-failure-request",
        requested_by="operator-a",
    )

    assert result.disposition is ManualReviewAdmissionDisposition.ADMITTED
    assert result.mailbox_id is not None
    assert notifier.mailbox_ids == [result.mailbox_id]
    handoff = database.actor_v2_mailbox_handoffs.read(result.mailbox_id)
    assert handoff is not None
    assert handoff.state is MailboxHandoffState.PENDING


@pytest.mark.asyncio
async def test_review_due_scanner_notifies_exact_fenced_mailbox_id(
    tmp_path: Path,
) -> None:
    """The due scanner publishes sidecar identity, never an Actor wake request."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "review-notifier")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="review-notifier",
        ttl_seconds=3600.0,
    )
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="review-notifier-plan",
        admission_grant=grant,
    )
    notifier = _MailboxHandoffNotifier()
    scanner = DurableReviewDueScannerService(
        _repository(database, now),
        mailbox_handoff_notifier=notifier,
    )

    try:
        summary = await scanner.run_once()
        assert summary.dispatched_count == 1
        assert len(summary.dispatched_mailbox_ids) == 1
        mailbox_id = summary.dispatched_mailbox_ids[0]
        assert notifier.mailbox_ids == [mailbox_id]
        handoff = database.actor_v2_mailbox_handoffs.read(mailbox_id)
        assert handoff is not None
        assert handoff.state is MailboxHandoffState.PENDING
        assert handoff.evidence.state is MailboxHandoffEvidenceState.FENCED
        assert handoff.evidence.identity.key == key
        assert handoff.evidence.identity.ownership_generation == generation
    finally:
        await scanner.shutdown()


@pytest.mark.asyncio
async def test_review_due_notifier_failure_keeps_scanner_healthy_and_handoff_pending(
    tmp_path: Path,
) -> None:
    """An advisory failure cannot turn committed review work into a scan failure."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "review-notifier-failure")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="review-notifier-failure",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="review-notifier-failure-plan",
        admission_grant=grant,
    )
    notifier = _MailboxHandoffNotifier(fail=True)
    scanner = DurableReviewDueScannerService(
        _repository(database, now),
        mailbox_handoff_notifier=notifier,
    )

    try:
        summary = await scanner.run_once()
        assert summary.dispatched_count == 1
        mailbox_id = summary.dispatched_mailbox_ids[0]
        assert notifier.mailbox_ids == [mailbox_id]
        assert scanner.health_snapshot().consecutive_failures == 0
        handoff = database.actor_v2_mailbox_handoffs.read(mailbox_id)
        assert handoff is not None
        assert handoff.state is MailboxHandoffState.PENDING
    finally:
        await scanner.shutdown()


def _remove_handoff_for_historical_replay(
    database: DatabaseManager,
    *,
    event_id: str,
    evidence_state: str,
) -> None:
    """Make one durable mailbox historical without deriving new fence proof."""

    with database.connect() as conn:
        mailbox = conn.execute(
            "SELECT mailbox_id FROM agent_session_mailbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        assert mailbox is not None
        conn.execute(
            "DROP TRIGGER IF EXISTS trg_agent_session_mailbox_handoff_delete_forbidden"
        )
        deleted = conn.execute(
            "DELETE FROM agent_session_mailbox_handoffs WHERE mailbox_id = ?",
            (int(mailbox["mailbox_id"]),),
        )
        assert deleted.rowcount == 1
    if evidence_state == "unknown":
        # Startup migration records old mailbox rows as unknown, not as evidence
        # reconstructed from the currently live Actor ownership.
        database.initialize()
    elif evidence_state != "missing":
        raise AssertionError(f"unsupported historical evidence state: {evidence_state}")


def _mailbox_snapshot(database: DatabaseManager, event_id: str) -> tuple[object, ...]:
    with database.connect() as conn:
        row = conn.execute(
            "SELECT * FROM agent_session_mailbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    assert row is not None
    return tuple(row)


def _lossless_mailbox_snapshot(
    database: DatabaseManager,
    event_id: str,
) -> tuple[object, ...]:
    with sqlite3.connect(database.config.sqlite_path) as conn:
        conn.text_factory = bytes
        row = conn.execute(
            "SELECT * FROM agent_session_mailbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    assert row is not None
    return tuple(row)


def _lossless_logical_mailbox_snapshot(
    database: DatabaseManager,
    *,
    key: SessionKey,
    event_id: str,
) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(database.config.sqlite_path) as conn:
        conn.text_factory = bytes
        rows = conn.execute(
            """
            SELECT * FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(event_id AS BLOB) = ?
            ORDER BY mailbox_id
            """,
            (
                key.profile_id.encode(),
                key.session_id.encode(),
                event_id.encode(),
            ),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _lossless_all_mailbox_snapshot(
    database: DatabaseManager,
) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(database.config.sqlite_path) as conn:
        conn.text_factory = bytes
        rows = conn.execute(
            "SELECT * FROM agent_session_mailbox ORDER BY mailbox_id"
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _insert_mailbox_clone(
    database: DatabaseManager,
    event_id: str,
    *,
    overrides: dict[str, str],
) -> None:
    columns = (
        "event_id",
        "profile_id",
        "session_id",
        "ownership_generation",
        "kind",
        "source",
        "occurred_at",
        "payload_json",
        "causation_id",
        "correlation_id",
        "trace_id",
        "status",
        "attempt_count",
        "available_at",
        "claim_id",
        "lease_owner",
        "lease_until",
        "created_at",
        "handled_at",
        "last_error",
    )
    select_expressions = tuple(overrides.get(column, column) for column in columns)
    with sqlite3.connect(database.config.sqlite_path) as conn:
        inserted = conn.execute(
            f"""
            INSERT INTO agent_session_mailbox ({", ".join(columns)})
            SELECT {", ".join(select_expressions)}
            FROM agent_session_mailbox
            WHERE event_id = ?
            """,
            (event_id,),
        )
    assert inserted.rowcount == 1


def _weaken_mailbox_key_affinity(
    database: DatabaseManager,
    field_name: str,
) -> None:
    with sqlite3.connect(database.config.sqlite_path) as conn:
        indexes = conn.execute(
            """
            SELECT sql FROM sqlite_schema
            WHERE type = 'index' AND tbl_name = 'agent_session_mailbox'
              AND sql IS NOT NULL
            ORDER BY name
            """
        ).fetchall()
        dependents = conn.execute(
            """
            SELECT type, name, sql FROM sqlite_schema
            WHERE type IN ('trigger', 'view')
              AND instr(sql, 'agent_session_mailbox') > 0
            ORDER BY CASE type WHEN 'trigger' THEN 0 ELSE 1 END, name
            """
        ).fetchall()
        schema = conn.execute(
            """
            SELECT sql FROM sqlite_schema
            WHERE type = 'table' AND name = 'agent_session_mailbox'
            """
        ).fetchone()
        assert schema is not None
        create_sql = str(schema[0])
        assert f"{field_name} TEXT NOT NULL" in create_sql
        weak_sql = create_sql.replace(
            "agent_session_mailbox",
            "agent_session_mailbox_weak",
            1,
        ).replace(
            f"{field_name} TEXT NOT NULL",
            f"{field_name} NOT NULL",
            1,
        )
        conn.execute(weak_sql)
        conn.execute(
            """
            INSERT INTO agent_session_mailbox_weak
            SELECT * FROM agent_session_mailbox
            """
        )
        for object_type, name, _sql in dependents:
            quoted_name = str(name).replace('"', '""')
            conn.execute(f'DROP {str(object_type).upper()} "{quoted_name}"')
        conn.execute("DROP TABLE agent_session_mailbox")
        conn.execute(
            """
            ALTER TABLE agent_session_mailbox_weak
            RENAME TO agent_session_mailbox
            """
        )
        for (index_sql,) in indexes:
            conn.execute(str(index_sql))
        for _object_type, _name, dependent_sql in reversed(dependents):
            assert dependent_sql is not None
            conn.execute(str(dependent_sql))


def _schedule_snapshot(database: DatabaseManager, plan_id: str) -> tuple[object, ...]:
    with database.connect() as conn:
        row = conn.execute(
            "SELECT * FROM agent_review_schedules WHERE plan_id = ?",
            (plan_id,),
        ).fetchone()
    assert row is not None
    return tuple(row)


def _review_due_journal_snapshot(
    database: DatabaseManager,
) -> tuple[tuple[object, ...], ...]:
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM agent_review_schedule_events
            WHERE source = ?
            ORDER BY schedule_event_seq
            """,
            (REVIEW_DUE_EVENT_SOURCE,),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _lossless_review_due_journal_snapshot(
    database: DatabaseManager,
) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(database.config.sqlite_path) as conn:
        conn.text_factory = bytes
        rows = conn.execute(
            """
            SELECT * FROM agent_review_schedule_events
            WHERE source = ?
            ORDER BY schedule_event_seq
            """,
            (REVIEW_DUE_EVENT_SOURCE,),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


async def _prepare_exact_delivery_replay(
    database: DatabaseManager,
    store: SQLiteSessionActorStore,
    now: list[float],
    *,
    keep_journal: bool,
    key: SessionKey | None = None,
    historical_mailbox: bool = False,
) -> tuple[SessionKey, int, str]:
    """Prepare an exact replay, optionally from a pre-handoff mailbox row."""

    key = key or SessionKey("profile-a", "session-a")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="plan-a",
    )
    if historical_mailbox:
        event_id = _insert_historical_review_due_mailbox(
            database,
            key=key,
            plan_id="plan-a",
            now=now[0],
        )
        return key, generation, event_id
    first = _repository(database, now).dispatch_due(limit=1)
    assert first.dispatched_count == 1
    event_id = first.results[0].event_id
    with database.connect() as conn:
        updated = conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'scheduled', delivery_cycle = 0,
                available_at = ?, last_error = '', updated_at = ?
            WHERE plan_id = 'plan-a'
            """,
            (now[0], now[0]),
        )
        assert updated.rowcount == 1
        if not keep_journal:
            conn.execute(
                """
                DELETE FROM agent_review_schedule_events
                WHERE source = ?
                """,
                (REVIEW_DUE_EVENT_SOURCE,),
            )
    return key, generation, event_id


def _insert_historical_review_due_mailbox(
    database: DatabaseManager,
    *,
    key: SessionKey,
    plan_id: str,
    now: float,
) -> str:
    """Insert a pre-sidecar ReviewDue row with the current exact wire shape."""

    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT *
            FROM agent_review_schedules
            WHERE profile_id = ? AND session_id = ? AND plan_id = ?
            """,
            (key.profile_id, key.session_id, plan_id),
        ).fetchone()
        assert schedule is not None
        delivery_cycle = int(schedule["delivery_cycle"])
        event_id = review_due_event_id(
            key=key,
            plan_id=str(schedule["plan_id"]),
            plan_revision=int(schedule["plan_revision"]),
            ownership_generation=int(schedule["ownership_generation"]),
            delivery_cycle=delivery_cycle,
        )
        payload: dict[str, object] = {
            "version": 1 if delivery_cycle == 0 else 2,
            "event_id": event_id,
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "plan_id": str(schedule["plan_id"]),
            "plan_revision": int(schedule["plan_revision"]),
            "ownership_generation": int(schedule["ownership_generation"]),
            "trigger": str(schedule["trigger"]),
            "outcome": str(schedule["outcome"]),
            "reason": str(schedule["reason"]),
            "scheduled_from": float(schedule["scheduled_from"]),
            "next_review_at": float(schedule["next_review_at"]),
            "attempt_count": int(schedule["attempt_count"]),
            "committed_state_revision": int(schedule["committed_state_revision"]),
            "expected_active_epoch": schedule["expected_active_epoch"],
            "expected_activity_generation": schedule[
                "expected_activity_generation"
            ],
        }
        if delivery_cycle > 0:
            payload["delivery_cycle"] = delivery_cycle
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json, causation_id,
                correlation_id, trace_id, status, attempt_count,
                available_at, claim_id, lease_owner, lease_until,
                created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '',
                      NULL, ?, NULL, '')
            """,
            (
                event_id,
                key.profile_id,
                key.session_id,
                int(schedule["ownership_generation"]),
                REVIEW_DUE_EVENT_KIND,
                REVIEW_DUE_EVENT_SOURCE,
                float(schedule["next_review_at"]),
                json.dumps(
                    payload,
                    ensure_ascii=True,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                str(schedule["plan_id"]),
                str(schedule["plan_id"]),
                event_id,
                float(schedule["next_review_at"]),
                now,
            ),
        )
        assert inserted.rowcount == 1
    return event_id


def _freeze_due_schedule(
    database: DatabaseManager,
    *,
    key: SessionKey,
    generation: int,
    failure: str,
) -> str:
    if failure == "ownership_missing":
        with database.connect() as conn:
            conn.execute(
                """
                DELETE FROM agent_session_runtime_ownership
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            )
        return "ownership_missing"
    if failure == "ownership_migrating":
        # The migration API correctly rejects live handoffs. This fixture
        # models an already-persisted migration state so the scanner reader's
        # fail-closed behavior remains covered independently of that writer.
        with database.connect() as conn:
            updated = conn.execute(
                """
                UPDATE agent_session_runtime_ownership
                SET status = 'migrating', pending_mode = ?, generation = ?,
                    migration_reason = ?, updated_at = ?
                WHERE profile_id = ? AND session_id = ?
                  AND mode = 'actor_v2' AND status = 'active'
                  AND generation = ?
                """,
                (
                    AgentRuntimeOwnershipMode.LEGACY.value,
                    generation + 1,
                    "historical migration snapshot for scanner fence test",
                    100.0,
                    key.profile_id,
                    key.session_id,
                    generation,
                ),
            )
        assert updated.rowcount == 1
        return "ownership_migrating"
    if failure == "aggregate_missing":
        # This simulates legacy/corrupt storage which predates the current FK.
        with sqlite3.connect(database.config.sqlite_path) as conn:
            conn.execute(
                """
                DELETE FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            )
        return "aggregate_missing"
    if failure == "aggregate_generation_mismatch":
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_session_aggregates
                SET ownership_generation = ownership_generation + 1
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            )
        return "aggregate_generation_mismatch"
    if failure == "schedule_generation_mismatch":
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_review_schedules
                SET ownership_generation = ownership_generation + 1
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            )
        return "schedule_generation_mismatch"
    raise AssertionError(f"unsupported fence failure: {failure}")


def _invalidate_admission_fence(
    database: DatabaseManager,
    grant: ActorV2AdmissionGrant,
    *,
    state: str,
) -> str:
    """Invalidate one committed fence without changing its Actor ownership row."""

    if state == "revoked":
        database.actor_v2_admission_fences.revoke(
            grant,
            reason="integration test revokes actor admission",
        )
        return "admission_fence_revoked"
    if state == "expired":
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_session_actor_v2_admission_fences
                SET expires_at = 0
                WHERE profile_id = ? AND session_id = ?
                  AND fence_id = ? AND generation = ?
                """,
                (
                    grant.fence.key.profile_id,
                    grant.fence.key.session_id,
                    grant.fence.fence_id,
                    grant.fence.generation,
                ),
            )
        return "admission_fence_expired"
    if state == "missing":
        with database.connect() as conn:
            conn.execute(
                """
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = ? AND session_id = ?
                  AND fence_id = ? AND generation = ?
                """,
                (
                    grant.fence.key.profile_id,
                    grant.fence.key.session_id,
                    grant.fence.fence_id,
                    grant.fence.generation,
                ),
            )
        return "admission_fence_missing"
    raise AssertionError(f"unsupported admission fence state: {state}")


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
async def test_manual_review_admission_does_not_compete_with_due_scanner(
    tmp_path: Path,
) -> None:
    """A due delivery that claimed the plan wins over a later manual request."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "profile-a:manual-room-b")
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-plan-b",
    )
    repository = _repository(database, now)

    summary = repository.dispatch_due(limit=1)
    result = repository.admit_manual_review(
        key,
        request_id="operator-request-b",
        requested_by="operator-a",
    )

    assert summary.dispatched_count == 1
    assert result.disposition is ManualReviewAdmissionDisposition.ALREADY_CLAIMED
    with database.connect() as conn:
        manual_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ?
              AND kind = 'ManualReviewRequested'
            """,
            (key.profile_id, key.session_id),
        ).fetchone()[0]
    assert manual_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source",
    (
        MANUAL_REVIEW_EVENT_SOURCE,
        "untrusted_manual_review_source",
    ),
)
async def test_manual_review_generic_enqueue_requires_schedule_admission(
    tmp_path: Path,
    source: str,
) -> None:
    """No generic mailbox path may bypass the schedule-claim transaction."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "profile-a:manual-store-boundary")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-store-boundary-plan",
    )
    request = ManualReviewRequest(
        key=key,
        request_id="operator-request-store-boundary",
        ownership_generation=generation,
        plan_id="manual-store-boundary-plan",
        plan_revision=1,
        delivery_cycle=0,
        requested_by="operator-a",
        reason="generic_enqueue_must_not_admit",
    )
    envelope = SessionEventEnvelope(
        event_id=request.event_id,
        key=key,
        kind=MANUAL_REVIEW_EVENT_KIND,
        ownership_generation=generation,
        payload=request.to_payload(),
        source=source,
        occurred_at=now[0],
        causation_id=request.request_id,
        correlation_id=request.request_id,
        trace_id=request.event_id,
        available_at=now[0],
        created_at=now[0],
    )

    with pytest.raises(ManualReviewAdmissionRequiredError, match="must be written"):
        await store.enqueue(envelope)

    with database.connect() as conn:
        manual_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(kind AS BLOB) = ?
            """,
            (
                key.profile_id.encode(),
                key.session_id.encode(),
                MANUAL_REVIEW_EVENT_KIND.encode(),
            ),
        ).fetchone()[0]
        schedule = conn.execute(
            """
            SELECT status, delivery_cycle
            FROM agent_review_schedules
            WHERE profile_id = ? AND session_id = ? AND plan_id = ?
            """,
            (key.profile_id, key.session_id, "manual-store-boundary-plan"),
        ).fetchone()
    assert manual_count == 0
    assert schedule is not None
    assert tuple(schedule) == ("scheduled", 0)


@pytest.mark.asyncio
async def test_manual_review_request_raw_duplicate_is_rejected(
    tmp_path: Path,
) -> None:
    """The raw-key partial index rejects a duplicate admission request id."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "profile-a:manual-raw-duplicate")
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-raw-duplicate-plan",
    )
    accepted = _repository(database, now).admit_manual_review(
        key,
        request_id="operator-request-raw-duplicate",
        requested_by="operator-a",
    )
    assert accepted.disposition is ManualReviewAdmissionDisposition.ADMITTED

    with sqlite3.connect(database.config.sqlite_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
            conn.execute(
                """
                INSERT INTO agent_session_mailbox (
                    event_id, profile_id, session_id, ownership_generation,
                    kind, source, occurred_at, payload_json, causation_id,
                    correlation_id, trace_id, status, attempt_count,
                    available_at, claim_id, lease_owner, lease_until,
                    created_at, handled_at, last_error
                )
                SELECT 'manual-review-raw-duplicate',
                       CAST(profile_id AS BLOB), CAST(session_id AS BLOB),
                       ownership_generation,
                       CAST(kind AS BLOB), CAST(source AS BLOB),
                       occurred_at, payload_json, CAST(causation_id AS BLOB),
                       correlation_id, 'manual-review-raw-duplicate',
                       status, attempt_count, available_at,
                       claim_id, lease_owner, lease_until,
                       created_at, handled_at, last_error
                FROM agent_session_mailbox
                WHERE event_id = ?
                """,
                (accepted.event_id,),
            )
        unique_index = conn.execute(
            "PRAGMA index_list('agent_session_mailbox')"
        ).fetchall()
        manual_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(kind AS BLOB) = ?
              AND CAST(source AS BLOB) = ?
              AND CAST(causation_id AS BLOB) = ?
            """,
            (
                key.profile_id.encode(),
                key.session_id.encode(),
                MANUAL_REVIEW_EVENT_KIND.encode(),
                MANUAL_REVIEW_EVENT_SOURCE.encode(),
                b"operator-request-raw-duplicate",
            ),
        ).fetchone()[0]
    assert any(
        row[1] == _MANUAL_REVIEW_REQUEST_UNIQUE_INDEX and row[2] == 1
        for row in unique_index
    )
    assert manual_count == 1


@pytest.mark.asyncio
async def test_manual_review_unique_index_migration_reports_raw_duplicates(
    tmp_path: Path,
) -> None:
    """A pre-index legacy duplicate fails with an actionable raw-key finding."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "profile-a:manual-migration-duplicate")
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-migration-duplicate-plan",
    )
    accepted = _repository(database, now).admit_manual_review(
        key,
        request_id="operator-request-migration-duplicate",
        requested_by="operator-a",
    )
    assert accepted.disposition is ManualReviewAdmissionDisposition.ADMITTED
    with database.connect() as conn:
        conn.execute(f"DROP INDEX {_MANUAL_REVIEW_REQUEST_UNIQUE_INDEX}")
    _insert_mailbox_clone(
        database,
        accepted.event_id,
        overrides={
            "event_id": "'manual-review-legacy-duplicate'",
            "profile_id": "CAST(profile_id AS BLOB)",
            "session_id": "CAST(session_id AS BLOB)",
            "kind": "CAST(kind AS BLOB)",
            "source": "CAST(source AS BLOB)",
            "causation_id": "CAST(causation_id AS BLOB)",
            "trace_id": "'manual-review-legacy-duplicate'",
        },
    )

    with pytest.raises(sqlite3.IntegrityError, match="duplicate raw request identity"):
        database.initialize()


@pytest.mark.asyncio
async def test_manual_review_request_id_rejects_immutable_field_drift(
    tmp_path: Path,
) -> None:
    """One request id cannot be reused with another audit identity."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "profile-a:manual-room-identity")
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-plan-identity",
    )
    repository = _repository(database, now)

    admitted = repository.admit_manual_review(
        key,
        request_id="operator-request-identity",
        requested_by="operator-a",
        reason="operator_requested_review",
    )
    assert admitted.disposition is ManualReviewAdmissionDisposition.ADMITTED

    with pytest.raises(
        ManualReviewAdmissionError,
        match="immutable request fields",
    ):
        repository.admit_manual_review(
            key,
            request_id="operator-request-identity",
            requested_by="operator-b",
            reason="operator_requested_review",
        )

    with database.connect() as conn:
        mailbox_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ?
              AND kind = 'ManualReviewRequested'
            """,
            (key.profile_id, key.session_id),
        ).fetchone()[0]
    assert mailbox_count == 1


@pytest.mark.asyncio
async def test_manual_review_schedule_handoff_blocks_ownership_migration(
    tmp_path: Path,
) -> None:
    """A seed schedule handoff prevents a partial ownership migration."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "profile-a:manual-room-migrating")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-plan-migrating",
    )
    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="mailbox handoff blocks actor refence",
    ):
        database.agent_runtime_ownership.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=generation,
            reason="manual admission migration fence test",
        )

    owner = database.agent_runtime_ownership.get(key)
    assert owner is not None
    assert owner.mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert owner.status.value == "active"
    assert owner.generation == generation
    with database.connect() as conn:
        manual_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ?
              AND kind = 'ManualReviewRequested'
            """,
            (key.profile_id, key.session_id),
        ).fetchone()[0]
    assert manual_count == 0


@pytest.mark.asyncio
async def test_manual_review_admission_is_profile_scoped_for_shared_session(
    tmp_path: Path,
) -> None:
    """The same operator request id is independent across profile-owned sessions."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    first_key = SessionKey("profile-a", "bot-a:instance-a:shared-room")
    second_key = SessionKey("profile-b", "bot-b:instance-a:shared-room")
    await _seed_due_schedule(
        database,
        store,
        key=first_key,
        plan_id="manual-plan-profile-a",
    )
    await _seed_due_schedule(
        database,
        store,
        key=second_key,
        plan_id="manual-plan-profile-b",
    )
    repository = _repository(database, now)

    first = repository.admit_manual_review(
        first_key,
        request_id="operator-request-shared",
        requested_by="operator-a",
    )
    second = repository.admit_manual_review(
        second_key,
        request_id="operator-request-shared",
        requested_by="operator-a",
    )

    assert first.disposition is ManualReviewAdmissionDisposition.ADMITTED
    assert second.disposition is ManualReviewAdmissionDisposition.ADMITTED
    assert first.event_id != second.event_id
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT profile_id, session_id, causation_id
            FROM agent_session_mailbox
            WHERE kind = 'ManualReviewRequested'
            ORDER BY profile_id
            """
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        (first_key.profile_id, first_key.session_id, "operator-request-shared"),
        (second_key.profile_id, second_key.session_id, "operator-request-shared"),
    ]


@pytest.mark.asyncio
async def test_manual_review_admission_honors_profile_filter(tmp_path: Path) -> None:
    """A profile-scoped diagnostic admission cannot target another profile."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-b", "profile-b:manual-room")
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-plan-profile-b",
    )

    result = _repository(database, now, profile_id="profile-a").admit_manual_review(
        key,
        request_id="operator-request-profile-b",
        requested_by="operator-a",
    )

    assert result.disposition is ManualReviewAdmissionDisposition.REJECTED
    assert result.reason == "profile_filter_mismatch"
    with database.connect() as conn:
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
    assert mailbox_count == 1  # The seeding envelope only.


@pytest.mark.asyncio
async def test_review_wake_keyset_honors_repository_profile_scope(
    tmp_path: Path,
) -> None:
    """A profile-owned scanner cannot discover another profile's wake debt."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    first_key = SessionKey("profile-a", "shared-review-room")
    second_key = SessionKey("profile-b", "shared-review-room")
    await _seed_due_schedule(
        database,
        store,
        key=first_key,
        plan_id="review-wake-profile-a",
    )
    await _seed_due_schedule(
        database,
        store,
        key=second_key,
        plan_id="review-wake-profile-b",
    )
    assert _repository(database, now).dispatch_due(limit=2).dispatched_count == 2

    repository = _repository(database, now, profile_id=first_key.profile_id)
    debts = repository.pending_review_wake_debts(limit=10)

    assert len(debts) == 1
    assert debts[0].request.key == first_key
    assert debts[0].cursor is not None
    assert repository.pending_review_wake_requests(
        limit=10,
        after=debts[0].cursor,
    ) == ()
    with pytest.raises(ValueError, match="offset cannot be combined"):
        repository.pending_review_wake_debts(
            limit=1,
            offset=1,
            after=debts[0].cursor,
        )



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
    assert "delivery_cycle" not in payload
    restarted = _repository(database, now)
    assert restarted.dispatch_due(limit=10).attempted_count == 0
    assert restarted.pending_review_due_keys() == (key,)
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, delivery_cycle
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert schedule is not None
    assert str(schedule["status"]) == "claimed"
    assert int(schedule["delivery_cycle"]) == 1


def test_review_due_delivery_cycle_identity_is_versioned_and_deterministic() -> None:
    key = SessionKey("profile-a", "session-a")
    legacy = review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=1,
    )

    assert legacy == review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=1,
        delivery_cycle=0,
    )
    assert legacy.startswith("review-due:v1:")
    cycle_one = review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=1,
        delivery_cycle=1,
    )
    assert cycle_one.startswith("review-due:v2:")
    assert cycle_one != legacy
    assert cycle_one == review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=1,
        delivery_cycle=1,
    )
    assert cycle_one != review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=1,
        delivery_cycle=2,
    )
    with pytest.raises(ValueError, match="delivery_cycle"):
        review_due_event_id(
            key=key,
            plan_id="plan-a",
            plan_revision=1,
            ownership_generation=1,
            delivery_cycle=-1,
        )


def test_reducer_rejects_unbound_v2_review_due_provenance() -> None:
    key = SessionKey("profile-a", "session-a")
    aggregate = AgentSessionAggregate(
        key=key,
        ownership_generation=1,
        state=AgentSessionState.ACTIVE_CHAT,
        current_plan_id="plan-a",
        review_plan_revision=1,
        review_plan={"plan_id": "plan-a", "applied_delay_seconds": 30.0},
    )
    event_id = review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=1,
        delivery_cycle=1,
    )
    payload = {
        "version": 2,
        "event_id": event_id,
        "session_key": {
            "profile_id": key.profile_id,
            "session_id": key.session_id,
        },
        "plan_id": "plan-a",
        "plan_revision": 1,
        "ownership_generation": 1,
        "delivery_cycle": 1,
        "attempt_count": 1,
    }
    reducer = AgentSessionReducer()
    valid = SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="ReviewDue",
        ownership_generation=1,
        source=REVIEW_DUE_EVENT_SOURCE,
        payload=payload,
    )

    assert reducer.reduce(aggregate, valid).disposition == "review_due_deferred"
    forged_id = SessionEventEnvelope(
        event_id="review-due:v2:forged",
        key=key,
        kind="ReviewDue",
        ownership_generation=1,
        source=REVIEW_DUE_EVENT_SOURCE,
        payload={**payload, "event_id": "review-due:v2:forged"},
    )
    forged_source = SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="ReviewDue",
        ownership_generation=1,
        source="manual",
        payload=payload,
    )
    missing_cycle = SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="ReviewDue",
        ownership_generation=1,
        source=REVIEW_DUE_EVENT_SOURCE,
        payload={key: value for key, value in payload.items() if key != "delivery_cycle"},
    )

    assert reducer.reduce(aggregate, forged_id).result == {
        "mismatch": ["delivery_identity_changed"]
    }
    assert reducer.reduce(aggregate, forged_source).result == {
        "mismatch": ["delivery_source_changed"]
    }
    assert reducer.reduce(aggregate, missing_cycle).result == {
        "mismatch": ["delivery_cycle_missing"]
    }


def test_reducer_rejects_unbound_v1_review_due_provenance() -> None:
    key = SessionKey("profile-a", "session-a")
    aggregate = AgentSessionAggregate(
        key=key,
        ownership_generation=1,
        state=AgentSessionState.ACTIVE_CHAT,
        current_plan_id="plan-a",
        review_plan_revision=1,
        review_plan={"plan_id": "plan-a", "applied_delay_seconds": 30.0},
    )
    event_id = review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=1,
        delivery_cycle=0,
    )
    payload = {
        "version": 1,
        "event_id": event_id,
        "session_key": {
            "profile_id": key.profile_id,
            "session_id": key.session_id,
        },
        "plan_id": "plan-a",
        "plan_revision": 1,
        "ownership_generation": 1,
        "attempt_count": 1,
    }
    reducer = AgentSessionReducer()
    valid = SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="ReviewDue",
        ownership_generation=1,
        source=REVIEW_DUE_EVENT_SOURCE,
        payload=payload,
    )

    assert reducer.reduce(aggregate, valid).disposition == "review_due_deferred"
    forged_id = SessionEventEnvelope(
        event_id="review-due:v1:forged",
        key=key,
        kind="ReviewDue",
        ownership_generation=1,
        source=REVIEW_DUE_EVENT_SOURCE,
        payload={**payload, "event_id": "review-due:v1:forged"},
    )
    forged_source = SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="ReviewDue",
        ownership_generation=1,
        source="manual",
        payload=payload,
    )

    assert reducer.reduce(aggregate, forged_id).result == {
        "mismatch": ["delivery_identity_changed"]
    }
    assert reducer.reduce(aggregate, forged_source).result == {
        "mismatch": ["delivery_source_changed"]
    }


@pytest.mark.asyncio
async def test_delivery_cycle_migration_advances_only_exact_legacy_due_debt(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    cases = {
        "no-mailbox": SessionKey("profile-a", "no-mailbox"),
        "exact": SessionKey("profile-a", "exact"),
        "wrong-source": SessionKey("profile-a", "wrong-source"),
        "wrong-plan": SessionKey("profile-a", "wrong-plan"),
        "wrong-generation": SessionKey("profile-a", "wrong-generation"),
    }
    generations = {
        name: await _seed_due_schedule(
            database,
            store,
            key=key,
            plan_id=f"plan-{name}",
        )
        for name, key in cases.items()
    }
    with database.connect() as conn:
        mailbox_rows = []
        for name in (
            "exact",
            "wrong-source",
            "wrong-plan",
            "wrong-generation",
        ):
            key = cases[name]
            generation = generations[name]
            event_id = review_due_event_id(
                key=key,
                plan_id=f"plan-{name}",
                plan_revision=1,
                ownership_generation=generation,
                delivery_cycle=0,
            )
            payload = json.dumps(
                {
                    "version": 1,
                    "event_id": event_id,
                    "session_key": {
                        "profile_id": key.profile_id,
                        "session_id": key.session_id,
                    },
                    "plan_id": f"plan-{name}",
                    "plan_revision": 1,
                    "ownership_generation": generation,
                    "attempt_count": 0,
                }
            )
            mailbox_rows.append(
                (
                    event_id,
                    key.profile_id,
                    key.session_id,
                    generation + (1 if name == "wrong-generation" else 0),
                    (
                        "other-source"
                        if name == "wrong-source"
                        else REVIEW_DUE_EVENT_SOURCE
                    ),
                    payload,
                    (
                        "other-plan"
                        if name == "wrong-plan"
                        else f"plan-{name}"
                    ),
                )
            )
        conn.executemany(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json, causation_id,
                available_at, created_at
            ) VALUES (?, ?, ?, ?, 'ReviewDue', ?, 100, ?, ?, 100, 100)
            """,
            mailbox_rows,
        )
        conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'claimed'
            WHERE plan_id = 'plan-exact'
            """
        )
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("PRAGMA legacy_alter_table = ON")
        conn.execute(
            "ALTER TABLE agent_review_schedules RENAME TO review_schedules_current"
        )
        conn.execute(
            """
            CREATE TABLE agent_review_schedules (
                plan_id TEXT PRIMARY KEY,
                profile_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                ownership_generation INTEGER NOT NULL DEFAULT 0,
                plan_revision INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'scheduled',
                trigger TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                requested_delay_seconds REAL,
                applied_delay_seconds REAL NOT NULL,
                scheduled_from REAL NOT NULL,
                next_review_at REAL NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                fallback_reason TEXT NOT NULL DEFAULT '',
                mention_sensitivity TEXT NOT NULL DEFAULT 'normal',
                active_reply_threshold_json TEXT NOT NULL DEFAULT '{}',
                model_execution_id TEXT NOT NULL DEFAULT '',
                prompt_signature TEXT NOT NULL DEFAULT '',
                expected_active_epoch INTEGER,
                expected_activity_generation INTEGER,
                committed_state_revision INTEGER NOT NULL,
                available_at REAL NOT NULL,
                claim_owner TEXT NOT NULL DEFAULT '',
                claim_until REAL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(profile_id, session_id)
                    REFERENCES agent_session_aggregates(profile_id, session_id)
                    ON DELETE CASCADE,
                UNIQUE(profile_id, session_id, plan_revision),
                CHECK(
                    status IN (
                        'scheduled', 'claimed', 'completed', 'failed', 'superseded'
                    )
                ),
                CHECK(ownership_generation >= 0),
                CHECK(plan_revision >= 0),
                CHECK(applied_delay_seconds >= 0),
                CHECK(committed_state_revision >= 0),
                CHECK(attempt_count >= 0)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO agent_review_schedules (
                plan_id, profile_id, session_id, ownership_generation,
                plan_revision, status, trigger, outcome, source,
                requested_delay_seconds, applied_delay_seconds, scheduled_from,
                next_review_at, reason, fallback_reason, mention_sensitivity,
                active_reply_threshold_json, model_execution_id,
                prompt_signature, expected_active_epoch,
                expected_activity_generation, committed_state_revision,
                available_at, claim_owner, claim_until, attempt_count,
                last_error, created_at, updated_at
            )
            SELECT
                plan_id, profile_id, session_id, ownership_generation,
                plan_revision, status, trigger, outcome, source,
                requested_delay_seconds, applied_delay_seconds, scheduled_from,
                next_review_at, reason, fallback_reason, mention_sensitivity,
                active_reply_threshold_json, model_execution_id,
                prompt_signature, expected_active_epoch,
                expected_activity_generation, committed_state_revision,
                available_at, claim_owner, claim_until, attempt_count,
                last_error, created_at, updated_at
            FROM review_schedules_current
            """
        )
        conn.execute("DROP TABLE review_schedules_current")

    database.initialize()

    with database.connect() as conn:
        columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(agent_review_schedules)"
            ).fetchall()
        }
        rows = conn.execute(
            """
            SELECT plan_id, status, delivery_cycle
            FROM agent_review_schedules ORDER BY plan_id
            """
        ).fetchall()
    assert "delivery_cycle" in columns
    cycles = {str(row["plan_id"]): int(row["delivery_cycle"]) for row in rows}
    assert cycles == {
        "plan-exact": 1,
        "plan-no-mailbox": 0,
        "plan-wrong-generation": 0,
        "plan-wrong-plan": 0,
        "plan-wrong-source": 0,
    }
    exact_schedule = next(row for row in rows if row["plan_id"] == "plan-exact")
    assert str(exact_schedule["status"]) == "claimed"

    exact_key = cases["exact"]
    claim = await store.claim_next(exact_key, worker_id="legacy-v1-review-due")
    assert claim is not None
    aggregate = await store.load(exact_key)
    transition = AgentSessionReducer().reduce(aggregate, claim.envelope)
    assert transition.disposition == "review_started"
    await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )
    with database.connect() as conn:
        exact_after = conn.execute(
            """
            SELECT schedule.status, schedule.delivery_cycle, mailbox.status
            FROM agent_review_schedules AS schedule
            JOIN agent_session_mailbox AS mailbox
              ON mailbox.profile_id = schedule.profile_id
             AND mailbox.session_id = schedule.session_id
             AND mailbox.causation_id = schedule.plan_id
            WHERE schedule.plan_id = 'plan-exact'
            """
        ).fetchone()
    assert exact_after is not None
    assert tuple(exact_after) == ("claimed", 1, "completed")


@pytest.mark.asyncio
async def test_reducer_deferred_due_dispatches_distinct_next_cycle_once(
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
            UPDATE agent_session_aggregates
            SET state = ?, state_revision = state_revision + 1
            WHERE profile_id = ? AND session_id = ?
            """,
            (AgentSessionState.ACTIVE_CHAT, key.profile_id, key.session_id),
        )
    repository = _repository(database, now)

    first = repository.dispatch_due(limit=1)

    assert first.dispatched_count == 1
    assert first.results[0].delivery_cycle == 0
    first_event_id = first.results[0].event_id
    claim = await store.claim_next(key, worker_id="review-due-reducer")
    assert claim is not None
    aggregate = await store.load(key)
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(busy_review_retry_seconds=5.0)
    )
    transition = reducer.reduce(aggregate, claim.envelope)
    assert transition.disposition == "review_due_deferred"
    assert transition.result == {"retry_at": 105.0, "attempt_count": 1}
    deferred = await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )
    assert deferred.current_plan_id == "plan-a"
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, attempt_count, delivery_cycle, available_at
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == ("scheduled", 1, 1, 105.0)

    now[0] = 105.0
    summaries = await asyncio.gather(
        asyncio.to_thread(partial(repository.dispatch_due, limit=1)),
        asyncio.to_thread(
            partial(_repository(database, now).dispatch_due, limit=1)
        ),
    )

    assert sum(summary.dispatched_count for summary in summaries) == 1
    assert sum(summary.attempted_count for summary in summaries) == 1
    second_result = next(
        result
        for summary in summaries
        for result in summary.results
        if result.disposition is ReviewDueDisposition.DISPATCHED
    )
    assert second_result.delivery_cycle == 1
    assert second_result.event_id != first_event_id
    assert second_result.event_id == review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=generation,
        delivery_cycle=1,
    )
    rows = _mailbox_rows(database)
    assert len(rows) == 2
    assert [str(row["status"]) for row in rows] == ["completed", "pending"]
    second_payload = json.loads(str(rows[1]["payload_json"]))
    assert second_payload["version"] == 2
    assert second_payload["delivery_cycle"] == 1
    assert second_payload["attempt_count"] == 1
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, delivery_cycle, last_error
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
        journal = conn.execute(
            """
            SELECT schedule_event_id, event_id, metadata_json
            FROM agent_review_schedule_events
            WHERE event_type = 'due_dispatched'
              AND source = 'durable_review_due_scanner'
            ORDER BY created_at, schedule_event_id
            """
        ).fetchall()
    assert schedule is not None
    assert tuple(schedule) == ("claimed", 2, "")
    assert len(journal) == 2
    assert len({str(row["schedule_event_id"]) for row in journal}) == 2
    assert {json.loads(str(row["metadata_json"]))["delivery_cycle"] for row in journal} == {
        0,
        1,
    }


@pytest.mark.asyncio
async def test_next_delivery_cycle_fences_stale_processing_due(
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
    first_event_id = first.results[0].event_id
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET status = 'processing', claim_id = 'stale-claim',
                lease_owner = 'stale-worker', lease_until = 200
            WHERE event_id = ?
            """,
            (first_event_id,),
        )
        conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'scheduled', available_at = 100
            WHERE plan_id = 'plan-a'
            """
        )

    second = _repository(database, now).dispatch_due(limit=1)

    assert second.dispatched_count == 1
    assert second.results[0].delivery_cycle == 1
    assert second.results[0].event_id == review_due_event_id(
        key=key,
        plan_id="plan-a",
        plan_revision=1,
        ownership_generation=generation,
        delivery_cycle=1,
    )
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id, status, claim_id, lease_owner, lease_until,
                   last_error
            FROM agent_session_mailbox
            WHERE kind = 'ReviewDue'
            ORDER BY mailbox_id
            """
        ).fetchall()
    assert len(rows) == 2
    assert tuple(rows[0]) == (
        first_event_id,
        "failed",
        "",
        "",
        None,
        "review_due_exact_plan_fence_superseded",
    )
    assert str(rows[1]["status"]) == "pending"


@pytest.mark.asyncio
async def test_stale_plan_is_superseded_without_review_due_mailbox(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    frozen_key = SessionKey("a-profile", "same-session")
    key = SessionKey("b-profile", "same-session")
    frozen_generation = await _seed_due_schedule(
        database,
        store,
        key=frozen_key,
        plan_id="frozen-plan",
    )
    await _seed_due_schedule(database, store, key=key, plan_id="old-plan")
    _freeze_due_schedule(
        database,
        key=frozen_key,
        generation=frozen_generation,
        failure="ownership_missing",
    )
    frozen_before = _schedule_snapshot(database, "frozen-plan")
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
    assert summary.results[0].key == key
    assert summary.results[0].reason == "aggregate_current_plan_mismatch"
    assert _mailbox_rows(database) == []
    assert _schedule_snapshot(database, "frozen-plan") == frozen_before
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, last_error FROM agent_review_schedules
            WHERE plan_id = 'old-plan'
            """
        ).fetchone()
        event = conn.execute(
            """
            SELECT event_type, outcome, reason
            FROM agent_review_schedule_events
            WHERE source = 'durable_review_due_scanner'
              AND plan_id = 'old-plan'
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
async def test_review_due_schedule_handoff_blocks_ownership_migration(
    tmp_path: Path,
) -> None:
    """A schedule producer cannot be refenced while its handoff is live."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "session-a")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="plan-a",
    )
    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="mailbox handoff blocks actor refence",
    ):
        database.agent_runtime_ownership.begin_migration(
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
    owner = database.agent_runtime_ownership.get(key)
    assert owner is not None
    assert owner.status.value == "active"
    assert owner.generation == generation


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_state", ["revoked", "expired", "missing"])
async def test_invalid_admission_fence_skips_due_without_mutating_schedule(
    tmp_path: Path,
    fence_state: str,
) -> None:
    """A fenced owner cannot emit ReviewDue after its admission becomes invalid."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", f"fenced-due-{fence_state}")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="review-due-fence-test",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="fenced-due-plan",
        admission_grant=grant,
    )
    schedule_before = _schedule_snapshot(database, "fenced-due-plan")
    journal_before = _review_due_journal_snapshot(database)
    expected_reason = _invalidate_admission_fence(
        database,
        grant,
        state=fence_state,
    )

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.fence_skipped_count == 1
    assert summary.results[0].disposition is ReviewDueDisposition.FENCE_SKIPPED
    assert summary.results[0].reason == expected_reason
    assert _mailbox_rows(database) == []
    assert _schedule_snapshot(database, "fenced-due-plan") == schedule_before
    assert _review_due_journal_snapshot(database) == journal_before


@pytest.mark.asyncio
async def test_review_due_final_admission_gate_rolls_back_candidate_writes(
    tmp_path: Path,
) -> None:
    """A fence deleted after mailbox staging leaves no due side effect visible."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "review-due-final-gate")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="review-due-final-gate-test",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="review-due-final-gate-plan",
        admission_grant=grant,
    )
    schedule_before = _schedule_snapshot(database, "review-due-final-gate-plan")
    journal_before = _review_due_journal_snapshot(database)
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER remove_review_due_fence_before_final_gate
            AFTER INSERT ON agent_review_schedule_events
            WHEN NEW.event_type = 'due_dispatched'
                 AND NEW.source = 'durable_review_due_scanner'
            BEGIN
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = NEW.profile_id AND session_id = NEW.session_id;
            END
            """
        )

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.fence_skipped_count == 1
    assert summary.results[0].reason == "admission_fence_missing"
    assert _mailbox_rows(database) == []
    assert _schedule_snapshot(database, "review-due-final-gate-plan") == schedule_before
    assert _review_due_journal_snapshot(database) == journal_before
    with database.connect() as conn:
        handoff_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox_handoffs AS handoff
            JOIN agent_session_mailbox AS mailbox
              ON mailbox.mailbox_id = handoff.mailbox_id
            WHERE mailbox.kind = 'ReviewDue'
              AND mailbox.source = 'durable_review_due_scanner'
            """
        ).fetchone()[0]
    assert handoff_count == 0
    with database.connect() as conn:
        fence = conn.execute(
            """
            SELECT status
            FROM agent_session_actor_v2_admission_fences
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert fence is not None
    assert str(fence["status"]) == "committed"


@pytest.mark.asyncio
async def test_manual_review_final_admission_gate_rolls_back_candidate_writes(
    tmp_path: Path,
) -> None:
    """A fence deleted after manual staging rejects without a durable mailbox."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "manual-review-final-gate")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="manual-review-final-gate-test",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-review-final-gate-plan",
        admission_grant=grant,
    )
    schedule_before = _schedule_snapshot(database, "manual-review-final-gate-plan")
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER remove_manual_review_fence_before_final_gate
            AFTER INSERT ON agent_review_schedule_events
            WHEN NEW.event_type = 'manual_dispatched'
                 AND NEW.source = 'manual_review_admission'
            BEGIN
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = NEW.profile_id AND session_id = NEW.session_id;
            END
            """
        )

    result = _repository(database, now).admit_manual_review(
        key,
        request_id="manual-review-final-gate-request",
        requested_by="operator-a",
    )

    assert result.disposition is ManualReviewAdmissionDisposition.REJECTED
    assert result.reason == "admission_fence_missing"
    assert result.wake_request is None
    assert _schedule_snapshot(database, "manual-review-final-gate-plan") == schedule_before
    with database.connect() as conn:
        manual_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox
            WHERE kind = 'ManualReviewRequested'
              AND source = 'manual_review_admission'
            """
        ).fetchone()[0]
        handoff_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox_handoffs AS handoff
            JOIN agent_session_mailbox AS mailbox
              ON mailbox.mailbox_id = handoff.mailbox_id
            WHERE mailbox.kind = 'ManualReviewRequested'
              AND mailbox.source = 'manual_review_admission'
            """
        ).fetchone()[0]
        fence = conn.execute(
            """
            SELECT status
            FROM agent_session_actor_v2_admission_fences
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert manual_count == 0
    assert handoff_count == 0
    assert fence is not None
    assert str(fence["status"]) == "committed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fenced",
    [pytest.param(False, id="unfenced-legacy"), pytest.param(True, id="fenced")],
)
async def test_review_due_new_mailbox_records_captured_handoff_evidence(
    tmp_path: Path,
    fenced: bool,
) -> None:
    """A fresh ReviewDue row gets exactly the owner evidence captured by dispatch."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", f"review-due-handoff-{fenced}")
    grant = (
        database.actor_v2_admission_fences.reserve(
            key,
            holder_id="review-due-handoff-test",
            ttl_seconds=3600.0,
        )
        if fenced
        else None
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="review-due-handoff-plan",
        admission_grant=grant,
    )

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.dispatched_count == 1
    result = summary.results[0]
    assert result.event_id
    handoff = _mailbox_handoff_row(database, event_id=result.event_id)
    assert handoff is not None
    assert str(handoff["event_id"]) == result.event_id
    assert int(handoff["ownership_generation"]) == result.ownership_generation
    assert str(handoff["evidence_state"]) == (
        "fenced" if fenced else "unfenced_legacy"
    )
    assert str(handoff["state"]) == ("pending" if fenced else "blocked")
    if grant is None:
        assert str(handoff["admission_fence_id"]) == ""
        assert int(handoff["admission_fence_generation"]) == 0
    else:
        assert str(handoff["admission_fence_id"]) == grant.fence.fence_id
        assert int(handoff["admission_fence_generation"]) == grant.fence.generation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fenced",
    [pytest.param(False, id="unfenced-legacy"), pytest.param(True, id="fenced")],
)
async def test_manual_review_new_mailbox_records_captured_handoff_evidence(
    tmp_path: Path,
    fenced: bool,
) -> None:
    """A fresh ManualReview request uses the same schedule mailbox handoff writer."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", f"manual-review-handoff-{fenced}")
    grant = (
        database.actor_v2_admission_fences.reserve(
            key,
            holder_id="manual-review-handoff-test",
            ttl_seconds=3600.0,
        )
        if fenced
        else None
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-review-handoff-plan",
        admission_grant=grant,
    )

    result = _repository(database, now).admit_manual_review(
        key,
        request_id="manual-review-handoff-request",
        requested_by="operator-a",
    )

    assert result.disposition is ManualReviewAdmissionDisposition.ADMITTED
    assert result.event_id
    handoff = _mailbox_handoff_row(database, event_id=result.event_id)
    assert handoff is not None
    assert str(handoff["event_id"]) == result.event_id
    assert int(handoff["ownership_generation"]) == result.ownership_generation
    assert str(handoff["evidence_state"]) == (
        "fenced" if fenced else "unfenced_legacy"
    )
    assert str(handoff["state"]) == ("pending" if fenced else "blocked")
    if grant is None:
        assert str(handoff["admission_fence_id"]) == ""
        assert int(handoff["admission_fence_generation"]) == 0
    else:
        assert str(handoff["admission_fence_id"]) == grant.fence.fence_id
        assert int(handoff["admission_fence_generation"]) == grant.fence.generation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "historical_state",
    [pytest.param("missing"), pytest.param("unknown")],
)
async def test_review_due_replay_never_upgrades_historical_handoff_evidence(
    tmp_path: Path,
    historical_state: str,
) -> None:
    """A later fenced owner cannot retrofit evidence onto an old ReviewDue row."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", f"review-due-replay-{historical_state}")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="review-due-replay-test",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="review-due-replay-plan",
        admission_grant=grant,
    )
    first = _repository(database, now).dispatch_due(limit=1)
    assert first.dispatched_count == 1
    event_id = first.results[0].event_id
    _remove_handoff_for_historical_replay(
        database,
        event_id=event_id,
        evidence_state=historical_state,
    )
    with database.connect() as conn:
        updated = conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'scheduled', delivery_cycle = 0,
                available_at = ?, last_error = '', updated_at = ?
            WHERE plan_id = 'review-due-replay-plan'
            """,
            (now[0], now[0]),
        )
        assert updated.rowcount == 1

    replay = _repository(database, now).dispatch_due(limit=1)

    assert replay.dispatched_count == 1
    assert replay.results[0].mailbox_inserted is False
    handoff = _mailbox_handoff_row(database, event_id=event_id)
    if historical_state == "missing":
        assert handoff is not None
        assert handoff["evidence_state"] is None
    else:
        assert handoff is not None
        assert str(handoff["evidence_state"]) == "unknown"
        assert str(handoff["state"]) == "blocked"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "historical_state",
    [pytest.param("missing"), pytest.param("unknown")],
)
async def test_manual_review_duplicate_never_upgrades_historical_handoff_evidence(
    tmp_path: Path,
    historical_state: str,
) -> None:
    """A duplicate manual request does not derive sidecar evidence from its owner."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", f"manual-review-replay-{historical_state}")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="manual-review-replay-test",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="manual-review-replay-plan",
        admission_grant=grant,
    )
    repository = _repository(database, now)
    first = repository.admit_manual_review(
        key,
        request_id="manual-review-replay-request",
        requested_by="operator-a",
    )
    assert first.disposition is ManualReviewAdmissionDisposition.ADMITTED
    _remove_handoff_for_historical_replay(
        database,
        event_id=first.event_id,
        evidence_state=historical_state,
    )

    duplicate = repository.admit_manual_review(
        key,
        request_id="manual-review-replay-request",
        requested_by="operator-a",
    )

    assert duplicate.disposition is ManualReviewAdmissionDisposition.DUPLICATE
    assert duplicate.mailbox_inserted is False
    handoff = _mailbox_handoff_row(database, event_id=first.event_id)
    if historical_state == "missing":
        assert handoff is not None
        assert handoff["evidence_state"] is None
    else:
        assert handoff is not None
        assert str(handoff["evidence_state"]) == "unknown"
        assert str(handoff["state"]) == "blocked"


@pytest.mark.asyncio
async def test_invalid_fenced_schedules_do_not_starve_live_due_work(
    tmp_path: Path,
) -> None:
    """A bounded scan prioritizes live admission over frozen stale schedules."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    invalid_keys: list[SessionKey] = []
    for index in range(2):
        key = SessionKey("profile-a", f"fenced-starvation-{index}")
        grant = database.actor_v2_admission_fences.reserve(
            key,
            holder_id=f"review-starvation-{index}",
            ttl_seconds=3600.0,
        )
        await _seed_due_schedule(
            database,
            store,
            key=key,
            plan_id=f"fenced-starvation-plan-{index}",
            admission_grant=grant,
        )
        database.actor_v2_admission_fences.revoke(
            grant,
            reason="test freezes stale due schedule",
        )
        invalid_keys.append(key)
    live_key = SessionKey("profile-a", "live-after-fenced-starvation")
    await _seed_due_schedule(
        database,
        store,
        key=live_key,
        plan_id="live-after-fenced-starvation-plan",
    )

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.dispatched_count == 1
    assert summary.results[0].key == live_key
    assert {row["session_id"] for row in _mailbox_rows(database)} == {
        live_key.session_id
    }
    with database.connect() as conn:
        frozen = conn.execute(
            """
            SELECT status
            FROM agent_review_schedules
            WHERE profile_id = ?
              AND session_id IN (?, ?)
            ORDER BY session_id
            """,
            ("profile-a", *(key.session_id for key in invalid_keys)),
        ).fetchall()
    assert [str(row["status"]) for row in frozen] == ["scheduled", "scheduled"]


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_state", ["revoked", "expired", "missing"])
async def test_invalid_admission_fence_rejects_manual_review_without_wake(
    tmp_path: Path,
    fence_state: str,
) -> None:
    """Manual admission cannot bypass the same fence checked by due scanning."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", f"fenced-manual-{fence_state}")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="manual-review-fence-test",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="fenced-manual-plan",
        admission_grant=grant,
    )
    schedule_before = _schedule_snapshot(database, "fenced-manual-plan")
    expected_reason = _invalidate_admission_fence(
        database,
        grant,
        state=fence_state,
    )
    admission = ManualReviewAdmissionService(_repository(database, now))

    result = await admission.request(
        key,
        request_id="fenced-manual-request",
        requested_by="operator-a",
    )

    assert result.disposition is ManualReviewAdmissionDisposition.REJECTED
    assert result.reason == expected_reason
    assert _mailbox_rows(database) == []
    assert _schedule_snapshot(database, "fenced-manual-plan") == schedule_before


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_state", ["revoked", "expired", "missing"])
async def test_invalid_admission_fence_is_excluded_from_review_wake_debt(
    tmp_path: Path,
    fence_state: str,
) -> None:
    """Revoked or expired ownership cannot keep driving bare SessionKey wakes."""

    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", f"fenced-debt-{fence_state}")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="review-wake-debt-fence-test",
        ttl_seconds=3600.0,
    )
    await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="fenced-debt-plan",
        admission_grant=grant,
    )
    repository = _repository(database, now)
    assert repository.dispatch_due(limit=1).dispatched_count == 1
    assert repository.pending_review_due_keys() == (key,)
    assert repository.pending_review_wake_keys() == (key,)

    _invalidate_admission_fence(database, grant, state=fence_state)

    assert repository.pending_review_due_keys() == ()
    assert repository.pending_review_wake_keys() == ()



@pytest.mark.asyncio
async def test_active_review_due_handoff_blocks_refence_before_new_generation_dispatch(
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
    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="mailbox handoff blocks actor refence",
    ):
        database.agent_runtime_ownership.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=generation,
            reason="fence claimed due event",
        )

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
    assert len(rows) == 1
    assert str(rows[0]["event_id"]) == old_event_id
    assert str(rows[0]["status"]) == "pending"
    assert int(rows[0]["ownership_generation"]) == generation


@pytest.mark.parametrize(
    "fence_failure",
    [
        "ownership_missing",
        "ownership_migrating",
        "aggregate_missing",
        "aggregate_generation_mismatch",
        "schedule_generation_mismatch",
    ],
)
@pytest.mark.asyncio
async def test_frozen_first_row_does_not_starve_later_due_schedule_across_passes(
    tmp_path: Path,
    fence_failure: str,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    frozen_key = SessionKey("a-profile", "same-session")
    valid_key = SessionKey("b-profile", "same-session")
    frozen_generation = await _seed_due_schedule(
        database,
        store,
        key=frozen_key,
        plan_id="plan-a",
    )
    await _seed_due_schedule(database, store, key=valid_key, plan_id="plan-b")
    expected_reason = _freeze_due_schedule(
        database,
        key=frozen_key,
        generation=frozen_generation,
        failure=fence_failure,
    )
    frozen_before = _schedule_snapshot(database, "plan-a")
    repository = _repository(database, now)

    first = repository.dispatch_due(limit=1)

    assert first.dispatched_count == 1
    assert first.results[0].key == valid_key
    assert _schedule_snapshot(database, "plan-a") == frozen_before
    rows = _mailbox_rows(database)
    assert len(rows) == 1
    assert str(rows[0]["profile_id"]) == valid_key.profile_id

    second = repository.dispatch_due(limit=1)

    assert second.fence_skipped_count == 1
    assert second.results[0].key == frozen_key
    assert second.results[0].reason == expected_reason
    assert _schedule_snapshot(database, "plan-a") == frozen_before


@pytest.mark.asyncio
async def test_all_frozen_rows_keep_stable_order_and_remain_unchanged(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    keys = (
        SessionKey("a-profile", "same-session"),
        SessionKey("b-profile", "same-session"),
    )
    snapshots: dict[str, tuple[object, ...]] = {}
    for index, key in enumerate(keys, start=1):
        plan_id = f"plan-{index}"
        generation = await _seed_due_schedule(
            database,
            store,
            key=key,
            plan_id=plan_id,
        )
        _freeze_due_schedule(
            database,
            key=key,
            generation=generation,
            failure="ownership_missing",
        )
        snapshots[plan_id] = _schedule_snapshot(database, plan_id)

    summary = _repository(database, now).dispatch_due(limit=2)

    assert [result.key for result in summary.results] == list(keys)
    assert all(
        result.disposition is ReviewDueDisposition.FENCE_SKIPPED
        for result in summary.results
    )
    assert _mailbox_rows(database) == []
    assert {
        plan_id: _schedule_snapshot(database, plan_id)
        for plan_id in snapshots
    } == snapshots



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


@pytest.mark.parametrize(
    ("field", "drifted_value"),
    [
        pytest.param("ownership_generation", 999, id="ownership-generation"),
        pytest.param("kind", "WrongKind", id="kind"),
        pytest.param("source", "wrong-source", id="source"),
        pytest.param("occurred_at", 99.0, id="occurred-at"),
        pytest.param("payload_json", "{}", id="payload"),
        pytest.param("causation_id", "wrong-plan", id="causation"),
        pytest.param("correlation_id", "wrong-plan", id="correlation"),
        pytest.param("trace_id", "wrong-trace", id="trace"),
        pytest.param("created_at", 99.0, id="created-at"),
    ],
)
@pytest.mark.asyncio
async def test_existing_mailbox_immutable_envelope_drift_defers_atomically(
    tmp_path: Path,
    field: str,
    drifted_value: object,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    _key, _generation, event_id = await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=False,
        historical_mailbox=True,
    )
    with database.connect() as conn:
        conn.execute(
            f"UPDATE agent_session_mailbox SET {field} = ? WHERE event_id = ?",
            (drifted_value, event_id),
        )
        mailbox_before = conn.execute(
            "SELECT * FROM agent_session_mailbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        schedule_before = conn.execute(
            """
            SELECT attempt_count FROM agent_review_schedules
            WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert mailbox_before is not None
    assert schedule_before is not None
    mailbox_snapshot = tuple(mailbox_before)
    attempt_count = int(schedule_before["attempt_count"])

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.deferred_count == 1
    assert summary.results[0].reason == "mailbox_identity_conflict"
    with database.connect() as conn:
        mailbox_after = conn.execute(
            "SELECT * FROM agent_session_mailbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        schedule_after = conn.execute(
            """
            SELECT status, attempt_count, delivery_cycle, available_at,
                   last_error
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert mailbox_after is not None
    assert tuple(mailbox_after) == mailbox_snapshot
    assert schedule_after is not None
    assert tuple(schedule_after) == (
        "scheduled",
        attempt_count + 1,
        0,
        105.0,
        "mailbox_identity_conflict",
    )
    assert _review_due_journal_snapshot(database) == ()


@pytest.mark.parametrize(
    ("field", "drifted_value", "storage_class"),
    [
        pytest.param(
            "ownership_generation",
            sqlite3.Binary(b"1"),
            "blob",
            id="blob-generation",
        ),
        pytest.param(
            "ownership_generation",
            1.5,
            "real",
            id="real-generation",
        ),
        pytest.param("kind", "1", "text", id="numeric-text"),
        pytest.param("kind", "true", "text", id="bool-like-text"),
    ],
)
@pytest.mark.asyncio
async def test_existing_mailbox_noncanonical_sqlite_representation_is_rejected(
    tmp_path: Path,
    field: str,
    drifted_value: object,
    storage_class: str,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    _key, _generation, event_id = await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=False,
        historical_mailbox=True,
    )
    with database.connect() as conn:
        conn.execute(
            f"UPDATE agent_session_mailbox SET {field} = ? WHERE event_id = ?",
            (drifted_value, event_id),
        )
        persisted_type = conn.execute(
            f"""
            SELECT typeof({field}) AS storage_class
            FROM agent_session_mailbox WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
    assert persisted_type is not None
    assert str(persisted_type["storage_class"]) == storage_class
    mailbox_before = _mailbox_snapshot(database, event_id)

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.deferred_count == 1
    assert summary.results[0].reason == "mailbox_identity_conflict"
    assert _mailbox_snapshot(database, event_id) == mailbox_before
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, attempt_count, delivery_cycle, last_error
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == (
        "scheduled",
        1,
        0,
        "mailbox_identity_conflict",
    )
    assert _review_due_journal_snapshot(database) == ()


@pytest.mark.asyncio
async def test_existing_mailbox_invalid_utf8_text_defers_without_partial_commit(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    _key, _generation, event_id = await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=False,
        historical_mailbox=True,
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET source = CAST(X'80' AS TEXT)
            WHERE event_id = ?
            """,
            (event_id,),
        )
        representation = conn.execute(
            """
            SELECT typeof(source) AS storage_class,
                   hex(CAST(source AS BLOB)) AS raw_hex
            FROM agent_session_mailbox WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
    assert representation is not None
    assert tuple(representation) == ("text", "80")
    mailbox_before = _lossless_mailbox_snapshot(database, event_id)

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.deferred_count == 1
    assert summary.results[0].reason == "mailbox_identity_conflict"
    assert _lossless_mailbox_snapshot(database, event_id) == mailbox_before
    assert _review_due_journal_snapshot(database) == ()
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, attempt_count, delivery_cycle, available_at,
                   last_error
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == (
        "scheduled",
        1,
        0,
        105.0,
        "mailbox_identity_conflict",
    )


def test_raw_logical_key_indexes_are_non_unique_and_selected(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, _store = _make_runtime(tmp_path, now)

    _assert_raw_logical_key_indexes(database)


def test_raw_logical_key_index_audit_repairs_same_table_drift(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, _store = _make_runtime(tmp_path, now)
    with database.connect() as conn:
        conn.execute(f"DROP INDEX {_MAILBOX_RAW_LOGICAL_KEY_INDEX}")
        conn.execute(f"DROP INDEX {_SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX}")
        conn.execute(
            f"""
            CREATE INDEX {_MAILBOX_RAW_LOGICAL_KEY_INDEX}
            ON agent_session_mailbox(profile_id, session_id, event_id)
            """
        )
        conn.execute(
            f"""
            CREATE INDEX {_SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX}
            ON agent_review_schedule_events(schedule_event_id)
            """
        )

    database.initialize()
    database.initialize()

    _assert_raw_logical_key_indexes(database)


def test_raw_logical_key_index_audit_rejects_cross_table_name_conflict(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, _store = _make_runtime(tmp_path, now)
    with database.connect() as conn:
        conn.execute(f"DROP INDEX {_MAILBOX_RAW_LOGICAL_KEY_INDEX}")
        conn.execute(
            f"""
            CREATE INDEX {_MAILBOX_RAW_LOGICAL_KEY_INDEX}
            ON agent_review_schedule_events(schedule_event_id)
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="unexpected table"):
        database.initialize()

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT tbl_name FROM sqlite_master
            WHERE type = 'index' AND name = ?
            """,
            (_MAILBOX_RAW_LOGICAL_KEY_INDEX,),
        ).fetchone()
    assert row is not None
    assert str(row["tbl_name"]) == "agent_review_schedule_events"


@pytest.mark.parametrize(
    (
        "key_field",
        "replacement",
        "storage_class",
        "key",
        "weaken_key_affinity",
    ),
    [
        pytest.param(
            "event_id",
            "CAST(event_id AS BLOB)",
            "blob",
            SessionKey("profile-a", "session-a"),
            False,
            id="event-id-blob",
        ),
        pytest.param(
            "profile_id",
            "CAST(profile_id AS BLOB)",
            "blob",
            SessionKey("profile-a", "session-a"),
            False,
            id="profile-id-blob",
        ),
        pytest.param(
            "session_id",
            "CAST(session_id AS BLOB)",
            "blob",
            SessionKey("profile-a", "session-a"),
            False,
            id="session-id-blob",
        ),
        pytest.param(
            "profile_id",
            "CAST(profile_id AS INTEGER)",
            "integer",
            SessionKey("1", "session-a"),
            True,
            id="profile-id-integer",
        ),
        pytest.param(
            "session_id",
            "CAST(session_id AS REAL)",
            "real",
            SessionKey("profile-a", "1.0"),
            True,
            id="session-id-real",
        ),
    ],
)
@pytest.mark.asyncio
async def test_mailbox_single_logical_key_alias_defers_without_duplicate(
    tmp_path: Path,
    key_field: str,
    replacement: str,
    storage_class: str,
    key: SessionKey,
    weaken_key_affinity: bool,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    _key, _generation, event_id = await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=False,
        key=key,
        historical_mailbox=True,
    )
    if weaken_key_affinity:
        _weaken_mailbox_key_affinity(database, key_field)
    with sqlite3.connect(database.config.sqlite_path) as conn:
        updated = conn.execute(
            f"""
            UPDATE agent_session_mailbox SET {key_field} = {replacement}
            WHERE event_id = ?
            """,
            (event_id,),
        )
        assert updated.rowcount == 1
        representation = conn.execute(
            f"""
            SELECT typeof({key_field})
            FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(event_id AS BLOB) = ?
            """,
            (
                key.profile_id.encode(),
                key.session_id.encode(),
                event_id.encode(),
            ),
        ).fetchone()
    assert representation is not None
    assert str(representation[0]) == storage_class
    mailbox_before = _lossless_logical_mailbox_snapshot(
        database,
        key=key,
        event_id=event_id,
    )
    assert len(mailbox_before) == 1

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.deferred_count == 1
    assert summary.results[0].reason == "mailbox_identity_conflict"
    assert (
        _lossless_logical_mailbox_snapshot(
            database,
            key=key,
            event_id=event_id,
        )
        == mailbox_before
    )
    assert _review_due_journal_snapshot(database) == ()
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, attempt_count, delivery_cycle, last_error
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == (
        "scheduled",
        1,
        0,
        "mailbox_identity_conflict",
    )


@pytest.mark.asyncio
async def test_mailbox_multiple_logical_aliases_do_not_terminalize_old_debt(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key, _generation, event_id = await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=False,
    )
    _insert_mailbox_clone(
        database,
        event_id,
        overrides={
            "event_id": "'older-review-due'",
            "trace_id": "'older-review-due'",
        },
    )
    _insert_mailbox_clone(
        database,
        event_id,
        overrides={"event_id": "CAST(event_id AS BLOB)"},
    )
    _insert_mailbox_clone(
        database,
        event_id,
        overrides={"profile_id": "CAST(profile_id AS BLOB)"},
    )
    logical_rows = _lossless_logical_mailbox_snapshot(
        database,
        key=key,
        event_id=event_id,
    )
    assert len(logical_rows) == 3
    mailbox_before = _lossless_all_mailbox_snapshot(database)

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.deferred_count == 1
    assert summary.results[0].reason == "mailbox_identity_conflict"
    assert _lossless_all_mailbox_snapshot(database) == mailbox_before
    assert _review_due_journal_snapshot(database) == ()
    with database.connect() as conn:
        older = conn.execute(
            """
            SELECT status, handled_at, last_error
            FROM agent_session_mailbox WHERE event_id = 'older-review-due'
            """
        ).fetchone()
        schedule = conn.execute(
            """
            SELECT status, attempt_count, delivery_cycle, last_error
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert older is not None
    assert tuple(older) == ("pending", None, "")
    assert schedule is not None
    assert tuple(schedule) == (
        "scheduled",
        1,
        0,
        "mailbox_identity_conflict",
    )


@pytest.mark.asyncio
async def test_existing_mailbox_exact_text_logical_key_replays_normally(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key, _generation, event_id = await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=False,
    )
    before = _lossless_logical_mailbox_snapshot(
        database,
        key=key,
        event_id=event_id,
    )
    assert len(before) == 1
    with database.connect() as conn:
        representation = conn.execute(
            """
            SELECT typeof(profile_id), typeof(session_id), typeof(event_id)
            FROM agent_session_mailbox WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
    assert representation is not None
    assert tuple(representation) == ("text", "text", "text")

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.dispatched_count == 1
    assert summary.results[0].mailbox_inserted is False
    assert (
        _lossless_logical_mailbox_snapshot(
            database,
            key=key,
            event_id=event_id,
        )
        == before
    )
    assert len(_review_due_journal_snapshot(database)) == 1


@pytest.mark.asyncio
async def test_existing_mailbox_delivery_state_and_available_at_are_mutable(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    _key, _generation, event_id = await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=False,
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET status = 'processing', attempt_count = 3,
                available_at = 250.0, claim_id = 'claim-a',
                lease_owner = 'worker-a', lease_until = 300.0,
                last_error = 'prior-retry'
            WHERE event_id = ?
            """,
            (event_id,),
        )
    mailbox_before = tuple(_mailbox_rows(database)[0])

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.dispatched_count == 1
    assert summary.results[0].mailbox_inserted is False
    assert tuple(_mailbox_rows(database)[0]) == mailbox_before
    assert len(_review_due_journal_snapshot(database)) == 1


@pytest.mark.parametrize(
    ("field", "drifted_value"),
    [
        pytest.param("previous_plan_id", "wrong-plan", id="previous-plan-id"),
        pytest.param("trigger", "wrong-trigger", id="trigger"),
        pytest.param("requested_delay_seconds", 1.0, id="requested-delay"),
        pytest.param("applied_delay_seconds", 1.0, id="applied-delay"),
        pytest.param("scheduled_from", 1.0, id="scheduled-from"),
        pytest.param("next_review_at", 1.0, id="next-review-at"),
        pytest.param("fallback_reason", "wrong-fallback", id="fallback-reason"),
        pytest.param("model_execution_id", "wrong-model", id="model-execution"),
        pytest.param("prompt_signature", "wrong-prompt", id="prompt-signature"),
        pytest.param("expected_active_epoch", 7, id="expected-active-epoch"),
        pytest.param(
            "expected_activity_generation",
            8,
            id="expected-activity-generation",
        ),
        pytest.param("committed_state_revision", 999, id="state-revision"),
        pytest.param("operation_id", "wrong-operation", id="operation-id"),
        pytest.param("trace_id", "wrong-trace", id="trace-id"),
        pytest.param("created_at", 999.0, id="created-at"),
    ],
)
@pytest.mark.asyncio
async def test_existing_schedule_journal_omitted_field_drift_rolls_back_dispatch(
    tmp_path: Path,
    field: str,
    drifted_value: object,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    _key, _generation, event_id = await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=True,
    )
    with database.connect() as conn:
        conn.execute(
            f"""
            UPDATE agent_review_schedule_events SET {field} = ?
            WHERE source = ?
            """,
            (drifted_value, REVIEW_DUE_EVENT_SOURCE),
        )
    schedule_before = _schedule_snapshot(database, "plan-a")
    mailbox_before = tuple(_mailbox_rows(database)[0])
    journal_before = _review_due_journal_snapshot(database)

    with pytest.raises(
        ReviewDueConflict,
        match="deterministic review schedule event contains conflicting payload",
    ):
        _repository(database, now).dispatch_due(limit=1)

    assert _schedule_snapshot(database, "plan-a") == schedule_before
    assert tuple(_mailbox_rows(database)[0]) == mailbox_before
    assert _review_due_journal_snapshot(database) == journal_before
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, attempt_count, delivery_cycle
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == ("scheduled", 0, 0)
    assert str(mailbox_before[1]) == event_id


@pytest.mark.parametrize(
    ("field", "drifted_value", "storage_class"),
    [
        pytest.param(
            "ownership_generation",
            sqlite3.Binary(b"1"),
            "blob",
            id="blob-generation",
        ),
        pytest.param(
            "committed_state_revision",
            1.5,
            "real",
            id="real-state-revision",
        ),
        pytest.param("operation_id", "1", "text", id="numeric-text"),
        pytest.param("operation_id", "true", "text", id="bool-like-text"),
    ],
)
@pytest.mark.asyncio
async def test_existing_schedule_journal_noncanonical_representation_conflicts(
    tmp_path: Path,
    field: str,
    drifted_value: object,
    storage_class: str,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=True,
    )
    with database.connect() as conn:
        conn.execute(
            f"""
            UPDATE agent_review_schedule_events SET {field} = ?
            WHERE source = ?
            """,
            (drifted_value, REVIEW_DUE_EVENT_SOURCE),
        )
        persisted_type = conn.execute(
            f"""
            SELECT typeof({field}) AS storage_class
            FROM agent_review_schedule_events WHERE source = ?
            """,
            (REVIEW_DUE_EVENT_SOURCE,),
        ).fetchone()
    assert persisted_type is not None
    assert str(persisted_type["storage_class"]) == storage_class
    schedule_before = _schedule_snapshot(database, "plan-a")
    mailbox_before = tuple(_mailbox_rows(database)[0])
    journal_before = _review_due_journal_snapshot(database)

    with pytest.raises(ReviewDueConflict):
        _repository(database, now).dispatch_due(limit=1)

    assert _schedule_snapshot(database, "plan-a") == schedule_before
    assert tuple(_mailbox_rows(database)[0]) == mailbox_before
    assert _review_due_journal_snapshot(database) == journal_before


@pytest.mark.asyncio
async def test_existing_schedule_journal_invalid_utf8_text_rolls_back_dispatch(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=True,
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_review_schedule_events
            SET trigger = CAST(X'80' AS TEXT)
            WHERE source = ?
            """,
            (REVIEW_DUE_EVENT_SOURCE,),
        )
        representation = conn.execute(
            """
            SELECT typeof(trigger) AS storage_class,
                   hex(CAST(trigger AS BLOB)) AS raw_hex
            FROM agent_review_schedule_events WHERE source = ?
            """,
            (REVIEW_DUE_EVENT_SOURCE,),
        ).fetchone()
    assert representation is not None
    assert tuple(representation) == ("text", "80")
    schedule_before = _schedule_snapshot(database, "plan-a")
    mailbox_before = tuple(_mailbox_rows(database)[0])
    journal_before = _lossless_review_due_journal_snapshot(database)

    with pytest.raises(ReviewDueConflict, match="trigger"):
        _repository(database, now).dispatch_due(limit=1)

    assert _schedule_snapshot(database, "plan-a") == schedule_before
    assert tuple(_mailbox_rows(database)[0]) == mailbox_before
    assert _lossless_review_due_journal_snapshot(database) == journal_before


@pytest.mark.asyncio
async def test_schedule_journal_blob_logical_key_alias_rolls_back_dispatch(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    await _prepare_exact_delivery_replay(
        database,
        store,
        now,
        keep_journal=True,
    )
    with sqlite3.connect(database.config.sqlite_path) as conn:
        updated = conn.execute(
            """
            UPDATE agent_review_schedule_events
            SET schedule_event_id = CAST(schedule_event_id AS BLOB)
            WHERE source = ?
            """,
            (REVIEW_DUE_EVENT_SOURCE,),
        )
        assert updated.rowcount == 1
        representation = conn.execute(
            """
            SELECT typeof(schedule_event_id)
            FROM agent_review_schedule_events WHERE source = ?
            """,
            (REVIEW_DUE_EVENT_SOURCE,),
        ).fetchone()
    assert representation is not None
    assert str(representation[0]) == "blob"
    schedule_before = _schedule_snapshot(database, "plan-a")
    mailbox_before = tuple(_mailbox_rows(database)[0])
    journal_before = _lossless_review_due_journal_snapshot(database)
    assert len(journal_before) == 1

    with pytest.raises(
        ReviewDueConflict,
        match="deterministic review schedule event contains conflicting payload",
    ):
        _repository(database, now).dispatch_due(limit=1)

    assert _schedule_snapshot(database, "plan-a") == schedule_before
    assert tuple(_mailbox_rows(database)[0]) == mailbox_before
    assert _lossless_review_due_journal_snapshot(database) == journal_before
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, attempt_count, delivery_cycle
            FROM agent_review_schedules WHERE plan_id = 'plan-a'
            """
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == ("scheduled", 0, 0)


@pytest.mark.asyncio
async def test_schedule_journal_exact_same_transaction_replay_is_idempotent(
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
        conn.execute("BEGIN IMMEDIATE")
        schedule = conn.execute(
            "SELECT * FROM agent_review_schedules WHERE plan_id = 'plan-a'"
        ).fetchone()
        aggregate = conn.execute(
            """
            SELECT * FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        assert schedule is not None
        assert aggregate is not None
        for _attempt in range(2):
            DurableReviewDueRepository._append_schedule_event(
                conn,
                schedule,
                aggregate,
                event_id=event_id,
                event_type="due_dispatched",
                outcome="claimed",
                reason="review_schedule_due",
                now=now[0],
            )
        count = conn.execute(
            """
            SELECT COUNT(*) AS count FROM agent_review_schedule_events
            WHERE source = ?
            """,
            (REVIEW_DUE_EVENT_SOURCE,),
        ).fetchone()
    assert count is not None
    assert int(count["count"]) == 1


@pytest.mark.asyncio
async def test_schedule_journal_replay_with_different_created_at_conflicts(
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
        conn.execute("BEGIN IMMEDIATE")
        schedule = conn.execute(
            "SELECT * FROM agent_review_schedules WHERE plan_id = 'plan-a'"
        ).fetchone()
        aggregate = conn.execute(
            """
            SELECT * FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        assert schedule is not None
        assert aggregate is not None
        DurableReviewDueRepository._append_schedule_event(
            conn,
            schedule,
            aggregate,
            event_id=event_id,
            event_type="due_dispatched",
            outcome="claimed",
            reason="review_schedule_due",
            now=now[0],
        )
        with pytest.raises(ReviewDueConflict, match="created_at"):
            DurableReviewDueRepository._append_schedule_event(
                conn,
                schedule,
                aggregate,
                event_id=event_id,
                event_type="due_dispatched",
                outcome="claimed",
                reason="review_schedule_due",
                now=now[0] + 1.0,
            )


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
