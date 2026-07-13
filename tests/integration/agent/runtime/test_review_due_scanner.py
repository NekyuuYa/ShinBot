"""Integration tests for durable actor ReviewDue scanning and wake debt."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass, field
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
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
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
    ReviewDueConflict,
    ReviewDueDisposition,
    ReviewDueWakeError,
    review_due_event_id,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord

_MAILBOX_RAW_LOGICAL_KEY_INDEX = "idx_agent_session_mailbox_raw_logical_key"
_SCHEDULE_EVENT_RAW_LOGICAL_KEY_INDEX = (
    "idx_agent_review_schedule_events_raw_logical_key"
)


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
) -> tuple[SessionKey, int, str]:
    key = key or SessionKey("profile-a", "session-a")
    generation = await _seed_due_schedule(
        database,
        store,
        key=key,
        plan_id="plan-a",
    )
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
        database.agent_runtime_ownership.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=generation,
            reason="freeze schedule for review due ordering test",
        )
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
async def test_first_actionable_message_reaches_review_through_durable_scanner(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store = _make_runtime(tmp_path, now)
    key = SessionKey("profile-a", "profile-a:group:room-a")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="initial review scanner integration test",
        legacy_session_id="legacy:profile-a:group:room-a",
    ).ownership
    await store.ensure(key, ownership_generation=ownership.generation)
    message_log_id = database.message_logs.insert(
        MessageLogRecord(
            session_id="instance-a:group:room-a",
            platform_msg_id="platform:initial-review",
            sender_id="user-a",
            sender_name="User A",
            raw_text="hello",
            content_json="[]",
            role="user",
            created_at=10.0,
        )
    )
    event_id = "message-received:initial-review"
    message = SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=ownership.generation,
        source="agent_route_outbox",
        occurred_at=10.0,
        created_at=10.0,
        available_at=10.0,
        trace_id="trace:initial-review",
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
            "bot_id": key.profile_id,
            "bot_binding_id": "binding-a",
            "base_session_id": "instance-a:group:room-a",
            "bot_session_id": key.session_id,
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
            "trace_id": "trace:initial-review",
            "observed_at": 10.0,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
    )
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(default_review_delay_seconds=0.0)
    )
    await store.enqueue(message)
    message_claim = await store.claim_next(key, worker_id="message-worker")
    assert message_claim is not None
    initial = await store.load(key)
    scheduled = await store.commit(
        message_claim,
        reducer.reduce(initial, message_claim.envelope),
        expected_revision=initial.state_revision,
    )

    summary = _repository(database, now).dispatch_due(limit=1)

    assert summary.dispatched_count == 1
    due_claim = await store.claim_next(key, worker_id="review-worker")
    assert due_claim is not None
    assert due_claim.envelope.kind == AgentSessionEventKind.REVIEW_DUE
    due_transition = reducer.reduce(scheduled, due_claim.envelope)
    assert due_transition.disposition == "review_started"
    assert due_transition.caused_plan_id == scheduled.current_plan_id
    reviewing = await store.commit(
        due_claim,
        due_transition,
        expected_revision=scheduled.state_revision,
    )
    assert reviewing.state == AgentSessionState.REVIEW
    assert reviewing.review_operation_id
    with database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT status, delivery_cycle FROM agent_review_schedules
            WHERE plan_id = ?
            """,
            (reviewing.current_plan_id,),
        ).fetchone()
        effect = conn.execute(
            """
            SELECT kind, status FROM agent_effect_outbox
            WHERE operation_id = ?
            """,
            (reviewing.review_operation_id,),
        ).fetchone()
    assert schedule is not None
    assert tuple(schedule) == ("claimed", 1)
    assert effect is not None
    assert tuple(effect) == ("run_review_workflow", "pending")


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
