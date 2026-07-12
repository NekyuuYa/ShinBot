from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path

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
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager


def _make_database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _activate_actor_v2(database: DatabaseManager, key: SessionKey) -> int:
    """Claim the actor runtime owner and return its active fence generation."""

    return database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="session actor recovery integration test",
    ).ownership.generation


async def _seed_settling(
    database: DatabaseManager,
    store: SQLiteSessionActorStore,
    key: SessionKey,
) -> int:
    generation = _activate_actor_v2(database, key)
    await store.ensure(key, ownership_generation=generation)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = 'active_chat_settling',
                state_revision = 4,
                event_sequence = 7,
                active_epoch = 2,
                activity_generation = 3,
                idle_planning_operation_id = 'idle-planning-1'
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )
    return generation


def _insert_effect(
    database: DatabaseManager,
    key: SessionKey,
    *,
    effect_id: str,
    operation_id: str,
    ownership_generation: int,
    status: str,
) -> None:
    contract = builtin_effect_contract("run_idle_review_planning")
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id, event_id,
                ownership_generation, operation_id, kind, contract_version,
                contract_signature, payload_json, status, attempt_count,
                available_at, claim_id, lease_owner, lease_until, created_at,
                updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, 0, 100, '', '', NULL, 100, 100, NULL, '')
            """,
            (
                effect_id,
                f"idempotency:{effect_id}",
                key.profile_id,
                key.session_id,
                f"source:{effect_id}",
                ownership_generation,
                operation_id,
                contract.effect_kind,
                contract.version,
                contract.signature,
                status,
            ),
        )


@pytest.mark.asyncio
async def test_restart_enqueues_and_drains_fenced_non_idle_recovery(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    original_store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    await _seed_settling(database, original_store, key)

    restarted_store = SQLiteSessionActorStore(database, clock=lambda: 200.0)
    seen_payloads: list[dict[str, object]] = []

    def handler(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        assert event.kind == "RecoveryRequested"
        seen_payloads.append(dict(event.payload))
        return SessionTransition(
            aggregate=aggregate.advance(
                state="idle",
                idle_planning_operation_id="",
                updated_at=event.occurred_at,
            ),
            disposition="recovery_settled",
        )

    registry = AgentSessionActorRegistry(store=restarted_store, handler=handler)
    try:
        assert await registry.recover() == 1
        await registry.wait_idle(key)
    finally:
        await registry.shutdown()

    recovered = await restarted_store.load(key)
    assert recovered.state == "idle"
    assert seen_payloads == [
        {
            "reason": "non_idle_without_live_completion",
            "expected_state": "active_chat_settling",
            "expected_state_revision": 4,
            "expected_event_sequence": 7,
            "expected_active_epoch": 2,
            "expected_activity_generation": 3,
            "operation_id": "idle-planning-1",
            "review_operation_id": "",
            "active_reply_operation_id": "",
            "active_chat_round_operation_id": "",
            "idle_planning_operation_id": "idle-planning-1",
        }
    ]
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id, kind, source, status, correlation_id, payload_json
            FROM agent_session_mailbox
            """
        ).fetchall()
    assert len(rows) == 1
    assert str(rows[0]["event_id"]).startswith("recovery-requested:")
    assert tuple(rows[0][name] for name in ("kind", "source", "status")) == (
        "RecoveryRequested",
        "session_actor_recovery",
        "completed",
    )
    assert rows[0]["correlation_id"] == "idle-planning-1"
    assert json.loads(rows[0]["payload_json"]) == seen_payloads[0]

    second_registry = AgentSessionActorRegistry(store=restarted_store, handler=handler)
    try:
        assert await second_registry.recover() == 0
        assert second_registry.actor_for(key) is None
    finally:
        await second_registry.shutdown()


@pytest.mark.parametrize("status", ["pending", "processing"])
@pytest.mark.asyncio
async def test_recovery_waits_for_relevant_live_effect(
    tmp_path: Path,
    status: str,
) -> None:
    database = _make_database(tmp_path)
    store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_settling(database, store, key)
    _insert_effect(
        database,
        key,
        effect_id="effect-live",
        operation_id="idle-planning-1",
        ownership_generation=generation,
        status=status,
    )

    assert await store.enqueue_recovery_requests() == 0
    assert await store.pending_keys() == []

    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET status = 'failed'
            WHERE effect_id = 'effect-live'
            """
        )
    assert await store.enqueue_recovery_requests() == 1
    assert await store.enqueue_recovery_requests() == 0
    assert await store.pending_keys() == [key]


@pytest.mark.asyncio
async def test_recovery_ignores_unrelated_live_effect(tmp_path: Path) -> None:
    database = _make_database(tmp_path)
    store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_settling(database, store, key)
    _insert_effect(
        database,
        key,
        effect_id="effect-unrelated",
        operation_id="different-operation",
        ownership_generation=generation,
        status="pending",
    )

    assert await store.enqueue_recovery_requests() == 1


@pytest.mark.parametrize("mailbox_status", ["pending", "processing"])
@pytest.mark.asyncio
async def test_recovery_waits_for_existing_mailbox_path(
    tmp_path: Path,
    mailbox_status: str,
) -> None:
    database = _make_database(tmp_path)
    store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_settling(database, store, key)
    await store.enqueue(
        SessionEventEnvelope(
            event_id="completion-1",
            key=key,
            kind="IdleReviewPlanningCompleted",
            ownership_generation=generation,
            correlation_id="idle-planning-1",
        )
    )
    if mailbox_status == "processing":
        assert await store.claim_next(key, worker_id="worker-1") is not None

    assert await store.enqueue_recovery_requests() == 0
    with database.connect() as conn:
        recovery_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_session_mailbox
            WHERE kind = 'RecoveryRequested'
            """
        ).fetchone()[0]
    assert recovery_count == 0


@pytest.mark.asyncio
async def test_invalid_clock_cannot_create_infinite_mailbox_head(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    now = [math.inf]
    store = SQLiteSessionActorStore(database, clock=lambda: now[0])
    key = SessionKey("profile-a", "bot:group:room")
    generation = _activate_actor_v2(database, key)

    with pytest.raises(ValueError, match="clock.*finite and non-negative"):
        await store.enqueue(
            SessionEventEnvelope(
                event_id="poison",
                key=key,
                kind="MessageReceived",
                ownership_generation=generation,
            )
        )
    now[0] = 100.0
    await store.enqueue(
        SessionEventEnvelope(
            event_id="healthy",
            key=key,
            kind="MessageReceived",
            ownership_generation=generation,
        )
    )
    claim = await store.claim_next(key, worker_id="worker-1")
    assert claim is not None
    assert claim.envelope.event_id == "healthy"


@pytest.mark.asyncio
async def test_derived_lease_overflow_does_not_claim_mailbox_head(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    store = SQLiteSessionActorStore(
        database,
        lease_seconds=1e308,
        clock=lambda: 1e308,
    )
    key = SessionKey("profile-a", "bot:group:room")
    generation = _activate_actor_v2(database, key)
    await store.enqueue(
        SessionEventEnvelope(
            event_id="event-1",
            key=key,
            kind="MessageReceived",
            ownership_generation=generation,
            occurred_at=1.0,
            available_at=1.0,
            created_at=1.0,
        )
    )

    with pytest.raises(ValueError, match="lease_until.*finite and non-negative"):
        await store.claim_next(key, worker_id="worker-1")
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, lease_until
            FROM agent_session_mailbox
            WHERE event_id = 'event-1'
            """
        ).fetchone()
    assert tuple(row) == ("pending", None)


@pytest.mark.asyncio
async def test_effect_outbox_rejects_unknown_status(tmp_path: Path) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    generation = _activate_actor_v2(database, key)
    await store.ensure(key, ownership_generation=generation)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        _insert_effect(
            database,
            key,
            effect_id="effect-invalid",
            operation_id="operation-1",
            ownership_generation=generation,
            status="unknown",
        )
