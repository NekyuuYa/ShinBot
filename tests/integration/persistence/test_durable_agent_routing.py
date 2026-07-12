"""Integration tests for recoverable core-to-Agent routing transactions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.core.dispatch.agent_delivery import AgentRouteDelivery
from shinbot.core.dispatch.agent_identity import SessionKeyFactory
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipEvidenceConflict,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipNotFound,
)
from shinbot.core.dispatch.durable_routing import MessageRoutingJobEnvelope
from shinbot.persistence import DatabaseManager, MessageLogRecord
from shinbot.persistence.repositories.durable_routing import (
    ClaimedMessageRoutingJob,
    DurableMessageRoutingRepository,
    DurableRoutingConflict,
    DurableRoutingLeaseLost,
)
from shinbot.persistence.schema import SCHEMA_STATEMENTS
from shinbot.schema.routing import MessageRoutingSkipReason, MessageRoutingStatus


class _Clock:
    def __init__(self, value: float = 1_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture
def routing_store(
    tmp_path: Path,
) -> tuple[DatabaseManager, DurableMessageRoutingRepository, _Clock]:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    default_key = SessionKeyFactory().create(
        bot_config_id="bot-a",
        bot_id="bot-a",
        bot_session_id="bot-a:group:room",
        base_session_id="instance-main:group:room",
    )
    db.agent_runtime_ownership.claim(
        default_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="durable routing integration fixture",
        legacy_session_id="instance-main:group:room",
    )
    clock = _Clock()
    store = DurableMessageRoutingRepository(db, lease_seconds=5.0, clock=clock)
    return db, store, clock


def _record(*, raw_text: str = "hello") -> MessageLogRecord:
    return MessageLogRecord(
        session_id="instance-main:group:room",
        platform_msg_id="platform-message-a",
        sender_id="user-a",
        sender_name="User A",
        content_json='[{"type":"text","attrs":{"text":"hello"}}]',
        raw_text=raw_text,
        role="user",
        is_mentioned=True,
        created_at=1_000_000.0,
    )


def _job(*, payload_value: str = "message-created") -> MessageRoutingJobEnvelope:
    return MessageRoutingJobEnvelope(
        job_id="routing-job-a",
        idempotency_key="ingress:instance-main:platform-message-a",
        trace_id="trace-a",
        correlation_id="correlation-a",
        causation_id="platform-message-a",
        occurred_at=999.0,
        payload={"event_type": payload_value, "instance_id": "instance-main"},
    )


def _delivery(
    message_log_id: int,
    *,
    bot_id: str = "bot-a",
    route_rule_id: str = "builtin.agent_entry_fallback",
) -> AgentRouteDelivery:
    base_session_id = "instance-main:group:room"
    bot_session_id = f"{bot_id}:group:room"
    return AgentRouteDelivery(
        session_key=SessionKeyFactory().create(
            bot_config_id=bot_id,
            bot_id=bot_id,
            bot_session_id=bot_session_id,
            base_session_id=base_session_id,
        ),
        bot_id=bot_id,
        bot_binding_id=f"{bot_id}-binding",
        base_session_id=base_session_id,
        bot_session_id=bot_session_id,
        message_log_id=message_log_id,
        sender_id="user-a",
        instance_id="instance-main",
        platform="mock",
        self_id=f"{bot_id}-self",
        is_private=False,
        is_mentioned=True,
        is_mention_to_other=False,
        is_reply_to_bot=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        already_handled=False,
        is_stopped=False,
        trace_id="trace-a",
        observed_at=999.0,
        route_rule_id=route_rule_id,
    )


def _activate_delivery_owner(
    db: DatabaseManager,
    delivery: AgentRouteDelivery,
) -> None:
    db.agent_runtime_ownership.claim(
        delivery.session_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="durable routing test activation",
        legacy_session_id=delivery.base_session_id,
    )


def _persist_and_claim(
    store: DurableMessageRoutingRepository,
) -> tuple[int, ClaimedMessageRoutingJob]:
    persisted = store.persist_message_and_job(_record(), _job())
    claim = store.claim_next_job(worker_id="router-a")
    assert claim is not None
    return persisted.message_log_id, claim


def _install_abort_trigger(
    db: DatabaseManager,
    *,
    name: str,
    table: str,
    timing_sql: str,
    when_sql: str = "",
) -> None:
    with db.connect() as conn:
        conn.execute(
            f"""
            CREATE TRIGGER {name}
            {timing_sql} ON {table} {when_sql}
            BEGIN
                SELECT RAISE(ABORT, 'simulated crash boundary');
            END
            """
        )


def _drop_trigger(db: DatabaseManager, name: str) -> None:
    with db.connect() as conn:
        conn.execute(f"DROP TRIGGER {name}")


def test_schema_creates_recoverable_routing_tables(routing_store) -> None:
    db, _store, _clock = routing_store

    with db.connect() as conn:
        tables = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        job_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(message_routing_jobs)")
        }
        outbox_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(agent_route_outbox)")
        }

    assert {"message_routing_jobs", "agent_route_outbox"} <= tables
    assert {
        "version",
        "idempotency_key",
        "trace_id",
        "profile_id",
        "session_id",
        "ownership_generation",
        "claim_id",
        "lease_owner",
        "lease_until",
        "decision_id",
    } <= job_columns
    assert {
        "version",
        "idempotency_key",
        "trace_id",
        "claim_id",
        "lease_owner",
        "lease_until",
        "event_id",
        "route_rule_id",
        "ownership_generation",
    } <= outbox_columns


def test_message_log_and_routing_job_share_one_crash_boundary(routing_store) -> None:
    db, store, _clock = routing_store
    _install_abort_trigger(
        db,
        name="abort_routing_job_insert",
        table="message_routing_jobs",
        timing_sql="BEFORE INSERT",
    )

    with pytest.raises(sqlite3.IntegrityError, match="simulated crash boundary"):
        store.persist_message_and_job(_record(), _job())

    with db.connect() as conn:
        message_count = conn.execute("SELECT COUNT(*) FROM message_logs").fetchone()[0]
        job_count = conn.execute("SELECT COUNT(*) FROM message_routing_jobs").fetchone()[0]
    assert message_count == 0
    assert job_count == 0

    _drop_trigger(db, "abort_routing_job_insert")
    result = store.persist_message_and_job(_record(), _job())
    assert result.inserted is True


def test_routing_job_persists_canonical_session_ownership_fence(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    envelope = replace(
        _job(),
        profile_id="bot-a",
        session_id="bot-a:group:room",
        ownership_generation=1,
    )

    persisted = store.persist_message_and_job(_record(), envelope)
    restored = store.get_job(persisted.routing_job_id)

    assert restored is not None
    assert restored.envelope.profile_id == "bot-a"
    assert restored.envelope.session_id == "bot-a:group:room"
    assert restored.envelope.ownership_generation == 1
    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation
            FROM message_routing_jobs
            """
        ).fetchone()
    assert tuple(row) == ("bot-a", "bot-a:group:room", 1)


def test_actor_migration_refences_job_claim_and_abort_recovers(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    key = SessionKeyFactory().create(
        bot_config_id="bot-a",
        bot_id="bot-a",
        bot_session_id="bot-a:group:room",
        base_session_id="instance-main:group:room",
    )
    owner = db.agent_runtime_ownership.get(key)
    assert owner is not None
    envelope = replace(
        _job(),
        profile_id=key.profile_id,
        session_id=key.session_id,
        ownership_generation=owner.generation,
    )
    persisted = store.persist_message_and_job(_record(), envelope)
    old_claim = store.claim_job(
        persisted.routing_job_id,
        worker_id="live-router",
        ignore_available_at=True,
    )
    assert old_claim is not None

    migrating = db.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="exercise routing fence",
    )

    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT status, ownership_generation, attempt_count,
                   claim_id, lease_owner, lease_until
            FROM message_routing_jobs
            """
        ).fetchone()
    assert tuple(row) == ("pending", migrating.generation, 1, "", "", None)
    assert store.claim_next_job(worker_id="recovery-during-migration") is None
    assert (
        store.claim_job(
            persisted.routing_job_id,
            worker_id="live-during-migration",
            ignore_available_at=True,
        )
        is None
    )
    with pytest.raises(DurableRoutingConflict, match="claim no longer matches"):
        store.complete_dispatched_without_agent(old_claim)
    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        db.agent_runtime_ownership.complete_migration(
            key,
            expected_generation=migrating.generation,
            reason="must not abandon buffered ingress",
        )
    assert caught.value.evidence == ("actor_message_routing_job",)

    restored = db.agent_runtime_ownership.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="restore actor routing",
    )
    recovered = store.claim_next_job(worker_id="recovered-router")
    assert recovered is not None
    assert recovered.envelope.ownership_generation == restored.generation
    assert recovered.attempt_count == 2
    store.complete_dispatched_without_agent(recovered)


def test_legacy_to_actor_activation_refences_buffered_routing_job(
    tmp_path: Path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    clock = _Clock()
    store = DurableMessageRoutingRepository(db, lease_seconds=5.0, clock=clock)
    key = SessionKeyFactory().create(
        bot_config_id="bot-a",
        bot_id="bot-a",
        bot_session_id="bot-a:group:room",
        base_session_id="instance-main:group:room",
    )
    legacy = db.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy baseline",
        legacy_session_id="instance-main:group:room",
    ).ownership
    migrating = db.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=legacy.generation,
        reason="buffer actor ingress",
    )
    envelope = replace(
        _job(),
        profile_id=key.profile_id,
        session_id=key.session_id,
        ownership_generation=migrating.generation,
    )
    persisted = store.persist_message_and_job(_record(), envelope)

    assert store.claim_next_job(worker_id="router-during-migration") is None
    with db.connect() as conn:
        attempt_count = conn.execute(
            "SELECT attempt_count FROM message_routing_jobs"
        ).fetchone()[0]
    assert attempt_count == 0

    activated = db.agent_runtime_ownership.complete_migration(
        key,
        expected_generation=migrating.generation,
        reason="activate actor runtime",
    )
    recovered = store.claim_job(
        persisted.routing_job_id,
        worker_id="actor-router",
        ignore_available_at=True,
    )
    assert recovered is not None
    assert recovered.envelope.ownership_generation == activated.generation
    assert recovered.attempt_count == 1


def test_message_and_job_insert_is_idempotent_but_rejects_conflicts(routing_store) -> None:
    db, store, _clock = routing_store
    first = store.persist_message_and_job(_record(), _job())
    duplicate = store.persist_message_and_job(_record(), _job())

    assert duplicate.duplicate is True
    assert duplicate.message_log_id == first.message_log_id
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM message_logs").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM message_routing_jobs").fetchone()[0] == 1

    with pytest.raises(DurableRoutingConflict, match="different work"):
        store.persist_message_and_job(_record(raw_text="different"), _job())
    with pytest.raises(DurableRoutingConflict, match="different work"):
        store.persist_message_and_job(_record(), _job(payload_value="notice-created"))
    with pytest.raises(DurableRoutingConflict, match="different work"):
        store.persist_message_and_job(
            _record(),
            replace(_job(), job_id="different-job-id"),
        )


def test_route_decision_and_all_outbox_rows_share_one_crash_boundary(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    message_log_id, claim = _persist_and_claim(store)
    delivery = _delivery(message_log_id)
    _install_abort_trigger(
        db,
        name="abort_routing_job_complete",
        table="message_routing_jobs",
        timing_sql="BEFORE UPDATE OF status",
        when_sql="WHEN NEW.status = 'completed'",
    )

    with pytest.raises(sqlite3.IntegrityError, match="simulated crash boundary"):
        store.complete_with_agent_deliveries(claim, [delivery])

    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM agent_route_outbox").fetchone()[0] == 0
        job = conn.execute("SELECT status FROM message_routing_jobs").fetchone()
        message = conn.execute("SELECT routing_status FROM message_logs").fetchone()
    assert job["status"] == "processing"
    assert message["routing_status"] == MessageRoutingStatus.PENDING.value

    _drop_trigger(db, "abort_routing_job_complete")
    committed = store.complete_with_agent_deliveries(claim, [delivery])
    assert committed.inserted_delivery_count == 1


def test_route_decision_isolates_profiles_and_route_rules(routing_store) -> None:
    db, store, _clock = routing_store
    message_log_id, claim = _persist_and_claim(store)
    deliveries = [
        _delivery(message_log_id, bot_id="bot-a"),
        _delivery(message_log_id, bot_id="bot-b"),
        _delivery(
            message_log_id,
            bot_id="bot-a",
            route_rule_id="plugin.audit_agent_entry",
        ),
    ]
    for delivery in deliveries:
        _activate_delivery_owner(db, delivery)

    result = store.complete_with_agent_deliveries(claim, deliveries)
    replay = store.complete_with_agent_deliveries(claim, list(reversed(deliveries)))

    assert result.inserted_delivery_count == 3
    assert replay.duplicate is True
    assert len(set(result.delivery_ids)) == 3
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT profile_id, session_id, route_rule_id, event_id
            FROM agent_route_outbox
            ORDER BY profile_id, route_rule_id
            """
        ).fetchall()
    assert len(rows) == 3
    assert len({str(row["event_id"]) for row in rows}) == 2
    bot_a_event_ids = {
        str(row["event_id"]) for row in rows if str(row["profile_id"]) == "bot-a"
    }
    assert len(bot_a_event_ids) == 1
    assert {
        (str(row["profile_id"]), str(row["route_rule_id"])) for row in rows
    } == {
        ("bot-a", "builtin.agent_entry_fallback"),
        ("bot-b", "builtin.agent_entry_fallback"),
        ("bot-a", "plugin.audit_agent_entry"),
    }


def test_completed_route_decision_rejects_a_conflicting_replay(routing_store) -> None:
    _db, store, _clock = routing_store
    message_log_id, claim = _persist_and_claim(store)
    store.complete_with_agent_deliveries(claim, [_delivery(message_log_id)])

    with pytest.raises(DurableRoutingConflict, match="different decision"):
        store.complete_with_agent_deliveries(
            claim,
            [
                _delivery(
                    message_log_id,
                    route_rule_id="plugin.different_agent_entry",
                )
            ],
        )


def test_route_decision_fails_closed_without_actor_ownership(routing_store) -> None:
    db, store, _clock = routing_store
    message_log_id, claim = _persist_and_claim(store)
    unowned = _delivery(message_log_id, bot_id="bot-unowned")

    with pytest.raises(AgentRuntimeOwnershipNotFound):
        store.complete_with_agent_deliveries(claim, [unowned])

    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM agent_route_outbox").fetchone()[0] == 0
        job = conn.execute("SELECT status FROM message_routing_jobs").fetchone()
    assert job["status"] == "processing"


def test_multi_profile_decision_rolls_back_when_one_owner_is_missing(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    message_log_id, claim = _persist_and_claim(store)
    deliveries = [
        _delivery(message_log_id, bot_id="bot-a"),
        _delivery(message_log_id, bot_id="bot-unowned"),
    ]

    with pytest.raises(AgentRuntimeOwnershipNotFound):
        store.complete_with_agent_deliveries(claim, deliveries)

    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM agent_route_outbox").fetchone()[0] == 0
        message = conn.execute("SELECT routing_status FROM message_logs").fetchone()
    assert message["routing_status"] == MessageRoutingStatus.PENDING.value


def test_routing_job_claim_rejects_aba_after_expiry_and_reclaim(routing_store) -> None:
    _db, store, clock = routing_store
    persisted = store.persist_message_and_job(_record(), _job())
    first = store.claim_next_job(worker_id="router-a")
    assert first is not None
    clock.advance(6.0)
    second = store.claim_next_job(worker_id="router-b")
    assert second is not None
    assert first.claim_id != second.claim_id

    with pytest.raises(DurableRoutingLeaseLost, match="no longer owned"):
        store.complete_with_agent_deliveries(
            first,
            [_delivery(persisted.message_log_id)],
        )
    committed = store.complete_with_agent_deliveries(
        second,
        [_delivery(persisted.message_log_id)],
    )
    assert committed.inserted_delivery_count == 1


def test_route_decision_rejects_a_forged_claim_identity(routing_store) -> None:
    _db, store, _clock = routing_store
    message_log_id, claim = _persist_and_claim(store)
    forged = replace(
        claim,
        envelope=replace(claim.envelope, trace_id="forged-trace"),
    )

    with pytest.raises(DurableRoutingConflict, match="claim no longer matches"):
        store.complete_with_agent_deliveries(forged, [_delivery(message_log_id)])


def test_skipped_decision_is_explicit_and_creates_no_agent_outbox(routing_store) -> None:
    db, store, _clock = routing_store
    _message_log_id, claim = _persist_and_claim(store)

    result = store.complete_without_agent_delivery(
        claim,
        skip_reason=MessageRoutingSkipReason.NO_ROUTE_MATCHED,
    )

    assert result.delivery_ids == ()
    with db.connect() as conn:
        message = conn.execute(
            "SELECT routing_status, routing_skip_reason FROM message_logs"
        ).fetchone()
        assert conn.execute("SELECT COUNT(*) FROM agent_route_outbox").fetchone()[0] == 0
    assert message["routing_status"] == MessageRoutingStatus.SKIPPED.value
    assert message["routing_skip_reason"] == MessageRoutingSkipReason.NO_ROUTE_MATCHED.value


def test_mailbox_insert_and_outbox_completion_share_one_crash_boundary(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    store.complete_with_agent_deliveries(job_claim, [_delivery(message_log_id)])
    delivery_claim = store.claim_next_delivery(worker_id="relay-a")
    assert delivery_claim is not None
    _install_abort_trigger(
        db,
        name="abort_outbox_complete",
        table="agent_route_outbox",
        timing_sql="BEFORE UPDATE OF status",
        when_sql="WHEN NEW.status = 'completed'",
    )

    with pytest.raises(sqlite3.IntegrityError, match="simulated crash boundary"):
        store.relay_delivery(delivery_claim)

    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()[0] == 0
        outbox = conn.execute("SELECT status FROM agent_route_outbox").fetchone()
    assert outbox["status"] == "processing"

    _drop_trigger(db, "abort_outbox_complete")
    relayed = store.relay_delivery(delivery_claim)
    assert relayed.mailbox_inserted is True
    with db.connect() as conn:
        aggregate = conn.execute(
            "SELECT ownership_generation FROM agent_session_aggregates"
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT ownership_generation, causation_id, correlation_id
            FROM agent_session_mailbox
            """
        ).fetchone()
    assert aggregate["ownership_generation"] == 1
    assert tuple(mailbox) == (1, "routing-job-a", "routing-job-a")


def test_relay_rejects_existing_aggregate_from_another_generation(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    delivery = _delivery(message_log_id)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="relay-a")
    assert delivery_claim is not None
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation,
                created_at, updated_at
            ) VALUES (?, ?, 0, ?, ?)
            """,
            (
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
                1_000.0,
                1_000.0,
            ),
        )

    with pytest.raises(DurableRoutingConflict, match="ownership generation"):
        store.relay_delivery(delivery_claim)

    with db.connect() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0] == 0


def test_relay_is_idempotent_and_validates_existing_mailbox_identity(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    delivery = _delivery(message_log_id)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="relay-a")
    assert delivery_claim is not None

    first = store.relay_delivery(delivery_claim)
    replay = store.relay_delivery(delivery_claim)

    assert first.mailbox_inserted is True
    assert replay.duplicate is True
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()[0] == 1

        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET payload_json = '{"forged":true}'
            WHERE event_id = ?
            """,
            (delivery.event_id,),
        )
    with pytest.raises(DurableRoutingConflict, match="different work"):
        store.relay_delivery(delivery_claim)


def test_outbox_claim_rejects_a_resigned_noncanonical_payload(routing_store) -> None:
    db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    delivery = _delivery(message_log_id)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    with db.connect() as conn:
        payload = delivery.to_payload()
        payload["forged_extra"] = True
        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        conn.execute(
            """
            UPDATE agent_route_outbox
            SET payload_json = ?, payload_digest = ?
            WHERE delivery_id = ?
            """,
            (payload_json, digest, delivery.delivery_id),
        )

    with pytest.raises(DurableRoutingConflict, match="canonical delivery contract"):
        store.claim_next_delivery(worker_id="relay-a")


def test_outbox_claim_rejects_aba_after_expiry_and_reclaim(routing_store) -> None:
    _db, store, clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    store.complete_with_agent_deliveries(job_claim, [_delivery(message_log_id)])
    first = store.claim_next_delivery(worker_id="relay-a")
    assert first is not None
    clock.advance(6.0)
    second = store.claim_next_delivery(worker_id="relay-b")
    assert second is not None
    assert first.claim_id != second.claim_id

    with pytest.raises(DurableRoutingLeaseLost, match="no longer owned"):
        store.relay_delivery(first)
    result = store.relay_delivery(second)
    assert result.mailbox_inserted is True


def test_relay_is_fenced_by_actor_ownership_generation(routing_store) -> None:
    db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    delivery = _delivery(message_log_id)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="relay-a")
    assert delivery_claim is not None
    ownership = db.agent_runtime_ownership.get(delivery.session_key)
    assert ownership is not None
    migrating = db.agent_runtime_ownership.begin_migration(
        delivery.session_key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=ownership.generation,
        reason="test ownership fence",
    )

    with pytest.raises(DurableRoutingConflict, match="claim no longer matches"):
        store.relay_delivery(delivery_claim)
    assert store.claim_next_delivery(worker_id="relay-during-migration") is None
    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        db.agent_runtime_ownership.complete_migration(
            delivery.session_key,
            expected_generation=migrating.generation,
            reason="must not abandon route outbox",
        )
    assert caught.value.evidence == ("actor_route_outbox",)

    with db.connect() as conn:
        pending = conn.execute(
            """
            SELECT status, ownership_generation, attempt_count,
                   claim_id, lease_owner, lease_until
            FROM agent_route_outbox
            """
        ).fetchone()
    assert tuple(pending) == ("pending", migrating.generation, 1, "", "", None)

    restored = db.agent_runtime_ownership.abort_migration(
        delivery.session_key,
        expected_generation=migrating.generation,
        reason="restore actor relay",
    )
    recovered = store.claim_next_delivery(worker_id="relay-recovered")
    assert recovered is not None
    assert recovered.ownership_generation == restored.generation
    result = store.relay_delivery(recovered)
    assert result.mailbox_inserted is True

    with db.connect() as conn:
        aggregate = conn.execute(
            "SELECT ownership_generation FROM agent_session_aggregates"
        ).fetchone()
        mailbox = conn.execute(
            "SELECT ownership_generation FROM agent_session_mailbox"
        ).fetchone()
        terminal_job = conn.execute(
            "SELECT status, ownership_generation FROM message_routing_jobs"
        ).fetchone()
    assert tuple(aggregate) == (restored.generation,)
    assert tuple(mailbox) == (restored.generation,)
    assert tuple(terminal_job) == ("completed", 0)


def test_terminal_routing_history_does_not_block_actor_to_legacy_migration(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    key = SessionKeyFactory().create(
        bot_config_id="bot-a",
        bot_id="bot-a",
        bot_session_id="bot-a:group:room",
        base_session_id="instance-main:group:room",
    )
    owner = db.agent_runtime_ownership.get(key)
    assert owner is not None
    envelope = replace(
        _job(),
        profile_id=key.profile_id,
        session_id=key.session_id,
        ownership_generation=owner.generation,
    )
    persisted = store.persist_message_and_job(_record(), envelope)
    job_claim = store.claim_next_job(worker_id="router")
    assert job_claim is not None
    store.complete_with_agent_deliveries(
        job_claim,
        [_delivery(persisted.message_log_id)],
    )
    delivery_claim = store.claim_next_delivery(worker_id="relay")
    assert delivery_claim is not None
    store.retry_or_fail_delivery(
        delivery_claim,
        error_code="terminal_test",
        error_message="intentional terminal history",
    )

    migrating = db.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="terminal history is inert",
    )
    completed = db.agent_runtime_ownership.complete_migration(
        key,
        expected_generation=migrating.generation,
        reason="activate legacy with retained history",
    )

    assert completed.mode is AgentRuntimeOwnershipMode.LEGACY
    with db.connect() as conn:
        job = conn.execute(
            "SELECT status, ownership_generation FROM message_routing_jobs"
        ).fetchone()
        outbox = conn.execute(
            "SELECT status, ownership_generation FROM agent_route_outbox"
        ).fetchone()
    assert tuple(job) == ("completed", owner.generation)
    assert tuple(outbox) == ("failed", owner.generation)


def test_intermediate_outbox_schema_migrates_old_rows_fail_closed(
    routing_store,
) -> None:
    db, store, _clock = routing_store
    persisted = store.persist_message_and_job(_record(), _job())
    delivery = _delivery(persisted.message_log_id)
    payload_json = json.dumps(
        delivery.to_payload(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    payload_digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    current_ddl = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_route_outbox" in statement
    )
    intermediate_ddl = current_ddl.replace(
        "        ownership_generation INTEGER NOT NULL,\n",
        "",
    ).replace(
        "        CHECK(ownership_generation >= 1),\n",
        "",
    )
    with db.connect() as conn:
        conn.execute("DROP TABLE agent_route_outbox")
        conn.execute(intermediate_ddl)
        conn.execute(
            """
            INSERT INTO agent_route_outbox (
                delivery_id, idempotency_key, routing_job_id, profile_id,
                session_id, message_log_id, route_rule_id, version, event_id,
                payload_json, payload_digest, trace_id, correlation_id,
                causation_id, status, attempt_count, available_at,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
            """,
            (
                delivery.delivery_id,
                delivery.idempotency_key,
                _job().job_id,
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
                persisted.message_log_id,
                delivery.route_rule_id,
                delivery.version,
                delivery.event_id,
                payload_json,
                payload_digest,
                delivery.trace_id,
                _job().correlation_id,
                _job().job_id,
                1_000.0,
                1_000.0,
                1_000.0,
            ),
        )

    db.initialize()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT ownership_generation FROM agent_route_outbox"
        ).fetchone()
    assert row["ownership_generation"] == 0

    claim = store.claim_next_delivery(worker_id="relay-a")
    assert claim is None
    with db.connect() as conn:
        attempt_count = conn.execute(
            "SELECT attempt_count FROM agent_route_outbox"
        ).fetchone()[0]
        assert conn.execute("SELECT COUNT(*) FROM agent_session_aggregates").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()[0] == 0
    assert attempt_count == 0


def test_intermediate_routing_job_schema_backfills_session_fence_without_resigning(
    routing_store,
) -> None:
    db, _store, _clock = routing_store
    current_ddl = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS message_routing_jobs" in statement
    )
    ownership_check = """        CHECK(ownership_generation >= 0),
        CHECK(
            (
                profile_id = ''
                AND session_id = ''
                AND ownership_generation = 0
            )
            OR
            (
                profile_id != ''
                AND session_id != ''
                AND ownership_generation >= 1
            )
        ),
"""
    intermediate_ddl = (
        current_ddl.replace("        profile_id TEXT NOT NULL DEFAULT '',\n", "")
        .replace("        session_id TEXT NOT NULL DEFAULT '',\n", "")
        .replace("        ownership_generation INTEGER NOT NULL DEFAULT 0,\n", "")
        .replace(ownership_check, "")
    )
    payload_json = json.dumps(
        {
            "version": 1,
            "bot_id": "bot-a",
            "bot_session_id": "bot-a:group:room",
            "base_session_id": "instance-main:group:room",
            "ownership_generation": 4,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    payload_digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    with db.connect() as conn:
        conn.execute("DROP TABLE agent_route_outbox")
        conn.execute("DROP TABLE message_routing_jobs")
        conn.execute(intermediate_ddl)
        message_log_id = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES ('instance-main:group:room', 'user', 1000.0)
            """
        ).lastrowid
        assert message_log_id is not None
        conn.execute(
            """
            INSERT INTO message_routing_jobs (
                routing_job_id, idempotency_key, message_log_id, version,
                message_fingerprint, payload_json, payload_digest, trace_id,
                correlation_id, causation_id, occurred_at, status,
                attempt_count, available_at, created_at, updated_at
            ) VALUES (
                'routing-job-intermediate', 'routing-key-intermediate', ?, 1,
                'message-fingerprint', ?, ?, 'trace-a',
                'routing-job-intermediate', 'platform-message-a', 1000.0,
                'pending', 0, 1000.0, 1000.0, 1000.0
            )
            """,
            (message_log_id, payload_json, payload_digest),
        )

    db.initialize()

    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation,
                   payload_json, payload_digest
            FROM message_routing_jobs
            """
        ).fetchone()
        indexes = {
            str(item["name"])
            for item in conn.execute(
                "PRAGMA index_list(message_routing_jobs)"
            ).fetchall()
        }
    assert tuple(row) == (
        "bot-a",
        "bot-a:group:room",
        4,
        payload_json,
        payload_digest,
    )
    assert "idx_message_routing_jobs_session" in indexes


def test_relay_preserves_profile_and_route_rule_isolation(routing_store) -> None:
    db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    deliveries = [
        _delivery(message_log_id, bot_id="bot-a"),
        _delivery(message_log_id, bot_id="bot-b"),
        _delivery(
            message_log_id,
            bot_id="bot-a",
            route_rule_id="plugin.audit_agent_entry",
        ),
    ]
    for delivery in deliveries:
        _activate_delivery_owner(db, delivery)
    store.complete_with_agent_deliveries(job_claim, deliveries)

    results = []
    while (claim := store.claim_next_delivery(worker_id="relay-a")) is not None:
        results.append(store.relay_delivery(claim))

    assert len(results) == 3
    with db.connect() as conn:
        mailbox_rows = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation, event_id,
                   payload_json, causation_id, correlation_id
            FROM agent_session_mailbox
            ORDER BY profile_id, event_id
            """
        ).fetchall()
    assert len(mailbox_rows) == 2
    assert len({str(row["event_id"]) for row in mailbox_rows}) == 2
    assert {str(row["profile_id"]) for row in mailbox_rows} == {"bot-a", "bot-b"}
    assert {int(row["ownership_generation"]) for row in mailbox_rows} == {1}
    assert {str(row["causation_id"]) for row in mailbox_rows} == {"routing-job-a"}
    assert {str(row["correlation_id"]) for row in mailbox_rows} == {"routing-job-a"}
    assert all("route_rule_id" not in str(row["payload_json"]) for row in mailbox_rows)
