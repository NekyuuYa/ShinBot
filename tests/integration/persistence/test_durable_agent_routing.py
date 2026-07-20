"""Integration tests for recoverable core-to-Agent routing transactions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.core.dispatch.actor_v2_admission import (
    ActorV2AdmissionFenceNotFound,
    ActorV2AdmissionFenceStatus,
)
from shinbot.core.dispatch.agent_delivery import AgentRouteDelivery
from shinbot.core.dispatch.agent_identity import SessionKey, SessionKeyFactory
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipEvidenceConflict,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipNotFound,
)
from shinbot.core.dispatch.durable_routing import (
    AGENT_ROUTE_MAILBOX_KIND,
    AGENT_ROUTE_MAILBOX_SOURCE,
    MessageRoutingJobEnvelope,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.mailbox_handoff import (
    MailboxHandoffEvidenceState,
    MailboxHandoffState,
)
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


def _fenced_routing_claim(
    tmp_path: Path,
) -> tuple[
    DatabaseManager,
    DurableMessageRoutingRepository,
    ClaimedMessageRoutingJob,
    AgentRouteDelivery,
]:
    """Build one claimed routing job with committed admission evidence."""

    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    clock = _Clock()
    store = DurableMessageRoutingRepository(db, lease_seconds=5.0, clock=clock)
    delivery_key = _delivery(1).session_key
    grant = db.actor_v2_admission_fences.reserve(
        delivery_key,
        holder_id="durable-routing-final-gate",
        ttl_seconds=60.0,
    )
    ownership = db.agent_runtime_ownership.claim(
        delivery_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="durable routing final-gate test activation",
        legacy_session_id="instance-main:group:room",
        admission_grant=grant,
    ).ownership
    envelope = replace(
        _job(),
        profile_id=delivery_key.profile_id,
        session_id=delivery_key.session_id,
        ownership_generation=ownership.generation,
        admission_fence_id=grant.fence.fence_id,
        admission_fence_generation=grant.fence.generation,
    )
    persisted = store.persist_message_and_job(_record(), envelope)
    claim = store.claim_next_job(worker_id="fenced-router")
    assert claim is not None
    return db, store, claim, _delivery(persisted.message_log_id)


def test_fenced_job_claim_scope_cannot_select_sibling_actor_request(tmp_path: Path) -> None:
    """An exact relay claim retains both ownership and admission-fence scope."""

    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    store = DurableMessageRoutingRepository(db, lease_seconds=5.0, clock=_Clock())
    first_key = SessionKey("profile-first", "profile-first:group:room")
    second_key = SessionKey("profile-second", "profile-second:group:room")

    def persist_fenced_job(
        key: SessionKey,
        *,
        suffix: str,
    ) -> FencedMailboxWakeRequest:
        grant = db.actor_v2_admission_fences.reserve(
            key,
            holder_id=f"fenced-scope-{suffix}",
            ttl_seconds=60.0,
        )
        ownership = db.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason=f"fenced scope claim test {suffix}",
            legacy_session_id=f"instance-main:group:{suffix}",
            admission_grant=grant,
        ).ownership
        envelope = MessageRoutingJobEnvelope(
            job_id=f"fenced-scope-job-{suffix}",
            idempotency_key=f"fenced-scope-idempotency-{suffix}",
            trace_id=f"fenced-scope-trace-{suffix}",
            correlation_id=f"fenced-scope-correlation-{suffix}",
            causation_id=f"fenced-scope-causation-{suffix}",
            payload={"kind": "fenced-scope", "suffix": suffix},
            profile_id=key.profile_id,
            session_id=key.session_id,
            ownership_generation=ownership.generation,
            admission_fence_id=ownership.admission_fence_id,
            admission_fence_generation=ownership.admission_fence_generation,
            occurred_at=1_000.0,
            available_at=1_000.0,
        )
        store.persist_message_and_job(
            replace(
                _record(raw_text=f"scope-{suffix}"),
                session_id=f"instance-main:group:{suffix}",
                platform_msg_id=f"scope-message-{suffix}",
            ),
            envelope,
        )
        return FencedMailboxWakeRequest(
            key=key,
            ownership_generation=ownership.generation,
            admission_fence_id=ownership.admission_fence_id,
            admission_fence_generation=ownership.admission_fence_generation,
        )

    first_request = persist_fenced_job(first_key, suffix="first")
    second_request = persist_fenced_job(second_key, suffix="second")

    assert store.is_live_fenced_request(first_request) is True
    assert store.is_live_fenced_request(second_request) is True

    first_claim = store.claim_next_job(
        worker_id="fenced-scope-first",
        expected_fenced_request=first_request,
    )

    assert first_claim is not None
    assert first_claim.envelope.profile_id == first_key.profile_id
    assert first_claim.envelope.session_id == first_key.session_id
    assert store.next_job_available_at(expected_fenced_request=first_request) == 1_005.0

    sibling_claim = store.claim_next_job(
        worker_id="fenced-scope-second",
        expected_fenced_request=second_request,
    )

    assert sibling_claim is not None
    assert sibling_claim.envelope.profile_id == second_key.profile_id
    assert sibling_claim.envelope.session_id == second_key.session_id


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


def _insert_pending_route_mailbox(
    db: DatabaseManager,
    key: SessionKey,
    event_id: str,
) -> None:
    """Insert one pending route mailbox event for keyset pagination tests."""

    ownership = db.agent_runtime_ownership.get(key)
    if ownership is None:
        ownership = db.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="durable route wake keyset test owner",
            legacy_session_id=key.session_id,
        ).ownership
    with db.connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, state,
                state_revision, event_sequence, activity_generation,
                active_epoch, review_plan_json, current_plan_id,
                review_plan_revision, active_reply_resume_json,
                active_chat_state_json, review_operation_id,
                active_reply_operation_id, active_chat_round_operation_id,
                idle_planning_operation_id, data_json, created_at, updated_at
            ) VALUES (?, ?, ?, 'idle', 0, 0, 0, 0, '{}', '', 0, '{}', '{}',
                      '', '', '', '', '{}', 1000, 1000)
            """,
            (key.profile_id, key.session_id, ownership.generation),
        )
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json, causation_id,
                correlation_id, trace_id, status, attempt_count,
                available_at, claim_id, lease_owner, lease_until,
                created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, 1000, '{}', '', '', '', 'pending',
                      0, 1000, '', '', NULL, 1000, NULL, '')
            """,
            (
                event_id,
                key.profile_id,
                key.session_id,
                ownership.generation,
                AGENT_ROUTE_MAILBOX_KIND,
                AGENT_ROUTE_MAILBOX_SOURCE,
            ),
        )


def _insert_historical_route_mailbox(
    db: DatabaseManager,
    delivery: AgentRouteDelivery,
    *,
    has_unknown_handoff: bool,
) -> int:
    """Insert a pre-dual-write mailbox matching one claimed route delivery."""

    with db.connect() as conn:
        outbox = conn.execute(
            """
            SELECT routing_job_id, ownership_generation
            FROM agent_route_outbox
            WHERE delivery_id = ?
            """,
            (delivery.delivery_id,),
        ).fetchone()
        assert outbox is not None
        ownership_generation = int(outbox["ownership_generation"])
        conn.execute(
            """
            INSERT OR IGNORE INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 1000.0, 1000.0)
            """,
            (
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
                ownership_generation,
            ),
        )
        payload_json = json.dumps(
            delivery.to_mailbox_payload(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json, causation_id,
                correlation_id, trace_id,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 1000.0,
                      '', '', NULL, 1000.0, NULL, '')
            """,
            (
                delivery.event_id,
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
                ownership_generation,
                AGENT_ROUTE_MAILBOX_KIND,
                AGENT_ROUTE_MAILBOX_SOURCE,
                delivery.observed_at,
                payload_json,
                str(outbox["routing_job_id"]),
                str(outbox["routing_job_id"]),
                delivery.trace_id,
            ),
        )
        mailbox_id = int(inserted.lastrowid)
        if has_unknown_handoff:
            conn.execute(
                """
                INSERT INTO agent_session_mailbox_handoffs (
                    mailbox_id, handoff_id,
                    profile_id, session_id, event_id, ownership_generation,
                    evidence_state, admission_fence_id, admission_fence_generation,
                    state, attempt_count, available_at,
                    claim_id, lease_owner, lease_until,
                    target_id, target_incarnation_id, target_disposition,
                    created_at, updated_at, claimed_at, settled_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, 'unknown', '', 0,
                          'blocked', 0, 1000.0, '', '', NULL, '', '', '',
                          1000.0, 1000.0, NULL, NULL, '')
                """,
                (
                    mailbox_id,
                    f"historical-unknown-{mailbox_id}",
                    delivery.session_key.profile_id,
                    delivery.session_key.session_id,
                    delivery.event_id,
                    ownership_generation,
                ),
            )
    return mailbox_id


def _install_fence_delete_trigger(
    db: DatabaseManager,
    *,
    name: str,
    table: str,
    timing_sql: str,
    match_new_fence: bool,
) -> None:
    """Delete the current fence after a candidate write for final-gate tests."""

    fence_match = (
        """
              AND fence_id = NEW.admission_fence_id
              AND generation = NEW.admission_fence_generation
        """
        if match_new_fence
        else ""
    )
    with db.connect() as conn:
        conn.execute(
            f"""
            CREATE TRIGGER {name}
            {timing_sql} ON {table}
            BEGIN
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = NEW.profile_id
                  AND session_id = NEW.session_id
                  {fence_match};
            END
            """
        )


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


def test_decision_final_fence_gate_rolls_back_all_candidate_writes(
    tmp_path: Path,
) -> None:
    """A fence lost after outbox insertion cannot commit a route decision."""

    db, store, claim, delivery = _fenced_routing_claim(tmp_path)
    _install_fence_delete_trigger(
        db,
        name="delete_fence_after_outbox_insert",
        table="agent_route_outbox",
        timing_sql="AFTER INSERT",
        match_new_fence=True,
    )

    try:
        with pytest.raises(ActorV2AdmissionFenceNotFound, match="does not exist"):
            store.complete_with_agent_deliveries(claim, [delivery])
    finally:
        _drop_trigger(db, "delete_fence_after_outbox_insert")

    with db.connect() as conn:
        outbox_count = conn.execute("SELECT COUNT(*) FROM agent_route_outbox").fetchone()[0]
        job = conn.execute("SELECT status FROM message_routing_jobs").fetchone()
        message = conn.execute("SELECT routing_status FROM message_logs").fetchone()
    fence = db.actor_v2_admission_fences.get(delivery.session_key)

    assert outbox_count == 0
    assert job["status"] == "processing"
    assert message["routing_status"] == MessageRoutingStatus.PENDING.value
    assert fence is not None
    assert fence.status is ActorV2AdmissionFenceStatus.COMMITTED


def test_decision_final_gate_rejects_rewritten_outbox_identity(
    tmp_path: Path,
) -> None:
    """The final gate must retain the identity captured before outbox writes."""

    db, store, claim, delivery = _fenced_routing_claim(tmp_path)
    alternate_key = SessionKey("alternate-profile", "alternate-profile:group:room")
    alternate_owner = db.agent_runtime_ownership.claim(
        alternate_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="alternate durable routing final-gate owner",
        legacy_session_id="alternate-instance:group:room",
    ).ownership
    with db.connect() as conn:
        conn.execute(
            f"""
            CREATE TRIGGER rewrite_outbox_identity_after_decision_completion
            AFTER UPDATE OF status ON message_routing_jobs
            WHEN NEW.status = 'completed'
            BEGIN
                UPDATE agent_route_outbox
                SET profile_id = '{alternate_key.profile_id}',
                    session_id = '{alternate_key.session_id}',
                    ownership_generation = {alternate_owner.generation},
                    admission_fence_id = '',
                    admission_fence_generation = 0
                WHERE routing_job_id = NEW.routing_job_id;
            END
            """
        )

    try:
        with pytest.raises(DurableRoutingConflict, match="identity changed"):
            store.complete_with_agent_deliveries(claim, [delivery])
    finally:
        _drop_trigger(db, "rewrite_outbox_identity_after_decision_completion")

    with db.connect() as conn:
        outbox_count = conn.execute("SELECT COUNT(*) FROM agent_route_outbox").fetchone()[0]
        job = conn.execute("SELECT status FROM message_routing_jobs").fetchone()
        message = conn.execute("SELECT routing_status FROM message_logs").fetchone()

    assert outbox_count == 0
    assert job["status"] == "processing"
    assert message["routing_status"] == MessageRoutingStatus.PENDING.value


def test_decision_final_gate_rejects_rewritten_job_identity(
    tmp_path: Path,
) -> None:
    """A route decision cannot commit after its job identity is rewritten."""

    db, store, claim, delivery = _fenced_routing_claim(tmp_path)
    alternate_key = SessionKey("alternate-profile", "alternate-profile:group:room")
    alternate_owner = db.agent_runtime_ownership.claim(
        alternate_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="alternate durable routing job final-gate owner",
        legacy_session_id="alternate-instance:group:room",
    ).ownership
    with db.connect() as conn:
        conn.execute(
            f"""
            CREATE TRIGGER rewrite_job_identity_after_decision_completion
            AFTER UPDATE OF status ON message_routing_jobs
            WHEN NEW.status = 'completed'
            BEGIN
                UPDATE message_routing_jobs
                SET profile_id = '{alternate_key.profile_id}',
                    session_id = '{alternate_key.session_id}',
                    ownership_generation = {alternate_owner.generation},
                    admission_fence_id = '',
                    admission_fence_generation = 0
                WHERE routing_job_id = NEW.routing_job_id;
            END
            """
        )

    try:
        with pytest.raises(DurableRoutingConflict, match="identity changed"):
            store.complete_with_agent_deliveries(claim, [delivery])
    finally:
        _drop_trigger(db, "rewrite_job_identity_after_decision_completion")

    with db.connect() as conn:
        outbox_count = conn.execute("SELECT COUNT(*) FROM agent_route_outbox").fetchone()[0]
        job = conn.execute("SELECT status FROM message_routing_jobs").fetchone()
        message = conn.execute("SELECT routing_status FROM message_logs").fetchone()
    fence = db.actor_v2_admission_fences.get(delivery.session_key)

    assert outbox_count == 0
    assert job["status"] == "processing"
    assert message["routing_status"] == MessageRoutingStatus.PENDING.value
    assert fence is not None
    assert fence.status is ActorV2AdmissionFenceStatus.COMMITTED


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
    assert relayed.wake_request.key == delivery_claim.delivery.session_key
    assert relayed.wake_request.ownership_generation == 1
    assert not relayed.wake_request.has_admission_fence
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


def test_relay_dual_writes_fenced_mailbox_handoff_for_new_mailbox(
    tmp_path: Path,
) -> None:
    """A newly relayed fenced mailbox retains the exact outbox fence evidence."""

    db, store, job_claim, delivery = _fenced_routing_claim(tmp_path)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="fenced-relay")
    assert delivery_claim is not None

    relayed = store.relay_delivery(delivery_claim)

    with db.connect() as conn:
        mailbox_id = int(
            conn.execute(
                "SELECT mailbox_id FROM agent_session_mailbox WHERE event_id = ?",
                (delivery.event_id,),
            ).fetchone()[0]
        )
    handoff = db.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert handoff is not None
    assert handoff.evidence.state is MailboxHandoffEvidenceState.FENCED
    assert handoff.state is MailboxHandoffState.PENDING
    assert handoff.evidence.as_fenced_wake_request() == relayed.wake_request


def test_relay_dual_writes_explicit_legacy_handoff_without_fence(routing_store) -> None:
    """A newly relayed unfenced mailbox is durably blocked as legacy work."""

    db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    delivery = _delivery(message_log_id)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="legacy-relay")
    assert delivery_claim is not None

    relayed = store.relay_delivery(delivery_claim)

    with db.connect() as conn:
        mailbox_id = int(
            conn.execute(
                "SELECT mailbox_id FROM agent_session_mailbox WHERE event_id = ?",
                (delivery.event_id,),
            ).fetchone()[0]
        )
    handoff = db.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert handoff is not None
    assert handoff.evidence.state is MailboxHandoffEvidenceState.UNFENCED_LEGACY
    assert handoff.state is MailboxHandoffState.BLOCKED
    assert handoff.evidence.admission_fence_id == ""
    assert handoff.evidence.admission_fence_generation == 0
    assert not relayed.wake_request.has_admission_fence


@pytest.mark.parametrize(
    "has_unknown_handoff",
    [False, True],
    ids=["missing-sidecar", "unknown-sidecar"],
)
def test_relay_duplicate_historic_mailbox_does_not_upgrade_handoff_evidence(
    routing_store,
    has_unknown_handoff: bool,
) -> None:
    """Replaying a pre-dual-write mailbox never infers or upgrades its evidence."""

    db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    delivery = _delivery(message_log_id)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="historic-relay")
    assert delivery_claim is not None
    mailbox_id = _insert_historical_route_mailbox(
        db,
        delivery,
        has_unknown_handoff=has_unknown_handoff,
    )

    relayed = store.relay_delivery(delivery_claim)
    completed_replay = store.relay_delivery(delivery_claim)

    assert relayed.duplicate is True
    assert completed_replay.duplicate is True
    assert completed_replay.wake_request == relayed.wake_request
    handoff = db.actor_v2_mailbox_handoffs.read(mailbox_id)
    if has_unknown_handoff:
        assert handoff is not None
        assert handoff.evidence.state is MailboxHandoffEvidenceState.UNKNOWN
        assert handoff.state is MailboxHandoffState.BLOCKED
    else:
        assert handoff is None


def test_relay_final_fence_gate_rolls_back_all_candidate_writes(
    tmp_path: Path,
) -> None:
    """A fence lost after mailbox insertion cannot complete its outbox row."""

    db, store, job_claim, delivery = _fenced_routing_claim(tmp_path)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="fenced-relay")
    assert delivery_claim is not None
    _install_fence_delete_trigger(
        db,
        name="delete_fence_after_mailbox_insert",
        table="agent_session_mailbox",
        timing_sql="AFTER INSERT",
        match_new_fence=False,
    )

    try:
        with pytest.raises(ActorV2AdmissionFenceNotFound, match="does not exist"):
            store.relay_delivery(delivery_claim)
    finally:
        _drop_trigger(db, "delete_fence_after_mailbox_insert")

    with db.connect() as conn:
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
        aggregate_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_aggregates"
        ).fetchone()[0]
        outbox = conn.execute("SELECT status FROM agent_route_outbox").fetchone()
    fence = db.actor_v2_admission_fences.get(delivery.session_key)

    assert mailbox_count == 0
    assert aggregate_count == 0
    assert outbox["status"] == "processing"
    assert fence is not None
    assert fence.status is ActorV2AdmissionFenceStatus.COMMITTED


def test_relay_final_gate_rolls_back_mailbox_handoff_and_outbox_completion(
    tmp_path: Path,
) -> None:
    """A fence revoked after sidecar/outbox writes rolls back the full candidate."""

    db, store, job_claim, delivery = _fenced_routing_claim(tmp_path)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="fenced-relay-final-gate")
    assert delivery_claim is not None
    _install_fence_delete_trigger(
        db,
        name="delete_fence_after_route_outbox_completion",
        table="agent_route_outbox",
        timing_sql="AFTER UPDATE OF status",
        match_new_fence=True,
    )

    try:
        with pytest.raises(ActorV2AdmissionFenceNotFound, match="does not exist"):
            store.relay_delivery(delivery_claim)
    finally:
        _drop_trigger(db, "delete_fence_after_route_outbox_completion")

    with db.connect() as conn:
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
        handoff_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox_handoffs"
        ).fetchone()[0]
        outbox = conn.execute("SELECT status FROM agent_route_outbox").fetchone()
    fence = db.actor_v2_admission_fences.get(delivery.session_key)

    assert mailbox_count == 0
    assert handoff_count == 0
    assert outbox["status"] == "processing"
    assert fence is not None
    assert fence.status is ActorV2AdmissionFenceStatus.COMMITTED


def test_relay_final_gate_rejects_rewritten_outbox_identity(
    tmp_path: Path,
) -> None:
    """A relay cannot commit after a trigger rewrites its captured identity."""

    db, store, job_claim, delivery = _fenced_routing_claim(tmp_path)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="fenced-relay-identity")
    assert delivery_claim is not None
    alternate_key = SessionKey("alternate-profile", "alternate-profile:group:room")
    alternate_owner = db.agent_runtime_ownership.claim(
        alternate_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="alternate durable relay final-gate owner",
        legacy_session_id="alternate-instance:group:room",
    ).ownership
    with db.connect() as conn:
        conn.execute(
            f"""
            CREATE TRIGGER rewrite_outbox_identity_after_relay_completion
            AFTER UPDATE OF status ON agent_route_outbox
            WHEN NEW.status = 'completed'
            BEGIN
                UPDATE agent_route_outbox
                SET profile_id = '{alternate_key.profile_id}',
                    session_id = '{alternate_key.session_id}',
                    ownership_generation = {alternate_owner.generation},
                    admission_fence_id = '',
                    admission_fence_generation = 0
                WHERE delivery_id = NEW.delivery_id;
            END
            """
        )

    try:
        with pytest.raises(DurableRoutingConflict, match="identity changed"):
            store.relay_delivery(delivery_claim)
    finally:
        _drop_trigger(db, "rewrite_outbox_identity_after_relay_completion")

    with db.connect() as conn:
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
        handoff_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox_handoffs"
        ).fetchone()[0]
        outbox = conn.execute("SELECT status FROM agent_route_outbox").fetchone()
    fence = db.actor_v2_admission_fences.get(delivery.session_key)

    assert mailbox_count == 0
    assert handoff_count == 0
    assert outbox["status"] == "processing"
    assert fence is not None
    assert fence.status is ActorV2AdmissionFenceStatus.COMMITTED


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
    assert replay.wake_request == first.wake_request
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


def test_pending_route_wake_requests_retain_current_ownership_identity(
    routing_store,
) -> None:
    """Restart recovery returns the same full identity emitted by relay."""

    _db, store, _clock = routing_store
    message_log_id, job_claim = _persist_and_claim(store)
    delivery = _delivery(message_log_id)
    store.complete_with_agent_deliveries(job_claim, [delivery])
    delivery_claim = store.claim_next_delivery(worker_id="relay-a")
    assert delivery_claim is not None
    relayed = store.relay_delivery(delivery_claim)

    requests = store.pending_route_wake_requests()

    assert requests == (relayed.wake_request,)


def test_pending_route_wake_debts_use_keyset_cursor_under_churn(routing_store) -> None:
    """Keyset pages survive consumption and appends without skipping debt."""

    db, store, _clock = routing_store
    first_key = SessionKey("profile-a", "route-keyset:first")
    shared_key = SessionKey("profile-a", "route-keyset:shared")
    last_key = SessionKey("profile-a", "route-keyset:last")
    appended_key = SessionKey("profile-a", "route-keyset:appended")

    _insert_pending_route_mailbox(db, first_key, "route-keyset:first")
    _insert_pending_route_mailbox(db, shared_key, "route-keyset:shared-old")
    _insert_pending_route_mailbox(db, shared_key, "route-keyset:shared-new")
    _insert_pending_route_mailbox(db, last_key, "route-keyset:last")

    first_page = store.pending_route_wake_debts(limit=2)
    assert tuple(debt.event_id for debt in first_page) == (
        "route-keyset:first",
        "route-keyset:shared-new",
    )
    assert first_page[-1].cursor is not None

    with db.connect() as conn:
        conn.execute(
            "DELETE FROM agent_session_mailbox WHERE event_id = ?",
            ("route-keyset:first",),
        )
    _insert_pending_route_mailbox(db, appended_key, "route-keyset:appended")

    second_page = store.pending_route_wake_debts(
        limit=2,
        after=first_page[-1].cursor,
    )
    assert tuple(debt.event_id for debt in second_page) == (
        "route-keyset:last",
        "route-keyset:appended",
    )
    with pytest.raises(ValueError, match="offset cannot be combined"):
        store.pending_route_wake_debts(
            limit=1,
            offset=1,
            after=first_page[-1].cursor,
        )

    old_first = first_page[0]
    _insert_pending_route_mailbox(db, first_key, "route-keyset:first")
    assert not store.is_pending_route_wake_debt(old_first)


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
    intermediate_ddl = current_ddl
    for column in (
        "        profile_id TEXT NOT NULL DEFAULT '',\n",
        "        session_id TEXT NOT NULL DEFAULT '',\n",
        "        ownership_generation INTEGER NOT NULL DEFAULT 0,\n",
        "        admission_fence_id TEXT NOT NULL DEFAULT '',\n",
        "        admission_fence_generation INTEGER NOT NULL DEFAULT 0,\n",
    ):
        intermediate_ddl = intermediate_ddl.replace(column, "")
    scope_check_start = intermediate_ddl.index(
        "        CHECK(ownership_generation >= 0),"
    )
    scope_check_end = intermediate_ddl.index(
        "        CHECK(status IN",
        scope_check_start,
    )
    intermediate_ddl = (
        intermediate_ddl[:scope_check_start] + intermediate_ddl[scope_check_end:]
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
                   admission_fence_id, admission_fence_generation,
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
        "",
        0,
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
