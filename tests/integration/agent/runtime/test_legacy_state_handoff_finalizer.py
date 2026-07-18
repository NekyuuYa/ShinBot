"""Integration coverage for atomic idle legacy-state handoff finalization."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.legacy_state_handoff import (
    ActorV2LegacyIdleStateTargetPreparer,
)
from shinbot.agent.runtime.session_actor.legacy_state_handoff_finalizer import (
    ActorV2LegacyIdleStateFinalizationBlocked,
    SQLiteActorV2LegacyIdleStateFinalizer,
)
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainReceipt,
)
from shinbot.core.dispatch.actor_v2_legacy_state_handoff import (
    ActorV2LegacyStateHandoffManifest,
    ActorV2LegacyStateHandoffMaterialization,
)
from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierGrant,
    ActorV2MigrationBarrierLost,
    ActorV2MigrationBarrierStatus,
)
from shinbot.core.dispatch.agent_delivery import AgentRouteDelivery
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainRepository,
)
from shinbot.persistence.repositories.actor_v2_ingress_drain import (
    ActorV2IngressDrainRepository,
)
from shinbot.persistence.repositories.actor_v2_legacy_state_handoff import (
    ActorV2LegacyStateHandoffRepository,
)
from shinbot.persistence.repositories.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierRepository,
)
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)
from shinbot.persistence.repositories.durable_routing import (
    DurableMessageRoutingRepository,
)

_LEGACY_SESSION_ID = "legacy-session-a"
_PROFILE_ID = "profile-a"
_BOT_SESSION_ID = "profile-a:group:room"


@dataclass(frozen=True)
class _PreparedHandoff:
    """One frozen supported source and its immutable preparation record."""

    database: DatabaseManager
    now: list[float]
    key: SessionKey
    grant: ActorV2MigrationBarrierGrant
    manifest: ActorV2LegacyStateHandoffManifest
    materialization: ActorV2LegacyStateHandoffMaterialization
    message_log_id: int
    delivery: AgentRouteDelivery


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for finalizer integration tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _canonical_json(value: object) -> str:
    """Serialize data with the durable routing and handoff canonical form."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _digest(value: str) -> str:
    """Build a deterministic SHA-256 digest for immutable durable evidence."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _repositories(
    database: DatabaseManager,
    now: list[float],
) -> tuple[
    AgentRuntimeOwnershipRepository,
    ActorV2MigrationBarrierRepository,
    ActorV2IngressDrainRepository,
    ActorV2CoreIngressDrainRepository,
    ActorV2LegacyStateHandoffRepository,
]:
    """Install deterministic barrier, drain, and source-state repositories."""

    ownership = AgentRuntimeOwnershipRepository(database, clock=lambda: now[0])
    barrier = ActorV2MigrationBarrierRepository(
        database,
        clock=lambda: now[0],
        barrier_id_factory=lambda: "migration-barrier-a",
        holder_token_factory=lambda: "migration-holder-token-secret",
    )
    ingress = ActorV2IngressDrainRepository(
        database,
        clock=lambda: now[0],
        member_id_factory=lambda: "member-a",
        request_id_factory=lambda: "adapter-drain-request-unused",
        holder_token_factory=lambda: "participant-token-secret",
    )
    core = ActorV2CoreIngressDrainRepository(
        database,
        clock=lambda: now[0],
        request_id_factory=lambda: "core-drain-request-a",
    )
    handoff = ActorV2LegacyStateHandoffRepository(
        database,
        clock=lambda: now[0],
        manifest_id_factory=lambda: "legacy-manifest-a",
    )
    database.agent_runtime_ownership = ownership
    database.actor_v2_migration_barriers = barrier
    database.actor_v2_ingress_drains = ingress
    database.actor_v2_core_ingress_drains = core
    database.actor_v2_legacy_state_handoffs = handoff
    return ownership, barrier, ingress, core, handoff


def _start_boundary(
    ownership: AgentRuntimeOwnershipRepository,
    barrier: ActorV2MigrationBarrierRepository,
    ingress: ActorV2IngressDrainRepository,
    core: ActorV2CoreIngressDrainRepository,
) -> tuple[SessionKey, ActorV2MigrationBarrierGrant, object]:
    """Create one legacy owner and its holder-fenced core drain boundary."""

    key = SessionKey(_PROFILE_ID, _BOT_SESSION_ID)
    source = ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy source for idle-state finalization",
        legacy_session_id=_LEGACY_SESSION_ID,
        requested_by="test",
    ).ownership
    participant = ingress.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )
    grant = barrier.start_legacy_to_actor_v2(
        key,
        expected_generation=source.generation,
        adapter_instance_ids=("adapter-a",),
        holder_id="cutover-controller-a",
        reason="begin idle-state finalization boundary",
    )
    request = core.begin_drain(grant)
    return key, grant, (request, participant)


def _confirm_core_drain(
    core: ActorV2CoreIngressDrainRepository,
    grant: ActorV2MigrationBarrierGrant,
    boundary: object,
) -> None:
    """Persist the exact local drain receipts required for source capture."""

    request, participant = boundary
    core.acknowledge_quiescent(
        request_id=request.request_id,
        participant_grant=participant,
        receipt=ActorV2CoreIngressDrainReceipt(
            core_ingress_digest=_digest("core-ingress-a"),
            legacy_quiescence_digest=_digest("legacy-quiescence-a"),
            proof_epoch=1,
            summary_code="process.local_quiescent",
        ),
    )
    core.confirm_drained(request_id=request.request_id, barrier_grant=grant)


def _delivery(key: SessionKey, message_log_id: int) -> AgentRouteDelivery:
    """Build the canonical route delivery retained by a frozen legacy source."""

    return AgentRouteDelivery(
        session_key=key,
        bot_id=_PROFILE_ID,
        bot_binding_id="binding-a",
        base_session_id=_LEGACY_SESSION_ID,
        bot_session_id=_BOT_SESSION_ID,
        message_log_id=message_log_id,
        sender_id="user-a",
        instance_id="adapter-a",
        platform="mock",
        self_id="bot-a",
        is_private=False,
        is_mentioned=True,
        is_mention_to_other=False,
        is_reply_to_bot=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        already_handled=False,
        is_stopped=False,
        trace_id="trace-a",
        observed_at=10.0,
        route_rule_id="builtin.agent_entry_fallback",
    )


def _insert_frozen_legacy_source(
    database: DatabaseManager,
    *,
    key: SessionKey,
    migration_generation: int,
    next_review_at: float,
    include_recent_mention: bool = False,
) -> tuple[int, AgentRouteDelivery]:
    """Write a supported frozen source and its retained durable route evidence.

    The routing rows model work that was already durably buffered at the barrier
    generation.  They are inserted directly because normal route admission only
    creates Actor-owned deliveries and this test targets a legacy source.
    """

    with database.connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO message_logs (
                session_id, sender_id, content_json, raw_text, role, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (_LEGACY_SESSION_ID, "user-a", "[]", "hello", "user", 10.0),
        )
        message_log_id = int(cursor.lastrowid)
        delivery = _delivery(key, message_log_id)
        delivery_payload_json = _canonical_json(delivery.to_payload())
        routing_payload_json = _canonical_json(
            {"event_type": "message-created", "instance_id": "adapter-a"}
        )
        routing_job_id = f"legacy-handoff-routing-job:{message_log_id}"
        conn.execute(
            """
            INSERT INTO message_routing_jobs (
                routing_job_id, idempotency_key, message_log_id, version,
                profile_id, session_id, ownership_generation,
                message_fingerprint, payload_json, payload_digest, trace_id,
                correlation_id, occurred_at, status, available_at, created_at,
                updated_at, completed_at
            ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?)
            """,
            (
                routing_job_id,
                f"legacy-handoff-routing-idempotency:{message_log_id}",
                message_log_id,
                key.profile_id,
                key.session_id,
                migration_generation,
                f"legacy-handoff-fingerprint:{message_log_id}",
                routing_payload_json,
                _digest(routing_payload_json),
                delivery.trace_id,
                f"legacy-handoff-correlation:{message_log_id}",
                delivery.observed_at,
                delivery.observed_at,
                delivery.observed_at,
                delivery.observed_at,
                delivery.observed_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_route_outbox (
                delivery_id, idempotency_key, routing_job_id, profile_id,
                session_id, message_log_id, route_rule_id, version,
                ownership_generation, event_id, payload_json, payload_digest,
                trace_id, correlation_id, causation_id, status, available_at,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
            """,
            (
                delivery.delivery_id,
                delivery.idempotency_key,
                routing_job_id,
                key.profile_id,
                key.session_id,
                message_log_id,
                delivery.route_rule_id,
                delivery.version,
                migration_generation,
                delivery.event_id,
                delivery_payload_json,
                _digest(delivery_payload_json),
                delivery.trace_id,
                f"legacy-handoff-correlation:{message_log_id}",
                routing_job_id,
                delivery.observed_at,
                delivery.observed_at,
                delivery.observed_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (
                session_id, state, next_review_at, review_reason,
                mention_sensitivity, active_reply_threshold_json,
                active_chat_state_json, state_resume_json, updated_at
            ) VALUES (?, 'idle', ?, 'deferred_review', 'high', ?, '{}', '{}', ?)
            """,
            (
                _LEGACY_SESSION_ID,
                next_review_at,
                _canonical_json({"at_count": 2, "window_seconds": 45.0}),
                100.0,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_messages (
                session_id, message_log_id, sender_id, created_at,
                response_profile, is_mentioned, is_reply_to_bot,
                is_mention_to_other, is_poke_to_bot, is_poke_to_other,
                self_platform_id, trace_id, review_consumed, chat_consumed
            ) VALUES (?, ?, ?, ?, ?, 1, 0, 0, 0, 0, ?, ?, 1, 0)
            """,
            (
                _LEGACY_SESSION_ID,
                message_log_id,
                delivery.sender_id,
                delivery.observed_at,
                "normal",
                delivery.self_id,
                delivery.trace_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_ranges (
                session_id, start_msg_log_id, end_msg_log_id, start_at,
                end_at, message_count, review_consumed, chat_consumed
            ) VALUES (?, ?, ?, ?, ?, 1, 1, 0)
            """,
            (
                _LEGACY_SESSION_ID,
                message_log_id,
                message_log_id,
                delivery.observed_at,
                delivery.observed_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_high_priority_events (
                session_id, message_log_id, sender_id, kind, reason, created_at, handled
            ) VALUES (?, ?, ?, 'mention', 'message_mentions_self', ?, 1)
            """,
            (
                _LEGACY_SESSION_ID,
                message_log_id,
                delivery.sender_id,
                delivery.observed_at,
            ),
        )
        if include_recent_mention:
            conn.execute(
                """
                INSERT INTO agent_recent_mentions (session_id, timestamp)
                VALUES (?, ?)
                """,
                (_LEGACY_SESSION_ID, delivery.observed_at),
            )
    return message_log_id, delivery


def _prepared_handoff(
    tmp_path: Path,
    *,
    next_review_at: float = 120.0,
    include_recent_mention: bool = False,
) -> _PreparedHandoff:
    """Capture and prepare a source under a confirmed deterministic boundary."""

    now = [100.0]
    database = _database(tmp_path)
    ownership, barrier, ingress, core, handoff = _repositories(database, now)
    key, grant, boundary = _start_boundary(ownership, barrier, ingress, core)
    message_log_id, delivery = _insert_frozen_legacy_source(
        database,
        key=key,
        migration_generation=grant.barrier.migration_generation,
        next_review_at=next_review_at,
        include_recent_mention=include_recent_mention,
    )
    now[0] = 101.0
    _confirm_core_drain(core, grant, boundary)
    now[0] = 102.0
    manifest = handoff.capture(grant)
    now[0] = 103.0
    materialization = handoff.materialize(
        barrier_grant=grant,
        manifest_id=manifest.manifest_id,
        materializer=ActorV2LegacyIdleStateTargetPreparer(),
    )
    return _PreparedHandoff(
        database=database,
        now=now,
        key=key,
        grant=grant,
        manifest=manifest,
        materialization=materialization,
        message_log_id=message_log_id,
        delivery=delivery,
    )


def test_finalizer_commits_supported_idle_state_atomically(tmp_path: Path) -> None:
    """One finalization commits ledger, review timing, provenance, and ownership."""

    prepared = _prepared_handoff(tmp_path)
    prepared.now[0] = 110.0
    finalizer = SQLiteActorV2LegacyIdleStateFinalizer(
        prepared.database,
        clock=lambda: prepared.now[0],
    )

    result = finalizer.finalize(
        barrier_grant=prepared.grant,
        manifest_id=prepared.manifest.manifest_id,
        reason="preserve a supported idle source as Actor v2 state",
        requested_by="cutover-controller-a",
    )

    assert result.manifest_id == prepared.manifest.manifest_id
    assert result.target_digest == prepared.materialization.target_digest
    assert result.ledger_entry_count == 1
    assert result.review_plan_id == (
        "legacy-state-handoff:legacy-manifest-a:review-plan"
    )
    assert result.ownership.mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert result.ownership.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert result.ownership.generation == prepared.grant.barrier.migration_generation + 1
    assert result.barrier.status is ActorV2MigrationBarrierStatus.COMPLETED
    assert result.barrier.completed_at == 110.0
    assert result.barrier.completion_manifest_id == prepared.manifest.manifest_id

    with prepared.database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT ownership_generation, state, current_plan_id, review_plan_json, data_json
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (prepared.key.profile_id, prepared.key.session_id),
        ).fetchone()
        assert aggregate is not None
        assert int(aggregate["ownership_generation"]) == result.ownership.generation
        assert str(aggregate["state"]) == "idle"
        assert str(aggregate["current_plan_id"]) == result.review_plan_id
        review_plan = json.loads(str(aggregate["review_plan_json"]))
        assert review_plan["applied_delay_seconds"] == 10.0
        assert review_plan["next_review_at"] == 120.0
        aggregate_data = json.loads(str(aggregate["data_json"]))
        assert aggregate_data["legacy_state_handoff"] == {
            "manifest_id": prepared.manifest.manifest_id,
            "materializer_id": prepared.materialization.materializer_id,
            "materializer_version": prepared.materialization.materializer_version,
            "source_digest": prepared.manifest.source_digest,
            "target_digest": prepared.materialization.target_digest,
            "target_schema_version": prepared.materialization.target_schema_version,
        }

        ledger = conn.execute(
            """
            SELECT ownership_generation, message_log_id, source_event_id,
                   review_consumption_id, high_priority_consumption_id
            FROM agent_message_ledger
            WHERE profile_id = ? AND session_id = ?
            """,
            (prepared.key.profile_id, prepared.key.session_id),
        ).fetchone()
        assert ledger is not None
        assert int(ledger["ownership_generation"]) == result.ownership.generation
        assert int(ledger["message_log_id"]) == prepared.message_log_id
        assert str(ledger["source_event_id"]) == prepared.delivery.event_id
        assert str(ledger["review_consumption_id"])
        assert str(ledger["high_priority_consumption_id"])

        schedule = conn.execute(
            """
            SELECT ownership_generation, applied_delay_seconds, scheduled_from,
                   next_review_at, reason, mention_sensitivity
            FROM agent_review_schedules
            WHERE plan_id = ?
            """,
            (result.review_plan_id,),
        ).fetchone()
        assert schedule is not None
        assert int(schedule["ownership_generation"]) == result.ownership.generation
        assert float(schedule["applied_delay_seconds"]) == 10.0
        assert float(schedule["scheduled_from"]) == 110.0
        assert float(schedule["next_review_at"]) == 120.0
        assert str(schedule["reason"]) == "deferred_review"
        assert str(schedule["mention_sensitivity"]) == "high"

        outbox = conn.execute(
            """
            SELECT status, failed_at, last_error_code, last_error_message,
                   claim_id, lease_owner, lease_until
            FROM agent_route_outbox
            WHERE delivery_id = ?
            """,
            (prepared.delivery.delivery_id,),
        ).fetchone()
        assert outbox is not None
        assert str(outbox["status"]) == "failed"
        assert float(outbox["failed_at"]) == 110.0
        assert str(outbox["last_error_code"]) == "legacy_state_handoff_materialized"
        assert str(outbox["claim_id"]) == ""
        assert str(outbox["lease_owner"]) == ""
        assert outbox["lease_until"] is None

        completion = conn.execute(
            """
            SELECT manifest_id, materializer_id, materializer_version,
                   target_schema_version, source_digest, target_digest,
                   ownership_generation, completion_reason, completed_at
            FROM agent_session_actor_v2_legacy_state_handoff_finalizations
            WHERE barrier_id = ?
            """,
            (prepared.grant.barrier.barrier_id,),
        ).fetchone()
        assert completion is not None
        assert str(completion["manifest_id"]) == prepared.manifest.manifest_id
        assert str(completion["materializer_id"]) == prepared.materialization.materializer_id
        assert int(completion["materializer_version"]) == 1
        assert int(completion["target_schema_version"]) == 1
        assert str(completion["source_digest"]) == prepared.manifest.source_digest
        assert str(completion["target_digest"]) == prepared.materialization.target_digest
        assert int(completion["ownership_generation"]) == result.ownership.generation
        assert float(completion["completed_at"]) == 110.0
        with pytest.raises(sqlite3.IntegrityError, match="finalization is immutable"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_legacy_state_handoff_finalizations
                SET completion_reason = 'tampered'
                WHERE barrier_id = ?
                """,
                (prepared.grant.barrier.barrier_id,),
            )

    assert (
        DurableMessageRoutingRepository(
            prepared.database,
            clock=lambda: prepared.now[0],
        ).claim_next_delivery(worker_id="would-be-replay-worker")
        is None
    )
    prepared.database.initialize()
    assert prepared.database.actor_v2_migration_barriers.get(prepared.key) == result.barrier
    with pytest.raises(ActorV2MigrationBarrierLost, match="no longer matches"):
        finalizer.finalize(
            barrier_grant=prepared.grant,
            manifest_id=prepared.manifest.manifest_id,
            reason="a completed handoff cannot be finalized twice",
        )


def test_finalizer_maps_elapsed_review_to_its_commit_clock(tmp_path: Path) -> None:
    """An overdue legacy timer becomes due at the single ownership commit time."""

    prepared = _prepared_handoff(tmp_path, next_review_at=99.0)
    prepared.now[0] = 110.0

    result = SQLiteActorV2LegacyIdleStateFinalizer(
        prepared.database,
        clock=lambda: prepared.now[0],
    ).finalize(
        barrier_grant=prepared.grant,
        manifest_id=prepared.manifest.manifest_id,
        reason="preserve an already due review at the target commit clock",
    )

    with prepared.database.connect() as conn:
        schedule = conn.execute(
            """
            SELECT ownership_generation, applied_delay_seconds, scheduled_from, next_review_at
            FROM agent_review_schedules
            WHERE plan_id = ?
            """,
            (result.review_plan_id,),
        ).fetchone()
    assert schedule is not None
    assert int(schedule["ownership_generation"]) == result.ownership.generation
    assert float(schedule["applied_delay_seconds"]) == 0.0
    assert float(schedule["scheduled_from"]) == 110.0
    assert float(schedule["next_review_at"]) == 110.0


def test_finalization_startup_validation_requires_its_completed_actor_owner(
    tmp_path: Path,
) -> None:
    """A sidecar cannot survive restart after its final Actor owner is changed."""

    prepared = _prepared_handoff(tmp_path)
    prepared.now[0] = 110.0
    SQLiteActorV2LegacyIdleStateFinalizer(
        prepared.database,
        clock=lambda: prepared.now[0],
    ).finalize(
        barrier_grant=prepared.grant,
        manifest_id=prepared.manifest.manifest_id,
        reason="commit one source before owner-integrity validation",
    )
    with prepared.database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_runtime_ownership
            SET mode = 'legacy'
            WHERE profile_id = ? AND session_id = ?
            """,
            (prepared.key.profile_id, prepared.key.session_id),
        )

    with pytest.raises(sqlite3.IntegrityError, match="completed Actor owner"):
        prepared.database.initialize()


def test_finalizer_rolls_back_when_legacy_semantics_lack_a_materializer(
    tmp_path: Path,
) -> None:
    """Unsupported legacy context leaves all target and source-boundary rows intact."""

    prepared = _prepared_handoff(tmp_path, include_recent_mention=True)
    prepared.now[0] = 110.0
    finalizer = SQLiteActorV2LegacyIdleStateFinalizer(
        prepared.database,
        clock=lambda: prepared.now[0],
    )

    with pytest.raises(ActorV2LegacyIdleStateFinalizationBlocked) as blocked:
        finalizer.finalize(
            barrier_grant=prepared.grant,
            manifest_id=prepared.manifest.manifest_id,
            reason="reject source context without an Actor semantics materializer",
        )

    assert blocked.value.blockers == ("legacy_recent_mentions_materializer_unavailable",)
    ownership = prepared.database.agent_runtime_ownership.get(prepared.key)
    assert ownership is not None
    assert ownership.mode is AgentRuntimeOwnershipMode.LEGACY
    assert ownership.status is AgentRuntimeOwnershipStatus.MIGRATING
    assert ownership.generation == prepared.grant.barrier.migration_generation
    assert (
        prepared.database.actor_v2_migration_barriers.get(prepared.key).status
        is ActorV2MigrationBarrierStatus.MIGRATING
    )
    with prepared.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM agent_session_aggregates").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM agent_session_actor_v2_legacy_state_handoff_finalizations"
            ).fetchone()[0]
            == 0
        )
        outbox = conn.execute(
            "SELECT status FROM agent_route_outbox WHERE delivery_id = ?",
            (prepared.delivery.delivery_id,),
        ).fetchone()
    assert outbox is not None
    assert str(outbox["status"]) == "pending"


def test_finalizer_rejects_live_outbox_payload_drift_without_partial_commit(
    tmp_path: Path,
) -> None:
    """A noncanonical outbox replay cannot be merged with frozen ledger evidence."""

    prepared = _prepared_handoff(tmp_path)
    noncanonical_payload = json.dumps(
        prepared.delivery.to_payload(),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
    )
    with prepared.database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_route_outbox
            SET payload_json = ?, payload_digest = ?
            WHERE delivery_id = ?
            """,
            (
                noncanonical_payload,
                _digest(noncanonical_payload),
                prepared.delivery.delivery_id,
            ),
        )

    prepared.now[0] = 110.0
    finalizer = SQLiteActorV2LegacyIdleStateFinalizer(
        prepared.database,
        clock=lambda: prepared.now[0],
    )
    with pytest.raises(ActorV2LegacyIdleStateFinalizationBlocked) as blocked:
        finalizer.finalize(
            barrier_grant=prepared.grant,
            manifest_id=prepared.manifest.manifest_id,
            reason="reject a replay payload that drifted after source capture",
        )

    assert blocked.value.blockers == ("legacy_verified_route_delivery_drift",)
    ownership = prepared.database.agent_runtime_ownership.get(prepared.key)
    assert ownership is not None
    assert ownership.status is AgentRuntimeOwnershipStatus.MIGRATING
    with prepared.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM agent_session_aggregates").fetchone()[0] == 0
        outbox = conn.execute(
            "SELECT status, last_error_code FROM agent_route_outbox WHERE delivery_id = ?",
            (prepared.delivery.delivery_id,),
        ).fetchone()
    assert outbox is not None
    assert str(outbox["status"]) == "pending"
    assert str(outbox["last_error_code"]) == ""
