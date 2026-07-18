"""Integration coverage for frozen legacy source-state handoff staging."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.clean_session_activation import (
    SQLiteCleanSessionActivationPreflight,
)
from shinbot.agent.runtime.session_actor.legacy_state_handoff import (
    ActorV2LegacyStateSnapshotStager,
)
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainNotReady,
    ActorV2CoreIngressDrainReceipt,
)
from shinbot.core.dispatch.actor_v2_legacy_state_handoff import (
    ActorV2LegacyStateHandoffScope,
)
from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierGrant,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
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


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for handoff staging tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _digest(value: str) -> str:
    """Build deterministic token-free local drain evidence."""

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
    """Install deterministic repositories across one shared persistence domain."""

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
    """Create one source owner, barrier, and open core drain request."""

    key = SessionKey("profile-a", "profile-a:group:room")
    source = ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy source for state handoff",
        legacy_session_id="legacy-session-a",
        requested_by="test",
    ).ownership
    participant = ingress.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )
    barrier_grant = barrier.start_legacy_to_actor_v2(
        key,
        expected_generation=source.generation,
        adapter_instance_ids=("adapter-a",),
        holder_id="cutover-controller-a",
        reason="begin source-state handoff boundary",
    )
    request = core.begin_drain(barrier_grant)
    return key, barrier_grant, (request, participant)


def _confirm_core_drain(
    core: ActorV2CoreIngressDrainRepository,
    barrier_grant: ActorV2MigrationBarrierGrant,
    boundary: object,
) -> None:
    """Write the one required local receipt and confirm durable source drain."""

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
    core.confirm_drained(
        request_id=request.request_id,
        barrier_grant=barrier_grant,
    )


def _insert_legacy_source_state(database: DatabaseManager) -> int:
    """Write each legacy scheduling projection after barrier, before local freeze."""

    with database.connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO message_logs (
                session_id, sender_id, content_json, raw_text, role, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("legacy-session-a", "user-a", "[]", "hello", "user", 10.0),
        )
        message_log_id = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (
                session_id, state, next_review_at, review_reason,
                mention_sensitivity, active_reply_threshold_json,
                active_chat_state_json, state_resume_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-session-a",
                "idle",
                120.0,
                "deferred_review",
                "high",
                '{"at_count":2,"window_seconds":45.0}',
                "{}",
                "{}",
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
            ) VALUES (?, ?, ?, ?, ?, 1, 0, 0, 0, 0, ?, ?, 0, 0)
            """,
            (
                "legacy-session-a",
                message_log_id,
                "user-a",
                10.0,
                "normal",
                "bot-a",
                "trace-a",
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_ranges (
                session_id, start_msg_log_id, end_msg_log_id, start_at,
                end_at, message_count, review_consumed, chat_consumed
            ) VALUES (?, ?, ?, ?, ?, 1, 0, 0)
            """,
            ("legacy-session-a", message_log_id, message_log_id, 10.0, 10.0),
        )
        conn.execute(
            """
            INSERT INTO agent_high_priority_events (
                session_id, message_log_id, sender_id, kind, reason, created_at, handled
            ) VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (
                "legacy-session-a",
                message_log_id,
                "user-a",
                "mention",
                "message_mentions_self",
                10.0,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_recent_mentions (session_id, timestamp)
            VALUES (?, ?)
            """,
            ("legacy-session-a", 10.0),
        )
        conn.execute(
            """
            INSERT INTO agent_review_summaries (
                session_id, start_msg_log_id, end_msg_log_id, start_at, end_at,
                message_count, summary, candidate_message_ids_json, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-session-a",
                message_log_id,
                message_log_id,
                10.0,
                10.0,
                1,
                "review-summary",
                f"[{message_log_id}]",
                "review_complete",
                11.0,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_summaries (
                session_id, summary_type, content, source_run_id,
                msg_log_start, msg_log_end, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-session-a",
                "review",
                "prompt-summary",
                "review-run-a",
                message_log_id,
                message_log_id,
                '{"origin":"review"}',
                12.0,
            ),
        )
    return message_log_id


def test_handoff_requires_confirmed_core_drain_before_source_capture(tmp_path: Path) -> None:
    """An open request cannot become a source snapshot merely by holding a barrier."""

    now = [100.0]
    database = _database(tmp_path)
    ownership, barrier, ingress, core, handoff = _repositories(database, now)
    _key, barrier_grant, _boundary = _start_boundary(ownership, barrier, ingress, core)

    with pytest.raises(ActorV2CoreIngressDrainNotReady, match="not durably drained"):
        handoff.capture(barrier_grant)


@pytest.mark.asyncio
async def test_handoff_captures_complete_legacy_source_and_stages_actor_target(
    tmp_path: Path,
) -> None:
    """All legacy decisions survive as immutable source and target-staging data."""

    now = [100.0]
    database = _database(tmp_path)
    ownership, barrier, ingress, core, handoff = _repositories(database, now)
    key, barrier_grant, boundary = _start_boundary(ownership, barrier, ingress, core)
    message_log_id = _insert_legacy_source_state(database)

    now[0] = 101.0
    _confirm_core_drain(core, barrier_grant, boundary)
    now[0] = 102.0
    manifest = handoff.capture(barrier_grant)

    assert manifest.key == key
    assert manifest.scope == ActorV2LegacyStateHandoffScope(
        legacy_session_id="legacy-session-a",
        members=(key,),
    )
    source = manifest.source_payload_as_dict()
    assert source["scheduler_state"] == {
        "state": "idle",
        "next_review_at": 120.0,
        "review_reason": "deferred_review",
        "mention_sensitivity": "high",
        "active_reply_threshold": {"at_count": 2, "window_seconds": 45.0},
        "active_chat_state": {},
        "state_resume": {},
        "updated_at": 100.0,
    }
    assert source["unread_messages"][0]["message_log_id"] == message_log_id
    assert source["route_deliveries"] == [
        {"message_log_id": message_log_id, "status": "missing"}
    ]
    assert source["unread_ranges"][0]["message_count"] == 1
    assert source["high_priority_events"][0]["kind"] == "mention"
    assert source["recent_mentions"][0]["timestamp"] == 10.0
    assert source["review_summaries"][0]["candidate_message_ids"] == [message_log_id]
    assert source["summaries"][0]["metadata"] == {"origin": "review"}
    assert handoff.capture(barrier_grant) == manifest

    now[0] = 103.0
    materialization = handoff.materialize(
        barrier_grant=barrier_grant,
        manifest_id=manifest.manifest_id,
        materializer=ActorV2LegacyStateSnapshotStager(),
    )
    staged = materialization.target_payload_as_dict()
    assert staged["kind"] == "actor_v2_legacy_state_stage"
    assert staged["source_digest"] == manifest.source_digest
    assert staged["legacy_source"] == source
    now[0] = 104.0
    assert (
        handoff.materialize(
            barrier_grant=barrier_grant,
            manifest_id=manifest.manifest_id,
            materializer=ActorV2LegacyStateSnapshotStager(),
        )
        == materialization
    )
    assert handoff.list_materializations(manifest.manifest_id) == (materialization,)

    with database.connect() as conn:
        aggregate_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_aggregates"
        ).fetchone()[0]
        assert aggregate_count == 0
        stored = conn.execute(
            """
            SELECT source_payload_json
            FROM agent_session_actor_v2_legacy_state_handoff_manifests
            WHERE manifest_id = ?
            """,
            (manifest.manifest_id,),
        ).fetchone()
        assert stored is not None
        assert "migration-holder-token-secret" not in str(stored["source_payload_json"])
        with pytest.raises(sqlite3.IntegrityError, match="manifest is immutable"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_legacy_state_handoff_manifests
                SET source_digest = ?
                WHERE manifest_id = ?
                """,
                (_digest("tampered"), manifest.manifest_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="materialization is immutable"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_legacy_state_handoff_materializations
                SET target_digest = ?
                WHERE manifest_id = ?
                """,
                (_digest("tampered-target"), manifest.manifest_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="manifest history cannot be deleted"):
            conn.execute(
                """
                DELETE FROM agent_session_actor_v2_legacy_state_handoff_manifests
                WHERE manifest_id = ?
                """,
                (manifest.manifest_id,),
            )

    database.initialize()
    assert handoff.get(manifest.manifest_id) == manifest

    readiness = await SQLiteCleanSessionActivationPreflight(database).check()
    blockers = {blocker.code: blocker.count for blocker in readiness.blockers}
    assert (
        blockers[
            "actor_v2_residual_agent_session_actor_v2_legacy_state_handoff_manifests"
        ]
        == 1
    )
    assert (
        blockers[
            "actor_v2_residual_agent_session_actor_v2_legacy_state_handoff_materializations"
        ]
        == 1
    )
