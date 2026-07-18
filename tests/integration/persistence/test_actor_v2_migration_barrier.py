"""Integration coverage for the dormant holder-fenced Actor v2 migration barrier."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2LegacyStateHandoffRequired,
    ActorV2MigrationBarrierConflict,
    ActorV2MigrationBarrierLost,
    ActorV2MigrationBarrierStatus,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.core.dispatch.legacy_recovery_gate import LegacyRecoveryGateMode
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierRepository,
)
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for migration-barrier tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _repositories(
    database: DatabaseManager,
    now: list[float],
) -> tuple[ActorV2MigrationBarrierRepository, AgentRuntimeOwnershipRepository]:
    """Install deterministic ownership and barrier repositories sharing one clock."""

    ownership = AgentRuntimeOwnershipRepository(database, clock=lambda: now[0])
    barrier = ActorV2MigrationBarrierRepository(
        database,
        clock=lambda: now[0],
        barrier_id_factory=lambda: "migration-barrier-a",
        holder_token_factory=lambda: "migration-holder-token-secret",
    )
    database.agent_runtime_ownership = ownership
    database.actor_v2_migration_barriers = barrier
    return barrier, ownership


def _legacy_source(
    ownership: AgentRuntimeOwnershipRepository,
) -> tuple[SessionKey, int]:
    """Create one active unfenced legacy source eligible for a barrier."""

    key = SessionKey("profile-a", "profile-a:group:room")
    source = ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy source for migration barrier",
        legacy_session_id="instance-a:group:room",
        requested_by="test",
    ).ownership
    return key, source.generation


def test_start_atomically_fences_recovery_and_ownership(tmp_path: Path) -> None:
    """One holder grant creates the only durable migration authority."""

    now = [100.0]
    database = _database(tmp_path)
    barrier, ownership = _repositories(database, now)
    key, source_generation = _legacy_source(ownership)

    grant = barrier.start_legacy_to_actor_v2(
        key,
        expected_generation=source_generation,
        adapter_instance_ids=("adapter-a",),
        holder_id="cutover-controller-a",
        reason="begin fenced legacy-to-actor migration",
    )

    assert grant.barrier.status is ActorV2MigrationBarrierStatus.MIGRATING
    assert grant.barrier.legacy_session_id == "instance-a:group:room"
    assert grant.barrier.adapter_instance_ids == ("adapter-a",)
    assert grant.barrier.source_generation == source_generation
    assert grant.barrier.migration_generation == source_generation + 1
    current = ownership.get(key)
    assert current is not None
    assert current.mode is AgentRuntimeOwnershipMode.LEGACY
    assert current.status is AgentRuntimeOwnershipStatus.MIGRATING
    assert current.pending_mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert current.generation == grant.barrier.migration_generation
    assert (
        database.actor_v2_legacy_recovery_gate.snapshot().mode
        is LegacyRecoveryGateMode.FENCED_ONLY
    )
    with database.connect() as conn:
        stored = conn.execute(
            "SELECT holder_token_digest FROM agent_session_actor_v2_migration_barriers"
        ).fetchone()
    assert stored is not None
    assert str(stored["holder_token_digest"]) != grant.holder_token

    with pytest.raises(ActorV2MigrationBarrierConflict, match="holder capability"):
        ownership.begin_migration(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            expected_generation=current.generation,
            reason="generic caller cannot replace active barrier",
        )
    with pytest.raises(ActorV2MigrationBarrierConflict, match="holder capability"):
        ownership.abort_migration(
            key,
            expected_generation=current.generation,
            reason="generic caller cannot abort active barrier",
        )
    with pytest.raises(ActorV2MigrationBarrierConflict, match="holder capability"):
        ownership.complete_migration(
            key,
            expected_generation=current.generation,
            reason="generic caller cannot complete active barrier",
        )


def test_holder_abort_restores_legacy_and_keeps_terminal_history(tmp_path: Path) -> None:
    """Only the exact holder can roll back its source before target publication."""

    now = [100.0]
    database = _database(tmp_path)
    barrier, ownership = _repositories(database, now)
    key, source_generation = _legacy_source(ownership)
    grant = barrier.start_legacy_to_actor_v2(
        key,
        expected_generation=source_generation,
        adapter_instance_ids=("adapter-a",),
        holder_id="cutover-controller-a",
        reason="begin migration to test controlled abort",
    )

    now[0] = 101.0
    result = barrier.abort(
        grant,
        reason="all local freezes thawed after target preflight failure",
    )

    assert result.barrier.status is ActorV2MigrationBarrierStatus.ABORTED
    assert result.barrier.abort_reason == "all local freezes thawed after target preflight failure"
    assert result.ownership.mode is AgentRuntimeOwnershipMode.LEGACY
    assert result.ownership.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert result.ownership.generation == grant.barrier.migration_generation + 1
    assert barrier.get(key) == result.barrier
    with pytest.raises(ActorV2MigrationBarrierLost):
        barrier.abort(grant, reason="stale holder cannot abort twice")
    with pytest.raises(ActorV2MigrationBarrierConflict, match="history already exists"):
        barrier.start_legacy_to_actor_v2(
            key,
            expected_generation=result.ownership.generation,
            adapter_instance_ids=("adapter-a",),
            holder_id="replacement-controller",
            reason="history must not be silently reused",
        )


def test_barrier_requires_an_active_unfenced_legacy_source(tmp_path: Path) -> None:
    """An Actor owner or any other source state cannot become a legacy barrier."""

    now = [100.0]
    database = _database(tmp_path)
    barrier, ownership = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    actor = ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="wrong source for legacy migration barrier",
        legacy_session_id="instance-a:group:room",
        requested_by="test",
    ).ownership

    with pytest.raises(ActorV2MigrationBarrierConflict, match="active unfenced legacy"):
        barrier.start_legacy_to_actor_v2(
            key,
            expected_generation=actor.generation,
            adapter_instance_ids=("adapter-a",),
            holder_id="cutover-controller-a",
            reason="cannot migrate an Actor source through legacy barrier",
        )


def test_barrier_rejects_legacy_scheduler_state_without_a_handoff_manifest(
    tmp_path: Path,
) -> None:
    """A live review plan cannot be frozen into an unsupported migration gap."""

    now = [100.0]
    database = _database(tmp_path)
    barrier, ownership = _repositories(database, now)
    key, source_generation = _legacy_source(ownership)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (
                session_id, state, next_review_at, review_reason,
                mention_sensitivity, active_reply_threshold_json,
                active_chat_state_json, state_resume_json, updated_at
            ) VALUES (?, 'idle', ?, 'deferred_review', 'normal', '{}', '{}', '{}', ?)
            """,
            ("instance-a:group:room", 120.0, now[0]),
        )

    with pytest.raises(ActorV2LegacyStateHandoffRequired) as blocked:
        barrier.start_legacy_to_actor_v2(
            key,
            expected_generation=source_generation,
            adapter_instance_ids=("adapter-a",),
            holder_id="cutover-controller-a",
            reason="reject unsupported legacy scheduler handoff",
        )

    assert blocked.value.evidence == ("legacy_scheduler_state",)
    current = ownership.get(key)
    assert current is not None
    assert current.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert current.generation == source_generation
    assert barrier.get(key) is None


def test_barrier_allows_an_empty_default_legacy_scheduler_row(tmp_path: Path) -> None:
    """A default idle row contains no review or active-chat decision to transfer."""

    now = [100.0]
    database = _database(tmp_path)
    barrier, ownership = _repositories(database, now)
    key, source_generation = _legacy_source(ownership)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (
                session_id, state, updated_at
            ) VALUES (?, 'idle', ?)
            """,
            ("instance-a:group:room", now[0]),
        )

    grant = barrier.start_legacy_to_actor_v2(
        key,
        expected_generation=source_generation,
        adapter_instance_ids=("adapter-a",),
        holder_id="cutover-controller-a",
        reason="default scheduler state needs no handoff",
    )

    assert grant.barrier.status is ActorV2MigrationBarrierStatus.MIGRATING


def test_barrier_rejects_immutable_sql_mutation(tmp_path: Path) -> None:
    """Raw SQL cannot replace a barrier holder or erase its migration history."""

    now = [100.0]
    database = _database(tmp_path)
    barrier, ownership = _repositories(database, now)
    key, source_generation = _legacy_source(ownership)
    grant = barrier.start_legacy_to_actor_v2(
        key,
        expected_generation=source_generation,
        adapter_instance_ids=("adapter-a",),
        holder_id="cutover-controller-a",
        reason="begin immutable barrier test",
    )

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="migration barrier"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_migration_barriers
                SET holder_id = 'tampered'
                WHERE barrier_id = ?
                """,
                (grant.barrier.barrier_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="history cannot be deleted"):
            conn.execute(
                """
                DELETE FROM agent_session_actor_v2_migration_barriers
                WHERE barrier_id = ?
                """,
                (grant.barrier.barrier_id,),
            )
    current = ownership.get(key)
    assert current is not None
    assert current.status is AgentRuntimeOwnershipStatus.MIGRATING
