"""Integration coverage for the dormant durable Actor v2 canary lease."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.canary_isolation_lease import (
    SQLiteActorV2CanaryIsolationLease,
)
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.actor_v2_canary_isolation import (
    ActorV2CanaryIsolationLeaseBlocked,
    ActorV2CanaryIsolationLeaseConflict,
    ActorV2CanaryIsolationLeaseLost,
    ActorV2CanaryIsolationLeaseStatus,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.legacy_recovery_gate import LegacyRecoveryGateBlocked
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_canary_isolation_lease import (
    ActorV2CanaryIsolationLeaseRepository,
)


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for canary-lease tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _repository(
    database: DatabaseManager,
    now: list[float],
) -> ActorV2CanaryIsolationLeaseRepository:
    """Install a deterministic lease repository into one database domain."""

    sequence = ["canary-token-a", "canary-token-b", "canary-token-c"]
    repository = ActorV2CanaryIsolationLeaseRepository(
        database,
        clock=lambda: now[0],
        holder_token_factory=lambda: sequence.pop(0),
    )
    database.actor_v2_canary_isolation_leases = repository
    return repository


def test_fresh_schema_exposes_empty_durable_canary_isolation_slot(tmp_path: Path) -> None:
    """Schema initialization creates the contract without inventing a holder."""

    database = _database(tmp_path)

    assert isinstance(
        database.actor_v2_canary_isolation_leases,
        ActorV2CanaryIsolationLeaseRepository,
    )
    assert database.actor_v2_canary_isolation_leases.get() is None
    with database.connect() as conn:
        table = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'agent_runtime_actor_v2_canary_isolation_leases'
            """
        ).fetchone()
        rows = conn.execute(
            "SELECT COUNT(*) AS count FROM agent_runtime_actor_v2_canary_isolation_leases"
        ).fetchone()

    assert table is not None
    assert rows is not None
    assert rows["count"] == 0


def test_release_and_replacement_epoch_fence_stale_holders(tmp_path: Path) -> None:
    """Only the current opaque epoch can validate or release the singleton slot."""

    now = [10.0]
    database = _database(tmp_path)
    repository = _repository(database, now)
    first = repository.acquire(holder_id="canary-controller")

    assert first.lease.lease_epoch == 1
    assert repository.validate(first) == first.lease
    with pytest.raises(ActorV2CanaryIsolationLeaseConflict, match="active"):
        repository.acquire(holder_id="other-controller")

    now[0] = 11.0
    released = repository.release(first)

    assert released.status is ActorV2CanaryIsolationLeaseStatus.RELEASED
    assert released.released_at == 11.0
    assert repository.release(first) == released
    with pytest.raises(ActorV2CanaryIsolationLeaseLost, match="no longer active"):
        repository.validate(first)

    now[0] = 12.0
    second = repository.acquire(holder_id="canary-controller")

    assert second.lease.lease_epoch == 2
    assert second.lease.created_at == 12.0
    with pytest.raises(ActorV2CanaryIsolationLeaseLost, match="no longer belongs"):
        repository.release(first)
    assert repository.validate(second) == second.lease


def test_explicit_revocation_preserves_history_without_allowing_stale_takeover(
    tmp_path: Path,
) -> None:
    """Revocation is exact, durable, and distinct from a normal release."""

    now = [10.0]
    database = _database(tmp_path)
    repository = _repository(database, now)
    first = repository.acquire(holder_id="canary-controller")

    now[0] = 11.0
    revoked = repository.revoke(first.lease, reason="operator verified old process stopped")

    assert revoked.status is ActorV2CanaryIsolationLeaseStatus.REVOKED
    assert revoked.revoked_at == 11.0
    assert revoked.revocation_reason == "operator verified old process stopped"
    assert repository.release(first) == revoked

    now[0] = 12.0
    second = repository.acquire(holder_id="replacement-controller")

    assert second.lease.lease_epoch == 2
    with pytest.raises(ActorV2CanaryIsolationLeaseConflict, match="changed"):
        repository.revoke(first.lease, reason="stale operator observation")


def test_active_canary_lease_blocks_actor_admission_and_legacy_broad_recovery(
    tmp_path: Path,
) -> None:
    """The durable lease closes all currently known competing actor entry points."""

    now = [10.0]
    database = _database(tmp_path)
    repository = _repository(database, now)
    grant = repository.acquire(holder_id="canary-controller")
    actor_key = SessionKey("profile-a", "profile-a:group:room")

    with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
        database.actor_v2_admission_fences.reserve(
            actor_key,
            holder_id="actor-admission",
            ttl_seconds=30.0,
        )
    with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
        database.agent_runtime_ownership.claim(
            actor_key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="actor ownership during canary",
        )
    with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
        database.actor_v2_legacy_recovery_gate.acquire_legacy_recovery(
            holder_id="legacy-recovery"
        )

    legacy = database.agent_runtime_ownership.claim(
        SessionKey("profile-legacy", "profile-legacy:group:room"),
        AgentRuntimeOwnershipMode.LEGACY,
        reason="unrelated legacy ownership remains outside canary scope",
    ).ownership
    assert legacy.mode is AgentRuntimeOwnershipMode.LEGACY

    repository.release(grant)
    admission = database.actor_v2_admission_fences.reserve(
        SessionKey("profile-b", "profile-b:group:room"),
        holder_id="actor-admission-after-release",
        ttl_seconds=30.0,
    )
    assert admission.fence.holder_id == "actor-admission-after-release"


def test_active_canary_lease_blocks_existing_actor_ownership_use_and_new_migrations(
    tmp_path: Path,
) -> None:
    """A canary freezes both live Actor owners and transitions toward Actor v2."""

    now = [10.0]
    database = _database(tmp_path)
    repository = _repository(database, now)
    actor_key = SessionKey("profile-actor", "profile-actor:group:room")
    legacy_key = SessionKey("profile-legacy", "profile-legacy:group:room")
    actor = database.agent_runtime_ownership.claim(
        actor_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor ownership before clean canary",
    ).ownership
    legacy = database.agent_runtime_ownership.claim(
        legacy_key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy ownership before clean canary",
    ).ownership
    grant = repository.acquire(holder_id="canary-controller")

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
            database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                actor_key,
                expected_generation=actor.generation,
            )

    with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
        database.agent_runtime_ownership.begin_migration(
            actor_key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=actor.generation,
            reason="pause actor owner for clean canary",
        )
    with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
        database.agent_runtime_ownership.begin_migration(
            legacy_key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            expected_generation=legacy.generation,
            reason="prevent actor activation during clean canary",
        )

    assert database.agent_runtime_ownership.get(actor_key) == actor
    assert database.agent_runtime_ownership.get(legacy_key) == legacy
    repository.release(grant)


def test_active_canary_lease_freezes_inflight_actor_migration_settlement(
    tmp_path: Path,
) -> None:
    """A lease acquired mid-transition prevents either migration outcome."""

    now = [10.0]
    database = _database(tmp_path)
    repository = _repository(database, now)
    actor_key = SessionKey("profile-actor", "profile-actor:group:room")
    legacy_key = SessionKey("profile-legacy", "profile-legacy:group:room")
    actor = database.agent_runtime_ownership.claim(
        actor_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor ownership before transition",
    ).ownership
    legacy = database.agent_runtime_ownership.claim(
        legacy_key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy ownership before transition",
    ).ownership
    actor_to_legacy = database.agent_runtime_ownership.begin_migration(
        actor_key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=actor.generation,
        reason="actor-to-legacy transition before canary",
    )
    legacy_to_actor = database.agent_runtime_ownership.begin_migration(
        legacy_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=legacy.generation,
        reason="legacy-to-actor transition before canary",
    )
    grant = repository.acquire(holder_id="canary-controller")

    for key, migration in (
        (actor_key, actor_to_legacy),
        (legacy_key, legacy_to_actor),
    ):
        with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
            database.agent_runtime_ownership.complete_migration(
                key,
                expected_generation=migration.generation,
                reason="settlement blocked by clean canary",
            )
        with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
            database.agent_runtime_ownership.abort_migration(
                key,
                expected_generation=migration.generation,
                reason="rollback blocked by clean canary",
            )
        assert database.agent_runtime_ownership.get(key) == migration

    repository.release(grant)


@pytest.mark.asyncio
async def test_active_canary_lease_blocks_existing_actor_mailbox_claim_without_mutation(
    tmp_path: Path,
) -> None:
    """A pre-existing unbound actor cannot acquire new mailbox work during a canary."""

    now = [10.0]
    database = _database(tmp_path)
    repository = _repository(database, now)
    key = SessionKey("profile-actor", "profile-actor:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owner before clean canary",
    ).ownership
    store = SQLiteSessionActorStore(database, clock=lambda: now[0])
    await store.enqueue(
        SessionEventEnvelope(
            event_id="mailbox-before-canary",
            key=key,
            kind="TestCanaryMailboxEvent",
            ownership_generation=owner.generation,
            occurred_at=now[0],
        )
    )
    grant = repository.acquire(holder_id="canary-controller")

    with pytest.raises(ActorV2CanaryIsolationLeaseBlocked, match="active canary"):
        await store.claim_next(key, worker_id="existing-actor-worker")

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, attempt_count, claim_id, lease_owner, lease_until
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, "mailbox-before-canary"),
        ).fetchone()

    assert row is not None
    assert tuple(row) == ("pending", 0, "", "", None)
    repository.release(grant)


def test_canary_acquisition_refuses_an_active_legacy_recovery_permit(tmp_path: Path) -> None:
    """Legacy broad recovery and a clean canary cannot overlap by acquisition order."""

    now = [10.0]
    database = _database(tmp_path)
    repository = _repository(database, now)
    permit = database.actor_v2_legacy_recovery_gate.acquire_legacy_recovery(
        holder_id="legacy-recovery"
    )

    with pytest.raises(LegacyRecoveryGateBlocked, match="active legacy recovery"):
        repository.acquire(holder_id="canary-controller")

    database.actor_v2_legacy_recovery_gate.release_legacy_recovery(permit)
    grant = repository.acquire(holder_id="canary-controller")
    assert grant.lease.lease_epoch == 1


@pytest.mark.asyncio
async def test_sqlite_adapter_fails_closed_after_release_or_external_revocation(
    tmp_path: Path,
) -> None:
    """The lifecycle adapter observes durable lease loss without exposing a token."""

    now = [10.0]
    database = _database(tmp_path)
    repository = _repository(database, now)
    lease = SQLiteActorV2CanaryIsolationLease.acquire(
        repository,
        holder_id="canary-controller",
    )

    assert lease.persistence_domain is database
    assert lease.active is True
    snapshot = repository.get()
    assert snapshot is not None
    repository.revoke(snapshot, reason="operator stop proof")

    assert lease.active is False
    await lease.release()
    assert lease.active is False


def test_weak_canary_lease_schema_fails_closed_on_startup(tmp_path: Path) -> None:
    """A superficially similar table cannot silently weaken epoch fencing."""

    database = _database(tmp_path)
    with database.connect() as conn:
        conn.execute("DROP TABLE agent_runtime_actor_v2_canary_isolation_leases")
        conn.execute(
            """
            CREATE TABLE agent_runtime_actor_v2_canary_isolation_leases (
                lease_id INTEGER,
                lease_epoch INTEGER,
                holder_id TEXT,
                holder_token_digest TEXT,
                status TEXT,
                created_at REAL,
                updated_at REAL,
                released_at REAL,
                revoked_at REAL,
                revocation_reason TEXT
            )
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="does not match its immutable contract"):
        database.initialize()
