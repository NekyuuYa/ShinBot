"""Integration coverage for the fail-closed clean Actor v2 preflight."""

from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.canary_isolation_lease import (
    SQLiteActorV2CanaryIsolationLease,
)
from shinbot.agent.runtime.session_actor.canary_lifecycle import (
    ActorV2CanaryLifecycleController,
    ActorV2CanaryLifecycleState,
)
from shinbot.agent.runtime.session_actor.clean_session_activation import (
    CleanSessionActivationReadiness,
    SQLiteCleanSessionActivationPreflight,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_clean_session_actor_v2_effect_contracts,
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.harness import (
    ActorRuntimeActivationScope,
    ActorRuntimeHarness,
)
from shinbot.agent.runtime.session_actor.reducer import AgentSessionReducer
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager


def _database(tmp_path: Path) -> DatabaseManager:
    """Build an initialized SQLite domain for one preflight observation."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


async def _noop_effect_handler(
    _context: EffectExecutionContext,
) -> EffectHandlerResult:
    """Provide an inert executable binding for a cold-start composition test."""

    return EffectHandlerResult()


@pytest.mark.asyncio
async def test_empty_sqlite_domain_permits_clean_session_activation_preflight(
    tmp_path: Path,
) -> None:
    """Schema creation alone is not evidence of prior Actor v2 work."""

    database = _database(tmp_path)
    preflight = SQLiteCleanSessionActivationPreflight(database)

    readiness = await preflight.check()

    assert preflight.persistence_domain is database
    assert readiness == CleanSessionActivationReadiness()
    assert readiness.permitted is True


@pytest.mark.asyncio
async def test_clean_canary_lifecycle_cold_starts_real_sqlite_domain_without_recovery(
    tmp_path: Path,
) -> None:
    """A clean start must not create ownership or replay any durable history."""

    database = _database(tmp_path)
    authority = builtin_effect_contract_authority()
    actor_store = SQLiteSessionActorStore(
        database,
        effect_contract_authority=authority,
    )
    effect_store = SQLiteDurableEffectStore(
        database,
        contract_authority=authority,
    )
    handlers = EffectHandlerRegistry(contract_authority=authority)
    for contract in builtin_clean_session_actor_v2_effect_contracts():
        handlers.register(
            contract.effect_kind,
            _noop_effect_handler,
            contract=contract,
        )
    registry = AgentSessionActorRegistry(
        store=actor_store,
        handler=AgentSessionReducer().reduce,
    )
    executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        session_registry=registry,
        poll_interval_seconds=60.0,
        renew_interval_seconds=None,
    )
    preflight = SQLiteCleanSessionActivationPreflight(database)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
        activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
        clean_session_preflight=preflight,
    )
    lease = SQLiteActorV2CanaryIsolationLease.acquire(
        database.actor_v2_canary_isolation_leases,
        holder_id="clean-canary-integration-test",
    )
    controller = ActorV2CanaryLifecycleController(
        harness=harness,
        isolation_lease=lease,
    )

    try:
        snapshot = await controller.activate()

        assert snapshot.state is ActorV2CanaryLifecycleState.ACTIVE
        assert harness.active is True
        assert executor.started is True
        assert not hasattr(controller, "actor_wake_target")
        assert not hasattr(controller, "session_actor_registry")
        with database.connect() as conn:
            counts = tuple(
                conn.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM agent_session_runtime_ownership),
                        (SELECT COUNT(*) FROM agent_session_aggregates),
                        (SELECT COUNT(*) FROM agent_session_mailbox),
                        (SELECT COUNT(*) FROM agent_effect_outbox),
                        (SELECT COUNT(*) FROM agent_route_outbox)
                    """
                ).fetchone()
            )
        assert counts == (0, 0, 0, 0, 0)
    finally:
        await controller.shutdown()

    assert controller.snapshot.state is ActorV2CanaryLifecycleState.CLOSED
    assert harness.shutdown_complete is True
    assert lease.active is False
    assert await preflight.check() == CleanSessionActivationReadiness()


@pytest.mark.asyncio
async def test_actor_v2_ownership_and_actor_residual_rows_block_clean_preflight(
    tmp_path: Path,
) -> None:
    """Current ownership and state residue each independently reject a domain."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test actor ownership",
    ).ownership
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 1.0, 1.0)
            """,
            (key.profile_id, key.session_id, ownership.generation),
        )

    readiness = await SQLiteCleanSessionActivationPreflight(database).check()
    blockers = {blocker.code: blocker.count for blocker in readiness.blockers}

    assert readiness.permitted is False
    assert blockers["actor_v2_ownership_history_present"] == 1
    assert blockers["actor_v2_residual_agent_session_aggregates"] == 1


@pytest.mark.asyncio
async def test_advanced_effect_scrub_cursor_blocks_clean_preflight(
    tmp_path: Path,
) -> None:
    """The schema's zero cursor is inert, but scrubbed history is not."""

    database = _database(tmp_path)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_scrub_state
            SET last_effect_seq = 1, updated_at = 1.0
            WHERE cursor_name = 'claimable'
            """
        )

    readiness = await SQLiteCleanSessionActivationPreflight(database).check()

    assert readiness.permitted is False
    assert readiness.blockers[0].code == "actor_v2_residual_agent_effect_scrub_state"
    assert readiness.blockers[0].count == 1


@pytest.mark.asyncio
async def test_historical_actor_v2_ownership_event_blocks_after_legacy_rollback(
    tmp_path: Path,
) -> None:
    """A cleared current row cannot erase the fact that v2 once owned a domain."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    claimed = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test actor ownership",
    ).ownership
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=claimed.generation,
        reason="test legacy rollback",
    )
    restored = database.agent_runtime_ownership.complete_migration(
        key,
        expected_generation=migrating.generation,
        reason="test rollback complete",
    )

    readiness = await SQLiteCleanSessionActivationPreflight(database).check()
    blockers = {blocker.code: blocker.count for blocker in readiness.blockers}

    assert restored.mode is AgentRuntimeOwnershipMode.LEGACY
    assert readiness.permitted is False
    assert blockers == {"actor_v2_ownership_history_present": 1}


@pytest.mark.asyncio
async def test_legacy_ownership_without_actor_v2_history_does_not_block_preflight(
    tmp_path: Path,
) -> None:
    """The clean check distinguishes legacy ownership from Actor v2 residue."""

    database = _database(tmp_path)
    database.agent_runtime_ownership.claim(
        SessionKey("profile-a", "profile-a:group:room"),
        AgentRuntimeOwnershipMode.LEGACY,
        reason="test legacy ownership",
    )

    readiness = await SQLiteCleanSessionActivationPreflight(database).check()

    assert readiness == CleanSessionActivationReadiness()
    assert readiness.permitted is True
