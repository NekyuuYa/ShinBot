"""Integration coverage for the unmounted complete-history harness lifecycle."""

from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.harness import ActorRuntimeHarness
from shinbot.agent.runtime.session_actor.history_lifecycle import (
    ActorRuntimeHistoryLifecycleController,
    ActorRuntimeHistoryLifecycleState,
)
from shinbot.agent.runtime.session_actor.reducer import AgentSessionReducer
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.legacy_recovery_gate import (
    LegacyRecoveryGateBlocked,
    LegacyRecoveryGateMode,
)
from shinbot.persistence import DatabaseManager


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized SQLite domain for a history lifecycle test."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


async def _noop_effect_handler(_context: EffectExecutionContext) -> EffectHandlerResult:
    """Bind a complete test-only historical graph without performing I/O."""

    return EffectHandlerResult()


def _components(
    database: DatabaseManager,
) -> tuple[
    ActorRuntimeHistoryLifecycleController,
    ActorRuntimeHarness,
    DurableEffectExecutor,
]:
    """Compose one full but unmounted history lifecycle for the database."""

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
    for contract in authority.contracts():
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
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
    )
    controller = ActorRuntimeHistoryLifecycleController(
        harness=harness,
        legacy_recovery_gate=database.actor_v2_legacy_recovery_gate,
        holder_id="history-lifecycle-integration",
    )
    return controller, harness, executor


@pytest.mark.asyncio
async def test_history_lifecycle_owns_real_registry_and_executor_until_stop(
    tmp_path: Path,
) -> None:
    """A recovery permit remains held until all harness workers have stopped."""

    database = _database(tmp_path)
    controller, harness, executor = _components(database)
    gate = database.actor_v2_legacy_recovery_gate

    try:
        snapshot = await controller.activate()

        assert snapshot.state is ActorRuntimeHistoryLifecycleState.ACTIVE
        assert harness.active is True
        assert executor.started is True
        assert gate.snapshot().mode is LegacyRecoveryGateMode.LEGACY_RECOVERY_ACTIVE
        assert not hasattr(controller, "actor_wake_target")
        assert not hasattr(controller, "session_actor_registry")
        with pytest.raises(LegacyRecoveryGateBlocked, match="active legacy recovery"):
            database.actor_v2_admission_fences.reserve(
                SessionKey("fenced-profile", "fenced-session"),
                holder_id="blocked-admission",
                ttl_seconds=30.0,
            )
    finally:
        await controller.shutdown()

    assert controller.snapshot.state is ActorRuntimeHistoryLifecycleState.CLOSED
    assert harness.shutdown_complete is True
    assert executor.started is False
    assert gate.snapshot().mode is LegacyRecoveryGateMode.LEGACY_OPEN


@pytest.mark.asyncio
async def test_history_lifecycle_cannot_start_after_fenced_admission(
    tmp_path: Path,
) -> None:
    """An existing fenced admission blocks historical worker startup entirely."""

    database = _database(tmp_path)
    database.actor_v2_admission_fences.reserve(
        SessionKey("fenced-profile", "fenced-session"),
        holder_id="existing-admission",
        ttl_seconds=30.0,
    )
    controller, harness, executor = _components(database)

    with pytest.raises(LegacyRecoveryGateBlocked, match="fenced"):
        await controller.activate()

    assert controller.snapshot.state is ActorRuntimeHistoryLifecycleState.READY
    assert harness.active is False
    assert executor.started is False
    assert (
        database.actor_v2_legacy_recovery_gate.snapshot().mode
        is LegacyRecoveryGateMode.FENCED_ONLY
    )

    await controller.shutdown()
    assert controller.snapshot.state is ActorRuntimeHistoryLifecycleState.CLOSED
    assert harness.shutdown_complete is True
