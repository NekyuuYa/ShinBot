"""Tests for the deliberately inactive Actor v2 composition root."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.delayed_control_handler import (
    DELAYED_CONTROL_EFFECT_KINDS,
    register_delayed_control_effect_handlers,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.effect_executor import EffectHandlerResult
from shinbot.agent.runtime.session_actor.idle_review_planning_adapter import (
    IdleReviewPlanningWorkflowOutput,
    IdleReviewPlanningWorkflowRequest,
    register_idle_review_planning_effect_handler,
)
from shinbot.agent.runtime.session_actor.runtime_assembly import (
    ActorV2RuntimeActivationBlocked,
    ActorV2RuntimeAssembly,
)
from shinbot.persistence import DatabaseManager


@dataclass(slots=True)
class _PlannerWorkflow:
    """Pure planner used only to prove partial handler registration."""

    async def run_idle_review_planning(
        self,
        request: IdleReviewPlanningWorkflowRequest,
    ) -> IdleReviewPlanningWorkflowOutput:
        """Return a defaulted outcome without touching any runtime state."""

        del request
        return IdleReviewPlanningWorkflowOutput()


def _database(tmp_path: Path) -> DatabaseManager:
    """Build an initialized SQLite domain for composition checks."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


@pytest.mark.asyncio
async def test_inactive_assembly_never_exposes_a_wake_target_or_starts_workers(
    tmp_path: Path,
) -> None:
    """Constructing the future graph cannot turn it into a second writer."""

    assembly = ActorV2RuntimeAssembly.compose_inactive(_database(tmp_path))

    assert assembly.actor_wake_target is None
    assert assembly.effects_running is False
    assert assembly.readiness.activation_permitted is False
    assert assembly.readiness.handler_graph_complete is False
    assert any(
        contract.effect_kind == "run_idle_review_planning"
        for contract in assembly.readiness.missing_handler_contracts
    )

    with pytest.raises(ActorV2RuntimeActivationBlocked, match="activation is blocked"):
        await assembly.activate()

    assert assembly.actor_wake_target is None
    assert assembly.effects_running is False
    await assembly.shutdown()
    assert assembly.closed is True


@pytest.mark.asyncio
async def test_partial_composition_reports_exact_handler_progress_without_activation(
    tmp_path: Path,
) -> None:
    """One completed adapter reduces diagnostics but cannot start Actor v2."""

    workflow = _PlannerWorkflow()
    def configure_handlers(registry) -> None:
        register_idle_review_planning_effect_handler(registry, workflow=workflow)
        register_delayed_control_effect_handlers(registry)

    assembly = ActorV2RuntimeAssembly.compose_inactive(
        _database(tmp_path),
        configure_handlers=configure_handlers,
    )

    missing = assembly.readiness.missing_handler_contracts
    assert all(
        contract.effect_kind != "run_idle_review_planning" for contract in missing
    )
    assert all(
        contract.effect_kind not in DELAYED_CONTROL_EFFECT_KINDS
        for contract in missing
    )
    assert missing
    assert assembly.actor_wake_target is None
    assert assembly.effects_running is False

    await assembly.shutdown()


@pytest.mark.asyncio
async def test_readiness_reports_non_async_handlers_without_starting_workers(
    tmp_path: Path,
) -> None:
    """Readiness must use the same async-handler checks as future activation."""

    def configure_handlers(registry) -> None:
        for contract in builtin_session_actor_effect_contracts():
            if contract.effect_kind == "run_idle_review_planning":
                registry.register(
                    contract.effect_kind,
                    lambda _context: EffectHandlerResult(),
                    contract=contract,
                )

    assembly = ActorV2RuntimeAssembly.compose_inactive(
        _database(tmp_path),
        configure_handlers=configure_handlers,
    )

    failures = assembly.readiness.handler_failures
    assert any(
        failure.contract.effect_kind == "run_idle_review_planning"
        and failure.reason == "registered handler is not async-callable"
        for failure in failures
    )
    assert assembly.readiness.handler_graph_complete is False
    assert assembly.effects_running is False

    await assembly.shutdown()
