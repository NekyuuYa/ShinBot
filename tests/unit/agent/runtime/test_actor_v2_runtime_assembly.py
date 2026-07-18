"""Tests for the deliberately inactive Actor v2 composition root."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from shinbot.agent.runtime.service_health import RuntimeServiceStatus
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
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveReplyWorkflowOutput,
    ActiveReplyWorkflowRequest,
    ReviewWorkflowOutput,
    ReviewWorkflowRequest,
    register_actor_workflow_effect_handlers,
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


@dataclass(slots=True)
class _WorkflowLedger:
    """Empty read-only ledger used only to compose workflow handlers."""

    async def list_captured_unread(
        self,
        **_kwargs: object,
    ) -> tuple[object, ...]:
        """Return no rows because readiness never invokes a model workflow."""

        return ()


@dataclass(slots=True)
class _ActiveReplyWorkflow:
    """Pure active-reply implementation used only for handler graph wiring."""

    async def run_active_reply(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ActiveReplyWorkflowOutput:
        """Return an empty result without touching external runtime state."""

        del request
        return ActiveReplyWorkflowOutput()


@dataclass(slots=True)
class _ReviewWorkflow:
    """Pure review implementation used only for handler graph wiring."""

    async def run_review(self, request: ReviewWorkflowRequest) -> ReviewWorkflowOutput:
        """Return an active-chat placeholder that is never invoked in this test."""

        del request
        return ReviewWorkflowOutput(enter_active_chat=True, next_review_outcome=None)


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

    database = _database(tmp_path)
    assembly = ActorV2RuntimeAssembly.compose_inactive(database)

    assert assembly.actor_wake_target is None
    assert assembly.effects_running is False
    assert assembly.shutdown_complete is False
    assert not hasattr(assembly, "handler_registry")
    assert not hasattr(assembly, "recovery_scanner")
    assert not hasattr(assembly, "recovery_scanner_service")
    assert not hasattr(assembly, "recovery_commit_coordinator")
    assert not hasattr(assembly, "review_due_scanner")
    assert not hasattr(assembly, "workflow_ledger")
    assert not hasattr(assembly, "review_execution_gate_store")
    assert not hasattr(assembly, "model_execution_witness_store")
    assert not hasattr(assembly, "model_execution_cancellation_gate_store")
    assert assembly.recovery_materialization_states == (
        "active_chat",
        "active_chat_settling",
        "active_reply",
        "review",
    )
    assert assembly.readiness.activation_permitted is False
    assert assembly.readiness.activation_blockers == (
        "actor_v2_complete_history_handler_graph_incomplete",
        "actor_v2_diagnostic_assembly_unmounted",
        "actor_v2_durable_isolation_lease_unavailable",
        "actor_v2_ownership_ingress_cutover_controller_unavailable",
        "actor_v2_legacy_state_handoff_manifest_unavailable",
        "actor_v2_base_session_migration_scope_unresolved",
        "actor_v2_wake_target_unpublished",
        "actor_v2_recovery_and_timer_supervision_unmounted",
        "actor_v2_management_mailbox_admission_unavailable",
    )
    assert assembly.readiness.handler_graph_complete is False
    assert [snapshot.status for snapshot in assembly.diagnostics.background_service_health] == [
        RuntimeServiceStatus.STOPPED,
        RuntimeServiceStatus.STOPPED,
    ]
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
    assert assembly.shutdown_complete is True


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
    assert all(contract.effect_kind != "run_idle_review_planning" for contract in missing)
    assert all(contract.effect_kind not in DELAYED_CONTROL_EFFECT_KINDS for contract in missing)
    assert missing
    assert assembly.actor_wake_target is None
    assert assembly.effects_running is False

    await assembly.shutdown()


@pytest.mark.asyncio
async def test_workflow_handler_registration_keeps_assembly_inactive(
    tmp_path: Path,
) -> None:
    """Review handler readiness is diagnostic-only until the activation contract exists."""

    ledger = _WorkflowLedger()
    active_reply_workflow = _ActiveReplyWorkflow()
    review_workflow = _ReviewWorkflow()

    def configure_handlers(registry) -> None:
        register_actor_workflow_effect_handlers(
            registry,
            ledger=ledger,
            active_reply_workflow=active_reply_workflow,
            review_workflow=review_workflow,
        )

    assembly = ActorV2RuntimeAssembly.compose_inactive(
        _database(tmp_path),
        configure_handlers=configure_handlers,
    )

    missing = assembly.readiness.missing_handler_contracts
    assert all(
        contract.effect_kind not in {"run_active_reply_workflow", "run_review_workflow"}
        for contract in missing
    )
    assert missing
    assert assembly.actor_wake_target is None
    assert assembly.effects_running is False
    with pytest.raises(ActorV2RuntimeActivationBlocked):
        await assembly.activate()
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
