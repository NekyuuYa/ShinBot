"""Deliberately inactive composition root for the future Actor v2 runtime.

This component binds the durable stores, actor registry, effect executor,
recovery graph, and handler graph to one database domain. It intentionally
cannot start workers or expose a core wake target: production ownership remains
with the legacy runtime until the complete activation contract is satisfied.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from shinbot.agent.runtime.service_health import RuntimeServiceHealthSnapshot
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectExecutionContract,
    EffectHandlerRegistry,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.harness import (
    ActorRuntimeHarness,
    RequiredEffectContractFailure,
)
from shinbot.agent.runtime.session_actor.message_ledger import MessageLedgerEntry
from shinbot.agent.runtime.session_actor.model_execution_cancellation_gate import (
    SQLiteModelExecutionCancellationGateStore,
)
from shinbot.agent.runtime.session_actor.model_execution_witness import (
    SQLiteModelExecutionWitnessStore,
)
from shinbot.agent.runtime.session_actor.recovery_commit_coordinator import (
    SQLiteRecoveryCommitCoordinator,
)
from shinbot.agent.runtime.session_actor.recovery_materializers import (
    builtin_recovery_materializers,
)
from shinbot.agent.runtime.session_actor.recovery_scanner import (
    SQLiteRecoveryGraphScanner,
)
from shinbot.agent.runtime.session_actor.recovery_scanner_service import (
    DurableRecoveryScannerService,
)
from shinbot.agent.runtime.session_actor.reducer import AgentSessionReducer
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.review_due_scanner import (
    DurableReviewDueRepository,
    DurableReviewDueScannerService,
)
from shinbot.agent.runtime.session_actor.review_execution_gate import (
    SQLiteReviewExecutionGateStore,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore

if TYPE_CHECKING:
    from shinbot.persistence import DatabaseManager


HandlerConfigurator = Callable[[EffectHandlerRegistry], None]

_DIAGNOSTIC_ACTIVATION_BLOCKERS = (
    "actor_v2_diagnostic_assembly_unmounted",
    "actor_v2_durable_isolation_lease_unavailable",
    "actor_v2_ownership_ingress_cutover_controller_unavailable",
    "actor_v2_legacy_state_handoff_manifest_unavailable",
    "actor_v2_base_session_migration_scope_unresolved",
    "actor_v2_wake_target_unpublished",
    "actor_v2_recovery_and_timer_supervision_unmounted",
    "actor_v2_management_mailbox_admission_unavailable",
)


class ActorV2WorkflowLedger:
    """Read-only workflow view over the assembly's private actor store.

    Handlers need durable ledger projections, not the store's mailbox and
    transition writers.  Keeping this facade narrow makes the profile handler
    graph unable to mutate actor state while it is being composed.
    """

    def __init__(self, store: SQLiteSessionActorStore) -> None:
        """Bind one ledger facade to the assembly's store instance."""

        self._store = store

    async def list_captured_unread(
        self,
        *,
        key: SessionKey,
        ownership_generation: int,
        input_watermark: int,
        input_ledger_sequence: int,
    ) -> Sequence[MessageLedgerEntry]:
        """Return only unread rows inside one actor operation fence."""

        return await self._store.list_captured_unread(
            key=key,
            ownership_generation=ownership_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
        )

    async def list_message_ledger(
        self,
        key: SessionKey,
    ) -> Sequence[MessageLedgerEntry]:
        """Return one complete durable ledger without exposing write methods."""

        return await self._store.list_message_ledger(key)


@dataclass(slots=True, frozen=True)
class ActorV2RuntimeCompositionPorts:
    """One-shot internal ports available only during inactive composition.

    This value is supplied to a configurator before the assembly is returned.
    It is deliberately not retained as an assembly property, so diagnostic
    callers cannot start supervisors, mutate a handler graph, or obtain actor
    persistence writers after construction.
    """

    handler_registry: EffectHandlerRegistry
    workflow_ledger: ActorV2WorkflowLedger
    review_execution_gate_store: SQLiteReviewExecutionGateStore
    model_execution_cancellation_gate_store: SQLiteModelExecutionCancellationGateStore


ProfileHandlerConfigurator = Callable[[ActorV2RuntimeCompositionPorts], None]


@dataclass(slots=True, frozen=True)
class ActorV2RuntimeReadiness:
    """Observable handler-graph status for a deliberately inactive assembly."""

    handler_failures: tuple[RequiredEffectContractFailure, ...]
    clean_session_handler_failures: tuple[RequiredEffectContractFailure, ...]
    activation_permitted: bool = False

    @property
    def missing_handler_contracts(self) -> tuple[EffectExecutionContract, ...]:
        """Return contracts that have no bound handler at all."""

        return tuple(
            failure.contract
            for failure in self.handler_failures
            if failure.reason.startswith("no durable effect handler")
        )

    @property
    def handler_graph_complete(self) -> bool:
        """Return whether every required contract has an async-safe handler."""

        return not self.handler_failures

    @property
    def clean_session_handler_graph_complete(self) -> bool:
        """Return whether current Actor-native session work has all handlers.

        Historical contracts intentionally remain outside this coverage. This
        property is not a production activation permit.
        """

        return not self.clean_session_handler_failures

    @property
    def activation_blockers(self) -> tuple[str, ...]:
        """Return stable reasons this diagnostic assembly cannot activate.

        Handler coverage is reported separately from the lifecycle blockers so
        an operator cannot mistake clean-session handler completeness for a
        production cutover permit.
        """

        handler_blockers = (
            ("actor_v2_complete_history_handler_graph_incomplete",) if self.handler_failures else ()
        )
        return (*handler_blockers, *_DIAGNOSTIC_ACTIVATION_BLOCKERS)


@dataclass(slots=True, frozen=True)
class ActorV2RuntimeDiagnostics:
    """Read-only visibility into an inactive Actor v2 assembly.

    This intentionally contains no scanner, registry, executor, store, or
    handler-registry reference. Those objects can mutate durable state and
    remain private to runtime composition until an activation supervisor owns
    their lifecycle.
    """

    readiness: ActorV2RuntimeReadiness
    effects_running: bool
    closed: bool
    shutdown_complete: bool
    recovery_materialization_states: tuple[str, ...]
    background_service_health: tuple[RuntimeServiceHealthSnapshot, ...]
    actor_wake_target_available: bool = False


class ActorV2RuntimeActivationBlocked(RuntimeError):
    """Raised when code attempts to activate the diagnostic-only assembly."""

    def __init__(self, readiness: ActorV2RuntimeReadiness) -> None:
        """Render the exact missing contracts rather than starting partial work."""

        self.readiness = readiness
        blockers = ", ".join(readiness.activation_blockers)
        failures = ", ".join(
            f"{failure.contract.effect_kind}:v{failure.contract.version} ({failure.reason})"
            for failure in readiness.handler_failures
        )
        detail = failures or blockers or "runtime activation gate is intentionally closed"
        super().__init__(f"Actor v2 activation is blocked: {detail}")


class ActorV2RuntimeAssembly:
    """Own an unstarted Actor v2 dependency graph in one persistence domain.

    The assembly is a composition and diagnostics seam, not a feature flag.
    Its ``actor_wake_target`` remains ``None`` and ``activate`` always fails
    closed. This prevents a partial handler graph from becoming a second writer
    merely because it was constructed during development or an integration test.
    """

    def __init__(
        self,
        *,
        actor_store: SQLiteSessionActorStore,
        effect_store: SQLiteDurableEffectStore,
        handlers: EffectHandlerRegistry,
        registry: AgentSessionActorRegistry,
        executor: DurableEffectExecutor,
        harness: ActorRuntimeHarness,
        recovery_scanner: SQLiteRecoveryGraphScanner,
        recovery_scanner_service: DurableRecoveryScannerService,
        recovery_commit_coordinator: SQLiteRecoveryCommitCoordinator,
        review_due_scanner: DurableReviewDueScannerService,
        workflow_ledger: ActorV2WorkflowLedger,
        review_execution_gate_store: SQLiteReviewExecutionGateStore,
        model_execution_witness_store: SQLiteModelExecutionWitnessStore,
        model_execution_cancellation_gate_store: SQLiteModelExecutionCancellationGateStore,
    ) -> None:
        self._actor_store = actor_store
        self._effect_store = effect_store
        self._handlers = handlers
        self._registry = registry
        self._executor = executor
        self._harness = harness
        self._recovery_scanner = recovery_scanner
        self._recovery_scanner_service = recovery_scanner_service
        self._recovery_commit_coordinator = recovery_commit_coordinator
        self._review_due_scanner = review_due_scanner
        self._workflow_ledger = workflow_ledger
        self._review_execution_gate_store = review_execution_gate_store
        self._model_execution_witness_store = model_execution_witness_store
        self._model_execution_cancellation_gate_store = model_execution_cancellation_gate_store

    @classmethod
    def compose_inactive(
        cls,
        database: DatabaseManager,
        *,
        reducer: AgentSessionReducer | None = None,
        configure_handlers: HandlerConfigurator | None = None,
        configure_profile_handlers: ProfileHandlerConfigurator | None = None,
        effect_contract_authority: EffectContractAuthority | None = None,
    ) -> ActorV2RuntimeAssembly:
        """Compose all durable components without starting or exposing them.

        Args:
            database: Initialized database that owns every component transaction.
            reducer: Optional pure reducer retained by the actor registry.
            configure_handlers: Optional explicit registration hook for tests or
                incomplete development slices. It cannot make this assembly live.
            configure_profile_handlers: One-shot internal profile graph hook.
                It receives narrow construction ports before this assembly is
                returned and cannot publish those ports afterward.
            effect_contract_authority: Exact sealed contract graph shared by all
                components. Omitting it uses the complete built-in graph.
        """

        authority = effect_contract_authority or builtin_effect_contract_authority()
        recovery_scanner = SQLiteRecoveryGraphScanner(database)
        recovery_commit_coordinator = SQLiteRecoveryCommitCoordinator(
            recovery_scanner.graph_reader,
            materializers=builtin_recovery_materializers(),
        )
        actor_store = SQLiteSessionActorStore(
            database,
            effect_contract_authority=authority,
            recovery_commit_coordinator=recovery_commit_coordinator,
        )
        workflow_ledger = ActorV2WorkflowLedger(actor_store)
        effect_store = SQLiteDurableEffectStore(
            database,
            contract_authority=authority,
        )
        review_execution_gate_store = SQLiteReviewExecutionGateStore(database)
        model_execution_witness_store = SQLiteModelExecutionWitnessStore(database)
        model_execution_cancellation_gate_store = SQLiteModelExecutionCancellationGateStore(
            database
        )
        handlers = EffectHandlerRegistry(contract_authority=authority)
        if configure_handlers is not None:
            configure_handlers(handlers)
        session_reducer = reducer or AgentSessionReducer()
        registry = AgentSessionActorRegistry(
            store=actor_store,
            handler=session_reducer.reduce,
        )
        review_due_scanner = DurableReviewDueScannerService(
            DurableReviewDueRepository(database),
        )
        recovery_scanner_service = DurableRecoveryScannerService(
            recovery_scanner,
        )
        executor = DurableEffectExecutor(
            store=effect_store,
            handlers=handlers,
            session_registry=registry,
            review_execution_gate_store=review_execution_gate_store,
            model_execution_witness_store=model_execution_witness_store,
        )
        harness = ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            allow_partial_contracts=True,
        )
        if configure_profile_handlers is not None:
            configure_profile_handlers(
                ActorV2RuntimeCompositionPorts(
                    handler_registry=handlers,
                    workflow_ledger=workflow_ledger,
                    review_execution_gate_store=review_execution_gate_store,
                    model_execution_cancellation_gate_store=(
                        model_execution_cancellation_gate_store
                    ),
                )
            )
        return cls(
            actor_store=actor_store,
            effect_store=effect_store,
            handlers=handlers,
            registry=registry,
            executor=executor,
            harness=harness,
            recovery_scanner=recovery_scanner,
            recovery_scanner_service=recovery_scanner_service,
            recovery_commit_coordinator=recovery_commit_coordinator,
            review_due_scanner=review_due_scanner,
            workflow_ledger=workflow_ledger,
            review_execution_gate_store=review_execution_gate_store,
            model_execution_witness_store=model_execution_witness_store,
            model_execution_cancellation_gate_store=(model_execution_cancellation_gate_store),
        )

    @property
    def readiness(self) -> ActorV2RuntimeReadiness:
        """Return the exact activation-preflight failures without starting work."""

        return ActorV2RuntimeReadiness(
            handler_failures=self._harness.required_handler_failures(),
            clean_session_handler_failures=(self._harness.clean_session_handler_failures()),
        )

    @property
    def diagnostics(self) -> ActorV2RuntimeDiagnostics:
        """Return a snapshot that cannot invoke inactive runtime components."""

        return ActorV2RuntimeDiagnostics(
            readiness=self.readiness,
            effects_running=self.effects_running,
            closed=self.closed,
            shutdown_complete=self.shutdown_complete,
            recovery_materialization_states=self.recovery_materialization_states,
            background_service_health=(
                self._recovery_scanner_service.health_snapshot(),
                self._review_due_scanner.health_snapshot(),
            ),
        )

    @property
    def recovery_materialization_states(self) -> tuple[str, ...]:
        """Return no-replay state shapes wired into the inactive store."""

        return self._recovery_commit_coordinator.materializer_states

    @property
    def actor_wake_target(self) -> None:
        """Never expose an actor wake target from an inactive assembly."""

        return None

    @property
    def effects_running(self) -> bool:
        """Return whether the assembly has accidentally started effect workers."""

        return self._executor.running

    @property
    def closed(self) -> bool:
        """Return whether the owned partial harness has been shut down."""

        return self._harness.closed

    @property
    def shutdown_complete(self) -> bool:
        """Return whether the owned harness confirmed both components stopped."""

        return self._harness.shutdown_complete

    async def activate(self) -> None:
        """Fail closed until a future, explicit production activation change."""

        raise ActorV2RuntimeActivationBlocked(self.readiness)

    async def shutdown(self) -> None:
        """Close unstarted dependencies without draining or starting workers."""

        await self._recovery_scanner_service.shutdown()
        await self._review_due_scanner.shutdown()
        await self._harness.shutdown(drain=False)


__all__ = [
    "ActorV2RuntimeActivationBlocked",
    "ActorV2RuntimeAssembly",
    "ActorV2RuntimeDiagnostics",
    "ActorV2RuntimeReadiness",
    "ActorV2WorkflowLedger",
    "HandlerConfigurator",
]
