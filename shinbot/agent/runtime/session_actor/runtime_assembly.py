"""Deliberately inactive composition root for the future Actor v2 runtime.

This component binds the durable stores, actor registry, effect executor, and
handler graph to one database domain. It intentionally cannot start workers or
expose a core wake target: production ownership remains with the legacy runtime
until the complete activation contract is satisfied.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

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
from shinbot.agent.runtime.session_actor.reducer import AgentSessionReducer
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore

if TYPE_CHECKING:
    from shinbot.persistence import DatabaseManager


HandlerConfigurator = Callable[[EffectHandlerRegistry], None]


@dataclass(slots=True, frozen=True)
class ActorV2RuntimeReadiness:
    """Observable handler-graph status for a deliberately inactive assembly."""

    handler_failures: tuple[RequiredEffectContractFailure, ...]
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


class ActorV2RuntimeActivationBlocked(RuntimeError):
    """Raised when code attempts to activate the diagnostic-only assembly."""

    def __init__(self, readiness: ActorV2RuntimeReadiness) -> None:
        """Render the exact missing contracts rather than starting partial work."""

        self.readiness = readiness
        failures = ", ".join(
            f"{failure.contract.effect_kind}:v{failure.contract.version} "
            f"({failure.reason})"
            for failure in readiness.handler_failures
        )
        detail = failures or "runtime activation gate is intentionally closed"
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
    ) -> None:
        self._actor_store = actor_store
        self._effect_store = effect_store
        self._handlers = handlers
        self._registry = registry
        self._executor = executor
        self._harness = harness

    @classmethod
    def compose_inactive(
        cls,
        database: DatabaseManager,
        *,
        reducer: AgentSessionReducer | None = None,
        configure_handlers: HandlerConfigurator | None = None,
        effect_contract_authority: EffectContractAuthority | None = None,
    ) -> ActorV2RuntimeAssembly:
        """Compose all durable components without starting or exposing them.

        Args:
            database: Initialized database that owns every component transaction.
            reducer: Optional pure reducer retained by the actor registry.
            configure_handlers: Optional explicit registration hook for tests or
                incomplete development slices. It cannot make this assembly live.
            effect_contract_authority: Exact sealed contract graph shared by all
                components. Omitting it uses the complete built-in graph.
        """

        authority = effect_contract_authority or builtin_effect_contract_authority()
        actor_store = SQLiteSessionActorStore(
            database,
            effect_contract_authority=authority,
        )
        effect_store = SQLiteDurableEffectStore(
            database,
            contract_authority=authority,
        )
        handlers = EffectHandlerRegistry(contract_authority=authority)
        if configure_handlers is not None:
            configure_handlers(handlers)
        session_reducer = reducer or AgentSessionReducer()
        registry = AgentSessionActorRegistry(
            store=actor_store,
            handler=session_reducer.reduce,
        )
        executor = DurableEffectExecutor(
            store=effect_store,
            handlers=handlers,
            session_registry=registry,
        )
        harness = ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            allow_partial_contracts=True,
        )
        return cls(
            actor_store=actor_store,
            effect_store=effect_store,
            handlers=handlers,
            registry=registry,
            executor=executor,
            harness=harness,
        )

    @property
    def handler_registry(self) -> EffectHandlerRegistry:
        """Return the unsealed registry for explicit incomplete-slice wiring."""

        return self._handlers

    @property
    def readiness(self) -> ActorV2RuntimeReadiness:
        """Return the exact activation-preflight failures without starting work."""

        return ActorV2RuntimeReadiness(
            handler_failures=self._harness.required_handler_failures()
        )

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

    async def activate(self) -> None:
        """Fail closed until a future, explicit production activation change."""

        raise ActorV2RuntimeActivationBlocked(self.readiness)

    async def shutdown(self) -> None:
        """Close unstarted dependencies without draining or starting workers."""

        await self._harness.shutdown(drain=False)


__all__ = [
    "ActorV2RuntimeActivationBlocked",
    "ActorV2RuntimeAssembly",
    "ActorV2RuntimeReadiness",
    "HandlerConfigurator",
]
