from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace

import pytest

from shinbot.agent.runtime.session_actor.clean_session_activation import (
    CleanSessionActivationBlocker,
    CleanSessionActivationReadiness,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS,
    DEFAULT_OUTCOME_FENCE_FIELDS,
    EffectContractAuthority,
    EffectExecutionContract,
    EffectLane,
    builtin_clean_session_actor_v2_effect_contracts,
    builtin_effect_contract_authority,
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectExecutor,
    EffectExecutionContext,
    EffectHandlerRegistry,
    EffectHandlerResult,
    EffectSettlementResult,
)
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    EventEnqueueResult,
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    builtin_external_action_effect_contracts,
)
from shinbot.agent.runtime.session_actor.harness import (
    ActorRuntimeActivationScope,
    ActorRuntimeCleanSessionPreflightError,
    ActorRuntimeHarness,
    ActorRuntimeHarnessActivationError,
    ActorRuntimeHistoryRecoveryPermitRequired,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry

_FULL_CONTRACTS = (
    *builtin_session_actor_effect_contracts(),
    *builtin_external_action_effect_contracts(),
)
_CLEAN_SESSION_CONTRACTS = builtin_clean_session_actor_v2_effect_contracts()


class _EmptySessionStore:
    """Session-store stub that records whether activation attempts recovery."""

    def __init__(
        self,
        effect_contract_authority: EffectContractAuthority | None = None,
        persistence_domain: object | None = None,
    ) -> None:
        self.pending_keys_calls = 0
        self._effect_contract_authority = (
            effect_contract_authority or builtin_effect_contract_authority()
        )
        self._persistence_domain = persistence_domain or object()

    @property
    def persistence_domain(self) -> object:
        """Return the transaction domain used by this fake actor store."""

        return self._persistence_domain

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the immutable authority used by the fake actor store."""

        return self._effect_contract_authority

    def bind_effect_contract_authority(
        self,
        authority: EffectContractAuthority,
    ) -> None:
        """Bind the composition authority before the fake store is observed."""

        self._effect_contract_authority = authority

    def bind_persistence_domain(self, domain: object) -> None:
        """Bind the transaction domain before the fake store is observed."""

        self._persistence_domain = domain

    async def enqueue(self, envelope: SessionEventEnvelope) -> EventEnqueueResult:
        raise AssertionError(f"unexpected mailbox enqueue: {envelope.event_id}")

    async def ensure(self, key) -> object:
        raise AssertionError(f"unexpected aggregate ensure: {key}")

    async def load(self, key) -> object:
        raise AssertionError(f"unexpected aggregate load: {key}")

    async def claim_next(
        self,
        key,
        *,
        worker_id: str,
    ) -> ClaimedSessionEvent | None:
        raise AssertionError(f"unexpected mailbox claim: {key}:{worker_id}")

    async def commit(
        self,
        claim: ClaimedSessionEvent,
        transition: SessionTransition,
        *,
        expected_revision: int,
    ) -> object:
        raise AssertionError(f"unexpected mailbox commit: {claim.envelope.event_id}")

    async def release(self, claim: ClaimedSessionEvent, *, error: str) -> None:
        raise AssertionError(f"unexpected mailbox release: {claim.envelope.event_id}:{error}")

    async def fail(self, claim: ClaimedSessionEvent, *, error: str) -> None:
        raise AssertionError(f"unexpected mailbox fail: {claim.envelope.event_id}:{error}")

    async def recover(self, key, *, worker_id: str) -> int:
        raise AssertionError(f"unexpected per-session recovery: {key}:{worker_id}")

    async def pending_keys(self) -> list:
        self.pending_keys_calls += 1
        return []


class _EmptyEffectStore:
    """Effect-store stub that keeps executor workers idle without I/O."""

    def __init__(
        self,
        effect_contract_authority: EffectContractAuthority | None = None,
        persistence_domain: object | None = None,
    ) -> None:
        self.recover_expired_calls = 0
        self._effect_contract_authority = (
            effect_contract_authority or builtin_effect_contract_authority()
        )
        self._persistence_domain = persistence_domain or object()

    @property
    def persistence_domain(self) -> object:
        """Return the transaction domain used by this fake outbox."""

        return self._persistence_domain

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the immutable authority used by the fake outbox."""

        return self._effect_contract_authority

    def bind_effect_contract_authority(
        self,
        authority: EffectContractAuthority,
    ) -> None:
        """Bind the composition authority before the fake store is observed."""

        self._effect_contract_authority = authority

    def bind_persistence_domain(self, domain: object) -> None:
        """Bind the transaction domain before the fake store is observed."""

        self._persistence_domain = domain

    async def claim_next(
        self,
        *,
        worker_id: str,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
    ) -> ClaimedEffect | None:
        del worker_id, effect_contracts, excluded_effect_contracts
        return None

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        raise AssertionError(f"unexpected effect lease renewal: {claim.effect.effect_id}")

    async def complete_with_event(
        self,
        claim: ClaimedEffect,
        completion_envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
    ) -> EffectSettlementResult:
        del outcome_fence_fields
        raise AssertionError(
            f"unexpected effect completion: {claim.effect.effect_id}:{completion_envelope.event_id}"
        )

    async def release_for_retry(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        available_at: float,
    ) -> None:
        raise AssertionError(
            f"unexpected effect retry: {claim.effect.effect_id}:{error}:{available_at}"
        )

    async def fail_with_event(
        self,
        claim: ClaimedEffect,
        failure_envelope: SessionEventEnvelope,
        *,
        error: str,
        outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
    ) -> EffectSettlementResult:
        del outcome_fence_fields
        raise AssertionError(
            f"unexpected effect failure: {claim.effect.effect_id}:"
            f"{failure_envelope.event_id}:{error}"
        )

    async def release(self, claim: ClaimedEffect, *, error: str) -> None:
        raise AssertionError(f"unexpected effect release: {claim.effect.effect_id}:{error}")

    async def recover_expired(self, *, worker_id: str) -> int:
        del worker_id
        self.recover_expired_calls += 1
        return 0

    async def next_available_at(
        self,
        *,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
    ) -> float | None:
        del effect_contracts, excluded_effect_contracts
        return None


class _ControlledCleanSessionExecutor:
    """Lifecycle double for clean-session startup and cancellation paths."""

    def __init__(
        self,
        handlers: EffectHandlerRegistry,
        session_registry: AgentSessionActorRegistry,
        *,
        starts_workers: bool,
        startup_error: Exception | None = None,
        authority_supplier: Callable[[], EffectContractAuthority] | None = None,
        on_start: Callable[[], None] | None = None,
    ) -> None:
        self.handler_registry = handlers
        self.session_registry = session_registry
        self.persistence_domain = session_registry.persistence_domain
        self._authority_supplier = authority_supplier or (lambda: handlers.effect_contract_authority)
        self._starts_workers = starts_workers
        self._startup_error = startup_error
        self._on_start = on_start
        self.healthy = True
        self.started = False
        self.closed = False
        self.start_calls = 0
        self.shutdown_calls = 0
        self.startup_started = asyncio.Event()
        self.continue_startup = asyncio.Event()
        self.continue_startup.set()
        self.shutdown_started = asyncio.Event()
        self.continue_shutdown = asyncio.Event()
        self.continue_shutdown.set()

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Resolve the authority at each harness validation boundary."""

        return self._authority_supplier()

    @property
    def running(self) -> bool:
        """Report whether the double claims live handler-bound workers."""

        return self.started

    async def start_clean_session(self) -> int:
        """Apply the configured startup outcome after an async boundary."""

        self.start_calls += 1
        self.startup_started.set()
        await self.continue_startup.wait()
        await asyncio.sleep(0)
        if self._on_start is not None:
            self._on_start()
        if self._startup_error is not None:
            raise self._startup_error
        self.started = self._starts_workers
        return 0

    async def shutdown(self, *, drain: bool = False) -> None:
        """Record cleanup and optionally pause it for cancellation tests."""

        del drain
        self.shutdown_calls += 1
        self.shutdown_started.set()
        await self.continue_shutdown.wait()
        self.started = False
        self.closed = True


class _StaticCleanSessionPreflight:
    """Read-only clean-domain proof double with observable invocation order."""

    def __init__(
        self,
        persistence_domain: object,
        readiness: CleanSessionActivationReadiness | None = None,
        on_check: Callable[[], None] | None = None,
    ) -> None:
        self._persistence_domain = persistence_domain
        self.readiness = readiness or CleanSessionActivationReadiness()
        self._on_check = on_check
        self.check_calls = 0

    @property
    def persistence_domain(self) -> object:
        """Return the exact domain this fake claims to have inspected."""

        return self._persistence_domain

    async def check(self) -> CleanSessionActivationReadiness:
        """Return the configured immutable readiness result."""

        self.check_calls += 1
        if self._on_check is not None:
            await asyncio.sleep(0)
            self._on_check()
        return self.readiness


async def _effect_handler(_context: EffectExecutionContext) -> EffectHandlerResult:
    return EffectHandlerResult()


def _sync_effect_handler(_context: EffectExecutionContext) -> EffectHandlerResult:
    return EffectHandlerResult()


def _contract(*, timeout_seconds: float = 5.0) -> EffectExecutionContract:
    return EffectExecutionContract(
        effect_kind="required_effect",
        version=1,
        lane=EffectLane.DEFAULT,
        completion_event_kind="RequiredEffectCompleted",
        timeout_seconds=timeout_seconds,
        max_attempts=1,
        retry_base_seconds=0.0,
        retry_max_seconds=0.0,
    )


def _full_handlers(
    *,
    omitted_handler_ref: tuple[str, int] | None = None,
    replacement_contract: EffectExecutionContract | None = None,
) -> EffectHandlerRegistry:
    """Build a full actor-v2 handler graph using inert handlers."""

    authority_contracts = tuple(
        (
            replacement_contract
            if replacement_contract is not None and replacement_contract.ref == expected.ref
            else expected
        )
        for expected in _FULL_CONTRACTS
    )
    authority = (
        builtin_effect_contract_authority()
        if replacement_contract is None
        else EffectContractAuthority(authority_contracts)
    )
    handlers = EffectHandlerRegistry(contract_authority=authority)
    for expected in _FULL_CONTRACTS:
        if expected.ref == omitted_handler_ref:
            continue
        registered = (
            replacement_contract
            if replacement_contract is not None and replacement_contract.ref == expected.ref
            else expected
        )
        handlers.register(
            registered.effect_kind,
            _effect_handler,
            contract=registered,
        )
    return handlers


def _clean_session_handlers(
    *,
    omitted_handler_ref: tuple[str, int] | None = None,
) -> EffectHandlerRegistry:
    """Build the current clean-session graph against complete authority."""

    handlers = EffectHandlerRegistry(contract_authority=builtin_effect_contract_authority())
    for contract in _CLEAN_SESSION_CONTRACTS:
        if contract.ref == omitted_handler_ref:
            continue
        handlers.register(
            contract.effect_kind,
            _effect_handler,
            contract=contract,
        )
    return handlers


def _components(
    handlers: EffectHandlerRegistry,
    *,
    session_store: _EmptySessionStore | None = None,
    effect_store: _EmptyEffectStore | None = None,
) -> tuple[_EmptySessionStore, _EmptyEffectStore, AgentSessionActorRegistry, DurableEffectExecutor]:
    authority = handlers.effect_contract_authority
    persistence_domain = object()
    resolved_session_store = session_store or _EmptySessionStore(
        authority,
        persistence_domain,
    )
    resolved_session_store.bind_effect_contract_authority(authority)
    resolved_session_store.bind_persistence_domain(persistence_domain)

    def reduce_unexpected_event(*_args: object) -> SessionTransition:
        raise AssertionError("unexpected actor event reduction")

    registry = AgentSessionActorRegistry(
        store=resolved_session_store,
        handler=reduce_unexpected_event,
    )
    resolved_effect_store = effect_store or _EmptyEffectStore(
        authority,
        persistence_domain,
    )
    resolved_effect_store.bind_effect_contract_authority(authority)
    resolved_effect_store.bind_persistence_domain(persistence_domain)
    executor = DurableEffectExecutor(
        store=resolved_effect_store,
        handlers=handlers,
        session_registry=registry,
        poll_interval_seconds=60.0,
        renew_interval_seconds=None,
    )
    return resolved_session_store, resolved_effect_store, registry, executor


def _partial_harness(
    handlers: EffectHandlerRegistry,
    required: EffectExecutionContract,
) -> tuple[ActorRuntimeHarness, _EmptySessionStore, _EmptyEffectStore, DurableEffectExecutor]:
    session_store, effect_store, registry, executor = _components(handlers)
    return (
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            required_effect_contracts=(required,),
            allow_partial_contracts=True,
        ),
        session_store,
        effect_store,
        executor,
    )


def _full_harness(
    handlers: EffectHandlerRegistry,
    *,
    session_store: _EmptySessionStore | None = None,
    effect_store: _EmptyEffectStore | None = None,
) -> tuple[
    ActorRuntimeHarness,
    _EmptySessionStore,
    _EmptyEffectStore,
    AgentSessionActorRegistry,
    DurableEffectExecutor,
]:
    resolved_session_store, resolved_effect_store, registry, executor = _components(
        handlers,
        session_store=session_store,
        effect_store=effect_store,
    )
    return (
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
        ),
        resolved_session_store,
        resolved_effect_store,
        registry,
        executor,
    )


def _clean_session_harness(
    handlers: EffectHandlerRegistry,
    *,
    readiness: CleanSessionActivationReadiness | None = None,
    session_store: _EmptySessionStore | None = None,
    effect_store: _EmptyEffectStore | None = None,
    preflight_on_check: Callable[[], None] | None = None,
) -> tuple[
    ActorRuntimeHarness,
    _StaticCleanSessionPreflight,
    _EmptySessionStore,
    _EmptyEffectStore,
    AgentSessionActorRegistry,
    DurableEffectExecutor,
]:
    """Compose an executable clean-session harness over inert stores."""

    session_store, effect_store, registry, executor = _components(
        handlers,
        session_store=session_store,
        effect_store=effect_store,
    )
    preflight = _StaticCleanSessionPreflight(
        registry.persistence_domain,
        readiness,
        preflight_on_check,
    )
    return (
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
            clean_session_preflight=preflight,
        ),
        preflight,
        session_store,
        effect_store,
        registry,
        executor,
    )


def test_harness_construction_is_inactive_and_does_not_expose_wake_targets() -> None:
    required = _contract()
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(required.effect_kind, _effect_handler, contract=required)
    harness, session_store, effect_store, executor = _partial_harness(handlers, required)

    assert harness.active is False
    assert harness.closed is False
    assert harness.required_effect_contracts == (required,)
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    assert not hasattr(harness, "actor_wake_target")
    assert not hasattr(harness, "session_actor_registry")


def test_default_handler_registry_uses_the_builtin_composition_authority() -> None:
    handlers = EffectHandlerRegistry()

    assert handlers.effect_contract_authority is builtin_effect_contract_authority()
    assert handlers.effect_contract_authority.contracts() == tuple(
        sorted(
            _FULL_CONTRACTS,
            key=lambda contract: (contract.effect_kind, contract.version),
        )
    )


@pytest.mark.asyncio
async def test_partial_harness_cannot_activate_or_start_any_worker() -> None:
    required = _contract()
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(required.effect_kind, _effect_handler, contract=required)
    harness, session_store, effect_store, executor = _partial_harness(handlers, required)

    with pytest.raises(RuntimeError, match="non-activatable"):
        await harness.activate()

    assert harness.active is False
    assert harness.closed is False
    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_activation_rejects_missing_full_graph_handler_without_starting_work() -> None:
    missing = _FULL_CONTRACTS[0]
    handlers = _full_handlers(omitted_handler_ref=missing.ref)
    harness, session_store, effect_store, _registry, executor = _full_harness(handlers)

    with pytest.raises(ActorRuntimeHarnessActivationError) as raised:
        await harness.activate()

    assert any(
        failure.contract == missing and "handler" in failure.reason
        for failure in raised.value.failures
    )
    assert handlers.sealed is False
    assert harness.active is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


def test_construction_rejects_changed_full_graph_contract_without_starting_work() -> None:
    required = _FULL_CONTRACTS[0]
    replacement = replace(required, timeout_seconds=required.timeout_seconds + 1.0)
    handlers = _full_handlers(replacement_contract=replacement)
    session_store, effect_store, registry, executor = _components(handlers)

    with pytest.raises(ValueError, match="does not exactly match"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
        )

    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0


def test_construction_rejects_extra_registered_contract_without_starting_work() -> None:
    extra = _contract()
    authority = EffectContractAuthority((*_FULL_CONTRACTS, extra))
    handlers = EffectHandlerRegistry(contract_authority=authority)
    for contract in (*_FULL_CONTRACTS, extra):
        handlers.register(contract.effect_kind, _effect_handler, contract=contract)
    session_store, effect_store, registry, executor = _components(handlers)

    with pytest.raises(ValueError, match="does not exactly match"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
        )

    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0


@pytest.mark.asyncio
async def test_activation_rejects_sync_handler_without_starting_work() -> None:
    required = _FULL_CONTRACTS[0]
    handlers = _full_handlers()
    handlers.register(
        required.effect_kind,
        _sync_effect_handler,
        contract=required,
        replace_existing=True,
    )
    harness, session_store, effect_store, _registry, executor = _full_harness(handlers)

    with pytest.raises(ActorRuntimeHarnessActivationError) as raised:
        await harness.activate()

    assert any(
        failure.contract == required
        and failure.reason == "registered handler is not async-callable"
        for failure in raised.value.failures
    )
    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_clean_session_activation_seals_the_exact_handler_graph_and_is_idempotent() -> None:
    handlers = _clean_session_handlers()
    harness, preflight, session_store, effect_store, _registry, executor = _clean_session_harness(
        handlers
    )

    try:
        await harness.activate()
        await harness.activate()

        assert harness.active is True
        assert executor.started is True
        assert handlers.sealed is True
        assert preflight.check_calls == 1
        assert session_store.pending_keys_calls == 0
        assert effect_store.recover_expired_calls == 0
        with pytest.raises(RuntimeError, match="sealed"):
            handlers.register_contract(_contract())
        with pytest.raises(RuntimeError, match="sealed"):
            handlers.register(
                _CLEAN_SESSION_CONTRACTS[0].effect_kind,
                _effect_handler,
                replace_existing=True,
            )
    finally:
        await harness.shutdown()

    assert harness.active is False
    assert harness.closed is True
    assert executor.started is False


@pytest.mark.asyncio
async def test_complete_history_activation_requires_a_lifecycle_owning_controller() -> None:
    """A bare permit cannot make long-lived historical recovery safe."""

    handlers = _full_handlers()
    harness, session_store, effect_store, registry, executor = _full_harness(handlers)

    with pytest.raises(ActorRuntimeHistoryRecoveryPermitRequired):
        await harness.activate()

    assert handlers.sealed is False
    assert harness.active is False
    assert harness.closed is False
    assert registry.accepting is True
    assert executor.running is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


def test_clean_session_scope_requires_preflight_for_the_shared_domain() -> None:
    """A clean graph cannot validate one database and execute another."""

    handlers = _clean_session_handlers()
    session_store, effect_store, registry, executor = _components(handlers)
    del session_store, effect_store

    with pytest.raises(ValueError, match="requires a durable empty-domain preflight"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
        )

    with pytest.raises(ValueError, match="must inspect the shared persistence domain"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
            clean_session_preflight=_StaticCleanSessionPreflight(object()),
        )

    assert handlers.sealed is False
    assert executor.started is False


@pytest.mark.asyncio
async def test_clean_session_scope_activates_only_current_actor_native_contracts() -> None:
    """A proven empty domain may omit the quarantined historical contracts."""

    handlers = _clean_session_handlers()
    harness, preflight, session_store, effect_store, _registry, executor = _clean_session_harness(
        handlers
    )

    try:
        assert len(_CLEAN_SESSION_CONTRACTS) == 23
        assert len(_FULL_CONTRACTS) - len(_CLEAN_SESSION_CONTRACTS) == 13
        assert {contract.ref for contract in _FULL_CONTRACTS} - {
            contract.ref for contract in _CLEAN_SESSION_CONTRACTS
        } == (ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS)
        assert harness.activation_scope is ActorRuntimeActivationScope.CLEAN_SESSION
        assert harness.required_effect_contracts == _CLEAN_SESSION_CONTRACTS
        assert harness.required_handler_failures() == ()
        assert harness.clean_session_handler_failures() == ()
        historical_failures = harness.handler_failures_for(
            contract
            for contract in _FULL_CONTRACTS
            if contract.ref in ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS
        )
        assert {failure.contract.ref for failure in historical_failures} == (
            ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS
        )
        assert all(
            failure.reason.startswith("no durable effect handler is registered")
            for failure in historical_failures
        )

        await harness.activate()

        assert preflight.check_calls == 1
        assert harness.active is True
        assert handlers.sealed is True
        assert executor.started is True
        assert session_store.pending_keys_calls == 0
        assert effect_store.recover_expired_calls == 0
    finally:
        await harness.shutdown()


@pytest.mark.asyncio
async def test_clean_session_preflight_blocks_before_recovery_worker_start_or_seal() -> None:
    """A residual-state rejection must leave the inactive graph untouched."""

    blocked = CleanSessionActivationReadiness(
        blockers=(
            CleanSessionActivationBlocker(
                code="actor_v2_ownership_history_present",
                count=1,
            ),
        )
    )
    handlers = _clean_session_handlers()
    harness, preflight, session_store, effect_store, _registry, executor = _clean_session_harness(
        handlers, readiness=blocked
    )

    with pytest.raises(ActorRuntimeCleanSessionPreflightError) as raised:
        await harness.activate()

    assert raised.value.readiness is blocked
    assert preflight.check_calls == 1
    assert handlers.sealed is False
    assert harness.active is False
    assert harness.closed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_clean_session_scope_rejects_a_bound_historical_handler_before_startup() -> None:
    """A clean activation may decode history but must never run it."""

    historical_contract = next(
        contract
        for contract in _FULL_CONTRACTS
        if contract.ref in ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS
    )
    handlers = _clean_session_handlers()
    handlers.register(
        historical_contract.effect_kind,
        _effect_handler,
        contract=historical_contract,
    )
    harness, preflight, session_store, effect_store, _registry, executor = _clean_session_harness(
        handlers
    )

    with pytest.raises(ActorRuntimeHarnessActivationError) as raised:
        await harness.activate()

    assert any(
        failure.contract == historical_contract
        and failure.reason == "registered handler is outside the activation graph"
        for failure in raised.value.failures
    )
    assert preflight.check_calls == 1
    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_clean_session_preflight_domain_drift_blocks_before_its_check() -> None:
    """A mutable proof cannot switch databases between composition and start."""

    handlers = _clean_session_handlers()
    harness, preflight, session_store, effect_store, _registry, executor = _clean_session_harness(
        handlers
    )
    preflight._persistence_domain = object()

    with pytest.raises(RuntimeError, match="preflight persistence domain changed"):
        await harness.activate()

    assert preflight.check_calls == 0
    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_default_scope_still_requires_the_complete_historical_contract_graph() -> None:
    """The new clean scope cannot silently weaken the existing default gate."""

    handlers = _clean_session_handlers()
    harness, session_store, effect_store, _registry, executor = _full_harness(handlers)
    historical_contract = next(
        contract
        for contract in _FULL_CONTRACTS
        if contract.ref in ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS
    )

    assert harness.activation_scope is ActorRuntimeActivationScope.COMPLETE_HISTORY
    assert harness.required_effect_contracts == _FULL_CONTRACTS
    with pytest.raises(ActorRuntimeHarnessActivationError) as raised:
        await harness.activate()

    assert any(
        failure.contract == historical_contract
        and failure.reason.startswith("no durable effect handler is registered")
        for failure in raised.value.failures
    )
    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_post_activation_binding_drift_marks_harness_unhealthy() -> None:
    handlers = _clean_session_handlers()
    harness, _preflight, _session_store, effect_store, _registry, executor = _clean_session_harness(
        handlers
    )

    try:
        await harness.activate()
        assert harness.active is True
        assert executor.healthy is True

        effect_store.bind_effect_contract_authority(
            EffectContractAuthority(handlers.effect_contract_authority.contracts())
        )
        executor.wake()
        for _attempt in range(100):
            if not executor.healthy:
                break
            await asyncio.sleep(0)

        assert executor.healthy is False
        assert executor.binding_failure is not None
        assert executor.started is False
        assert executor.running is False
        assert harness.active is False
    finally:
        await harness.shutdown()


def test_full_harness_rejects_incomplete_or_extra_required_contract_definitions() -> None:
    handlers = _full_handlers()
    session_store, effect_store, registry, executor = _components(handlers)
    del session_store, effect_store

    with pytest.raises(ValueError, match="exact complete"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            required_effect_contracts=_FULL_CONTRACTS[:-1],
        )

    with pytest.raises(ValueError, match="unexpected="):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            required_effect_contracts=(*_FULL_CONTRACTS, _contract()),
        )


def test_partial_harness_allows_isolated_contract_shape_checks() -> None:
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    session_store, effect_store, registry, executor = _components(handlers)
    del session_store, effect_store

    with pytest.raises(ValueError, match="conflicting policies"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
            required_effect_contracts=(
                _contract(timeout_seconds=5.0),
                _contract(timeout_seconds=10.0),
            ),
            allow_partial_contracts=True,
        )


def test_harness_rejects_a_handler_registry_not_owned_by_its_executor() -> None:
    executor_handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    expected_handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    session_store, effect_store, registry, executor = _components(executor_handlers)
    del session_store, effect_store

    with pytest.raises(ValueError, match="executor's handler registry"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=expected_handlers,
            allow_partial_contracts=True,
        )


def test_harness_rejects_an_executor_bound_to_another_wake_target() -> None:
    handlers = _full_handlers()
    session_store = _EmptySessionStore(handlers.effect_contract_authority)

    def reduce_unexpected_event(*_args: object) -> SessionTransition:
        raise AssertionError("unexpected actor event reduction")

    registry = AgentSessionActorRegistry(
        store=session_store,
        handler=reduce_unexpected_event,
    )
    wrong_registry = AgentSessionActorRegistry(
        store=session_store,
        handler=reduce_unexpected_event,
    )
    effect_store = _EmptyEffectStore(handlers.effect_contract_authority)
    executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        session_registry=wrong_registry,
        poll_interval_seconds=60.0,
        renew_interval_seconds=None,
    )

    with pytest.raises(ValueError, match="executor's actor registry"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
        )

    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    assert executor.running is False


def test_harness_rejects_split_actor_and_effect_store_authorities() -> None:
    """Equal-looking policy graphs are unsafe when they are separate roots."""

    handlers = _full_handlers()
    actor_authority = handlers.effect_contract_authority
    effect_authority = EffectContractAuthority(_FULL_CONTRACTS)
    assert effect_authority.contracts() == actor_authority.contracts()
    assert effect_authority is not actor_authority
    persistence_domain = object()
    session_store = _EmptySessionStore(actor_authority, persistence_domain)

    def reduce_unexpected_event(*_args: object) -> SessionTransition:
        raise AssertionError("unexpected actor event reduction")

    registry = AgentSessionActorRegistry(
        store=session_store,
        handler=reduce_unexpected_event,
    )
    effect_store = _EmptyEffectStore(effect_authority, persistence_domain)
    executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        session_registry=registry,
        poll_interval_seconds=60.0,
        renew_interval_seconds=None,
    )

    with pytest.raises(ValueError, match="authority graph is split"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
        )

    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    assert executor.running is False


def test_harness_rejects_split_persistence_domains() -> None:
    """Actor commits and effect claims must address the same database domain."""

    handlers = _full_handlers()
    session_store = _EmptySessionStore(
        handlers.effect_contract_authority,
        object(),
    )

    def reduce_unexpected_event(*_args: object) -> SessionTransition:
        raise AssertionError("unexpected actor event reduction")

    registry = AgentSessionActorRegistry(
        store=session_store,
        handler=reduce_unexpected_event,
    )
    effect_store = _EmptyEffectStore(
        handlers.effect_contract_authority,
        object(),
    )
    executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        session_registry=registry,
        poll_interval_seconds=60.0,
        renew_interval_seconds=None,
    )

    with pytest.raises(ValueError, match="shared persistence domain"):
        ActorRuntimeHarness(
            registry=registry,
            effect_executor=executor,
            handlers=handlers,
        )

    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    assert executor.running is False


@pytest.mark.asyncio
async def test_activation_rechecks_authority_identity_before_recovery() -> None:
    """Registration after composition cannot silently replace the graph root."""

    handlers = EffectHandlerRegistry(
        contracts=_FULL_CONTRACTS,
        include_builtin_contracts=False,
    )
    for contract in _FULL_CONTRACTS:
        handlers.register(contract.effect_kind, _effect_handler, contract=contract)
    harness, session_store, effect_store, _registry, executor = _full_harness(handlers)

    handlers.register(
        _contract().effect_kind,
        _effect_handler,
        contract=_contract(),
    )

    with pytest.raises(ValueError, match="authority graph is split"):
        await harness.activate()

    assert handlers.sealed is False
    assert executor.running is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.parametrize("component", ("actor_store", "effect_store"))
@pytest.mark.asyncio
async def test_activation_rejects_store_authority_rebinding(
    component: str,
) -> None:
    """A store cannot swap an equal policy snapshot after composition."""

    handlers = _full_handlers()
    harness, session_store, effect_store, _registry, executor = _full_harness(handlers)
    rebound = EffectContractAuthority(_FULL_CONTRACTS)
    assert rebound.contracts() == handlers.effect_contract_authority.contracts()
    assert rebound is not handlers.effect_contract_authority
    if component == "actor_store":
        session_store.bind_effect_contract_authority(rebound)
    else:
        effect_store.bind_effect_contract_authority(rebound)

    with pytest.raises(RuntimeError, match="changed.*authority"):
        await harness.activate()

    assert executor.running is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_clean_session_activation_rechecks_actor_authority_after_preflight() -> None:
    """An async clean-session proof cannot hide actor-store rebinding."""

    handlers = _clean_session_handlers()
    session_store = _EmptySessionStore()

    def switch_actor_authority() -> None:
        session_store.bind_effect_contract_authority(
            EffectContractAuthority(session_store.effect_contract_authority.contracts())
        )

    harness, preflight, resolved_session_store, resolved_effect_store, registry, executor = (
        _clean_session_harness(
            handlers,
            session_store=session_store,
            preflight_on_check=switch_actor_authority,
        )
    )

    with pytest.raises(RuntimeError, match="changed effect authority"):
        await harness.activate()

    assert preflight.check_calls == 1
    assert harness.active is False
    assert harness.closed is False
    assert registry.accepting is True
    assert executor.running is False
    assert resolved_session_store.pending_keys_calls == 0
    assert resolved_effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_clean_session_activation_rechecks_effect_authority_after_start() -> None:
    """An awaited executor start cannot hide effect-store rebinding."""

    handlers = _clean_session_handlers()
    session_store, effect_store, registry, _unused_executor = _components(handlers)

    def switch_effect_authority() -> None:
        effect_store.bind_effect_contract_authority(
            EffectContractAuthority(effect_store.effect_contract_authority.contracts())
        )

    executor = _ControlledCleanSessionExecutor(
        handlers,
        registry,
        starts_workers=True,
        authority_supplier=lambda: effect_store.effect_contract_authority,
        on_start=switch_effect_authority,
    )
    preflight = _StaticCleanSessionPreflight(registry.persistence_domain)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
        activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
        clean_session_preflight=preflight,
    )

    with pytest.raises(ValueError, match="authority graph is split"):
        await harness.activate()

    assert preflight.check_calls == 1
    assert harness.active is False
    assert harness.closed is True
    assert registry.accepting is False
    assert executor.running is False
    assert executor.start_calls == 1
    assert executor.shutdown_calls == 1
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0


@pytest.mark.asyncio
async def test_harness_rejects_an_orphan_only_executor_that_is_already_running() -> None:
    """An orphan worker is still live work and cannot be adopted as inactive."""

    required = _contract()
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    session_store, effect_store, registry, executor = _components(handlers)
    del session_store, effect_store
    await executor.start()
    try:
        assert executor.running is True
        assert executor.started is False
        with pytest.raises(ValueError, match="inactive effect executor"):
            ActorRuntimeHarness(
                registry=registry,
                effect_executor=executor,
                handlers=handlers,
                required_effect_contracts=(required,),
                allow_partial_contracts=True,
            )
    finally:
        await executor.shutdown()


@pytest.mark.asyncio
async def test_activation_rejects_executor_without_handler_bound_workers() -> None:
    """A clean contract graph must still produce live handler workers."""

    handlers = _clean_session_handlers()
    session_store, effect_store, registry, _unused_executor = _components(handlers)
    executor = _ControlledCleanSessionExecutor(
        handlers,
        registry,
        starts_workers=False,
    )
    preflight = _StaticCleanSessionPreflight(registry.persistence_domain)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
        activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
        clean_session_preflight=preflight,
    )

    with pytest.raises(RuntimeError, match="did not start handler-bound workers"):
        await harness.activate()

    assert preflight.check_calls == 1
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    assert executor.start_calls == 1
    assert executor.shutdown_calls == 1
    assert harness.active is False
    assert harness.closed is True
    assert registry.accepting is False


@pytest.mark.asyncio
async def test_clean_session_startup_failure_closes_a_partially_started_harness() -> None:
    handlers = _clean_session_handlers()
    session_store, effect_store, registry, _unused_executor = _components(handlers)
    executor = _ControlledCleanSessionExecutor(
        handlers,
        registry,
        starts_workers=True,
        startup_error=RuntimeError("effect startup failed"),
    )
    preflight = _StaticCleanSessionPreflight(registry.persistence_domain)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
        activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
        clean_session_preflight=preflight,
    )

    with pytest.raises(RuntimeError, match="effect startup failed"):
        await harness.activate()

    assert preflight.check_calls == 1
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    assert handlers.sealed is True
    assert harness.active is False
    assert harness.closed is True
    assert executor.closed is True
    assert registry.accepting is False


@pytest.mark.asyncio
async def test_cancelling_clean_session_start_closes_both_runtime_halves() -> None:
    handlers = _clean_session_handlers()
    _session_store, _effect_store, registry, _unused_executor = _components(handlers)
    executor = _ControlledCleanSessionExecutor(
        handlers,
        registry,
        starts_workers=True,
    )
    executor.continue_startup.clear()
    preflight = _StaticCleanSessionPreflight(registry.persistence_domain)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
        activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
        clean_session_preflight=preflight,
    )

    activation = asyncio.create_task(harness.activate())
    await asyncio.wait_for(executor.startup_started.wait(), timeout=1.0)
    activation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await activation

    assert handlers.sealed is True
    assert harness.active is False
    assert harness.closed is True
    assert executor.closed is True
    assert registry.accepting is False


@pytest.mark.asyncio
async def test_cancelling_clean_session_start_closes_both_runtime_halves_after_yield() -> None:
    handlers = _clean_session_handlers()
    _session_store, _effect_store, registry, _unused_executor = _components(handlers)
    executor = _ControlledCleanSessionExecutor(
        handlers,
        registry,
        starts_workers=True,
    )
    executor.continue_startup.clear()
    preflight = _StaticCleanSessionPreflight(registry.persistence_domain)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
        activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
        clean_session_preflight=preflight,
    )

    activation = asyncio.create_task(harness.activate())
    await asyncio.wait_for(executor.startup_started.wait(), timeout=1.0)
    await asyncio.sleep(0)
    activation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await activation

    assert handlers.sealed is True
    assert harness.active is False
    assert harness.closed is True
    assert executor.closed is True
    assert registry.accepting is False


@pytest.mark.asyncio
async def test_repeated_cancellation_does_not_interrupt_activation_cleanup() -> None:
    handlers = _clean_session_handlers()
    _session_store, _effect_store, registry, _unused_executor = _components(handlers)
    executor = _ControlledCleanSessionExecutor(
        handlers,
        registry,
        starts_workers=True,
    )
    executor.continue_startup.clear()
    executor.continue_shutdown.clear()
    preflight = _StaticCleanSessionPreflight(registry.persistence_domain)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
        activation_scope=ActorRuntimeActivationScope.CLEAN_SESSION,
        clean_session_preflight=preflight,
    )

    activation = asyncio.create_task(harness.activate())
    await asyncio.wait_for(executor.startup_started.wait(), timeout=1.0)
    activation.cancel()
    await asyncio.wait_for(executor.shutdown_started.wait(), timeout=1.0)
    activation.cancel()
    executor.continue_shutdown.set()

    with pytest.raises(asyncio.CancelledError):
        await activation

    assert harness.active is False
    assert harness.closed is True
    assert executor.closed is True
    assert registry.accepting is False
