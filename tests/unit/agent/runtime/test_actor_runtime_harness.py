from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from shinbot.agent.runtime.session_actor.effect_contracts import (
    DEFAULT_OUTCOME_FENCE_FIELDS,
    EffectExecutionContract,
    EffectLane,
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
    ActorRuntimeHarness,
    ActorRuntimeHarnessActivationError,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry

_FULL_CONTRACTS = (
    *builtin_session_actor_effect_contracts(),
    *builtin_external_action_effect_contracts(),
)


class _EmptySessionStore:
    """Session-store stub that records whether activation attempts recovery."""

    def __init__(self) -> None:
        self.pending_keys_calls = 0

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


class _BlockingRecoverySessionStore(_EmptySessionStore):
    """Session store that pauses harness activation inside registry recovery."""

    def __init__(self) -> None:
        super().__init__()
        self.recovery_started = asyncio.Event()
        self.continue_recovery = asyncio.Event()

    async def pending_keys(self) -> list:
        self.pending_keys_calls += 1
        self.recovery_started.set()
        await self.continue_recovery.wait()
        return []


class _EmptyEffectStore:
    """Effect-store stub that keeps executor workers idle without I/O."""

    def __init__(self) -> None:
        self.recover_expired_calls = 0

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
            f"unexpected effect completion: {claim.effect.effect_id}:"
            f"{completion_envelope.event_id}"
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


class _FailingStartupEffectStore(_EmptyEffectStore):
    """Effect-store fake that fails before executor workers can start."""

    async def recover_expired(self, *, worker_id: str) -> int:
        del worker_id
        self.recover_expired_calls += 1
        raise RuntimeError("effect recovery failed")


class _BlockingStartupEffectStore(_EmptyEffectStore):
    """Effect store that pauses harness activation inside executor startup."""

    def __init__(self) -> None:
        super().__init__()
        self.startup_started = asyncio.Event()
        self.continue_startup = asyncio.Event()

    async def recover_expired(self, *, worker_id: str) -> int:
        del worker_id
        self.recover_expired_calls += 1
        self.startup_started.set()
        await self.continue_startup.wait()
        return 0


class _BlockingShutdownExecutor:
    """Executor double that pauses cleanup to exercise repeated cancellation."""

    def __init__(self, handlers: EffectHandlerRegistry) -> None:
        self.handler_registry = handlers
        self.started = False
        self.closed = False
        self.shutdown_started = asyncio.Event()
        self.continue_shutdown = asyncio.Event()

    @property
    def running(self) -> bool:
        """Return whether the test double has entered its started state."""

        return self.started

    async def start(self) -> int:
        self.started = True
        return 0

    async def shutdown(self, *, drain: bool = False) -> None:
        del drain
        self.shutdown_started.set()
        await self.continue_shutdown.wait()
        self.started = False
        self.closed = True


class _UnhealthyStartupExecutor:
    """Executor double that starts no handler-bound worker."""

    def __init__(self, handlers: EffectHandlerRegistry) -> None:
        self.handler_registry = handlers
        self.started = False
        self.closed = False
        self.start_calls = 0
        self.shutdown_calls = 0

    @property
    def running(self) -> bool:
        """Report no live workers before activation starts."""

        return False

    async def start(self) -> int:
        """Pretend startup completed without creating runnable workers."""

        self.start_calls += 1
        return 0

    async def shutdown(self, *, drain: bool = False) -> None:
        """Record the cleanup requested by the failed activation."""

        del drain
        self.shutdown_calls += 1
        self.closed = True


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

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    for expected in _FULL_CONTRACTS:
        registered = (
            replacement_contract
            if replacement_contract is not None
            and replacement_contract.ref == expected.ref
            else expected
        )
        handlers.register_contract(registered)
    for expected in _FULL_CONTRACTS:
        if expected.ref == omitted_handler_ref:
            continue
        registered = (
            replacement_contract
            if replacement_contract is not None
            and replacement_contract.ref == expected.ref
            else expected
        )
        handlers.register(
            registered.effect_kind,
            _effect_handler,
            contract=registered,
        )
    return handlers


def _components(
    handlers: EffectHandlerRegistry,
    *,
    session_store: _EmptySessionStore | None = None,
    effect_store: _EmptyEffectStore | None = None,
) -> tuple[_EmptySessionStore, _EmptyEffectStore, AgentSessionActorRegistry, DurableEffectExecutor]:
    resolved_session_store = session_store or _EmptySessionStore()

    def reduce_unexpected_event(*_args: object) -> SessionTransition:
        raise AssertionError("unexpected actor event reduction")

    registry = AgentSessionActorRegistry(
        store=resolved_session_store,
        handler=reduce_unexpected_event,
    )
    resolved_effect_store = effect_store or _EmptyEffectStore()
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
) -> tuple[ActorRuntimeHarness, _EmptySessionStore, _EmptyEffectStore, AgentSessionActorRegistry, DurableEffectExecutor]:
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


@pytest.mark.asyncio
async def test_activation_rejects_changed_full_graph_contract_without_starting_work() -> None:
    required = _FULL_CONTRACTS[0]
    replacement = replace(required, timeout_seconds=required.timeout_seconds + 1.0)
    handlers = _full_handlers(replacement_contract=replacement)
    harness, session_store, effect_store, _registry, executor = _full_harness(handlers)

    with pytest.raises(ActorRuntimeHarnessActivationError) as raised:
        await harness.activate()

    assert any(
        failure.contract == required
        and failure.reason == "registered contract does not match required contract"
        for failure in raised.value.failures
    )
    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


@pytest.mark.asyncio
async def test_activation_rejects_extra_registered_contract_without_starting_work() -> None:
    extra = _contract()
    handlers = _full_handlers()
    handlers.register(extra.effect_kind, _effect_handler, contract=extra)
    harness, session_store, effect_store, _registry, executor = _full_harness(handlers)

    with pytest.raises(ActorRuntimeHarnessActivationError) as raised:
        await harness.activate()

    assert any(
        failure.contract == extra
        and failure.reason == "registered contract is outside the activation graph"
        for failure in raised.value.failures
    )
    assert handlers.sealed is False
    assert executor.started is False
    assert session_store.pending_keys_calls == 0
    assert effect_store.recover_expired_calls == 0
    await harness.shutdown()


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
async def test_full_activation_seals_the_exact_handler_graph_and_is_idempotent() -> None:
    handlers = _full_handlers()
    harness, session_store, effect_store, _registry, executor = _full_harness(handlers)

    try:
        await harness.activate()
        await harness.activate()

        assert harness.active is True
        assert executor.started is True
        assert handlers.sealed is True
        assert session_store.pending_keys_calls == 1
        assert effect_store.recover_expired_calls == 1
        with pytest.raises(RuntimeError, match="sealed"):
            handlers.register_contract(_contract())
        with pytest.raises(RuntimeError, match="sealed"):
            handlers.register(
                _FULL_CONTRACTS[0].effect_kind,
                _effect_handler,
                replace_existing=True,
            )
    finally:
        await harness.shutdown()

    assert harness.active is False
    assert harness.closed is True
    assert executor.started is False


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
    """A complete contract graph must still produce live handler workers."""

    handlers = _full_handlers()
    session_store = _EmptySessionStore()

    def reduce_unexpected_event(*_args: object) -> SessionTransition:
        raise AssertionError("unexpected actor event reduction")

    registry = AgentSessionActorRegistry(
        store=session_store,
        handler=reduce_unexpected_event,
    )
    executor = _UnhealthyStartupExecutor(handlers)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
    )

    with pytest.raises(RuntimeError, match="did not start handler-bound workers"):
        await harness.activate()

    assert session_store.pending_keys_calls == 1
    assert executor.start_calls == 1
    assert executor.shutdown_calls == 1
    assert harness.active is False
    assert harness.closed is True
    assert registry.accepting is False


@pytest.mark.asyncio
async def test_activation_failure_closes_a_partially_started_full_harness() -> None:
    handlers = _full_handlers()
    session_store = _EmptySessionStore()
    effect_store = _FailingStartupEffectStore()
    harness, _session_store, _effect_store, registry, executor = _full_harness(
        handlers,
        session_store=session_store,
        effect_store=effect_store,
    )

    with pytest.raises(RuntimeError, match="effect recovery failed"):
        await harness.activate()

    assert effect_store.recover_expired_calls == 1
    assert handlers.sealed is True
    assert harness.active is False
    assert harness.closed is True
    assert executor.closed is True
    assert registry.accepting is False


@pytest.mark.asyncio
async def test_cancelling_registry_recovery_closes_both_runtime_halves() -> None:
    handlers = _full_handlers()
    session_store = _BlockingRecoverySessionStore()
    harness, _session_store, _effect_store, registry, executor = _full_harness(
        handlers,
        session_store=session_store,
    )

    activation = asyncio.create_task(harness.activate())
    await asyncio.wait_for(session_store.recovery_started.wait(), timeout=1.0)
    activation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await activation

    assert handlers.sealed is True
    assert harness.active is False
    assert harness.closed is True
    assert executor.closed is True
    assert registry.accepting is False


@pytest.mark.asyncio
async def test_cancelling_executor_start_closes_both_runtime_halves() -> None:
    handlers = _full_handlers()
    effect_store = _BlockingStartupEffectStore()
    harness, _session_store, _effect_store, registry, executor = _full_harness(
        handlers,
        effect_store=effect_store,
    )

    activation = asyncio.create_task(harness.activate())
    await asyncio.wait_for(effect_store.startup_started.wait(), timeout=1.0)
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
    handlers = _full_handlers()
    session_store = _BlockingRecoverySessionStore()

    def reduce_unexpected_event(*_args: object) -> SessionTransition:
        raise AssertionError("unexpected actor event reduction")

    registry = AgentSessionActorRegistry(
        store=session_store,
        handler=reduce_unexpected_event,
    )
    executor = _BlockingShutdownExecutor(handlers)
    harness = ActorRuntimeHarness(
        registry=registry,
        effect_executor=executor,
        handlers=handlers,
    )

    activation = asyncio.create_task(harness.activate())
    await asyncio.wait_for(session_store.recovery_started.wait(), timeout=1.0)
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
