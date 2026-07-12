"""Inactive composition root for the durable session-actor runtime.

The harness deliberately owns only actor-v2 lifecycle composition.  It does
not bind ingress, timers, or any legacy ``AgentRuntime`` service, so creating
one cannot accidentally introduce a second writer for a live session.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from inspect import iscoroutinefunction

from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectExecutionContract,
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectHandlerNotFound,
    EffectHandlerRegistry,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    builtin_external_action_effect_contracts,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry

_FULL_ACTOR_V2_REQUIRED_EFFECT_CONTRACTS = (
    *builtin_session_actor_effect_contracts(),
    *builtin_external_action_effect_contracts(),
)


@dataclass(slots=True, frozen=True)
class RequiredEffectContractFailure:
    """One required contract that cannot safely be activated.

    Attributes:
        contract: The exact contract expected by the harness composition.
        reason: Human-readable explanation of the validation failure.
    """

    contract: EffectExecutionContract
    reason: str


class ActorRuntimeHarnessActivationError(RuntimeError):
    """Raised before activation when required effect handling is incomplete."""

    def __init__(
        self,
        failures: tuple[RequiredEffectContractFailure, ...],
    ) -> None:
        """Describe every missing or incompatible required contract.

        Args:
            failures: Contract validation failures detected before any task starts.
        """

        self.failures = failures
        rendered = ", ".join(
            f"{failure.contract.effect_kind}:v{failure.contract.version}"
            f" ({failure.reason})"
            for failure in failures
        )
        super().__init__(f"actor runtime activation blocked: {rendered}")


class ActorRuntimeHarness:
    """Own inactive actor-v2 components until an explicit lifecycle activation.

    The caller constructs the registry, executor, and handler registry first.
    This class intentionally does not expose those components as alternate
    routing or wake targets.  It exists to make the future actor-v2 activation
    gate explicit while remaining entirely independent of the legacy runtime.
    """

    def __init__(
        self,
        *,
        registry: AgentSessionActorRegistry,
        effect_executor: DurableEffectExecutor,
        handlers: EffectHandlerRegistry,
        required_effect_contracts: Iterable[EffectExecutionContract] | None = None,
        allow_partial_contracts: bool = False,
    ) -> None:
        """Build an inactive harness around already-composed actor components.

        Args:
            registry: Durable mailbox registry owned by this harness.
            effect_executor: Durable outbox executor that wakes ``registry``.
            handlers: Exact handler registry used to declare required contracts.
            required_effect_contracts: Contracts that must have matching
                handlers before activation can start any work. Omitting this
                requires every built-in actor-v2 and external-action contract.
            allow_partial_contracts: Test-only opt-out for an isolated harness
                that intentionally cannot activate the complete Actor v2 graph.

        Raises:
            ValueError: If a required contract reference is declared with two
                different immutable execution policies; the complete actor-v2
                contract set is not declared without an explicit partial
                opt-out; the executor is already active or closed; or
                ``handlers`` is not the registry used by ``effect_executor``.
        """

        if effect_executor.running:
            raise ValueError("actor runtime harness requires an inactive effect executor")
        if effect_executor.closed:
            raise ValueError("actor runtime harness cannot own a closed effect executor")
        if not registry.accepting:
            raise ValueError("actor runtime harness cannot own a closed actor registry")
        if effect_executor.handler_registry is not handlers:
            raise ValueError(
                "actor runtime harness requires the executor's handler registry"
            )

        self._registry = registry
        self._effect_executor = effect_executor
        self._handlers = handlers
        self._required_effect_contracts = _normalize_required_contracts(
            _FULL_ACTOR_V2_REQUIRED_EFFECT_CONTRACTS
            if required_effect_contracts is None
            else required_effect_contracts
        )
        self._allow_partial_contracts = allow_partial_contracts
        if not self._allow_partial_contracts:
            _require_exact_actor_contracts(self._required_effect_contracts)
        self._lifecycle_lock = asyncio.Lock()
        self._active = False
        self._closed = False

    @property
    def active(self) -> bool:
        """Return whether this harness has completed its activation sequence."""

        return self._active

    @property
    def closed(self) -> bool:
        """Return whether harness shutdown has been requested."""

        return self._closed

    @property
    def required_effect_contracts(self) -> tuple[EffectExecutionContract, ...]:
        """Return the immutable contracts validated before activation."""

        return self._required_effect_contracts

    async def activate(self) -> None:
        """Validate required handlers, recover mailboxes, and start effect work.

        Required-handler validation happens before either the actor registry or
        effect executor receives a lifecycle call.  A missing handler therefore
        fails closed without starting a background task or recovering live
        mailbox work.

        Raises:
            ActorRuntimeHarnessActivationError: If any required effect contract
                is missing, changed, or has no async-callable handler.
            RuntimeError: If the harness is partial or has already been shut
                down.
        """

        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("a closed actor runtime harness cannot be activated")
            if self._active:
                return
            if self._allow_partial_contracts:
                raise RuntimeError(
                    "partial actor runtime harnesses are deliberately non-activatable"
                )
            self._validate_required_handlers()
            self._handlers.seal()
            try:
                await self._registry.recover()
                await self._effect_executor.start()
                if not self._effect_executor.started:
                    raise RuntimeError(
                        "actor runtime effect executor did not start "
                        "handler-bound workers"
                    )
            except BaseException:
                # Registry recovery can create idle actors before the executor
                # claims its first effect. A startup failure must close both
                # halves instead of leaving a partial second writer alive.
                self._closed = True
                self._active = False
                await self._shutdown_components(drain=False)
                raise
            self._active = True

    async def shutdown(self, *, drain: bool = True) -> None:
        """Stop executor work before closing the actor mailbox registry.

        Args:
            drain: Whether owned components should finish their currently
                recoverable work before stopping.
        """

        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            try:
                await self._shutdown_components(drain=drain)
            finally:
                self._active = False

    def _validate_required_handlers(self) -> None:
        failures: list[RequiredEffectContractFailure] = []
        required_by_ref = {
            contract.ref: contract for contract in self._required_effect_contracts
        }
        registered_by_ref = {
            contract.ref: contract for contract in self._handlers.contracts()
        }
        for required in self._required_effect_contracts:
            registered = registered_by_ref.get(required.ref)
            if registered is None:
                failures.append(
                    RequiredEffectContractFailure(
                        contract=required,
                        reason="required contract is not registered",
                    )
                )
                continue
            if registered != required:
                failures.append(
                    RequiredEffectContractFailure(
                        contract=required,
                        reason="registered contract does not match required contract",
                    )
                )
                continue
            try:
                resolved_contract, handler = self._handlers.resolve(
                    required.effect_kind,
                    required.version,
                )
            except EffectHandlerNotFound as exc:
                failures.append(
                    RequiredEffectContractFailure(
                        contract=required,
                        reason=str(exc),
                    )
                )
                continue
            if resolved_contract != required:
                failures.append(
                    RequiredEffectContractFailure(
                        contract=required,
                        reason="registered contract does not match required contract",
                    )
                )
                continue
            if not _is_async_effect_handler(handler):
                failures.append(
                    RequiredEffectContractFailure(
                        contract=required,
                        reason="registered handler is not async-callable",
                    )
                )
        for registered in self._handlers.contracts():
            if registered.ref not in required_by_ref:
                failures.append(
                    RequiredEffectContractFailure(
                        contract=registered,
                        reason="registered contract is outside the activation graph",
                    )
                )
        if failures:
            raise ActorRuntimeHarnessActivationError(tuple(failures))

    async def _shutdown_components(self, *, drain: bool) -> None:
        """Close both runtime halves despite repeated caller cancellation."""

        task = asyncio.create_task(
            self._shutdown_components_once(drain=drain),
            name="agent-session-actor-harness-shutdown",
        )
        cancelled_while_waiting = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Do not cancel the shutdown task: a second cancellation of the
                # caller must not leave either runtime half alive.
                cancelled_while_waiting = True
        task.result()
        if cancelled_while_waiting:
            raise asyncio.CancelledError

    async def _shutdown_components_once(self, *, drain: bool) -> None:
        """Close executor then registry, always attempting both halves."""

        try:
            await self._effect_executor.shutdown(drain=drain)
        finally:
            await self._registry.shutdown(drain=drain)


def _normalize_required_contracts(
    contracts: Iterable[EffectExecutionContract],
) -> tuple[EffectExecutionContract, ...]:
    """Deduplicate exact required contracts and reject conflicting references."""

    by_ref: dict[tuple[str, int], EffectExecutionContract] = {}
    for contract in contracts:
        if not isinstance(contract, EffectExecutionContract):
            raise TypeError(
                "required_effect_contracts must contain EffectExecutionContract values"
            )
        previous = by_ref.get(contract.ref)
        if previous is not None and previous != contract:
            raise ValueError(
                "required effect contract reference has conflicting policies: "
                f"{contract.effect_kind}:v{contract.version}"
            )
        by_ref[contract.ref] = contract
    return tuple(by_ref.values())


def _require_exact_actor_contracts(
    required: tuple[EffectExecutionContract, ...],
) -> None:
    """Require an exact, complete actor-v2 contract graph for activation."""

    required_by_ref = {contract.ref: contract for contract in required}
    missing = [
        contract
        for contract in _FULL_ACTOR_V2_REQUIRED_EFFECT_CONTRACTS
        if required_by_ref.get(contract.ref) != contract
    ]
    expected_by_ref = {
        contract.ref: contract for contract in _FULL_ACTOR_V2_REQUIRED_EFFECT_CONTRACTS
    }
    unexpected = [
        contract
        for contract in required
        if expected_by_ref.get(contract.ref) != contract
    ]
    if missing or unexpected:
        parts: list[str] = []
        if missing:
            parts.append(
                "missing="
                + ", ".join(
                    f"{contract.effect_kind}:v{contract.version}"
                    for contract in missing
                )
            )
        if unexpected:
            parts.append(
                "unexpected="
                + ", ".join(
                    f"{contract.effect_kind}:v{contract.version}"
                    for contract in unexpected
                )
            )
        raise ValueError(
            "actor runtime harness requires the exact complete actor-v2 "
            "contract set unless allow_partial_contracts=True: " + "; ".join(parts)
        )


def _is_async_effect_handler(handler: object) -> bool:
    """Return whether a function or callable object has an async call surface."""

    return iscoroutinefunction(handler) or iscoroutinefunction(type(handler).__call__)


__all__ = [
    "ActorRuntimeHarness",
    "ActorRuntimeHarnessActivationError",
    "RequiredEffectContractFailure",
]
