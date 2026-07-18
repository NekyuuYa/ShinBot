"""Inactive composition root for the durable session-actor runtime.

The harness deliberately owns only actor-v2 lifecycle composition.  It does
not bind ingress, timers, or any legacy ``AgentRuntime`` service, so creating
one cannot accidentally introduce a second writer for a live session.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from inspect import iscoroutinefunction

from shinbot.agent.runtime.session_actor.clean_session_activation import (
    CleanSessionActivationPreflight,
    CleanSessionActivationReadiness,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectExecutionContract,
    builtin_clean_session_actor_v2_effect_contracts,
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
from shinbot.core.dispatch.legacy_recovery_gate import LegacyRecoveryPermit

_FULL_ACTOR_V2_REQUIRED_EFFECT_CONTRACTS = (
    *builtin_session_actor_effect_contracts(),
    *builtin_external_action_effect_contracts(),
)


class ActorRuntimeActivationScope(StrEnum):
    """The durable contract set a harness is authorized to execute."""

    COMPLETE_HISTORY = "complete_history"
    CLEAN_SESSION = "clean_session"


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
            f"{failure.contract.effect_kind}:v{failure.contract.version} ({failure.reason})"
            for failure in failures
        )
        super().__init__(f"actor runtime activation blocked: {rendered}")


class ActorRuntimeCleanSessionPreflightError(RuntimeError):
    """Raised when a clean-session activation finds Actor v2 residual state."""

    def __init__(self, readiness: CleanSessionActivationReadiness) -> None:
        """Render only stable residual evidence codes and counts."""

        self.readiness = readiness
        rendered = ", ".join(f"{blocker.code}:{blocker.count}" for blocker in readiness.blockers)
        super().__init__("clean-session Actor activation is blocked: " + rendered)


class ActorRuntimeHistoryRecoveryPermitRequired(RuntimeError):
    """Raised until a controller owns complete-history recovery and actor lifetime."""


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
        activation_scope: ActorRuntimeActivationScope = (
            ActorRuntimeActivationScope.COMPLETE_HISTORY
        ),
        clean_session_preflight: CleanSessionActivationPreflight | None = None,
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
            activation_scope: Explicit contract scope authorized for execution.
                ``clean_session`` requires an empty-domain preflight and never
                grants stateful migration or restart recovery authority.
            clean_session_preflight: Read-only durable preflight required only
                for the ``clean_session`` activation scope.

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
            raise ValueError("actor runtime harness requires the executor's handler registry")
        if effect_executor.session_registry is not registry:
            raise ValueError("actor runtime harness requires the executor's actor registry")
        if effect_executor.persistence_domain is not registry.persistence_domain:
            raise ValueError("actor runtime harness requires one shared persistence domain")

        try:
            scope = ActorRuntimeActivationScope(activation_scope)
        except (TypeError, ValueError) as exc:
            raise ValueError("actor runtime activation scope is invalid") from exc
        if scope is ActorRuntimeActivationScope.CLEAN_SESSION:
            if allow_partial_contracts:
                raise ValueError(
                    "clean-session activation cannot use a partial actor runtime harness"
                )
            if clean_session_preflight is None:
                raise ValueError(
                    "clean-session activation requires a durable empty-domain preflight"
                )
            if clean_session_preflight.persistence_domain is not registry.persistence_domain:
                raise ValueError(
                    "clean-session activation preflight must inspect the shared persistence domain"
                )
        elif clean_session_preflight is not None:
            raise ValueError("clean-session preflight is only valid for clean-session activation")
        self._registry = registry
        self._effect_executor = effect_executor
        self._handlers = handlers
        self._activation_scope = scope
        self._required_effect_contracts = _normalize_required_contracts(
            (
                _FULL_ACTOR_V2_REQUIRED_EFFECT_CONTRACTS
                if scope is ActorRuntimeActivationScope.COMPLETE_HISTORY
                else builtin_clean_session_actor_v2_effect_contracts()
            )
            if required_effect_contracts is None
            else required_effect_contracts
        )
        self._allow_partial_contracts = allow_partial_contracts
        self._clean_session_preflight = clean_session_preflight
        if scope is ActorRuntimeActivationScope.CLEAN_SESSION:
            _require_exact_clean_session_contracts(self._required_effect_contracts)
        elif not self._allow_partial_contracts:
            _require_exact_actor_contracts(self._required_effect_contracts)
        self._effect_contract_authority = registry.effect_contract_authority
        self._validate_shared_effect_authority()
        self._lifecycle_lock = asyncio.Lock()
        self._active = False
        self._closed = False
        self._shutdown_complete = False

    @property
    def active(self) -> bool:
        """Return whether this harness has completed its activation sequence."""

        return self._active and self._effect_executor.healthy and self._effect_executor.started

    @property
    def closed(self) -> bool:
        """Return whether harness shutdown has been requested."""

        return self._closed

    @property
    def shutdown_complete(self) -> bool:
        """Return whether both owned runtime halves confirmed shutdown.

        A closed harness can still report ``False`` after a failed stop attempt.
        Callers that release an external exclusion lease must require this
        stronger proof instead of treating a shutdown request as completion.
        """

        return self._shutdown_complete

    @property
    def required_effect_contracts(self) -> tuple[EffectExecutionContract, ...]:
        """Return the immutable contracts validated before activation."""

        return self._required_effect_contracts

    @property
    def activation_scope(self) -> ActorRuntimeActivationScope:
        """Return the exact durable contract scope selected at composition."""

        return self._activation_scope

    @property
    def complete_history_activation_ready(self) -> bool:
        """Return whether this harness can start the complete historical graph.

        This is a read-only composition check for lifecycle owners. A
        ``COMPLETE_HISTORY`` scope alone is not sufficient: a diagnostic
        partial harness may use that scope while deliberately lacking the full
        immutable contract graph. Activation revalidates the same condition
        after its await boundaries.
        """

        return (
            self._activation_scope is ActorRuntimeActivationScope.COMPLETE_HISTORY
            and not self._allow_partial_contracts
            and not self.required_handler_failures()
        )

    @property
    def persistence_domain(self) -> object:
        """Return the shared durable domain without exposing actor components."""

        return self._registry.persistence_domain

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
            await self._validate_clean_session_preflight()
            self._validate_shared_effect_authority()
            self._validate_required_handlers()
            if self._activation_scope is ActorRuntimeActivationScope.COMPLETE_HISTORY:
                raise ActorRuntimeHistoryRecoveryPermitRequired(
                    "complete-history actor activation requires a controller that "
                    "holds the durable recovery permit for the full actor lifecycle"
                )
            self._handlers.seal()
            try:
                self._validate_shared_effect_authority()
                await self._effect_executor.start_clean_session()
                self._validate_shared_effect_authority()
                if not self._effect_executor.started:
                    raise RuntimeError(
                        "actor runtime effect executor did not start handler-bound workers"
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

    async def _activate_complete_history_under_legacy_recovery_lifecycle(
        self,
        permit: LegacyRecoveryPermit,
    ) -> None:
        """Start complete-history work while an external controller retains ``permit``.

        This private entry point is intentionally not a public activation API.
        The caller must own the permit until :meth:`shutdown` proves that both
        the effect executor and every registry actor have stopped.
        """

        if not isinstance(permit, LegacyRecoveryPermit):
            raise TypeError("permit must be a LegacyRecoveryPermit")
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("a closed actor runtime harness cannot be activated")
            if self._active:
                return
            if self._allow_partial_contracts:
                raise RuntimeError(
                    "partial actor runtime harnesses are deliberately non-activatable"
                )
            if self._activation_scope is not ActorRuntimeActivationScope.COMPLETE_HISTORY:
                raise RuntimeError(
                    "legacy recovery lifecycle requires a complete-history actor runtime harness"
                )
            self._validate_shared_effect_authority()
            self._validate_required_handlers()
            self._handlers.seal()
            try:
                self._validate_shared_effect_authority()
                await self._registry._recover_under_legacy_recovery_lifecycle(permit)
                self._validate_shared_effect_authority()
                await self._effect_executor.start()
                self._validate_shared_effect_authority()
                if not self._effect_executor.started:
                    raise RuntimeError(
                        "actor runtime effect executor did not start handler-bound workers"
                    )
            except BaseException:
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
            if self._shutdown_complete:
                return
            self._closed = True
            try:
                await self._shutdown_components(drain=drain)
            finally:
                self._active = False

    def _validate_required_handlers(self) -> None:
        failures = self.required_handler_failures()
        if failures:
            raise ActorRuntimeHarnessActivationError(failures)

    def required_handler_failures(self) -> tuple[RequiredEffectContractFailure, ...]:
        """Return every handler-graph failure without changing runtime state.

        This is the same preflight used by :meth:`activate`, exposed for
        inactive composition diagnostics. Calling it never seals handlers,
        starts workers, recovers mailboxes, or changes ownership.
        """

        return self._handler_failures_for(
            self._required_effect_contracts,
            reject_contracts_outside_graph=(
                self._activation_scope is ActorRuntimeActivationScope.COMPLETE_HISTORY
            ),
            reject_handled_contracts_outside_graph=True,
        )

    def clean_session_handler_failures(
        self,
    ) -> tuple[RequiredEffectContractFailure, ...]:
        """Return missing handlers for current Actor-native session work only.

        Historical contracts remain part of the immutable effect authority but
        are intentionally excluded here. An empty result does not authorize
        activation: ingress, timer, recovery, and ownership lifecycle gates
        still need their own proofs.
        """

        return self.handler_failures_for(builtin_clean_session_actor_v2_effect_contracts())

    def handler_failures_for(
        self,
        required_effect_contracts: Iterable[EffectExecutionContract],
    ) -> tuple[RequiredEffectContractFailure, ...]:
        """Validate a read-only subset of the immutable authority graph.

        This is intended for diagnostics of future activation slices. Unlike
        :meth:`required_handler_failures`, it permits other known authority
        contracts to remain unbound so historical rows can be isolated instead
        of accidentally treated as current executable work.
        """

        return self._handler_failures_for(
            _normalize_required_contracts(required_effect_contracts),
            reject_contracts_outside_graph=False,
            reject_handled_contracts_outside_graph=False,
        )

    def _handler_failures_for(
        self,
        required_contracts: tuple[EffectExecutionContract, ...],
        *,
        reject_contracts_outside_graph: bool,
        reject_handled_contracts_outside_graph: bool,
    ) -> tuple[RequiredEffectContractFailure, ...]:
        """Run shared pure handler checks for one declared contract set."""

        failures: list[RequiredEffectContractFailure] = []
        required_by_ref = {contract.ref: contract for contract in required_contracts}
        registered_by_ref = {contract.ref: contract for contract in self._handlers.contracts()}
        for required in required_contracts:
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
        if reject_contracts_outside_graph:
            for registered in self._handlers.contracts():
                if registered.ref not in required_by_ref:
                    failures.append(
                        RequiredEffectContractFailure(
                            contract=registered,
                            reason="registered contract is outside the activation graph",
                        )
                    )
        if reject_handled_contracts_outside_graph:
            for handled in self._handlers.handled_contracts():
                if handled.ref not in required_by_ref:
                    failures.append(
                        RequiredEffectContractFailure(
                            contract=handled,
                            reason="registered handler is outside the activation graph",
                        )
                    )
        return tuple(failures)

    def _validate_shared_effect_authority(self) -> None:
        """Fail closed unless every runtime half shares one exact authority."""

        if self._effect_executor.session_registry is not self._registry:
            raise ValueError("actor runtime executor wake target changed after composition")
        if self._effect_executor.persistence_domain is not self._registry.persistence_domain:
            raise ValueError("actor runtime persistence domain changed after composition")
        authorities = (
            ("actor registry", self._registry.effect_contract_authority),
            ("effect executor", self._effect_executor.effect_contract_authority),
            ("handler registry", self._handlers.effect_contract_authority),
        )
        for component, authority in authorities:
            if authority is not self._effect_contract_authority:
                raise ValueError(
                    "actor runtime authority graph is split: "
                    f"{component} does not share the composition authority"
                )
        authoritative_by_ref = {
            contract.ref: contract for contract in self._effect_contract_authority.contracts()
        }
        required_by_ref = {contract.ref: contract for contract in self._required_effect_contracts}
        if self._allow_partial_contracts:
            if authoritative_by_ref != required_by_ref:
                raise ValueError(
                    "actor runtime composition authority does not exactly match "
                    "the required effect contract graph"
                )
            return
        expected_authority_by_ref = {
            contract.ref: contract for contract in _FULL_ACTOR_V2_REQUIRED_EFFECT_CONTRACTS
        }
        if authoritative_by_ref != expected_authority_by_ref:
            raise ValueError(
                "actor runtime composition authority does not exactly match "
                "the complete actor-v2 contract authority"
            )
        expected_required_by_ref = (
            expected_authority_by_ref
            if self._activation_scope is ActorRuntimeActivationScope.COMPLETE_HISTORY
            else {
                contract.ref: contract
                for contract in builtin_clean_session_actor_v2_effect_contracts()
            }
        )
        if required_by_ref != expected_required_by_ref:
            raise ValueError(
                "actor runtime activation graph does not exactly match "
                f"the {self._activation_scope.value} contract scope"
            )

    async def _validate_clean_session_preflight(self) -> None:
        """Reject a clean scope when durable Actor history is still present."""

        if self._activation_scope is not ActorRuntimeActivationScope.CLEAN_SESSION:
            return
        preflight = self._clean_session_preflight
        assert preflight is not None
        if preflight.persistence_domain is not self._registry.persistence_domain:
            raise RuntimeError(
                "clean-session activation preflight persistence domain changed after composition"
            )
        readiness = await preflight.check()
        if not isinstance(readiness, CleanSessionActivationReadiness):
            raise TypeError("clean-session activation preflight returned an invalid readiness")
        if not readiness.permitted:
            raise ActorRuntimeCleanSessionPreflightError(readiness)

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
        self._shutdown_complete = True
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
            raise TypeError("required_effect_contracts must contain EffectExecutionContract values")
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
        contract for contract in required if expected_by_ref.get(contract.ref) != contract
    ]
    if missing or unexpected:
        parts: list[str] = []
        if missing:
            parts.append(
                "missing="
                + ", ".join(f"{contract.effect_kind}:v{contract.version}" for contract in missing)
            )
        if unexpected:
            parts.append(
                "unexpected="
                + ", ".join(
                    f"{contract.effect_kind}:v{contract.version}" for contract in unexpected
                )
            )
        raise ValueError(
            "actor runtime harness requires the exact complete actor-v2 "
            "contract set unless allow_partial_contracts=True: " + "; ".join(parts)
        )


def _require_exact_clean_session_contracts(
    required: tuple[EffectExecutionContract, ...],
) -> None:
    """Require the fixed current-contract subset for clean Actor startup."""

    required_by_ref = {contract.ref: contract for contract in required}
    expected_contracts = builtin_clean_session_actor_v2_effect_contracts()
    expected_by_ref = {contract.ref: contract for contract in expected_contracts}
    if required_by_ref == expected_by_ref:
        return
    missing = [
        contract for contract in expected_contracts if required_by_ref.get(contract.ref) != contract
    ]
    unexpected = [
        contract for contract in required if expected_by_ref.get(contract.ref) != contract
    ]
    parts: list[str] = []
    if missing:
        parts.append(
            "missing="
            + ", ".join(f"{contract.effect_kind}:v{contract.version}" for contract in missing)
        )
    if unexpected:
        parts.append(
            "unexpected="
            + ", ".join(f"{contract.effect_kind}:v{contract.version}" for contract in unexpected)
        )
    raise ValueError(
        "clean-session actor runtime harness requires the exact current "
        "contract set: " + "; ".join(parts)
    )


def _is_async_effect_handler(handler: object) -> bool:
    """Return whether a function or callable object has an async call surface."""

    return iscoroutinefunction(handler) or iscoroutinefunction(type(handler).__call__)


__all__ = [
    "ActorRuntimeActivationScope",
    "ActorRuntimeCleanSessionPreflightError",
    "ActorRuntimeHarness",
    "ActorRuntimeHarnessActivationError",
    "ActorRuntimeHistoryRecoveryPermitRequired",
    "RequiredEffectContractFailure",
]
