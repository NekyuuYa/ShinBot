"""Durable, lease-fenced execution of session actor effects.

The session actor commits effects to an outbox, then this executor runs their
handlers outside the actor transaction.  Handler outcomes are returned to the
actor only through a mailbox event committed atomically with effect settlement.
External I/O is therefore at-least-once: handlers must pass the durable
``idempotency_key`` to every downstream operation that can have side effects.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from inspect import isawaitable
from typing import Any, Protocol, runtime_checkable

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    DEFAULT_OUTCOME_FENCE_FIELDS,
    EffectContractAuthority,
    EffectExecutionContract,
    EffectLane,
    builtin_effect_contract_authority,
    builtin_session_actor_effect_contracts,
    resolved_outcome_fence_fields,
)
from shinbot.agent.runtime.session_actor.effect_execution_errors import (
    EffectExecutionCancelled,
    EffectExecutionDeferred,
)
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope
from shinbot.agent.runtime.session_actor.model_execution_witness import (
    MODEL_EXECUTION_WITNESSED_EFFECT_KINDS,
    ModelExecutionClaim,
    ModelExecutionPermit,
    ModelExecutionPermitDisposition,
    ModelExecutionWitnessStorePort,
)
from shinbot.agent.runtime.session_actor.review_execution_gate import (
    ReviewExecutionClaim,
    ReviewExecutionGateStorePort,
    ReviewExecutionPermit,
    ReviewExecutionPermitDisposition,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffNotifier

logger = logging.getLogger(__name__)


class _FrozenDict(dict[str, Any]):
    """JSON-compatible dictionary that rejects handler mutation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable effect payloads are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class _FrozenList(list[Any]):
    """JSON-compatible list that rejects handler mutation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable effect payloads are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("durable effect mapping keys must be strings")
        return _FrozenDict((key, _freeze_json(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return _FrozenList(_freeze_json(item) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("durable effect numeric values must be finite")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"durable effect values must be JSON-compatible, got {type(value)!r}")


class DurableEffectStatus(StrEnum):
    """Durable lifecycle state for an outbox effect."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class EffectSettlementStatus(StrEnum):
    """Outcome of an atomic effect-and-mailbox settlement transaction."""

    COMMITTED = "committed"
    ALREADY_COMMITTED = "already_committed"
    PRECONDITION_SKIPPED = "precondition_skipped"
    CANCELLED = "cancelled"


class EffectRunStatus(StrEnum):
    """Observable result of one executor claim attempt."""

    EMPTY = "empty"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"
    DEFERRED = "deferred"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"
    CLAIM_LOST = "claim_lost"


class LocalOperationQuiescenceScope(StrEnum):
    """Scope of an operation quiescence observation.

    The effect executor only owns tasks created by this executor instance in
    the current Python process. It cannot observe another executor process, a
    stale durable lease, or an external action that has crossed the process
    boundary.
    """

    LOCAL_PROCESS = "local_process"


class LocalOperationQuiescenceStatus(StrEnum):
    """Outcome of one local operation quiescence request."""

    NO_LOCAL_HANDLER_TASKS = "no_local_handler_tasks"
    QUIESCENT = "quiescent"
    TIMED_OUT = "timed_out"


class EffectQuarantineReason(StrEnum):
    """Store-owned terminal reasons that must not impersonate domain outcomes."""

    MALFORMED_EFFECT_ROW = "malformed_effect_row"
    UNSUPPORTED_CONTRACT = "unsupported_contract"
    CONTRACT_SIGNATURE_MISMATCH = "contract_signature_mismatch"
    OUTCOME_FENCE_MISSING = "outcome_fence_missing"
    CONTRACT_RESOLUTION_INCONSISTENT = "contract_resolution_inconsistent"
    LANE_MISMATCH = "lane_mismatch"


class EffectExecutorError(RuntimeError):
    """Base error raised by durable effect execution."""


class EffectStoreBindingChanged(EffectExecutorError):
    """Raised when a durable store changes a composed runtime identity."""


class FencedEffectExecutionLeaseLost(EffectStoreBindingChanged):
    """Raised when a scoped executor loses its durable target lease authority."""


class EffectAuthorityChanged(EffectStoreBindingChanged):
    """Raised when a composed durable store swaps its authority snapshot."""


class EffectClaimLost(EffectExecutorError):
    """Raised when an effect claim is no longer the current fenced lease."""


class EffectHandlerNotFound(EffectExecutorError):
    """Raised when no handler is registered for a durable effect kind."""


class EffectContractSignatureMismatch(EffectExecutorError):
    """Raised when persisted work does not match its registered contract."""


class EffectExecutionConfigurationError(EffectExecutorError):
    """Raised when an effect lacks a required execution liveness safeguard."""


@dataclass(slots=True, frozen=True)
class DurableEffectEnvelope:
    """One durable effect read from the actor effect outbox."""

    effect_id: str
    key: SessionKey
    kind: str
    idempotency_key: str
    ownership_generation: int = 0
    contract_version: int = 1
    contract_signature: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    source_event_id: str = ""
    operation_id: str = ""
    trace_id: str = ""
    available_at: float = 0.0
    created_at: float = 0.0

    def __post_init__(self) -> None:
        """Normalize durable identities and detach mutable input mappings."""

        effect_id = str(self.effect_id or "").strip()
        kind = str(self.kind or "").strip()
        idempotency_key = str(self.idempotency_key or "").strip()
        contract_signature = str(self.contract_signature or "").strip()
        if not effect_id:
            raise ValueError("effect_id must not be empty")
        if not kind:
            raise ValueError("effect kind must not be empty")
        if not idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        if self.contract_version < 1:
            raise ValueError("contract_version must be at least one")
        if self.ownership_generation < 0:
            raise ValueError("ownership_generation must not be negative")
        if not contract_signature:
            raise ValueError("contract_signature must not be empty")
        object.__setattr__(self, "effect_id", effect_id)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "contract_signature", contract_signature)
        object.__setattr__(self, "payload", _freeze_json(self.payload))

    def outcome_fence_payload(self, fields: tuple[str, ...]) -> dict[str, Any]:
        """Project declared durable fence fields into an outcome envelope.

        Contracts own the projection declaration.  Missing optional values stay
        absent so legacy records retain their historic outcome shape.
        """

        return {
            field_name: self.payload[field_name]
            for field_name in fields
            if field_name in self.payload
        }


@dataclass(slots=True, frozen=True)
class ClaimedEffect:
    """One uniquely fenced lease for a durable effect.

    ``claim_id`` must be regenerated on every claim, including a reclaim by the
    same worker.  Worker identity alone cannot prevent an ABA stale completion.
    """

    claim_id: str
    effect: DurableEffectEnvelope
    worker_id: str
    attempt_count: int
    claimed_at: float = 0.0
    lease_expires_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate claim fencing and retry metadata."""

        claim_id = str(self.claim_id or "").strip()
        worker_id = str(self.worker_id or "").strip()
        if not claim_id:
            raise ValueError("claim_id must not be empty")
        if not worker_id:
            raise ValueError("worker_id must not be empty")
        if self.attempt_count < 1:
            raise ValueError("attempt_count must be at least one")
        object.__setattr__(self, "claim_id", claim_id)
        object.__setattr__(self, "worker_id", worker_id)

    @property
    def key(self) -> SessionKey:
        """Return the owning actor key."""

        return self.effect.key


@dataclass(slots=True, frozen=True)
class InFlightEffectHandlerKey:
    """Immutable local identity for one executing durable effect handler.

    ``claim_id`` is part of the identity on purpose. A reclaimed durable
    effect can have the same session, operation, kind, and effect id while a
    cancellation tail from the old fenced claim is still unwinding.
    """

    key: SessionKey
    operation_id: str
    effect_kind: str
    effect_id: str
    claim_id: str

    def __post_init__(self) -> None:
        """Normalize durable task identity fields."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("key must be a SessionKey")
        operation_id = str(self.operation_id or "").strip()
        effect_kind = str(self.effect_kind or "").strip()
        effect_id = str(self.effect_id or "").strip()
        claim_id = str(self.claim_id or "").strip()
        if not effect_kind:
            raise ValueError("effect_kind must not be empty")
        if not effect_id:
            raise ValueError("effect_id must not be empty")
        if not claim_id:
            raise ValueError("claim_id must not be empty")
        object.__setattr__(self, "operation_id", operation_id)
        object.__setattr__(self, "effect_kind", effect_kind)
        object.__setattr__(self, "effect_id", effect_id)
        object.__setattr__(self, "claim_id", claim_id)

    @classmethod
    def from_claim(cls, claim: ClaimedEffect) -> InFlightEffectHandlerKey:
        """Build the exact local task identity for one fenced claim."""

        return cls(
            key=claim.key,
            operation_id=claim.effect.operation_id,
            effect_kind=claim.effect.kind,
            effect_id=claim.effect.effect_id,
            claim_id=claim.claim_id,
        )


@dataclass(slots=True, frozen=True)
class LocalOperationQuiescence:
    """Result of cancelling and observing local handler tasks for one operation.

    This is deliberately a process-local observation. In particular,
    :attr:`status` being :attr:`~LocalOperationQuiescenceStatus.NO_LOCAL_HANDLER_TASKS`
    means only that this executor found no matching task in its own registry;
    it is never a proof that another process, a durable lease, or an external
    side effect is quiescent.
    """

    scope: LocalOperationQuiescenceScope
    status: LocalOperationQuiescenceStatus
    key: SessionKey
    operation_id: str
    matched_handler_keys: tuple[InFlightEffectHandlerKey, ...] = ()
    cancelled_handler_keys: tuple[InFlightEffectHandlerKey, ...] = ()
    remaining_handler_keys: tuple[InFlightEffectHandlerKey, ...] = ()

    @property
    def locally_confirmed_quiescent(self) -> bool:
        """Return whether observed local tasks were confirmed to have ended.

        This property deliberately excludes ``NO_LOCAL_HANDLER_TASKS``. The
        latter is an empty local lookup, while this result proves that at least
        one locally observed task ended before the report was returned.
        """

        return self.status is LocalOperationQuiescenceStatus.QUIESCENT


@dataclass(slots=True, frozen=True)
class LocalEffectExecutorQuiescence:
    """Process-local handler quiescence report for one whole effect executor.

    This result is intentionally narrower than a durable lease or external
    side-effect proof. It becomes useful for a future target-retirement
    sequence only after the target has stopped the executor from claiming new
    work. At that point, ``QUIESCENT`` proves every handler task observed by
    this executor instance ended before the report was returned.
    """

    scope: LocalOperationQuiescenceScope
    status: LocalOperationQuiescenceStatus
    matched_handler_keys: tuple[InFlightEffectHandlerKey, ...] = ()
    cancelled_handler_keys: tuple[InFlightEffectHandlerKey, ...] = ()
    remaining_handler_keys: tuple[InFlightEffectHandlerKey, ...] = ()

    @property
    def locally_confirmed_quiescent(self) -> bool:
        """Return whether observed local handler tasks all ended."""

        return self.status is LocalOperationQuiescenceStatus.QUIESCENT


@dataclass(slots=True, frozen=True)
class EffectHandlerResult:
    """Mailbox completion requested by a successful effect handler."""

    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the completion kind and detach mutable payload input."""

        object.__setattr__(self, "payload", _freeze_json(self.payload))


@dataclass(slots=True, frozen=True)
class EffectSettlementResult:
    """Proof that effect settlement and mailbox insertion are durable."""

    status: EffectSettlementStatus
    effect_id: str
    event_id: str
    key: SessionKey
    wake_request: FencedMailboxWakeRequest | None = None
    mailbox_id: int | None = None

    def __post_init__(self) -> None:
        """Validate the optional immutable identity of the settled mailbox."""

        if self.mailbox_id is None:
            return
        if isinstance(self.mailbox_id, bool) or not isinstance(self.mailbox_id, int):
            raise ValueError("mailbox_id must be an integer or None")
        if self.mailbox_id < 1:
            raise ValueError("mailbox_id must be positive when supplied")


@dataclass(slots=True, frozen=True)
class EffectExpiryRecoveryResult:
    """Exact durable outcomes produced while recovering expired effect leases.

    Fenced callers retain the returned mailbox identities for a separately
    supervised handoff path. They must not collapse those notifications into a
    key-only registry wake.
    """

    recovered_count: int
    notifications: tuple[EffectSettlementResult, ...] = ()

    def __post_init__(self) -> None:
        """Validate one bounded typed recovery observation."""

        if isinstance(self.recovered_count, bool) or not isinstance(
            self.recovered_count,
            int,
        ):
            raise ValueError("recovered_count must be an integer")
        if self.recovered_count < 0:
            raise ValueError("recovered_count must not be negative")
        notifications = tuple(self.notifications)
        if any(not isinstance(item, EffectSettlementResult) for item in notifications):
            raise TypeError("recovery notifications must be EffectSettlementResult values")
        object.__setattr__(self, "notifications", notifications)


@dataclass(slots=True, frozen=True)
class EffectRunResult:
    """Result returned after one claim attempt."""

    status: EffectRunStatus
    effect_id: str = ""
    event_id: str = ""
    attempt_count: int = 0
    retry_at: float | None = None
    error: str = ""


class DurableEffectStore(Protocol):
    """Atomic persistence operations required by the effect executor."""

    @property
    def persistence_domain(self) -> object:
        """Return the stable identity of the backing transaction domain."""

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the sealed authority used for claims and settlement."""

    async def claim_next(
        self,
        *,
        worker_id: str,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ClaimedEffect | None:
        """Claim the next available effect with a newly generated claim id."""

    async def drain_quarantine_notifications(
        self,
    ) -> tuple[EffectSettlementResult, ...]:
        """Return raw-row quarantines committed while scanning for a claim."""

    async def renew_lease(
        self,
        claim: ClaimedEffect,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ClaimedEffect:
        """Extend a lease only if ``claim_id`` is still authoritative."""

    async def complete_with_event(
        self,
        claim: ClaimedEffect,
        completion_envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult:
        """Atomically complete an effect and insert its completion event."""

    async def release_for_retry(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        available_at: float,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult | None:
        """Release the current claim or report a gate-driven cancellation."""

    async def defer_without_attempt(
        self,
        claim: ClaimedEffect,
        *,
        reason: str,
        available_at: float,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult | None:
        """Release one live claim or report a gate-driven cancellation."""

    async def fail_with_event(
        self,
        claim: ClaimedEffect,
        failure_envelope: SessionEventEnvelope,
        *,
        error: str,
        outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult:
        """Atomically fail an effect and insert an ``EffectFailed`` event."""

    async def quarantine(
        self,
        claim: ClaimedEffect,
        *,
        reason: EffectQuarantineReason,
        message: str,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult:
        """Terminalize unsupported work with a store-owned diagnostic event."""

    async def release(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult | None:
        """Release a live shutdown claim or report a gate cancellation."""

    async def recover_expired(
        self,
        *,
        worker_id: str,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> int:
        """Maintain expired claims and publish any durable blocker notices."""

    async def next_available_at(
        self,
        *,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> float | None:
        """Return the earliest pending availability or processing lease expiry."""


class SessionActorWakeTarget(Protocol):
    """Registry surface used after a mailbox event has committed."""

    def wake(self, key: SessionKey) -> Awaitable[None] | None:
        """Wake an actor without performing another mailbox write."""


@runtime_checkable
class FencedEffectRecoveryStore(Protocol):
    """Store capability required for one explicit scoped-history recovery."""

    async def recover_expired_fenced(
        self,
        *,
        worker_id: str,
        execution_binding: FencedActorExecutionBinding,
    ) -> EffectExpiryRecoveryResult:
        """Recover only expired work protected by the exact live target lease."""


class EffectHandler(Protocol):
    """Async handler for one kind of effect."""

    async def __call__(self, context: EffectExecutionContext) -> EffectHandlerResult:
        """Run external work using ``context.idempotency_key`` for side effects."""


class EffectHandlerRegistry:
    """Explicit mapping from durable effect kinds to async handlers."""

    def __init__(
        self,
        *,
        contracts: Iterable[EffectExecutionContract] | None = None,
        include_builtin_contracts: bool = True,
        contract_authority: EffectContractAuthority | None = None,
    ) -> None:
        """Initialize a mutable handler binding over one contract authority.

        Args:
            contracts: Initial contracts when constructing a local authority.
            include_builtin_contracts: Whether construction without independent
                contracts binds the complete built-in Actor v2 authority. When
                ``contracts`` is supplied, those contracts extend the built-in
                session-actor policies through a local authority.
            contract_authority: Exact immutable graph to bind. When supplied,
                contracts cannot be added, removed, or replaced; only handlers
                may be attached to policies in this authority.

        Raises:
            TypeError: If ``contract_authority`` has the wrong type.
            ValueError: If both an authority and independent contracts are given.
        """

        self._handlers: dict[tuple[str, int], EffectHandler] = {}
        self._contracts: dict[tuple[str, int], EffectExecutionContract] = {}
        self._sealed = False
        if contract_authority is None and include_builtin_contracts and contracts is None:
            contract_authority = builtin_effect_contract_authority()
        if contract_authority is not None and not isinstance(
            contract_authority,
            EffectContractAuthority,
        ):
            raise TypeError("contract_authority must be an EffectContractAuthority")
        if contract_authority is not None and contracts is not None:
            raise ValueError("contracts cannot be supplied with an immutable contract_authority")
        self._authority_locked = contract_authority is not None
        self._effect_contract_authority = contract_authority or EffectContractAuthority(())
        if contract_authority is not None:
            self._contracts = {
                contract.ref: contract for contract in contract_authority.contracts()
            }
            return
        initial = list(contracts or ())
        if include_builtin_contracts:
            initial = [*builtin_session_actor_effect_contracts(), *initial]
        for contract in initial:
            self.register_contract(contract)

    @property
    def sealed(self) -> bool:
        """Return whether contract and handler registration is frozen."""

        return self._sealed

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the immutable snapshot governing registered handlers."""

        return self._effect_contract_authority

    def seal(self) -> None:
        """Irreversibly freeze handler and contract registration.

        Durable effect workers resolve handlers repeatedly while running.  An
        activation gate therefore seals the registry before it starts workers,
        so the validated contract graph cannot be changed after activation.
        """

        self._sealed = True

    def contracts(self) -> tuple[EffectExecutionContract, ...]:
        """Return the registered contracts in stable durable identity order."""

        return tuple(
            sorted(
                self._contracts.values(),
                key=lambda contract: (contract.effect_kind, contract.version),
            )
        )

    def handled_contracts(self) -> tuple[EffectExecutionContract, ...]:
        """Return contracts that have an explicitly registered handler.

        A registered contract is durable policy metadata, not proof that a
        process can execute that effect.  Callers that claim work must use
        this projection so an incompletely wired known contract remains
        recoverable instead of being treated as an unknown orphan.
        """

        return tuple(contract for contract in self.contracts() if contract.ref in self._handlers)

    def register_contract(
        self,
        contract: EffectExecutionContract,
        *,
        replace_existing: bool = False,
    ) -> None:
        """Register the immutable policy for one durable effect kind."""

        self._require_mutable()
        if not isinstance(contract, EffectExecutionContract):
            raise TypeError("contract must be an EffectExecutionContract")
        key = (contract.effect_kind, contract.version)
        if self._authority_locked:
            authoritative = self._contracts.get(key)
            if authoritative is None:
                raise ValueError(
                    "effect contract is outside the immutable authority: "
                    f"{contract.effect_kind}:v{contract.version}"
                )
            if authoritative != contract:
                raise ValueError(
                    "effect contract does not match the immutable authority: "
                    f"{contract.effect_kind}:v{contract.version}"
                )
            return
        if key in self._contracts and not replace_existing:
            if self._contracts[key] == contract:
                return
            raise ValueError(
                f"effect contract is already registered: {contract.effect_kind}:v{contract.version}"
            )
        self._contracts[key] = contract
        self._effect_contract_authority = EffectContractAuthority(self._contracts.values())

    def register(
        self,
        kind: str,
        handler: EffectHandler,
        *,
        contract: EffectExecutionContract | None = None,
        replace_existing: bool = False,
    ) -> None:
        """Register one handler, rejecting accidental ownership replacement."""

        self._require_mutable()
        normalized = str(kind or "").strip()
        if not normalized:
            raise ValueError("effect kind must not be empty")
        if contract is not None:
            if contract.effect_kind != normalized:
                raise ValueError("handler kind does not match its effect contract")
            self.register_contract(contract, replace_existing=replace_existing)
            resolved_contract = contract
        else:
            matching_contracts = tuple(
                registered
                for registered in self._contracts.values()
                if registered.effect_kind == normalized
            )
            if not matching_contracts:
                raise ValueError(
                    "a durable effect contract is required before handler "
                    f"registration: {normalized}"
                )
            if len(matching_contracts) != 1:
                versions = ", ".join(
                    f"v{registered.version}"
                    for registered in sorted(
                        matching_contracts,
                        key=lambda registered: registered.version,
                    )
                )
                raise ValueError(
                    "an explicit durable effect contract is required when "
                    f"multiple versions are registered for {normalized}: {versions}"
                )
            resolved_contract = matching_contracts[0]
        version = resolved_contract.version
        if (normalized, version) not in self._contracts:
            raise ValueError(
                f"a durable effect contract is required before handler registration: {normalized}"
            )
        key = (normalized, version)
        if key in self._handlers and not replace_existing:
            raise ValueError(f"effect handler is already registered: {normalized}:v{version}")
        self._handlers[key] = handler

    def resolve(
        self,
        kind: str,
        version: int = 1,
    ) -> tuple[EffectExecutionContract, EffectHandler]:
        """Return the durable contract and handler for *kind*."""

        normalized = str(kind or "").strip()
        contract = self._contracts.get((normalized, version))
        if contract is None:
            raise EffectHandlerNotFound(
                f"no durable effect contract is registered for {normalized!r}"
            )
        try:
            return contract, self._handlers[(normalized, version)]
        except KeyError as exc:
            raise EffectHandlerNotFound(
                f"no durable effect handler is registered for {normalized!r} version {version}"
            ) from exc

    def contract_for(self, kind: str, version: int = 1) -> EffectExecutionContract:
        """Return the registered durable contract for *kind*."""

        normalized = str(kind or "").strip()
        try:
            return self._contracts[(normalized, version)]
        except KeyError as exc:
            raise EffectHandlerNotFound(
                f"no durable effect contract is registered for {normalized!r} version {version}"
            ) from exc

    def effect_contracts_for_lane(
        self,
        lane: EffectLane,
    ) -> tuple[tuple[str, int], ...]:
        """Return lane-owned contract refs in deterministic priority order."""

        contracts = sorted(
            (contract for contract in self._contracts.values() if contract.lane is lane),
            key=lambda contract: (
                contract.priority,
                contract.effect_kind,
                contract.version,
            ),
        )
        return tuple(contract.ref for contract in contracts)

    def handled_effect_contracts_for_lane(
        self,
        lane: EffectLane,
    ) -> tuple[tuple[str, int], ...]:
        """Return handler-bound contract refs for one execution lane.

        Unlike :meth:`effect_contracts_for_lane`, this deliberately excludes
        policy-only contracts whose handler has not been composed yet.
        """

        contracts = sorted(
            (contract for contract in self.handled_contracts() if contract.lane is lane),
            key=lambda contract: (
                contract.priority,
                contract.effect_kind,
                contract.version,
            ),
        )
        return tuple(contract.ref for contract in contracts)

    def lanes(self) -> tuple[EffectLane, ...]:
        """Return lanes that own at least one registered contract."""

        return tuple(lane for lane in EffectLane if self.effect_contracts_for_lane(lane))

    def handled_lanes(self) -> tuple[EffectLane, ...]:
        """Return lanes with at least one contract bound to a handler."""

        return tuple(lane for lane in EffectLane if self.handled_effect_contracts_for_lane(lane))

    def _require_mutable(self) -> None:
        if self._sealed:
            raise RuntimeError("effect handler registry is sealed")


class EffectExecutionContext:
    """Lease-aware context passed to an effect handler.

    Handlers may call :meth:`renew_lease` around unusually long indivisible
    operations.  The executor can also renew automatically while the handler is
    awaiting.  Every external write must use :attr:`idempotency_key` because a
    crash can occur after the write but before durable completion.
    """

    def __init__(
        self,
        store: DurableEffectStore,
        claim: ClaimedEffect,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> None:
        self._store = store
        self._claim = claim
        self._execution_binding = execution_binding
        self._renew_lock = asyncio.Lock()
        self._revoked = False
        self._model_execution_witness_started = False

    @property
    def claim(self) -> ClaimedEffect:
        """Return the latest renewed claim snapshot."""

        return self._claim

    @property
    def effect(self) -> DurableEffectEnvelope:
        """Return the durable handler input."""

        return self._claim.effect

    @property
    def execution_binding(self) -> FencedActorExecutionBinding | None:
        """Return the optional target lease scoped to this handler execution.

        Infrastructure handlers use this only to pass the same lease authority
        into their own durable pre-dispatch or witness transactions. Domain
        handlers must not persist or transform this opaque capability.
        """

        return self._execution_binding

    @property
    def idempotency_key(self) -> str:
        """Return the mandatory downstream idempotency key."""

        return self._claim.effect.idempotency_key

    async def renew_lease(self) -> ClaimedEffect:
        """Renew and retain the current claim, preserving its fencing id."""

        async with self._renew_lock:
            if self._revoked:
                raise EffectClaimLost("effect execution context has been revoked")
            current = self._claim
            if self._execution_binding is None:
                renewed = await self._store.renew_lease(current)
            else:
                renewed = await self._store.renew_lease(
                    current,
                    execution_binding=self._execution_binding,
                )
            if renewed.claim_id != current.claim_id:
                raise EffectClaimLost("lease renewal changed the effect claim id")
            if renewed.worker_id != current.worker_id:
                raise EffectClaimLost("lease renewal changed the effect worker id")
            if renewed.effect.effect_id != current.effect.effect_id:
                raise EffectClaimLost("lease renewal changed the effect identity")
            self._claim = renewed
            return renewed

    def revoke(self) -> None:
        """Prevent a timed-out or cancelled handler from renewing its old claim."""

        self._revoked = True

    @property
    def model_execution_witness_started(self) -> bool:
        """Return whether this handler task crossed the model-call start fence."""

        return self._model_execution_witness_started

    def mark_model_execution_witness_started(self) -> None:
        """Record that the executor persisted a model start witness for this task."""

        self._model_execution_witness_started = True


@dataclass(slots=True)
class _InFlightEffectHandler:
    """Executor-owned local task and context for one fenced handler claim."""

    identity: InFlightEffectHandlerKey
    task: asyncio.Task[EffectHandlerResult]
    context: EffectExecutionContext


Clock = Callable[[], float]


class DurableEffectExecutor:
    """Supervise durable effect handlers outside session actor transactions."""

    def __init__(
        self,
        *,
        store: DurableEffectStore,
        handlers: EffectHandlerRegistry,
        session_registry: SessionActorWakeTarget | None = None,
        mailbox_handoff_notifier: MailboxHandoffNotifier | None = None,
        worker_id: str | None = None,
        worker_count: int = 1,
        control_worker_count: int = 1,
        orphan_worker_count: int = 1,
        poll_interval_seconds: float = 1.0,
        renew_interval_seconds: float | None = 10.0,
        review_execution_gate_store: ReviewExecutionGateStorePort | None = None,
        model_execution_witness_store: ModelExecutionWitnessStorePort | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
        clock: Clock | None = None,
    ) -> None:
        """Initialize an executor without starting worker tasks.

        Args:
            store: Durable outbox and atomic settlement implementation.
            handlers: Registry of async effect handlers.
            session_registry: Legacy actor registry woken only after an
                explicitly unfenced settlement commits. Fenced executors must
                not receive this key-only target.
            mailbox_handoff_notifier: Optional advisory sink for a committed
                admission-fenced mailbox id. It is never an authorization
                boundary and does not activate or bind an Actor v2 target.
            worker_id: Optional stable process-level worker prefix.
            worker_count: Worker count for planner and default lanes.
            control_worker_count: Dedicated workers reserved for control effects.
            orphan_worker_count: Workers that terminally fail unknown effect kinds.
            poll_interval_seconds: Recovery polling bound when no wake is received.
            renew_interval_seconds: Automatic lease renewal cadence, or ``None``
                for non-review effects. Review workflow effects require renewal
                before their handler task can start.
            review_execution_gate_store: Durable start/finish witness store for
                ``run_review_workflow``. Review work remains fail-closed when
                this port is not composed.
            model_execution_witness_store: Optional durable start/finish
                witness store for non-review model workflow effects. It is
                opt-in so direct executor users retain their historic behavior;
                the inactive Actor v2 assembly always composes this port.
            execution_binding: Optional exact target lease capability. When
                supplied, this executor is restricted to one owner request and
                must be started with :meth:`start_fenced`.
            clock: Injectable wall clock for retry and event timestamps.
        """

        if worker_count < 1:
            raise ValueError("worker_count must be at least one")
        if control_worker_count < 1:
            raise ValueError("control_worker_count must be at least one")
        if orphan_worker_count < 1:
            raise ValueError("orphan_worker_count must be at least one")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if not math.isfinite(poll_interval_seconds):
            raise ValueError("poll_interval_seconds must be finite")
        if renew_interval_seconds is not None and renew_interval_seconds <= 0:
            raise ValueError("renew_interval_seconds must be positive or None")
        if renew_interval_seconds is not None and not math.isfinite(renew_interval_seconds):
            raise ValueError("renew_interval_seconds must be finite or None")
        if mailbox_handoff_notifier is not None and not callable(
            getattr(mailbox_handoff_notifier, "notify", None)
        ):
            raise TypeError("mailbox_handoff_notifier must implement notify(mailbox_id)")
        if execution_binding is not None and not isinstance(
            execution_binding,
            FencedActorExecutionBinding,
        ):
            raise TypeError("execution_binding must be a FencedActorExecutionBinding")
        if session_registry is not None and not callable(
            getattr(session_registry, "wake", None)
        ):
            raise TypeError("session_registry must implement wake(key)")
        if execution_binding is None and session_registry is None:
            raise ValueError("an unfenced effect executor requires a session_registry")
        if execution_binding is not None and session_registry is not None:
            raise ValueError(
                "a fenced effect executor must not receive a legacy session_registry"
            )
        if execution_binding is not None and renew_interval_seconds is None:
            raise ValueError(
                "a fenced effect executor requires automatic lease renewal"
            )
        self._store = store
        self._handlers = handlers
        self._effect_contract_authority: EffectContractAuthority | None = None
        self._persistence_domain: object | None = None
        self._session_registry = session_registry
        self._mailbox_handoff_notifier = mailbox_handoff_notifier
        self.worker_id = str(worker_id or f"effect-executor:{uuid.uuid4().hex}")
        self._worker_count = worker_count
        self._control_worker_count = control_worker_count
        self._orphan_worker_count = orphan_worker_count
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._renew_interval_seconds = renew_interval_seconds
        self._review_execution_gate_store = review_execution_gate_store
        self._model_execution_witness_store = model_execution_witness_store
        self._execution_binding = execution_binding
        self._clock = clock or time.time
        self._lane_wake_events = {lane: asyncio.Event() for lane in EffectLane}
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self._start_lock = asyncio.Lock()
        self._legacy_wake_recovery_lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._cancellation_tails: set[asyncio.Task[Any]] = set()
        self._review_execution_finish_tasks: set[asyncio.Task[ReviewExecutionPermit]] = set()
        self._model_execution_finish_tasks: set[asyncio.Task[ModelExecutionPermit]] = set()
        self._in_flight_handler_tasks: dict[
            InFlightEffectHandlerKey,
            _InFlightEffectHandler,
        ] = {}
        self._active_claims: dict[str, ClaimedEffect] = {}
        self._pending_legacy_wakes: set[SessionKey] = set()
        self._binding_failure: EffectStoreBindingChanged | None = None
        self._closing = False
        self._drain_on_shutdown = False
        self._recover_expired_claims = True
        self._fenced_history_recovery_active = False

    @property
    def started(self) -> bool:
        """Return whether at least one handler-bound worker is live.

        Orphan workers may run to terminally settle genuinely unknown durable
        effects.  They do not make this executor capable of handling any
        known effect contract and therefore do not make it ``started``.
        """

        return self.healthy and any(not task.done() for task in self._handler_tasks)

    @property
    def running(self) -> bool:
        """Return whether any worker, including an orphan worker, is live."""

        return self.healthy and any(not task.done() for task in self._tasks)

    @property
    def healthy(self) -> bool:
        """Return whether no fatal composition drift has been observed."""

        return self._binding_failure is None

    @property
    def binding_failure(self) -> EffectStoreBindingChanged | None:
        """Return the fatal store-binding failure, if one was observed."""

        return self._binding_failure

    @property
    def has_runnable_handlers(self) -> bool:
        """Return whether the registry currently has a non-orphan handler lane."""

        return any(lane is not EffectLane.ORPHAN for lane in self._handlers.handled_lanes())

    @property
    def handler_registry(self) -> EffectHandlerRegistry:
        """Return the handler registry whose contracts this executor runs."""

        return self._handlers

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the exact authority used by the durable effect store.

        Resolution is intentionally lazy so isolated executor test doubles can
        be assembled without starting runtime work. The Actor runtime harness
        always resolves and validates this property before recovery or workers.
        """

        authority = self._store.effect_contract_authority
        if not isinstance(authority, EffectContractAuthority):
            raise TypeError("durable effect store must expose an EffectContractAuthority")
        if not authority.sealed:
            raise TypeError("durable effect store authority must be sealed")
        composed_authority = self._effect_contract_authority
        if composed_authority is None:
            self._effect_contract_authority = authority
        elif authority is not composed_authority:
            raise EffectAuthorityChanged(
                "durable effect store changed authority after executor composition"
            )
        return authority

    @property
    def session_registry(self) -> SessionActorWakeTarget | None:
        """Return the legacy key-only wake target, if this executor has one."""

        return self._session_registry

    @property
    def persistence_domain(self) -> object:
        """Return the exact transaction domain used by the effect store."""

        domain = self._store.persistence_domain
        if domain is None:
            raise TypeError("durable effect store persistence_domain must not be None")
        composed_domain = self._persistence_domain
        if composed_domain is None:
            self._persistence_domain = domain
        elif domain is not composed_domain:
            raise EffectStoreBindingChanged(
                "durable effect store changed persistence domain after composition"
            )
        return domain

    def _validate_store_binding(self) -> None:
        """Verify authority and persistence identities remain immutable."""

        _ = self.effect_contract_authority
        persistence_domain = self.persistence_domain
        gate_store = self._review_execution_gate_store
        if gate_store is not None:
            gate_domain = gate_store.persistence_domain
            if gate_domain is None:
                raise TypeError("review execution gate store persistence_domain must not be None")
            if gate_domain is not persistence_domain:
                raise EffectStoreBindingChanged(
                    "review execution gate store uses a different persistence domain"
                )
        model_witness_store = self._model_execution_witness_store
        if model_witness_store is not None:
            witness_domain = model_witness_store.persistence_domain
            if witness_domain is None:
                raise TypeError("model execution witness store persistence_domain must not be None")
            if witness_domain is not persistence_domain:
                raise EffectStoreBindingChanged(
                    "model execution witness store uses a different persistence domain"
                )

    @staticmethod
    def _validate_fenced_recovery_result(
        result: EffectExpiryRecoveryResult,
        execution_binding: FencedActorExecutionBinding,
    ) -> None:
        """Reject a scoped recovery result that widens its owner identity."""

        request = execution_binding.request
        for notification in result.notifications:
            if (
                notification.key != request.key
                or notification.wake_request != request
                or notification.mailbox_id is None
            ):
                raise RuntimeError(
                    "fenced effect recovery returned a mailbox outside its execution binding"
                )

    @property
    def closed(self) -> bool:
        """Return whether executor shutdown has begun."""

        return self._closing

    @property
    def execution_binding(self) -> FencedActorExecutionBinding | None:
        """Return the optional durable target lease capability for this executor."""

        return self._execution_binding

    def wake(self) -> None:
        """Notify workers that committed outbox effects may be available."""

        if not self._closing:
            self._idle_event.clear()
            for wake_event in self._lane_wake_events.values():
                wake_event.set()

    async def start(self) -> int:
        """Recover expired claims and start supervised effect workers."""

        if self._execution_binding is not None:
            raise RuntimeError("a fenced effect executor must use start_fenced")
        return await self._start(recover_expired=True)

    async def start_clean_session(self) -> int:
        """Start workers after a harness has proven an empty durable domain.

        This intentionally does not release expired claims before or during
        polling. It is a narrow lifecycle primitive for
        :class:`ActorRuntimeHarness`; callers still need an external ingress
        and ownership controller before they can create new durable work.
        """

        if self._execution_binding is not None:
            raise RuntimeError("a fenced effect executor must use start_fenced")
        return await self._start(recover_expired=False)

    async def start_fenced(self) -> int:
        """Start one scoped executor without broad expired-effect recovery.

        A target lease grants authority for one current owner request only. It
        cannot be used to scan or recover other sessions' historical effects.
        A later lifecycle controller must provide a separately fenced recovery
        protocol before restarting model/external-action work after a crash.
        """

        if self._execution_binding is None:
            raise RuntimeError("start_fenced requires an execution_binding")
        return await self._start(recover_expired=False)

    async def recover_fenced_history(self) -> EffectExpiryRecoveryResult:
        """Recover one inactive target's expired effects without waking by key.

        This is deliberately a pre-start lifecycle primitive. It does not
        start workers, drain arbitrary store notifications, or call a mailbox
        notifier. A caller must subsequently bind a fenced handoff dispatcher
        that can redrive the exact returned mailbox sidecars.
        """

        execution_binding = self._execution_binding
        if execution_binding is None:
            raise RuntimeError("fenced effect history recovery requires an execution_binding")
        if not isinstance(self._store, FencedEffectRecoveryStore):
            raise TypeError(
                "durable effect store does not support explicit fenced history recovery"
            )
        async with self._start_lock:
            self._validate_store_binding()
            if self._tasks:
                raise RuntimeError(
                    "fenced effect history recovery must finish before executor workers start"
                )
            if self._active_claims:
                raise RuntimeError(
                    "fenced effect history recovery cannot overlap an active effect claim"
                )
            if self._closing:
                raise RuntimeError("a closed durable effect executor cannot recover history")
            self._fenced_history_recovery_active = True
            try:
                result = await self._store.recover_expired_fenced(
                    worker_id=self.worker_id,
                    execution_binding=execution_binding,
                )
                if not isinstance(result, EffectExpiryRecoveryResult):
                    raise TypeError("fenced effect recovery store returned an invalid result")
                self._validate_fenced_recovery_result(result, execution_binding)
                # This query is a final target-lease validation after the store's
                # recovery transaction. Its value is intentionally irrelevant.
                await self._store.next_available_at(execution_binding=execution_binding)
                self._validate_store_binding()
                return result
            finally:
                self._fenced_history_recovery_active = False

    async def _start(self, *, recover_expired: bool) -> int:
        """Start worker tasks under one already-selected recovery policy."""

        async with self._start_lock:
            self._validate_store_binding()
            if self._tasks:
                return 0
            if self._closing:
                raise RuntimeError("a closed durable effect executor cannot be restarted")
            self._recover_expired_claims = bool(recover_expired)
            recovered = 0
            if self._execution_binding is not None:
                # This is a read-only liveness boundary. It prevents a target
                # that has already lost its lease from spawning even a single
                # handler task, while deliberately avoiding broad recovery.
                await self._store.next_available_at(
                    execution_binding=self._execution_binding,
                )
            if self._recover_expired_claims and self._execution_binding is None:
                recovered = await self._store.recover_expired(worker_id=self.worker_id)
            self._validate_store_binding()
            tasks: list[asyncio.Task[None]] = []
            handler_tasks: list[asyncio.Task[None]] = []
            for lane in self._handlers.handled_lanes():
                if lane is EffectLane.ORPHAN:
                    continue
                count = (
                    self._control_worker_count if lane is EffectLane.CONTROL else self._worker_count
                )
                lane_tasks = [
                    asyncio.create_task(
                        self._worker_loop(lane, index),
                        name=(f"agent-effect-executor:{self.worker_id}:{lane.value}:{index}"),
                    )
                    for index in range(count)
                ]
                tasks.extend(lane_tasks)
                handler_tasks.extend(lane_tasks)
            tasks.extend(
                asyncio.create_task(
                    self._worker_loop(EffectLane.ORPHAN, index),
                    name=(
                        f"agent-effect-executor:{self.worker_id}:{EffectLane.ORPHAN.value}:{index}"
                    ),
                )
                for index in range(self._orphan_worker_count)
            )
            self._tasks = tasks
            self._handler_tasks = set(handler_tasks)
            self.wake()
            return recovered

    async def run_once(
        self,
        *,
        worker_id: str | None = None,
        lane: EffectLane | None = None,
    ) -> EffectRunResult:
        """Claim and execute at most one currently available effect."""

        self._require_no_fenced_history_recovery()
        self._validate_store_binding()
        effective_worker_id = worker_id or self.worker_id
        if self._recover_expired_claims and self._execution_binding is None:
            await self._store.recover_expired(worker_id=effective_worker_id)
        self._validate_store_binding()
        await self._recover_pending_legacy_wakes()
        effect_contracts, excluded_effect_contracts = self._claim_filter(lane)
        self._require_no_fenced_history_recovery()
        claim = await self._store.claim_next(
            worker_id=effective_worker_id,
            effect_contracts=effect_contracts,
            excluded_effect_contracts=excluded_effect_contracts,
            **self._execution_binding_kwargs(),
        )
        if claim is not None:
            self._idle_event.clear()
            self._active_claims[claim.claim_id] = claim
        try:
            await self._drain_store_notifications()
        except asyncio.CancelledError:
            if claim is not None:
                await self._release_after_cancellation(claim)
                self._active_claims.pop(claim.claim_id, None)
            raise
        except EffectStoreBindingChanged:
            if claim is not None:
                self._active_claims.pop(claim.claim_id, None)
            raise
        except Exception:
            if claim is not None:
                await self._release_unstarted_claim(claim)
                self._active_claims.pop(claim.claim_id, None)
            raise
        if claim is None:
            if not self._active_claims:
                self._idle_event.set()
            return EffectRunResult(status=EffectRunStatus.EMPTY)
        try:
            return await self._execute_claim(claim, lane=lane)
        finally:
            self._active_claims.pop(claim.claim_id, None)

    def _require_no_fenced_history_recovery(self) -> None:
        """Reject ad hoc execution while target-history recovery owns the scope."""

        if self._fenced_history_recovery_active:
            raise RuntimeError(
                "effect execution cannot overlap fenced effect history recovery"
            )

    async def wait_idle(self) -> None:
        """Wait until no worker owns a claim and no effect is immediately claimable."""

        await self._idle_event.wait()

    def local_in_flight_handler_keys(
        self,
        *,
        key: SessionKey,
        operation_id: str,
        effect_kind: str | None = None,
        effect_id: str | None = None,
    ) -> tuple[InFlightEffectHandlerKey, ...]:
        """Return currently live handler identities for one local operation.

        The returned identities are drawn only from handler tasks created by
        this executor instance. An empty tuple is not a cross-process lease or
        side-effect safety proof; callers need durable cancellation evidence
        before treating an operation as globally stopped.
        """

        normalized_key = self._require_session_key(key)
        normalized_operation_id = self._require_operation_id(operation_id)
        normalized_effect_kind = self._optional_effect_identity_part(
            effect_kind,
            field_name="effect_kind",
        )
        normalized_effect_id = self._optional_effect_identity_part(
            effect_id,
            field_name="effect_id",
        )
        return tuple(
            tracked.identity
            for tracked in self._matching_local_handler_tasks(
                key=normalized_key,
                operation_id=normalized_operation_id,
                effect_kind=normalized_effect_kind,
                effect_id=normalized_effect_id,
            )
        )

    async def ensure_local_operation_quiescent(
        self,
        *,
        key: SessionKey,
        operation_id: str,
        cancel: bool = True,
        timeout_seconds: float | None = None,
        effect_kind: str | None = None,
        effect_id: str | None = None,
    ) -> LocalOperationQuiescence:
        """Cancel and await this executor's live handlers for one operation.

        This method is intentionally useful only as a local execution-control
        primitive. It revokes matching handler contexts before requesting task
        cancellation, then waits for those actual task objects to finish. A
        durable control handler must still obtain store-owned proof before it
        can claim cross-process operation quiescence.

        Args:
            key: Exact durable session that owns the operation.
            operation_id: Exact durable operation identifier to inspect.
            cancel: Whether to request cancellation before waiting. ``False``
                only observes natural completion.
            timeout_seconds: Optional local wait bound. Timeout does not
                unregister or discard still-running handler tasks.
            effect_kind: Optional exact durable effect kind filter.
            effect_id: Optional exact durable effect id filter.

        Returns:
            A report explicitly scoped to this process. ``NO_LOCAL_HANDLER_TASKS``
            means no matching local task was found. ``QUIESCENT`` means one or
            more locally observed tasks finished before the report. Neither is
            a distributed quiescence proof.
        """

        normalized_key = self._require_session_key(key)
        normalized_operation_id = self._require_operation_id(operation_id)
        normalized_effect_kind = self._optional_effect_identity_part(
            effect_kind,
            field_name="effect_kind",
        )
        normalized_effect_id = self._optional_effect_identity_part(
            effect_id,
            field_name="effect_id",
        )
        timeout = self._validate_quiescence_timeout(timeout_seconds)
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        matched: dict[InFlightEffectHandlerKey, None] = {}
        cancelled: dict[InFlightEffectHandlerKey, None] = {}

        while True:
            tracked_handlers = self._matching_local_handler_tasks(
                key=normalized_key,
                operation_id=normalized_operation_id,
                effect_kind=normalized_effect_kind,
                effect_id=normalized_effect_id,
            )
            if not tracked_handlers:
                status = (
                    LocalOperationQuiescenceStatus.QUIESCENT
                    if matched
                    else LocalOperationQuiescenceStatus.NO_LOCAL_HANDLER_TASKS
                )
                return self._local_operation_quiescence_report(
                    status=status,
                    key=normalized_key,
                    operation_id=normalized_operation_id,
                    matched=matched,
                    cancelled=cancelled,
                )

            current_task = asyncio.current_task()
            if any(tracked.task is current_task for tracked in tracked_handlers):
                raise RuntimeError("an effect handler cannot wait for its own operation quiescence")

            for tracked in tracked_handlers:
                matched.setdefault(tracked.identity, None)
                if cancel and self._cancel_in_flight_handler_task(tracked):
                    cancelled.setdefault(tracked.identity, None)

            remaining_timeout = self._remaining_quiescence_timeout(deadline)
            _done, pending = await asyncio.wait(
                tuple(tracked.task for tracked in tracked_handlers),
                timeout=remaining_timeout,
            )
            if pending:
                remaining = self._matching_local_handler_tasks(
                    key=normalized_key,
                    operation_id=normalized_operation_id,
                    effect_kind=normalized_effect_kind,
                    effect_id=normalized_effect_id,
                )
                if not remaining:
                    continue
                return self._local_operation_quiescence_report(
                    status=LocalOperationQuiescenceStatus.TIMED_OUT,
                    key=normalized_key,
                    operation_id=normalized_operation_id,
                    matched=matched,
                    cancelled=cancelled,
                    remaining=remaining,
                )

    async def ensure_local_executor_quiescent(
        self,
        *,
        cancel: bool = True,
        timeout_seconds: float | None = None,
    ) -> LocalEffectExecutorQuiescence:
        """Cancel and observe every handler task owned by this executor.

        This is a local task observation, not a distributed stop proof. A
        target-retirement controller must first stop this executor from
        claiming new work, then require a ``QUIESCENT`` report before it may
        release the target lease that authorizes those tasks.

        Args:
            cancel: Whether to request cancellation before waiting. ``False``
                only observes natural completion.
            timeout_seconds: Optional local wait bound. A timeout leaves live
                tasks registered so a later caller can continue observing them.

        Returns:
            A deterministic report of tasks observed by this process. An empty
            lookup is intentionally distinct from confirmed quiescence.
        """

        timeout = self._validate_quiescence_timeout(timeout_seconds)
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        matched: dict[InFlightEffectHandlerKey, None] = {}
        cancelled: dict[InFlightEffectHandlerKey, None] = {}

        while True:
            tracked_handlers = self._all_local_handler_tasks()
            if not tracked_handlers:
                status = (
                    LocalOperationQuiescenceStatus.QUIESCENT
                    if matched
                    else LocalOperationQuiescenceStatus.NO_LOCAL_HANDLER_TASKS
                )
                return self._local_executor_quiescence_report(
                    status=status,
                    matched=matched,
                    cancelled=cancelled,
                )

            current_task = asyncio.current_task()
            if any(tracked.task is current_task for tracked in tracked_handlers):
                raise RuntimeError("an effect handler cannot wait for executor quiescence")

            for tracked in tracked_handlers:
                matched.setdefault(tracked.identity, None)
                if cancel and self._cancel_in_flight_handler_task(tracked):
                    cancelled.setdefault(tracked.identity, None)

            remaining_timeout = self._remaining_quiescence_timeout(deadline)
            _done, pending = await asyncio.wait(
                tuple(tracked.task for tracked in tracked_handlers),
                timeout=remaining_timeout,
            )
            if pending:
                remaining = self._all_local_handler_tasks()
                if not remaining:
                    continue
                return self._local_executor_quiescence_report(
                    status=LocalOperationQuiescenceStatus.TIMED_OUT,
                    matched=matched,
                    cancelled=cancelled,
                    remaining=remaining,
                )

    async def shutdown(self, *, drain: bool = False) -> None:
        """Stop workers and leave every interrupted claim durably recoverable.

        Args:
            drain: Finish current work and all currently claimable effects before
                stopping. Future delayed retries remain durable for the next start.
        """

        if self._closing:
            tasks = list(self._tasks)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            return
        self._closing = True
        self._drain_on_shutdown = drain
        tasks = list(self._tasks)
        for wake_event in self._lane_wake_events.values():
            wake_event.set()
        if not drain:
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for tail in list(self._cancellation_tails):
            tail.cancel()
        # A review start witness may only be acknowledged after its real
        # handler task ends. Do not cancel these finish tasks during shutdown;
        # doing so would turn a local stop into a false distributed quiescence
        # proof on the next process.
        review_finish_tasks = list(self._review_execution_finish_tasks)
        model_finish_tasks = list(self._model_execution_finish_tasks)
        if review_finish_tasks or model_finish_tasks:
            await asyncio.gather(
                *review_finish_tasks,
                *model_finish_tasks,
                return_exceptions=True,
            )
        self._tasks.clear()
        self._handler_tasks.clear()
        self._active_claims.clear()
        self._idle_event.set()

    async def _worker_loop(self, lane: EffectLane, index: int) -> None:
        worker_id = f"{self.worker_id}:{lane.value}:{index}"
        while True:
            if not self.healthy:
                return
            if self._closing and not self._drain_on_shutdown:
                return
            try:
                result = await self.run_once(worker_id=worker_id, lane=lane)
            except asyncio.CancelledError:
                raise
            except EffectStoreBindingChanged as exc:
                self._mark_binding_failure(exc)
                logger.critical(
                    "durable effect executor stopped after store binding drift",
                    extra={"worker_id": worker_id, "error": _error_text(exc)},
                )
                return
            except Exception:
                logger.exception("durable effect worker iteration failed")
                result = EffectRunResult(status=EffectRunStatus.EMPTY)
            if result.status != EffectRunStatus.EMPTY:
                continue
            if self._closing:
                return
            try:
                await self._recover_pending_legacy_wakes()
                await self._wait_for_work(lane)
            except EffectStoreBindingChanged as exc:
                self._mark_binding_failure(exc)
                logger.critical(
                    "durable effect executor stopped after store binding drift",
                    extra={"worker_id": worker_id, "error": _error_text(exc)},
                )
                return

    async def _wait_for_work(self, lane: EffectLane) -> None:
        wake_event = self._lane_wake_events[lane]
        wake_event.clear()
        timeout = self._poll_interval_seconds
        try:
            effect_contracts, excluded_effect_contracts = self._claim_filter(lane)
            next_available_at = await self._store.next_available_at(
                effect_contracts=effect_contracts,
                excluded_effect_contracts=excluded_effect_contracts,
                **self._execution_binding_kwargs(),
            )
            self._validate_store_binding()
        except EffectStoreBindingChanged:
            raise
        except Exception:
            logger.exception("failed to inspect durable effect availability")
        else:
            if next_available_at is not None:
                timeout = min(timeout, max(0.0, next_available_at - self._clock()))
        if timeout <= 0:
            return
        try:
            await asyncio.wait_for(wake_event.wait(), timeout=timeout)
        except TimeoutError:
            pass

    async def _execute_claim(
        self,
        initial_claim: ClaimedEffect,
        *,
        lane: EffectLane | None,
    ) -> EffectRunResult:
        self._validate_store_binding()
        self._validate_claim_execution_binding(initial_claim)
        context = EffectExecutionContext(
            self._store,
            initial_claim,
            execution_binding=self._execution_binding,
        )
        try:
            contract = self._handlers.contract_for(
                initial_claim.effect.kind,
                initial_claim.effect.contract_version,
            )
        except EffectHandlerNotFound as exc:
            return await self._quarantine_claim(
                initial_claim,
                reason=EffectQuarantineReason.UNSUPPORTED_CONTRACT,
                message=_error_text(exc),
            )
        if initial_claim.effect.contract_signature != contract.signature:
            exc = EffectContractSignatureMismatch(
                "persisted effect contract signature does not match "
                f"{contract.effect_kind}:v{contract.version}"
            )
            return await self._quarantine_claim(
                initial_claim,
                reason=EffectQuarantineReason.CONTRACT_SIGNATURE_MISMATCH,
                message=_error_text(exc),
            )
        missing_fences = _missing_explicit_outcome_fences(
            initial_claim.effect,
            contract,
        )
        if missing_fences:
            return await self._quarantine_claim(
                initial_claim,
                reason=EffectQuarantineReason.OUTCOME_FENCE_MISSING,
                message=(
                    "explicit effect contract payload is missing outcome fences: "
                    + ", ".join(missing_fences)
                ),
            )
        try:
            resolved_contract, handler = self._handlers.resolve(
                initial_claim.effect.kind,
                initial_claim.effect.contract_version,
            )
        except EffectHandlerNotFound as exc:
            return await self._quarantine_claim(
                initial_claim,
                reason=EffectQuarantineReason.UNSUPPORTED_CONTRACT,
                message=_error_text(exc),
            )
        if resolved_contract != contract:
            return await self._quarantine_claim(
                initial_claim,
                reason=EffectQuarantineReason.CONTRACT_RESOLUTION_INCONSISTENT,
                message="effect handler resolved a different durable contract",
            )
        if lane is not None and lane is not contract.lane:
            return await self._quarantine_claim(
                initial_claim,
                reason=EffectQuarantineReason.LANE_MISMATCH,
                message="effect store returned work owned by a different lane",
            )
        try:
            self._validate_store_binding()
            handler_result = await self._run_handler(handler, context, contract)
            self._validate_store_binding()
            completion = self._completion_envelope(
                context.claim,
                handler_result,
                contract,
            )
            settlement = await self._store.complete_with_event(
                context.claim,
                completion,
                outcome_fence_fields=resolved_outcome_fence_fields(contract),
                **self._execution_binding_kwargs(),
            )
            self._validate_store_binding()
        except asyncio.CancelledError:
            if not context.model_execution_witness_started:
                await self._release_after_cancellation(context.claim)
            raise
        except FencedWakeTargetLeaseError as exc:
            context.revoke()
            raise FencedEffectExecutionLeaseLost(
                "fenced effect execution lost target lease authority"
            ) from exc
        except EffectStoreBindingChanged:
            context.revoke()
            raise
        except EffectClaimLost as exc:
            return self._claim_lost_result(context.claim, exc)
        except EffectExecutionCancelled as exc:
            return EffectRunResult(
                status=EffectRunStatus.CANCELLED,
                effect_id=context.claim.effect.effect_id,
                attempt_count=context.claim.attempt_count,
                error=exc.reason,
            )
        except EffectExecutionDeferred as exc:
            return await self._defer_without_attempt(context.claim, exc)
        except Exception as exc:
            return await self._retry_or_fail(
                context.claim,
                exc,
                contract,
                force_terminal=context.model_execution_witness_started,
            )

        if settlement.status is EffectSettlementStatus.CANCELLED:
            return EffectRunResult(
                status=EffectRunStatus.CANCELLED,
                effect_id=context.claim.effect.effect_id,
                attempt_count=context.claim.attempt_count,
            )
        self._validate_settlement(context.claim, completion, settlement)
        await self._wake_after_settlement(settlement)
        run_status = (
            EffectRunStatus.SKIPPED
            if settlement.status == EffectSettlementStatus.PRECONDITION_SKIPPED
            else EffectRunStatus.COMPLETED
        )
        return EffectRunResult(
            status=run_status,
            effect_id=context.claim.effect.effect_id,
            event_id=settlement.event_id,
            attempt_count=context.claim.attempt_count,
        )

    async def _run_handler(
        self,
        handler: EffectHandler,
        context: EffectExecutionContext,
        contract: EffectExecutionContract,
    ) -> EffectHandlerResult:
        if self._execution_binding is not None:
            # Every scoped handler, including control-only handlers that do
            # not create a model witness or external-action receipt, crosses
            # one fresh target-lease check before Python work can begin.
            await context.renew_lease()
        review_claim = await self._begin_review_execution(context)
        model_claim = await self._begin_model_execution(context)
        try:
            handler_task = asyncio.create_task(
                handler(context),
                name=f"agent-effect-handler:{context.effect.effect_id}",
            )
        except BaseException:
            if model_claim is not None:
                await self._finish_model_execution_without_task(model_claim)
            if review_claim is not None:
                permit = await self._finish_review_execution_without_task(review_claim)
                self._raise_if_review_execution_cancelled(permit)
            raise

        review_finish_task = (
            self._start_review_execution_finish_task(handler_task, review_claim)
            if review_claim is not None
            else None
        )
        model_finish_task = (
            self._start_model_execution_finish_task(handler_task, model_claim)
            if model_claim is not None
            else None
        )
        handler_identity: InFlightEffectHandlerKey | None = None
        renew_task: asyncio.Task[None] | None = None
        timeout_task: asyncio.Task[None] | None = None
        result: EffectHandlerResult | None = None
        try:
            handler_identity = self._register_in_flight_handler_task(
                context,
                handler_task,
            )
            if self._renew_interval_seconds is not None:
                renew_task = asyncio.create_task(
                    self._renew_while_running(context, handler_task),
                    name=f"agent-effect-renew:{context.effect.effect_id}",
                )
            timeout_task = asyncio.create_task(
                asyncio.sleep(contract.timeout_seconds),
                name=f"agent-effect-timeout:{context.effect.effect_id}",
            )
            watched: set[asyncio.Task[Any]] = {handler_task, timeout_task}
            if renew_task is not None:
                watched.add(renew_task)
            done, _pending = await asyncio.wait(
                watched,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if timeout_task in done and not handler_task.done():
                context.revoke()
                handler_task.cancel()
                if review_finish_task is None:
                    self._observe_cancellation_tail(handler_task)
                raise TimeoutError(f"effect handler exceeded {contract.timeout_seconds:g} seconds")
            if renew_task is not None and renew_task in done and not handler_task.done():
                renewal_error = renew_task.exception()
                context.revoke()
                handler_task.cancel()
                if review_finish_task is None:
                    self._observe_cancellation_tail(handler_task)
                if renewal_error is None:
                    raise EffectClaimLost("lease renewal stopped before handler completion")
                raise renewal_error
            result = await handler_task
            if not isinstance(result, EffectHandlerResult):
                raise TypeError("effect handlers must return EffectHandlerResult")
        finally:
            if timeout_task is not None:
                timeout_task.cancel()
            if renew_task is not None and not renew_task.done():
                renew_task.cancel()
            if renew_task is not None:
                await asyncio.gather(renew_task, return_exceptions=True)
            if not handler_task.done():
                context.revoke()
                handler_task.cancel()
                if review_finish_task is None:
                    self._observe_cancellation_tail(handler_task)
            if review_finish_task is not None:
                permit = await self._await_review_execution_finish(review_finish_task)
                self._raise_if_review_execution_cancelled(permit)
            if model_finish_task is not None:
                permit = await self._await_model_execution_finish(model_finish_task)
                self._raise_if_model_execution_deferred(permit)
            elif handler_identity is not None and handler_task.done():
                self._unregister_in_flight_handler_task(
                    handler_identity,
                    handler_task,
                )
            if timeout_task is not None:
                await asyncio.gather(timeout_task, return_exceptions=True)

        assert result is not None
        return result

    async def _begin_review_execution(
        self,
        context: EffectExecutionContext,
    ) -> ReviewExecutionClaim | None:
        """Persist a review start witness before creating its handler task."""

        if context.effect.kind != "run_review_workflow":
            return None
        if self._renew_interval_seconds is None:
            raise EffectExecutionConfigurationError(
                "run_review_workflow requires automatic effect lease renewal"
            )
        gate_store = self._review_execution_gate_store
        if gate_store is None:
            raise EffectExecutionDeferred(
                "review_execution_gate_store_unconfigured",
                delay_seconds=1.0,
            )
        claim = ReviewExecutionClaim(
            key=context.claim.key,
            ownership_generation=context.effect.ownership_generation,
            review_effect_id=context.effect.effect_id,
            review_operation_id=context.effect.operation_id,
            review_effect_kind=context.effect.kind,
            review_contract_version=context.effect.contract_version,
            review_contract_signature=context.effect.contract_signature,
            claim_id=context.claim.claim_id,
            worker_id=context.claim.worker_id,
        )
        permit = await gate_store.begin_execution(
            claim,
            **self._execution_binding_kwargs(),
        )
        if permit.claim != claim:
            raise EffectStoreBindingChanged(
                "review execution gate store returned a different claim identity"
            )
        if permit.cancelled:
            raise EffectExecutionCancelled(
                "review_execution_cancelled_before_task_start:" + permit.cancellation_effect_id
            )
        if permit.deferred:
            raise EffectExecutionDeferred(permit.blocker_code, delay_seconds=1.0)
        if permit.disposition is not ReviewExecutionPermitDisposition.STARTED:
            raise EffectStoreBindingChanged(
                "review execution gate store returned an unsupported permit"
            )
        return claim

    async def _begin_model_execution(
        self,
        context: EffectExecutionContext,
    ) -> ModelExecutionClaim | None:
        """Persist a non-review model start witness before creating its task."""

        if context.effect.kind not in MODEL_EXECUTION_WITNESSED_EFFECT_KINDS:
            return None
        witness_store = self._model_execution_witness_store
        if witness_store is None:
            return None
        claim = ModelExecutionClaim(
            key=context.claim.key,
            ownership_generation=context.effect.ownership_generation,
            effect_id=context.effect.effect_id,
            operation_id=context.effect.operation_id,
            effect_kind=context.effect.kind,
            contract_version=context.effect.contract_version,
            contract_signature=context.effect.contract_signature,
            claim_id=context.claim.claim_id,
            worker_id=context.claim.worker_id,
        )
        permit = await witness_store.begin_execution(
            claim,
            **self._execution_binding_kwargs(),
        )
        if permit.claim != claim:
            raise EffectStoreBindingChanged(
                "model execution witness store returned a different claim identity"
            )
        if permit.cancelled:
            raise EffectExecutionCancelled(
                "model_execution_cancelled_before_task_start:" + permit.cancellation_effect_id
            )
        if permit.deferred:
            raise EffectExecutionDeferred(permit.blocker_code, delay_seconds=1.0)
        if permit.disposition is not ModelExecutionPermitDisposition.STARTED:
            raise EffectStoreBindingChanged(
                "model execution witness store returned an unsupported permit"
            )
        context.mark_model_execution_witness_started()
        return claim

    async def _finish_review_execution_without_task(
        self,
        claim: ReviewExecutionClaim,
    ) -> ReviewExecutionPermit:
        """Close a start witness after task creation itself failed."""

        gate_store = self._review_execution_gate_store
        assert gate_store is not None
        return await gate_store.finish_execution(
            claim,
            **self._execution_binding_kwargs(),
        )

    async def _finish_model_execution_without_task(
        self,
        claim: ModelExecutionClaim,
    ) -> ModelExecutionPermit:
        """Close a generic start witness after task creation itself failed."""

        witness_store = self._model_execution_witness_store
        assert witness_store is not None
        return await witness_store.finish_execution(
            claim,
            **self._execution_binding_kwargs(),
        )

    def _start_review_execution_finish_task(
        self,
        handler_task: asyncio.Task[EffectHandlerResult],
        claim: ReviewExecutionClaim,
    ) -> asyncio.Task[ReviewExecutionPermit]:
        """Ensure a review witness is finished only after its real task ends."""

        finish_task = asyncio.create_task(
            self._finish_review_execution_after_handler(handler_task, claim),
            name=f"agent-review-execution-finish:{claim.review_effect_id}:{claim.claim_id}",
        )
        self._review_execution_finish_tasks.add(finish_task)

        def _finished(completed: asyncio.Task[ReviewExecutionPermit]) -> None:
            self._review_execution_finish_tasks.discard(completed)
            if completed.cancelled():
                logger.critical(
                    "review execution finish witness task was cancelled",
                    extra={"effect_id": claim.review_effect_id, "claim_id": claim.claim_id},
                )
                return
            try:
                error = completed.exception()
            except asyncio.CancelledError:
                return
            if error is not None:
                logger.error(
                    "failed to persist review execution finish witness",
                    extra={
                        "effect_id": claim.review_effect_id,
                        "claim_id": claim.claim_id,
                    },
                    exc_info=(type(error), error, error.__traceback__),
                )

        finish_task.add_done_callback(_finished)
        return finish_task

    def _start_model_execution_finish_task(
        self,
        handler_task: asyncio.Task[EffectHandlerResult],
        claim: ModelExecutionClaim,
    ) -> asyncio.Task[ModelExecutionPermit]:
        """Persist generic finish evidence only after the real task exits."""

        finish_task = asyncio.create_task(
            self._finish_model_execution_after_handler(handler_task, claim),
            name=f"agent-model-execution-finish:{claim.effect_id}:{claim.claim_id}",
        )
        self._model_execution_finish_tasks.add(finish_task)

        def _finished(completed: asyncio.Task[ModelExecutionPermit]) -> None:
            self._model_execution_finish_tasks.discard(completed)
            if completed.cancelled():
                logger.critical(
                    "model execution finish witness task was cancelled",
                    extra={"effect_id": claim.effect_id, "claim_id": claim.claim_id},
                )
                return
            try:
                error = completed.exception()
            except asyncio.CancelledError:
                return
            if error is not None:
                logger.error(
                    "failed to persist model execution finish witness",
                    extra={"effect_id": claim.effect_id, "claim_id": claim.claim_id},
                    exc_info=(type(error), error, error.__traceback__),
                )

        finish_task.add_done_callback(_finished)
        return finish_task

    async def _finish_review_execution_after_handler(
        self,
        handler_task: asyncio.Task[EffectHandlerResult],
        claim: ReviewExecutionClaim,
    ) -> ReviewExecutionPermit:
        """Wait for an actual task exit, then record the durable finish witness."""

        try:
            await asyncio.shield(handler_task)
        except BaseException:
            # A handler exception or its own cancellation makes its task done.
            # Cancellation of this observer does not: leaving the witness
            # running is the only honest durable result while the handler may
            # still be alive.
            if not handler_task.done():
                raise
        gate_store = self._review_execution_gate_store
        assert gate_store is not None
        return await gate_store.finish_execution(
            claim,
            **self._execution_binding_kwargs(),
        )

    async def _finish_model_execution_after_handler(
        self,
        handler_task: asyncio.Task[EffectHandlerResult],
        claim: ModelExecutionClaim,
    ) -> ModelExecutionPermit:
        """Wait for a task exit, then persist its generic model finish witness."""

        try:
            await asyncio.shield(handler_task)
        except BaseException:
            if not handler_task.done():
                raise
        witness_store = self._model_execution_witness_store
        assert witness_store is not None
        return await witness_store.finish_execution(
            claim,
            **self._execution_binding_kwargs(),
        )

    @staticmethod
    async def _await_review_execution_finish(
        finish_task: asyncio.Task[ReviewExecutionPermit],
    ) -> ReviewExecutionPermit:
        """Await a durable finish tail without cancelling it with the caller."""

        return await asyncio.shield(finish_task)

    @staticmethod
    async def _await_model_execution_finish(
        finish_task: asyncio.Task[ModelExecutionPermit],
    ) -> ModelExecutionPermit:
        """Await generic finish evidence without cancelling its observer tail."""

        return await asyncio.shield(finish_task)

    @staticmethod
    def _raise_if_review_execution_cancelled(
        permit: ReviewExecutionPermit,
    ) -> None:
        """Turn a fenced finish permit into the executor's no-mailbox outcome."""

        if permit.cancelled:
            raise EffectExecutionCancelled(
                "review_execution_cancelled_after_task_exit:" + permit.cancellation_effect_id
            )
        if permit.disposition is not ReviewExecutionPermitDisposition.STARTED:
            raise EffectStoreBindingChanged(
                "review execution finish returned an unsupported permit"
            )

    @staticmethod
    def _raise_if_model_execution_deferred(
        permit: ModelExecutionPermit,
    ) -> None:
        """Keep unsettled generic evidence in processing rather than retrying it."""

        if permit.cancelled:
            raise EffectExecutionCancelled(
                "model_execution_cancelled_after_task_exit:" + permit.cancellation_effect_id
            )
        if permit.deferred:
            raise EffectClaimLost(
                "model execution witness became unresolved: " + permit.blocker_code
            )
        if permit.disposition is not ModelExecutionPermitDisposition.STARTED:
            raise EffectStoreBindingChanged("model execution finish returned an unsupported permit")

    async def _renew_while_running(
        self,
        context: EffectExecutionContext,
        handler_task: asyncio.Task[EffectHandlerResult],
    ) -> None:
        assert self._renew_interval_seconds is not None
        while not handler_task.done():
            await asyncio.sleep(self._renew_interval_seconds)
            if handler_task.done():
                return
            self._validate_store_binding()
            await context.renew_lease()
            self._validate_store_binding()

    async def _retry_or_fail(
        self,
        claim: ClaimedEffect,
        exc: BaseException,
        contract: EffectExecutionContract,
        *,
        force_terminal: bool = False,
    ) -> EffectRunResult:
        self._validate_store_binding()
        error = _error_text(exc)
        if not force_terminal and claim.attempt_count < contract.max_attempts:
            retry_at = self._clock() + self._retry_delay(
                contract,
                claim.attempt_count,
            )
            try:
                release = await self._store.release_for_retry(
                    claim,
                    error=error,
                    available_at=retry_at,
                    **self._execution_binding_kwargs(),
                )
                self._validate_store_binding()
            except EffectClaimLost as claim_exc:
                return self._claim_lost_result(claim, claim_exc)
            if release is not None and release.status is EffectSettlementStatus.CANCELLED:
                return EffectRunResult(
                    status=EffectRunStatus.CANCELLED,
                    effect_id=claim.effect.effect_id,
                    attempt_count=claim.attempt_count,
                    error=error,
                )
            self.wake()
            return EffectRunResult(
                status=EffectRunStatus.RETRY_SCHEDULED,
                effect_id=claim.effect.effect_id,
                attempt_count=claim.attempt_count,
                retry_at=retry_at,
                error=error,
            )

        failure = self._failure_envelope(claim, exc, contract)
        try:
            settlement = await self._store.fail_with_event(
                claim,
                failure,
                error=error,
                outcome_fence_fields=resolved_outcome_fence_fields(contract),
                **self._execution_binding_kwargs(),
            )
            self._validate_store_binding()
        except EffectClaimLost as claim_exc:
            return self._claim_lost_result(claim, claim_exc)
        if settlement.status is EffectSettlementStatus.CANCELLED:
            return EffectRunResult(
                status=EffectRunStatus.CANCELLED,
                effect_id=claim.effect.effect_id,
                attempt_count=claim.attempt_count,
                error=error,
            )
        self._validate_settlement(claim, failure, settlement)
        await self._wake_after_settlement(settlement)
        return EffectRunResult(
            status=EffectRunStatus.FAILED,
            effect_id=claim.effect.effect_id,
            event_id=failure.event_id,
            attempt_count=claim.attempt_count,
            error=error,
        )

    async def _defer_without_attempt(
        self,
        claim: ClaimedEffect,
        exc: EffectExecutionDeferred,
    ) -> EffectRunResult:
        """Persist a non-terminal control wait without burning retry budget."""

        self._validate_store_binding()
        available_at = self._clock() + exc.delay_seconds
        try:
            deferred = await self._store.defer_without_attempt(
                claim,
                reason=exc.reason,
                available_at=available_at,
                **self._execution_binding_kwargs(),
            )
            self._validate_store_binding()
        except EffectClaimLost as claim_exc:
            return self._claim_lost_result(claim, claim_exc)
        if deferred is not None and deferred.status is EffectSettlementStatus.CANCELLED:
            return EffectRunResult(
                status=EffectRunStatus.CANCELLED,
                effect_id=claim.effect.effect_id,
                attempt_count=max(0, claim.attempt_count - 1),
                error=exc.reason,
            )
        self.wake()
        return EffectRunResult(
            status=EffectRunStatus.DEFERRED,
            effect_id=claim.effect.effect_id,
            attempt_count=max(0, claim.attempt_count - 1),
            retry_at=available_at,
            error=exc.reason,
        )

    async def _quarantine_claim(
        self,
        claim: ClaimedEffect,
        *,
        reason: EffectQuarantineReason,
        message: str,
    ) -> EffectRunResult:
        """Commit one non-domain diagnostic without invoking an effect handler."""

        self._validate_store_binding()
        try:
            settlement = await self._store.quarantine(
                claim,
                reason=reason,
                message=message,
                **self._execution_binding_kwargs(),
            )
            self._validate_store_binding()
        except EffectClaimLost as exc:
            return self._claim_lost_result(claim, exc)
        if settlement.status is EffectSettlementStatus.CANCELLED:
            return EffectRunResult(
                status=EffectRunStatus.CANCELLED,
                effect_id=claim.effect.effect_id,
                attempt_count=claim.attempt_count,
                error=f"{reason.value}: {message}",
            )
        if settlement.effect_id != claim.effect.effect_id:
            raise RuntimeError("effect quarantine returned a different effect id")
        if settlement.key != claim.key:
            raise RuntimeError("effect quarantine returned a different actor key")
        if settlement.event_id != quarantined_event_id(claim.effect):
            raise RuntimeError("effect quarantine returned a different mailbox event id")
        await self._wake_after_settlement(settlement)
        return EffectRunResult(
            status=EffectRunStatus.FAILED,
            effect_id=claim.effect.effect_id,
            event_id=settlement.event_id,
            attempt_count=claim.attempt_count,
            error=f"{reason.value}: {message}",
        )

    async def _release_after_cancellation(self, claim: ClaimedEffect) -> None:
        release_task = asyncio.create_task(
            self._store.release(
                claim,
                error="effect_executor_shutdown",
                **self._execution_binding_kwargs(),
            ),
            name=f"agent-effect-release:{claim.effect.effect_id}",
        )
        try:
            await asyncio.shield(release_task)
        except EffectClaimLost:
            return
        except asyncio.CancelledError:
            await asyncio.gather(release_task, return_exceptions=True)
        except Exception:
            logger.exception(
                "failed to release durable effect claim during shutdown",
                extra={"effect_id": claim.effect.effect_id},
            )

    async def _release_unstarted_claim(self, claim: ClaimedEffect) -> None:
        try:
            await self._store.release(
                claim,
                error="effect_executor_pre_execution_failure",
                **self._execution_binding_kwargs(),
            )
        except EffectClaimLost:
            return

    def _mark_binding_failure(self, exc: EffectStoreBindingChanged) -> None:
        """Make composition drift fatal and stop every sibling worker."""

        if self._binding_failure is None:
            self._binding_failure = exc
        self._idle_event.set()
        for wake_event in self._lane_wake_events.values():
            wake_event.set()
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current and not task.done():
                task.cancel()

    async def _wake_after_settlement(self, settlement: EffectSettlementResult) -> None:
        """Publish a committed mailbox outcome without weakening fenced evidence.

        A fenced result can only be discovered through its immutable mailbox
        sidecar.  It must never be compressed to a session key because the
        legacy registry cannot validate the admission fence or mailbox identity.
        Unfenced historical results retain the existing exact-key wake path.
        """

        wake_request = settlement.wake_request
        if wake_request is None:
            if settlement.mailbox_id is not None:
                raise RuntimeError(
                    "effect settlement with mailbox_id is missing wake evidence"
                )
            await self._wake_legacy_after_commit(settlement.key)
            return
        if wake_request.has_admission_fence:
            mailbox_id = settlement.mailbox_id
            if mailbox_id is None:
                raise RuntimeError(
                    "fenced effect settlement is missing its durable mailbox_id"
                )
            await self._notify_fenced_mailbox_handoff(settlement, mailbox_id)
            return
        await self._wake_legacy_after_commit(settlement.key)

    async def _notify_fenced_mailbox_handoff(
        self,
        settlement: EffectSettlementResult,
        mailbox_id: int,
    ) -> None:
        """Send a best-effort hint without touching the legacy actor registry."""

        notifier = self._mailbox_handoff_notifier
        if notifier is None:
            logger.debug(
                "retaining fenced effect mailbox handoff for future pull delivery",
                extra={
                    "mailbox_id": mailbox_id,
                    "effect_id": settlement.effect_id,
                    "event_id": settlement.event_id,
                },
            )
            return
        try:
            outcome = notifier.notify(mailbox_id)
            if isawaitable(outcome):
                await outcome
        except asyncio.CancelledError:
            raise
        except Exception:
            # The mailbox handoff sidecar committed with the settlement remains
            # the durable debt. A notifier is advisory, so it cannot fall back
            # to key-only wake or rerun an already settled effect.
            logger.exception(
                "failed to notify fenced effect mailbox handoff",
                extra={
                    "mailbox_id": mailbox_id,
                    "effect_id": settlement.effect_id,
                    "event_id": settlement.event_id,
                },
            )

    async def _wake_legacy_after_commit(self, key: SessionKey) -> None:
        """Wake an explicitly unfenced legacy mailbox by its session key."""

        session_registry = self._session_registry
        if session_registry is None:
            raise RuntimeError("a fenced effect executor cannot perform a legacy key wake")
        self._pending_legacy_wakes.add(key)
        try:
            outcome = session_registry.wake(key)
            if isawaitable(outcome):
                await outcome
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "failed to wake session actor after effect settlement",
                extra={"profile_id": key.profile_id, "session_id": key.session_id},
            )
            # The mailbox is already durable. Retain this exact key as wake
            # debt; never rerun the effect handler or invoke broad registry
            # recovery, which can write legacy recovery mailbox events.
            self._pending_legacy_wakes.add(key)
            await self._recover_pending_legacy_wakes()
        else:
            self._pending_legacy_wakes.discard(key)

    async def _recover_pending_legacy_wakes(self) -> None:
        """Retry only unfenced legacy wakes whose exact session keys are known."""

        if not self._pending_legacy_wakes:
            return
        async with self._legacy_wake_recovery_lock:
            if not self._pending_legacy_wakes:
                return
            session_registry = self._session_registry
            if session_registry is None:
                raise RuntimeError("a fenced effect executor cannot retry legacy key wakes")
            for key in tuple(self._pending_legacy_wakes):
                try:
                    outcome = session_registry.wake(key)
                    if isawaitable(outcome):
                        await outcome
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "failed to redrive committed effect mailbox wakeup",
                        extra={
                            "profile_id": key.profile_id,
                            "session_id": key.session_id,
                        },
                    )
                else:
                    self._pending_legacy_wakes.discard(key)

    async def _drain_store_notifications(self) -> None:
        """Publish store-owned committed mailbox events without fence downgrade."""

        if self._execution_binding is not None:
            # Scoped claims never perform the broad malformed-row or expiry
            # maintenance that populates this store-instance queue. Draining
            # it here could consume and wake a foreign session's maintenance
            # outcome, so a target-bound executor must leave it untouched for
            # an explicit unscoped maintenance controller.
            return
        self._validate_store_binding()
        notifications = await self._store.drain_quarantine_notifications()
        self._validate_store_binding()
        legacy_notified_keys: set[SessionKey] = set()
        for notification in notifications:
            if notification.status not in {
                EffectSettlementStatus.COMMITTED,
                EffectSettlementStatus.ALREADY_COMMITTED,
            }:
                raise RuntimeError("effect store returned an uncommitted mailbox notification")
            wake_request = notification.wake_request
            if wake_request is None:
                if notification.mailbox_id is not None:
                    raise RuntimeError(
                        "effect mailbox notification with mailbox_id is missing wake evidence"
                    )
                legacy_notified_keys.add(notification.key)
                continue
            if wake_request.has_admission_fence:
                await self._wake_after_settlement(notification)
                continue
            legacy_notified_keys.add(notification.key)
        self._pending_legacy_wakes.update(legacy_notified_keys)
        for key in legacy_notified_keys:
            await self._wake_legacy_after_commit(key)

    def _completion_envelope(
        self,
        claim: ClaimedEffect,
        result: EffectHandlerResult,
        contract: EffectExecutionContract,
    ) -> SessionEventEnvelope:
        effect = claim.effect
        now = self._clock()
        payload = {
            **result.payload,
            **effect.outcome_fence_payload(resolved_outcome_fence_fields(contract)),
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "operation_id": effect.operation_id,
            "idempotency_key": effect.idempotency_key,
            "attempt_count": claim.attempt_count,
            "contract_version": effect.contract_version,
            "contract_signature": effect.contract_signature,
        }
        return SessionEventEnvelope(
            event_id=completion_event_id(effect),
            key=effect.key,
            kind=contract.completion_event_kind,
            ownership_generation=effect.ownership_generation,
            payload=payload,
            source=contract.completion_source,
            occurred_at=now,
            causation_id=effect.source_event_id,
            correlation_id=effect.operation_id or effect.effect_id,
            trace_id=effect.trace_id,
            available_at=now,
            created_at=now,
        )

    def _failure_envelope(
        self,
        claim: ClaimedEffect,
        exc: BaseException,
        contract: EffectExecutionContract,
    ) -> SessionEventEnvelope:
        effect = claim.effect
        now = self._clock()
        # These fields identify a visible action's canonical request. They are
        # copied from the committed effect, never from handler output, so the
        # actor can reject a terminal failure for a different action slot.
        action_identity = {
            field_name: effect.payload[field_name]
            for field_name in ("action_ordinal", "request_digest")
            if field_name in effect.payload
        }
        return SessionEventEnvelope(
            event_id=failure_event_id(effect),
            key=effect.key,
            kind="EffectFailed",
            ownership_generation=effect.ownership_generation,
            payload={
                **effect.outcome_fence_payload(resolved_outcome_fence_fields(contract)),
                **action_identity,
                "effect_id": effect.effect_id,
                "effect_kind": effect.kind,
                "operation_id": effect.operation_id,
                "idempotency_key": effect.idempotency_key,
                "attempt_count": claim.attempt_count,
                "contract_version": effect.contract_version,
                "contract_signature": effect.contract_signature,
                "failure_code": type(exc).__name__,
                "failure_message": str(exc),
            },
            source="effect_executor",
            occurred_at=now,
            causation_id=effect.source_event_id,
            correlation_id=effect.operation_id or effect.effect_id,
            trace_id=effect.trace_id,
            available_at=now,
            created_at=now,
        )

    @staticmethod
    def _retry_delay(
        contract: EffectExecutionContract,
        attempt_count: int,
    ) -> float:
        exponent = max(0, attempt_count - 1)
        if contract.retry_base_seconds == 0:
            return 0.0
        ratio = contract.retry_max_seconds / contract.retry_base_seconds
        maximum_exponent = max(0, math.ceil(math.log2(ratio)))
        bounded_exponent = min(exponent, maximum_exponent)
        return min(
            contract.retry_max_seconds,
            math.ldexp(contract.retry_base_seconds, bounded_exponent),
        )

    def _claim_filter(
        self,
        lane: EffectLane | None,
    ) -> tuple[
        tuple[tuple[str, int], ...] | None,
        tuple[tuple[str, int], ...],
    ]:
        registered = tuple(contract.ref for contract in self._handlers.contracts())
        handled = tuple(
            contract_ref
            for handled_lane in self._handlers.handled_lanes()
            for contract_ref in self._handlers.handled_effect_contracts_for_lane(handled_lane)
        )
        if lane is EffectLane.ORPHAN:
            return None, registered
        if lane is None:
            return handled, ()
        return self._handlers.handled_effect_contracts_for_lane(lane), ()

    def _execution_binding_kwargs(self) -> dict[str, FencedActorExecutionBinding]:
        """Return an optional typed keyword without widening legacy store calls."""

        if self._execution_binding is None:
            return {}
        return {"execution_binding": self._execution_binding}

    def _validate_claim_execution_binding(self, claim: ClaimedEffect) -> None:
        """Reject a store result that escapes this executor's target lease scope."""

        binding = self._execution_binding
        if binding is None:
            return
        if (
            claim.key != binding.request.key
            or claim.effect.ownership_generation != binding.request.ownership_generation
        ):
            raise FencedEffectExecutionLeaseLost(
                "fenced effect store returned work outside its execution binding"
            )

    def _register_in_flight_handler_task(
        self,
        context: EffectExecutionContext,
        task: asyncio.Task[EffectHandlerResult],
    ) -> InFlightEffectHandlerKey:
        """Track one real handler task until that exact task has finished."""

        identity = InFlightEffectHandlerKey.from_claim(context.claim)
        existing = self._in_flight_handler_tasks.get(identity)
        if existing is not None and existing.task is not task:
            raise RuntimeError(
                "duplicate live durable effect handler task identity: "
                f"{identity.effect_id}:{identity.claim_id}"
            )
        self._in_flight_handler_tasks[identity] = _InFlightEffectHandler(
            identity=identity,
            task=task,
            context=context,
        )
        task.add_done_callback(
            lambda completed, tracked_identity=identity: self._unregister_in_flight_handler_task(
                tracked_identity,
                completed,
            )
        )
        return identity

    def _unregister_in_flight_handler_task(
        self,
        identity: InFlightEffectHandlerKey,
        task: asyncio.Task[Any],
    ) -> None:
        """Remove a task only when it still owns its exact registry entry."""

        tracked = self._in_flight_handler_tasks.get(identity)
        if tracked is not None and tracked.task is task:
            self._in_flight_handler_tasks.pop(identity, None)

    def _cancel_in_flight_handler_task(
        self,
        tracked: _InFlightEffectHandler,
    ) -> bool:
        """Revoke and cancel one local handler while preserving its tail record."""

        tracked.context.revoke()
        if tracked.task.done():
            self._unregister_in_flight_handler_task(
                tracked.identity,
                tracked.task,
            )
            return False
        cancelled = tracked.task.cancel()
        if cancelled:
            self._observe_cancellation_tail(tracked.task)
        return cancelled

    def _matching_local_handler_tasks(
        self,
        *,
        key: SessionKey,
        operation_id: str,
        effect_kind: str | None = None,
        effect_id: str | None = None,
    ) -> tuple[_InFlightEffectHandler, ...]:
        """Return live local handlers, cleaning only their own stale entries."""

        matching: list[_InFlightEffectHandler] = []
        for identity, tracked in tuple(self._in_flight_handler_tasks.items()):
            if tracked.task.done():
                self._unregister_in_flight_handler_task(identity, tracked.task)
                continue
            if identity.key != key or identity.operation_id != operation_id:
                continue
            if effect_kind is not None and identity.effect_kind != effect_kind:
                continue
            if effect_id is not None and identity.effect_id != effect_id:
                continue
            matching.append(tracked)
        return tuple(
            sorted(
                matching,
                key=lambda tracked: (
                    tracked.identity.effect_kind,
                    tracked.identity.effect_id,
                    tracked.identity.claim_id,
                ),
            )
        )

    def _all_local_handler_tasks(self) -> tuple[_InFlightEffectHandler, ...]:
        """Return every currently live handler task owned by this executor."""

        tracked_handlers: list[_InFlightEffectHandler] = []
        for identity, tracked in tuple(self._in_flight_handler_tasks.items()):
            if tracked.task.done():
                self._unregister_in_flight_handler_task(identity, tracked.task)
                continue
            tracked_handlers.append(tracked)
        return tuple(
            sorted(
                tracked_handlers,
                key=lambda tracked: (
                    tracked.identity.key.profile_id,
                    tracked.identity.key.session_id,
                    tracked.identity.operation_id,
                    tracked.identity.effect_kind,
                    tracked.identity.effect_id,
                    tracked.identity.claim_id,
                ),
            )
        )

    @staticmethod
    def _require_session_key(key: SessionKey) -> SessionKey:
        """Require a canonical session key for local operation lookup."""

        if not isinstance(key, SessionKey):
            raise TypeError("key must be a SessionKey")
        return key

    @staticmethod
    def _require_operation_id(operation_id: str) -> str:
        """Normalize a non-empty durable operation identifier."""

        normalized = str(operation_id or "").strip()
        if not normalized:
            raise ValueError("operation_id must not be empty")
        return normalized

    @staticmethod
    def _optional_effect_identity_part(
        value: str | None,
        *,
        field_name: str,
    ) -> str | None:
        """Normalize an optional exact effect identity filter."""

        if value is None:
            return None
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError(f"{field_name} must not be empty when supplied")
        return normalized

    @staticmethod
    def _validate_quiescence_timeout(timeout_seconds: float | None) -> float | None:
        """Validate an optional local wait bound without changing task state."""

        if timeout_seconds is None:
            return None
        timeout = float(timeout_seconds)
        if timeout < 0 or not math.isfinite(timeout):
            raise ValueError("timeout_seconds must be finite and non-negative")
        return timeout

    @staticmethod
    def _remaining_quiescence_timeout(deadline: float | None) -> float | None:
        """Return the remaining local wait time for one quiescence request."""

        if deadline is None:
            return None
        return max(0.0, deadline - asyncio.get_running_loop().time())

    def _local_operation_quiescence_report(
        self,
        *,
        status: LocalOperationQuiescenceStatus,
        key: SessionKey,
        operation_id: str,
        matched: Mapping[InFlightEffectHandlerKey, None],
        cancelled: Mapping[InFlightEffectHandlerKey, None],
        remaining: tuple[_InFlightEffectHandler, ...] = (),
    ) -> LocalOperationQuiescence:
        """Build a deterministic report for a process-local task observation."""

        def sort_key(identity: InFlightEffectHandlerKey) -> tuple[str, str, str]:
            return (
                identity.effect_kind,
                identity.effect_id,
                identity.claim_id,
            )

        return LocalOperationQuiescence(
            scope=LocalOperationQuiescenceScope.LOCAL_PROCESS,
            status=status,
            key=key,
            operation_id=operation_id,
            matched_handler_keys=tuple(sorted(matched, key=sort_key)),
            cancelled_handler_keys=tuple(sorted(cancelled, key=sort_key)),
            remaining_handler_keys=tuple(tracked.identity for tracked in remaining),
        )

    def _local_executor_quiescence_report(
        self,
        *,
        status: LocalOperationQuiescenceStatus,
        matched: Mapping[InFlightEffectHandlerKey, None],
        cancelled: Mapping[InFlightEffectHandlerKey, None],
        remaining: tuple[_InFlightEffectHandler, ...] = (),
    ) -> LocalEffectExecutorQuiescence:
        """Build a deterministic full-executor local quiescence observation."""

        def sort_key(identity: InFlightEffectHandlerKey) -> tuple[str, ...]:
            return (
                identity.key.profile_id,
                identity.key.session_id,
                identity.operation_id,
                identity.effect_kind,
                identity.effect_id,
                identity.claim_id,
            )

        return LocalEffectExecutorQuiescence(
            scope=LocalOperationQuiescenceScope.LOCAL_PROCESS,
            status=status,
            matched_handler_keys=tuple(sorted(matched, key=sort_key)),
            cancelled_handler_keys=tuple(sorted(cancelled, key=sort_key)),
            remaining_handler_keys=tuple(
                tracked.identity for tracked in remaining
            ),
        )

    def _observe_cancellation_tail(self, task: asyncio.Task[Any]) -> None:
        if task.done():
            _consume_task_result(task)
            return
        self._cancellation_tails.add(task)

        def _finished(completed: asyncio.Task[Any]) -> None:
            self._cancellation_tails.discard(completed)
            _consume_task_result(completed)

        task.add_done_callback(_finished)

    @staticmethod
    def _validate_settlement(
        claim: ClaimedEffect,
        envelope: SessionEventEnvelope,
        settlement: EffectSettlementResult,
    ) -> None:
        if settlement.effect_id != claim.effect.effect_id:
            raise RuntimeError("effect settlement returned a different effect id")
        if settlement.key != claim.key:
            raise RuntimeError("effect settlement returned a different actor key")
        expected_event_id = (
            skipped_event_id(claim.effect)
            if settlement.status == EffectSettlementStatus.PRECONDITION_SKIPPED
            else envelope.event_id
        )
        if settlement.event_id != expected_event_id:
            raise RuntimeError("effect settlement returned a different mailbox event id")

    @staticmethod
    def _claim_lost_result(
        claim: ClaimedEffect,
        exc: BaseException,
    ) -> EffectRunResult:
        return EffectRunResult(
            status=EffectRunStatus.CLAIM_LOST,
            effect_id=claim.effect.effect_id,
            attempt_count=claim.attempt_count,
            error=_error_text(exc),
        )


def completion_event_id(effect: DurableEffectEnvelope) -> str:
    """Return the stable mailbox id for a successful effect completion."""

    configured = _payload_text(effect.payload, "completion_event_id")
    if not configured:
        configured = _payload_text(effect.payload, "deadline_event_id")
    if configured:
        return configured
    return derived_effect_event_id(
        key=effect.key,
        effect_id=effect.effect_id,
        outcome="completed",
    )


def failure_event_id(effect: DurableEffectEnvelope) -> str:
    """Return the stable mailbox id for a terminal effect failure."""

    configured = _payload_text(effect.payload, "failure_event_id")
    if configured:
        return configured
    return derived_effect_event_id(
        key=effect.key,
        effect_id=effect.effect_id,
        outcome="failed",
    )


def skipped_event_id(effect: DurableEffectEnvelope) -> str:
    """Return the stable mailbox id for an atomically skipped effect."""

    return derived_effect_event_id(
        key=effect.key,
        effect_id=effect.effect_id,
        outcome="skipped",
    )


def quarantined_event_id(effect: DurableEffectEnvelope) -> str:
    """Return the stable mailbox id for a store-owned quarantine diagnostic."""

    return derived_effect_event_id(
        key=effect.key,
        effect_id=effect.effect_id,
        outcome="quarantined",
    )


def derived_effect_event_id(
    *,
    key: SessionKey,
    effect_id: str,
    outcome: str,
) -> str:
    """Return the canonical derived mailbox identity for one effect outcome.

    Reducers that validate a completion before materializing a full
    :class:`DurableEffectEnvelope` use this same function.  Keeping the
    derivation in one place prevents an otherwise silent mismatch between an
    executor-generated event and the actor's provenance fence.
    """

    normalized_effect_id = str(effect_id or "").strip()
    normalized_outcome = str(outcome or "").strip()
    if not normalized_effect_id:
        raise ValueError("effect_id must not be empty")
    if not normalized_outcome:
        raise ValueError("outcome must not be empty")
    identity = "\x1f".join(
        (
            key.profile_id,
            key.session_id,
            normalized_effect_id,
            normalized_outcome,
        )
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"effect-{normalized_outcome}:{digest}"


def _payload_text(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    return str(value or "").strip()


def _error_text(exc: BaseException) -> str:
    message = str(exc).strip()
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def _missing_explicit_outcome_fences(
    effect: DurableEffectEnvelope,
    contract: EffectExecutionContract,
) -> tuple[str, ...]:
    """Return fields absent from an explicitly declared outcome projection."""

    declared = contract.outcome_fence_fields
    if declared is None:
        return ()
    return tuple(field_name for field_name in declared if field_name not in effect.payload)


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return
    try:
        task.exception()
    except (asyncio.CancelledError, Exception):
        return


__all__ = [
    "ClaimedEffect",
    "DurableEffectEnvelope",
    "DurableEffectExecutor",
    "DurableEffectStatus",
    "DurableEffectStore",
    "EffectAuthorityChanged",
    "EffectExecutionContract",
    "EffectExecutionConfigurationError",
    "EffectContractSignatureMismatch",
    "EffectClaimLost",
    "EffectExecutionCancelled",
    "EffectExecutionDeferred",
    "EffectExecutionContext",
    "EffectExpiryRecoveryResult",
    "EffectQuarantineReason",
    "EffectExecutorError",
    "EffectHandler",
    "EffectHandlerNotFound",
    "EffectHandlerRegistry",
    "EffectHandlerResult",
    "EffectLane",
    "EffectRunResult",
    "EffectRunStatus",
    "EffectSettlementResult",
    "EffectSettlementStatus",
    "EffectStoreBindingChanged",
    "FencedEffectRecoveryStore",
    "InFlightEffectHandlerKey",
    "LocalOperationQuiescence",
    "LocalOperationQuiescenceScope",
    "LocalOperationQuiescenceStatus",
    "MailboxHandoffNotifier",
    "SessionActorWakeTarget",
    "builtin_session_actor_effect_contracts",
    "completion_event_id",
    "derived_effect_event_id",
    "failure_event_id",
    "quarantined_event_id",
    "skipped_event_id",
]
