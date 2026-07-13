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
from typing import Any, Protocol

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
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope

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
    raise TypeError(
        "durable effect values must be JSON-compatible, "
        f"got {type(value)!r}"
    )


class DurableEffectStatus(StrEnum):
    """Durable lifecycle state for an outbox effect."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class EffectSettlementStatus(StrEnum):
    """Outcome of an atomic effect-and-mailbox settlement transaction."""

    COMMITTED = "committed"
    ALREADY_COMMITTED = "already_committed"
    PRECONDITION_SKIPPED = "precondition_skipped"


class EffectRunStatus(StrEnum):
    """Observable result of one executor claim attempt."""

    EMPTY = "empty"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"
    CLAIM_LOST = "claim_lost"


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


class EffectAuthorityChanged(EffectStoreBindingChanged):
    """Raised when a composed durable store swaps its authority snapshot."""


class EffectClaimLost(EffectExecutorError):
    """Raised when an effect claim is no longer the current fenced lease."""


class EffectHandlerNotFound(EffectExecutorError):
    """Raised when no handler is registered for a durable effect kind."""


class EffectContractSignatureMismatch(EffectExecutorError):
    """Raised when persisted work does not match its registered contract."""


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
    ) -> ClaimedEffect | None:
        """Claim the next available effect with a newly generated claim id."""

    async def drain_quarantine_notifications(
        self,
    ) -> tuple[EffectSettlementResult, ...]:
        """Return raw-row quarantines committed while scanning for a claim."""

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        """Extend a lease only if ``claim_id`` is still authoritative."""

    async def complete_with_event(
        self,
        claim: ClaimedEffect,
        completion_envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
    ) -> EffectSettlementResult:
        """Atomically complete an effect and insert its completion event."""

    async def release_for_retry(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        available_at: float,
    ) -> None:
        """Release the current claim to pending with a bounded retry time."""

    async def fail_with_event(
        self,
        claim: ClaimedEffect,
        failure_envelope: SessionEventEnvelope,
        *,
        error: str,
        outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
    ) -> EffectSettlementResult:
        """Atomically fail an effect and insert an ``EffectFailed`` event."""

    async def quarantine(
        self,
        claim: ClaimedEffect,
        *,
        reason: EffectQuarantineReason,
        message: str,
    ) -> EffectSettlementResult:
        """Terminalize unsupported work with a store-owned diagnostic event."""

    async def release(self, claim: ClaimedEffect, *, error: str) -> None:
        """Immediately release a live claim during executor shutdown."""

    async def recover_expired(self, *, worker_id: str) -> int:
        """Return expired processing claims to pending state."""

    async def next_available_at(
        self,
        *,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
    ) -> float | None:
        """Return the earliest pending availability or processing lease expiry."""


class SessionActorWakeTarget(Protocol):
    """Registry surface used after a mailbox event has committed."""

    def wake(self, key: SessionKey) -> Awaitable[None] | None:
        """Wake an actor without performing another mailbox write."""

    def recover(self) -> Awaitable[int]:
        """Discover and wake actors for mailbox events already committed."""


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
        if (
            contract_authority is None
            and include_builtin_contracts
            and contracts is None
        ):
            contract_authority = builtin_effect_contract_authority()
        if contract_authority is not None and not isinstance(
            contract_authority,
            EffectContractAuthority,
        ):
            raise TypeError("contract_authority must be an EffectContractAuthority")
        if contract_authority is not None and contracts is not None:
            raise ValueError(
                "contracts cannot be supplied with an immutable contract_authority"
            )
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

        return tuple(
            contract
            for contract in self.contracts()
            if contract.ref in self._handlers
        )

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
                "effect contract is already registered: "
                f"{contract.effect_kind}:v{contract.version}"
            )
        self._contracts[key] = contract
        self._effect_contract_authority = EffectContractAuthority(
            self._contracts.values()
        )

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
            raise ValueError(
                f"effect handler is already registered: {normalized}:v{version}"
            )
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
                "no durable effect handler is registered for "
                f"{normalized!r} version {version}"
            ) from exc

    def contract_for(self, kind: str, version: int = 1) -> EffectExecutionContract:
        """Return the registered durable contract for *kind*."""

        normalized = str(kind or "").strip()
        try:
            return self._contracts[(normalized, version)]
        except KeyError as exc:
            raise EffectHandlerNotFound(
                "no durable effect contract is registered for "
                f"{normalized!r} version {version}"
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
            (
                contract
                for contract in self.handled_contracts()
                if contract.lane is lane
            ),
            key=lambda contract: (
                contract.priority,
                contract.effect_kind,
                contract.version,
            ),
        )
        return tuple(contract.ref for contract in contracts)

    def lanes(self) -> tuple[EffectLane, ...]:
        """Return lanes that own at least one registered contract."""

        return tuple(
            lane for lane in EffectLane if self.effect_contracts_for_lane(lane)
        )

    def handled_lanes(self) -> tuple[EffectLane, ...]:
        """Return lanes with at least one contract bound to a handler."""

        return tuple(
            lane
            for lane in EffectLane
            if self.handled_effect_contracts_for_lane(lane)
        )

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

    def __init__(self, store: DurableEffectStore, claim: ClaimedEffect) -> None:
        self._store = store
        self._claim = claim
        self._renew_lock = asyncio.Lock()
        self._revoked = False

    @property
    def claim(self) -> ClaimedEffect:
        """Return the latest renewed claim snapshot."""

        return self._claim

    @property
    def effect(self) -> DurableEffectEnvelope:
        """Return the durable handler input."""

        return self._claim.effect

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
            renewed = await self._store.renew_lease(current)
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


Clock = Callable[[], float]


class DurableEffectExecutor:
    """Supervise durable effect handlers outside session actor transactions."""

    def __init__(
        self,
        *,
        store: DurableEffectStore,
        handlers: EffectHandlerRegistry,
        session_registry: SessionActorWakeTarget,
        worker_id: str | None = None,
        worker_count: int = 1,
        control_worker_count: int = 1,
        orphan_worker_count: int = 1,
        poll_interval_seconds: float = 1.0,
        renew_interval_seconds: float | None = 10.0,
        clock: Clock | None = None,
    ) -> None:
        """Initialize an executor without starting worker tasks.

        Args:
            store: Durable outbox and atomic settlement implementation.
            handlers: Registry of async effect handlers.
            session_registry: Actor registry woken only after settlement commits.
            worker_id: Optional stable process-level worker prefix.
            worker_count: Worker count for planner and default lanes.
            control_worker_count: Dedicated workers reserved for control effects.
            orphan_worker_count: Workers that terminally fail unknown effect kinds.
            poll_interval_seconds: Recovery polling bound when no wake is received.
            renew_interval_seconds: Automatic lease renewal cadence, or ``None``.
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
        if renew_interval_seconds is not None and not math.isfinite(
            renew_interval_seconds
        ):
            raise ValueError("renew_interval_seconds must be finite or None")
        self._store = store
        self._handlers = handlers
        self._effect_contract_authority: EffectContractAuthority | None = None
        self._persistence_domain: object | None = None
        self._session_registry = session_registry
        self.worker_id = str(worker_id or f"effect-executor:{uuid.uuid4().hex}")
        self._worker_count = worker_count
        self._control_worker_count = control_worker_count
        self._orphan_worker_count = orphan_worker_count
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._renew_interval_seconds = renew_interval_seconds
        self._clock = clock or time.time
        self._lane_wake_events = {lane: asyncio.Event() for lane in EffectLane}
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self._start_lock = asyncio.Lock()
        self._wake_recovery_lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._cancellation_tails: set[asyncio.Task[Any]] = set()
        self._active_claims: dict[str, ClaimedEffect] = {}
        self._pending_wakes: set[SessionKey] = set()
        self._binding_failure: EffectStoreBindingChanged | None = None
        self._closing = False
        self._drain_on_shutdown = False

    @property
    def started(self) -> bool:
        """Return whether at least one handler-bound worker is live.

        Orphan workers may run to terminally settle genuinely unknown durable
        effects.  They do not make this executor capable of handling any
        known effect contract and therefore do not make it ``started``.
        """

        return self.healthy and any(
            not task.done() for task in self._handler_tasks
        )

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

        return any(
            lane is not EffectLane.ORPHAN for lane in self._handlers.handled_lanes()
        )

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
            raise TypeError(
                "durable effect store must expose an EffectContractAuthority"
            )
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
    def session_registry(self) -> SessionActorWakeTarget:
        """Return the exact actor wake target used after durable settlement."""

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
        _ = self.persistence_domain

    @property
    def closed(self) -> bool:
        """Return whether executor shutdown has begun."""

        return self._closing

    def wake(self) -> None:
        """Notify workers that committed outbox effects may be available."""

        if not self._closing:
            self._idle_event.clear()
            for wake_event in self._lane_wake_events.values():
                wake_event.set()

    async def start(self) -> int:
        """Recover expired claims and start supervised effect workers."""

        async with self._start_lock:
            self._validate_store_binding()
            if self._tasks:
                return 0
            if self._closing:
                raise RuntimeError("a closed durable effect executor cannot be restarted")
            recovered = await self._store.recover_expired(worker_id=self.worker_id)
            self._validate_store_binding()
            tasks: list[asyncio.Task[None]] = []
            handler_tasks: list[asyncio.Task[None]] = []
            for lane in self._handlers.handled_lanes():
                if lane is EffectLane.ORPHAN:
                    continue
                count = (
                    self._control_worker_count
                    if lane is EffectLane.CONTROL
                    else self._worker_count
                )
                lane_tasks = [
                    asyncio.create_task(
                        self._worker_loop(lane, index),
                        name=(
                            f"agent-effect-executor:{self.worker_id}:"
                            f"{lane.value}:{index}"
                        ),
                    )
                    for index in range(count)
                ]
                tasks.extend(lane_tasks)
                handler_tasks.extend(lane_tasks)
            tasks.extend(
                asyncio.create_task(
                    self._worker_loop(EffectLane.ORPHAN, index),
                    name=(
                        f"agent-effect-executor:{self.worker_id}:"
                        f"{EffectLane.ORPHAN.value}:{index}"
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

        self._validate_store_binding()
        await self._recover_pending_wakes()
        effect_contracts, excluded_effect_contracts = self._claim_filter(lane)
        claim = await self._store.claim_next(
            worker_id=worker_id or self.worker_id,
            effect_contracts=effect_contracts,
            excluded_effect_contracts=excluded_effect_contracts,
        )
        if claim is not None:
            self._idle_event.clear()
            self._active_claims[claim.claim_id] = claim
        try:
            self._validate_store_binding()
            quarantine_notifications = (
                await self._store.drain_quarantine_notifications()
            )
            self._validate_store_binding()
            notified_keys: set[SessionKey] = set()
            for notification in quarantine_notifications:
                if notification.status not in {
                    EffectSettlementStatus.COMMITTED,
                    EffectSettlementStatus.ALREADY_COMMITTED,
                }:
                    raise RuntimeError(
                        "effect store returned an uncommitted quarantine notification"
                    )
                notified_keys.add(notification.key)
            self._pending_wakes.update(notified_keys)
            for key in notified_keys:
                await self._wake_after_commit(key)
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

    async def wait_idle(self) -> None:
        """Wait until no worker owns a claim and no effect is immediately claimable."""

        await self._idle_event.wait()

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
                await self._recover_pending_wakes()
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
        context = EffectExecutionContext(self._store, initial_claim)
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
            )
            self._validate_store_binding()
        except asyncio.CancelledError:
            await self._release_after_cancellation(context.claim)
            raise
        except EffectStoreBindingChanged:
            context.revoke()
            raise
        except EffectClaimLost as exc:
            return self._claim_lost_result(context.claim, exc)
        except Exception as exc:
            return await self._retry_or_fail(context.claim, exc, contract)

        self._validate_settlement(context.claim, completion, settlement)
        await self._wake_after_commit(context.claim.key)
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
        handler_task = asyncio.create_task(
            handler(context),
            name=f"agent-effect-handler:{context.effect.effect_id}",
        )
        renew_task: asyncio.Task[None] | None = None
        if self._renew_interval_seconds is not None:
            renew_task = asyncio.create_task(
                self._renew_while_running(context, handler_task),
                name=f"agent-effect-renew:{context.effect.effect_id}",
            )
        timeout_task = asyncio.create_task(
            asyncio.sleep(contract.timeout_seconds),
            name=f"agent-effect-timeout:{context.effect.effect_id}",
        )
        try:
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
                self._observe_cancellation_tail(handler_task)
                raise TimeoutError(
                    f"effect handler exceeded {contract.timeout_seconds:g} seconds"
                )
            if renew_task is not None and renew_task in done and not handler_task.done():
                renewal_error = renew_task.exception()
                context.revoke()
                handler_task.cancel()
                self._observe_cancellation_tail(handler_task)
                if renewal_error is None:
                    raise EffectClaimLost("lease renewal stopped before handler completion")
                raise renewal_error
            result = await handler_task
            if not isinstance(result, EffectHandlerResult):
                raise TypeError("effect handlers must return EffectHandlerResult")
            return result
        finally:
            timeout_task.cancel()
            if renew_task is not None and not renew_task.done():
                renew_task.cancel()
            if renew_task is not None:
                await asyncio.gather(renew_task, return_exceptions=True)
            if not handler_task.done():
                context.revoke()
                handler_task.cancel()
                self._observe_cancellation_tail(handler_task)
            await asyncio.gather(timeout_task, return_exceptions=True)

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
    ) -> EffectRunResult:
        self._validate_store_binding()
        error = _error_text(exc)
        if claim.attempt_count < contract.max_attempts:
            retry_at = self._clock() + self._retry_delay(
                contract,
                claim.attempt_count,
            )
            try:
                await self._store.release_for_retry(
                    claim,
                    error=error,
                    available_at=retry_at,
                )
                self._validate_store_binding()
            except EffectClaimLost as claim_exc:
                return self._claim_lost_result(claim, claim_exc)
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
            )
            self._validate_store_binding()
        except EffectClaimLost as claim_exc:
            return self._claim_lost_result(claim, claim_exc)
        self._validate_settlement(claim, failure, settlement)
        await self._wake_after_commit(claim.key)
        return EffectRunResult(
            status=EffectRunStatus.FAILED,
            effect_id=claim.effect.effect_id,
            event_id=failure.event_id,
            attempt_count=claim.attempt_count,
            error=error,
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
            )
            self._validate_store_binding()
        except EffectClaimLost as exc:
            return self._claim_lost_result(claim, exc)
        if settlement.effect_id != claim.effect.effect_id:
            raise RuntimeError("effect quarantine returned a different effect id")
        if settlement.key != claim.key:
            raise RuntimeError("effect quarantine returned a different actor key")
        if settlement.event_id != quarantined_event_id(claim.effect):
            raise RuntimeError("effect quarantine returned a different mailbox event id")
        await self._wake_after_commit(claim.key)
        return EffectRunResult(
            status=EffectRunStatus.FAILED,
            effect_id=claim.effect.effect_id,
            event_id=settlement.event_id,
            attempt_count=claim.attempt_count,
            error=f"{reason.value}: {message}",
        )

    async def _release_after_cancellation(self, claim: ClaimedEffect) -> None:
        release_task = asyncio.create_task(
            self._store.release(claim, error="effect_executor_shutdown"),
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

    async def _wake_after_commit(self, key: SessionKey) -> None:
        self._pending_wakes.add(key)
        try:
            outcome = self._session_registry.wake(key)
            if isawaitable(outcome):
                await outcome
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "failed to wake session actor after effect settlement",
                extra={"profile_id": key.profile_id, "session_id": key.session_id},
            )
            # The mailbox is already durable. Record wake debt and recover via
            # mailbox discovery; never rerun the effect handler for wake errors.
            self._pending_wakes.add(key)
            await self._recover_pending_wakes()
        else:
            self._pending_wakes.discard(key)

    async def _recover_pending_wakes(self) -> None:
        if not self._pending_wakes:
            return
        async with self._wake_recovery_lock:
            if not self._pending_wakes:
                return
            try:
                await self._session_registry.recover()
            except Exception:
                logger.exception("failed to recover committed effect mailbox wakeups")
                return
            self._pending_wakes.clear()

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
                **effect.outcome_fence_payload(
                    resolved_outcome_fence_fields(contract)
                ),
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
            for contract_ref in self._handlers.handled_effect_contracts_for_lane(
                handled_lane
            )
        )
        if lane is EffectLane.ORPHAN:
            return None, registered
        if lane is None:
            return handled, ()
        return self._handlers.handled_effect_contracts_for_lane(lane), ()

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
    "EffectContractSignatureMismatch",
    "EffectClaimLost",
    "EffectExecutionContext",
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
    "SessionActorWakeTarget",
    "builtin_session_actor_effect_contracts",
    "completion_event_id",
    "derived_effect_event_id",
    "failure_event_id",
    "quarantined_event_id",
    "skipped_event_id",
]
