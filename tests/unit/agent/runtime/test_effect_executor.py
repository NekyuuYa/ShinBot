from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass, replace

import pytest

import shinbot.agent.runtime.session_actor.reducer as session_actor_reducer
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    DEFAULT_OUTCOME_FENCE_FIELDS,
    EffectContractAuthority,
    builtin_effect_contract,
    builtin_effect_contract_authority,
    resolved_outcome_fence_fields,
    validate_effect_declaration,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    DurableEffectExecutor,
    DurableEffectStatus,
    EffectAuthorityChanged,
    EffectClaimLost,
    EffectExecutionContext,
    EffectExecutionContract,
    EffectHandlerRegistry,
    EffectHandlerResult,
    EffectLane,
    EffectQuarantineReason,
    EffectRunStatus,
    EffectSettlementResult,
    EffectSettlementStatus,
    completion_event_id,
    quarantined_event_id,
)
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope


@dataclass(slots=True)
class _EffectRecord:
    effect: DurableEffectEnvelope
    status: DurableEffectStatus = DurableEffectStatus.PENDING
    attempt_count: int = 0
    claim_id: str = ""
    worker_id: str = ""
    lease_until: float | None = None
    available_at: float = 0.0
    last_error: str = ""
    settled_claim: ClaimedEffect | None = None
    settled_event: SessionEventEnvelope | None = None


class _MemoryEffectStore:
    """Reference implementation of the effect-store atomicity contract."""

    def __init__(self, now: list[float], *, lease_seconds: float = 5.0) -> None:
        self._clock = lambda: now[0]
        self._lease_seconds = lease_seconds
        self._effect_contract_authority = builtin_effect_contract_authority()
        self._lock = asyncio.Lock()
        self.records: dict[str, _EffectRecord] = {}
        self.order: list[str] = []
        self.mailbox: dict[tuple[SessionKey, str], SessionEventEnvelope] = {}
        self.actions: list[str] = []
        self.completion_fence_fields: list[tuple[str, ...]] = []
        self.failure_fence_fields: list[tuple[str, ...]] = []
        self.quarantine_notifications: list[EffectSettlementResult] = []

    @property
    def persistence_domain(self) -> object:
        """Return the transaction domain used by this executor test store."""

        return self

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the stable authority exposed by this executor test store."""

        return self._effect_contract_authority

    def bind_effect_contract_authority(
        self,
        authority: EffectContractAuthority,
    ) -> None:
        """Replace the authority for explicit composition-drift tests."""

        self._effect_contract_authority = authority

    async def seed(
        self,
        effect: DurableEffectEnvelope,
        *,
        attempt_count: int = 0,
    ) -> None:
        async with self._lock:
            self.records[effect.effect_id] = _EffectRecord(
                effect=effect,
                attempt_count=attempt_count,
                available_at=effect.available_at,
            )
            self.order.append(effect.effect_id)

    async def claim_next(
        self,
        *,
        worker_id: str,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
    ) -> ClaimedEffect | None:
        async with self._lock:
            now = self._clock()
            order = list(self.order)
            if effect_contracts is not None:
                priorities = {
                    contract_ref: index for index, contract_ref in enumerate(effect_contracts)
                }
                order.sort(
                    key=lambda effect_id: priorities.get(
                        (
                            self.records[effect_id].effect.kind,
                            self.records[effect_id].effect.contract_version,
                        ),
                        len(priorities),
                    )
                )
            for effect_id in order:
                record = self.records[effect_id]
                contract_ref = (
                    record.effect.kind,
                    record.effect.contract_version,
                )
                if effect_contracts is not None and contract_ref not in effect_contracts:
                    continue
                if contract_ref in excluded_effect_contracts:
                    continue
                if record.status != DurableEffectStatus.PENDING:
                    continue
                if record.available_at > now:
                    continue
                record.status = DurableEffectStatus.PROCESSING
                record.attempt_count += 1
                record.claim_id = uuid.uuid4().hex
                record.worker_id = worker_id
                record.lease_until = now + self._lease_seconds
                return ClaimedEffect(
                    claim_id=record.claim_id,
                    effect=record.effect,
                    worker_id=worker_id,
                    attempt_count=record.attempt_count,
                    claimed_at=now,
                    lease_expires_at=record.lease_until,
                )
        return None

    async def drain_quarantine_notifications(
        self,
    ) -> tuple[EffectSettlementResult, ...]:
        notifications = tuple(self.quarantine_notifications)
        self.quarantine_notifications.clear()
        return notifications

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        async with self._lock:
            record = self._owned_record(claim)
            record.lease_until = self._clock() + self._lease_seconds
            return ClaimedEffect(
                claim_id=claim.claim_id,
                effect=claim.effect,
                worker_id=claim.worker_id,
                attempt_count=claim.attempt_count,
                claimed_at=claim.claimed_at,
                lease_expires_at=record.lease_until,
            )

    async def complete_with_event(
        self,
        claim: ClaimedEffect,
        completion_envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
    ) -> EffectSettlementResult:
        async with self._lock:
            self.completion_fence_fields.append(outcome_fence_fields)
            record = self.records[claim.effect.effect_id]
            duplicate = self._duplicate_settlement(record, claim, completion_envelope)
            if duplicate is not None:
                return duplicate
            self._owned_record(claim)
            self._insert_mailbox(completion_envelope)
            record.status = DurableEffectStatus.COMPLETED
            record.settled_claim = claim
            record.settled_event = completion_envelope
            self._clear_claim(record)
            self.actions.append(f"commit:{completion_envelope.event_id}")
            return EffectSettlementResult(
                status=EffectSettlementStatus.COMMITTED,
                effect_id=claim.effect.effect_id,
                event_id=completion_envelope.event_id,
                key=claim.key,
            )

    async def release_for_retry(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        available_at: float,
    ) -> None:
        async with self._lock:
            record = self._owned_record(claim)
            record.status = DurableEffectStatus.PENDING
            record.available_at = available_at
            record.last_error = error
            self._clear_claim(record)

    async def fail_with_event(
        self,
        claim: ClaimedEffect,
        failure_envelope: SessionEventEnvelope,
        *,
        error: str,
        outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
    ) -> EffectSettlementResult:
        async with self._lock:
            self.failure_fence_fields.append(outcome_fence_fields)
            record = self.records[claim.effect.effect_id]
            duplicate = self._duplicate_settlement(record, claim, failure_envelope)
            if duplicate is not None:
                return duplicate
            self._owned_record(claim)
            self._insert_mailbox(failure_envelope)
            record.status = DurableEffectStatus.FAILED
            record.last_error = error
            record.settled_claim = claim
            record.settled_event = failure_envelope
            self._clear_claim(record)
            self.actions.append(f"fail:{failure_envelope.event_id}")
            return EffectSettlementResult(
                status=EffectSettlementStatus.COMMITTED,
                effect_id=claim.effect.effect_id,
                event_id=failure_envelope.event_id,
                key=claim.key,
            )

    async def quarantine(
        self,
        claim: ClaimedEffect,
        *,
        reason: EffectQuarantineReason,
        message: str,
    ) -> EffectSettlementResult:
        async with self._lock:
            record = self.records[claim.effect.effect_id]
            self._owned_record(claim)
            effect = claim.effect
            envelope = SessionEventEnvelope(
                event_id=quarantined_event_id(effect),
                key=claim.key,
                kind="EffectQuarantined",
                ownership_generation=effect.ownership_generation,
                payload={
                    "attempt_count": claim.attempt_count,
                    "contract_signature": effect.contract_signature,
                    "contract_version": effect.contract_version,
                    "effect_id": effect.effect_id,
                    "effect_kind": effect.kind,
                    "failure_code": reason.value,
                    "failure_message": message,
                    "idempotency_key": effect.idempotency_key,
                    "operation_id": effect.operation_id,
                    "reason_code": reason.value,
                    "reason_message": message,
                },
                source="effect_store",
                occurred_at=self._clock(),
                causation_id=effect.source_event_id,
                correlation_id=effect.operation_id or effect.effect_id,
                trace_id=effect.trace_id,
                available_at=self._clock(),
                created_at=self._clock(),
            )
            self._insert_mailbox(envelope)
            record.status = DurableEffectStatus.FAILED
            record.last_error = f"{reason.value}: {message}"
            record.settled_claim = claim
            record.settled_event = envelope
            self._clear_claim(record)
            self.actions.append(f"quarantine:{envelope.event_id}")
            return EffectSettlementResult(
                status=EffectSettlementStatus.COMMITTED,
                effect_id=effect.effect_id,
                event_id=envelope.event_id,
                key=claim.key,
            )

    async def release(self, claim: ClaimedEffect, *, error: str) -> None:
        async with self._lock:
            record = self._owned_record(claim)
            record.status = DurableEffectStatus.PENDING
            record.available_at = self._clock()
            record.last_error = error
            self._clear_claim(record)

    async def recover_expired(self, *, worker_id: str) -> int:
        del worker_id
        recovered = 0
        async with self._lock:
            now = self._clock()
            for record in self.records.values():
                if record.status != DurableEffectStatus.PROCESSING:
                    continue
                if record.lease_until is not None and record.lease_until > now:
                    continue
                record.status = DurableEffectStatus.PENDING
                record.available_at = now
                record.last_error = "effect_lease_recovered"
                self._clear_claim(record)
                recovered += 1
        return recovered

    async def next_available_at(
        self,
        *,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
    ) -> float | None:
        async with self._lock:
            candidates = [
                record.available_at
                if record.status == DurableEffectStatus.PENDING
                else record.lease_until
                for record in self.records.values()
                if record.status in {DurableEffectStatus.PENDING, DurableEffectStatus.PROCESSING}
                and (
                    effect_contracts is None
                    or (record.effect.kind, record.effect.contract_version) in effect_contracts
                )
                and (record.effect.kind, record.effect.contract_version)
                not in excluded_effect_contracts
            ]
        values = [value for value in candidates if value is not None]
        return min(values) if values else None

    def _owned_record(self, claim: ClaimedEffect) -> _EffectRecord:
        record = self.records[claim.effect.effect_id]
        if (
            record.status != DurableEffectStatus.PROCESSING
            or record.claim_id != claim.claim_id
            or record.worker_id != claim.worker_id
        ):
            raise EffectClaimLost("effect is not owned by this claim")
        return record

    def _duplicate_settlement(
        self,
        record: _EffectRecord,
        claim: ClaimedEffect,
        envelope: SessionEventEnvelope,
    ) -> EffectSettlementResult | None:
        if record.status not in {
            DurableEffectStatus.COMPLETED,
            DurableEffectStatus.FAILED,
        }:
            return None
        if record.settled_claim is None or record.settled_event is None:
            raise AssertionError("settled record is incomplete")
        if record.settled_claim.claim_id != claim.claim_id:
            raise EffectClaimLost("a newer claim already settled the effect")
        if record.settled_event != envelope:
            raise EffectClaimLost("claim attempted a different duplicate settlement")
        return EffectSettlementResult(
            status=EffectSettlementStatus.ALREADY_COMMITTED,
            effect_id=claim.effect.effect_id,
            event_id=envelope.event_id,
            key=claim.key,
        )

    def _insert_mailbox(self, envelope: SessionEventEnvelope) -> None:
        key = (envelope.key, envelope.event_id)
        existing = self.mailbox.get(key)
        if existing is not None and existing != envelope:
            raise RuntimeError("mailbox event id conflict")
        self.mailbox[key] = envelope

    @staticmethod
    def _clear_claim(record: _EffectRecord) -> None:
        record.claim_id = ""
        record.worker_id = ""
        record.lease_until = None


class _ClaimSwitchingAuthorityEffectStore(_MemoryEffectStore):
    """Effect store that swaps to an equal authority while returning a claim."""

    async def claim_next(
        self,
        *,
        worker_id: str,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
    ) -> ClaimedEffect | None:
        claim = await super().claim_next(
            worker_id=worker_id,
            effect_contracts=effect_contracts,
            excluded_effect_contracts=excluded_effect_contracts,
        )
        if claim is not None:
            self.bind_effect_contract_authority(
                EffectContractAuthority(self.effect_contract_authority.contracts())
            )
        return claim


class _WakeRegistry:
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions
        self.keys: list[SessionKey] = []

    async def wake(self, key: SessionKey) -> None:
        self.keys.append(key)
        self.actions.append(f"wake:{key.profile_id}:{key.session_id}")

    async def recover(self) -> int:
        self.actions.append("recover")
        return 0


class _BlockingWakeRegistry:
    def __init__(self) -> None:
        self.block = True
        self.started = asyncio.Event()
        self.keys: list[SessionKey] = []
        self.recoveries = 0

    async def wake(self, key: SessionKey) -> None:
        self.keys.append(key)
        if self.block:
            self.started.set()
            await asyncio.Event().wait()

    async def recover(self) -> int:
        self.recoveries += 1
        return 2


def _contract(
    *,
    kind: str = "external_write",
    version: int = 1,
    lane: EffectLane = EffectLane.DEFAULT,
    completion_event_kind: str = "EffectCompleted",
    timeout_seconds: float = 30.0,
    max_attempts: int = 3,
    retry_base_seconds: float = 5.0,
    retry_max_seconds: float = 20.0,
    priority: int = 100,
    outcome_fence_fields: tuple[str, ...] | None = None,
) -> EffectExecutionContract:
    return EffectExecutionContract(
        effect_kind=kind,
        version=version,
        lane=lane,
        completion_event_kind=completion_event_kind,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        retry_base_seconds=retry_base_seconds,
        retry_max_seconds=retry_max_seconds,
        priority=priority,
        outcome_fence_fields=outcome_fence_fields,
    )


def _effect(
    effect_id: str = "effect-1",
    *,
    contract: EffectExecutionContract | None = None,
    completion_event_id: str | None = None,
    extra_payload: dict[str, object] | None = None,
) -> DurableEffectEnvelope:
    resolved_contract = contract or _contract()
    payload: dict[str, object] = {"input": "durable"}
    payload.update(extra_payload or {})
    if completion_event_id is not None:
        payload["completion_event_id"] = completion_event_id
    return DurableEffectEnvelope(
        effect_id=effect_id,
        key=SessionKey("profile-a", "bot:group:room"),
        kind=resolved_contract.effect_kind,
        idempotency_key=f"idempotency:{effect_id}",
        contract_version=resolved_contract.version,
        contract_signature=resolved_contract.signature,
        payload=payload,
        source_event_id="message:42",
        operation_id="operation-1",
        trace_id="trace-1",
    )


def _executor(
    store: _MemoryEffectStore,
    registry: _WakeRegistry,
    handler,
    *,
    now: list[float],
    max_attempts: int = 3,
    timeout_seconds: float = 30.0,
    lane: EffectLane = EffectLane.DEFAULT,
) -> DurableEffectExecutor:
    contract = _contract(
        max_attempts=max_attempts,
        timeout_seconds=timeout_seconds,
        lane=lane,
    )
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register("external_write", handler, contract=contract)
    return DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        worker_id="effect-worker",
        poll_interval_seconds=0.01,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )


def _historical_contract_signature(contract: EffectExecutionContract) -> str:
    """Reproduce the pre-outcome-declaration durable policy digest."""

    policy = {
        "completion_event_kind": contract.completion_event_kind,
        "completion_source": contract.completion_source,
        "effect_kind": contract.effect_kind,
        "lane": contract.lane.value,
        "max_attempts": contract.max_attempts,
        "priority": contract.priority,
        "retry_base_seconds": contract.retry_base_seconds,
        "retry_max_seconds": contract.retry_max_seconds,
        "timeout_seconds": contract.timeout_seconds,
        "version": contract.version,
    }
    canonical = json.dumps(
        policy,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def test_handler_registration_without_contract_requires_an_unambiguous_version() -> None:
    """Avoid silently binding a handler to an arbitrary durable version."""

    version_one = _contract(version=1)
    version_two = _contract(version=2)
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register_contract(version_one)
    handlers.register_contract(version_two)

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        return EffectHandlerResult()

    with pytest.raises(ValueError, match="multiple versions"):
        handlers.register(version_one.effect_kind, handler)

    assert handlers.handled_contracts() == ()


def test_handler_registration_without_contract_uses_the_only_known_version() -> None:
    """Permit concise registration when one durable version is unambiguous."""

    version_two = _contract(version=2)
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register_contract(version_two)

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        return EffectHandlerResult()

    handlers.register(version_two.effect_kind, handler)

    resolved_contract, resolved_handler = handlers.resolve(
        version_two.effect_kind,
        version_two.version,
    )
    assert resolved_contract == version_two
    assert resolved_handler is handler


@pytest.mark.asyncio
async def test_claim_time_authority_switch_never_invokes_handler() -> None:
    now = [100.0]
    store = _ClaimSwitchingAuthorityEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = _contract()
    await store.seed(_effect(contract=contract))
    handler_calls: list[str] = []

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        handler_calls.append(context.effect.effect_id)
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    with pytest.raises(EffectAuthorityChanged, match="changed authority"):
        await executor.run_once()

    assert handler_calls == []
    assert store.records["effect-1"].status is DurableEffectStatus.PROCESSING
    assert store.records["effect-1"].settled_event is None


@pytest.mark.asyncio
async def test_handler_time_authority_switch_cannot_settle_or_retry() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = _contract()
    await store.seed(_effect(contract=contract))
    handler_calls: list[str] = []

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        handler_calls.append(context.effect.effect_id)
        store.bind_effect_contract_authority(
            EffectContractAuthority(store.effect_contract_authority.contracts())
        )
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    with pytest.raises(EffectAuthorityChanged, match="changed authority"):
        await executor.run_once()

    assert handler_calls == ["effect-1"]
    assert store.records["effect-1"].status is DurableEffectStatus.PROCESSING
    assert store.records["effect-1"].settled_event is None


@pytest.mark.asyncio
async def test_handler_lanes_do_not_claim_known_contracts_without_handlers() -> None:
    """Keep incompletely wired known work recoverable for a later activation."""

    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    unwired = _contract(kind="known_unwired")
    wired = _contract(kind="known_wired")
    await store.seed(_effect("known-unwired", contract=unwired))
    await store.seed(_effect("known-wired", contract=wired))

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register_contract(unwired)
    handlers.register(wired.effect_kind, handler, contract=wired)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.COMPLETED
    assert result.effect_id == "known-wired"
    assert store.records["known-unwired"].status is DurableEffectStatus.PENDING
    assert store.records["known-unwired"].attempt_count == 0
    assert handlers.effect_contracts_for_lane(EffectLane.DEFAULT) == (
        unwired.ref,
        wired.ref,
    )
    assert handlers.handled_effect_contracts_for_lane(EffectLane.DEFAULT) == (
        wired.ref,
    )


@pytest.mark.asyncio
async def test_orphan_lane_skips_known_unwired_contracts_but_fails_unknown_work() -> None:
    """Reserve orphan settlement for work whose contract is genuinely unknown."""

    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    unwired = _contract(kind="known_unwired")
    unknown = _contract(kind="unknown_contract")
    await store.seed(_effect("known-unwired", contract=unwired))
    await store.seed(_effect("unknown", contract=unknown))
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register_contract(unwired)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once(lane=EffectLane.ORPHAN)

    assert result.status is EffectRunStatus.FAILED
    assert result.effect_id == "unknown"
    assert store.records["unknown"].status is DurableEffectStatus.FAILED
    assert store.records["known-unwired"].status is DurableEffectStatus.PENDING
    assert store.records["known-unwired"].attempt_count == 0
    event = next(iter(store.mailbox.values()))
    assert event.kind == "EffectQuarantined"
    assert event.payload["effect_kind"] == unknown.effect_kind
    assert event.payload["reason_code"] == "unsupported_contract"


@pytest.mark.asyncio
async def test_orphan_only_executor_is_not_started_as_a_handler_runtime() -> None:
    """Background orphan cleanup must not look like a healthy handler runtime."""

    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        poll_interval_seconds=60.0,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    await executor.start()
    try:
        assert executor.running is True
        assert executor.has_runnable_handlers is False
        assert executor.started is False
    finally:
        await executor.shutdown()


@pytest.mark.asyncio
async def test_stale_aba_claim_cannot_commit_after_reclaim() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    effect = _effect(completion_event_id="planner-completed:operation-1")
    await store.seed(effect)
    first_started = asyncio.Event()
    finish_first = asyncio.Event()
    observed_keys: list[str] = []
    calls = 0

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        nonlocal calls
        calls += 1
        observed_keys.append(context.idempotency_key)
        if calls == 1:
            first_started.set()
            await finish_first.wait()
        return EffectHandlerResult(payload={"outcome": "planned"})

    first_executor = _executor(store, registry, handler, now=now)
    second_executor = _executor(store, registry, handler, now=now)
    first_attempt = asyncio.create_task(first_executor.run_once(worker_id="same-worker"))
    await first_started.wait()

    now[0] = 106.0
    assert await store.recover_expired(worker_id="recovery") == 1
    second_result = await second_executor.run_once(worker_id="same-worker")
    finish_first.set()
    first_result = await first_attempt

    assert second_result.status == EffectRunStatus.COMPLETED
    assert first_result.status == EffectRunStatus.CLAIM_LOST
    assert observed_keys == [effect.idempotency_key, effect.idempotency_key]
    assert len(store.mailbox) == 1
    record = store.records[effect.effect_id]
    assert record.settled_claim is not None
    assert record.settled_claim.attempt_count == 2
    assert registry.keys == [effect.key]


@pytest.mark.asyncio
async def test_failed_handler_retries_after_restart_and_preserves_idempotency_key() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    effect = _effect()
    await store.seed(effect)
    seen: list[str] = []

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        seen.append(context.idempotency_key)
        if len(seen) == 1:
            raise OSError("provider unavailable")
        return EffectHandlerResult(payload={"value": "ok"})

    first_executor = _executor(store, registry, handler, now=now)
    first = await first_executor.run_once()
    assert first.status == EffectRunStatus.RETRY_SCHEDULED
    assert first.retry_at == 105.0
    assert (await first_executor.run_once()).status == EffectRunStatus.EMPTY

    now[0] = 105.0
    restarted_executor = _executor(store, registry, handler, now=now)
    second = await restarted_executor.run_once()

    assert second.status == EffectRunStatus.COMPLETED
    assert seen == [effect.idempotency_key, effect.idempotency_key]
    assert store.records[effect.effect_id].attempt_count == 2
    assert store.records[effect.effect_id].status == DurableEffectStatus.COMPLETED


@pytest.mark.asyncio
async def test_terminal_failure_atomically_inserts_effect_failed_before_wake() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = _contract(max_attempts=2)
    effect = _effect(
        contract=contract,
        extra_payload={
            "plan_id": "plan-1",
            "active_epoch": 4,
            "activity_generation": 7,
            "action_ordinal": 2,
            "expected_active_epoch": 4,
            "expected_activity_generation": 7,
            "request_digest": "a" * 64,
        },
    )
    await store.seed(effect)

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        raise RuntimeError("planner output was invalid")

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register("external_write", handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        worker_id="effect-worker",
        poll_interval_seconds=0.01,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )
    first = await executor.run_once()
    assert first.status == EffectRunStatus.RETRY_SCHEDULED
    now[0] = 105.0
    terminal = await executor.run_once()

    assert terminal.status == EffectRunStatus.FAILED
    event = next(iter(store.mailbox.values()))
    assert event.kind == "EffectFailed"
    assert event.payload["attempt_count"] == 2
    assert event.payload["failure_code"] == "RuntimeError"
    assert event.payload["plan_id"] == "plan-1"
    assert event.payload["active_epoch"] == 4
    assert event.payload["activity_generation"] == 7
    assert event.payload["action_ordinal"] == 2
    assert event.payload["expected_active_epoch"] == 4
    assert event.payload["expected_activity_generation"] == 7
    assert event.payload["request_digest"] == "a" * 64
    assert store.records[effect.effect_id].status == DurableEffectStatus.FAILED
    assert store.actions == [
        f"fail:{event.event_id}",
        f"wake:{effect.key.profile_id}:{effect.key.session_id}",
    ]


@pytest.mark.asyncio
async def test_duplicate_completion_is_idempotent_and_event_id_is_deterministic() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    effect = _effect()
    await store.seed(effect)
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None
    event = SessionEventEnvelope(
        event_id=completion_event_id(effect),
        key=effect.key,
        kind="EffectCompleted",
        payload={"effect_id": effect.effect_id},
    )

    committed = await store.complete_with_event(claim, event)
    duplicate = await store.complete_with_event(claim, event)

    assert committed.status == EffectSettlementStatus.COMMITTED
    assert duplicate.status == EffectSettlementStatus.ALREADY_COMMITTED
    assert committed.event_id == duplicate.event_id == completion_event_id(effect)
    assert len(store.mailbox) == 1


@pytest.mark.asyncio
async def test_shutdown_releases_claim_for_recovery_by_new_executor() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    effect = _effect()
    await store.seed(effect)
    first_started = asyncio.Event()
    first_cancelled = asyncio.Event()
    calls = 0

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        nonlocal calls
        calls += 1
        assert context.idempotency_key == effect.idempotency_key
        if calls == 1:
            first_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                first_cancelled.set()
                raise
        return EffectHandlerResult(payload={"recovered": True})

    first_executor = _executor(store, registry, handler, now=now)
    await first_executor.start()
    await asyncio.wait_for(first_started.wait(), timeout=1.0)
    await first_executor.shutdown(drain=False)

    record = store.records[effect.effect_id]
    assert first_cancelled.is_set()
    assert record.status == DurableEffectStatus.PENDING
    assert record.last_error == "effect_executor_shutdown"

    second_executor = _executor(store, registry, handler, now=now)
    recovered = await second_executor.run_once()
    assert recovered.status == EffectRunStatus.COMPLETED
    assert record.attempt_count == 2


@pytest.mark.asyncio
async def test_wake_cancellation_preserves_notifications_and_releases_unstarted_claim(
) -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    effect = _effect()
    await store.seed(effect)
    notification_keys = {
        SessionKey("profile-a", "bot:group:bad-a"),
        SessionKey("profile-a", "bot:group:bad-b"),
    }
    store.quarantine_notifications.extend(
        EffectSettlementResult(
            status=EffectSettlementStatus.COMMITTED,
            effect_id=f"bad-{index}",
            event_id=f"quarantine-{index}",
            key=key,
        )
        for index, key in enumerate(notification_keys)
    )
    calls = 0

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        nonlocal calls
        calls += 1
        return EffectHandlerResult()

    contract = _contract()
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    wake_registry = _BlockingWakeRegistry()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=wake_registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )
    task = asyncio.create_task(executor.run_once())
    await asyncio.wait_for(wake_registry.started.wait(), timeout=0.5)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    record = store.records[effect.effect_id]
    assert record.status is DurableEffectStatus.PENDING
    assert record.last_error == "effect_executor_shutdown"
    assert calls == 0
    assert not executor._active_claims
    assert executor._pending_wakes == notification_keys

    wake_registry.block = False
    result = await executor.run_once()

    assert wake_registry.recoveries == 1
    assert not executor._pending_wakes
    assert result.status is EffectRunStatus.COMPLETED
    assert calls == 1


@pytest.mark.asyncio
async def test_handler_can_explicitly_renew_long_running_claim() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    effect = _effect()
    await store.seed(effect)
    claim_ids: list[str] = []

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        claim_ids.append(context.claim.claim_id)
        now[0] = 104.0
        renewed = await context.renew_lease()
        claim_ids.append(renewed.claim_id)
        assert renewed.lease_expires_at == 109.0
        return EffectHandlerResult()

    executor = _executor(store, registry, handler, now=now)
    result = await executor.run_once()

    assert result.status == EffectRunStatus.COMPLETED
    assert claim_ids[0] == claim_ids[1]


@pytest.mark.asyncio
async def test_durable_fences_override_handler_completion_payload() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    effect = _effect(
        extra_payload={
            "plan_id": "authoritative-plan",
            "active_epoch": 2,
            "activity_generation": 3,
            "input_ledger_sequence": 17,
        }
    )
    await store.seed(effect)

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        return EffectHandlerResult(
            payload={
                "plan_id": "forged-plan",
                "active_epoch": 999,
                "activity_generation": 999,
                "outcome": "planned",
            }
        )

    executor = _executor(store, registry, handler, now=now)
    result = await executor.run_once()

    assert result.status == EffectRunStatus.COMPLETED
    event = next(iter(store.mailbox.values()))
    assert event.payload["plan_id"] == "authoritative-plan"
    assert event.payload["active_epoch"] == 2
    assert event.payload["activity_generation"] == 3
    assert event.payload["input_ledger_sequence"] == 17
    assert event.payload["outcome"] == "planned"


@dataclass(slots=True, frozen=True)
class _ActorEffectContractExpectation:
    """Frozen v1/v2 compatibility and optional current-v3 contract facts."""

    effect_kind: str
    v1_signature: str
    v2_signature: str
    v2_outcome_fence_fields: set[str]
    v3_signature: str | None = None
    v3_outcome_fence_fields: set[str] | None = None


_ACTOR_EFFECT_CONTRACT_MATRIX: tuple[_ActorEffectContractExpectation, ...] = (
    _ActorEffectContractExpectation(
        "enqueue_idle_review_planning_deadline",
        "3ef2697bcae60f3ad1b269917daf640ad8fc3601a8d2a141484a30d28c76a97a",
        "a2dd7e9359ea9a61c99509c3d5fbd90064dc7e3b80b50aea721ab342a12fea8f",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "deadline_event_id",
            "failure_event_id",
            "source",
            "trigger",
        },
    ),
    _ActorEffectContractExpectation(
        "active_chat_runtime_reconciliation",
        "a64a88da7387a3ca1cc305172f5b7e46bf333058ad655d0170dd0b0125887ab1",
        "effb2bc168531ce2ff402f567c813e6cecbde64f156d6735d065e00f74ba3845",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "desired_state",
            "control_effect_kind",
            "control_effect_id",
            "reconciliation_cycle",
        },
    ),
    _ActorEffectContractExpectation(
        "idle_review_planning_cancellation_reconciliation",
        "6850fb9f10fdb5d0b139e39bcb1e394b9f6c69a591a63ab5ed63458272fc331d",
        "30ce30bf2da1fb2610df3bf08860b0e2cb0e99ad4bcebbc69742583fa33e49af",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "desired_state",
            "control_effect_kind",
            "control_effect_id",
            "reconciliation_cycle",
        },
    ),
    _ActorEffectContractExpectation(
        "cancel_idle_review_planning",
        "b33dee9304b03b6ba87ace4dcb82f640a76215503a6ef8a04e8aca3115eeb2df",
        "854182201f0f1a0a2513a013edc439c18165513cda63eb449166afde63f1ddae",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "superseded_by_event_id",
        },
    ),
    _ActorEffectContractExpectation(
        "stop_active_chat_runtime",
        "e972918e09cf20bc6ab80194636dd2392e44a270e8cdade9ca1f1776dccc2400",
        "931c7b0614c2c833f25a21bca0938ccd5fd26fa1e546381e8af3851096f5f286",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
        },
    ),
    _ActorEffectContractExpectation(
        "cancel_review_workflow",
        "ee222172ee0f101d8ee8585f380685f1445f29b5c44f82ec0326277fd2da4370",
        "bfa750297968f3f079d2e6070dccb3d5c40dd4916bee27e299144ee6752a68a9",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "superseded_by_event_id",
        },
    ),
    _ActorEffectContractExpectation(
        "enqueue_active_chat_exit_request",
        "793ade014d0032b31aa0177420e41997a02a3a66fb4e4b7cad44828fa9789cff",
        "10b120c016023a6c99c997c0bbcc41d7cbfd2cb15c2cc4ba3ebe4f7b9f2f86df",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "superseded_by_event_id",
            "trigger",
            "expected_active_epoch",
            "expected_message_watermark",
        },
    ),
    _ActorEffectContractExpectation(
        "enqueue_active_chat_round_due",
        "0eeed00a50957415526ab0c2d6caf4157c38aee6307aff095b89ebf0dfdd3230",
        "cc4ab0a043d88d9f926847fb02ad91532672adf1ab3ac077c0a4215b055bfa61",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "superseded_by_event_id",
            "schedule_id",
            "schedule_revision",
        },
    ),
    _ActorEffectContractExpectation(
        "run_active_reply_workflow",
        "8f0a8991b6202f0a277d1d16c35de09cf8f02951f54e04f886b00c93d7f80f80",
        "a7b5461c1882b7bbd947e12fd9ba39e7dfaf8e35ef1d267cd8fa5d3c1e654c80",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
        },
    ),
    _ActorEffectContractExpectation(
        "run_active_chat_bootstrap",
        "1f4b0372633f7ca9f794618409fad81a92721cc108ede35c457092a3f43c0f94",
        "7a9e87fa306a389432fa886a7b42b5d12ad75b3fd84a95f7467e85382bad20b7",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
        },
        v3_signature="1b78b5d692155f549b73492f0949765c18693384cb0565f5c3c251794d59e91c",
        v3_outcome_fence_fields={
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "handoff_message_log_ids",
            "handoff_operation_id",
        },
    ),
    _ActorEffectContractExpectation(
        "run_active_chat_round",
        "883d69bc054c750c2fb7ba92b4b9e5164faacd89895e32cabdc66d6a7f0c6eb5",
        "5647695cf181283d51aa57b58989b62ffd39682f843272618939abb3610c663d",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
        },
        v3_signature="6cf248e0db727b8a6542bb2347d42203ff68298660a09da8f9775fd8355199ac",
        v3_outcome_fence_fields={
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "active_chat_interest_value",
            "bootstrap_disposition",
            "message_log_ids",
            "round_schedule_id",
        },
    ),
    _ActorEffectContractExpectation(
        "run_review_workflow",
        "9edc415f442b9136e24864705f0df9711b125d9375d32befc47776a01011013e",
        "e1bbaa80c643890b441264013887dbfc202abaa03c8cc47f8ab2547b14d7b250",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
        },
    ),
    _ActorEffectContractExpectation(
        "run_idle_review_planning",
        "bbca6267e9a16a690312269aa770263a99180ee753e1abcb75aeeb79fbc8562d",
        "13f2272fb25d1e4cb5f6cc5135026e3588cd597cae2911ad25460d599f8477ee",
        {
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "source",
            "trigger",
        },
    ),
)


@pytest.mark.parametrize(
    "expectation",
    _ACTOR_EFFECT_CONTRACT_MATRIX,
    ids=lambda expectation: expectation.effect_kind,
)
def test_builtin_actor_effect_contract_matrix_is_exact(
    expectation: _ActorEffectContractExpectation,
) -> None:
    current = builtin_effect_contract(expectation.effect_kind)
    legacy = builtin_effect_contract(expectation.effect_kind, version=1)
    v2 = builtin_effect_contract(expectation.effect_kind, version=2)

    assert v2.version == 2
    assert v2.signature == expectation.v2_signature
    assert legacy.version == 1
    assert legacy.signature == expectation.v1_signature
    assert legacy.outcome_fence_fields is None
    assert legacy.signature == _historical_contract_signature(legacy)
    legacy_expected = set(DEFAULT_OUTCOME_FENCE_FIELDS)
    if expectation.effect_kind in {
        "cancel_review_workflow",
        "enqueue_active_chat_exit_request",
        "enqueue_active_chat_round_due",
    }:
        legacy_expected.update(expectation.v2_outcome_fence_fields)
    assert set(resolved_outcome_fence_fields(legacy)) == legacy_expected
    assert v2.outcome_fence_fields is not None
    assert set(v2.outcome_fence_fields) == expectation.v2_outcome_fence_fields
    assert v2.signature != legacy.signature

    if expectation.v3_signature is None:
        assert expectation.v3_outcome_fence_fields is None
        assert current == v2
        return

    assert expectation.v3_outcome_fence_fields is not None
    assert current.version == 3
    assert current.signature == expectation.v3_signature
    assert current.outcome_fence_fields is not None
    assert set(current.outcome_fence_fields) == expectation.v3_outcome_fence_fields
    assert current.signature != v2.signature


def _valid_outcome_fence_payload(
    field_names: set[str] | tuple[str, ...],
) -> dict[str, object]:
    """Build schema-valid values for built-in outcome-fence declarations."""

    nonnegative_integer_fields = {
        "action_ordinal",
        "active_epoch",
        "activity_generation",
        "expected_active_epoch",
        "expected_activity_generation",
        "expected_message_watermark",
        "expected_state_revision",
        "input_ledger_sequence",
        "input_watermark",
        "reconciliation_cycle",
        "schedule_revision",
        "state_revision",
    }
    values: dict[str, object] = {}
    for field_name in field_names:
        if field_name in {"handoff_message_log_ids", "message_log_ids"}:
            values[field_name] = [101, 102]
        elif field_name == "active_chat_interest_value":
            values[field_name] = 12.5
        elif field_name == "bootstrap_disposition":
            values[field_name] = "watch"
        elif field_name == "handoff_operation_id":
            values[field_name] = "handoff-operation-1"
        elif field_name == "round_schedule_id":
            values[field_name] = "round-schedule-1"
        elif field_name in nonnegative_integer_fields:
            values[field_name] = 7
        else:
            values[field_name] = f"durable:{field_name}"
    return values


@pytest.mark.parametrize(
    "expectation",
    _ACTOR_EFFECT_CONTRACT_MATRIX,
    ids=lambda expectation: expectation.effect_kind,
)
def test_durable_effect_requires_every_current_contract_fence_field(
    expectation: _ActorEffectContractExpectation,
) -> None:
    current = builtin_effect_contract(expectation.effect_kind)
    expected_fields = set(current.outcome_fence_fields or ())
    payload = _valid_outcome_fence_payload(expected_fields)
    effect = session_actor_reducer._durable_effect(
        effect_id="effect-1",
        kind=expectation.effect_kind,
        idempotency_key="effect-1",
        operation_id="operation-1",
        payload=payload,
    )

    assert effect.contract_version == current.version
    assert effect.contract_signature == current.signature
    assert (
        validate_effect_declaration(
            effect,
            authority=builtin_effect_contract_authority(),
        )
        == current
    )

    for missing_field in sorted(expected_fields):
        incomplete_payload = dict(payload)
        incomplete_payload.pop(missing_field)
        with pytest.raises(ValueError, match=missing_field):
            session_actor_reducer._durable_effect(
                effect_id=f"effect-missing-{missing_field}",
                kind=expectation.effect_kind,
                idempotency_key=f"effect-missing-{missing_field}",
                operation_id="operation-2",
                payload=incomplete_payload,
            )


@pytest.mark.parametrize(
    ("effect_kind", "expected_fields"),
    [
        (
            "cancel_review_workflow",
            {
                "plan_id",
                "active_epoch",
                "activity_generation",
                "input_watermark",
                "input_ledger_sequence",
                "completion_event_id",
                "failure_event_id",
                "superseded_by_event_id",
            },
        ),
        (
            "enqueue_active_chat_exit_request",
            {
                "plan_id",
                "active_epoch",
                "activity_generation",
                "input_watermark",
                "input_ledger_sequence",
                "completion_event_id",
                "failure_event_id",
                "superseded_by_event_id",
                "trigger",
                "expected_active_epoch",
                "expected_message_watermark",
            },
        ),
        (
            "enqueue_active_chat_round_due",
            {
                "plan_id",
                "active_epoch",
                "activity_generation",
                "input_watermark",
                "input_ledger_sequence",
                "completion_event_id",
                "failure_event_id",
                "superseded_by_event_id",
                "schedule_id",
                "schedule_revision",
            },
        ),
    ],
)
def test_existing_v2_control_contract_fields_remain_unchanged(
    effect_kind: str,
    expected_fields: set[str],
) -> None:
    current = builtin_effect_contract(effect_kind)

    assert set(current.outcome_fence_fields or ()) == expected_fields


def test_outcome_fence_declaration_changes_contract_signature() -> None:
    contract = _contract(
        version=2,
        outcome_fence_fields=("plan_id", "input_watermark"),
    )
    reordered = replace(
        contract,
        outcome_fence_fields=("input_watermark", "plan_id"),
    )
    changed = replace(
        contract,
        outcome_fence_fields=("plan_id", "activity_generation"),
    )

    assert reordered.outcome_fence_fields == contract.outcome_fence_fields
    assert reordered.signature == contract.signature
    assert changed.signature != contract.signature


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "effect_kind",
    (
        "cancel_review_workflow",
        "enqueue_active_chat_exit_request",
        "enqueue_active_chat_round_due",
        "run_active_chat_bootstrap",
        "run_active_chat_round",
    ),
)
async def test_executor_projects_only_declared_current_completion_fences(
    effect_kind: str,
) -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = builtin_effect_contract(effect_kind)
    fence_fields = resolved_outcome_fence_fields(contract)
    fence_payload = _valid_outcome_fence_payload(fence_fields)
    await store.seed(
        _effect(
            contract=contract,
            extra_payload={
                **fence_payload,
                "undeclared_effect_payload": "must not escape",
            },
        )
    )

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        return EffectHandlerResult(
            payload={
                "handler_result": "kept",
                "plan_id": "forged-handler-value",
            }
        )

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.COMPLETED
    event = next(iter(store.mailbox.values()))
    assert {field_name: event.payload[field_name] for field_name in fence_fields} == (fence_payload)
    assert event.payload["handler_result"] == "kept"
    assert "input" not in event.payload
    assert "undeclared_effect_payload" not in event.payload
    assert store.completion_fence_fields == [fence_fields]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "effect_kind",
    (
        "cancel_review_workflow",
        "enqueue_active_chat_exit_request",
        "enqueue_active_chat_round_due",
        "run_active_chat_bootstrap",
        "run_active_chat_round",
    ),
)
async def test_executor_projects_only_declared_current_failure_fences(
    effect_kind: str,
) -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = replace(builtin_effect_contract(effect_kind), max_attempts=1)
    fence_fields = resolved_outcome_fence_fields(contract)
    fence_payload = _valid_outcome_fence_payload(fence_fields)
    await store.seed(
        _effect(
            contract=contract,
            extra_payload={
                **fence_payload,
                "undeclared_effect_payload": "must not escape",
            },
        )
    )

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        raise RuntimeError("terminal handler failure")

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.FAILED
    event = next(iter(store.mailbox.values()))
    assert event.kind == "EffectFailed"
    assert {field_name: event.payload[field_name] for field_name in fence_fields} == (fence_payload)
    assert "input" not in event.payload
    assert "undeclared_effect_payload" not in event.payload
    assert store.failure_fence_fields == [fence_fields]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("effect_kind", "required_fields"),
    (
        (
            "cancel_review_workflow",
            ("completion_event_id", "failure_event_id", "superseded_by_event_id"),
        ),
        (
            "enqueue_active_chat_exit_request",
            (
                "completion_event_id",
                "failure_event_id",
                "expected_message_watermark",
            ),
        ),
        (
            "enqueue_active_chat_round_due",
            (
                "completion_event_id",
                "failure_event_id",
                "schedule_id",
                "schedule_revision",
            ),
        ),
    ),
)
async def test_executor_projects_legacy_v1_control_compatibility_fences(
    effect_kind: str,
    required_fields: tuple[str, ...],
) -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = builtin_effect_contract(effect_kind, version=1)
    fence_fields = resolved_outcome_fence_fields(contract)
    fence_payload = _valid_outcome_fence_payload(fence_fields)
    await store.seed(_effect(contract=contract, extra_payload=fence_payload))

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.COMPLETED
    event = next(iter(store.mailbox.values()))
    assert all(
        event.payload[field_name] == fence_payload[field_name] for field_name in required_fields
    )
    assert store.completion_fence_fields == [fence_fields]
    assert contract.signature == _historical_contract_signature(contract)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("effect_kind", "required_fields"),
    (
        (
            "enqueue_active_chat_exit_request",
            ("failure_event_id", "expected_message_watermark"),
        ),
        (
            "enqueue_active_chat_round_due",
            ("failure_event_id", "schedule_id", "schedule_revision"),
        ),
    ),
)
async def test_executor_projects_legacy_v1_failure_compatibility_fences(
    effect_kind: str,
    required_fields: tuple[str, ...],
) -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = builtin_effect_contract(effect_kind, version=1)
    fence_fields = resolved_outcome_fence_fields(contract)
    fence_payload = _valid_outcome_fence_payload(fence_fields)
    await store.seed(
        _effect(contract=contract, extra_payload=fence_payload),
        attempt_count=contract.max_attempts - 1,
    )

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        raise RuntimeError("terminal handler failure")

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.FAILED
    event = next(iter(store.mailbox.values()))
    assert event.kind == "EffectFailed"
    assert all(
        event.payload[field_name] == fence_payload[field_name] for field_name in required_fields
    )
    assert store.failure_fence_fields == [fence_fields]
    assert contract.signature == _historical_contract_signature(contract)


def test_retry_delay_caps_before_computing_large_exponent() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        return EffectHandlerResult()

    executor = _executor(store, registry, handler, now=now)

    assert executor._retry_delay(_contract(), 10**100) == 20.0


def test_effect_payload_is_deeply_immutable_and_rejects_non_finite_json() -> None:
    contract = _contract()
    source = {"nested": {"items": [1, {"value": "stable"}]}}
    effect = _effect(contract=contract, extra_payload=source)

    source["nested"]["items"].append(2)  # type: ignore[index,union-attr]
    nested = effect.payload["nested"]
    assert nested["items"] == [1, {"value": "stable"}]
    with pytest.raises(TypeError, match="immutable"):
        nested["items"].append(3)
    with pytest.raises(TypeError, match="mapping keys"):
        DurableEffectEnvelope(
            effect_id="bad-key",
            key=effect.key,
            kind=contract.effect_kind,
            idempotency_key="bad-key",
            contract_version=contract.version,
            contract_signature=contract.signature,
            payload={1: "not-a-string"},  # type: ignore[dict-item]
        )
    with pytest.raises(ValueError, match="finite"):
        DurableEffectEnvelope(
            effect_id="bad-number",
            key=effect.key,
            kind=contract.effect_kind,
            idempotency_key="bad-number",
            contract_version=contract.version,
            contract_signature=contract.signature,
            payload={"nested": [float("nan")]},
        )


@pytest.mark.asyncio
async def test_control_lane_runs_while_all_planner_workers_are_blocked() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    wake_registry = _WakeRegistry(store.actions)
    planner_contract = _contract(
        kind="planner",
        lane=EffectLane.PLANNER,
        timeout_seconds=5.0,
    )
    deadline_contract = _contract(
        kind="deadline",
        lane=EffectLane.CONTROL,
        completion_event_kind="DeadlineReached",
        timeout_seconds=1.0,
        priority=0,
    )
    planner_release = asyncio.Event()
    planner_started = asyncio.Event()
    deadline_completed = asyncio.Event()
    started_count = 0

    async def planner_handler(
        _context: EffectExecutionContext,
    ) -> EffectHandlerResult:
        nonlocal started_count
        started_count += 1
        if started_count == 2:
            planner_started.set()
        while not planner_release.is_set():
            try:
                await planner_release.wait()
            except asyncio.CancelledError:
                continue
        return EffectHandlerResult()

    async def deadline_handler(
        _context: EffectExecutionContext,
    ) -> EffectHandlerResult:
        deadline_completed.set()
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register("planner", planner_handler, contract=planner_contract)
    handlers.register("deadline", deadline_handler, contract=deadline_contract)
    for index in range(2):
        await store.seed(_effect(f"planner-{index}", contract=planner_contract))
    await store.seed(_effect("deadline-1", contract=deadline_contract))
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=wake_registry,
        worker_id="lane-test",
        worker_count=2,
        control_worker_count=1,
        poll_interval_seconds=0.01,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )
    await executor.start()
    try:
        await asyncio.wait_for(planner_started.wait(), timeout=0.5)
        await asyncio.wait_for(deadline_completed.wait(), timeout=0.2)
        for _index in range(10):
            if store.records["deadline-1"].status is DurableEffectStatus.COMPLETED:
                break
            await asyncio.sleep(0)
        assert store.records["deadline-1"].status is DurableEffectStatus.COMPLETED
        assert all(
            store.records[f"planner-{index}"].status is DurableEffectStatus.PROCESSING
            for index in range(2)
        )
    finally:
        planner_release.set()
        await executor.shutdown(drain=True)


@pytest.mark.asyncio
async def test_timeout_releases_worker_without_awaiting_cancellation_tail() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    wake_registry = _WakeRegistry(store.actions)
    contract = _contract(
        kind="planner",
        lane=EffectLane.PLANNER,
        timeout_seconds=0.02,
        max_attempts=1,
    )
    first_cancelled = asyncio.Event()
    release_late_handler = asyncio.Event()
    second_completed = asyncio.Event()

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        if context.effect.effect_id == "first":
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                first_cancelled.set()
                await release_late_handler.wait()
                return EffectHandlerResult(payload={"late": True})
        second_completed.set()
        return EffectHandlerResult(payload={"second": True})

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register("planner", handler, contract=contract)
    await store.seed(_effect("first", contract=contract))
    await store.seed(_effect("second", contract=contract))
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=wake_registry,
        worker_id="timeout-test",
        worker_count=1,
        poll_interval_seconds=0.01,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )
    await executor.start()
    try:
        await asyncio.wait_for(first_cancelled.wait(), timeout=0.5)
        await asyncio.wait_for(second_completed.wait(), timeout=0.5)
        for _index in range(10):
            if store.records["second"].status is DurableEffectStatus.COMPLETED:
                break
            await asyncio.sleep(0)
        first_record = store.records["first"]
        assert first_record.status is DurableEffectStatus.FAILED
        assert first_record.settled_event is not None
        assert first_record.settled_event.kind == "EffectFailed"
        assert store.records["second"].status is DurableEffectStatus.COMPLETED
        mailbox_count = len(store.mailbox)
        release_late_handler.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert len(store.mailbox) == mailbox_count
    finally:
        release_late_handler.set()
        await executor.shutdown(drain=True)


@pytest.mark.asyncio
async def test_persisted_v1_is_orphaned_when_restart_has_only_v2_contract() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    wake_registry = _WakeRegistry(store.actions)
    version_one = _contract(version=1)
    version_two = _contract(version=2)
    await store.seed(_effect(contract=version_one))
    calls = 0

    async def v2_handler(
        _context: EffectExecutionContext,
    ) -> EffectHandlerResult:
        nonlocal calls
        calls += 1
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register("external_write", v2_handler, contract=version_two)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=wake_registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once(lane=EffectLane.ORPHAN)

    assert result.status is EffectRunStatus.FAILED
    assert calls == 0
    event = next(iter(store.mailbox.values()))
    assert event.kind == "EffectQuarantined"
    assert event.payload["contract_version"] == 1
    assert event.payload["reason_code"] == "unsupported_contract"


@pytest.mark.asyncio
async def test_contract_signature_drift_fails_without_running_handler() -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    wake_registry = _WakeRegistry(store.actions)
    persisted_contract = _contract(timeout_seconds=10.0)
    changed_without_version_bump = _contract(timeout_seconds=20.0)
    await store.seed(_effect(contract=persisted_contract))
    calls = 0

    async def handler(
        _context: EffectExecutionContext,
    ) -> EffectHandlerResult:
        nonlocal calls
        calls += 1
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(
        "external_write",
        handler,
        contract=changed_without_version_bump,
    )
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=wake_registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.FAILED
    assert calls == 0
    event = next(iter(store.mailbox.values()))
    assert event.kind == "EffectQuarantined"
    assert event.payload["reason_code"] == "contract_signature_mismatch"
