from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass, replace

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    DEFAULT_OUTCOME_FENCE_FIELDS,
    builtin_effect_contract,
    resolved_outcome_fence_fields,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    DurableEffectExecutor,
    DurableEffectStatus,
    EffectClaimLost,
    EffectContractSignatureMismatch,
    EffectExecutionContext,
    EffectExecutionContract,
    EffectHandlerRegistry,
    EffectHandlerResult,
    EffectLane,
    EffectRunStatus,
    EffectSettlementResult,
    EffectSettlementStatus,
    completion_event_id,
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
        self._lock = asyncio.Lock()
        self.records: dict[str, _EffectRecord] = {}
        self.order: list[str] = []
        self.mailbox: dict[tuple[SessionKey, str], SessionEventEnvelope] = {}
        self.actions: list[str] = []
        self.completion_fence_fields: list[tuple[str, ...]] = []
        self.failure_fence_fields: list[tuple[str, ...]] = []

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
    assert event.payload["effect_kind"] == unknown.effect_kind
    assert event.payload["failure_code"] == "EffectHandlerNotFound"


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
def test_builtin_effect_contract_resolves_current_v2_and_legacy_v1(
    effect_kind: str,
    expected_fields: set[str],
) -> None:
    current = builtin_effect_contract(effect_kind)
    legacy = builtin_effect_contract(effect_kind, version=1)

    assert current.version == 2
    assert legacy.version == 1
    assert legacy.outcome_fence_fields is None
    assert set(resolved_outcome_fence_fields(legacy)) == {
        *DEFAULT_OUTCOME_FENCE_FIELDS,
        *expected_fields,
    }
    assert legacy.signature == _historical_contract_signature(legacy)
    assert current.outcome_fence_fields is not None
    assert set(current.outcome_fence_fields) == expected_fields
    assert current.signature != legacy.signature


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
    ),
)
async def test_executor_projects_only_declared_v2_completion_fences(
    effect_kind: str,
) -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = builtin_effect_contract(effect_kind)
    fence_fields = resolved_outcome_fence_fields(contract)
    fence_payload = {field_name: f"durable:{field_name}" for field_name in fence_fields}
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
    ),
)
async def test_executor_projects_only_declared_v2_failure_fences(
    effect_kind: str,
) -> None:
    now = [100.0]
    store = _MemoryEffectStore(now)
    registry = _WakeRegistry(store.actions)
    contract = replace(builtin_effect_contract(effect_kind), max_attempts=1)
    fence_fields = resolved_outcome_fence_fields(contract)
    fence_payload = {field_name: f"durable:{field_name}" for field_name in fence_fields}
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
    fence_payload = {field_name: f"durable:{field_name}" for field_name in fence_fields}
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
    fence_payload = {field_name: f"durable:{field_name}" for field_name in fence_fields}
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
    assert event.kind == "EffectFailed"
    assert event.payload["contract_version"] == 1
    assert event.payload["failure_code"] == "EffectHandlerNotFound"


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
    assert event.payload["failure_code"] == EffectContractSignatureMismatch.__name__
