from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    DEFAULT_OUTCOME_FENCE_FIELDS,
    EffectContractAuthority,
    builtin_effect_contract,
    builtin_effect_contract_authority,
    resolved_outcome_fence_fields,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectEnvelope,
    DurableEffectExecutor,
    DurableEffectStatus,
    EffectClaimLost,
    EffectExecutionContext,
    EffectExecutionContract,
    EffectHandlerRegistry,
    EffectHandlerResult,
    EffectLane,
    EffectRunStatus,
    EffectSettlementStatus,
    completion_event_id,
    failure_event_id,
    skipped_event_id,
)
from shinbot.agent.runtime.session_actor.effect_store import (
    EffectStoreConflict,
    SQLiteDurableEffectStore,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager


class _WakeRegistry:
    def __init__(self) -> None:
        self.keys: list[SessionKey] = []

    async def wake(self, key: SessionKey) -> None:
        self.keys.append(key)

    async def recover(self) -> int:
        return 0


class _TransientWakeRegistry:
    def __init__(self, delegate: AgentSessionActorRegistry) -> None:
        self.delegate = delegate
        self.wake_attempts = 0
        self.recover_attempts = 0

    async def wake(self, key: SessionKey) -> None:
        self.wake_attempts += 1
        if self.wake_attempts == 1:
            raise RuntimeError("transient actor wake failure")
        await self.delegate.wake(key)

    async def recover(self) -> int:
        self.recover_attempts += 1
        return await self.delegate.recover()


async def _make_store(
    tmp_path: Path,
    now: list[float],
    *,
    contracts: tuple[EffectExecutionContract, ...] = (),
) -> tuple[DatabaseManager, SQLiteDurableEffectStore, SessionKey]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "bot:group:room")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="effect store test",
    ).ownership
    await SQLiteSessionActorStore(database, clock=lambda: now[0]).ensure(
        key,
        ownership_generation=ownership.generation,
    )
    return (
        database,
        SQLiteDurableEffectStore(
            database,
            lease_seconds=5.0,
            clock=lambda: now[0],
            contract_authority=EffectContractAuthority(
                (
                    *builtin_effect_contract_authority().contracts(),
                    _external_contract(),
                    *contracts,
                )
            ),
        ),
        key,
    )


def _seed_effect(
    database: DatabaseManager,
    key: SessionKey,
    *,
    effect_id: str = "effect-1",
    kind: str = "external_write",
    operation_id: str = "operation-1",
    payload: dict[str, object] | None = None,
    contract: EffectExecutionContract | None = None,
    now: float = 100.0,
) -> None:
    resolved_contract = contract or _external_contract()
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id, event_id,
                ownership_generation, operation_id, kind, contract_version,
                contract_signature,
                payload_json, status, attempt_count,
                available_at, claim_id, lease_owner, lease_until, created_at,
                updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, ?, NULL, '')
            """,
            (
                effect_id,
                f"idempotency:{effect_id}",
                key.profile_id,
                key.session_id,
                f"source:{effect_id}",
                1,
                operation_id,
                kind,
                resolved_contract.version,
                resolved_contract.signature,
                json.dumps(
                    payload or {},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                now,
                now,
                now,
            ),
        )


def _seed_operation(
    database: DatabaseManager,
    key: SessionKey,
    *,
    operation_id: str,
    status: str,
    now: float,
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, kind, status,
                ownership_generation,
                launched_by_event_id, state_revision, active_epoch,
                activity_generation, input_watermark, started_at, lease_owner,
                lease_until, superseded_at, finished_at, failure_code,
                failure_message, metadata_json
            ) VALUES (?, ?, ?, 'idle_review_planning', ?, 1, '', 0, 0, 0, NULL, ?, '', NULL, NULL, NULL, '', '', '{}')
            """,
            (operation_id, key.profile_id, key.session_id, status, now),
        )


def _completion(claim, *, event_id: str | None = None) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id or completion_event_id(claim.effect),
        key=claim.key,
        kind="EffectCompleted",
        ownership_generation=claim.effect.ownership_generation,
        payload={
            "effect_id": claim.effect.effect_id,
            "contract_version": claim.effect.contract_version,
            "contract_signature": claim.effect.contract_signature,
        },
        source="effect_executor",
        causation_id=claim.effect.source_event_id,
        correlation_id=claim.effect.operation_id,
    )


def _failure(
    claim,
    *,
    outcome_fence_fields: tuple[str, ...] = DEFAULT_OUTCOME_FENCE_FIELDS,
) -> SessionEventEnvelope:
    payload = {
        **claim.effect.outcome_fence_payload(outcome_fence_fields),
        **{
            field_name: claim.effect.payload[field_name]
            for field_name in ("action_ordinal", "request_digest")
            if field_name in claim.effect.payload
        },
        "attempt_count": claim.attempt_count,
        "contract_signature": claim.effect.contract_signature,
        "contract_version": claim.effect.contract_version,
        "effect_id": claim.effect.effect_id,
        "effect_kind": claim.effect.kind,
        "failure_code": "EffectHandlerError",
        "failure_message": "the controlled handler failed",
        "idempotency_key": claim.effect.idempotency_key,
        "operation_id": claim.effect.operation_id,
    }
    return SessionEventEnvelope(
        event_id=failure_event_id(claim.effect),
        key=claim.key,
        kind="EffectFailed",
        ownership_generation=claim.effect.ownership_generation,
        payload=payload,
        source="effect_executor",
        causation_id=claim.effect.source_event_id,
        correlation_id=claim.effect.operation_id or claim.effect.effect_id,
        trace_id=claim.effect.trace_id,
    )


def _external_contract(
    *,
    kind: str = "external_write",
    version: int = 1,
    completion_event_kind: str = "EffectCompleted",
    lane: EffectLane = EffectLane.DEFAULT,
    priority: int = 100,
    outcome_fence_fields: tuple[str, ...] | None = None,
) -> EffectExecutionContract:
    return EffectExecutionContract(
        effect_kind=kind,
        version=version,
        lane=lane,
        completion_event_kind=completion_event_kind,
        max_attempts=1,
        priority=priority,
        outcome_fence_fields=outcome_fence_fields,
    )


@pytest.mark.asyncio
async def test_sqlite_effect_store_fences_aba_and_replays_same_settlement(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key)
    first = await store.claim_next(worker_id="same-worker")
    assert first is not None

    now[0] = 106.0
    second = await store.claim_next(worker_id="same-worker")
    assert second is not None
    assert second.claim_id != first.claim_id
    assert second.attempt_count == 2
    envelope = _completion(second)
    committed = await store.complete_with_event(second, envelope)
    duplicate = await store.complete_with_event(second, envelope)

    assert committed.status == EffectSettlementStatus.COMMITTED
    assert duplicate.status == EffectSettlementStatus.ALREADY_COMMITTED
    with pytest.raises(EffectClaimLost):
        await store.complete_with_event(first, _completion(first))
    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, claim_id, lease_owner, attempt_count
            FROM agent_effect_outbox WHERE effect_id = 'effect-1'
            """
        ).fetchone()
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
    assert dict(effect) == {
        "status": DurableEffectStatus.COMPLETED.value,
        "claim_id": second.claim_id,
        "lease_owner": "",
        "attempt_count": 2,
    }
    assert mailbox_count == 1


@pytest.mark.asyncio
async def test_sqlite_effect_settlement_rolls_back_on_mailbox_identity_conflict(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key)
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None
    envelope = _completion(claim)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, kind, source, occurred_at,
                ownership_generation,
                payload_json, causation_id, correlation_id, trace_id,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, handled_at, last_error
            ) VALUES (?, ?, ?, 'DifferentEvent', '', ?, 1, '{}', '', '', '', 'pending', 0, ?, '', '', NULL, ?, NULL, '')
            """,
            (envelope.event_id, key.profile_id, key.session_id, now[0], now[0], now[0]),
        )

    with pytest.raises(EffectStoreConflict, match="already used"):
        await store.complete_with_event(claim, envelope)

    with database.connect() as conn:
        effect = conn.execute(
            "SELECT status, claim_id FROM agent_effect_outbox WHERE effect_id = 'effect-1'"
        ).fetchone()
    assert tuple(effect) == (DurableEffectStatus.PROCESSING.value, claim.claim_id)


@pytest.mark.asyncio
async def test_sqlite_effect_store_accepts_only_exact_terminal_failure_evidence(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(
        database,
        key,
        payload={
            "action_ordinal": 2,
            "plan_id": "plan-a",
            "request_digest": "a" * 64,
        },
    )
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None

    committed = await store.fail_with_event(
        claim,
        _failure(claim),
        error="the controlled handler failed",
    )
    duplicate = await store.fail_with_event(
        claim,
        _failure(claim),
        error="the controlled handler failed",
    )

    assert committed.status is EffectSettlementStatus.COMMITTED
    assert duplicate.status is EffectSettlementStatus.ALREADY_COMMITTED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("envelope_changes", "payload_changes", "match"),
    [
        ({"event_id": "forged-failure"}, {}, "event id"),
        ({"kind": "EffectCompleted"}, {}, "event kind"),
        ({"source": "forged"}, {}, "source"),
        ({"causation_id": "forged"}, {}, "causation"),
        ({"correlation_id": "forged"}, {}, "correlation"),
        ({"trace_id": "forged"}, {}, "trace"),
        ({}, {"plan_id": "forged"}, "fence plan_id"),
        ({}, {"action_ordinal": 3}, "action action_ordinal"),
        ({}, {"request_digest": "b" * 64}, "action request_digest"),
        ({}, {"attempt_count": 0}, "attempt"),
        ({}, {"failure_code": ""}, "failure code"),
    ],
)
async def test_sqlite_effect_store_rejects_forged_terminal_failure_evidence(
    tmp_path: Path,
    envelope_changes: dict[str, object],
    payload_changes: dict[str, object],
    match: str,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(
        database,
        key,
        payload={
            "action_ordinal": 2,
            "plan_id": "plan-a",
            "request_digest": "a" * 64,
        },
    )
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None
    failure = _failure(claim)
    forged = replace(
        failure,
        payload={**failure.payload, **payload_changes},
        **envelope_changes,
    )

    with pytest.raises(EffectStoreConflict, match=match):
        await store.fail_with_event(
            claim,
            forged,
            error="the controlled handler failed",
        )

    with database.connect() as conn:
        effect = conn.execute(
            "SELECT status, claim_id FROM agent_effect_outbox WHERE effect_id = 'effect-1'"
        ).fetchone()
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
    assert tuple(effect) == (DurableEffectStatus.PROCESSING.value, claim.claim_id)
    assert mailbox_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("changed", "missing"))
async def test_sqlite_effect_store_validates_declared_v2_failure_fences(
    tmp_path: Path,
    mutation: str,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    contract = builtin_effect_contract("cancel_review_workflow")
    outcome_fence_fields = resolved_outcome_fence_fields(contract)
    payload = {
        field_name: f"durable:{field_name}"
        for field_name in outcome_fence_fields
    }
    _seed_effect(
        database,
        key,
        kind=contract.effect_kind,
        contract=contract,
        payload=payload,
    )
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None
    failure = _failure(
        claim,
        outcome_fence_fields=outcome_fence_fields,
    )
    forged_payload = dict(failure.payload)
    if mutation == "changed":
        forged_payload["completion_event_id"] = "forged-completion"
    else:
        del forged_payload["completion_event_id"]
    forged = replace(failure, payload=forged_payload)

    with pytest.raises(EffectStoreConflict, match="fence completion_event_id"):
        await store.fail_with_event(
            claim,
            forged,
            error="the controlled handler failed",
            outcome_fence_fields=outcome_fence_fields,
        )

    with database.connect() as conn:
        effect = conn.execute(
            "SELECT status, claim_id FROM agent_effect_outbox WHERE effect_id = 'effect-1'"
        ).fetchone()
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
    assert tuple(effect) == (DurableEffectStatus.PROCESSING.value, claim.claim_id)
    assert mailbox_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ("completion", "failure"))
async def test_sqlite_effect_store_rejects_caller_weakened_v2_fence_projection(
    tmp_path: Path,
    terminal: str,
) -> None:
    """Settlement must derive v2 fences from authority, never a caller argument."""

    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    contract = builtin_effect_contract("cancel_review_workflow")
    fence_fields = resolved_outcome_fence_fields(contract)
    _seed_effect(
        database,
        key,
        kind=contract.effect_kind,
        contract=contract,
        payload={field_name: f"durable:{field_name}" for field_name in fence_fields},
    )
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None
    if terminal == "completion":
        envelope = replace(
            _completion(claim),
            kind=contract.completion_event_kind,
            source=contract.completion_source,
            payload={
                **claim.effect.outcome_fence_payload(fence_fields),
                "effect_id": claim.effect.effect_id,
                "contract_version": claim.effect.contract_version,
                "contract_signature": claim.effect.contract_signature,
            },
        )
        settle = store.complete_with_event
        kwargs: dict[str, object] = {}
    else:
        envelope = _failure(claim, outcome_fence_fields=fence_fields)
        settle = store.fail_with_event
        kwargs = {"error": "the controlled handler failed"}

    with pytest.raises(EffectStoreConflict, match="outcome_fence_fields differ"):
        await settle(
            claim,
            envelope,
            outcome_fence_fields=(),
            **kwargs,
        )

    with database.connect() as conn:
        row = conn.execute(
            "SELECT status, claim_id FROM agent_effect_outbox WHERE effect_id = ?",
            (claim.effect.effect_id,),
        ).fetchone()
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
    assert tuple(row) == (DurableEffectStatus.PROCESSING.value, claim.claim_id)
    assert mailbox_count == 0


@pytest.mark.asyncio
async def test_sqlite_effect_store_requires_authority_for_custom_v2_contracts(
    tmp_path: Path,
) -> None:
    """Custom v2 effects require an explicit sealed test/runtime authority."""

    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    contract = _external_contract(
        kind="test_authority_v2",
        version=2,
        outcome_fence_fields=("plan_id",),
    )
    _seed_effect(
        database,
        key,
        kind=contract.effect_kind,
        contract=contract,
        payload={"plan_id": "plan-a"},
    )
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None
    failure = _failure(
        claim,
        outcome_fence_fields=resolved_outcome_fence_fields(contract),
    )

    with pytest.raises(EffectStoreConflict, match="not authorized"):
        await store.fail_with_event(
            claim,
            failure,
            error="the controlled handler failed",
        )

    authorized_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: now[0],
        contract_authority=EffectContractAuthority(
            (
                *builtin_effect_contract_authority().contracts(),
                _external_contract(),
                contract,
            )
        ),
    )
    committed = await authorized_store.fail_with_event(
        claim,
        failure,
        error="the controlled handler failed",
    )

    assert committed.status is EffectSettlementStatus.COMMITTED


@pytest.mark.asyncio
async def test_sqlite_effect_store_rejects_persisted_v2_signature_outside_authority(
    tmp_path: Path,
) -> None:
    """An outbox row cannot choose a different v2 policy by changing its hash."""

    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    contract = builtin_effect_contract("cancel_review_workflow")
    fence_fields = resolved_outcome_fence_fields(contract)
    _seed_effect(
        database,
        key,
        kind=contract.effect_kind,
        contract=contract,
        payload={field_name: f"durable:{field_name}" for field_name in fence_fields},
    )
    with database.connect() as conn:
        conn.execute(
            "UPDATE agent_effect_outbox SET contract_signature = 'forged-policy'"
        )
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None

    with pytest.raises(EffectStoreConflict, match="signature does not match"):
        await store.fail_with_event(
            claim,
            _failure(claim, outcome_fence_fields=fence_fields),
            error="the controlled handler failed",
        )

    with database.connect() as conn:
        row = conn.execute(
            "SELECT status, claim_id FROM agent_effect_outbox WHERE effect_id = ?",
            (claim.effect.effect_id,),
        ).fetchone()
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
    assert tuple(row) == (DurableEffectStatus.PROCESSING.value, claim.claim_id)
    assert mailbox_count == 0


@pytest.mark.asyncio
async def test_sqlite_effect_store_renews_and_recovers_expired_claims(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key)
    first = await store.claim_next(worker_id="worker-a")
    assert first is not None

    now[0] = 104.0
    renewed = await store.renew_lease(first)
    assert renewed.claim_id == first.claim_id
    assert renewed.lease_expires_at == 109.0
    now[0] = 106.0
    assert await store.recover_expired(worker_id="recovery") == 0
    now[0] = 110.0
    assert await store.recover_expired(worker_id="recovery") == 1
    second = await store.claim_next(worker_id="worker-b")
    assert second is not None
    assert second.attempt_count == 2

    await store.release_for_retry(
        second,
        error="provider unavailable",
        available_at=115.0,
    )
    assert await store.next_available_at() == 115.0
    assert await store.claim_next(worker_id="worker-c") is None


@pytest.mark.asyncio
async def test_executor_atomically_skips_only_explicit_operation_precondition(
    tmp_path: Path,
) -> None:
    now = [100.0]
    operation_id = "idle-planning-1"
    intended_event_id = "idle-planning-deadline:1"
    deadline_contract = _external_contract(
        kind="deadline",
        completion_event_kind="IdleReviewPlanningDeadlineReached",
        priority=0,
        outcome_fence_fields=("completion_event_id", "input_watermark"),
    )
    stop_contract = _external_contract(kind="stop", priority=1)
    database, store, key = await _make_store(
        tmp_path,
        now,
        contracts=(deadline_contract, stop_contract),
    )
    _seed_operation(
        database,
        key,
        operation_id=operation_id,
        status="completed",
        now=now[0],
    )
    _seed_effect(
        database,
        key,
        effect_id="deadline-effect",
        kind=deadline_contract.effect_kind,
        contract=deadline_contract,
        operation_id=operation_id,
        payload={
            "completion_event_id": intended_event_id,
            "deadline_event_id": intended_event_id,
            "enqueue_only_if_operation_status": ["pending", "running"],
            "input_watermark": 17,
            "terminal_operation_disposition": "skip",
        },
    )
    _seed_effect(
        database,
        key,
        effect_id="stop-effect",
        kind=stop_contract.effect_kind,
        contract=stop_contract,
        operation_id=operation_id,
    )
    calls: list[str] = []

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        calls.append(context.effect.effect_id)
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register("deadline", handler, contract=deadline_contract)
    handlers.register("stop", handler, contract=stop_contract)
    registry = _WakeRegistry()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    skipped = await executor.run_once()
    completed = await executor.run_once()

    assert skipped.status == EffectRunStatus.SKIPPED
    assert completed.status == EffectRunStatus.COMPLETED
    assert calls == ["deadline-effect", "stop-effect"]
    assert registry.keys == [key, key]
    with database.connect() as conn:
        events = conn.execute(
            """
            SELECT event_id, kind, payload_json
            FROM agent_session_mailbox
            ORDER BY mailbox_id
            """
        ).fetchall()
    assert [row["kind"] for row in events] == ["EffectSkipped", "EffectCompleted"]
    skipped_payload = json.loads(events[0]["payload_json"])
    assert events[0]["event_id"] == skipped_event_id(
        _stored_deadline_effect(key, operation_id)
    )
    assert skipped_payload["intended_event_id"] == intended_event_id
    assert skipped_payload["intended_event_kind"] == "IdleReviewPlanningDeadlineReached"
    assert skipped_payload["actual_operation_status"] == "completed"
    assert skipped_payload["completion_event_id"] == intended_event_id
    assert skipped_payload["input_watermark"] == 17
    assert events[1]["kind"] == "EffectCompleted"


@pytest.mark.asyncio
async def test_sqlite_effect_completion_wakes_and_drains_real_session_actor(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, effect_store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key)
    actor_store = SQLiteSessionActorStore(
        database,
        retry_delay_seconds=0.0,
        clock=lambda: now[0],
    )

    def reduce_completion(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(
                data={"handled_effect_id": event.payload["effect_id"]},
                updated_at=now[0],
            ),
            disposition="effect_completion_handled",
        )

    actor_registry = AgentSessionActorRegistry(
        store=actor_store,
        handler=reduce_completion,
        retry_delay_seconds=0.0,
    )
    wake_registry = _TransientWakeRegistry(actor_registry)
    contract = _external_contract()
    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handler_calls = 0

    async def run_effect(_context: EffectExecutionContext) -> EffectHandlerResult:
        nonlocal handler_calls
        handler_calls += 1
        return EffectHandlerResult()

    handlers.register("external_write", run_effect, contract=contract)
    executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        session_registry=wake_registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )
    try:
        result = await executor.run_once()
        await actor_registry.wait_idle(key)
    finally:
        await actor_registry.shutdown()

    assert result.status == EffectRunStatus.COMPLETED
    assert handler_calls == 1
    assert wake_registry.wake_attempts == 1
    assert wake_registry.recover_attempts == 1
    aggregate = await actor_store.load(key)
    assert aggregate.data == {"handled_effect_id": "effect-1"}
    assert aggregate.event_sequence == 1
    with database.connect() as conn:
        mailbox_status = conn.execute(
            """
            SELECT status FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, result.event_id),
        ).fetchone()["status"]
    assert mailbox_status == "completed"


@pytest.mark.asyncio
async def test_control_claim_cannot_take_pending_planner_effect(tmp_path: Path) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    planner = _external_contract(kind="planner", lane=EffectLane.PLANNER)
    control = _external_contract(kind="control", lane=EffectLane.CONTROL)
    _seed_effect(
        database,
        key,
        effect_id="planner-1",
        kind=planner.effect_kind,
        contract=planner,
    )

    claim = await store.claim_next(
        worker_id="control-worker",
        effect_contracts=(control.ref,),
    )

    assert claim is None
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, attempt_count, claim_id
            FROM agent_effect_outbox WHERE effect_id = 'planner-1'
            """
        ).fetchone()
    assert tuple(row) == ("pending", 0, "")


@pytest.mark.asyncio
async def test_deadline_contract_priority_beats_older_control_backlog(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    deadline = _external_contract(
        kind="deadline",
        lane=EffectLane.CONTROL,
        priority=0,
    )
    stop = _external_contract(
        kind="stop",
        lane=EffectLane.CONTROL,
        priority=2,
    )
    for index in range(8):
        _seed_effect(
            database,
            key,
            effect_id=f"stop-{index}",
            kind=stop.effect_kind,
            contract=stop,
        )
    _seed_effect(
        database,
        key,
        effect_id="deadline-1",
        kind=deadline.effect_kind,
        contract=deadline,
    )

    claim = await store.claim_next(
        worker_id="control-worker",
        effect_contracts=(deadline.ref, stop.ref),
    )

    assert claim is not None
    assert claim.effect.effect_id == "deadline-1"


@pytest.mark.asyncio
async def test_contract_filter_distinguishes_versions_of_the_same_kind(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    version_one = _external_contract(version=1, lane=EffectLane.PLANNER)
    version_two = _external_contract(version=2, lane=EffectLane.CONTROL)
    _seed_effect(
        database,
        key,
        effect_id="v1-effect",
        contract=version_one,
    )
    _seed_effect(
        database,
        key,
        effect_id="v2-effect",
        contract=version_two,
    )

    control_claim = await store.claim_next(
        worker_id="control-worker",
        effect_contracts=(version_two.ref,),
    )

    assert control_claim is not None
    assert control_claim.effect.effect_id == "v2-effect"
    with database.connect() as conn:
        version_one_row = conn.execute(
            "SELECT status, attempt_count FROM agent_effect_outbox WHERE effect_id = ?",
            ("v1-effect",),
        ).fetchone()
    assert tuple(version_one_row) == ("pending", 0)


@pytest.mark.asyncio
async def test_settlement_rejects_missing_or_changed_contract_signature(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key)
    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None
    forged = SessionEventEnvelope(
        event_id=completion_event_id(claim.effect),
        key=claim.key,
        kind="EffectCompleted",
        payload={
            "effect_id": claim.effect.effect_id,
            "contract_version": claim.effect.contract_version,
            "contract_signature": "different-contract",
        },
    )

    with pytest.raises(EffectStoreConflict, match="contract_signature"):
        await store.complete_with_event(claim, forged)

    with database.connect() as conn:
        row = conn.execute(
            "SELECT status, claim_id FROM agent_effect_outbox WHERE effect_id = ?",
            (claim.effect.effect_id,),
        ).fetchone()
    assert tuple(row) == ("processing", claim.claim_id)


def _stored_deadline_effect(
    key: SessionKey,
    operation_id: str,
) -> DurableEffectEnvelope:
    """Build the persisted deadline identity used by deterministic id checks."""

    return DurableEffectEnvelope(
        effect_id="deadline-effect",
        key=key,
        kind="external_write",
        idempotency_key="idempotency:deadline-effect",
        ownership_generation=1,
        contract_version=_external_contract().version,
        contract_signature=_external_contract().signature,
        operation_id=operation_id,
        source_event_id="source:deadline-effect",
        payload={
            "deadline_event_id": "idle-planning-deadline:1",
            "enqueue_only_if_operation_status": ["pending", "running"],
            "terminal_operation_disposition": "skip",
        },
    )


def test_effect_outbox_migration_rebuilds_constraints_and_preserves_rows(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="migration fixture owner",
    ).ownership
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, owner.generation),
        )
        conn.execute("ALTER TABLE agent_effect_outbox RENAME TO effect_outbox_current")
        conn.execute(
            """
            CREATE TABLE agent_effect_outbox (
                effect_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                effect_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                operation_id TEXT NOT NULL DEFAULT '',
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL,
                claim_id TEXT NOT NULL DEFAULT '',
                lease_owner TEXT NOT NULL DEFAULT '',
                lease_until REAL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                completed_at REAL,
                last_error TEXT NOT NULL DEFAULT '',
                UNIQUE(profile_id, session_id, effect_id),
                UNIQUE(profile_id, session_id, idempotency_key)
            )
            """
        )
        conn.execute("DROP TABLE effect_outbox_current")
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id, event_id,
                kind, status, available_at, created_at, updated_at
            ) VALUES ('legacy-effect', 'legacy-key', ?, ?, 'source',
                      'legacy_kind', 'pending', 100.0, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id),
        )

    database.initialize()

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT effect_id, contract_version, contract_signature,
                   ownership_generation, status
            FROM agent_effect_outbox
            """
        ).fetchone()
        create_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = 'agent_effect_outbox'
                """
            ).fetchone()["sql"]
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO agent_effect_outbox (
                    effect_id, idempotency_key, profile_id, session_id,
                    ownership_generation, event_id, kind, contract_version,
                    contract_signature, status, available_at, created_at, updated_at
                ) VALUES ('bad-status', 'bad-status', ?, ?, 1, 'source',
                          'bad', 1, 'signature', 'invalid', 100.0, 100.0, 100.0)
                """,
                (key.profile_id, key.session_id),
            )
    assert tuple(row) == (
        "legacy-effect",
        1,
        "legacy-unsigned-v1",
        owner.generation,
        "pending",
    )
    assert "CHECK(status IN ('pending', 'processing', 'completed', 'failed'))" in (
        create_sql
    )


def test_fresh_operation_input_fences_are_paired_and_nonnegative(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="operation input fence fixture",
    ).ownership
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation,
                created_at, updated_at
            ) VALUES (?, ?, ?, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, owner.generation),
        )
        operation_columns = {
            str(row["name"]): row
            for row in conn.execute(
                "PRAGMA table_info(agent_session_operations)"
            )
        }
        consumption_columns = {
            str(row["name"]): row
            for row in conn.execute(
                "PRAGMA table_info(agent_message_ledger_consumptions)"
            )
        }
        assert operation_columns["input_ledger_sequence"]["notnull"] == 0
        assert consumption_columns["input_ledger_sequence"]["notnull"] == 1
        assert consumption_columns["input_ledger_sequence"]["dflt_value"] == "0"

        with pytest.raises(
            sqlite3.IntegrityError,
            match="operation input watermark and ledger sequence must be paired",
        ):
            conn.execute(
                """
                INSERT INTO agent_session_operations (
                    operation_id, profile_id, session_id,
                    ownership_generation, kind, input_watermark, started_at
                ) VALUES ('missing-sequence', ?, ?, ?, 'review', 10, 100.0)
                """,
                (key.profile_id, key.session_id, owner.generation),
            )
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, input_watermark, input_ledger_sequence, started_at
            ) VALUES ('valid-input', ?, ?, ?, 'review', 10, 1, 100.0)
            """,
            (key.profile_id, key.session_id, owner.generation),
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="operation input watermark and ledger sequence must be paired",
        ):
            conn.execute(
                """
                UPDATE agent_session_operations
                SET input_ledger_sequence = NULL
                WHERE operation_id = 'valid-input'
                """
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="operation input watermark and ledger sequence must be paired",
        ):
            conn.execute(
                """
                UPDATE agent_session_operations
                SET input_watermark = -1, input_ledger_sequence = 0
                WHERE operation_id = 'valid-input'
                """
            )


def test_operation_input_sequence_upgrade_recovers_ledger_boundary(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="operation input migration fixture",
    ).ownership
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation,
                created_at, updated_at
            ) VALUES (?, ?, ?, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, owner.generation),
        )
        message_ids = tuple(
            int(
                conn.execute(
                    """
                    INSERT INTO message_logs (session_id, role, created_at)
                    VALUES (?, 'user', ?)
                    """,
                    (key.session_id, float(sequence)),
                ).lastrowid
            )
            for sequence in range(1, 4)
        )
        ledger_rows = (
            (1, message_ids[0], 1.0),
            (2, message_ids[2], 2.0),
            (3, message_ids[1], 200.0),
        )
        for sequence, message_log_id, recorded_at in ledger_rows:
            conn.execute(
                """
                INSERT INTO agent_message_ledger (
                    profile_id, session_id, ledger_sequence, message_log_id,
                    ownership_generation, source_event_id, actor_event_id,
                    delivery_version, event_source, instance_id, event_type,
                    is_private, is_mentioned, is_mention_to_other,
                    is_reply_to_bot, is_poke_to_bot, is_poke_to_other,
                    already_handled, is_stopped, is_self_message,
                    eligible_for_work, priority_mention, priority_reply_to_bot,
                    priority_repeated_mention, priority_poke_to_bot,
                    priority_should_wake, priority_reasons_json,
                    observed_at, occurred_at, event_created_at, canonical_json,
                    recorded_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'test', 'adapter-a',
                          'message-created', 0, 0, 0, 0, 0, 0, 0, 0, 0,
                          1, 0, 0, 0, 0, 0, '[]', ?, ?, ?, '{}', ?, ?)
                """,
                (
                    key.profile_id,
                    key.session_id,
                    sequence,
                    message_log_id,
                    owner.generation,
                    f"source-{sequence}",
                    f"actor-{sequence}",
                    recorded_at,
                    recorded_at,
                    recorded_at,
                    recorded_at,
                    recorded_at,
                ),
            )

    with database.connect() as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("PRAGMA legacy_alter_table = ON")
        conn.execute("DROP TRIGGER trg_agent_operation_input_fence_insert")
        conn.execute("DROP TRIGGER trg_agent_operation_input_fence_update")
        conn.execute(
            """
            ALTER TABLE agent_message_ledger_consumptions
            RENAME TO consumptions_current
            """
        )
        conn.execute(
            """
            CREATE TABLE agent_message_ledger_consumptions (
                consumption_id TEXT PRIMARY KEY,
                profile_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                ownership_generation INTEGER NOT NULL,
                kind TEXT NOT NULL,
                selection TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                input_watermark INTEGER NOT NULL,
                explicit_message_log_ids_json TEXT NOT NULL DEFAULT '[]',
                canonical_json TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                trace_id TEXT NOT NULL DEFAULT '',
                occurred_at REAL NOT NULL,
                committed_at REAL NOT NULL,
                UNIQUE(profile_id, session_id, kind, idempotency_key)
            )
            """
        )
        conn.execute("DROP TABLE consumptions_current")
        conn.execute(
            "ALTER TABLE agent_session_operations RENAME TO operations_current"
        )
        conn.execute(
            """
            CREATE TABLE agent_session_operations (
                operation_id TEXT PRIMARY KEY,
                profile_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                ownership_generation INTEGER NOT NULL DEFAULT 0,
                kind TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                launched_by_event_id TEXT NOT NULL DEFAULT '',
                state_revision INTEGER NOT NULL DEFAULT 0,
                active_epoch INTEGER NOT NULL DEFAULT 0,
                activity_generation INTEGER NOT NULL DEFAULT 0,
                input_watermark INTEGER,
                started_at REAL NOT NULL,
                lease_owner TEXT NOT NULL DEFAULT '',
                lease_until REAL,
                superseded_at REAL,
                finished_at REAL,
                failure_code TEXT NOT NULL DEFAULT '',
                failure_message TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute("DROP TABLE operations_current")
        conn.executemany(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, input_watermark, started_at
            ) VALUES (?, ?, ?, ?, 'review', 'completed', ?, 100.0)
            """,
            (
                (
                    "recoverable-boundary",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    message_ids[2],
                ),
                (
                    "legacy-unknown-boundary",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    0,
                ),
                (
                    "late-old-boundary",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    message_ids[1],
                ),
                (
                    "no-input-boundary",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    None,
                ),
            ),
        )

    database.initialize()

    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT operation_id, input_watermark, input_ledger_sequence
            FROM agent_session_operations
            ORDER BY operation_id
            """
        ).fetchall()
        trigger_names = {
            str(row["name"])
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger' AND tbl_name = 'agent_session_operations'
                """
            )
        }
        consumption_columns = {
            str(row["name"]): row
            for row in conn.execute(
                "PRAGMA table_info(agent_message_ledger_consumptions)"
            )
        }

    assert [tuple(row) for row in rows] == [
        ("late-old-boundary", message_ids[1], 1),
        ("legacy-unknown-boundary", 0, 0),
        ("no-input-boundary", None, None),
        ("recoverable-boundary", message_ids[2], 2),
    ]
    assert trigger_names == {
        "trg_agent_operation_input_fence_insert",
        "trg_agent_operation_input_fence_update",
    }
    assert consumption_columns["input_ledger_sequence"]["notnull"] == 1
    assert consumption_columns["input_ledger_sequence"]["dflt_value"] == "0"
