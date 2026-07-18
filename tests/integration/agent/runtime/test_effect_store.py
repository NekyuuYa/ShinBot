from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

import shinbot.agent.runtime.session_actor.effect_store as effect_store_module
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
    EffectQuarantineReason,
    EffectRunStatus,
    EffectSettlementResult,
    EffectSettlementStatus,
    FencedEffectExecutionLeaseLost,
    completion_event_id,
    derived_effect_event_id,
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
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
    ExternalActionRequest,
    builtin_external_action_effect_contracts,
    materialize_external_action_effect,
)
from shinbot.agent.runtime.session_actor.model_execution_witness import (
    ModelExecutionClaim,
    SQLiteModelExecutionWitnessStore,
)
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import FencedActorExecutionBinding
from shinbot.core.dispatch.mailbox_handoff import (
    MailboxHandoffEvidenceState,
    MailboxHandoffState,
    MailboxHandoffTarget,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.canonical_json import (
    MAX_CANONICAL_JSON_BYTES,
    MAX_CANONICAL_JSON_NODES,
)
from tests.agent_runtime_helpers import wait_for_session_actor_idle


class _WakeRegistry:
    def __init__(self) -> None:
        self.keys: list[SessionKey] = []

    async def wake(self, key: SessionKey) -> None:
        self.keys.append(key)

    async def recover(self) -> int:
        return 0


def _evidence_prefix_text(evidence: dict[str, object]) -> str:
    return base64.b64decode(str(evidence["prefix_base64"])).decode("utf-8")


def _evidence_prefix_bytes(evidence: dict[str, object]) -> bytes:
    return base64.b64decode(str(evidence["prefix_base64"]))


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


async def _make_fenced_store(
    tmp_path: Path,
    now: list[float],
    *,
    contracts: tuple[EffectExecutionContract, ...] = (),
) -> tuple[
    DatabaseManager,
    SQLiteDurableEffectStore,
    SessionKey,
    ActorV2AdmissionGrant,
    int,
]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "bot:group:fenced-room")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="effect-store-fenced-test",
        ttl_seconds=3600.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="fenced effect store test",
        admission_grant=grant,
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
        grant,
        ownership.generation,
    )


def _execution_binding(
    database: DatabaseManager,
    *,
    key: SessionKey,
    ownership_generation: int,
    admission_grant: ActorV2AdmissionGrant,
    target_incarnation_id: str = "effect-executor-incarnation-a",
) -> FencedActorExecutionBinding:
    """Acquire one exact target lease for a fenced effect-execution test."""

    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership_generation,
        admission_fence_id=admission_grant.fence.fence_id,
        admission_fence_generation=admission_grant.fence.generation,
    )
    grant = database.actor_v2_fenced_wake_target_leases.acquire(
        request,
        target=MailboxHandoffTarget(
            "effect-executor-test-target",
            target_incarnation_id,
        ),
        ttl_seconds=60.0,
    )
    return FencedActorExecutionBinding(request=request, target_lease=grant)


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


def _replace_with_weak_effect_outbox(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE agent_effect_outbox RENAME TO effect_outbox_current")
    conn.execute(
        """
        CREATE TABLE agent_effect_outbox (
            effect_seq INTEGER PRIMARY KEY AUTOINCREMENT,
            effect_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            ownership_generation INTEGER NOT NULL DEFAULT 0,
            event_id TEXT NOT NULL,
            operation_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL,
            contract_version INTEGER NOT NULL,
            contract_signature TEXT NOT NULL,
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
            last_error TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute("DROP TABLE effect_outbox_current")


def _seed_blocked_external_action_effects(
    database: DatabaseManager,
    key: SessionKey,
    *,
    count: int,
    now: float,
) -> None:
    rows: list[tuple[object, ...]] = []
    for index in range(count):
        request = ExternalActionRequest(
            key=key,
            ownership_generation=1,
            operation_id="blocked-operation",
            source_event_id="blocked-operation-completed",
            instance_id="adapter-a",
            target_session_id="adapter-a:group:room",
            intent=ExternalActionIntent(
                kind=ExternalActionKind.SEND_POKE,
                tool_call_id=f"blocked-tool-{index}",
                action_ordinal=index + 1,
                payload={"ordinal": index + 1},
            ),
        )
        effect = materialize_external_action_effect(
            key=request.key,
            ownership_generation=request.ownership_generation,
            operation_id=request.operation_id,
            source_event_id=request.source_event_id,
            instance_id=request.instance_id,
            target_session_id=request.target_session_id,
            intent=request.intent,
        )
        rows.append(
            (
                effect.effect_id,
                effect.idempotency_key,
                key.profile_id,
                key.session_id,
                request.source_event_id,
                request.ownership_generation,
                effect.operation_id,
                effect.kind,
                effect.contract_version,
                effect.contract_signature,
                json.dumps(
                    effect.payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                now,
                now,
                now,
            )
        )
    with database.connect() as conn:
        conn.executemany(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id, event_id,
                ownership_generation, operation_id, kind, contract_version,
                contract_signature, payload_json, status, attempt_count,
                available_at, claim_id, lease_owner, lease_until, created_at,
                updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '',
                      NULL, ?, ?, NULL, '')
            """,
            rows,
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


def _completion(
    claim,
    *,
    event_id: str | None = None,
    contract: EffectExecutionContract | None = None,
) -> SessionEventEnvelope:
    resolved_contract = contract or _external_contract()
    fence_fields = resolved_outcome_fence_fields(resolved_contract)
    return SessionEventEnvelope(
        event_id=event_id or completion_event_id(claim.effect),
        key=claim.key,
        kind=resolved_contract.completion_event_kind,
        ownership_generation=claim.effect.ownership_generation,
        payload={
            **claim.effect.outcome_fence_payload(fence_fields),
            "effect_id": claim.effect.effect_id,
            "effect_kind": claim.effect.kind,
            "idempotency_key": claim.effect.idempotency_key,
            "operation_id": claim.effect.operation_id,
            "attempt_count": claim.attempt_count,
            "contract_version": claim.effect.contract_version,
            "contract_signature": claim.effect.contract_signature,
        },
        source=resolved_contract.completion_source,
        causation_id=claim.effect.source_event_id,
        correlation_id=claim.effect.operation_id or claim.effect.effect_id,
        trace_id=claim.effect.trace_id,
    )


def _seed_model_execution_effect(
    database: DatabaseManager,
    key: SessionKey,
    *,
    effect_id: str,
    operation_id: str,
    now: float,
) -> None:
    """Seed one valid active-reply effect for expiry notice tests."""

    contract = builtin_effect_contract("run_active_reply_workflow", version=2)
    _seed_effect(
        database,
        key,
        effect_id=effect_id,
        kind=contract.effect_kind,
        operation_id=operation_id,
        contract=contract,
        payload={
            "plan_id": "plan-a",
            "active_epoch": 1,
            "activity_generation": 1,
            "input_watermark": 1,
            "input_ledger_sequence": 1,
            "completion_event_id": f"completion:{effect_id}",
            "failure_event_id": f"failure:{effect_id}",
        },
        now=now,
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


def test_sqlite_actor_stores_expose_exact_composition_identities(tmp_path: Path) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    authority = builtin_effect_contract_authority()

    effect_store = SQLiteDurableEffectStore(database, contract_authority=authority)
    actor_store = SQLiteSessionActorStore(
        database,
        effect_contract_authority=authority,
    )

    assert effect_store.effect_contract_authority is authority
    assert actor_store.effect_contract_authority is authority
    assert effect_store.persistence_domain is database
    assert actor_store.persistence_domain is database


def test_scrub_query_plan_uses_primary_key_keyset_scan(tmp_path: Path) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    with database.connect() as conn:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN " + effect_store_module._EFFECT_SCRUB_PAGE_SQL,
            (0, 64),
        ).fetchall()

    details = tuple(str(row["detail"]) for row in plan)
    assert any(
        "SEARCH effect USING INTEGER PRIMARY KEY (rowid>?)" in detail
        for detail in details
    ), details
    assert all("MULTI-INDEX OR" not in detail for detail in details), details
    assert all("USE TEMP B-TREE" not in detail for detail in details), details


@pytest.mark.asyncio
async def test_scrub_cursor_pages_over_terminal_and_future_rows(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    rows = tuple(
        (
            f"inactive-{index}",
            f"inactive-key-{index}",
            key.profile_id,
            key.session_id,
            f"inactive-source-{index}",
            "completed" if index % 2 == 0 else "pending",
            100.0 if index % 2 == 0 else 10_000.0,
            100.0,
            100.0,
        )
        for index in range(512)
    )
    with database.connect() as conn:
        conn.executemany(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, 1, ?, '', 'external_write', 1,
                      'test-signature', '{}', ?, 0, ?, '', '', NULL, ?, ?,
                      NULL, '')
            """,
            rows,
        )

    for expected_cursor in (64, 128, 192):
        assert (
            await store.claim_next(
                worker_id=f"inactive-scrubber-{expected_cursor}",
                effect_contracts=(),
            )
            is None
        )
        with database.connect() as conn:
            cursor = conn.execute(
                """
                SELECT last_effect_seq FROM agent_effect_scrub_state
                WHERE cursor_name = 'claimable'
                """
            ).fetchone()["last_effect_seq"]
            changed = conn.execute(
                """
                SELECT COUNT(*) FROM agent_effect_outbox
                WHERE status NOT IN ('completed', 'pending')
                """
            ).fetchone()[0]
        assert cursor == expected_cursor
        assert changed == 0


@pytest.mark.asyncio
async def test_external_action_gate_defers_page_out_oversized_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    terminal_rows = tuple(
        (
            f"terminal-{index}",
            f"terminal-key-{index}",
            key.profile_id,
            key.session_id,
            f"terminal-source-{index}",
        )
        for index in range(64)
    )
    request = ExternalActionRequest(
        key=key,
        ownership_generation=1,
        operation_id="oversized-action-operation",
        source_event_id="oversized-action-source",
        instance_id="adapter-a",
        target_session_id="adapter-a:group:room",
        intent=ExternalActionIntent(
            kind=ExternalActionKind.SEND_POKE,
            tool_call_id="oversized-action-tool",
            action_ordinal=0,
            payload={"poke": True},
        ),
    )
    effect = materialize_external_action_effect(
        key=request.key,
        ownership_generation=request.ownership_generation,
        operation_id=request.operation_id,
        source_event_id=request.source_event_id,
        instance_id=request.instance_id,
        target_session_id=request.target_session_id,
        intent=request.intent,
    )
    oversized_payload = {
        **effect.payload,
        "padding": "x" * MAX_CANONICAL_JSON_BYTES,
    }
    with database.connect() as conn:
        conn.executemany(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, 1, ?, '', 'external_write', 1,
                      'test-signature', '{}', 'completed', 0, 100.0, '', '',
                      NULL, 100.0, 100.0, 100.0, '')
            """,
            terminal_rows,
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, 'pending', 0, 100.0,
                      '', '', NULL, 100.0, 100.0, NULL, '')
            """,
            (
                effect.effect_id,
                effect.idempotency_key,
                key.profile_id,
                key.session_id,
                request.source_event_id,
                request.operation_id,
                effect.kind,
                effect.contract_version,
                effect.contract_signature,
                json.dumps(
                    oversized_payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )

    streamed_effect_seqs: list[int] = []
    original_chunk_reader = effect_store_module._read_effect_raw_chunk

    def counted_chunk_reader(*args: object, **kwargs: object) -> object:
        streamed_effect_seqs.append(int(kwargs["effect_seq"]))
        return original_chunk_reader(*args, **kwargs)

    monkeypatch.setattr(
        effect_store_module,
        "_read_effect_raw_chunk",
        counted_chunk_reader,
    )
    external_contracts = tuple(
        contract.ref for contract in builtin_external_action_effect_contracts()
    )

    assert (
        await store.claim_next(
            worker_id="page-before-oversized",
            effect_contracts=external_contracts,
        )
        is None
    )
    with database.connect() as conn:
        status = conn.execute(
            """
            SELECT status FROM agent_effect_outbox WHERE effect_seq = 65
            """
        ).fetchone()["status"]
        cursor = conn.execute(
            """
            SELECT last_effect_seq FROM agent_effect_scrub_state
            WHERE cursor_name = 'claimable'
            """
        ).fetchone()["last_effect_seq"]
    assert status == "pending"
    assert cursor == 64
    assert streamed_effect_seqs == []

    assert (
        await store.claim_next(
            worker_id="oversized-scrubber",
            effect_contracts=external_contracts,
        )
        is None
    )
    with database.connect() as conn:
        status = conn.execute(
            "SELECT status FROM agent_effect_outbox WHERE effect_seq = 65"
        ).fetchone()["status"]
    assert status == "failed"
    assert set(streamed_effect_seqs) == {65}


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
async def test_sqlite_effect_settlement_returns_exact_fenced_wake_request(
    tmp_path: Path,
) -> None:
    """Committed and replayed outcomes retain the same full owner fence."""

    now = [100.0]
    database, store, key, grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    _seed_effect(database, key)
    claim = await store.claim_next(worker_id="fenced-settlement-worker")
    assert claim is not None

    expected = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership_generation,
        admission_fence_id=grant.fence.fence_id,
        admission_fence_generation=grant.fence.generation,
    )
    envelope = _completion(claim)
    committed = await store.complete_with_event(claim, envelope)
    duplicate = await store.complete_with_event(claim, envelope)
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT mailbox_id FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, envelope.event_id),
        ).fetchone()

    assert committed.status is EffectSettlementStatus.COMMITTED
    assert committed.wake_request == expected
    assert duplicate.status is EffectSettlementStatus.ALREADY_COMMITTED
    assert duplicate.wake_request == expected
    assert mailbox is not None
    assert committed.mailbox_id == int(mailbox["mailbox_id"])
    assert duplicate.mailbox_id == committed.mailbox_id


@pytest.mark.asyncio
async def test_scoped_effect_store_only_claims_its_exact_target_binding(
    tmp_path: Path,
) -> None:
    """A target lease cannot scan another active Actor v2 session's outbox."""

    now = [100.0]
    database, store, key, admission_grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=ownership_generation,
        admission_grant=admission_grant,
    )
    other_key = SessionKey("profile-b", "bot:group:other-room")
    other_ownership = database.agent_runtime_ownership.claim(
        other_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="other scoped effect session",
    ).ownership
    await SQLiteSessionActorStore(database, clock=lambda: now[0]).ensure(
        other_key,
        ownership_generation=other_ownership.generation,
    )
    _seed_effect(database, other_key, effect_id="other-session-effect", now=now[0])
    _seed_effect(database, key, effect_id="bound-session-effect", now=now[0])

    claim = await store.claim_next(
        worker_id="scoped-effect-worker",
        execution_binding=binding,
    )

    assert claim is not None
    assert claim.effect.effect_id == "bound-session-effect"
    await store.complete_with_event(
        claim,
        _completion(claim),
        execution_binding=binding,
    )
    assert (
        await store.claim_next(
            worker_id="scoped-effect-worker",
            execution_binding=binding,
        )
        is None
    )
    assert await store.next_available_at(execution_binding=binding) is None
    with database.connect() as conn:
        other_status = conn.execute(
            """
            SELECT status FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (other_key.profile_id, other_key.session_id, "other-session-effect"),
        ).fetchone()["status"]
    assert other_status == DurableEffectStatus.PENDING.value


@pytest.mark.asyncio
async def test_scoped_effect_executor_leaves_foreign_maintenance_notifications_untouched(
    tmp_path: Path,
) -> None:
    """A target-bound executor cannot consume another session's wake debt."""

    now = [100.0]
    database, store, key, admission_grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=ownership_generation,
        admission_grant=admission_grant,
    )
    _seed_effect(database, key, effect_id="bound-notification-effect", now=now[0])
    foreign_notification = EffectSettlementResult(
        status=EffectSettlementStatus.COMMITTED,
        effect_id="foreign-maintenance-effect",
        event_id="foreign-maintenance-event",
        key=SessionKey("profile-b", "bot:group:foreign-maintenance"),
    )
    store._quarantine_notifications.append(foreign_notification)

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(contract_authority=store.effect_contract_authority)
    handlers.register("external_write", handler)
    handlers.seal()
    wake_registry = _WakeRegistry()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=wake_registry,
        execution_binding=binding,
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.COMPLETED
    assert wake_registry.keys == []
    assert await store.drain_quarantine_notifications() == (foreign_notification,)


@pytest.mark.asyncio
async def test_scoped_effect_store_rejects_every_claim_lifecycle_write_after_target_loss(
    tmp_path: Path,
) -> None:
    """A released target lease cannot renew, settle, retry, or inspect its effect."""

    now = [100.0]
    database, store, key, admission_grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=ownership_generation,
        admission_grant=admission_grant,
    )
    _seed_effect(database, key, effect_id="target-loss-effect", now=now[0])
    claim = await store.claim_next(
        worker_id="target-loss-worker",
        execution_binding=binding,
    )
    assert claim is not None
    completion = _completion(claim)
    failure = _failure(claim)

    database.actor_v2_fenced_wake_target_leases.release(binding.target_lease)

    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.claim_next(
            worker_id="target-loss-worker",
            execution_binding=binding,
        )
    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.renew_lease(claim, execution_binding=binding)
    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.complete_with_event(
            claim,
            completion,
            execution_binding=binding,
        )
    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.fail_with_event(
            claim,
            failure,
            error="target lease is no longer live",
            execution_binding=binding,
        )
    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.release_for_retry(
            claim,
            error="target lease is no longer live",
            available_at=now[0] + 1.0,
            execution_binding=binding,
        )
    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.defer_without_attempt(
            claim,
            reason="target lease is no longer live",
            available_at=now[0] + 1.0,
            execution_binding=binding,
        )
    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.quarantine(
            claim,
            reason=EffectQuarantineReason.UNSUPPORTED_CONTRACT,
            message="target lease is no longer live",
            execution_binding=binding,
        )
    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.release(
            claim,
            error="target lease is no longer live",
            execution_binding=binding,
        )
    with pytest.raises(FencedEffectExecutionLeaseLost):
        await store.next_available_at(execution_binding=binding)
    with pytest.raises(ValueError, match="explicit recovery controller"):
        await store.recover_expired(
            worker_id="target-loss-worker",
            execution_binding=binding,
        )

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, claim_id, lease_owner
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, claim.effect.effect_id),
        ).fetchone()
    assert dict(row) == {
        "status": DurableEffectStatus.PROCESSING.value,
        "claim_id": claim.claim_id,
        "lease_owner": claim.worker_id,
    }


@pytest.mark.asyncio
async def test_scoped_effect_executor_does_not_recover_other_session_expired_work(
    tmp_path: Path,
) -> None:
    """Fenced startup is not a broad historical effect recovery mechanism."""

    now = [100.0]
    database, store, key, admission_grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=ownership_generation,
        admission_grant=admission_grant,
    )
    other_key = SessionKey("profile-b", "bot:group:expired-other-room")
    other_ownership = database.agent_runtime_ownership.claim(
        other_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="other expired effect session",
    ).ownership
    await SQLiteSessionActorStore(database, clock=lambda: now[0]).ensure(
        other_key,
        ownership_generation=other_ownership.generation,
    )
    _seed_effect(database, other_key, effect_id="expired-other-effect", now=now[0])
    other_claim = await store.claim_next(worker_id="other-expired-worker")
    assert other_claim is not None
    now[0] = 106.0

    handlers = EffectHandlerRegistry(contract_authority=store.effect_contract_authority)
    handlers.seal()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=_WakeRegistry(),
        execution_binding=binding,
        poll_interval_seconds=0.01,
    )
    try:
        assert await executor.start_fenced() == 0
    finally:
        await executor.shutdown(drain=False)

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, claim_id, lease_owner
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (other_key.profile_id, other_key.session_id, other_claim.effect.effect_id),
        ).fetchone()
    assert dict(row) == {
        "status": DurableEffectStatus.PROCESSING.value,
        "claim_id": other_claim.claim_id,
        "lease_owner": other_claim.worker_id,
    }


@pytest.mark.asyncio
async def test_scoped_effect_executor_refuses_to_start_after_target_loss(
    tmp_path: Path,
) -> None:
    """A target that already lost its lease cannot spawn worker tasks."""

    now = [100.0]
    database, store, key, admission_grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=ownership_generation,
        admission_grant=admission_grant,
    )
    handlers = EffectHandlerRegistry(contract_authority=store.effect_contract_authority)
    handlers.seal()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=_WakeRegistry(),
        execution_binding=binding,
    )
    database.actor_v2_fenced_wake_target_leases.release(binding.target_lease)

    try:
        with pytest.raises(FencedEffectExecutionLeaseLost):
            await executor.start_fenced()
        assert executor.running is False
    finally:
        await executor.shutdown(drain=False)


def test_scoped_effect_executor_requires_automatic_lease_renewal(tmp_path: Path) -> None:
    """A fenced target cannot opt out of lost-lease detection while running."""

    now = [100.0]
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "bot:group:renewal-required")
    admission_grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="renewal-required-test",
        ttl_seconds=60.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="renewal requirement test",
        admission_grant=admission_grant,
    ).ownership
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=ownership.generation,
        admission_grant=admission_grant,
    )
    store = SQLiteDurableEffectStore(database, clock=lambda: now[0])
    handlers = EffectHandlerRegistry(contract_authority=store.effect_contract_authority)
    handlers.seal()

    with pytest.raises(ValueError, match="requires automatic lease renewal"):
        DurableEffectExecutor(
            store=store,
            handlers=handlers,
            session_registry=_WakeRegistry(),
            execution_binding=binding,
            renew_interval_seconds=None,
        )


@pytest.mark.asyncio
async def test_scoped_effect_executor_rechecks_target_before_generic_handler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A target lost after claim cannot start an unwitnessed control handler."""

    now = [100.0]
    database, store, key, admission_grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=ownership_generation,
        admission_grant=admission_grant,
    )
    _seed_effect(database, key, effect_id="generic-handler-target-loss", now=now[0])
    handler_calls = 0

    async def generic_handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        nonlocal handler_calls
        handler_calls += 1
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(contract_authority=store.effect_contract_authority)
    handlers.register("external_write", generic_handler)
    handlers.seal()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=_WakeRegistry(),
        execution_binding=binding,
    )
    original_claim_next = store.claim_next

    async def claim_then_lose_target(**kwargs: object):
        claim = await original_claim_next(**kwargs)
        if claim is not None:
            database.actor_v2_fenced_wake_target_leases.release(binding.target_lease)
        return claim

    monkeypatch.setattr(store, "claim_next", claim_then_lose_target)

    with pytest.raises(FencedEffectExecutionLeaseLost):
        await executor.run_once()
    assert handler_calls == 0


@pytest.mark.asyncio
async def test_scoped_effect_executor_cancels_handler_and_stops_after_target_loss(
    tmp_path: Path,
) -> None:
    """A lost target lease cancels local work instead of scheduling a retry."""

    now = [100.0]
    database, store, key, admission_grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=ownership_generation,
        admission_grant=admission_grant,
    )
    _seed_effect(database, key, effect_id="target-loss-handler-effect", now=now[0])
    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()

    async def blocking_handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        handler_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise

    handlers = EffectHandlerRegistry(contract_authority=store.effect_contract_authority)
    handlers.register("external_write", blocking_handler)
    handlers.seal()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=_WakeRegistry(),
        execution_binding=binding,
        poll_interval_seconds=0.01,
        renew_interval_seconds=0.01,
    )
    try:
        await executor.start_fenced()
        await asyncio.wait_for(handler_started.wait(), timeout=1.0)
        database.actor_v2_fenced_wake_target_leases.release(binding.target_lease)
        await asyncio.wait_for(handler_cancelled.wait(), timeout=1.0)
        for _ in range(100):
            if not executor.healthy:
                break
            await asyncio.sleep(0.01)
        assert executor.healthy is False
        assert isinstance(executor.binding_failure, FencedEffectExecutionLeaseLost)
    finally:
        await executor.shutdown(drain=False)


@pytest.mark.asyncio
async def test_sqlite_effect_skip_and_quarantine_return_exact_fenced_wake_request(
    tmp_path: Path,
) -> None:
    """Every terminal mailbox variant projects its final actor incarnation."""

    now = [100.0]
    deadline_contract = _external_contract(
        kind="fenced-deadline",
        completion_event_kind="FencedDeadlineReached",
        outcome_fence_fields=("completion_event_id", "input_watermark"),
    )
    database, store, key, grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
        contracts=(deadline_contract,),
    )
    expected = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership_generation,
        admission_fence_id=grant.fence.fence_id,
        admission_fence_generation=grant.fence.generation,
    )
    _seed_operation(
        database,
        key,
        operation_id="fenced-deadline-operation",
        status="completed",
        now=now[0],
    )
    _seed_effect(
        database,
        key,
        effect_id="fenced-deadline-effect",
        kind=deadline_contract.effect_kind,
        contract=deadline_contract,
        operation_id="fenced-deadline-operation",
        payload={
            "completion_event_id": "fenced-deadline:1",
            "enqueue_only_if_operation_status": ["pending", "running"],
            "input_watermark": 7,
            "terminal_operation_disposition": "skip",
        },
    )
    skipped_claim = await store.claim_next(worker_id="fenced-skip-worker")
    assert skipped_claim is not None

    skipped = await store.complete_with_event(
        skipped_claim,
        _completion(skipped_claim, contract=deadline_contract),
    )
    skipped_duplicate = await store.complete_with_event(
        skipped_claim,
        _completion(skipped_claim, contract=deadline_contract),
    )

    assert skipped.status is EffectSettlementStatus.PRECONDITION_SKIPPED
    assert skipped.wake_request == expected
    assert skipped_duplicate.status is EffectSettlementStatus.PRECONDITION_SKIPPED
    assert skipped_duplicate.wake_request == expected
    assert skipped.mailbox_id is not None
    assert skipped_duplicate.mailbox_id == skipped.mailbox_id

    _seed_effect(database, key, effect_id="fenced-quarantine-effect")
    quarantine_claim = await store.claim_next(worker_id="fenced-quarantine-worker")
    assert quarantine_claim is not None
    quarantined = await store.quarantine(
        quarantine_claim,
        reason=EffectQuarantineReason.UNSUPPORTED_CONTRACT,
        message="fenced quarantine test",
    )
    quarantine_duplicate = await store.quarantine(
        quarantine_claim,
        reason=EffectQuarantineReason.UNSUPPORTED_CONTRACT,
        message="fenced quarantine test",
    )

    assert quarantined.status is EffectSettlementStatus.COMMITTED
    assert quarantined.wake_request == expected
    assert quarantine_duplicate.status is EffectSettlementStatus.ALREADY_COMMITTED
    assert quarantine_duplicate.wake_request == expected
    assert quarantined.mailbox_id is not None
    assert quarantine_duplicate.mailbox_id == quarantined.mailbox_id


@pytest.mark.asyncio
async def test_sqlite_effect_settlement_dual_writes_fenced_mailbox_handoff(
    tmp_path: Path,
) -> None:
    """A newly settled fenced outcome records its immutable handoff evidence."""

    now = [100.0]
    database, store, key, grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    _seed_effect(database, key, effect_id="fenced-handoff-settlement")
    claim = await store.claim_next(worker_id="fenced-handoff-worker")
    assert claim is not None
    envelope = _completion(claim)

    result = await store.complete_with_event(claim, envelope)

    assert result.status is EffectSettlementStatus.COMMITTED
    assert result.mailbox_id is not None
    with database.connect() as conn:
        handoff = conn.execute(
            """
            SELECT handoff.mailbox_id, handoff.event_id, handoff.ownership_generation,
                   handoff.evidence_state, handoff.admission_fence_id,
                   handoff.admission_fence_generation, handoff.state
            FROM agent_session_mailbox_handoffs AS handoff
            JOIN agent_session_mailbox AS mailbox
              ON mailbox.mailbox_id = handoff.mailbox_id
            WHERE mailbox.profile_id = ?
              AND mailbox.session_id = ?
              AND mailbox.event_id = ?
            """,
            (key.profile_id, key.session_id, envelope.event_id),
        ).fetchone()
    assert handoff is not None
    assert result.mailbox_id == int(handoff["mailbox_id"])
    assert tuple(handoff)[1:] == (
        envelope.event_id,
        ownership_generation,
        MailboxHandoffEvidenceState.FENCED.value,
        grant.fence.fence_id,
        grant.fence.generation,
        MailboxHandoffState.PENDING.value,
    )


@pytest.mark.asyncio
async def test_sqlite_effect_quarantine_dual_writes_unfenced_legacy_handoff(
    tmp_path: Path,
) -> None:
    """A newly written unfenced diagnostic is explicitly blocked as legacy."""

    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key, effect_id="legacy-handoff-quarantine")
    claim = await store.claim_next(worker_id="legacy-handoff-worker")
    assert claim is not None

    result = await store.quarantine(
        claim,
        reason=EffectQuarantineReason.UNSUPPORTED_CONTRACT,
        message="explicit legacy handoff test",
    )

    assert result.status is EffectSettlementStatus.COMMITTED
    assert result.mailbox_id is not None
    with database.connect() as conn:
        handoff = conn.execute(
            """
            SELECT handoff.evidence_state, handoff.admission_fence_id,
                   handoff.admission_fence_generation, handoff.state
            FROM agent_session_mailbox_handoffs AS handoff
            JOIN agent_session_mailbox AS mailbox
              ON mailbox.mailbox_id = handoff.mailbox_id
            WHERE mailbox.profile_id = ?
              AND mailbox.session_id = ?
              AND mailbox.event_id = ?
            """,
            (key.profile_id, key.session_id, result.event_id),
        ).fetchone()
    assert handoff is not None
    assert tuple(handoff) == (
        MailboxHandoffEvidenceState.UNFENCED_LEGACY.value,
        "",
        0,
        MailboxHandoffState.BLOCKED.value,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("sidecar_state", ("missing", "unknown"))
async def test_sqlite_effect_replay_never_upgrades_historical_handoff_evidence(
    tmp_path: Path,
    sidecar_state: str,
) -> None:
    """A replay validates an existing mailbox but never refences it."""

    now = [100.0]
    database, store, key, _grant, _generation = await _make_fenced_store(tmp_path, now)
    _seed_effect(database, key, effect_id=f"historical-handoff-{sidecar_state}")
    claim = await store.claim_next(worker_id=f"historical-{sidecar_state}-worker")
    assert claim is not None
    envelope = _completion(claim)
    payload_json = effect_store_module._json_dumps(envelope.payload)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at,
                payload_json, causation_id, correlation_id, trace_id,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      'pending', 0, ?, '', '', NULL, ?, NULL, '')
            """,
            (
                envelope.event_id,
                key.profile_id,
                key.session_id,
                envelope.ownership_generation,
                envelope.kind,
                envelope.source,
                now[0],
                payload_json,
                envelope.causation_id,
                envelope.correlation_id,
                envelope.trace_id,
                now[0],
                now[0],
            ),
        )
        mailbox = conn.execute(
            """
            SELECT mailbox_id FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, envelope.event_id),
        ).fetchone()
    assert mailbox is not None
    mailbox_id = int(mailbox["mailbox_id"])
    if sidecar_state == "unknown":
        with database.connect() as conn:
            conn.execute("DROP TABLE agent_session_mailbox_handoffs")
        database.initialize()

    result = await store.complete_with_event(claim, envelope)

    assert result.status is EffectSettlementStatus.COMMITTED
    assert result.mailbox_id == mailbox_id
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    if sidecar_state == "missing":
        assert record is None
    else:
        assert record is not None
        assert record.evidence.state is MailboxHandoffEvidenceState.UNKNOWN
        assert record.state is MailboxHandoffState.BLOCKED


@pytest.mark.asyncio
async def test_sqlite_effect_expired_model_notice_dual_writes_fenced_handoff(
    tmp_path: Path,
) -> None:
    """Expired model execution evidence retains the original fence in its sidecar."""

    now = [100.0]
    database, store, key, grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    effect_id = "expired-model-handoff"
    _seed_model_execution_effect(
        database,
        key,
        effect_id=effect_id,
        operation_id="expired-model-handoff-operation",
        now=now[0],
    )
    claim = await store.claim_next(worker_id="expired-model-handoff-worker")
    assert claim is not None
    witness = SQLiteModelExecutionWitnessStore(database, clock=lambda: now[0])
    model_claim = ModelExecutionClaim(
        key=claim.key,
        ownership_generation=claim.effect.ownership_generation,
        effect_id=claim.effect.effect_id,
        operation_id=claim.effect.operation_id,
        effect_kind=claim.effect.kind,
        contract_version=claim.effect.contract_version,
        contract_signature=claim.effect.contract_signature,
        claim_id=claim.claim_id,
        worker_id=claim.worker_id,
    )
    await witness.begin_execution(model_claim)

    now[0] = 106.0
    assert await store.recover_expired(worker_id="expired-model-handoff-recovery") == 0
    notifications = await store.drain_quarantine_notifications()

    assert len(notifications) == 1
    notice = notifications[0]
    assert notice.status is EffectSettlementStatus.COMMITTED
    with database.connect() as conn:
        handoff = conn.execute(
            """
            SELECT handoff.event_id, handoff.ownership_generation,
                   handoff.evidence_state, handoff.admission_fence_id,
                   handoff.admission_fence_generation, handoff.state
            FROM agent_session_mailbox_handoffs AS handoff
            JOIN agent_session_mailbox AS mailbox
              ON mailbox.mailbox_id = handoff.mailbox_id
            WHERE mailbox.profile_id = ?
              AND mailbox.session_id = ?
              AND mailbox.event_id = ?
            """,
            (key.profile_id, key.session_id, notice.event_id),
        ).fetchone()
    assert handoff is not None
    assert tuple(handoff) == (
        notice.event_id,
        ownership_generation,
        MailboxHandoffEvidenceState.FENCED.value,
        grant.fence.fence_id,
        grant.fence.generation,
        MailboxHandoffState.PENDING.value,
    )


@pytest.mark.asyncio
async def test_sqlite_effect_expired_notice_handoff_trigger_rolls_back_candidate(
    tmp_path: Path,
) -> None:
    """A fence lost after sidecar staging rolls back the entire expiry notice."""

    now = [100.0]
    database, store, key, _grant, _ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    effect_id = "expired-model-handoff-trigger"
    _seed_model_execution_effect(
        database,
        key,
        effect_id=effect_id,
        operation_id="expired-model-handoff-trigger-operation",
        now=now[0],
    )
    claim = await store.claim_next(worker_id="expired-model-trigger-worker")
    assert claim is not None
    witness = SQLiteModelExecutionWitnessStore(database, clock=lambda: now[0])
    await witness.begin_execution(
        ModelExecutionClaim(
            key=claim.key,
            ownership_generation=claim.effect.ownership_generation,
            effect_id=claim.effect.effect_id,
            operation_id=claim.effect.operation_id,
            effect_kind=claim.effect.kind,
            contract_version=claim.effect.contract_version,
            contract_signature=claim.effect.contract_signature,
            claim_id=claim.claim_id,
            worker_id=claim.worker_id,
        )
    )
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER remove_expired_notice_fence_after_handoff
            AFTER INSERT ON agent_session_mailbox_handoffs
            WHEN NEW.profile_id = 'profile-a'
              AND NEW.session_id = 'bot:group:fenced-room'
            BEGIN
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = NEW.profile_id AND session_id = NEW.session_id;
            END
            """
        )

    now[0] = 106.0
    with pytest.raises(EffectClaimLost, match="ownership generation"):
        await store.recover_expired(worker_id="expired-model-trigger-recovery")

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, claim_id, lease_owner, last_error
            FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            (effect_id,),
        ).fetchone()
        execution = conn.execute(
            """
            SELECT execution_status, unknown_at, unknown_reason
            FROM agent_model_execution_runs
            WHERE effect_id = ?
            """,
            (effect_id,),
        ).fetchone()
        mailbox_count = conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()[0]
        handoff_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox_handoffs"
        ).fetchone()[0]
        fence = conn.execute(
            """
            SELECT status
            FROM agent_session_actor_v2_admission_fences
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert effect is not None
    assert tuple(effect) == ("processing", claim.claim_id, claim.worker_id, "")
    assert execution is not None
    assert tuple(execution) == ("running", None, "")
    assert mailbox_count == 0
    assert handoff_count == 0
    assert fence is not None
    assert fence["status"] == "committed"


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_state", ("missing", "revoked", "expired"))
async def test_sqlite_effect_settlement_fails_closed_for_invalid_admission_fence(
    tmp_path: Path,
    fence_state: str,
) -> None:
    """An invalid committed fence cannot emit a mailbox outcome or retry it."""

    now = [100.0]
    database, store, key, grant, _generation = await _make_fenced_store(tmp_path, now)
    _seed_effect(database, key)
    claim = await store.claim_next(worker_id="invalid-fence-worker")
    assert claim is not None
    if fence_state == "missing":
        with database.connect() as conn:
            conn.execute(
                """
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            )
    elif fence_state == "revoked":
        database.actor_v2_admission_fences.revoke(
            grant,
            reason="effect settlement fence test",
        )
    else:
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_session_actor_v2_admission_fences
                SET expires_at = 0
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            )

    with pytest.raises(EffectClaimLost, match="ownership generation"):
        await store.complete_with_event(claim, _completion(claim))

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, claim_id, lease_owner, completed_at
            FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            (claim.effect.effect_id,),
        ).fetchone()
        mailbox_count = conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()[0]
    assert effect is not None
    assert tuple(effect) == ("processing", claim.claim_id, claim.worker_id, None)
    assert mailbox_count == 0


@pytest.mark.asyncio
async def test_sqlite_effect_final_fence_gate_rolls_back_mailbox_and_outbox(
    tmp_path: Path,
) -> None:
    """A fence lost after terminal staging leaves no settlement mutation visible."""

    now = [100.0]
    database, store, key, _grant, _generation = await _make_fenced_store(tmp_path, now)
    _seed_effect(database, key)
    claim = await store.claim_next(worker_id="final-gate-worker")
    assert claim is not None
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER remove_effect_settlement_fence_before_final_gate
            AFTER UPDATE OF status ON agent_effect_outbox
            WHEN NEW.effect_id = 'effect-1' AND NEW.status = 'completed'
            BEGIN
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = NEW.profile_id AND session_id = NEW.session_id;
            END
            """
        )

    with pytest.raises(EffectClaimLost, match="ownership generation"):
        await store.complete_with_event(claim, _completion(claim))

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, claim_id, lease_owner, completed_at
            FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            (claim.effect.effect_id,),
        ).fetchone()
        mailbox_count = conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()[0]
        handoff_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox_handoffs"
        ).fetchone()[0]
        fence = conn.execute(
            """
            SELECT status
            FROM agent_session_actor_v2_admission_fences
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert effect is not None
    assert tuple(effect) == ("processing", claim.claim_id, claim.worker_id, None)
    assert mailbox_count == 0
    assert handoff_count == 0
    assert fence is not None
    assert fence["status"] == "committed"


@pytest.mark.asyncio
async def test_sqlite_quarantine_final_fence_gate_rolls_back_all_candidate_writes(
    tmp_path: Path,
) -> None:
    """A lost fence rolls back the diagnostic mailbox, outbox, and gate mutation."""

    now = [100.0]
    database, store, key, _grant, _generation = await _make_fenced_store(tmp_path, now)
    _seed_effect(database, key, effect_id="quarantine-final-gate-effect")
    claim = await store.claim_next(worker_id="quarantine-final-gate-worker")
    assert claim is not None
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER remove_quarantine_fence_before_final_gate
            AFTER UPDATE OF status ON agent_effect_outbox
            WHEN NEW.effect_id = 'quarantine-final-gate-effect' AND NEW.status = 'failed'
            BEGIN
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = NEW.profile_id AND session_id = NEW.session_id;
            END
            """
        )

    with pytest.raises(EffectClaimLost, match="ownership generation"):
        await store.quarantine(
            claim,
            reason=EffectQuarantineReason.UNSUPPORTED_CONTRACT,
            message="final gate rollback test",
        )

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, claim_id, lease_owner, completed_at, last_error
            FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            (claim.effect.effect_id,),
        ).fetchone()
        mailbox_count = conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()[0]
        handoff_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox_handoffs"
        ).fetchone()[0]
        fence = conn.execute(
            """
            SELECT status
            FROM agent_session_actor_v2_admission_fences
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert effect is not None
    assert tuple(effect) == ("processing", claim.claim_id, claim.worker_id, None, "")
    assert mailbox_count == 0
    assert handoff_count == 0
    assert fence is not None
    assert fence["status"] == "committed"


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
async def test_sqlite_effect_store_accepts_exact_success_completion_evidence(
    tmp_path: Path,
) -> None:
    now = [100.0]
    contract = _external_contract(
        kind="verified_completion",
        version=2,
        completion_event_kind="VerifiedCompletion",
        outcome_fence_fields=("plan_id",),
    )
    database, store, key = await _make_store(
        tmp_path,
        now,
        contracts=(contract,),
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
    completion = _completion(claim, contract=contract)

    settled = await store.complete_with_event(claim, completion)

    assert settled.status is EffectSettlementStatus.COMMITTED
    with database.connect() as conn:
        effect = conn.execute(
            "SELECT status, claim_id FROM agent_effect_outbox WHERE effect_id = ?",
            (claim.effect.effect_id,),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT kind, source, payload_json
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (
                claim.key.profile_id,
                claim.key.session_id,
                completion.event_id,
            ),
        ).fetchone()
    assert tuple(effect) == (DurableEffectStatus.COMPLETED.value, claim.claim_id)
    assert mailbox is not None
    assert tuple(mailbox)[:2] == (
        contract.completion_event_kind,
        contract.completion_source,
    )
    assert json.loads(str(mailbox["payload_json"])) == dict(completion.payload)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("envelope_changes", "payload_changes", "payload_delete", "match"),
    [
        ({"event_id": "forged-completion"}, {}, "", "event id"),
        ({"kind": "ForgedCompletion"}, {}, "", "event kind"),
        ({"source": "forged"}, {}, "", "source"),
        ({"causation_id": "forged"}, {}, "", "causation"),
        ({"correlation_id": "forged"}, {}, "", "correlation"),
        ({"trace_id": "forged"}, {}, "", "trace"),
        ({"ownership_generation": 2}, {}, "", "ownership generation"),
        ({}, {"effect_id": "forged"}, "", "effect_id"),
        ({}, {"effect_kind": "forged"}, "", "effect_kind"),
        ({}, {"idempotency_key": "forged"}, "", "idempotency_key"),
        ({}, {"operation_id": "forged"}, "", "operation_id"),
        ({}, {"contract_version": 1}, "", "contract_version"),
        ({}, {"contract_signature": "forged"}, "", "contract_signature"),
        ({}, {"attempt_count": 2}, "", "attempt count"),
        ({}, {}, "attempt_count", "attempt count"),
        ({}, {"plan_id": "forged"}, "", "fence plan_id"),
        ({}, {}, "plan_id", "fence plan_id"),
    ],
)
async def test_sqlite_effect_store_rejects_forged_success_completion_evidence(
    tmp_path: Path,
    envelope_changes: dict[str, object],
    payload_changes: dict[str, object],
    payload_delete: str,
    match: str,
) -> None:
    now = [100.0]
    contract = _external_contract(
        kind="verified_completion",
        version=2,
        completion_event_kind="VerifiedCompletion",
        outcome_fence_fields=("plan_id",),
    )
    database, store, key = await _make_store(
        tmp_path,
        now,
        contracts=(contract,),
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
    completion = _completion(claim, contract=contract)
    forged_payload = {**completion.payload, **payload_changes}
    if payload_delete:
        forged_payload.pop(payload_delete)
    forged = replace(
        completion,
        payload=forged_payload,
        **envelope_changes,
    )

    with pytest.raises(EffectStoreConflict, match=match):
        await store.complete_with_event(claim, forged)

    with database.connect() as conn:
        effect = conn.execute(
            "SELECT status, claim_id FROM agent_effect_outbox WHERE effect_id = ?",
            (claim.effect.effect_id,),
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
async def test_sqlite_executor_quarantines_unknown_contract_without_retry(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    unknown = _external_contract(kind="unknown_effect", version=7)
    _seed_effect(
        database,
        key,
        kind=unknown.effect_kind,
        contract=unknown,
    )
    wake_registry = _WakeRegistry()
    executor = DurableEffectExecutor(
        store=store,
        handlers=EffectHandlerRegistry(include_builtin_contracts=False),
        session_registry=wake_registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once(lane=EffectLane.ORPHAN)
    replay = await executor.run_once(lane=EffectLane.ORPHAN)

    assert result.status is EffectRunStatus.FAILED
    assert replay.status is EffectRunStatus.EMPTY
    assert wake_registry.keys == [key]
    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, attempt_count, lease_owner, lease_until, last_error
            FROM agent_effect_outbox WHERE effect_id = 'effect-1'
            """
        ).fetchone()
        event = conn.execute(
            """
            SELECT kind, source, ownership_generation, causation_id,
                   correlation_id, payload_json
            FROM agent_session_mailbox
            WHERE event_id = ?
            """,
            (result.event_id,),
        ).fetchone()
    assert tuple(effect)[:4] == ("failed", 1, "", None)
    assert "unsupported_contract" in str(effect["last_error"])
    assert event is not None
    payload = json.loads(str(event["payload_json"]))
    assert tuple(event)[:5] == (
        "EffectQuarantined",
        "effect_store",
        1,
        "source:effect-1",
        "operation-1",
    )
    assert payload["reason_code"] == "unsupported_contract"
    assert payload["failure_code"] == "unsupported_contract"
    assert payload["contract_version"] == 7


@pytest.mark.asyncio
async def test_sqlite_executor_quarantines_signature_drift_without_handler(
    tmp_path: Path,
) -> None:
    now = [100.0]
    contract = _external_contract(kind="signed_effect", version=2)
    database, store, key = await _make_store(tmp_path, now, contracts=(contract,))
    _seed_effect(
        database,
        key,
        kind=contract.effect_kind,
        contract=contract,
    )
    with database.connect() as conn:
        conn.execute(
            "UPDATE agent_effect_outbox SET contract_signature = 'drifted-signature'"
        )
    calls = 0

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        nonlocal calls
        calls += 1
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=_WakeRegistry(),
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.FAILED
    assert calls == 0
    with database.connect() as conn:
        effect = conn.execute(
            "SELECT status, attempt_count FROM agent_effect_outbox"
        ).fetchone()
        event = conn.execute(
            "SELECT kind, payload_json FROM agent_session_mailbox"
        ).fetchone()
    assert tuple(effect) == ("failed", 1)
    assert event["kind"] == "EffectQuarantined"
    payload = json.loads(str(event["payload_json"]))
    assert payload["reason_code"] == "contract_signature_mismatch"
    assert payload["contract_signature"] == "drifted-signature"


@pytest.mark.asyncio
async def test_sqlite_executor_quarantines_incomplete_explicit_v2_before_handler(
    tmp_path: Path,
) -> None:
    now = [100.0]
    contract = _external_contract(
        kind="fenced_effect",
        version=2,
        outcome_fence_fields=("plan_id", "input_watermark"),
    )
    database, store, key = await _make_store(tmp_path, now, contracts=(contract,))
    _seed_effect(
        database,
        key,
        kind=contract.effect_kind,
        contract=contract,
        payload={"plan_id": "plan-a"},
    )
    calls = 0

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        nonlocal calls
        calls += 1
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=_WakeRegistry(),
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.FAILED
    assert calls == 0
    with database.connect() as conn:
        effect = conn.execute(
            "SELECT status, attempt_count FROM agent_effect_outbox"
        ).fetchone()
        event = conn.execute(
            "SELECT kind, payload_json FROM agent_session_mailbox"
        ).fetchone()
    assert tuple(effect) == ("failed", 1)
    payload = json.loads(str(event["payload_json"]))
    assert event["kind"] == "EffectQuarantined"
    assert payload["reason_code"] == "outcome_fence_missing"
    assert "input_watermark" in payload["reason_message"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("column_name", "malformed_value", "expected_violation"),
    (
        ("contract_version", "abc", "contract_version_not_integer"),
        ("payload_json", "not-json", "payload_json_invalid"),
        ("payload_json", "[]", "payload_json_not_object"),
        ("payload_json", '{"x": 1}', "payload_json_noncanonical"),
        ("payload_json", '{"b":1,"a":2}', "payload_json_noncanonical"),
        ("payload_json", '{"value":1e400}', "payload_json_nonfinite"),
        (
            "attempt_count",
            (1 << 63) - 1,
            "attempt_count_not_claimable",
        ),
    ),
)
async def test_claim_quarantines_malformed_rows_before_handler_and_continues(
    tmp_path: Path,
    column_name: str,
    malformed_value: object,
    expected_violation: str,
) -> None:
    now = [100.0]
    contract = _external_contract()
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key, effect_id="bad-effect", contract=contract)
    _seed_effect(database, key, effect_id="good-effect", contract=contract)
    with database.connect() as conn:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            f"UPDATE agent_effect_outbox SET {column_name} = ? "
            "WHERE effect_id = 'bad-effect'",
            (malformed_value,),
        )

    calls: list[str] = []

    async def handler(context: EffectExecutionContext) -> EffectHandlerResult:
        calls.append(context.claim.effect.effect_id)
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    wake_registry = _WakeRegistry()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=wake_registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.COMPLETED
    assert result.effect_id == "good-effect"
    assert calls == ["good-effect"]
    assert wake_registry.keys == [key, key]
    with database.connect() as conn:
        bad_effect = conn.execute(
            """
            SELECT status, attempt_count, kind, claim_id, lease_owner,
                   lease_until, completed_at, last_error
            FROM agent_effect_outbox
            WHERE effect_id = 'bad-effect'
            """
        ).fetchone()
        event = conn.execute(
            """
            SELECT kind, source, payload_json
            FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()
    assert bad_effect is not None
    expected_attempt_count = (
        int(malformed_value) if column_name == "attempt_count" else 0
    )
    assert tuple(bad_effect)[:6] == (
        "failed",
        expected_attempt_count,
        "__malformed_persisted_effect__",
        "",
        "",
        None,
    )
    assert bad_effect["completed_at"] == now[0]
    assert "malformed_effect_row" in str(bad_effect["last_error"])
    assert event is not None
    assert tuple(event)[:2] == ("EffectQuarantined", "effect_store")
    payload = json.loads(str(event["payload_json"]))
    assert payload["failure_code"] == "malformed_effect_row"
    assert payload["reason_code"] == "malformed_effect_row"
    assert expected_violation in payload["violations"]
    assert _evidence_prefix_text(payload["raw_row"][column_name]) == str(
        malformed_value
    )
    assert await store.claim_next(worker_id="worker-after-quarantine") is None


@pytest.mark.asyncio
async def test_only_malformed_claim_still_wakes_its_diagnostic_mailbox(
    tmp_path: Path,
) -> None:
    now = [100.0]
    contract = _external_contract()
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key, effect_id="bad-effect", contract=contract)
    with database.connect() as conn:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET contract_version = 'abc'
            WHERE effect_id = 'bad-effect'
            """
        )
    calls = 0

    async def handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        nonlocal calls
        calls += 1
        return EffectHandlerResult()

    handlers = EffectHandlerRegistry(include_builtin_contracts=False)
    handlers.register(contract.effect_kind, handler, contract=contract)
    wake_registry = _WakeRegistry()
    executor = DurableEffectExecutor(
        store=store,
        handlers=handlers,
        session_registry=wake_registry,
        renew_interval_seconds=None,
        clock=lambda: now[0],
    )

    result = await executor.run_once()

    assert result.status is EffectRunStatus.EMPTY
    assert calls == 0
    assert wake_registry.keys == [key]
    with database.connect() as conn:
        effect = conn.execute(
            "SELECT status, attempt_count FROM agent_effect_outbox"
        ).fetchone()
        event = conn.execute(
            "SELECT kind, status FROM agent_session_mailbox"
        ).fetchone()
    assert tuple(effect) == ("failed", 0)
    assert tuple(event) == ("EffectQuarantined", "pending")


@pytest.mark.asyncio
async def test_malformed_effect_quarantine_returns_exact_fenced_wake_request(
    tmp_path: Path,
) -> None:
    """Store-owned diagnostics retain the final admission-fenced identity."""

    now = [100.0]
    database, store, key, grant, ownership_generation = await _make_fenced_store(
        tmp_path,
        now,
    )
    _seed_effect(database, key, effect_id="fenced-malformed-effect")
    with database.connect() as conn:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET contract_version = 'abc'
            WHERE effect_id = 'fenced-malformed-effect'
            """
        )

    assert await store.claim_next(worker_id="fenced-malformed-worker") is None

    notifications = await store.drain_quarantine_notifications()
    assert len(notifications) == 1
    assert notifications[0].status is EffectSettlementStatus.COMMITTED
    assert notifications[0].wake_request == FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership_generation,
        admission_fence_id=grant.fence.fence_id,
        admission_fence_generation=grant.fence.generation,
    )
    with database.connect() as conn:
        handoff = conn.execute(
            """
            SELECT handoff.evidence_state, handoff.admission_fence_id,
                   handoff.admission_fence_generation, handoff.state
            FROM agent_session_mailbox_handoffs AS handoff
            JOIN agent_session_mailbox AS mailbox
              ON mailbox.mailbox_id = handoff.mailbox_id
            WHERE mailbox.profile_id = ?
              AND mailbox.session_id = ?
              AND mailbox.event_id = ?
            """,
            (key.profile_id, key.session_id, notifications[0].event_id),
        ).fetchone()
    assert handoff is not None
    assert tuple(handoff) == (
        MailboxHandoffEvidenceState.FENCED.value,
        grant.fence.fence_id,
        grant.fence.generation,
        MailboxHandoffState.PENDING.value,
    )


@pytest.mark.asyncio
async def test_lone_surrogate_is_quarantined_without_blocking_following_work(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key, effect_id="surrogate-effect")
    _seed_effect(database, key, effect_id="following-effect")
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET payload_json = ?
            WHERE effect_id = 'surrogate-effect'
            """,
            ('{"x":"\\ud800"}',),
        )

    following = await store.claim_next(worker_id="worker-a")

    assert following is not None
    assert following.effect.effect_id == "following-effect"
    with database.connect() as conn:
        malformed = conn.execute(
            """
            SELECT status, attempt_count
            FROM agent_effect_outbox
            WHERE effect_id = 'surrogate-effect'
            """
        ).fetchone()
        event = conn.execute(
            """
            SELECT payload_json
            FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()
    assert tuple(malformed) == ("failed", 0)
    payload = json.loads(str(event["payload_json"]))
    assert "payload_json_invalid_utf8" in payload["violations"]
    assert _evidence_prefix_text(
        payload["raw_row"]["payload_json"]
    ) == '{"x":"\\ud800"}'

    database.initialize()
    database.initialize()
    with database.connect() as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()[0] == 1


@pytest.mark.asyncio
async def test_raw_claim_validation_is_lossless_bounded_and_continues(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    for effect_id in (
        "invalid-utf8-effect",
        "deep-effect",
        "duplicate-key-effect",
        "many-nodes-effect",
        "oversized-effect",
        "following-effect",
    ):
        _seed_effect(database, key, effect_id=effect_id)
    deep_payload = '{"x":' + ("[" * 1_200) + "0" + ("]" * 1_200) + "}"
    duplicate_payload = '{"x":1,"x":2}'
    many_nodes_payload = (
        '{"items":[' + ",".join("0" for _ in range(MAX_CANONICAL_JSON_NODES)) + "]}"
    )
    oversized_payload = (
        '{"blob":"' + ("x" * MAX_CANONICAL_JSON_BYTES) + '"}'
    )
    invalid_utf8_payload = b'{"x":\xff}'
    with database.connect() as conn:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET payload_json = CAST(X'7B2278223AFF7D' AS TEXT)
            WHERE effect_id = 'invalid-utf8-effect'
            """
        )
        conn.execute(
            "UPDATE agent_effect_outbox SET payload_json = ? "
            "WHERE effect_id = 'deep-effect'",
            (deep_payload,),
        )
        conn.execute(
            "UPDATE agent_effect_outbox SET payload_json = ? "
            "WHERE effect_id = 'duplicate-key-effect'",
            (duplicate_payload,),
        )
        conn.execute(
            "UPDATE agent_effect_outbox SET payload_json = ? "
            "WHERE effect_id = 'many-nodes-effect'",
            (many_nodes_payload,),
        )
        conn.execute(
            "UPDATE agent_effect_outbox SET payload_json = ? "
            "WHERE effect_id = 'oversized-effect'",
            (oversized_payload,),
        )

    assert await store.claim_next(worker_id="quarantine-worker") is None
    following = await store.claim_next(worker_id="worker-a")

    assert following is not None
    assert following.effect.effect_id == "following-effect"
    with database.connect() as conn:
        events = conn.execute(
            """
            SELECT causation_id, payload_json
            FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            ORDER BY causation_id
            """
        ).fetchall()
    payloads = {
        str(row["causation_id"]): json.loads(str(row["payload_json"]))
        for row in events
    }
    expected = {
        "source:invalid-utf8-effect": (
            "payload_json_invalid_utf8",
            invalid_utf8_payload,
        ),
        "source:deep-effect": ("payload_json_too_deep", deep_payload.encode()),
        "source:duplicate-key-effect": (
            "payload_json_duplicate_key",
            duplicate_payload.encode(),
        ),
        "source:many-nodes-effect": (
            "payload_json_too_many_nodes",
            many_nodes_payload.encode(),
        ),
        "source:oversized-effect": (
            "payload_json_too_large",
            oversized_payload.encode(),
        ),
    }
    assert set(payloads) == set(expected)
    for source_event_id, (violation, original_bytes) in expected.items():
        payload = payloads[source_event_id]
        evidence = payload["raw_row"]["payload_json"]
        assert violation in payload["violations"]
        assert evidence["storage_class"] == "text"
        assert evidence["byte_length"] == len(original_bytes)
        assert evidence["sha256"] == hashlib.sha256(original_bytes).hexdigest()
        assert _evidence_prefix_bytes(evidence) == original_bytes[:192]
        assert len(_evidence_prefix_bytes(evidence)) <= 192
        assert evidence["truncated"] is (len(original_bytes) > 192)


@pytest.mark.asyncio
async def test_unaddressable_runtime_poison_is_inert_and_does_not_block_work(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key, effect_id="unaddressable-poison")
    _seed_effect(database, key, effect_id="addressable-good")
    with database.connect() as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET profile_id = CAST(X'80' AS TEXT)
            WHERE effect_id = 'unaddressable-poison'
            """
        )

    claim = await store.claim_next(worker_id="worker-a")

    assert claim is not None
    assert claim.effect.effect_id == "addressable-good"
    with sqlite3.connect(database.config.sqlite_path) as raw_conn:
        poison = raw_conn.execute(
            """
            SELECT status, typeof(profile_id), hex(CAST(profile_id AS BLOB))
            FROM agent_effect_outbox
            WHERE effect_id = 'unaddressable-poison'
            """
        ).fetchone()
        diagnostics = raw_conn.execute(
            """
            SELECT COUNT(*) FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()[0]
    assert poison == ("pending", "text", "80")
    assert diagnostics == 0


@pytest.mark.asyncio
async def test_scrub_cursor_crosses_blocked_prefix_with_bounded_linear_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [100.0]
    database, first_store, key = await _make_store(tmp_path, now)
    blocked_count = 225
    _seed_blocked_external_action_effects(
        database,
        key,
        count=blocked_count,
        now=now[0],
    )
    _seed_effect(database, key, effect_id="poison-after-blocked-prefix")
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET payload_json = '{"x":1,"x":2}'
            WHERE effect_id = 'poison-after-blocked-prefix'
            """
        )

    raw_row_calls = 0
    original_raw_effect_row = effect_store_module._raw_effect_row

    def counted_raw_effect_row(*args: object, **kwargs: object):
        nonlocal raw_row_calls
        raw_row_calls += 1
        return original_raw_effect_row(*args, **kwargs)

    monkeypatch.setattr(
        effect_store_module,
        "_raw_effect_row",
        counted_raw_effect_row,
    )
    external_contracts = tuple(
        contract.ref for contract in builtin_external_action_effect_contracts()
    )
    second_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: now[0],
    )
    stores = [first_store, second_store]
    per_call_counts: list[int] = []
    for call_index in range(40):
        if call_index == 12:
            stores[0] = SQLiteDurableEffectStore(
                database,
                lease_seconds=5.0,
                clock=lambda: now[0],
            )
        before = raw_row_calls
        claimed = await stores[call_index % 2].claim_next(
            worker_id=f"worker-{call_index % 2}",
            effect_contracts=external_contracts,
        )
        assert claimed is None
        per_call_counts.append(raw_row_calls - before)
        with database.connect() as conn:
            poison_status = conn.execute(
                """
                SELECT status FROM agent_effect_outbox
                WHERE effect_id = 'poison-after-blocked-prefix'
                """
            ).fetchone()["status"]
        if poison_status == "failed":
            break
    else:
        pytest.fail("persisted scrub cursor never reached the poisoned row")

    assert len(per_call_counts) == 29
    assert max(per_call_counts) <= 8
    assert raw_row_calls <= blocked_count + 8
    with database.connect() as conn:
        cursor = conn.execute(
            """
            SELECT last_effect_seq FROM agent_effect_scrub_state
            WHERE cursor_name = 'claimable'
            """
        ).fetchone()["last_effect_seq"]
        blocked_pending = conn.execute(
            """
            SELECT COUNT(*) FROM agent_effect_outbox
            WHERE status = 'pending' AND effect_id != 'poison-after-blocked-prefix'
            """
        ).fetchone()[0]
    assert cursor == blocked_count + 1
    assert blocked_pending == blocked_count


@pytest.mark.asyncio
async def test_scrub_streams_at_most_one_oversized_row_per_transaction(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    oversized_ids = tuple(f"oversized-{index}" for index in range(3))
    for effect_id in (*oversized_ids, "small-following"):
        _seed_effect(database, key, effect_id=effect_id)
    oversized_payload = (
        '{"blob":"' + ("x" * MAX_CANONICAL_JSON_BYTES) + '"}'
    )
    with database.connect() as conn:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.executemany(
            """
            UPDATE agent_effect_outbox SET payload_json = ?
            WHERE effect_id = ?
            """,
            tuple((oversized_payload, effect_id) for effect_id in oversized_ids),
        )

    for expected_failed in range(1, len(oversized_ids) + 1):
        assert (
            await store.claim_next(
                worker_id=f"scrubber-{expected_failed}",
                effect_contracts=(),
            )
            is None
        )
        with database.connect() as conn:
            failed = conn.execute(
                """
                SELECT COUNT(*) FROM agent_effect_outbox
                WHERE status = 'failed'
                """
            ).fetchone()[0]
            cursor = conn.execute(
                """
                SELECT last_effect_seq FROM agent_effect_scrub_state
                WHERE cursor_name = 'claimable'
                """
            ).fetchone()["last_effect_seq"]
        assert failed == expected_failed
        assert cursor == expected_failed

    assert (
        await store.claim_next(worker_id="scrubber-small", effect_contracts=())
        is None
    )
    with database.connect() as conn:
        cursor = conn.execute(
            """
            SELECT last_effect_seq FROM agent_effect_scrub_state
            WHERE cursor_name = 'claimable'
            """
        ).fetchone()["last_effect_seq"]
        small_status = conn.execute(
            """
            SELECT status FROM agent_effect_outbox
            WHERE effect_id = 'small-following'
            """
        ).fetchone()["status"]
    assert cursor == 4
    assert small_status == "pending"


@pytest.mark.asyncio
async def test_inline_claim_does_not_stream_second_oversized_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [100.0]
    selected_contract = _external_contract(kind="selected-effect")
    filtered_contract = _external_contract(kind="filtered-effect")
    database, store, key = await _make_store(
        tmp_path,
        now,
        contracts=(selected_contract, filtered_contract),
    )
    _seed_effect(
        database,
        key,
        effect_id="filtered-prefix",
        kind=filtered_contract.effect_kind,
        contract=filtered_contract,
    )
    for effect_id in ("oversized-first", "oversized-second"):
        _seed_effect(
            database,
            key,
            effect_id=effect_id,
            kind=selected_contract.effect_kind,
            contract=selected_contract,
        )
    oversized_payload = (
        '{"blob":"' + ("x" * MAX_CANONICAL_JSON_BYTES) + '"}'
    )
    with database.connect() as conn:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.executemany(
            """
            UPDATE agent_effect_outbox SET payload_json = ?
            WHERE effect_id = ?
            """,
            (
                (oversized_payload, "oversized-first"),
                (oversized_payload, "oversized-second"),
            ),
        )

    streamed_effect_seqs: list[int] = []
    original_chunk_reader = effect_store_module._read_effect_raw_chunk

    def counted_chunk_reader(*args: object, **kwargs: object) -> object:
        streamed_effect_seqs.append(int(kwargs["effect_seq"]))
        return original_chunk_reader(*args, **kwargs)

    monkeypatch.setattr(
        effect_store_module,
        "_read_effect_raw_chunk",
        counted_chunk_reader,
    )

    assert (
        await store.claim_next(
            worker_id="inline-worker",
            effect_contracts=(selected_contract.ref,),
        )
        is None
    )
    with database.connect() as conn:
        statuses = {
            str(row["effect_id"]): str(row["status"])
            for row in conn.execute(
                """
                SELECT effect_id, status FROM agent_effect_outbox
                WHERE effect_id LIKE 'oversized-%'
                ORDER BY effect_seq
                """
            )
        }
    assert statuses == {
        "oversized-first": "failed",
        "oversized-second": "pending",
    }
    assert set(streamed_effect_seqs) == {2}

    assert (
        await store.claim_next(
            worker_id="scrub-worker",
            effect_contracts=(selected_contract.ref,),
        )
        is None
    )
    assert set(streamed_effect_seqs) == {2, 3}


@pytest.mark.asyncio
async def test_attempt_count_int64_boundary_never_reclaims_in_a_hot_loop(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key, effect_id="boundary-effect")
    _seed_effect(database, key, effect_id="following-effect")
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET attempt_count = ?
            WHERE effect_id = 'boundary-effect'
            """,
            ((1 << 63) - 2,),
        )

    boundary = await store.claim_next(worker_id="boundary-worker")
    assert boundary is not None
    assert boundary.effect.effect_id == "boundary-effect"
    assert boundary.attempt_count == (1 << 63) - 1
    await store.release(boundary, error="boundary retry")

    following = await store.claim_next(worker_id="following-worker")

    assert following is not None
    assert following.effect.effect_id == "following-effect"
    notifications = await store.drain_quarantine_notifications()
    assert len(notifications) == 1
    with database.connect() as conn:
        boundary_row = conn.execute(
            """
            SELECT status, attempt_count, completed_at
            FROM agent_effect_outbox
            WHERE effect_id = 'boundary-effect'
            """
        ).fetchone()
        event = conn.execute(
            """
            SELECT payload_json
            FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()
    assert tuple(boundary_row)[:2] == ("failed", (1 << 63) - 1)
    assert boundary_row["completed_at"] == now[0]
    payload = json.loads(str(event["payload_json"]))
    assert "attempt_count_not_claimable" in payload["violations"]


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
        await wait_for_session_actor_idle(database, actor_registry, key)
    finally:
        await actor_registry.shutdown()

    assert result.status == EffectRunStatus.COMPLETED
    assert handler_calls == 1
    assert wake_registry.wake_attempts == 2
    assert wake_registry.recover_attempts == 0
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("column_name", "malformed_value"),
    (
        ("effect_id", ""),
        ("effect_id", "\tbad-effect"),
        ("contract_signature", ""),
        ("ownership_generation", "abc"),
        ("contract_version", "abc"),
        ("attempt_count", "abc"),
        ("payload_json", "not-json"),
        ("payload_json", "[]"),
        ("payload_json", '{"x": 1}'),
    ),
)
async def test_fresh_effect_outbox_rejects_malformed_rows(
    tmp_path: Path,
    column_name: str,
    malformed_value: object,
) -> None:
    now = [100.0]
    database, store, key = await _make_store(tmp_path, now)
    _seed_effect(database, key, payload={"accepted": True})

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                f"UPDATE agent_effect_outbox SET {column_name} = ?",
                (malformed_value,),
            )

    claim = await store.claim_next(worker_id="worker-a")
    assert claim is not None
    assert dict(claim.effect.payload) == {"accepted": True}


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
    assert (
        "CHECK(status IN ('pending', 'processing', 'completed', 'failed', "
        "'cancelled'))"
    ) in (
        create_sql
    )


@pytest.mark.parametrize(
    "removed_fragment",
    (
        "UNIQUE(profile_id, session_id, effect_id),",
        """FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,""",
        "CHECK(status IN ('pending', 'processing', 'completed', 'failed', "
        "'cancelled')),",
        "CHECK(attempt_count <= 9223372036854775807),",
    ),
)
def test_effect_outbox_schema_verifier_rebuilds_any_weakened_contract(
    tmp_path: Path,
    removed_fragment: str,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    with database.connect() as conn:
        canonical_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = 'agent_effect_outbox'
                """
            ).fetchone()["sql"]
        )
        assert removed_fragment in canonical_sql
        weakened_sql = canonical_sql.replace(removed_fragment, "", 1)
        conn.execute("ALTER TABLE agent_effect_outbox RENAME TO effect_outbox_current")
        conn.execute(weakened_sql)
        conn.execute("DROP TABLE effect_outbox_current")

    database.initialize()

    with database.connect() as conn:
        rebuilt_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = 'agent_effect_outbox'
                """
            ).fetchone()["sql"]
        )
    assert rebuilt_sql == canonical_sql


def test_effect_outbox_schema_verifier_preserves_literal_case(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    with database.connect() as conn:
        canonical_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = 'agent_effect_outbox'
                """
            ).fetchone()["sql"]
        )
        assert "DEFAULT 'pending'" in canonical_sql
        weakened_sql = canonical_sql.replace(
            "DEFAULT 'pending'",
            "DEFAULT 'PENDING'",
            1,
        )
        conn.execute("ALTER TABLE agent_effect_outbox RENAME TO effect_outbox_current")
        conn.execute(weakened_sql)
        conn.execute("DROP TABLE effect_outbox_current")

    database.initialize()
    database.initialize()

    with database.connect() as conn:
        rebuilt_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = 'agent_effect_outbox'
                """
            ).fetchone()["sql"]
        )
    assert rebuilt_sql == canonical_sql


def test_effect_outbox_schema_supports_raw_sqlite_maintenance(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("raw-profile", "raw-profile:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="raw sqlite maintenance fixture",
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

    with sqlite3.connect(database.config.sqlite_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        schema_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = 'agent_effect_outbox'
                """
            ).fetchone()[0]
        )
        assert "shinbot_canonical" not in schema_sql
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, kind,
                contract_version, contract_signature, payload_json,
                available_at, created_at, updated_at
            ) VALUES ('raw-effect', 'raw-key', ?, ?,
                      ?, 'raw-source', 'raw-kind', 1, 'raw-signature', '{}',
                      100.0, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, owner.generation),
        )
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        conn.commit()
        conn.execute("VACUUM")
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    assert integrity == "ok"
    assert quick == "ok"
    database.initialize()
    database.initialize()


def test_effect_outbox_migration_quarantines_malformed_legacy_rows(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="malformed migration fixture owner",
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
                ownership_generation INTEGER NOT NULL DEFAULT 0,
                event_id TEXT NOT NULL,
                operation_id TEXT NOT NULL DEFAULT '',
                kind TEXT NOT NULL,
                contract_version INTEGER NOT NULL CHECK(contract_version >= 1),
                contract_signature TEXT NOT NULL,
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
                UNIQUE(profile_id, session_id, idempotency_key),
                CHECK(status IN ('pending', 'processing', 'completed', 'failed'))
            )
            """
        )
        conn.execute("DROP TABLE effect_outbox_current")
        conn.executemany(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, '', 'external_write', ?, 'signature', ?,
                      'pending', 0, 100.0, '', '', NULL, 100.0, 100.0, NULL, '')
            """,
            (
                (
                    "valid-effect",
                    "valid-key",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    "source:valid-effect",
                    1,
                    '{"accepted":true}',
                ),
                (
                    "text-version-effect",
                    "text-version-key",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    "source:text-version-effect",
                    "abc",
                    "{}",
                ),
                (
                    "invalid-json-effect",
                    "invalid-json-key",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    "source:invalid-json-effect",
                    1,
                    "not-json",
                ),
                (
                    "array-payload-effect",
                    "array-payload-key",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    "source:array-payload-effect",
                    1,
                    "[]",
                ),
                (
                    "noncanonical-effect",
                    "noncanonical-key",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    "source:noncanonical-effect",
                    1,
                    '{"x": 1}',
                ),
                (
                    "max-attempt-effect",
                    "max-attempt-key",
                    key.profile_id,
                    key.session_id,
                    owner.generation,
                    "source:max-attempt-effect",
                    1,
                    "{}",
                ),
            ),
        )
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET attempt_count = ?
            WHERE effect_id = 'max-attempt-effect'
            """,
            ((1 << 63) - 1,),
        )

    conflicting_event_id = derived_effect_event_id(
        key=key,
        effect_id="text-version-effect",
        outcome="quarantined",
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json,
                causation_id, correlation_id, trace_id,
                status, attempt_count, available_at,
                claim_id, lease_owner, lease_until,
                created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, 'ConflictingDiagnostic', 'fixture', 100.0, '{}',
                      '', '', '', 'pending', 0, 100.0, '', '', NULL,
                      100.0, NULL, '')
            """,
            (
                conflicting_event_id,
                key.profile_id,
                key.session_id,
                owner.generation,
            ),
        )
    with pytest.raises(
        sqlite3.IntegrityError,
        match="changed diagnostic identity",
    ):
        database.initialize()
    with database.connect() as conn:
        raw_row = conn.execute(
            """
            SELECT typeof(contract_version), contract_version, payload_json
            FROM agent_effect_outbox
            WHERE effect_id = 'text-version-effect'
            """
        ).fetchone()
        legacy_table = conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_effect_outbox_legacy'
            """
        ).fetchone()
        conn.execute(
            """
            DELETE FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, conflicting_event_id),
        )
    assert tuple(raw_row) == ("text", "abc", "{}")
    assert legacy_table is None

    database.initialize()
    database.initialize()

    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT effect_id, kind, contract_version, contract_signature,
                   status, attempt_count, completed_at
            FROM agent_effect_outbox
            ORDER BY effect_id
            """
        ).fetchall()
        events = conn.execute(
            """
            SELECT causation_id, kind, source, payload_json
            FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            ORDER BY causation_id
            """
        ).fetchall()
    by_effect_id = {str(row["effect_id"]): row for row in rows}
    valid = by_effect_id.pop("valid-effect")
    assert tuple(valid)[:6] == (
        "valid-effect",
        "external_write",
        1,
        "signature",
        "pending",
        0,
    )
    assert valid["completed_at"] is None
    assert set(by_effect_id) == {
        "array-payload-effect",
        "invalid-json-effect",
        "max-attempt-effect",
        "noncanonical-effect",
        "text-version-effect",
    }
    for effect_id, malformed in by_effect_id.items():
        expected_attempt_count = (
            (1 << 63) - 1 if effect_id == "max-attempt-effect" else 0
        )
        assert tuple(malformed)[1:6] == (
            "__malformed_persisted_effect__",
            1,
            "schema-quarantine-v1",
            "failed",
            expected_attempt_count,
        )
        assert malformed["completed_at"] is not None
    assert len(events) == 5
    payload_by_source = {
        str(event["causation_id"]): json.loads(str(event["payload_json"]))
        for event in events
    }
    assert all(
        tuple(event)[1:3] == ("EffectQuarantined", "effect_store")
        for event in events
    )
    assert (
        _evidence_prefix_text(
            payload_by_source["source:text-version-effect"]["raw_row"]
            ["contract_version"]
        )
        == "abc"
    )
    assert (
        _evidence_prefix_text(
            payload_by_source["source:invalid-json-effect"]["raw_row"]
            ["payload_json"]
        )
        == "not-json"
    )
    assert (
        _evidence_prefix_text(
            payload_by_source["source:array-payload-effect"]["raw_row"]
            ["payload_json"]
        )
        == "[]"
    )
    assert (
        _evidence_prefix_text(
            payload_by_source["source:noncanonical-effect"]["raw_row"]
            ["payload_json"]
        )
        == '{"x": 1}'
    )
    assert "payload_json_noncanonical" in payload_by_source[
        "source:noncanonical-effect"
    ]["violations"]
    assert "attempt_count_not_claimable" in payload_by_source[
        "source:max-attempt-effect"
    ]["violations"]
    assert {
        payload["failure_code"] for payload in payload_by_source.values()
    } == {"malformed_effect_row"}


@pytest.mark.asyncio
async def test_legacy_invalid_utf8_is_quarantined_from_raw_bytes(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, _store, key = await _make_store(tmp_path, now)
    with database.connect() as conn:
        _replace_with_weak_effect_outbox(conn)
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json,
                causation_id, correlation_id, trace_id,
                status, attempt_count, available_at,
                claim_id, lease_owner, lease_until,
                created_at, handled_at, last_error
            ) VALUES ('legacy-source', ?, ?, 1, 'LegacySource', 'fixture',
                      100.0, '{}', '', '', CAST(X'80' AS TEXT),
                      'completed', 0, 100.0, '', '', NULL, 100.0, 100.0, '')
            """,
            (key.profile_id, key.session_id),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES ('legacy-invalid-utf8', 'legacy-invalid-utf8-key', ?, ?, 1,
                      'legacy-source', '', 'external_write', 1, 'signature',
                      CAST(X'7B2278223AFF7D' AS TEXT), 'pending', 0, 100.0,
                      '', '', NULL, 100.0, 100.0, NULL, '')
            """,
            (key.profile_id, key.session_id),
        )

    database.initialize()
    database.initialize()

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, kind FROM agent_effect_outbox
            WHERE effect_id = 'legacy-invalid-utf8'
            """
        ).fetchone()
        event = conn.execute(
            """
            SELECT trace_id, payload_json FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()
    assert tuple(effect) == ("failed", "__malformed_persisted_effect__")
    assert event["trace_id"] == ""
    payload = json.loads(str(event["payload_json"]))
    assert "payload_json_invalid_utf8" in payload["violations"]
    assert "source_trace_id_invalid_utf8" in payload["violations"]
    payload_evidence = payload["raw_row"]["payload_json"]
    trace_evidence = payload["raw_row"]["source_trace_id"]
    assert _evidence_prefix_bytes(payload_evidence) == b'{"x":\xff}'
    assert payload_evidence["sha256"] == hashlib.sha256(b'{"x":\xff}').hexdigest()
    assert _evidence_prefix_bytes(trace_evidence) == b"\x80"
    assert trace_evidence["sha256"] == hashlib.sha256(b"\x80").hexdigest()


@pytest.mark.asyncio
async def test_unaddressable_legacy_invalid_utf8_rolls_back_rebuild(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, _store, key = await _make_store(tmp_path, now)
    with database.connect() as conn:
        _replace_with_weak_effect_outbox(conn)
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES ('unaddressable-effect', 'unaddressable-key',
                      CAST(X'80' AS TEXT), ?, 1, 'legacy-source', '',
                      'external_write', 1, 'signature', '{}', 'pending', 0,
                      100.0, '', '', NULL, 100.0, 100.0, NULL, '')
            """,
            (key.session_id,),
        )
    with sqlite3.connect(database.config.sqlite_path) as raw_conn:
        before = raw_conn.execute(
            """
            SELECT typeof(profile_id), hex(CAST(profile_id AS BLOB)),
                   typeof(payload_json), hex(CAST(payload_json AS BLOB))
            FROM agent_effect_outbox
            """
        ).fetchone()
        weak_sql = raw_conn.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_effect_outbox'
            """
        ).fetchone()[0]

    with pytest.raises(sqlite3.IntegrityError, match="canonical session"):
        database.initialize()

    with sqlite3.connect(database.config.sqlite_path) as raw_conn:
        after = raw_conn.execute(
            """
            SELECT typeof(profile_id), hex(CAST(profile_id AS BLOB)),
                   typeof(payload_json), hex(CAST(payload_json AS BLOB))
            FROM agent_effect_outbox
            """
        ).fetchone()
        after_sql = raw_conn.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_effect_outbox'
            """
        ).fetchone()[0]
        legacy_table = raw_conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_effect_outbox_legacy'
            """
        ).fetchone()
        diagnostics = raw_conn.execute(
            """
            SELECT COUNT(*) FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()[0]
    assert after == before == ("text", "80", "text", "7B7D")
    assert after_sql == weak_sql
    assert legacy_table is None
    assert diagnostics == 0


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
