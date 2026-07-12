"""Integration coverage for durable external-action receipts."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.effect_executor import ClaimedEffect
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.external_action_store import (
    ExternalActionClaimLost,
    ExternalActionConflict,
    ExternalActionEffectClaimLost,
    ExternalActionEffectConflict,
    ExternalActionMigrationBlocked,
    ExternalActionOwnershipLost,
    ExternalActionTerminalResult,
    SQLiteExternalActionReceiptStore,
    validate_external_action_migration,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
    ExternalActionReceiptStatus,
    ExternalActionRequest,
    materialize_external_action_effect,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMigrationConflict,
    AgentRuntimeOwnershipMode,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord

_EFFECT_CONTRACT_SIGNATURE = "external-action-contract-v1"


async def _make_store(
    tmp_path: Path,
    now: list[float],
    *,
    key: SessionKey | None = None,
) -> tuple[
    DatabaseManager,
    SQLiteExternalActionReceiptStore,
    SQLiteDurableEffectStore,
    SessionKey,
]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    resolved_key = key or SessionKey("profile-a", "profile-a:group:room")
    ownership = database.agent_runtime_ownership.claim(
        resolved_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="external action receipt test",
    ).ownership
    await SQLiteSessionActorStore(database, clock=lambda: now[0]).ensure(
        resolved_key,
        ownership_generation=ownership.generation,
    )
    return (
        database,
        SQLiteExternalActionReceiptStore(
            database,
            lease_seconds=5.0,
            clock=lambda: now[0],
        ),
        SQLiteDurableEffectStore(
            database,
            lease_seconds=5.0,
            clock=lambda: now[0],
        ),
        resolved_key,
    )


async def _add_actor_key(
    database: DatabaseManager,
    key: SessionKey,
    now: list[float],
) -> None:
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="additional external action profile",
    ).ownership
    await SQLiteSessionActorStore(database, clock=lambda: now[0]).ensure(
        key,
        ownership_generation=ownership.generation,
    )


def _request(
    key: SessionKey,
    *,
    kind: ExternalActionKind = ExternalActionKind.SEND_REPLY,
    operation_id: str = "active-chat-round-7",
    tool_call_id: str = "tool-call-1",
    action_ordinal: int = 0,
    payload: dict[str, object] | None = None,
    ownership_generation: int = 1,
    contract_version: int = 1,
    source_event_id: str = "round-completed-7",
) -> ExternalActionRequest:
    return ExternalActionRequest(
        key=key,
        ownership_generation=ownership_generation,
        operation_id=operation_id,
        source_event_id=source_event_id,
        instance_id="adapter-a",
        target_session_id="adapter-a:group:room",
        contract_version=contract_version,
        intent=ExternalActionIntent(
            kind=kind,
            tool_call_id=tool_call_id,
            action_ordinal=action_ordinal,
            payload=payload or {"text": "hello"},
        ),
    )


def _assistant_message(request: ExternalActionRequest) -> MessageLogRecord:
    return MessageLogRecord(
        session_id=request.target_session_id,
        platform_msg_id="platform-message-1",
        sender_id="bot-a",
        sender_name="ShinBot",
        content_json='[{"type":"text","text":"hello"}]',
        raw_text="hello",
        role="assistant",
        is_read=True,
        created_at=100_000.0,
    )


def _canonical_payload(request: ExternalActionRequest) -> str:
    return json.dumps(
        request.to_effect_payload(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


async def _seed_and_claim_effect(
    database: DatabaseManager,
    effect_store: SQLiteDurableEffectStore,
    request: ExternalActionRequest,
    *,
    worker_id: str,
    now: float,
) -> ClaimedEffect:
    effect = materialize_external_action_effect(
        key=request.key,
        ownership_generation=request.ownership_generation,
        operation_id=request.operation_id,
        source_event_id=request.source_event_id,
        instance_id=request.instance_id,
        target_session_id=request.target_session_id,
        intent=request.intent,
    )
    assert effect.contract_version == request.contract_version
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, started_at
            ) VALUES (?, ?, ?, ?, 'active_chat_round', 'completed', ?, ?)
            """,
            (
                request.operation_id,
                request.key.profile_id,
                request.key.session_id,
                request.ownership_generation,
                request.source_event_id,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '',
                      NULL, ?, ?, NULL, '')
            """,
            (
                effect.effect_id,
                effect.idempotency_key,
                request.key.profile_id,
                request.key.session_id,
                request.ownership_generation,
                request.source_event_id,
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
            ),
        )
    claim = await effect_store.claim_next(
        worker_id=worker_id,
        effect_contracts=((request.intent.kind.value, request.contract_version),),
    )
    assert claim is not None
    assert claim.effect.effect_id == request.effect_id
    return claim


async def _release_and_reclaim_effect(
    effect_store: SQLiteDurableEffectStore,
    claim: ClaimedEffect,
    request: ExternalActionRequest,
    *,
    worker_id: str,
    now: float,
) -> ClaimedEffect:
    await effect_store.release_for_retry(
        claim,
        error="pre-dispatch action retry",
        available_at=now,
    )
    reclaimed = await effect_store.claim_next(
        worker_id=worker_id,
        effect_contracts=((request.intent.kind.value, request.contract_version),),
    )
    assert reclaimed is not None
    assert reclaimed.effect.effect_id == request.effect_id
    return reclaimed


def _rewrite_pending_effect(
    database: DatabaseManager,
    request: ExternalActionRequest,
    *,
    now: float,
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations
            SET profile_id = ?, session_id = ?, ownership_generation = ?,
                status = 'completed'
            WHERE operation_id = ?
            """,
            (
                request.key.profile_id,
                request.key.session_id,
                request.ownership_generation,
                request.operation_id,
            ),
        )
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET idempotency_key = ?, profile_id = ?, session_id = ?,
                ownership_generation = ?, event_id = ?, operation_id = ?,
                kind = ?, contract_version = ?, contract_signature = ?,
                payload_json = ?, status = 'pending', claim_id = '',
                lease_owner = '', lease_until = NULL, available_at = ?,
                updated_at = ?
            WHERE effect_id = ?
            """,
            (
                request.idempotency_key,
                request.key.profile_id,
                request.key.session_id,
                request.ownership_generation,
                request.source_event_id,
                request.operation_id,
                request.intent.kind.value,
                request.contract_version,
                _EFFECT_CONTRACT_SIGNATURE,
                _canonical_payload(request),
                now,
                now,
                request.effect_id,
            ),
        )


def _advance_generation_for_test(
    database: DatabaseManager,
    request: ExternalActionRequest,
    *,
    generation: int,
    now: float,
) -> ExternalActionRequest:
    next_request = replace(request, ownership_generation=generation)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_runtime_ownership
            SET generation = ?, updated_at = ?
            WHERE profile_id = ? AND session_id = ?
            """,
            (generation, now, request.key.profile_id, request.key.session_id),
        )
        conn.execute(
            """
            UPDATE agent_session_aggregates SET ownership_generation = ?, updated_at = ?
            WHERE profile_id = ? AND session_id = ?
            """,
            (generation, now, request.key.profile_id, request.key.session_id),
        )
    _rewrite_pending_effect(database, next_request, now=now)
    return next_request


async def _claim_existing_effect(
    effect_store: SQLiteDurableEffectStore,
    request: ExternalActionRequest,
    *,
    worker_id: str,
) -> ClaimedEffect:
    claim = await effect_store.claim_next(
        worker_id=worker_id,
        effect_contracts=((request.intent.kind.value, request.contract_version),),
    )
    assert claim is not None
    assert claim.effect.effect_id == request.effect_id
    return claim


def test_fresh_database_installs_external_action_receipt_schema(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()

    with database.connect() as conn:
        tables = {
            str(row["name"])
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name LIKE 'agent_external_action_%'
                """
            ).fetchall()
        }
        receipt_indexes = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA index_list('agent_external_action_receipts')"
            ).fetchall()
        }
        attempt_indexes = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA index_list('agent_external_action_attempts')"
            ).fetchall()
        }

    assert tables == {
        "agent_external_action_receipts",
        "agent_external_action_attempts",
    }
    assert "idx_external_action_receipts_owner_status" in receipt_indexes
    assert "idx_external_action_receipts_claim" in receipt_indexes
    assert "idx_external_action_attempts_receipt" in attempt_indexes
    assert "idx_external_action_attempts_status" in attempt_indexes


def test_pre_receipt_database_upgrade_adds_tables_without_data_loss(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    with database.connect() as conn:
        message_log_id = conn.execute(
            """
            INSERT INTO message_logs (
                session_id, platform_msg_id, role, raw_text, created_at
            ) VALUES ('legacy-session', 'legacy-message', 'user',
                      'must survive migration', 1.0)
            """
        ).lastrowid
        assert message_log_id is not None
        conn.execute("DROP TABLE agent_external_action_attempts")
        conn.execute("DROP TABLE agent_external_action_receipts")

    restarted = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted.initialize()

    with restarted.connect() as conn:
        restored = conn.execute(
            "SELECT raw_text FROM message_logs WHERE id = ?",
            (message_log_id,),
        ).fetchone()
        receipt_table = conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_external_action_receipts'
            """
        ).fetchone()
        attempt_table = conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_external_action_attempts'
            """
        ).fetchone()
        routing_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info('message_routing_jobs')")
        }

    assert restored is not None
    assert restored["raw_text"] == "must survive migration"
    assert receipt_table is not None
    assert attempt_table is not None
    assert {"profile_id", "session_id", "ownership_generation"} <= routing_columns


@pytest.mark.asyncio
async def test_same_request_prepare_dedupes_across_store_restart(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )

    first = await store.prepare(request, effect_claim=effect_claim)
    replay = await store.prepare(request, effect_claim=effect_claim)
    restarted = SQLiteExternalActionReceiptStore(
        DatabaseManager.from_bootstrap(data_dir=tmp_path),
        lease_seconds=5.0,
        clock=lambda: now[0],
    )
    after_restart = await restarted.prepare(request, effect_claim=effect_claim)

    assert first == replay == after_restart
    with database.connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM agent_external_action_receipts"
        ).fetchone()[0]
    assert count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["version", "payload", "provenance"])
async def test_same_logical_key_rejects_changed_exact_request(
    tmp_path: Path,
    change: str,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key)
    first_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=first_claim)
    await effect_store.release_for_retry(
        first_claim,
        error="rewrite exact request",
        available_at=now[0],
    )
    if change == "version":
        changed = replace(request, contract_version=2)
    elif change == "payload":
        changed = _request(key, payload={"text": "changed"})
    else:
        changed = replace(request, source_event_id="different-completion")
    _rewrite_pending_effect(database, changed, now=now[0])
    changed_claim = await _claim_existing_effect(
        effect_store,
        changed,
        worker_id="worker-b",
    )

    assert changed.idempotency_key == request.idempotency_key
    with pytest.raises(ExternalActionConflict, match="different action request"):
        await store.prepare(changed, effect_claim=changed_claim)


@pytest.mark.asyncio
async def test_prepare_crash_allows_fresh_effect_claim_to_begin(tmp_path: Path) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    prepared = await store.prepare(request, effect_claim=first_effect_claim)

    now[0] = 106.0
    second_effect_claim = await _claim_existing_effect(
        effect_store,
        request,
        worker_id="worker-b",
    )
    replay = await store.prepare(request, effect_claim=second_effect_claim)
    action_claim = await store.begin_execution(
        request,
        effect_claim=second_effect_claim,
    )

    assert replay == prepared
    assert action_claim is not None
    assert action_claim.claim_id == second_effect_claim.claim_id
    assert action_claim.worker_id == second_effect_claim.worker_id
    assert action_claim.lease_expires_at <= second_effect_claim.lease_expires_at


@pytest.mark.asyncio
async def test_expired_dispatch_window_becomes_unknown_and_is_not_reclaimed(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=first_effect_claim)
    first_action_claim = await store.begin_execution(
        request,
        effect_claim=first_effect_claim,
    )
    assert first_action_claim is not None

    now[0] = 106.0
    second_effect_claim = await _claim_existing_effect(
        effect_store,
        request,
        worker_id="worker-b",
    )
    restarted = SQLiteExternalActionReceiptStore(
        DatabaseManager.from_bootstrap(data_dir=tmp_path),
        lease_seconds=5.0,
        clock=lambda: now[0],
    )
    terminal = await restarted.begin_execution(
        request,
        effect_claim=second_effect_claim,
    )
    assert isinstance(terminal, ExternalActionTerminalResult)
    assert terminal.reason_code == "execution_lease_expired"
    assert terminal.receipt.status is ExternalActionReceiptStatus.UNKNOWN
    restored = await restarted.get(key, request.idempotency_key)

    assert restored is not None
    assert restored.status is ExternalActionReceiptStatus.UNKNOWN
    assert restored.attempt_count == 1
    with database.connect() as conn:
        attempts = conn.execute(
            """
            SELECT status, claim_id FROM agent_external_action_attempts
            WHERE idempotency_key = ? ORDER BY attempt_count
            """,
            (request.idempotency_key,),
        ).fetchall()
    assert [tuple(row) for row in attempts] == [
        ("unknown", first_action_claim.claim_id)
    ]


@pytest.mark.asyncio
async def test_shutdown_reclaim_settles_unknown_and_old_claim_can_report_late_success(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=first_effect_claim)
    old_action_claim = await store.begin_execution(
        request,
        effect_claim=first_effect_claim,
    )
    assert old_action_claim is not None
    await effect_store.release(
        first_effect_claim,
        error="effect_executor_shutdown",
    )
    new_effect_claim = await _claim_existing_effect(
        effect_store,
        request,
        worker_id="worker-b",
    )
    terminal = await store.begin_execution(
        request,
        effect_claim=new_effect_claim,
    )
    assert isinstance(terminal, ExternalActionTerminalResult)
    assert terminal.reason_code == "outer_effect_claim_reclaimed"
    assert terminal.receipt.status is ExternalActionReceiptStatus.UNKNOWN
    assert terminal.receipt.claim_id == old_action_claim.claim_id
    evidence = json.loads(terminal.receipt.unknown_json)["evidence"]
    assert evidence["previous_action_claim_id"] == old_action_claim.claim_id
    assert evidence["current_effect_claim_id"] == new_effect_claim.claim_id
    with database.connect() as conn:
        attempts = conn.execute(
            """
            SELECT attempt_count, claim_id, status
            FROM agent_external_action_attempts
            WHERE idempotency_key = ? ORDER BY attempt_count
            """,
            (request.idempotency_key,),
        ).fetchall()
    assert [tuple(row) for row in attempts] == [
        (1, old_action_claim.claim_id, "unknown")
    ]

    settled = await store.settle_succeeded(
        old_action_claim,
        platform_result={"platform_message_id": "late-result"},
    )

    assert settled.status is ExternalActionReceiptStatus.SUCCEEDED
    assert settled.claim_id == old_action_claim.claim_id


@pytest.mark.asyncio
async def test_same_outer_claim_begin_is_idempotent_without_receipt_mutation(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    action_claim = await store.begin_execution(
        request,
        effect_claim=effect_claim,
    )
    assert action_claim is not None

    replay = await store.begin_execution(request, effect_claim=effect_claim)
    restored = await store.get(key, request.idempotency_key)

    assert replay is None
    assert restored is not None
    assert restored.status is ExternalActionReceiptStatus.EXECUTING
    assert restored.claim_id == action_claim.claim_id
    assert restored.attempt_count == 1
    with database.connect() as conn:
        attempt_count = conn.execute(
            """
            SELECT COUNT(*) AS count FROM agent_external_action_attempts
            WHERE idempotency_key = ?
            """,
            (request.idempotency_key,),
        ).fetchone()
    assert attempt_count is not None
    assert int(attempt_count["count"]) == 1


@pytest.mark.asyncio
async def test_rejected_attempt_retries_only_with_fresh_outer_claim(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_REACTION)
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=first_effect_claim)
    first = await store.begin_execution(request, effect_claim=first_effect_claim)
    assert first is not None
    rejected = await store.reject_before_dispatch(
        first,
        reason_code="adapter_unavailable",
    )
    with pytest.raises(ExternalActionEffectClaimLost, match="fresh durable effect"):
        await store.begin_execution(request, effect_claim=first_effect_claim)
    second_effect_claim = await _release_and_reclaim_effect(
        effect_store,
        first_effect_claim,
        request,
        worker_id="worker-b",
        now=now[0],
    )
    second = await store.begin_execution(request, effect_claim=second_effect_claim)

    assert rejected.status is ExternalActionReceiptStatus.REJECTED_BEFORE_DISPATCH
    assert second is not None
    assert second.claim_id == second_effect_claim.claim_id
    assert second.claim_id != first.claim_id
    assert second.attempt_count == 2
    with database.connect() as conn:
        attempts = conn.execute(
            """
            SELECT attempt_count, status FROM agent_external_action_attempts
            WHERE idempotency_key = ? ORDER BY attempt_count
            """,
            (request.idempotency_key,),
        ).fetchall()
    assert [tuple(row) for row in attempts] == [
        (1, "rejected_before_dispatch"),
        (2, "executing"),
    ]


@pytest.mark.asyncio
async def test_new_outer_claim_fences_stale_aba_settlement(tmp_path: Path) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="same-worker",
        now=now[0],
    )
    await store.prepare(request, effect_claim=first_effect_claim)
    first = await store.begin_execution(request, effect_claim=first_effect_claim)
    assert first is not None
    await store.reject_before_dispatch(first, reason_code="preflight_failed")
    second_effect_claim = await _release_and_reclaim_effect(
        effect_store,
        first_effect_claim,
        request,
        worker_id="same-worker",
        now=now[0],
    )
    second = await store.begin_execution(request, effect_claim=second_effect_claim)
    assert second is not None

    with pytest.raises(ExternalActionClaimLost, match="no longer owned"):
        await store.mark_unknown(first, reason_code="stale-result")
    settled = await store.settle_succeeded(second, platform_result={"ok": True})
    assert settled.status is ExternalActionReceiptStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_receipt_renewal_never_outlives_outer_effect_lease(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    action_claim = await store.begin_execution(request, effect_claim=effect_claim)
    assert action_claim is not None
    now[0] = 102.0

    capped = await store.renew_lease(action_claim, effect_claim=effect_claim)
    renewed_effect = await effect_store.renew_lease(effect_claim)
    renewed_action = await store.renew_lease(
        capped,
        effect_claim=renewed_effect,
    )

    assert capped.lease_expires_at == effect_claim.lease_expires_at
    assert renewed_action.lease_expires_at == renewed_effect.lease_expires_at


@pytest.mark.asyncio
async def test_reply_success_atomically_writes_assistant_log_and_receipt(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    claim = await store.begin_execution(request, effect_claim=effect_claim)
    assert claim is not None
    message = _assistant_message(request)

    receipt = await store.settle_succeeded(
        claim,
        platform_result={"platform_message_id": message.platform_msg_id},
        assistant_message=message,
    )

    assert receipt.status is ExternalActionReceiptStatus.SUCCEEDED
    assert receipt.assistant_message_log_id is not None
    persisted = database.message_logs.get(receipt.assistant_message_log_id)
    assert persisted is not None
    assert persisted["role"] == "assistant"
    assert persisted["session_id"] == request.target_session_id


@pytest.mark.asyncio
async def test_receipt_failure_rolls_back_assistant_log_and_attempt(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    claim = await store.begin_execution(request, effect_claim=effect_claim)
    assert claim is not None
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER fail_external_action_success
            BEFORE UPDATE OF status ON agent_external_action_receipts
            WHEN NEW.status = 'succeeded'
            BEGIN
                SELECT RAISE(ABORT, 'forced receipt failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="forced receipt failure"):
        await store.settle_succeeded(
            claim,
            platform_result={"platform_message_id": "platform-message-1"},
            assistant_message=_assistant_message(request),
        )

    with database.connect() as conn:
        log_count = conn.execute(
            "SELECT COUNT(*) FROM message_logs WHERE role = 'assistant'"
        ).fetchone()[0]
        receipt_status = conn.execute(
            """
            SELECT status FROM agent_external_action_receipts
            WHERE idempotency_key = ?
            """,
            (request.idempotency_key,),
        ).fetchone()[0]
        attempt_status = conn.execute(
            """
            SELECT status FROM agent_external_action_attempts WHERE claim_id = ?
            """,
            (claim.claim_id,),
        ).fetchone()[0]
    assert (log_count, receipt_status, attempt_status) == (0, "executing", "executing")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [ExternalActionKind.SEND_POKE, ExternalActionKind.SEND_REACTION],
)
async def test_non_reply_success_does_not_create_message_log(
    tmp_path: Path,
    kind: ExternalActionKind,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=kind)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    claim = await store.begin_execution(request, effect_claim=effect_claim)
    assert claim is not None

    receipt = await store.settle_succeeded(claim, platform_result={"ok": True})

    assert receipt.assistant_message_log_id is None
    with database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM message_logs").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_duplicate_success_fails_closed_on_attempt_journal_drift(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    action_claim = await store.begin_execution(request, effect_claim=effect_claim)
    assert action_claim is not None
    await store.settle_succeeded(action_claim, platform_result={"ok": True})
    with database.connect() as conn:
        conn.execute(
            "DELETE FROM agent_external_action_attempts WHERE claim_id = ?",
            (action_claim.claim_id,),
        )

    with pytest.raises(ExternalActionConflict, match="attempt journal"):
        await store.settle_succeeded(action_claim, platform_result={"ok": True})


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["succeeded", "unknown"])
async def test_platform_evidence_settles_after_ownership_generation_changes(
    tmp_path: Path,
    outcome: str,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    action_claim = await store.begin_execution(request, effect_claim=effect_claim)
    assert action_claim is not None
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_runtime_ownership
            SET generation = 2, updated_at = 101.0
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )

    if outcome == "succeeded":
        settled = await store.settle_succeeded(
            action_claim,
            platform_result={"ok": True},
        )
    else:
        settled = await store.mark_unknown(
            action_claim,
            reason_code="lost_ack_after_migration",
        )

    assert settled.status.value == outcome
    assert settled.ownership_generation == 1
    with pytest.raises(ExternalActionOwnershipLost):
        await store.reject_before_dispatch(
            action_claim,
            reason_code="stale_pre_dispatch_result",
        )


@pytest.mark.asyncio
async def test_expired_receipt_recovers_after_abnormal_generation_change(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    action_claim = await store.begin_execution(request, effect_claim=effect_claim)
    assert action_claim is not None
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_runtime_ownership
            SET generation = 2, updated_at = 106.0
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )
    now[0] = 106.0

    assert await store.recover_expired(worker_id="recovery") == 1
    restored = await store.get(key, request.idempotency_key)
    assert restored is not None
    assert restored.status is ExternalActionReceiptStatus.UNKNOWN
    assert restored.ownership_generation == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["succeeded", "unknown"])
async def test_terminal_receipt_dedupes_across_ownership_generations(
    tmp_path: Path,
    terminal_status: str,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=first_effect_claim)
    action_claim = await store.begin_execution(
        request,
        effect_claim=first_effect_claim,
    )
    assert action_claim is not None
    if terminal_status == "succeeded":
        terminal = await store.settle_succeeded(
            action_claim,
            platform_result={"ok": True},
        )
    else:
        terminal = await store.mark_unknown(
            action_claim,
            reason_code="dispatch_result_unknown",
        )
    now[0] = 102.0
    next_request = _advance_generation_for_test(
        database,
        request,
        generation=3,
        now=102.0,
    )
    next_effect_claim = await _claim_existing_effect(
        effect_store,
        next_request,
        worker_id="worker-b",
    )

    replay = await store.prepare(next_request, effect_claim=next_effect_claim)
    next_action = await store.begin_execution(
        next_request,
        effect_claim=next_effect_claim,
    )

    assert replay == terminal
    assert replay.ownership_generation == 1
    assert replay.status.value == terminal_status
    assert isinstance(next_action, ExternalActionTerminalResult)
    assert next_action.receipt == terminal


@pytest.mark.asyncio
async def test_nonterminal_receipt_cannot_cross_ownership_generation(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=first_effect_claim)
    now[0] = 102.0
    next_request = _advance_generation_for_test(
        database,
        request,
        generation=3,
        now=102.0,
    )
    next_effect_claim = await _claim_existing_effect(
        effect_store,
        next_request,
        worker_id="worker-b",
    )

    with pytest.raises(ExternalActionConflict, match="different ownership generation"):
        await store.prepare(next_request, effect_claim=next_effect_claim)


@pytest.mark.asyncio
async def test_prepare_and_begin_require_exact_active_generation(tmp_path: Path) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    stale = replace(request, ownership_generation=2)

    with pytest.raises(ExternalActionOwnershipLost):
        await store.prepare(stale, effect_claim=effect_claim)
    await store.prepare(request, effect_claim=effect_claim)
    with pytest.raises(ExternalActionOwnershipLost):
        await store.begin_execution(stale, effect_claim=effect_claim)


@pytest.mark.asyncio
async def test_effect_claim_provenance_and_operation_status_are_mandatory(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    forged = _request(key, kind=ExternalActionKind.SEND_POKE, payload={"user_id": "7"})

    with pytest.raises(ExternalActionEffectConflict, match="does not match"):
        await store.prepare(forged, effect_claim=effect_claim)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations SET status = 'superseded'
            WHERE operation_id = ?
            """,
            (request.operation_id,),
        )
    with pytest.raises(ExternalActionEffectClaimLost, match="no longer executable"):
        await store.prepare(request, effect_claim=effect_claim)


@pytest.mark.asyncio
async def test_receipts_are_isolated_by_profile_and_session_key(tmp_path: Path) -> None:
    now = [100.0]
    first_key = SessionKey("profile-a", "shared-session")
    database, store, effect_store, _key = await _make_store(
        tmp_path,
        now,
        key=first_key,
    )
    second_key = SessionKey("profile-b", "shared-session")
    await _add_actor_key(database, second_key, now)
    first_request = _request(first_key)
    second_request = _request(
        second_key,
        operation_id="active-chat-round-profile-b",
    )
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        first_request,
        worker_id="worker-a",
        now=now[0],
    )
    second_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        second_request,
        worker_id="worker-b",
        now=now[0],
    )

    first = await store.prepare(first_request, effect_claim=first_effect_claim)
    second = await store.prepare(second_request, effect_claim=second_effect_claim)

    assert first.receipt_seq != second.receipt_seq
    assert first.idempotency_key != second.idempotency_key
    assert await store.get(second_key, first.idempotency_key) is None
    assert await store.get(first_key, second.idempotency_key) is None


@pytest.mark.asyncio
async def test_migration_blocks_live_receipts_but_preserves_terminal_history(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    first_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=first_effect_claim)

    with database.connect() as conn, pytest.raises(ExternalActionMigrationBlocked):
        validate_external_action_migration(conn, key)
    first = await store.begin_execution(request, effect_claim=first_effect_claim)
    assert first is not None
    with database.connect() as conn, pytest.raises(ExternalActionMigrationBlocked):
        validate_external_action_migration(conn, key)
    await store.reject_before_dispatch(first, reason_code="preflight_failed")
    with database.connect() as conn, pytest.raises(ExternalActionMigrationBlocked):
        validate_external_action_migration(conn, key)
    second_effect_claim = await _release_and_reclaim_effect(
        effect_store,
        first_effect_claim,
        request,
        worker_id="worker-b",
        now=now[0],
    )
    second = await store.begin_execution(request, effect_claim=second_effect_claim)
    assert second is not None
    await store.mark_unknown(second, reason_code="dispatch_result_unknown")

    succeeded_request = _request(
        key,
        kind=ExternalActionKind.SEND_REACTION,
        operation_id="active-chat-round-8",
        tool_call_id="tool-call-2",
    )
    succeeded_effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        succeeded_request,
        worker_id="worker-c",
        now=now[0],
    )
    await store.prepare(succeeded_request, effect_claim=succeeded_effect_claim)
    succeeded_claim = await store.begin_execution(
        succeeded_request,
        effect_claim=succeeded_effect_claim,
    )
    assert succeeded_claim is not None
    await store.settle_succeeded(succeeded_claim, platform_result={"ok": True})

    with database.connect() as conn:
        validate_external_action_migration(conn, key)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "live_status",
    ["prepared", "executing", "rejected_before_dispatch"],
)
async def test_ownership_migration_atomically_rejects_live_receipts(
    tmp_path: Path,
    live_status: str,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    if live_status != "prepared":
        action_claim = await store.begin_execution(
            request,
            effect_claim=effect_claim,
        )
        assert action_claim is not None
        if live_status == "rejected_before_dispatch":
            await store.reject_before_dispatch(
                action_claim,
                reason_code="preflight_failed",
            )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="external-action receipts",
    ):
        database.agent_runtime_ownership.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=1,
            reason="must not cross a live external action",
        )

    ownership = database.agent_runtime_ownership.get(key)
    assert ownership is not None
    assert ownership.generation == 1
    assert ownership.actor_v2_active is True


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["succeeded", "unknown"])
async def test_terminal_receipts_remain_on_original_generation_during_abort(
    tmp_path: Path,
    terminal_status: str,
) -> None:
    now = [100.0]
    database, store, effect_store, key = await _make_store(tmp_path, now)
    request = _request(key, kind=ExternalActionKind.SEND_POKE)
    effect_claim = await _seed_and_claim_effect(
        database,
        effect_store,
        request,
        worker_id="worker-a",
        now=now[0],
    )
    await store.prepare(request, effect_claim=effect_claim)
    action_claim = await store.begin_execution(request, effect_claim=effect_claim)
    assert action_claim is not None
    if terminal_status == "succeeded":
        await store.settle_succeeded(action_claim, platform_result={"ok": True})
    else:
        await store.mark_unknown(
            action_claim,
            reason_code="dispatch_result_unknown",
        )

    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=1,
        reason="exercise terminal receipt migration",
    )
    aborted = database.agent_runtime_ownership.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="return to actor ownership",
    )
    receipt = await store.get(key, request.idempotency_key)

    assert aborted.generation == 3
    assert receipt is not None
    assert receipt.status.value == terminal_status
    assert receipt.ownership_generation == 1
