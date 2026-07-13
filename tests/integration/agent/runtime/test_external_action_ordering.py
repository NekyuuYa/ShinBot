"""Integration coverage for durable external-action ordering gates."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.external_action_store import (
    ClaimedExternalAction,
    ExternalActionConflict,
    ExternalActionOrderBlockedResult,
    SQLiteExternalActionReceiptStore,
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
    AgentRuntimeOwnershipMode,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.agent_external_action_reconciliation import (
    reconcile_abandoned_before_dispatch_receipts,
)


async def _make_stores(
    tmp_path: Path,
    now: list[float],
) -> tuple[
    DatabaseManager,
    SQLiteDurableEffectStore,
    SQLiteExternalActionReceiptStore,
    SessionKey,
]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-a", "profile-a:group:room")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="external action ordering test",
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
        ),
        SQLiteExternalActionReceiptStore(
            database,
            lease_seconds=5.0,
            clock=lambda: now[0],
        ),
        key,
    )


def _request(
    key: SessionKey,
    *,
    operation_id: str,
    action_ordinal: int,
    kind: ExternalActionKind = ExternalActionKind.SEND_POKE,
    tool_call_id: str | None = None,
) -> ExternalActionRequest:
    return ExternalActionRequest(
        key=key,
        ownership_generation=1,
        operation_id=operation_id,
        source_event_id=f"{operation_id}-completed",
        instance_id="adapter-a",
        target_session_id="adapter-a:group:room",
        intent=ExternalActionIntent(
            kind=kind,
            tool_call_id=tool_call_id or f"{operation_id}-tool-{action_ordinal}",
            action_ordinal=action_ordinal,
            payload={"ordinal": action_ordinal},
        ),
    )


def _materialized_effect(request: ExternalActionRequest):
    return materialize_external_action_effect(
        key=request.key,
        ownership_generation=request.ownership_generation,
        operation_id=request.operation_id,
        source_event_id=request.source_event_id,
        instance_id=request.instance_id,
        target_session_id=request.target_session_id,
        intent=request.intent,
    )


def _insert_pending_effect(
    database: DatabaseManager,
    request: ExternalActionRequest,
    *,
    now: float,
) -> None:
    effect = _materialized_effect(request)
    payload_json = json.dumps(
        effect.payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO agent_session_operations (
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
                request.operation_id,
                effect.kind,
                effect.contract_version,
                effect.contract_signature,
                payload_json,
                now,
                now,
                now,
            ),
        )


def _force_effect_claim(
    database: DatabaseManager,
    request: ExternalActionRequest,
    *,
    claim_id: str,
    worker_id: str,
    now: float,
) -> ClaimedEffect:
    """Construct a valid outer claim to exercise receipt-side defense in depth."""

    effect = _materialized_effect(request)
    lease_until = now + 5.0
    with database.connect() as conn:
        updated = conn.execute(
            """
            UPDATE agent_effect_outbox
            SET status = 'processing', attempt_count = 1, claim_id = ?,
                lease_owner = ?, lease_until = ?, updated_at = ?
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
              AND status = 'pending'
            """,
            (
                claim_id,
                worker_id,
                lease_until,
                now,
                request.key.profile_id,
                request.key.session_id,
                effect.effect_id,
            ),
        )
    assert updated.rowcount == 1
    envelope = DurableEffectEnvelope(
        effect_id=effect.effect_id,
        key=request.key,
        kind=effect.kind,
        idempotency_key=effect.idempotency_key,
        ownership_generation=request.ownership_generation,
        contract_version=effect.contract_version,
        contract_signature=effect.contract_signature,
        payload=effect.payload,
        source_event_id=request.source_event_id,
        operation_id=request.operation_id,
        available_at=now,
        created_at=now,
    )
    return ClaimedEffect(
        claim_id=claim_id,
        effect=envelope,
        worker_id=worker_id,
        attempt_count=1,
        claimed_at=now,
        lease_expires_at=lease_until,
    )


@pytest.mark.asyncio
async def test_effect_claims_follow_action_ordinals_not_outbox_insertion_order(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, effect_store, receipt_store, key = await _make_stores(tmp_path, now)
    first = _request(key, operation_id="operation-ordered", action_ordinal=0)
    follower = _request(key, operation_id="operation-ordered", action_ordinal=1)

    # The follower has the earlier outbox sequence, so ordinary FIFO selection
    # would claim it before its required predecessor.
    _insert_pending_effect(database, follower, now=now[0])
    _insert_pending_effect(database, first, now=now[0])

    first_effect_claim = await effect_store.claim_next(worker_id="worker-a")

    assert first_effect_claim is not None
    assert first_effect_claim.effect.effect_id == first.effect_id
    await receipt_store.prepare(first, effect_claim=first_effect_claim)
    first_action_claim = await receipt_store.begin_execution(
        first,
        effect_claim=first_effect_claim,
    )
    assert first_action_claim is not None
    assert isinstance(first_action_claim, ClaimedExternalAction)
    await receipt_store.settle_succeeded(
        first_action_claim,
        platform_result={"ok": True},
    )

    follower_effect_claim = await effect_store.claim_next(worker_id="worker-b")

    assert follower_effect_claim is not None
    assert follower_effect_claim.effect.effect_id == follower.effect_id


@pytest.mark.asyncio
async def test_blocked_operation_does_not_serialize_another_operation(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, effect_store, _receipt_store, key = await _make_stores(tmp_path, now)
    blocked_follower = _request(
        key,
        operation_id="operation-blocked",
        action_ordinal=1,
    )
    unrelated_first = _request(
        key,
        operation_id="operation-independent",
        action_ordinal=0,
    )

    _insert_pending_effect(database, blocked_follower, now=now[0])
    _insert_pending_effect(database, unrelated_first, now=now[0])

    claim = await effect_store.claim_next(worker_id="worker-a")

    assert claim is not None
    assert claim.effect.effect_id == unrelated_first.effect_id


@pytest.mark.asyncio
async def test_malformed_action_effect_payload_is_quarantined_without_stalling_worker(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, effect_store, _receipt_store, key = await _make_stores(tmp_path, now)
    with database.connect() as conn:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.executemany(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, 1, 'event:fixture', ?, ?, 1, 'contract-v1',
                      ?, 'pending', 0, ?, '', '', NULL, ?, ?, NULL, '')
            """,
            [
                (
                    "malformed-action-effect",
                    "malformed-action-key",
                    key.profile_id,
                    key.session_id,
                    "malformed-operation",
                    ExternalActionKind.SEND_POKE.value,
                    "not-json",
                    now[0],
                    now[0],
                    now[0],
                ),
                (
                    "unrelated-effect",
                    "unrelated-key",
                    key.profile_id,
                    key.session_id,
                    "",
                    "unrelated_effect",
                    "{}",
                    now[0],
                    now[0],
                    now[0],
                ),
            ],
        )

    claim = await effect_store.claim_next(
        worker_id="worker-a",
        effect_contracts=(("unrelated_effect", 1),),
    )

    assert claim is not None
    assert claim.effect.effect_id == "unrelated-effect"
    notifications = await effect_store.drain_quarantine_notifications()
    assert len(notifications) == 1
    assert notifications[0].key == key
    with database.connect() as conn:
        malformed = conn.execute(
            """
            SELECT status, attempt_count, claim_id, lease_owner, lease_until
            FROM agent_effect_outbox
            WHERE effect_id = 'malformed-action-effect'
            """
        ).fetchone()
        event = conn.execute(
            """
            SELECT kind, source, payload_json
            FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()
    assert malformed is not None
    assert tuple(malformed) == ("failed", 0, "", "", None)
    assert event is not None
    assert tuple(event)[:2] == ("EffectQuarantined", "effect_store")
    payload = json.loads(str(event["payload_json"]))
    assert payload["failure_code"] == "malformed_effect_row"
    raw_prefix = base64.b64decode(
        payload["raw_row"]["payload_json"]["prefix_base64"]
    ).decode("utf-8")
    assert raw_prefix == "not-json"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "expected_violation"),
    (
        ("operation_id", "external_action_operation_id_invalid"),
        ("action_ordinal", "external_action_action_ordinal_invalid"),
        ("request_digest", "external_action_request_digest_invalid"),
    ),
)
async def test_semantically_malformed_action_is_quarantined_before_order_gate(
    tmp_path: Path,
    mutation: str,
    expected_violation: str,
) -> None:
    now = [100.0]
    database, effect_store, _receipt_store, key = await _make_stores(tmp_path, now)
    malformed_request = _request(
        key,
        operation_id="operation-semantic",
        action_ordinal=0,
        tool_call_id="malformed-tool",
    )
    following_request = _request(
        key,
        operation_id="operation-semantic",
        action_ordinal=0,
        tool_call_id="following-tool",
    )
    _insert_pending_effect(database, malformed_request, now=now[0])
    _insert_pending_effect(database, following_request, now=now[0])
    malformed_effect = _materialized_effect(malformed_request)
    with database.connect() as conn:
        payload = dict(malformed_effect.payload)
        operation_id = malformed_request.operation_id
        if mutation == "operation_id":
            operation_id = ""
        else:
            payload.pop(mutation)
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET operation_id = ?, payload_json = ?
            WHERE effect_id = ?
            """,
            (
                operation_id,
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                malformed_effect.effect_id,
            ),
        )

    claim = await effect_store.claim_next(worker_id="worker-a")

    assert claim is not None
    assert claim.effect.effect_id == _materialized_effect(following_request).effect_id
    notifications = await effect_store.drain_quarantine_notifications()
    assert len(notifications) == 1
    with database.connect() as conn:
        malformed = conn.execute(
            """
            SELECT status, attempt_count
            FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            (malformed_effect.effect_id,),
        ).fetchone()
        event = conn.execute(
            """
            SELECT payload_json
            FROM agent_session_mailbox
            WHERE kind = 'EffectQuarantined'
            """
        ).fetchone()
    assert tuple(malformed) == ("failed", 0)
    event_payload = json.loads(str(event["payload_json"]))
    assert expected_violation in event_payload["violations"]


@pytest.mark.asyncio
async def test_external_action_contract_drift_is_left_to_executor_policy(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, effect_store, _receipt_store, key = await _make_stores(tmp_path, now)
    request = _request(
        key,
        operation_id="operation-drift",
        action_ordinal=0,
    )
    _insert_pending_effect(database, request, now=now[0])
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET contract_signature = 'drifted-signature'
            """
        )

    claim = await effect_store.claim_next(worker_id="worker-a")

    assert claim is not None
    assert claim.effect.contract_signature == "drifted-signature"
    assert await effect_store.drain_quarantine_notifications() == ()


@pytest.mark.asyncio
async def test_duplicate_action_ordinal_is_a_durable_conflict(
    tmp_path: Path,
) -> None:
    now = [100.0]
    database, effect_store, receipt_store, key = await _make_stores(tmp_path, now)
    first = _request(
        key,
        operation_id="operation-duplicate-ordinal",
        action_ordinal=0,
        kind=ExternalActionKind.SEND_POKE,
    )
    duplicate = _request(
        key,
        operation_id="operation-duplicate-ordinal",
        action_ordinal=0,
        kind=ExternalActionKind.SEND_REACTION,
    )
    _insert_pending_effect(database, first, now=now[0])
    _insert_pending_effect(database, duplicate, now=now[0])

    first_effect_claim = await effect_store.claim_next(worker_id="worker-a")

    assert first_effect_claim is not None
    assert first_effect_claim.effect.effect_id == first.effect_id
    await receipt_store.prepare(first, effect_claim=first_effect_claim)
    # The sibling effect remains pending rather than obtaining an adapter lease.
    assert await effect_store.claim_next(worker_id="worker-b") is None

    forced_duplicate_claim = _force_effect_claim(
        database,
        duplicate,
        claim_id="forced-duplicate-claim",
        worker_id="worker-b",
        now=now[0],
    )
    with pytest.raises(ExternalActionConflict, match="ordinal is already bound"):
        await receipt_store.prepare(duplicate, effect_claim=forced_duplicate_claim)


def test_receipt_upgrade_backfills_action_ordinal_and_preserves_it(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    request_json = json.dumps(
        {
            "action_ordinal": 3,
            "operation_id": "legacy-operation",
            "source_event_id": "legacy-event",
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    with database.connect() as conn:
        conn.execute("DROP TABLE agent_external_action_attempts")
        conn.execute("DROP TABLE agent_external_action_receipts")
        conn.execute(
            """
            CREATE TABLE agent_external_action_receipts (
                receipt_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT NOT NULL UNIQUE,
                effect_id TEXT NOT NULL UNIQUE,
                operation_id TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                ownership_generation INTEGER NOT NULL,
                action_kind TEXT NOT NULL,
                contract_version INTEGER NOT NULL,
                request_digest TEXT NOT NULL,
                request_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'prepared',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                claim_id TEXT NOT NULL DEFAULT '',
                lease_owner TEXT NOT NULL DEFAULT '',
                lease_until REAL,
                platform_result_json TEXT NOT NULL DEFAULT '{}',
                rejection_json TEXT NOT NULL DEFAULT '{}',
                unknown_json TEXT NOT NULL DEFAULT '{}',
                assistant_message_log_id INTEGER,
                prepared_at REAL NOT NULL,
                execution_started_at REAL,
                settled_at REAL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO agent_external_action_receipts (
                idempotency_key, effect_id, operation_id, profile_id,
                session_id, ownership_generation, action_kind,
                contract_version, request_digest, request_json, status,
                attempt_count, claim_id, lease_owner, lease_until,
                platform_result_json, rejection_json, unknown_json,
                assistant_message_log_id, prepared_at,
                execution_started_at, settled_at, updated_at
            ) VALUES (
                'legacy-key', 'legacy-effect', 'legacy-operation',
                'profile-a', 'profile-a:group:room', 1, 'send_poke', 1, ?, ?,
                'prepared', 0, '', '', NULL, '{}', '{}', '{}', NULL,
                5.0, NULL, NULL, 7.0
            )
            """,
            ("a" * 64, request_json),
        )

    upgraded = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    upgraded.initialize()
    with upgraded.connect() as conn:
        first = conn.execute(
            """
            SELECT action_ordinal, updated_at
            FROM agent_external_action_receipts
            WHERE idempotency_key = 'legacy-key'
            """
        ).fetchone()
        indexes = {
            str(row["name"]): int(row["unique"])
            for row in conn.execute("PRAGMA index_list('agent_external_action_receipts')")
        }

    restarted = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted.initialize()
    with restarted.connect() as conn:
        second = conn.execute(
            """
            SELECT action_ordinal, updated_at
            FROM agent_external_action_receipts
            WHERE idempotency_key = 'legacy-key'
            """
        ).fetchone()

    assert first is not None
    assert second is not None
    assert tuple(first) == (3, 7.0)
    assert tuple(second) == (3, 7.0)
    assert indexes["idx_external_action_receipts_operation_ordinal"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("settlement", "expected_reason"),
    [
        ("unknown", "predecessor_unknown"),
        ("rejected", "predecessor_rejected_before_dispatch"),
        ("abandoned", "predecessor_abandoned_before_dispatch"),
    ],
)
async def test_receipt_gate_exposes_unsucceeded_predecessor(
    tmp_path: Path,
    settlement: str,
    expected_reason: str,
) -> None:
    now = [100.0]
    database, effect_store, receipt_store, key = await _make_stores(tmp_path, now)
    predecessor = _request(
        key,
        operation_id="operation-non-success",
        action_ordinal=0,
    )
    follower = _request(
        key,
        operation_id="operation-non-success",
        action_ordinal=1,
    )
    _insert_pending_effect(database, predecessor, now=now[0])
    _insert_pending_effect(database, follower, now=now[0])

    predecessor_effect_claim = await effect_store.claim_next(worker_id="worker-a")
    assert predecessor_effect_claim is not None
    await receipt_store.prepare(predecessor, effect_claim=predecessor_effect_claim)
    if settlement == "abandoned":
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = 'failed', completed_at = ?, lease_until = NULL
                WHERE effect_id = ?
                """,
                (now[0], predecessor.effect_id),
            )
            assert (
                reconcile_abandoned_before_dispatch_receipts(
                    conn,
                    key,
                    now=now[0],
                )
                == 1
            )
        predecessor_receipt = await receipt_store.get(
            key,
            predecessor.idempotency_key,
        )
        assert predecessor_receipt is not None
    else:
        predecessor_action_claim = await receipt_store.begin_execution(
            predecessor,
            effect_claim=predecessor_effect_claim,
        )
        assert predecessor_action_claim is not None
        assert isinstance(predecessor_action_claim, ClaimedExternalAction)
        if settlement == "unknown":
            predecessor_receipt = await receipt_store.mark_unknown(
                predecessor_action_claim,
                reason_code="adapter_ack_lost",
            )
        else:
            predecessor_receipt = await receipt_store.reject_before_dispatch(
                predecessor_action_claim,
                reason_code="adapter_preflight_rejected",
            )
    expected_status = (
        ExternalActionReceiptStatus.UNKNOWN
        if settlement == "unknown"
        else (
            ExternalActionReceiptStatus.ABANDONED_BEFORE_DISPATCH
            if settlement == "abandoned"
            else ExternalActionReceiptStatus.REJECTED_BEFORE_DISPATCH
        )
    )
    assert predecessor_receipt.status is expected_status

    # Once the outer predecessor effect has settled, a permanently blocked
    # follower must not keep the executor wake loop hot.
    if settlement != "abandoned":
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = 'completed', completed_at = ?, lease_until = NULL
                WHERE effect_id = ?
                """,
                (now[0], predecessor.effect_id),
            )
    assert await effect_store.next_available_at() is None

    # Receipt execution validates the same durable predecessor condition even
    # when a caller somehow presents an otherwise valid outer effect claim.
    follower_effect_claim = _force_effect_claim(
        database,
        follower,
        claim_id="forced-follower-claim",
        worker_id="worker-b",
        now=now[0],
    )
    follower_receipt = await receipt_store.prepare(
        follower,
        effect_claim=follower_effect_claim,
    )
    result = await receipt_store.begin_execution(
        follower,
        effect_claim=follower_effect_claim,
    )

    assert isinstance(result, ExternalActionOrderBlockedResult)
    assert result.reason_code == expected_reason
    assert result.receipt == follower_receipt
    assert result.predecessor == predecessor_receipt
    assert result.receipt.status is ExternalActionReceiptStatus.PREPARED
    with database.connect() as conn:
        attempt_count = conn.execute(
            """
            SELECT COUNT(*) FROM agent_external_action_attempts
            WHERE idempotency_key = ?
            """,
            (follower.idempotency_key,),
        ).fetchone()[0]
    assert attempt_count == 0
