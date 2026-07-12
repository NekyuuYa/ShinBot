"""Integration coverage for terminal pre-dispatch external-action abandonment."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.external_action_store import (
    ExternalActionMigrationBlocked,
    validate_external_action_migration,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMigrationConflict,
    AgentRuntimeOwnershipMode,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.agent_external_action_reconciliation import (
    reconcile_abandoned_before_dispatch_receipts,
)
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)


def _database(
    tmp_path: Path,
) -> tuple[DatabaseManager, AgentRuntimeOwnershipRepository, SessionKey]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    repository = AgentRuntimeOwnershipRepository(database, clock=lambda: 50.0)
    key = SessionKey("profile-a", "profile-a:group:room")
    ownership = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="external action abandonment fixture",
    ).ownership
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 1.0, 1.0)
            """,
            (key.profile_id, key.session_id, ownership.generation),
        )
    return database, repository, key


def _seed_receipt(
    database: DatabaseManager,
    key: SessionKey,
    *,
    status: str,
    effect_status: str = "failed",
    payload_matches_effect: bool = True,
    action_ordinal: int = 0,
) -> tuple[str, str]:
    idempotency_key = f"external-action-idempotency:{status}:{action_ordinal}"
    effect_id = f"external-action:{status}:{action_ordinal}"
    operation_id = f"operation:{status}:{action_ordinal}"
    request_payload = {
        "action_ordinal": action_ordinal,
        "operation_id": operation_id,
        "source_event_id": "event:fixture",
    }
    effect_payload = (
        request_payload
        if payload_matches_effect
        else {**request_payload, "source_event_id": "event:tampered"}
    )
    request_json = json.dumps(
        request_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    effect_json = json.dumps(
        effect_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    attempt_count = 0 if status == "prepared" else 1
    claim_id = "" if status == "prepared" else f"claim:{status}:{action_ordinal}"
    lease_owner = "" if status == "prepared" else "worker:fixture"
    lease_until = 80.0 if status == "executing" else None
    execution_started_at = None if status == "prepared" else 2.0
    settled_at = None if status in {"prepared", "executing"} else 3.0
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, 1, 'event:fixture', ?, 'send_poke', 1,
                      'external-action-contract-v1', ?, ?, 5, 1.0, '', '',
                      NULL, 1.0, 4.0, 4.0, 'retry_exhausted')
            """,
            (
                effect_id,
                idempotency_key,
                key.profile_id,
                key.session_id,
                operation_id,
                effect_json,
                effect_status,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_external_action_receipts (
                idempotency_key, effect_id, operation_id, profile_id,
                session_id, ownership_generation, action_ordinal, action_kind,
                contract_version, request_digest, request_json, status,
                attempt_count, claim_id, lease_owner, lease_until,
                platform_result_json, rejection_json, unknown_json,
                assistant_message_log_id, prepared_at,
                execution_started_at, settled_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 1, ?, 'send_poke', 1, ?, ?, ?, ?, ?, ?, ?,
                      '{}', '{}', '{}', NULL, 1.0, ?, ?, 4.0)
            """,
            (
                idempotency_key,
                effect_id,
                operation_id,
                key.profile_id,
                key.session_id,
                action_ordinal,
                "a" * 64,
                request_json,
                status,
                attempt_count,
                claim_id,
                lease_owner,
                lease_until,
                execution_started_at,
                settled_at,
            ),
        )
        if status in {"executing", "rejected_before_dispatch", "unknown"}:
            attempt_settled_at = None if status == "executing" else 3.0
            conn.execute(
                """
                INSERT INTO agent_external_action_attempts (
                    idempotency_key, attempt_count, claim_id, lease_owner,
                    claimed_at, lease_until, status, platform_result_json,
                    rejection_json, unknown_json, assistant_message_log_id,
                    settled_at
                ) VALUES (?, 1, ?, ?, 2.0, 20.0, ?, '{}', '{}', '{}', NULL, ?)
                """,
                (
                    idempotency_key,
                    claim_id,
                    lease_owner,
                    status,
                    attempt_settled_at,
                ),
            )
    return idempotency_key, effect_id


def test_failed_effect_abandons_prepared_receipt_across_restart_and_migration(
    tmp_path: Path,
) -> None:
    database, _repository, key = _database(tmp_path)
    idempotency_key, _effect_id = _seed_receipt(
        database,
        key,
        status="prepared",
    )

    restarted = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted.initialize()
    repository = AgentRuntimeOwnershipRepository(restarted, clock=lambda: 50.0)
    migration = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=1,
        reason="failed outer action must not block migration forever",
    )

    assert migration.generation == 2
    with restarted.connect() as conn:
        receipt = conn.execute(
            """
            SELECT status, idempotency_key, ownership_generation, attempt_count,
                   settled_at
            FROM agent_external_action_receipts
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
        receipt_count = conn.execute(
            "SELECT COUNT(*) FROM agent_external_action_receipts"
        ).fetchone()[0]
        attempt_count = conn.execute(
            "SELECT COUNT(*) FROM agent_external_action_attempts"
        ).fetchone()[0]

    assert receipt is not None
    assert tuple(receipt) == (
        "abandoned_before_dispatch",
        idempotency_key,
        1,
        0,
        50.0,
    )
    assert receipt_count == 1
    assert attempt_count == 0


def test_reconciliation_is_idempotent_and_preserves_rejected_attempt_history(
    tmp_path: Path,
) -> None:
    database, _repository, key = _database(tmp_path)
    idempotency_key, _effect_id = _seed_receipt(
        database,
        key,
        status="rejected_before_dispatch",
    )

    with database.connect() as conn:
        first = reconcile_abandoned_before_dispatch_receipts(conn, key, now=50.0)
        second = reconcile_abandoned_before_dispatch_receipts(conn, key, now=60.0)
        receipt = conn.execute(
            """
            SELECT status, attempt_count, settled_at
            FROM agent_external_action_receipts
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
        attempt = conn.execute(
            """
            SELECT status, attempt_count, claim_id, lease_owner
            FROM agent_external_action_attempts
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
        validate_external_action_migration(conn, key)

    assert first == 1
    assert second == 0
    assert receipt is not None
    assert tuple(receipt) == ("abandoned_before_dispatch", 1, 50.0)
    assert attempt is not None
    assert tuple(attempt) == (
        "rejected_before_dispatch",
        1,
        "claim:rejected_before_dispatch:0",
        "worker:fixture",
    )


@pytest.mark.parametrize("status", ["executing", "unknown"])
def test_reconciliation_never_rewrites_maybe_dispatched_receipts(
    tmp_path: Path,
    status: str,
) -> None:
    database, _repository, key = _database(tmp_path)
    idempotency_key, _effect_id = _seed_receipt(database, key, status=status)

    with database.connect() as conn:
        reconciled = reconcile_abandoned_before_dispatch_receipts(conn, key, now=50.0)
        receipt = conn.execute(
            "SELECT status FROM agent_external_action_receipts WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if status == "executing":
            with pytest.raises(ExternalActionMigrationBlocked):
                validate_external_action_migration(conn, key)
        else:
            validate_external_action_migration(conn, key)

    assert reconciled == 0
    assert receipt is not None
    assert receipt["status"] == status


def test_reconciliation_fails_closed_when_parent_payload_identity_differs(
    tmp_path: Path,
) -> None:
    database, repository, key = _database(tmp_path)
    idempotency_key, _effect_id = _seed_receipt(
        database,
        key,
        status="prepared",
        payload_matches_effect=False,
    )

    with database.connect() as conn:
        reconciled = reconcile_abandoned_before_dispatch_receipts(conn, key, now=50.0)
        receipt = conn.execute(
            "SELECT status FROM agent_external_action_receipts WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()

    assert reconciled == 0
    assert receipt is not None
    assert receipt["status"] == "prepared"
    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="external-action receipts",
    ):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=1,
            reason="tampered failed effect must remain blocking",
        )


def test_existing_receipt_schema_upgrades_without_detaching_attempt_foreign_keys(
    tmp_path: Path,
) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
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
            CREATE TABLE agent_external_action_attempts (
                attempt_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT NOT NULL,
                attempt_count INTEGER NOT NULL,
                claim_id TEXT NOT NULL UNIQUE,
                lease_owner TEXT NOT NULL,
                claimed_at REAL NOT NULL,
                lease_until REAL NOT NULL,
                status TEXT NOT NULL,
                platform_result_json TEXT NOT NULL DEFAULT '{}',
                rejection_json TEXT NOT NULL DEFAULT '{}',
                unknown_json TEXT NOT NULL DEFAULT '{}',
                assistant_message_log_id INTEGER,
                settled_at REAL,
                FOREIGN KEY(idempotency_key)
                    REFERENCES agent_external_action_receipts(idempotency_key)
                    ON DELETE CASCADE
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
                'legacy-key', 'legacy-effect', 'legacy-operation', 'profile-a',
                'profile-a:group:room', 1, 'send_poke', 1, ?,
                '{"action_ordinal":7}', 'prepared', 0, '', '', NULL,
                '{}', '{}', '{}', NULL, 1.0, NULL, NULL, 1.0
            )
            """,
            ("a" * 64,),
        )

    restarted = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted.initialize()

    with restarted.connect() as conn:
        receipt_sql = conn.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_external_action_receipts'
            """
        ).fetchone()["sql"]
        action_ordinal = conn.execute(
            """
            SELECT action_ordinal FROM agent_external_action_receipts
            WHERE idempotency_key = 'legacy-key'
            """
        ).fetchone()["action_ordinal"]
        foreign_keys = conn.execute(
            "PRAGMA foreign_key_list('agent_external_action_attempts')"
        ).fetchall()
        indexes = conn.execute(
            "PRAGMA index_list('agent_external_action_receipts')"
        ).fetchall()
        conn.execute(
            """
            UPDATE agent_external_action_receipts
            SET status = 'abandoned_before_dispatch', settled_at = 2.0,
                updated_at = 2.0
            WHERE idempotency_key = 'legacy-key'
            """
        )

    assert "abandoned_before_dispatch" in str(receipt_sql)
    assert action_ordinal == 7
    assert any(
        row["table"] == "agent_external_action_receipts" for row in foreign_keys
    )
    assert any(
        row["name"] == "idx_external_action_receipts_operation_ordinal"
        and row["unique"] == 1
        for row in indexes
    )
