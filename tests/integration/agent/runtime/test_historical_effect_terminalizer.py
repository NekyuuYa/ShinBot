"""Integration coverage for fail-closed historical effect maintenance."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.historical_effect_terminalizer import (
    HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE,
    HistoricalEffectTerminalizationStatus,
    HistoricalEffectTerminalizer,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager

_EFFECT_KINDS = (
    "run_active_chat_bootstrap",
    "run_active_chat_round",
    "active_chat_runtime_reconciliation",
    "stop_active_chat_runtime",
    "cancel_idle_review_planning",
    "idle_review_planning_cancellation_reconciliation",
)
_CONTRACT_CASES = tuple(
    pytest.param(effect_kind, version, id=f"{effect_kind}-v{version}")
    for effect_kind in _EFFECT_KINDS
    for version in (1, 2)
)


def _json(value: Mapping[str, Any]) -> str:
    """Return the canonical JSON representation persisted by actor stores."""

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


async def _make_database(
    tmp_path: Path,
) -> tuple[DatabaseManager, SessionKey, HistoricalEffectTerminalizer]:
    """Create one active Actor v2 key with an otherwise empty actor store."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-terminalizer", "bot:group:historical-effects")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="historical terminalizer integration test",
    ).ownership
    await SQLiteSessionActorStore(database, clock=lambda: 100.0).ensure(
        key,
        ownership_generation=ownership.generation,
    )
    return (
        database,
        key,
        HistoricalEffectTerminalizer(database, clock=lambda: 500.0),
    )


def _seed_effect(
    database: DatabaseManager,
    *,
    key: SessionKey,
    effect_kind: str,
    version: int,
) -> dict[str, object]:
    """Seed one schema-valid inert historical effect and its exact state shape."""

    contract = builtin_effect_contract(effect_kind, version=version)
    effect_id = f"effect:{effect_kind}:v{version}"
    operation_id = f"operation:{effect_kind}:v{version}"
    source_event_id = f"source:{effect_kind}:v{version}"
    completion_event_id = f"completion:{effect_kind}:v{version}"
    failure_event_id = f"failure:{effect_kind}:v{version}"
    plan_id = "review-plan-historical"
    active_epoch = 3
    activity_generation = 7
    input_watermark = 42
    input_ledger_sequence = 9
    state = "active_chat"
    active_chat_state: dict[str, Any] = {"active_epoch": active_epoch}
    data: dict[str, Any] = {}
    operation_kind = effect_kind
    operation_status = "pending"

    if effect_kind in {"run_active_chat_bootstrap", "run_active_chat_round"}:
        operation_kind = (
            "active_chat_bootstrap"
            if effect_kind == "run_active_chat_bootstrap"
            else "active_chat_round"
        )
        fence: dict[str, Any] = {
            "operation_id": operation_id,
            "operation_kind": operation_kind,
            "source_event_id": source_event_id,
            "effect_id": effect_id,
            "effect_kind": effect_kind,
            "idempotency_key": effect_id,
            "completion_event_id": completion_event_id,
            "failure_event_id": failure_event_id,
            "ownership_generation": 1,
            "plan_id": plan_id,
            "active_epoch": active_epoch,
            "activity_generation": activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": input_ledger_sequence,
            "instance_id": "instance-historical",
            "target_session_id": "instance-historical:group:room",
        }
        if version == 2:
            fence.update(
                {
                    "contract_version": contract.version,
                    "contract_signature": contract.signature,
                }
            )
        if effect_kind == "run_active_chat_bootstrap":
            active_chat_state.update(
                {
                    "bootstrap_status": "pending",
                    "bootstrap_operation_id": operation_id,
                    "pending_message_log_ids": [],
                }
            )
        else:
            fence["message_log_ids"] = [41, 42]
            active_chat_state.update(
                {
                    "bootstrap_status": "completed",
                    "round_operation_id": operation_id,
                    "round_input_message_log_ids": [41, 42],
                }
            )
        data["operation_fences"] = {operation_id: fence}
        payload = fence
    elif effect_kind in {
        "stop_active_chat_runtime",
        "cancel_idle_review_planning",
    }:
        operation_kind = "idle_review_planning"
        operation_status = "completed"
        desired_state = (
            "stopped" if effect_kind == "stop_active_chat_runtime" else "cancelled"
        )
        state = "idle" if effect_kind == "stop_active_chat_runtime" else "active_chat"
        if effect_kind == "stop_active_chat_runtime":
            active_chat_state = {}
            payload = {
                "operation_id": operation_id,
                "plan_id": plan_id,
                "outcome": "planned",
                "reason": "historical_effect_cleanup",
                "active_epoch": active_epoch,
                "activity_generation": activity_generation,
                "input_watermark": input_watermark,
                "input_ledger_sequence": input_ledger_sequence,
                "completion_event_id": completion_event_id,
                "failure_event_id": failure_event_id,
            }
        else:
            operation_status = "superseded"
            active_chat_state.update({"bootstrap_status": "completed"})
            payload = {
                "operation_id": operation_id,
                "plan_id": plan_id,
                "active_epoch": active_epoch,
                "activity_generation": activity_generation,
                "input_watermark": input_watermark,
                "input_ledger_sequence": input_ledger_sequence,
                "completion_event_id": completion_event_id,
                "failure_event_id": failure_event_id,
                "superseded_by_event_id": "message:historical-cancel",
            }
        data["effect_control_intents"] = {
            effect_kind: {
                "desired_state": desired_state,
                "status": "requested",
                "effect_id": effect_id,
                "effect_kind": effect_kind,
                "idempotency_key": effect_id,
                "contract_version": contract.version,
                "contract_signature": contract.signature,
                "completion_event_id": completion_event_id,
                "failure_event_id": failure_event_id,
                "operation_id": operation_id,
                "plan_id": plan_id,
                "active_epoch": active_epoch,
                "activity_generation": activity_generation,
                "input_watermark": input_watermark,
                "input_ledger_sequence": input_ledger_sequence,
                "ownership_generation": 1,
                "causation_id": source_event_id,
                "expected_state": state,
                "expected_active_epoch": active_epoch,
                "expected_activity_generation": activity_generation,
                "expected_current_plan_id": plan_id,
            }
        }
    else:
        control_kind = (
            "stop_active_chat_runtime"
            if effect_kind == "active_chat_runtime_reconciliation"
            else "cancel_idle_review_planning"
        )
        state = "idle" if control_kind == "stop_active_chat_runtime" else "active_chat"
        if state == "idle":
            active_chat_state = {}
        else:
            active_chat_state.update({"bootstrap_status": "completed"})
        control_contract = builtin_effect_contract(control_kind, version=version)
        control_effect_id = f"effect:{control_kind}:v{version}"
        desired_state = "stopped" if control_kind == "stop_active_chat_runtime" else "cancelled"
        fence = {
            "operation_id": operation_id,
            "operation_kind": effect_kind,
            "source_event_id": source_event_id,
            "effect_id": effect_id,
            "effect_kind": effect_kind,
            "idempotency_key": effect_id,
            "completion_event_id": completion_event_id,
            "failure_event_id": failure_event_id,
            "ownership_generation": 1,
            "plan_id": plan_id,
            "active_epoch": active_epoch,
            "activity_generation": activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": input_ledger_sequence,
            "desired_state": desired_state,
            "control_effect_kind": control_kind,
            "control_effect_id": control_effect_id,
            "reconciliation_cycle": 1,
        }
        if version == 2:
            fence.update(
                {
                    "contract_version": contract.version,
                    "contract_signature": contract.signature,
                }
            )
        payload = {
            field_name: fence[field_name]
            for field_name in (
                "completion_event_id",
                "failure_event_id",
                "plan_id",
                "active_epoch",
                "activity_generation",
                "input_watermark",
                "input_ledger_sequence",
                "desired_state",
                "control_effect_kind",
                "control_effect_id",
                "reconciliation_cycle",
            )
        }
        data["operation_fences"] = {operation_id: fence}
        data["effect_control_intents"] = {
            control_kind: {
                "desired_state": desired_state,
                "status": "reconciliation_requested",
                "effect_id": control_effect_id,
                "effect_kind": control_kind,
                "idempotency_key": control_effect_id,
                "contract_version": control_contract.version,
                "contract_signature": control_contract.signature,
                "completion_event_id": f"completion:{control_kind}:v{version}",
                "failure_event_id": f"failure:{control_kind}:v{version}",
                "operation_id": f"operation:{control_kind}:v{version}",
                "plan_id": plan_id,
                "active_epoch": active_epoch,
                "activity_generation": activity_generation,
                "input_watermark": input_watermark,
                "input_ledger_sequence": input_ledger_sequence,
                "ownership_generation": 1,
                "causation_id": f"source:{control_kind}:v{version}",
                "expected_state": state,
                "expected_active_epoch": active_epoch,
                "expected_activity_generation": activity_generation,
                "expected_current_plan_id": plan_id,
                "reconciliation_kind": effect_kind,
                "reconciliation_operation_id": operation_id,
                "reconciliation_effect_id": effect_id,
                "reconciliation_idempotency_key": effect_id,
                "reconciliation_contract_version": contract.version,
                "reconciliation_contract_signature": contract.signature,
                "reconciliation_completion_event_id": completion_event_id,
                "reconciliation_failure_event_id": failure_event_id,
                "reconciliation_causation_id": source_event_id,
                "reconciliation_cycle": 1,
            }
        }

    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = ?, state_revision = 4, event_sequence = 8,
                activity_generation = ?, active_epoch = ?,
                current_plan_id = ?, review_plan_revision = 1,
                active_chat_state_json = ?, review_operation_id = '',
                active_reply_operation_id = '', active_chat_round_operation_id = ?,
                idle_planning_operation_id = '', data_json = ?, updated_at = 100.0
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = 1
            """,
            (
                state,
                activity_generation,
                active_epoch,
                plan_id,
                _json(active_chat_state),
                operation_id if effect_kind == "run_active_chat_round" else "",
                _json(data),
                key.profile_id,
                key.session_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, lease_owner, lease_until,
                metadata_json
            ) VALUES (?, ?, ?, 1, ?, ?, ?, 4, ?, ?, ?, ?, 100.0, '', NULL, '{}')
            """,
            (
                operation_id,
                key.profile_id,
                key.session_id,
                operation_kind,
                operation_status,
                source_event_id,
                active_epoch,
                activity_generation,
                input_watermark,
                input_ledger_sequence,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, 'pending', 0, 100.0,
                      '', '', NULL, 100.0, 100.0, NULL, '')
            """,
            (
                effect_id,
                effect_id,
                key.profile_id,
                key.session_id,
                source_event_id,
                operation_id,
                effect_kind,
                contract.version,
                contract.signature,
                _json(payload),
            ),
        )
    return {
        "effect_id": effect_id,
        "operation_id": operation_id,
        "payload": payload,
    }


def _outbox_row(database: DatabaseManager, *, key: SessionKey, effect_id: str) -> dict[str, object]:
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, effect_id),
        ).fetchone()
    assert row is not None
    return dict(row)


@pytest.mark.asyncio
@pytest.mark.parametrize(("effect_kind", "version"), _CONTRACT_CASES)
async def test_terminalizer_only_settles_exact_inert_historical_allowlist(
    tmp_path: Path,
    effect_kind: str,
    version: int,
) -> None:
    """Every supported v1/v2 shape becomes failed without an actor completion."""

    database, key, terminalizer = await _make_database(tmp_path)
    seeded = _seed_effect(
        database,
        key=key,
        effect_kind=effect_kind,
        version=version,
    )
    effect_id = str(seeded["effect_id"])
    with database.connect() as conn:
        aggregate_before = dict(
            conn.execute(
                """
                SELECT state, state_revision, active_chat_state_json, data_json
                FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
        )
        operation_before = dict(
            conn.execute(
                "SELECT * FROM agent_session_operations WHERE operation_id = ?",
                (seeded["operation_id"],),
            ).fetchone()
        )

    terminalized = await terminalizer.terminalize(key=key, effect_id=effect_id)
    database.initialize()
    replay = await terminalizer.terminalize(key=key, effect_id=effect_id)

    assert terminalized.status is HistoricalEffectTerminalizationStatus.TERMINALIZED
    assert terminalized.reason_code == HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE
    assert terminalized.audit_id
    assert replay.status is HistoricalEffectTerminalizationStatus.ALREADY_TERMINALIZED
    assert replay.audit_id == terminalized.audit_id
    row = _outbox_row(database, key=key, effect_id=effect_id)
    assert row["status"] == "failed"
    assert row["attempt_count"] == 0
    assert row["claim_id"] == ""
    assert row["lease_owner"] == ""
    assert row["lease_until"] is None
    assert row["completed_at"] == 500.0
    assert row["last_error"] == HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE
    with database.connect() as conn:
        aggregate_after = dict(
            conn.execute(
                """
                SELECT state, state_revision, active_chat_state_json, data_json
                FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
        )
        operation_after = dict(
            conn.execute(
                "SELECT * FROM agent_session_operations WHERE operation_id = ?",
                (seeded["operation_id"],),
            ).fetchone()
        )
        audits = conn.execute(
            """
            SELECT audit_id, effect_payload_sha256, failure_code, evidence_json
            FROM agent_historical_effect_terminalizations
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, effect_id),
        ).fetchall()
        mailbox_count = conn.execute(
            """
            SELECT count(*) AS value FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()["value"]
    assert aggregate_after == aggregate_before
    assert operation_after == operation_before
    assert mailbox_count == 0
    assert len(audits) == 1
    assert audits[0]["audit_id"] == terminalized.audit_id
    assert audits[0]["failure_code"] == HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE
    assert audits[0]["effect_payload_sha256"] == hashlib.sha256(
        _json(seeded["payload"]).encode("utf-8")
    ).hexdigest()
    assert json.loads(audits[0]["evidence_json"])["proof"] == (
        "never_claimed_no_live_mailbox_route_or_receipt"
    )


@pytest.mark.asyncio
async def test_terminalizer_rejects_a_changed_operation_fence_without_writing_audit(
    tmp_path: Path,
) -> None:
    """A stale aggregate cannot be used to settle a row by effect identity alone."""

    database, key, terminalizer = await _make_database(tmp_path)
    seeded = _seed_effect(
        database,
        key=key,
        effect_kind="run_active_chat_bootstrap",
        version=2,
    )
    effect_id = str(seeded["effect_id"])
    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT data_json FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        data = json.loads(aggregate["data_json"])
        data["operation_fences"][str(seeded["operation_id"])]["effect_id"] = "forged-effect"
        conn.execute(
            """
            UPDATE agent_session_aggregates SET data_json = ?
            WHERE profile_id = ? AND session_id = ?
            """,
            (_json(data), key.profile_id, key.session_id),
        )

    result = await terminalizer.terminalize(key=key, effect_id=effect_id)

    assert result.status is HistoricalEffectTerminalizationStatus.REJECTED
    assert result.reason_code == "workflow_operation_fence_changed"
    assert _outbox_row(database, key=key, effect_id=effect_id)["status"] == "pending"
    with database.connect() as conn:
        audit_count = conn.execute(
            "SELECT count(*) AS value FROM agent_historical_effect_terminalizations"
        ).fetchone()["value"]
    assert audit_count == 0


@pytest.mark.asyncio
async def test_terminalizer_rejects_contract_signature_drift_before_shape_checks(
    tmp_path: Path,
) -> None:
    """A persisted row cannot opt into maintenance with only a matching kind/version."""

    database, key, terminalizer = await _make_database(tmp_path)
    seeded = _seed_effect(
        database,
        key=key,
        effect_kind="run_active_chat_bootstrap",
        version=2,
    )
    effect_id = str(seeded["effect_id"])
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox SET contract_signature = 'forged-signature'
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, effect_id),
        )

    result = await terminalizer.terminalize(key=key, effect_id=effect_id)

    assert result.status is HistoricalEffectTerminalizationStatus.REJECTED
    assert result.reason_code == "historical_effect_contract_signature_changed"
    assert _outbox_row(database, key=key, effect_id=effect_id)["status"] == "pending"


@pytest.mark.asyncio
async def test_terminalizer_rejects_generation_aba_before_mutating_the_outbox(
    tmp_path: Path,
) -> None:
    """An old-generation row cannot be terminalized after ownership moves."""

    database, key, terminalizer = await _make_database(tmp_path)
    seeded = _seed_effect(
        database,
        key=key,
        effect_kind="run_active_chat_round",
        version=2,
    )
    effect_id = str(seeded["effect_id"])
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_runtime_ownership SET generation = 2
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )

    result = await terminalizer.terminalize(key=key, effect_id=effect_id)

    assert result.status is HistoricalEffectTerminalizationStatus.REJECTED
    assert result.reason_code == "effect_ownership_generation_not_active"
    assert _outbox_row(database, key=key, effect_id=effect_id)["status"] == "pending"


def _seed_live_mailbox(database: DatabaseManager, *, key: SessionKey) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation, kind,
                source, occurred_at, payload_json, available_at, status,
                attempt_count, claim_id, lease_owner, lease_until, created_at
            ) VALUES ('live-mailbox', ?, ?, 1, 'MaintenanceTest', 'test',
                      100.0, '{}', 100.0, 'pending', 0, '', '', NULL, 100.0)
            """,
            (key.profile_id, key.session_id),
        )


def _seed_live_receipt(database: DatabaseManager, *, key: SessionKey) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_external_action_receipts (
                idempotency_key, effect_id, operation_id, profile_id, session_id,
                ownership_generation, action_ordinal, action_kind,
                contract_version, request_digest, request_json, status,
                attempt_count, claim_id, lease_owner, lease_until,
                prepared_at, updated_at
            ) VALUES ('live-receipt-key', 'live-receipt-effect', 'live-receipt-op',
                      ?, ?, 1, 0, 'send_poke', 1, ?, '{}', 'prepared', 0,
                      '', '', NULL, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, "0" * 64),
        )


def _seed_live_route(database: DatabaseManager, *, key: SessionKey) -> None:
    digest = hashlib.sha256(b"{}").hexdigest()
    with database.connect() as conn:
        message_log_id = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES (?, 'user', 100.0)
            """,
            (key.session_id,),
        ).lastrowid
        assert isinstance(message_log_id, int)
        conn.execute(
            """
            INSERT INTO message_routing_jobs (
                routing_job_id, idempotency_key, message_log_id, version,
                profile_id, session_id, ownership_generation,
                message_fingerprint, payload_json, payload_digest, trace_id,
                correlation_id, causation_id, occurred_at, status, attempt_count,
                available_at, claim_id, lease_owner, lease_until,
                decision_version, decision_kind, decision_id,
                decision_payload_json, decision_payload_digest,
                created_at, updated_at
            ) VALUES ('live-route-job', 'live-route-job-key', ?, 1, ?, ?, 1,
                      'fingerprint', '{}', ?, 'trace:route', 'correlation:route',
                      '', 100.0, 'completed', 1, 100.0, 'route-claim',
                      'route-worker', NULL, NULL, '', '', '{}', '', 100.0, 100.0)
            """,
            (message_log_id, key.profile_id, key.session_id, digest),
        )
        conn.execute(
            """
            INSERT INTO agent_route_outbox (
                delivery_id, idempotency_key, routing_job_id, profile_id,
                session_id, message_log_id, route_rule_id, version,
                ownership_generation, event_id, payload_json, payload_digest,
                trace_id, correlation_id, causation_id, status, attempt_count,
                available_at, claim_id, lease_owner, lease_until, created_at,
                updated_at
            ) VALUES ('live-route-delivery', 'live-route-delivery-key',
                      'live-route-job', ?, ?, ?, 'agent-entry', 1, 1,
                      'live-route-event', '{}', ?, 'trace:route',
                      'correlation:route', '', 'pending', 0, 100.0, '', '',
                      NULL, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, message_log_id, digest),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("seed_live_work", "reason_code"),
    (
        pytest.param(_seed_live_mailbox, "live_mailbox_work_present", id="mailbox"),
        pytest.param(_seed_live_route, "live_route_work_present", id="route"),
        pytest.param(
            _seed_live_receipt,
            "live_external_action_receipt_present",
            id="external-receipt",
        ),
    ),
)
async def test_terminalizer_rejects_any_live_mailbox_route_or_receipt(
    tmp_path: Path,
    seed_live_work: Any,
    reason_code: str,
) -> None:
    """Maintenance cannot race any other independently executable work."""

    database, key, terminalizer = await _make_database(tmp_path)
    seeded = _seed_effect(
        database,
        key=key,
        effect_kind="run_active_chat_bootstrap",
        version=2,
    )
    effect_id = str(seeded["effect_id"])
    seed_live_work(database, key=key)

    result = await terminalizer.terminalize(key=key, effect_id=effect_id)

    assert result.status is HistoricalEffectTerminalizationStatus.REJECTED
    assert result.reason_code == reason_code
    assert _outbox_row(database, key=key, effect_id=effect_id)["status"] == "pending"
    with database.connect() as conn:
        assert conn.execute(
            "SELECT count(*) AS value FROM agent_historical_effect_terminalizations"
        ).fetchone()["value"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "attempt_count", "claim_id", "lease_owner", "lease_until", "reason_code"),
    (
        pytest.param(
            "processing",
            1,
            "expired-claim",
            "expired-worker",
            1.0,
            "effect_status_not_pending",
            id="expired-processing-lease",
        ),
        pytest.param(
            "pending",
            1,
            "",
            "",
            None,
            "effect_already_attempted",
            id="attempted-pending-row",
        ),
    ),
)
async def test_terminalizer_never_adopts_processing_or_attempted_effects(
    tmp_path: Path,
    status: str,
    attempt_count: int,
    claim_id: str,
    lease_owner: str,
    lease_until: float | None,
    reason_code: str,
) -> None:
    """An expiry is not authority to replay or terminalize a claimed effect."""

    database, key, terminalizer = await _make_database(tmp_path)
    seeded = _seed_effect(
        database,
        key=key,
        effect_kind="run_active_chat_bootstrap",
        version=2,
    )
    effect_id = str(seeded["effect_id"])
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET status = ?, attempt_count = ?, claim_id = ?, lease_owner = ?,
                lease_until = ?
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (
                status,
                attempt_count,
                claim_id,
                lease_owner,
                lease_until,
                key.profile_id,
                key.session_id,
                effect_id,
            ),
        )

    result = await terminalizer.terminalize(key=key, effect_id=effect_id)

    assert result.status is HistoricalEffectTerminalizationStatus.REJECTED
    assert result.reason_code == reason_code
    row = _outbox_row(database, key=key, effect_id=effect_id)
    assert row["status"] == status
    assert row["attempt_count"] == attempt_count
    assert row["claim_id"] == claim_id
    assert row["lease_owner"] == lease_owner
    assert row["lease_until"] == lease_until


@pytest.mark.asyncio
async def test_terminalizer_keeps_cancel_review_workflow_v1_as_a_blocker(
    tmp_path: Path,
) -> None:
    """Review cancellation has a distinct liveness proof and is never isolated here."""

    database, key, terminalizer = await _make_database(tmp_path)
    contract = builtin_effect_contract("cancel_review_workflow", version=1)
    effect_id = "effect:cancel-review-v1"
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, 1, 'source:cancel-review-v1',
                      'operation:cancel-review-v1', 'cancel_review_workflow',
                      ?, ?, '{}', 'pending', 0, 100.0, '', '', NULL,
                      100.0, 100.0, NULL, '')
            """,
            (
                effect_id,
                effect_id,
                key.profile_id,
                key.session_id,
                contract.version,
                contract.signature,
            ),
        )

    result = await terminalizer.terminalize(key=key, effect_id=effect_id)

    assert result.status is HistoricalEffectTerminalizationStatus.REJECTED
    assert result.reason_code == "historical_effect_contract_not_allowlisted"
    assert _outbox_row(database, key=key, effect_id=effect_id)["status"] == "pending"
