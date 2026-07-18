"""Integration tests for inactive typed recovery-graph discovery."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
    RecoveryDeliveryEnvelopeIdentity,
    RecoveryDeliveryPayload,
    RecoveryV1Policy,
    canonical_recovery_json,
    decode_recovery_delivery_payload,
)
from shinbot.agent.runtime.session_actor.recovery_graph_reader import (
    RecoveryDeliveryClaimLost,
    RecoveryGraphNotEligible,
    RecoveryGraphReadError,
    SQLiteRecoveryGraphReader,
)
from shinbot.agent.runtime.session_actor.recovery_scanner import (
    MAX_RECOVERY_SCAN_CANDIDATES,
    RecoveryScanDisposition,
    SQLiteRecoveryGraphScanner,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.mailbox_handoff import (
    MailboxHandoffEvidenceState,
    MailboxHandoffState,
)
from shinbot.persistence import DatabaseManager


def _make_database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _review_operation_fence_data(generation: int) -> str:
    """Return the full durable fence for one terminal review effect."""

    contract = builtin_effect_contract("run_review_workflow")
    return json.dumps(
        {
            "operation_fences": {
                "review-operation": {
                    "operation_id": "review-operation",
                    "operation_kind": "review",
                    "source_event_id": "review-launch",
                    "effect_id": "review-effect",
                    "effect_kind": "run_review_workflow",
                    "idempotency_key": "review-effect",
                    "completion_event_id": "review-completed",
                    "failure_event_id": "review-failed",
                    "ownership_generation": generation,
                    "plan_id": "",
                    "active_epoch": 0,
                    "activity_generation": 0,
                    "input_watermark": 0,
                    "input_ledger_sequence": 0,
                    "contract_version": contract.version,
                    "contract_signature": contract.signature,
                }
            }
        },
        separators=(",", ":"),
        sort_keys=True,
    )


async def _seed_orphaned_review(
    database: DatabaseManager,
    *,
    key: SessionKey,
    admission_grant: ActorV2AdmissionGrant | None = None,
) -> int:
    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="typed recovery graph scanner test",
        admission_grant=admission_grant,
    ).ownership.generation
    store = SQLiteSessionActorStore(database, clock=lambda: 10.0)
    await store.ensure(key, ownership_generation=generation)
    with database.connect() as conn:
        contract = builtin_effect_contract("run_review_workflow")
        updated = conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = 'review', state_revision = 1,
                review_operation_id = 'review-operation',
                data_json = ?, updated_at = 10
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            """,
            (
                _review_operation_fence_data(generation),
                key.profile_id,
                key.session_id,
                generation,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, metadata_json
            ) VALUES ('review-operation', ?, ?, ?, 'review', 'pending',
                      'review-launch', 1, 0, 0, 0, 0, 10, '{}')
            """,
            (key.profile_id, key.session_id, generation),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, 'review-launch', 'review-operation', ?,
                      ?, ?, ?, 'failed', 1, 10, '', '', NULL, 10, 10, 10,
                      'worker_lost_before_completion_delivery')
            """,
            (
                "review-effect",
                "review-effect",
                key.profile_id,
                key.session_id,
                generation,
                contract.effect_kind,
                contract.version,
                contract.signature,
                _review_operation_fence_data(generation),
            ),
        )
    assert updated.rowcount == 1
    return generation


def _invalidate_admission_fence(
    database: DatabaseManager,
    grant: ActorV2AdmissionGrant,
    *,
    state: str,
) -> str:
    """Invalidate one committed actor fence without rewriting actor state."""

    if state == "revoked":
        database.actor_v2_admission_fences.revoke(
            grant,
            reason="integration test revokes recovery admission",
        )
        return "admission_fence_revoked"
    if state == "expired":
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_session_actor_v2_admission_fences
                SET expires_at = 0
                WHERE profile_id = ? AND session_id = ?
                  AND fence_id = ? AND generation = ?
                """,
                (
                    grant.fence.key.profile_id,
                    grant.fence.key.session_id,
                    grant.fence.fence_id,
                    grant.fence.generation,
                ),
            )
        return "admission_fence_expired"
    if state == "missing":
        with database.connect() as conn:
            conn.execute(
                """
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = ? AND session_id = ?
                  AND fence_id = ? AND generation = ?
                """,
                (
                    grant.fence.key.profile_id,
                    grant.fence.key.session_id,
                    grant.fence.fence_id,
                    grant.fence.generation,
                ),
            )
        return "admission_fence_missing"
    raise AssertionError(f"unsupported admission fence state: {state}")


async def _seed_orphaned_review_with_cancellation_liveness(
    database: DatabaseManager,
    *,
    key: SessionKey,
    gate_status: str,
    include_running_witness: bool,
    witness_status: str = "running",
) -> int:
    """Add an interrupted old review beside an otherwise recoverable root."""

    generation = await _seed_orphaned_review(database, key=key)
    review_contract = builtin_effect_contract("run_review_workflow")
    cancellation_contract = builtin_effect_contract(
        "cancel_review_workflow",
        version=2,
    )
    target_status = "processing" if gate_status == "requested" else "cancelled"
    target_claim_id = "old-review-claim"
    control_status = "pending" if gate_status == "requested" else "completed"
    cancellation_payload = json.dumps(
        {
            "operation_id": "old-review-operation",
            "cancelled_operation_fence": {
                "operation_id": "old-review-operation",
                "effect_id": "old-review-effect",
                "effect_kind": "run_review_workflow",
                "contract_version": review_contract.version,
                "contract_signature": review_contract.signature,
                "ownership_generation": generation,
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, superseded_at, metadata_json
            ) VALUES ('old-review-operation', ?, ?, ?, 'review', 'superseded',
                      'old-review-launch', 1, 0, 0, 0, 0, 10, 15, '{}')
            """,
            (key.profile_id, key.session_id, generation),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES ('old-review-effect', 'old-review-effect', ?, ?, ?,
                      'old-review-launch', 'old-review-operation',
                      'run_review_workflow', ?, ?, '{}', ?, 1, 10, ?, ?, ?,
                      10, ?, ?, '')
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                review_contract.version,
                review_contract.signature,
                target_status,
                target_claim_id if target_status == "processing" else "",
                "old-review-worker" if target_status == "processing" else "",
                200 if target_status == "processing" else None,
                15 if target_status == "processing" else 20,
                None if target_status == "processing" else 20,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES ('cancel-old-review', 'cancel-old-review', ?, ?, ?,
                      'interrupt-event', 'old-review-operation',
                      'cancel_review_workflow', ?, ?, ?, ?, 1, 15, '', '', NULL,
                      15, ?, ?, '')
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                cancellation_contract.version,
                cancellation_contract.signature,
                cancellation_payload,
                control_status,
                15 if control_status == "pending" else 20,
                None if control_status == "pending" else 20,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_review_cancellation_gates (
                profile_id, session_id, ownership_generation,
                cancellation_effect_id, request_event_id,
                review_operation_id, review_effect_id, review_effect_kind,
                review_contract_version, review_contract_signature,
                gate_status, target_effect_status, target_effect_claim_id,
                target_effect_attempt_count, target_effect_terminal_at,
                created_at, updated_at
            ) VALUES (?, ?, ?, 'cancel-old-review', 'interrupt-event',
                      'old-review-operation', 'old-review-effect',
                      'run_review_workflow', ?, ?, ?, ?, ?, 1, ?, 15, ?)
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                review_contract.version,
                review_contract.signature,
                gate_status,
                target_status,
                target_claim_id,
                None if gate_status == "requested" else 20,
                15 if gate_status == "requested" else 20,
            ),
        )
        if include_running_witness:
            if witness_status not in {"running", "unknown"}:
                raise ValueError("review witness status must be running or unknown")
            conn.execute(
                """
                INSERT INTO agent_review_execution_runs (
                    profile_id, session_id, ownership_generation,
                    review_effect_id, review_operation_id, review_effect_kind,
                    review_contract_version, review_contract_signature,
                    claim_id, worker_id, execution_status, started_at,
                    unknown_at, unknown_reason
                ) VALUES (?, ?, ?, 'old-review-effect', 'old-review-operation',
                          'run_review_workflow', ?, ?, ?, 'old-review-worker',
                          ?, 10, ?, ?)
                """,
                (
                    key.profile_id,
                    key.session_id,
                    generation,
                    review_contract.version,
                    review_contract.signature,
                    target_claim_id,
                    witness_status,
                    15 if witness_status == "unknown" else None,
                    (
                        "review_execution_lease_expired_before_handler_terminal"
                        if witness_status == "unknown"
                        else ""
                    ),
                ),
            )
    return generation


async def _seed_orphaned_review_with_model_liveness(
    database: DatabaseManager,
    *,
    key: SessionKey,
    witness_status: str,
) -> int:
    """Attach a generic active-reply witness beside a recoverable review root."""

    if witness_status not in {"running", "unknown"}:
        raise ValueError("model witness status must be running or unknown")
    generation = await _seed_orphaned_review(database, key=key)
    contract = builtin_effect_contract("run_active_reply_workflow")
    unknown_at = 15 if witness_status == "unknown" else None
    unknown_reason = (
        "model_execution_lease_expired_before_handler_terminal"
        if witness_status == "unknown"
        else ""
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, superseded_at, metadata_json
            ) VALUES ('old-active-reply-operation', ?, ?, ?, 'active_reply', 'superseded',
                      'old-active-reply-launch', 1, 0, 0, 0, 0, 10, 15, '{}')
            """,
            (key.profile_id, key.session_id, generation),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES ('old-active-reply-effect', 'old-active-reply-effect', ?, ?, ?,
                      'old-active-reply-launch', 'old-active-reply-operation',
                      'run_active_reply_workflow', ?, ?, '{}', 'processing', 1, 10,
                      'old-active-reply-claim', 'old-active-reply-worker', 200,
                      10, 10, NULL, '')
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                contract.version,
                contract.signature,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_model_execution_runs (
                profile_id, session_id, ownership_generation,
                effect_id, operation_id, effect_kind,
                contract_version, contract_signature, claim_id, worker_id,
                execution_status, started_at, unknown_at, unknown_reason
            ) VALUES (?, ?, ?, 'old-active-reply-effect', 'old-active-reply-operation',
                      'run_active_reply_workflow', ?, ?, 'old-active-reply-claim',
                      'old-active-reply-worker', ?, 10, ?, ?)
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                contract.version,
                contract.signature,
                witness_status,
                unknown_at,
                unknown_reason,
            ),
        )
    return generation


async def _seed_orphaned_review_with_model_cancellation_gate(
    database: DatabaseManager,
    *,
    key: SessionKey,
    gate_status: str,
) -> int:
    """Attach one unresolved v3 model-cancellation gate to a safe root."""

    if gate_status not in {"requested", "cancelled", "terminal", "blocked"}:
        raise ValueError("model cancellation gate status is unsupported")
    generation = await _seed_orphaned_review(database, key=key)
    target_contract = builtin_effect_contract("run_idle_review_planning", version=3)
    control_contract = builtin_effect_contract("cancel_model_execution", version=3)
    operation_id = "old-idle-planning-operation"
    target_effect_id = "old-idle-planning-effect"
    control_effect_id = "cancel-old-idle-planning"
    request_event_id = "interrupt-idle-planning"
    target_claim_id = "old-idle-planning-claim"
    target_worker_id = "old-idle-planning-worker"
    target_status = (
        "cancelled" if gate_status in {"cancelled", "terminal"} else "processing"
    )
    target_execution_status = {
        "requested": "none",
        "cancelled": "running",
        "terminal": "none",
        "blocked": "unknown",
    }[gate_status]
    target_terminal_at = 20 if target_status == "cancelled" else None
    control_status = {
        "requested": "pending",
        "cancelled": "processing",
        "terminal": "pending",
        "blocked": "completed",
    }[gate_status]
    blocker_code = "model_execution_witness_unknown" if gate_status == "blocked" else ""
    cancellation_payload = json.dumps(
        {
            "operation_id": operation_id,
            "cancelled_model_effect_fence": {
                "operation_id": operation_id,
                "effect_id": target_effect_id,
                "effect_kind": "run_idle_review_planning",
                "contract_version": target_contract.version,
                "contract_signature": target_contract.signature,
                "ownership_generation": generation,
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, superseded_at, metadata_json
            ) VALUES (?, ?, ?, ?, 'idle_review_planning', 'superseded', ?, 1,
                      0, 0, 0, 0, 10, 15, '{}')
            """,
            (
                operation_id,
                key.profile_id,
                key.session_id,
                generation,
                "old-idle-planning-launch",
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, 'old-idle-planning-launch', ?,
                      'run_idle_review_planning', ?, ?, '{}', ?, 1, 10, ?, ?, ?,
                      10, ?, ?, '')
            """,
            (
                target_effect_id,
                target_effect_id,
                key.profile_id,
                key.session_id,
                generation,
                operation_id,
                target_contract.version,
                target_contract.signature,
                target_status,
                target_claim_id if target_status == "processing" else "",
                target_worker_id if target_status == "processing" else "",
                200 if target_status == "processing" else None,
                15 if target_status == "processing" else 20,
                target_terminal_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'cancel_model_execution', ?, ?, ?, ?,
                      1, 15, '', '', NULL, 15, ?, ?, '')
            """,
            (
                control_effect_id,
                control_effect_id,
                key.profile_id,
                key.session_id,
                generation,
                request_event_id,
                operation_id,
                control_contract.version,
                control_contract.signature,
                cancellation_payload,
                control_status,
                15 if control_status in {"pending", "processing"} else 20,
                None if control_status in {"pending", "processing"} else 20,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_model_execution_cancellation_gates (
                profile_id, session_id, ownership_generation,
                cancellation_effect_id, request_event_id,
                target_operation_id, target_effect_id, target_effect_kind,
                target_contract_version, target_contract_signature,
                target_effect_status, target_claim_id, target_worker_id,
                target_effect_attempt_count, target_execution_status,
                gate_status, target_effect_terminal_at, blocker_code,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'run_idle_review_planning', ?, ?, ?,
                      ?, ?, 1, ?, ?, ?, ?, 15, ?)
            """,
            (
                key.profile_id,
                key.session_id,
                generation,
                control_effect_id,
                request_event_id,
                operation_id,
                target_effect_id,
                target_contract.version,
                target_contract.signature,
                target_status,
                target_claim_id if gate_status != "terminal" else "",
                target_worker_id if gate_status != "terminal" else "",
                target_execution_status,
                gate_status,
                target_terminal_at,
                blocker_code,
                15 if gate_status == "requested" else 20,
            ),
        )
        if target_execution_status in {"running", "unknown"}:
            conn.execute(
                """
                INSERT INTO agent_model_execution_runs (
                    profile_id, session_id, ownership_generation,
                    effect_id, operation_id, effect_kind,
                    contract_version, contract_signature, claim_id, worker_id,
                    execution_status, started_at, unknown_at, unknown_reason
                ) VALUES (?, ?, ?, ?, ?, 'run_idle_review_planning', ?, ?, ?, ?,
                          ?, 10, ?, ?)
                """,
                (
                    key.profile_id,
                    key.session_id,
                    generation,
                    target_effect_id,
                    operation_id,
                    target_contract.version,
                    target_contract.signature,
                    target_claim_id,
                    target_worker_id,
                    target_execution_status,
                    15 if target_execution_status == "unknown" else None,
                    (
                        "model_execution_lease_expired_before_handler_terminal"
                        if target_execution_status == "unknown"
                        else ""
                    ),
                ),
            )
    return generation


def _insert_safe_historical_cancelled_gate(
    database: DatabaseManager,
    *,
    key: SessionKey,
    ownership_generation: int,
    token: int,
) -> None:
    """Insert one legacy cancelled gate with complete durable terminal proof."""

    review_contract = builtin_effect_contract("run_review_workflow")
    cancellation_contract = builtin_effect_contract(
        "cancel_review_workflow",
        version=2,
    )
    operation_id = f"historical-review-operation:{token}"
    review_effect_id = f"historical-review-effect:{token}"
    cancellation_effect_id = f"historical-cancel-review:{token}"
    request_event_id = f"historical-interrupt:{token}"
    cancellation_payload = json.dumps(
        {
            "operation_id": operation_id,
            "cancelled_operation_fence": {
                "operation_id": operation_id,
                "effect_id": review_effect_id,
                "effect_kind": "run_review_workflow",
                "contract_version": review_contract.version,
                "contract_signature": review_contract.signature,
                "ownership_generation": ownership_generation,
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, superseded_at, metadata_json
            ) VALUES (?, ?, ?, ?, 'review', 'superseded', ?, 1, 0, 0, 0, 0,
                      10, 15, '{}')
            """,
            (
                operation_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                f"historical-review-launch:{token}",
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'run_review_workflow', ?, ?, '{}',
                      'cancelled', 1, 10, '', '', NULL, 10, 20, 20, '')
            """,
            (
                review_effect_id,
                review_effect_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                f"historical-review-launch:{token}",
                operation_id,
                review_contract.version,
                review_contract.signature,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'cancel_review_workflow', ?, ?, ?,
                      'completed', 1, 15, '', '', NULL, 15, 20, 20, '')
            """,
            (
                cancellation_effect_id,
                cancellation_effect_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                request_event_id,
                operation_id,
                cancellation_contract.version,
                cancellation_contract.signature,
                cancellation_payload,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_review_cancellation_gates (
                profile_id, session_id, ownership_generation,
                cancellation_effect_id, request_event_id,
                review_operation_id, review_effect_id, review_effect_kind,
                review_contract_version, review_contract_signature,
                gate_status, target_effect_status, target_effect_claim_id,
                target_effect_attempt_count, target_effect_terminal_at,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'run_review_workflow', ?, ?,
                      'cancelled', 'cancelled', ?, 1, 20, 15, 20)
            """,
            (
                key.profile_id,
                key.session_id,
                ownership_generation,
                cancellation_effect_id,
                request_event_id,
                operation_id,
                review_effect_id,
                review_contract.version,
                review_contract.signature,
                f"historical-review-claim:{token}",
            ),
        )


def _durable_authority_snapshot(
    database: DatabaseManager,
    *,
    key: SessionKey,
    ownership_generation: int,
) -> tuple[object, ...]:
    """Capture the rows a read-only graph rebuild must not change."""

    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT state, state_revision, event_sequence, review_operation_id, data_json
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, ownership_generation),
        ).fetchone()
        operation = conn.execute(
            """
            SELECT status, lease_owner, lease_until, input_watermark,
                   input_ledger_sequence
            FROM agent_session_operations
            WHERE operation_id = 'review-operation'
            """,
        ).fetchone()
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_mailbox),
                (SELECT COUNT(*) FROM agent_session_recovery_cases),
                (SELECT COUNT(*) FROM agent_session_recovery_findings),
                (SELECT COUNT(*) FROM agent_state_transitions),
                (SELECT COUNT(*) FROM agent_effect_outbox)
            """
        ).fetchone()
    assert aggregate is not None
    assert operation is not None
    assert counts is not None
    return (tuple(aggregate), tuple(operation), tuple(counts))


async def test_scanner_emits_one_typed_delivery_for_orphaned_work(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)

    first = scanner.scan()
    second = scanner.scan()

    assert first.delivered_count == 1
    assert first.results[0].disposition is RecoveryScanDisposition.DELIVERED
    assert second.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    with database.connect() as conn:
        case = conn.execute(
            """
            SELECT case_id, status, delivery_count, next_delivery_cycle, last_event_id
            FROM agent_session_recovery_cases
            """
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT mailbox_id, event_id, profile_id, session_id, ownership_generation,
                   kind, source, payload_json, causation_id, correlation_id,
                   trace_id, status
            FROM agent_session_mailbox
            """
        ).fetchone()
    assert case is not None
    assert tuple(case[name] for name in ("status", "delivery_count", "next_delivery_cycle")) == (
        "open",
        1,
        1,
    )
    assert mailbox is not None
    assert tuple(mailbox[name] for name in ("kind", "source", "status")) == (
        RECOVERY_DELIVERY_EVENT_KIND,
        RECOVERY_DELIVERY_EVENT_SOURCE,
        "pending",
    )
    envelope = RecoveryDeliveryEnvelopeIdentity(
        event_id=str(mailbox["event_id"]),
        profile_id=str(mailbox["profile_id"]),
        session_id=str(mailbox["session_id"]),
        ownership_generation=int(mailbox["ownership_generation"]),
        kind=str(mailbox["kind"]),
        source=str(mailbox["source"]),
    )
    payload = decode_recovery_delivery_payload(
        json.loads(str(mailbox["payload_json"])),
        envelope=envelope,
    )
    assert payload.case_id == str(case["case_id"])
    assert payload.delivery_cycle == 0
    assert str(mailbox["causation_id"]) == payload.case_id
    assert str(mailbox["correlation_id"]) == payload.case_id
    assert str(mailbox["trace_id"]) == payload.event_id
    assert payload.certificate.subject.ownership_generation == generation
    handoff = database.actor_v2_mailbox_handoffs.read(int(mailbox["mailbox_id"]))
    assert handoff is not None
    assert handoff.evidence.state is MailboxHandoffEvidenceState.UNFENCED_LEGACY
    assert handoff.state is MailboxHandoffState.BLOCKED


async def test_fenced_recovery_delivery_exposes_exact_wake_identity(
    tmp_path: Path,
) -> None:
    """New and retained recovery deliveries keep the same full wake identity."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "fenced-recovery-wake")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="recovery-wake-test",
        ttl_seconds=3600.0,
    )
    generation = await _seed_orphaned_review(
        database,
        key=key,
        admission_grant=grant,
    )
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)

    first = scanner.scan()
    second = scanner.scan()

    expected = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=generation,
        admission_fence_id=grant.fence.fence_id,
        admission_fence_generation=grant.fence.generation,
    )
    assert first.results[0].disposition is RecoveryScanDisposition.DELIVERED
    assert first.results[0].wake_request == expected
    assert first.results[0].event_id
    assert first.results[0].mailbox_id is not None
    assert second.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    assert second.results[0].wake_request == expected
    assert second.results[0].event_id == first.results[0].event_id
    assert second.results[0].mailbox_id == first.results[0].mailbox_id
    assert first.wake_requests == (expected,)
    assert second.wake_requests == (expected,)
    assert scanner.pending_recovery_wake_requests() == (expected,)
    assert scanner.pending_recovery_wake_requests(profile_id=key.profile_id) == (
        expected,
    )
    assert scanner.pending_recovery_wake_requests(profile_id="other-profile") == ()
    assert scanner.is_pending_recovery_wake_request(expected)
    debts = scanner.pending_recovery_wake_debts()
    assert len(debts) == 1
    assert debts[0].request == expected
    assert debts[0].event_id == first.results[0].event_id
    assert scanner.is_pending_recovery_wake_debt(debts[0])
    assert scanner.pending_recovery_wake_debts(after=debts[0].cursor) == ()
    with database.connect() as conn:
        mailbox = conn.execute(
            "SELECT mailbox_id FROM agent_session_mailbox WHERE event_id = ?",
            (first.results[0].event_id,),
        ).fetchone()
    assert mailbox is not None
    assert int(mailbox["mailbox_id"]) == first.results[0].mailbox_id
    handoff = database.actor_v2_mailbox_handoffs.read(int(mailbox["mailbox_id"]))
    assert handoff is not None
    assert handoff.evidence.state is MailboxHandoffEvidenceState.FENCED
    assert handoff.evidence.as_fenced_wake_request() == expected
    assert handoff.state is MailboxHandoffState.PENDING


@pytest.mark.parametrize("historical_evidence", ["missing", "unfenced_legacy"])
async def test_recovery_replay_never_infers_or_upgrades_historical_handoff(
    tmp_path: Path,
    historical_evidence: str,
) -> None:
    """Retained recovery deliveries never gain current-owner fence evidence."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", f"recovery-historical-handoff-{historical_evidence}")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="recovery-historical-handoff-test",
        ttl_seconds=3600.0,
    )
    generation = await _seed_orphaned_review(database, key=key, admission_grant=grant)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = scanner.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    payload = RecoveryDeliveryPayload(certificate=certificate, delivery_cycle=0)
    payload_json = canonical_recovery_json(payload.to_record())
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_recovery_cases (
                case_id, profile_id, session_id, ownership_generation,
                certificate_version, policy_version, work_graph_digest,
                latest_certificate_digest, status, next_delivery_cycle,
                delivery_count, last_event_id, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', 0, 0, '', '', 100, 100)
            """,
            (
                certificate.case_identity.case_id,
                certificate.subject.profile_id,
                certificate.subject.session_id,
                certificate.subject.ownership_generation,
                certificate.version,
                certificate.policy_version,
                certificate.work_graph_digest,
                certificate.certificate_digest,
            ),
        )
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json,
                causation_id, correlation_id, trace_id,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, 100, ?, ?, ?, ?,
                      'pending', 0, 100, '', '', NULL, 100, NULL, '')
            """,
            (
                payload.event_id,
                certificate.subject.profile_id,
                certificate.subject.session_id,
                certificate.subject.ownership_generation,
                RECOVERY_DELIVERY_EVENT_KIND,
                RECOVERY_DELIVERY_EVENT_SOURCE,
                payload_json,
                payload.case_id,
                payload.case_id,
                payload.event_id,
            ),
        )
        mailbox_id = int(inserted.lastrowid)
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET next_delivery_cycle = 1,
                delivery_count = 1,
                last_event_id = ?,
                updated_at = 101
            WHERE case_id = ?
            """,
            (payload.event_id, certificate.case_identity.case_id),
        )
    if historical_evidence == "unfenced_legacy":
        database.actor_v2_mailbox_handoffs.record_unfenced_legacy_handoff(mailbox_id)

    replay = scanner.scan()

    assert replay.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    handoff = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    if historical_evidence == "missing":
        assert handoff is None
    else:
        assert handoff is not None
        assert handoff.evidence.state is MailboxHandoffEvidenceState.UNFENCED_LEGACY
        assert handoff.state is MailboxHandoffState.BLOCKED


@pytest.mark.parametrize("fence_state", ["revoked", "expired", "missing"])
async def test_invalid_admission_fence_blocks_recovery_discovery_writes(
    tmp_path: Path,
    fence_state: str,
) -> None:
    """A stale Actor owner cannot create a recovery case, finding, or mailbox row."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", f"fenced-recovery-{fence_state}")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="recovery-fence-test",
        ttl_seconds=3600.0,
    )
    await _seed_orphaned_review(
        database,
        key=key,
        admission_grant=grant,
    )
    expected_reason = _invalidate_admission_fence(
        database,
        grant,
        state=fence_state,
    )

    summary = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert len(summary.results) == 1
    assert summary.results[0].key == key
    assert summary.results[0].disposition is RecoveryScanDisposition.SKIPPED
    assert summary.results[0].reason_codes == (expected_reason,)
    with database.connect() as conn:
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_recovery_cases),
                (SELECT COUNT(*) FROM agent_session_recovery_findings),
                (
                    SELECT COUNT(*)
                    FROM agent_session_mailbox
                    WHERE kind = ? AND source = ?
                )
            """,
            (RECOVERY_DELIVERY_EVENT_KIND, RECOVERY_DELIVERY_EVENT_SOURCE),
        ).fetchone()
    assert tuple(counts) == (0, 0, 0)


async def test_reader_maps_missing_admission_fence_to_not_eligible(
    tmp_path: Path,
) -> None:
    """Raw recovery authority never leaks a fence-specific ownership exception."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "reader-missing-recovery-fence")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="reader-fence-test",
        ttl_seconds=3600.0,
    )
    generation = await _seed_orphaned_review(
        database,
        key=key,
        admission_grant=grant,
    )
    _invalidate_admission_fence(database, grant, state="missing")

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryGraphNotEligible) as raised:
            SQLiteRecoveryGraphReader(database).rebuild_certificate(
                conn,
                key=key,
                ownership_generation=generation,
            )
        conn.execute("ROLLBACK")

    assert raised.value.reason_code == "actor_v2_ownership_changed"


@pytest.mark.parametrize("fence_state", ["revoked", "expired", "missing", "invalid"])
async def test_pending_recovery_wake_query_excludes_non_live_fenced_debt(
    tmp_path: Path,
    fence_state: str,
) -> None:
    """A retained recovery mailbox cannot wake a revoked actor incarnation."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", f"pending-recovery-{fence_state}")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="pending-recovery-wake-test",
        ttl_seconds=3600.0,
    )
    generation = await _seed_orphaned_review(
        database,
        key=key,
        admission_grant=grant,
    )
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    delivered = scanner.scan()
    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=generation,
        admission_fence_id=grant.fence.fence_id,
        admission_fence_generation=grant.fence.generation,
    )
    assert delivered.wake_requests == (request,)
    assert scanner.pending_recovery_wake_requests() == (request,)

    if fence_state == "invalid":
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_session_runtime_ownership
                SET admission_fence_id = 'other-admission-fence'
                WHERE profile_id = ? AND session_id = ? AND generation = ?
                """,
                (key.profile_id, key.session_id, generation),
            )
    else:
        _invalidate_admission_fence(database, grant, state=fence_state)

    assert scanner.pending_recovery_wake_requests() == ()
    assert not scanner.is_pending_recovery_wake_request(request)


async def test_pending_recovery_wake_debt_uses_latest_event_and_keyset_cursor(
    tmp_path: Path,
) -> None:
    """Consumption before a later keyset page cannot skip a newer debt event."""

    database = _make_database(tmp_path)
    store = SQLiteSessionActorStore(database, clock=lambda: 10.0)
    first_key = SessionKey("profile-a", "recovery-keyset-000")
    shared_key = SessionKey("profile-a", "recovery-keyset-001")
    last_key = SessionKey("profile-a", "recovery-keyset-002")

    async def seed_mailbox(key: SessionKey, event_ids: tuple[str, ...]) -> None:
        generation = database.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="recovery keyset debt test",
        ).ownership.generation
        await store.ensure(key, ownership_generation=generation)
        with database.connect() as conn:
            for event_id in event_ids:
                conn.execute(
                    """
                    INSERT INTO agent_session_mailbox (
                        event_id, profile_id, session_id, ownership_generation,
                        kind, source, occurred_at, payload_json, causation_id,
                        correlation_id, trace_id, status, attempt_count,
                        available_at, claim_id, lease_owner, lease_until,
                        created_at, handled_at, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, 10, '{}', '', '', '', 'pending',
                              0, 10, '', '', NULL, 10, NULL, '')
                    """,
                    (
                        event_id,
                        key.profile_id,
                        key.session_id,
                        generation,
                        RECOVERY_DELIVERY_EVENT_KIND,
                        RECOVERY_DELIVERY_EVENT_SOURCE,
                    ),
                )

    await seed_mailbox(first_key, ("recovery-keyset:first",))
    await seed_mailbox(
        shared_key,
        ("recovery-keyset:shared-old", "recovery-keyset:shared-new"),
    )
    await seed_mailbox(last_key, ("recovery-keyset:last",))
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)

    first_page = scanner.pending_recovery_wake_debts(limit=2)

    assert tuple(debt.event_id for debt in first_page) == (
        "recovery-keyset:first",
        "recovery-keyset:shared-new",
    )
    assert first_page[1].request.key == shared_key
    with database.connect() as conn:
        conn.execute(
            "DELETE FROM agent_session_mailbox WHERE event_id = 'recovery-keyset:first'"
        )

    second_page = scanner.pending_recovery_wake_debts(
        limit=2,
        after=first_page[-1].cursor,
    )

    assert tuple(debt.event_id for debt in second_page) == ("recovery-keyset:last",)
    assert scanner.pending_recovery_wake_debts(limit=2, offset=2) == ()


async def test_final_admission_gate_rolls_back_candidate_side_effects(
    tmp_path: Path,
) -> None:
    """A fence lost during sidecar staging rolls back every candidate write."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "recovery-final-gate")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="recovery-final-gate-test",
        ttl_seconds=3600.0,
    )
    await _seed_orphaned_review(database, key=key, admission_grant=grant)
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER remove_recovery_admission_fence_before_final_gate
            AFTER INSERT ON agent_session_mailbox_handoffs
            BEGIN
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = NEW.profile_id
                  AND session_id = NEW.session_id;
            END
            """
        )

    summary = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert len(summary.results) == 1
    assert summary.results[0].key == key
    assert summary.results[0].disposition is RecoveryScanDisposition.SKIPPED
    assert summary.results[0].reason_codes == ("admission_fence_missing",)
    with database.connect() as conn:
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_recovery_cases),
                (SELECT COUNT(*) FROM agent_session_recovery_findings),
                (
                    SELECT COUNT(*)
                    FROM agent_session_mailbox
                    WHERE kind = ? AND source = ?
                ),
                (SELECT COUNT(*) FROM agent_session_mailbox_handoffs)
            """,
            (RECOVERY_DELIVERY_EVENT_KIND, RECOVERY_DELIVERY_EVENT_SOURCE),
        ).fetchone()
        fence = conn.execute(
            """
            SELECT status
            FROM agent_session_actor_v2_admission_fences
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert counts is not None
    assert tuple(counts) == (0, 0, 0, 0)
    assert fence is not None
    assert str(fence["status"]) == "committed"


async def test_recovery_candidate_discovery_rotates_beyond_one_bounded_page(
    tmp_path: Path,
) -> None:
    """A continuously non-idle first page cannot starve later recovery candidates."""

    database = _make_database(tmp_path)
    keys = tuple(
        SessionKey("profile-a", f"recovery-candidate-{index:03d}")
        for index in range(MAX_RECOVERY_SCAN_CANDIDATES + 1)
    )
    store = SQLiteSessionActorStore(database, clock=lambda: 10.0)
    for key in keys:
        generation = database.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="recovery candidate rotation test",
        ).ownership.generation
        await store.ensure(key, ownership_generation=generation)
        with database.connect() as conn:
            conn.execute(
                """
                UPDATE agent_session_aggregates
                SET state = 'review', state_revision = 1, updated_at = 10
                WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
                """,
                (key.profile_id, key.session_id, generation),
            )
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)

    first = scanner.scan(limit=MAX_RECOVERY_SCAN_CANDIDATES, profile_id="profile-a")
    second = scanner.scan(limit=MAX_RECOVERY_SCAN_CANDIDATES, profile_id="profile-a")

    assert len(first.results) == MAX_RECOVERY_SCAN_CANDIDATES
    assert len(second.results) == MAX_RECOVERY_SCAN_CANDIDATES
    assert keys[-1] not in {result.key for result in first.results}
    assert keys[-1] in {result.key for result in second.results}


async def test_scanner_does_not_exhaust_an_in_flight_final_delivery_cycle(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(
        database,
        clock=lambda: 100.0,
        max_delivery_cycles=1,
    )
    store = SQLiteSessionActorStore(
        database,
        clock=lambda: 200.0,
        retry_delay_seconds=0.0,
    )

    first = scanner.scan()
    claim = await store.claim_next(key, worker_id="recovery-claim-worker")
    assert claim is not None
    second = scanner.scan()
    await store.release(claim, error="retry recovery delivery")
    third = scanner.scan()
    retried_claim = await store.claim_next(key, worker_id="recovery-claim-worker")
    assert retried_claim is not None
    await store.fail(retried_claim, error="recovery delivery exhausted")
    final = scanner.scan()

    assert first.delivered_count == 1
    assert second.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    assert third.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    assert final.results[0].disposition is RecoveryScanDisposition.DELIVERY_EXHAUSTED
    with database.connect() as conn:
        case = conn.execute(
            """
            SELECT status, delivery_count
            FROM agent_session_recovery_cases
            """
        ).fetchone()
    assert case is not None
    assert tuple(case) == ("delivery_exhausted", 1)


async def test_reader_is_read_only_and_scanner_delegates_to_its_authority(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    policy = RecoveryV1Policy()
    reader = SQLiteRecoveryGraphReader(database, policy=policy)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0, policy=policy)
    before = _durable_authority_snapshot(
        database,
        key=key,
        ownership_generation=generation,
    )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        direct = reader.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        delegated = scanner.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        port = scanner.graph_reader.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")

    assert reader.persistence_domain is database
    assert scanner.graph_reader.persistence_domain is database
    assert reader.policy is policy
    assert scanner.policy is policy
    assert scanner.graph_reader.policy is policy
    assert direct.certificate_digest == delegated.certificate_digest
    assert direct.certificate_digest == port.certificate_digest
    assert all("row_id" not in node.facts for node in direct.nodes)
    assert (
        _durable_authority_snapshot(
            database,
            key=key,
            ownership_generation=generation,
        )
        == before
    )


async def test_reader_reports_corruption_without_recording_a_finding(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    reader = SQLiteRecoveryGraphReader(database)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET data_json = '{"duplicate":1,"duplicate":2}'
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryGraphReadError) as raised:
            reader.rebuild_certificate(
                conn,
                key=key,
                ownership_generation=generation,
            )
        conn.execute("ROLLBACK")

    assert raised.value.code == "recovery_authority_json_invalid"
    with database.connect() as conn:
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_mailbox),
                (SELECT COUNT(*) FROM agent_session_recovery_cases),
                (SELECT COUNT(*) FROM agent_session_recovery_findings)
            """
        ).fetchone()
    assert counts is not None
    assert tuple(counts) == (0, 0, 0)


async def test_reader_requires_a_caller_owned_transaction(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)

    with database.connect() as conn:
        with pytest.raises(ValueError, match="caller-owned transaction"):
            SQLiteRecoveryGraphReader(database).rebuild_certificate(
                conn,
                key=key,
                ownership_generation=generation,
            )


async def test_reader_marks_an_idle_aggregate_as_not_eligible(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="recovery graph reader ineligibility test",
    ).ownership.generation
    await SQLiteSessionActorStore(database, clock=lambda: 10.0).ensure(
        key,
        ownership_generation=generation,
    )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryGraphNotEligible) as raised:
            SQLiteRecoveryGraphReader(database).rebuild_certificate(
                conn,
                key=key,
                ownership_generation=generation,
            )
        conn.execute("ROLLBACK")

    assert raised.value.reason_code == "aggregate_idle"


async def test_scanner_rolls_back_partial_delivery_before_recording_finding(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER mutate_recovery_delivery_payload
            AFTER INSERT ON agent_session_mailbox
            WHEN NEW.source = 'durable_session_recovery_scanner'
            BEGIN
                UPDATE agent_session_mailbox
                SET payload_json = '{}'
                WHERE mailbox_id = NEW.mailbox_id;
            END
            """
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.finding_count == 1
    assert result.results[0].reason_codes == ("recovery_delivery_immutable_value_conflict",)
    with database.connect() as conn:
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_mailbox),
                (SELECT COUNT(*) FROM agent_session_recovery_cases),
                (SELECT COUNT(*) FROM agent_session_recovery_findings)
            """
        ).fetchone()
    assert counts is not None
    assert tuple(counts) == (0, 0, 1)


async def test_reader_validates_a_claimed_recovery_delivery_from_raw_authority(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scan = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    assert scan.delivered_count == 1
    store = SQLiteSessionActorStore(database, clock=lambda: 200.0)
    claim = await store.claim_next(key, worker_id="recovery-commit-worker")
    assert claim is not None
    reader = SQLiteRecoveryGraphReader(database)

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        validated = reader.validate_claimed_delivery(
            conn,
            claim=claim,
            commit_now=200.0,
        )
        conn.execute("ROLLBACK")

    assert validated.mailbox_id > 0
    assert validated.delivery.event_id == claim.envelope.event_id
    assert validated.delivery.case_id == claim.envelope.causation_id


async def test_reader_loads_a_raw_case_snapshot_for_claimed_delivery(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scan = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    assert scan.delivered_count == 1
    case_id = scan.results[0].case_id
    assert case_id

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        snapshot = SQLiteRecoveryGraphReader(database).load_case_snapshot(
            conn,
            case_id=case_id,
        )
        conn.execute("ROLLBACK")

    assert snapshot is not None
    assert snapshot.case_id == case_id
    assert snapshot.status == "open"
    assert snapshot.delivery_count == 1
    assert snapshot.next_delivery_cycle == 1
    assert snapshot.last_event_id.startswith("recovery-requested:v1:")


async def test_reader_rejects_a_recovery_case_storage_alias(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scan = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    case_id = scan.results[0].case_id
    assert case_id
    with database.connect() as conn:
        conn.execute("DROP TRIGGER trg_agent_recovery_case_identity_immutable")
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET work_graph_digest = CAST(work_graph_digest AS BLOB)
            WHERE case_id = ?
            """,
            (case_id,),
        )
        conn.execute("PRAGMA ignore_check_constraints = OFF")

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryGraphReadError) as raised:
            SQLiteRecoveryGraphReader(database).load_case_snapshot(
                conn,
                case_id=case_id,
            )
        conn.execute("ROLLBACK")

    assert raised.value.code == "recovery_authority_text_storage_class_invalid"


async def test_reader_rejects_a_claim_that_changed_after_actor_load(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    claim = await SQLiteSessionActorStore(database, clock=lambda: 200.0).claim_next(
        key,
        worker_id="recovery-commit-worker",
    )
    assert claim is not None
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET attempt_count = attempt_count + 1
            WHERE event_id = ?
            """,
            (claim.envelope.event_id,),
        )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryDeliveryClaimLost) as raised:
            SQLiteRecoveryGraphReader(database).validate_claimed_delivery(
                conn,
                claim=claim,
                commit_now=200.0,
            )
        conn.execute("ROLLBACK")

    assert raised.value.code == "recovery_delivery_claim_attempt_count_changed"


async def test_scanner_records_raw_json_finding_and_resolves_after_repair(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET data_json = '{"duplicate":1,"duplicate":2}'
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    broken = scanner.scan()

    assert broken.finding_count == 1
    assert broken.results[0].disposition is RecoveryScanDisposition.FINDING_RECORDED
    with database.connect() as conn:
        finding = conn.execute(
            """
            SELECT code, status, occurrence_count
            FROM agent_session_recovery_findings
            """
        ).fetchone()
        mailbox_count = conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()
    assert finding is not None
    assert str(finding["code"]) == "recovery_authority_json_invalid"
    assert tuple(finding[name] for name in ("status", "occurrence_count")) == (
        "open",
        1,
    )
    assert mailbox_count is not None
    assert int(mailbox_count[0]) == 0

    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET data_json = ?
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            """,
            (
                _review_operation_fence_data(generation),
                key.profile_id,
                key.session_id,
                generation,
            ),
        )

    repaired = scanner.scan()

    assert repaired.delivered_count == 1
    with database.connect() as conn:
        finding = conn.execute(
            """
            SELECT status, resolved_at
            FROM agent_session_recovery_findings
            """
        ).fetchone()
    assert finding is not None
    assert finding["status"] == "resolved"
    assert finding["resolved_at"] is not None


async def test_scanner_blocks_review_without_its_required_operation(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET review_operation_id = '', data_json = '{}'
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_state_requires_operation" in result.results[0].reason_codes
    with database.connect() as conn:
        case = conn.execute(
            "SELECT status, delivery_count FROM agent_session_recovery_cases"
        ).fetchone()
    assert case is not None
    assert tuple(case) == ("scanner_blocked", 0)


async def test_scanner_blocks_terminal_operation_referenced_by_review(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations
            SET status = 'completed', finished_at = 20
            WHERE operation_id = 'review-operation'
            """
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_references_terminal_operation" in result.results[0].reason_codes


async def test_scanner_leaves_quiescent_active_chat_without_recovery(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="quiescent active chat recovery scanner test",
    ).ownership.generation
    await SQLiteSessionActorStore(database, clock=lambda: 10.0).ensure(
        key,
        ownership_generation=generation,
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = 'active_chat', state_revision = 1,
                active_chat_state_json = '{"bootstrap_status":"completed"}',
                updated_at = 10
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.NO_RECOVERY
    with database.connect() as conn:
        case_count = conn.execute("SELECT COUNT(*) FROM agent_session_recovery_cases").fetchone()
    assert case_count is not None
    assert int(case_count[0]) == 0


async def test_scanner_blocks_unknown_external_action_receipt(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_external_action_receipts (
                idempotency_key, effect_id, operation_id, profile_id, session_id,
                ownership_generation, action_ordinal, action_kind, contract_version,
                request_digest, request_json, status, attempt_count, claim_id,
                lease_owner, lease_until, platform_result_json, rejection_json,
                unknown_json, prepared_at, execution_started_at, settled_at, updated_at
            ) VALUES (
                'external-action-idempotency', 'external-action-effect',
                'review-operation', ?, ?, ?, 0, 'send_poke', 1, ?, '{}',
                'unknown', 1, 'external-action-claim', 'external-action-worker',
                NULL, '{}', '{}', '{"reason":"ambiguous"}', 10, 11, 12, 12
            )
            """,
            (key.profile_id, key.session_id, generation, "a" * 64),
        )
        conn.execute(
            """
            INSERT INTO agent_external_action_attempts (
                idempotency_key, attempt_count, claim_id, lease_owner, claimed_at,
                lease_until, status, platform_result_json, rejection_json,
                unknown_json, settled_at
            ) VALUES (
                'external-action-idempotency', 1, 'external-action-claim',
                'external-action-worker', 11, 20, 'unknown', '{}', '{}',
                '{"reason":"ambiguous"}', 12
            )
            """
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "external_action_unknown" in result.results[0].reason_codes


async def test_scanner_blocks_orphan_without_its_terminal_workflow_effect(
    tmp_path: Path,
) -> None:
    """A root operation alone cannot authorize no-replay materialization."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    with database.connect() as conn:
        conn.execute("DELETE FROM agent_effect_outbox WHERE effect_id = 'review-effect'")

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "recovery_expected_effect_conflict" in result.results[0].reason_codes


async def test_scanner_blocks_unreferenced_live_operation(
    tmp_path: Path,
) -> None:
    """A second live operation makes the aggregate shape ambiguous."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, metadata_json
            ) VALUES ('unreferenced-live-operation', ?, ?, ?, 'review', 'pending',
                      'unreferenced-launch', 1, 0, 0, 0, 0, 10, '{}')
            """,
            (key.profile_id, key.session_id, generation),
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_unreferenced_live_operation" in result.results[0].reason_codes


async def test_scanner_waits_for_a_running_operation_lease(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations
            SET status = 'running', lease_owner = 'workflow-worker', lease_until = 200
            WHERE operation_id = 'review-operation'
            """
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.WAITING
    assert result.results[0].reason_codes == ("running_operation_lease",)


async def test_scanner_ignores_many_historical_cancelled_gates_with_terminal_proof(
    tmp_path: Path,
) -> None:
    """Safe legacy gate rows do not consume the bounded recovery authority budget."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    for token in range(9):
        _insert_safe_historical_cancelled_gate(
            database,
            key=key,
            ownership_generation=generation,
            token=token,
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 1
    assert result.results[0].disposition is RecoveryScanDisposition.DELIVERED
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert not any(node.kind == "review_cancellation_gate" for node in certificate.nodes)


async def test_scanner_projects_an_unsafe_gate_after_many_safe_historical_rows(
    tmp_path: Path,
) -> None:
    """Legacy compatibility filtering cannot hide a later malformed control fence."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    for token in range(9):
        _insert_safe_historical_cancelled_gate(
            database,
            key=key,
            ownership_generation=generation,
            token=token,
        )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET payload_json = '{}'
            WHERE profile_id = ?
              AND session_id = ?
              AND effect_id = 'historical-cancel-review:8'
            """,
            (key.profile_id, key.session_id),
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "review_cancellation_gate_control_conflict" in result.results[0].reason_codes


async def test_scanner_waits_for_remote_review_execution_after_outbox_cancellation(
    tmp_path: Path,
) -> None:
    """A cancelled old review remains live until its durable task witness ends."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review_with_cancellation_liveness(
        database,
        key=key,
        gate_status="cancelled",
        include_running_witness=True,
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.WAITING
    assert "review_execution_witness_running" in result.results[0].reason_codes
    with database.connect() as conn:
        case_count = conn.execute("SELECT COUNT(*) FROM agent_session_recovery_cases").fetchone()
        mailbox_count = conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert case_count is not None
    assert mailbox_count is not None
    assert int(case_count[0]) == 0
    assert int(mailbox_count[0]) == 0
    assert any(
        node.kind == "review_execution_run" and node.status == "running"
        for node in certificate.nodes
    )
    assert any(edge.relation == "executes" for edge in certificate.edges)


async def test_scanner_blocks_unknown_review_execution_after_outbox_cancellation(
    tmp_path: Path,
) -> None:
    """An expired execution witness is a blocker, not recoverable waiting work."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review_with_cancellation_liveness(
        database,
        key=key,
        gate_status="cancelled",
        include_running_witness=True,
        witness_status="unknown",
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "review_execution_witness_unknown" in result.results[0].reason_codes
    with database.connect() as conn:
        case = conn.execute(
            """
            SELECT status, delivery_count, last_error
            FROM agent_session_recovery_cases
            """
        ).fetchone()
        mailbox_count = conn.execute("SELECT COUNT(*) FROM agent_session_mailbox").fetchone()
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert case is not None
    assert mailbox_count is not None
    assert tuple(case[:2]) == ("scanner_blocked", 0)
    assert "review_execution_witness_unknown" in str(case["last_error"])
    assert int(mailbox_count[0]) == 0
    assert any(
        node.kind == "review_execution_run" and node.status == "unknown"
        for node in certificate.nodes
    )


async def test_scanner_waits_for_a_running_generic_model_execution(
    tmp_path: Path,
) -> None:
    """A remote active-reply model task is waiting work, never replayable work."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review_with_model_liveness(
        database,
        key=key,
        witness_status="running",
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.WAITING
    assert "model_execution_witness_running" in result.results[0].reason_codes
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert any(
        node.kind == "model_execution_run" and node.status == "running"
        for node in certificate.nodes
    )
    assert any(edge.relation == "executes" for edge in certificate.edges)


async def test_scanner_blocks_an_unknown_generic_model_execution(
    tmp_path: Path,
) -> None:
    """Unknown generic model work requires reconciliation rather than recovery."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review_with_model_liveness(
        database,
        key=key,
        witness_status="unknown",
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "model_execution_witness_unknown" in result.results[0].reason_codes
    with database.connect() as conn:
        case = conn.execute(
            """
            SELECT status, delivery_count, last_error
            FROM agent_session_recovery_cases
            """
        ).fetchone()
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert case is not None
    assert tuple(case[:2]) == ("scanner_blocked", 0)
    assert "model_execution_witness_unknown" in str(case["last_error"])
    assert any(
        node.kind == "model_execution_run" and node.status == "unknown"
        for node in certificate.nodes
    )


async def test_scanner_waits_for_a_requested_model_execution_cancellation_gate(
    tmp_path: Path,
) -> None:
    """A claimed target that has not started is gated rather than replayed."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review_with_model_cancellation_gate(
        database,
        key=key,
        gate_status="requested",
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.WAITING
    assert "model_execution_cancellation_gate_requested" in result.results[0].reason_codes
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert any(
        node.kind == "model_execution_cancellation_gate" and node.status == "requested"
        for node in certificate.nodes
    )
    assert any(edge.relation == "cancels_effect" for edge in certificate.edges)
    assert any(
        edge.relation == "awaits_cancellation_control" for edge in certificate.edges
    )


async def test_scanner_keeps_a_terminal_model_gate_reachable_until_control_completes(
    tmp_path: Path,
) -> None:
    """A quiescent target still needs its pending control completion delivered."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review_with_model_cancellation_gate(
        database,
        key=key,
        gate_status="terminal",
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.WAITING
    assert "model_execution_cancellation_control_live" in result.results[0].reason_codes
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert any(
        node.kind == "model_execution_cancellation_gate" and node.status == "terminal"
        for node in certificate.nodes
    )
    assert any(
        edge.relation == "awaits_cancellation_control" for edge in certificate.edges
    )


async def test_scanner_waits_for_remote_model_execution_after_gate_cancellation(
    tmp_path: Path,
) -> None:
    """A gated cancelled outbox may retain a remote running model witness."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review_with_model_cancellation_gate(
        database,
        key=key,
        gate_status="cancelled",
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.WAITING
    assert "model_execution_cancellation_gate_cancelled" in result.results[0].reason_codes
    assert "model_execution_witness_running" in result.results[0].reason_codes
    assert "model_execution_witness_claim_conflict" not in result.results[0].reason_codes
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert any(
        node.kind == "model_execution_cancellation_gate" and node.status == "cancelled"
        for node in certificate.nodes
    )
    assert any(
        node.kind == "model_execution_run" and node.status == "running"
        for node in certificate.nodes
    )


async def test_scanner_blocks_a_model_execution_cancellation_gate_with_unknown_witness(
    tmp_path: Path,
) -> None:
    """A gate with an unknown remote execution is explicit blocking authority."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review_with_model_cancellation_gate(
        database,
        key=key,
        gate_status="blocked",
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "model_execution_cancellation_gate_blocked" in result.results[0].reason_codes
    assert "model_execution_witness_unknown" in result.results[0].reason_codes
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = SQLiteRecoveryGraphReader(database).rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    assert any(
        node.kind == "model_execution_cancellation_gate" and node.status == "blocked"
        for node in certificate.nodes
    )


async def test_scanner_waits_for_an_unresolved_review_cancellation_gate(
    tmp_path: Path,
) -> None:
    """A requested gate without a started task is still pending durable work."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review_with_cancellation_liveness(
        database,
        key=key,
        gate_status="requested",
        include_running_witness=False,
    )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.WAITING
    assert "review_cancellation_gate_requested" in result.results[0].reason_codes


async def test_scanner_blocks_a_review_gate_with_a_tampered_target_fence(
    tmp_path: Path,
) -> None:
    """A liveness row with a mismatched immutable review fence cannot be ignored."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review_with_cancellation_liveness(
        database,
        key=key,
        gate_status="cancelled",
        include_running_witness=True,
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_review_cancellation_gates
            SET review_contract_signature = 'tampered-review-contract'
            WHERE cancellation_effect_id = 'cancel-old-review'
            """
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "review_cancellation_gate_target_conflict" in result.results[0].reason_codes


async def test_scanner_blocks_cross_generation_review_liveness_evidence(
    tmp_path: Path,
) -> None:
    """A new aggregate generation cannot hide an old unresolved review task."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    stale_generation = generation + 1
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_review_cancellation_gates (
                profile_id, session_id, ownership_generation,
                cancellation_effect_id, request_event_id,
                review_operation_id, review_effect_id, review_effect_kind,
                review_contract_version, review_contract_signature,
                gate_status, target_effect_status, target_effect_claim_id,
                target_effect_attempt_count, target_effect_terminal_at,
                created_at, updated_at
            ) VALUES (?, ?, ?, 'stale-cancel-review', 'stale-interrupt',
                      'stale-review-operation', 'stale-review-effect',
                      'run_review_workflow', 1, 'stale-review-contract',
                      'requested', 'processing', 'stale-claim', 1, NULL, 10, 10)
            """,
            (key.profile_id, key.session_id, stale_generation),
        )
        conn.execute(
            """
            INSERT INTO agent_review_execution_runs (
                profile_id, session_id, ownership_generation,
                review_effect_id, review_operation_id, review_effect_kind,
                review_contract_version, review_contract_signature,
                claim_id, worker_id, execution_status, started_at
            ) VALUES (?, ?, ?, 'stale-review-effect', 'stale-review-operation',
                      'run_review_workflow', 1, 'stale-review-contract',
                      'stale-claim', 'stale-worker', 'running', 10)
            """,
            (key.profile_id, key.session_id, stale_generation),
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "review_cancellation_gate_generation_conflict" in result.results[0].reason_codes
    assert "review_execution_witness_generation_conflict" in result.results[0].reason_codes


async def test_scanner_records_delivery_logical_key_storage_alias(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = scanner.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    payload = RecoveryDeliveryPayload(certificate=certificate, delivery_cycle=0)
    payload_json = canonical_recovery_json(payload.to_record())
    with database.connect() as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json, causation_id,
                correlation_id, trace_id, status, attempt_count, available_at,
                claim_id, lease_owner, lease_until, created_at, handled_at, last_error
            ) VALUES (
                CAST(? AS BLOB), CAST(? AS BLOB), CAST(? AS BLOB), ?, ?, ?,
                100.0, ?, ?, ?, ?, 'pending', 0, 100.0, '', '', NULL, 100.0,
                NULL, ''
            )
            """,
            (
                payload.event_id,
                key.profile_id,
                key.session_id,
                generation,
                RECOVERY_DELIVERY_EVENT_KIND,
                RECOVERY_DELIVERY_EVENT_SOURCE,
                payload_json,
                payload.case_id,
                payload.case_id,
                payload.event_id,
            ),
        )
        conn.execute("PRAGMA foreign_keys = ON")

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.finding_count == 1
    assert result.results[0].disposition is RecoveryScanDisposition.FINDING_RECORDED
    assert result.results[0].reason_codes == ("recovery_delivery_storage_class_conflict",)
    with database.connect() as conn:
        case_count = conn.execute("SELECT COUNT(*) FROM agent_session_recovery_cases").fetchone()
    assert case_count is not None
    assert int(case_count[0]) == 0


async def test_scanner_records_bounded_mailbox_row_overflow(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        for index in range(9):
            conn.execute(
                """
                INSERT INTO agent_session_mailbox (
                    event_id, profile_id, session_id, ownership_generation,
                    kind, source, occurred_at, payload_json, causation_id,
                    correlation_id, trace_id, status, attempt_count, available_at,
                    claim_id, lease_owner, lease_until, created_at, handled_at,
                    last_error
                ) VALUES (?, ?, ?, ?, 'MessageReceived', 'test', 10, '{}', '', '',
                          '', 'pending', 0, 10, '', '', NULL, 10, NULL, '')
                """,
                (f"ordinary-mailbox-{index}", key.profile_id, key.session_id, generation),
            )

    result = scanner.scan()

    assert result.finding_count == 1
    assert result.results[0].disposition is RecoveryScanDisposition.FINDING_RECORDED
    assert result.results[0].reason_codes == ("recovery_authority_row_limit_exceeded",)


async def test_scanner_records_raw_operation_identity_alias(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, started_at, metadata_json
            ) VALUES (CAST(? AS BLOB), ?, ?, ?, 'review', 'pending',
                      'aliased-review-launch', 1, 0, 0, 10, '{}')
            """,
            ("review-operation", key.profile_id, key.session_id, generation),
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.finding_count == 1
    assert result.results[0].disposition is RecoveryScanDisposition.FINDING_RECORDED
    assert result.results[0].reason_codes == ("recovery_authority_text_storage_class_invalid",)


async def test_scanner_blocks_missing_transition_journal_tail(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET event_sequence = 1
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_transition_tail_missing" in result.results[0].reason_codes


async def test_scanner_blocks_operation_kind_incompatible_with_state(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations
            SET kind = 'active_chat_round'
            WHERE operation_id = 'review-operation'
            """
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_operation_kind_conflict" in result.results[0].reason_codes
