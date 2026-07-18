"""Integration tests for durable Agent runtime ownership activation gates."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
    RecoveryAggregateFence,
    RecoveryDecision,
    RecoveryDecisionKind,
    RecoveryDeliveryPayload,
    RecoveryGraphNode,
    RecoverySubject,
    build_recovery_certificate,
    canonical_recovery_json,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipConflict,
    AgentRuntimeOwnershipEventType,
    AgentRuntimeOwnershipEvidenceConflict,
    AgentRuntimeOwnershipGenerationConflict,
    AgentRuntimeOwnershipMigrationConflict,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipRequired,
    AgentRuntimeOwnershipStatus,
)
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffReceipt,
    MailboxHandoffTarget,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)


def _database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _insert_actor_aggregate(
    database: DatabaseManager,
    key: SessionKey,
    *,
    mailbox: bool = False,
    ownership_generation: int = 0,
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 1.0, 1.0)
            """,
            (key.profile_id, key.session_id, ownership_generation),
        )
        if mailbox:
            conn.execute(
                """
                INSERT INTO agent_session_mailbox (
                    event_id, profile_id, session_id, kind, occurred_at,
                    available_at, created_at
                ) VALUES ('event-1', ?, ?, 'MessageReceived', 1.0, 1.0, 1.0)
                """,
                (key.profile_id, key.session_id),
            )


def _insert_actor_mailbox(
    database: DatabaseManager,
    key: SessionKey,
    *,
    event_id: str,
    ownership_generation: int,
    status: str = "pending",
) -> int:
    """Insert one actor mailbox row for ownership-refence boundary coverage."""

    claim_id = "mailbox-claim" if status == "processing" else ""
    lease_owner = "mailbox-worker" if status == "processing" else ""
    lease_until = 10.0 if status == "processing" else None
    with database.connect() as conn:
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, occurred_at, status, attempt_count, available_at,
                claim_id, lease_owner, lease_until, created_at
            ) VALUES (?, ?, ?, ?, 'MessageReceived', 1.0, ?, 1, 1.0,
                      ?, ?, ?, 1.0)
            """,
            (
                event_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                status,
                claim_id,
                lease_owner,
                lease_until,
            ),
        )
    assert inserted.lastrowid is not None
    return int(inserted.lastrowid)


def _insert_mailbox_handoff(
    database: DatabaseManager,
    key: SessionKey,
    *,
    mailbox_id: int,
    event_id: str,
    ownership_generation: int,
    evidence_state: str,
    state: str,
) -> None:
    """Insert schema-valid immutable handoff evidence for migration boundaries."""

    if evidence_state == "fenced" and state == "pending":
        values = (
            f"handoff:{mailbox_id}",
            "fenced",
            "fence:ownership-boundary",
            1,
            "pending",
            "",
            "",
            None,
            "",
            "",
            "",
            None,
        )
    elif evidence_state == "fenced" and state == "settled":
        values = (
            f"handoff:{mailbox_id}",
            "fenced",
            "fence:ownership-boundary",
            1,
            "settled",
            "",
            "",
            None,
            "target:ownership-boundary",
            "incarnation:ownership-boundary",
            "accepted",
            2.0,
        )
    elif evidence_state in {"unknown", "unfenced_legacy"} and state == "blocked":
        values = (
            f"handoff:{mailbox_id}",
            evidence_state,
            "",
            0,
            "blocked",
            "",
            "",
            None,
            "",
            "",
            "",
            None,
        )
    else:
        raise ValueError("unsupported mailbox handoff test state")

    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox_handoffs (
                mailbox_id, handoff_id,
                profile_id, session_id, event_id, ownership_generation,
                evidence_state, admission_fence_id, admission_fence_generation,
                state, attempt_count, available_at,
                claim_id, lease_owner, lease_until,
                target_id, target_incarnation_id, target_disposition,
                created_at, updated_at, claimed_at, settled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1.0,
                      ?, ?, ?, ?, ?, ?, 1.0, 1.0, NULL, ?, '')
            """,
            (
                mailbox_id,
                values[0],
                key.profile_id,
                key.session_id,
                event_id,
                ownership_generation,
                values[1],
                values[2],
                values[3],
                values[4],
                values[5],
                values[6],
                values[7],
                values[8],
                values[9],
                values[10],
                values[11],
            ),
        )


def _insert_review_cancellation_evidence(
    database: DatabaseManager,
    key: SessionKey,
    *,
    target_generation: int,
    gate_generation: int | None,
    run_generation: int | None,
    run_status: str = "cancelled",
) -> tuple[str, str, str]:
    """Insert one review-cancellation boundary with exact durable identities."""

    review_effect_id = "review-effect:ownership-boundary"
    cancellation_effect_id = "cancel-review-effect:ownership-boundary"
    review_operation_id = "review-operation:ownership-boundary"
    request_event_id = "review-cancel-request:ownership-boundary"
    claim_id = "expired-review-claim"
    review_contract = builtin_effect_contract("run_review_workflow")
    cancellation_contract = builtin_effect_contract("cancel_review_workflow")
    finished_at = None if run_status in {"running", "unknown"} else 20.0
    unknown_at = 20.0 if run_status == "unknown" else None
    unknown_reason = (
        "review_execution_lease_expired_before_handler_terminal" if run_status == "unknown" else ""
    )
    cancellation_payload = json.dumps(
        {
            "operation_id": review_operation_id,
            "cancelled_operation_fence": {
                "operation_id": review_operation_id,
                "effect_id": review_effect_id,
                "effect_kind": review_contract.effect_kind,
                "contract_version": review_contract.version,
                "contract_signature": review_contract.signature,
                "ownership_generation": target_generation,
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
                kind, status, started_at, finished_at
            ) VALUES (?, ?, ?, ?, 'review', 'superseded', 10.0, 20.0)
            """,
            (
                review_operation_id,
                key.profile_id,
                key.session_id,
                target_generation,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, created_at, updated_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, 'review-source:ownership-boundary', ?, ?,
                      ?, ?, '{}', 'cancelled', 1, 10.0, 10.0, 20.0, 20.0)
            """,
            (
                review_effect_id,
                review_effect_id,
                key.profile_id,
                key.session_id,
                target_generation,
                review_operation_id,
                review_contract.effect_kind,
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
                attempt_count, available_at, created_at, updated_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', 1,
                      10.0, 10.0, 20.0, 20.0)
            """,
            (
                cancellation_effect_id,
                cancellation_effect_id,
                key.profile_id,
                key.session_id,
                target_generation,
                request_event_id,
                review_operation_id,
                cancellation_contract.effect_kind,
                cancellation_contract.version,
                cancellation_contract.signature,
                cancellation_payload,
            ),
        )
        if gate_generation is not None:
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'cancelled',
                          'cancelled', ?, 1, 20.0, 20.0, 20.0)
                """,
                (
                    key.profile_id,
                    key.session_id,
                    gate_generation,
                    cancellation_effect_id,
                    request_event_id,
                    review_operation_id,
                    review_effect_id,
                    review_contract.effect_kind,
                    review_contract.version,
                    review_contract.signature,
                    claim_id,
                ),
            )
        if run_generation is not None:
            conn.execute(
                """
                INSERT INTO agent_review_execution_runs (
                    profile_id, session_id, ownership_generation,
                    review_effect_id, review_operation_id, review_effect_kind,
                    review_contract_version, review_contract_signature,
                    claim_id, worker_id, execution_status, started_at, finished_at,
                    unknown_at, unknown_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'remote-review-worker',
                          ?, 10.0, ?, ?, ?)
                """,
                (
                    key.profile_id,
                    key.session_id,
                    run_generation,
                    review_effect_id,
                    review_operation_id,
                    review_contract.effect_kind,
                    review_contract.version,
                    review_contract.signature,
                    claim_id,
                    run_status,
                    finished_at,
                    unknown_at,
                    unknown_reason,
                ),
            )
    return review_effect_id, cancellation_effect_id, review_operation_id


def _insert_model_execution_witness(
    database: DatabaseManager,
    key: SessionKey,
    *,
    ownership_generation: int,
    status: str,
) -> tuple[str, str]:
    """Insert one schema-valid generic model witness for ownership fencing."""

    effect_id = "active-reply-effect:model-execution-boundary"
    operation_id = "active-reply-operation:model-execution-boundary"
    claim_id = "active-reply-claim:model-execution-boundary"
    contract = builtin_effect_contract("run_active_reply_workflow")
    unknown_at = 20.0 if status == "unknown" else None
    unknown_reason = (
        "model_execution_lease_expired_before_handler_terminal"
        if status == "unknown"
        else ""
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'model-source:ownership-boundary', ?, ?,
                      ?, ?, '{}', 'processing', 1, 10.0, ?,
                      'remote-model-worker', 100.0, 10.0, 10.0)
            """,
            (
                effect_id,
                effect_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                operation_id,
                contract.effect_kind,
                contract.version,
                contract.signature,
                claim_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_model_execution_runs (
                profile_id, session_id, ownership_generation,
                effect_id, operation_id, effect_kind,
                contract_version, contract_signature, claim_id, worker_id,
                execution_status, started_at, finished_at, unknown_at,
                unknown_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'remote-model-worker',
                      ?, 10.0, NULL, ?, ?)
            """,
            (
                key.profile_id,
                key.session_id,
                ownership_generation,
                effect_id,
                operation_id,
                contract.effect_kind,
                contract.version,
                contract.signature,
                claim_id,
                status,
                unknown_at,
                unknown_reason,
            ),
        )
    return effect_id, claim_id


def _insert_model_execution_cancellation_evidence(
    database: DatabaseManager,
    key: SessionKey,
    *,
    ownership_generation: int,
    gate_status: str,
    terminal_witness: bool = False,
) -> tuple[str, str, str]:
    """Insert one schema-valid generic v3 cancellation boundary."""

    if gate_status not in {"requested", "cancelled", "terminal", "blocked"}:
        raise ValueError("unsupported model execution cancellation gate status")
    if terminal_witness and gate_status != "terminal":
        raise ValueError("only terminal model cancellation gates can finish a witness")
    target_effect_id = "idle-planning-effect:model-cancellation-boundary"
    control_effect_id = "cancel-idle-planning:model-cancellation-boundary"
    operation_id = "idle-planning-operation:model-cancellation-boundary"
    request_event_id = "idle-planning-cancel:model-cancellation-boundary"
    claim_id = "idle-planning-claim:model-cancellation-boundary"
    worker_id = "idle-planning-worker:model-cancellation-boundary"
    target_contract = builtin_effect_contract("run_idle_review_planning", version=3)
    control_contract = builtin_effect_contract("cancel_model_execution", version=3)
    target_execution_status = {
        "requested": "none",
        "cancelled": "running",
        "terminal": "finished" if terminal_witness else "none",
        "blocked": "unknown",
    }[gate_status]
    target_status = "processing" if gate_status in {"requested", "blocked"} else "cancelled"
    retained_claim = (
        claim_id
        if gate_status in {"requested", "cancelled", "blocked"} or terminal_witness
        else ""
    )
    retained_worker = (
        worker_id
        if gate_status in {"requested", "cancelled", "blocked"} or terminal_witness
        else ""
    )
    terminal_at = 20.0 if target_status == "cancelled" else None
    control_status = {
        "requested": "pending",
        "cancelled": "processing",
        "terminal": "completed",
        "blocked": "completed",
    }[gate_status]
    blocker_code = "model_execution_witness_unknown" if gate_status == "blocked" else ""
    payload_json = json.dumps(
        {
            "operation_id": operation_id,
            "cancelled_model_effect_fence": {
                "operation_id": operation_id,
                "effect_id": target_effect_id,
                "effect_kind": target_contract.effect_kind,
                "contract_version": target_contract.version,
                "contract_signature": target_contract.signature,
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
                kind, status, started_at, superseded_at
            ) VALUES (?, ?, ?, ?, 'idle_review_planning', 'superseded', 10.0, 15.0)
            """,
            (operation_id, key.profile_id, key.session_id, ownership_generation),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner, lease_until,
                created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, 'idle-planning-source', ?, ?, ?, ?, '{}', ?,
                      1, 10.0, ?, ?, ?, 10.0, ?, ?, '')
            """,
            (
                target_effect_id,
                target_effect_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                operation_id,
                target_contract.effect_kind,
                target_contract.version,
                target_contract.signature,
                target_status,
                claim_id if target_status == "processing" else "",
                worker_id if target_status == "processing" else "",
                100.0 if target_status == "processing" else None,
                15.0 if target_status == "processing" else 20.0,
                terminal_at,
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 15.0, '', '', NULL,
                      15.0, ?, ?, '')
            """,
            (
                control_effect_id,
                control_effect_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                request_event_id,
                operation_id,
                control_contract.effect_kind,
                control_contract.version,
                control_contract.signature,
                payload_json,
                control_status,
                15.0 if control_status in {"pending", "processing"} else 20.0,
                None if control_status in {"pending", "processing"} else 20.0,
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 15.0, ?)
            """,
            (
                key.profile_id,
                key.session_id,
                ownership_generation,
                control_effect_id,
                request_event_id,
                operation_id,
                target_effect_id,
                target_contract.effect_kind,
                target_contract.version,
                target_contract.signature,
                target_status,
                retained_claim,
                retained_worker,
                target_execution_status,
                gate_status,
                terminal_at,
                blocker_code,
                15.0 if gate_status == "requested" else 20.0,
            ),
        )
        if target_execution_status in {"running", "unknown", "finished"}:
            conn.execute(
                """
                INSERT INTO agent_model_execution_runs (
                    profile_id, session_id, ownership_generation,
                    effect_id, operation_id, effect_kind,
                    contract_version, contract_signature, claim_id, worker_id,
                    execution_status, started_at, finished_at, unknown_at,
                    unknown_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 10.0, ?, ?, ?)
                """,
                (
                    key.profile_id,
                    key.session_id,
                    ownership_generation,
                    target_effect_id,
                    operation_id,
                    target_contract.effect_kind,
                    target_contract.version,
                    target_contract.signature,
                    claim_id,
                    worker_id,
                    target_execution_status,
                    20.0 if target_execution_status == "finished" else None,
                    20.0 if target_execution_status == "unknown" else None,
                    (
                        "model_execution_lease_expired_before_handler_terminal"
                        if target_execution_status == "unknown"
                        else ""
                    ),
                ),
            )
    return target_effect_id, control_effect_id, operation_id


def _insert_typed_recovery_delivery(
    database: DatabaseManager,
    key: SessionKey,
    *,
    ownership_generation: int,
    case_status: str,
    mailbox_status: str,
) -> tuple[str, str]:
    """Insert one schema-valid typed recovery delivery for migration fencing."""

    certificate = build_recovery_certificate(
        subject=RecoverySubject(
            profile_id=key.profile_id,
            session_id=key.session_id,
            ownership_generation=ownership_generation,
        ),
        aggregate_fence=RecoveryAggregateFence(
            state="review",
            state_revision=1,
            event_sequence=1,
            activity_generation=0,
            active_epoch=0,
        ),
        nodes=(
            RecoveryGraphNode(
                identity="operation:recovery-test",
                kind="operation",
                authority="agent_session_operations",
                status="pending",
            ),
        ),
        edges=(),
        invariants=(),
        decision=RecoveryDecision(
            kind=RecoveryDecisionKind.RECOVER_ORPHANED_WORK,
            reason_codes=("orphaned_work_without_live_completion",),
            target_node_identities=("operation:recovery-test",),
        ),
    )
    delivery = RecoveryDeliveryPayload(certificate=certificate, delivery_cycle=0)
    payload_json = canonical_recovery_json(delivery.to_record())
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_recovery_cases (
                case_id, profile_id, session_id, ownership_generation,
                certificate_version, policy_version, work_graph_digest,
                latest_certificate_digest, status, next_delivery_cycle,
                delivery_count, last_event_id, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', 0, 0, '', '', 1, 1)
            """,
            (
                delivery.case_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                certificate.version,
                certificate.policy_version,
                certificate.work_graph_digest,
                certificate.certificate_digest,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json,
                causation_id, correlation_id, trace_id,
                status, attempt_count, available_at,
                claim_id, lease_owner, lease_until,
                created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, 1, 1,
                      '', '', NULL, 1, 1, '')
            """,
            (
                delivery.event_id,
                key.profile_id,
                key.session_id,
                ownership_generation,
                RECOVERY_DELIVERY_EVENT_KIND,
                RECOVERY_DELIVERY_EVENT_SOURCE,
                payload_json,
                delivery.case_id,
                delivery.case_id,
                delivery.event_id,
                mailbox_status,
            ),
        )
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET next_delivery_cycle = 1,
                delivery_count = 1,
                last_event_id = ?,
                updated_at = 2
            WHERE case_id = ?
            """,
            (delivery.event_id, delivery.case_id),
        )
        if case_status != "open":
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET status = ?, updated_at = 3
                WHERE case_id = ?
                """,
                (case_status, delivery.case_id),
            )
    return delivery.case_id, delivery.event_id


def _insert_all_legacy_evidence(
    database: DatabaseManager,
    legacy_session_id: str,
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (session_id, updated_at)
            VALUES (?, 1.0)
            """,
            (legacy_session_id,),
        )
        first = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES (?, 'user', 1.0)
            """,
            (legacy_session_id,),
        ).lastrowid
        second = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES (?, 'user', 2.0)
            """,
            (legacy_session_id,),
        ).lastrowid
        assert first is not None and second is not None
        conn.execute(
            """
            INSERT INTO agent_unread_messages (
                session_id, message_log_id, created_at
            ) VALUES (?, ?, 1.0)
            """,
            (legacy_session_id, first),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_ranges (
                session_id, start_msg_log_id, end_msg_log_id,
                start_at, end_at, message_count
            ) VALUES (?, ?, ?, 1.0, 2.0, 2)
            """,
            (legacy_session_id, first, second),
        )


def test_first_claim_is_idempotent_and_survives_repository_restart(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    key = SessionKey("bot-a", "bot-a:group:room")
    repository = AgentRuntimeOwnershipRepository(database, clock=lambda: 10.0)

    first = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor foundation enabled",
        legacy_session_id="instance-a:group:room",
        requested_by="test",
    )
    replay = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="same decision replayed",
        legacy_session_id="instance-a:group:room",
        requested_by="other-worker",
    )

    assert first.created is True
    assert replay.created is False
    assert replay.ownership == first.ownership
    assert replay.ownership.generation == 1
    assert len(repository.list_events(key)) == 1

    restarted_database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted_database.initialize()
    restored = restarted_database.agent_runtime_ownership.get(key)
    assert restored == first.ownership


def test_same_key_conflicting_mode_fails_closed(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("bot-a", "bot-a:group:room")
    repository = database.agent_runtime_ownership
    repository.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy remains active",
        legacy_session_id="instance-a:group:room",
    )

    with pytest.raises(AgentRuntimeOwnershipConflict, match="already claimed"):
        repository.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="conflicting rollout",
            legacy_session_id="instance-a:group:room",
        )

    restored = repository.get(key)
    assert restored is not None
    assert restored.mode is AgentRuntimeOwnershipMode.LEGACY


def test_actor_aggregate_and_mailbox_forbid_legacy_claim(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("bot-a", "bot-a:group:room")
    _insert_actor_aggregate(database, key, mailbox=True)

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        database.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            reason="unsafe legacy selection",
        )

    assert caught.value.evidence == ("actor_aggregate", "actor_mailbox")
    assert database.agent_runtime_ownership.get(key) is None


def test_legacy_scheduler_and_unread_state_forbid_actor_claim(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("bot-a", "bot-a:group:room")
    legacy_session_id = "instance-a:group:room"
    _insert_all_legacy_evidence(database, legacy_session_id)

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        database.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="unsafe actor selection",
            legacy_session_id=legacy_session_id,
        )

    assert caught.value.evidence == (
        "legacy_scheduler_state",
        "legacy_unread_messages",
        "legacy_unread_ranges",
    )


def test_actor_ownership_isolated_for_same_session_across_profiles(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key_a = SessionKey("profile-a", "shared:group:room")
    key_b = SessionKey("profile-b", "shared:group:room")

    owner_a = repository.claim(
        key_a,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="profile a rollout",
        legacy_session_id="instance:group:room",
    ).ownership
    owner_b = repository.claim(
        key_b,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="profile b rollout",
        legacy_session_id="instance:group:room",
    ).ownership

    assert owner_a.key != owner_b.key
    assert repository.get(key_a) == owner_a
    assert repository.get(key_b) == owner_b


def test_legacy_alias_conflicts_with_actor_owned_profile(tmp_path: Path) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    actor_key = SessionKey("profile-a", "profile-a:group:room")
    legacy_key = SessionKey("profile-b", "profile-b:group:room")
    alias = "instance:group:room"
    repository.claim(
        actor_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor profile active",
        legacy_session_id=alias,
    )

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        repository.claim(
            legacy_key,
            AgentRuntimeOwnershipMode.LEGACY,
            reason="legacy would share unscoped state",
            legacy_session_id=alias,
        )

    assert caught.value.evidence == ("actor_v2_ownership:profile-a:profile-a:group:room",)


def test_only_one_legacy_owner_may_use_an_unscoped_session_alias(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    alias = "instance:group:room"
    repository.claim(
        SessionKey("profile-a", "profile-a:group:room"),
        AgentRuntimeOwnershipMode.LEGACY,
        reason="first legacy owner",
        legacy_session_id=alias,
    )

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        repository.claim(
            SessionKey("profile-b", "profile-b:group:room"),
            AgentRuntimeOwnershipMode.LEGACY,
            reason="duplicate legacy owner",
            legacy_session_id=alias,
        )

    assert caught.value.evidence == ("legacy_ownership:profile-a:profile-a:group:room",)


def test_concurrent_same_mode_first_claim_creates_one_generation(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = AgentRuntimeOwnershipRepository(database, clock=lambda: 20.0)
    key = SessionKey("profile-a", "profile-a:group:room")
    workers = 8
    barrier = threading.Barrier(workers)

    def claim() -> bool:
        barrier.wait()
        return repository.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="concurrent rollout",
        ).created

    with ThreadPoolExecutor(max_workers=workers) as executor:
        created = list(executor.map(lambda _index: claim(), range(workers)))

    assert created.count(True) == 1
    assert created.count(False) == workers - 1
    restored = repository.get(key)
    assert restored is not None
    assert restored.generation == 1
    assert len(repository.list_events(key)) == 1


def test_concurrent_conflicting_first_claims_choose_exactly_one_mode(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    barrier = threading.Barrier(2)

    def claim(mode: AgentRuntimeOwnershipMode) -> str:
        barrier.wait()
        try:
            result = repository.claim(key, mode, reason=f"claim {mode.value}")
        except AgentRuntimeOwnershipConflict:
            return "conflict"
        return result.ownership.mode.value

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(
                claim,
                (
                    AgentRuntimeOwnershipMode.LEGACY,
                    AgentRuntimeOwnershipMode.ACTOR_V2,
                ),
            )
        )

    assert outcomes.count("conflict") == 1
    selected = database.agent_runtime_ownership.get(key)
    assert selected is not None
    assert outcomes.count(selected.mode.value) == 1
    assert len(repository.list_events(key)) == 1


def test_migration_uses_generation_cas_and_target_evidence_cleanup(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = AgentRuntimeOwnershipRepository(database, clock=lambda: 30.0)
    key = SessionKey("profile-a", "profile-a:group:room")
    legacy_session_id = "instance:group:room"
    claimed = repository.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy baseline",
        legacy_session_id=legacy_session_id,
    ).ownership
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=claimed.generation,
        reason="begin actor migration",
        requested_by="operator",
    )

    assert migrating.status is AgentRuntimeOwnershipStatus.MIGRATING
    assert migrating.mode is AgentRuntimeOwnershipMode.LEGACY
    assert migrating.pending_mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert migrating.generation == 2
    with pytest.raises(AgentRuntimeOwnershipGenerationConflict):
        repository.complete_migration(
            key,
            expected_generation=1,
            reason="stale completion",
        )
    with pytest.raises(AgentRuntimeOwnershipMigrationConflict):
        repository.claim(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            reason="claim during migration",
            legacy_session_id=legacy_session_id,
        )

    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (session_id, updated_at)
            VALUES (?, 1.0)
            """,
            (legacy_session_id,),
        )
    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict):
        repository.complete_migration(
            key,
            expected_generation=2,
            reason="legacy state not migrated",
        )
    with database.connect() as conn:
        conn.execute(
            "DELETE FROM agent_scheduler_states WHERE session_id = ?",
            (legacy_session_id,),
        )

    completed = repository.complete_migration(
        key,
        expected_generation=2,
        reason="legacy state migrated and verified",
        requested_by="operator",
    )

    assert completed.mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert completed.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert completed.pending_mode is None
    assert completed.generation == 3
    events = repository.list_events(key)
    assert [event.event_type for event in events] == [
        AgentRuntimeOwnershipEventType.CLAIMED,
        AgentRuntimeOwnershipEventType.MIGRATION_STARTED,
        AgentRuntimeOwnershipEventType.MIGRATION_COMPLETED,
    ]
    assert [event.reason for event in events] == [
        "legacy baseline",
        "begin actor migration",
        "legacy state migrated and verified",
    ]
    assert events[1].to_mode is AgentRuntimeOwnershipMode.ACTOR_V2


def test_actor_to_legacy_migration_requires_actor_state_cleanup(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    claimed = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor baseline",
    ).ownership
    _insert_actor_aggregate(database, key)
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=claimed.generation,
        reason="begin legacy rollback",
    )

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        repository.complete_migration(
            key,
            expected_generation=migrating.generation,
            reason="actor state still present",
        )
    assert "actor_aggregate" in caught.value.evidence
    with database.connect() as conn:
        conn.execute(
            """
            DELETE FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )
    completed = repository.complete_migration(
        key,
        expected_generation=migrating.generation,
        reason="actor state removed",
    )
    assert completed.mode is AgentRuntimeOwnershipMode.LEGACY


def test_open_typed_recovery_case_blocks_actor_owner_migration(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    claimed = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor recovery migration boundary",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=claimed.generation,
    )
    _insert_typed_recovery_delivery(
        database,
        key,
        ownership_generation=claimed.generation,
        case_status="open",
        mailbox_status="pending",
    )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="open typed recovery case",
    ):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=claimed.generation,
            reason="attempt migration with open recovery",
        )


def test_terminal_typed_recovery_history_is_not_refenced_during_abort(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    claimed = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor recovery history migration boundary",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=claimed.generation,
    )
    case_id, event_id = _insert_typed_recovery_delivery(
        database,
        key,
        ownership_generation=claimed.generation,
        case_status="superseded",
        mailbox_status="completed",
    )

    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=claimed.generation,
        reason="begin migration with settled recovery history",
    )
    aborted = repository.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="retain actor state after migration abort",
    )

    assert aborted.generation == claimed.generation + 2
    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT ownership_generation
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_session_mailbox
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        case = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (case_id,),
        ).fetchone()
    assert aggregate is not None
    assert mailbox is not None
    assert case is not None
    assert tuple(aggregate) == (aborted.generation,)
    assert tuple(mailbox) == (claimed.generation, "completed")
    assert tuple(case) == (claimed.generation, "superseded")

    restarted = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted.initialize()


@pytest.mark.parametrize(
    ("evidence_state", "handoff_state", "expected_evidence"),
    (
        pytest.param("unknown", "blocked", "unknown", id="unknown-sidecar"),
        pytest.param("fenced", "pending", "fenced", id="pending-fenced-sidecar"),
    ),
)
def test_existing_nonterminal_mailbox_handoff_blocks_actor_migration_before_state_change(
    tmp_path: Path,
    evidence_state: str,
    handoff_state: str,
    expected_evidence: str,
) -> None:
    """Existing immutable evidence blocks an actor transition before it starts."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor mailbox handoff boundary",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    event_id = f"handoff-boundary:{expected_evidence}"
    mailbox_id = _insert_actor_mailbox(
        database,
        key,
        event_id=event_id,
        ownership_generation=owner.generation,
        status="processing",
    )
    _insert_mailbox_handoff(
        database,
        key,
        mailbox_id=mailbox_id,
        event_id=event_id,
        ownership_generation=owner.generation,
        evidence_state=evidence_state,
        state=handoff_state,
    )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match=f"evidence_state={expected_evidence}",
    ):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=owner.generation,
            reason="must not migrate unproven mailbox handoff",
        )

    current = repository.get(key)
    assert current is not None
    assert current.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert current.generation == owner.generation
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT ownership_generation, status, claim_id, lease_owner, lease_until
            FROM agent_session_mailbox
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()
    assert mailbox is not None
    assert tuple(mailbox) == (
        owner.generation,
        "processing",
        "mailbox-claim",
        "mailbox-worker",
        10.0,
    )


def test_handoff_added_during_actor_migration_blocks_abort_before_release(
    tmp_path: Path,
) -> None:
    """The abort path repeats the sidecar gate after a migration has begun."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor sidecar arrival during migration",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    event_id = "late-handoff:ownership-boundary"
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="start actor migration before handoff is created",
    )
    mailbox_id = _insert_actor_mailbox(
        database,
        key,
        event_id=event_id,
        ownership_generation=owner.generation,
        status="processing",
    )
    _insert_mailbox_handoff(
        database,
        key,
        mailbox_id=mailbox_id,
        event_id=event_id,
        ownership_generation=owner.generation,
        evidence_state="unknown",
        state="blocked",
    )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="evidence_state=unknown",
    ):
        repository.abort_migration(
            key,
            expected_generation=migrating.generation,
            reason="must not release a newly immutable handoff",
        )

    current = repository.get(key)
    assert current is not None
    assert current.status is AgentRuntimeOwnershipStatus.MIGRATING
    assert current.generation == migrating.generation
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT ownership_generation, status, claim_id, lease_owner, lease_until
            FROM agent_session_mailbox
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()
    assert mailbox is not None
    assert tuple(mailbox) == (
        owner.generation,
        "processing",
        "mailbox-claim",
        "mailbox-worker",
        10.0,
    )


def test_missing_mailbox_handoff_blocks_actor_migration_before_state_change(
    tmp_path: Path,
) -> None:
    """A mailbox without immutable handoff evidence blocks actor refencing."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor mailbox without dual-written handoff",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    mailbox_id = _insert_actor_mailbox(
        database,
        key,
        event_id="missing-handoff:ownership-boundary",
        ownership_generation=owner.generation,
        status="processing",
    )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="evidence_state=missing",
    ):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=owner.generation,
            reason="mailbox producer omitted immutable handoff evidence",
        )

    current = repository.get(key)
    assert current is not None
    assert current.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert current.generation == owner.generation
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT ownership_generation, status, claim_id, lease_owner, lease_until
            FROM agent_session_mailbox
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()
    assert mailbox is not None
    assert tuple(mailbox) == (
        owner.generation,
        "processing",
        "mailbox-claim",
        "mailbox-worker",
        10.0,
    )


def test_existing_handoff_blocks_actor_activation_before_ownership_update(
    tmp_path: Path,
) -> None:
    """Actor activation cannot refence an existing nonterminal sidecar."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy actor activation handoff boundary",
    ).ownership
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=owner.generation,
        reason="prepare actor activation with immutable handoff evidence",
    )
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=migrating.generation,
    )
    event_id = "activation-handoff:ownership-boundary"
    mailbox_id = _insert_actor_mailbox(
        database,
        key,
        event_id=event_id,
        ownership_generation=migrating.generation,
        status="completed",
    )
    _insert_mailbox_handoff(
        database,
        key,
        mailbox_id=mailbox_id,
        event_id=event_id,
        ownership_generation=migrating.generation,
        evidence_state="unknown",
        state="blocked",
    )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="evidence_state=unknown",
    ):
        repository.complete_migration(
            key,
            expected_generation=migrating.generation,
            reason="must not refence immutable unknown handoff evidence",
        )

    current = repository.get(key)
    assert current is not None
    assert current.status is AgentRuntimeOwnershipStatus.MIGRATING
    assert current.generation == migrating.generation
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_session_mailbox
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()
    assert mailbox is not None
    assert tuple(mailbox) == (migrating.generation, "completed")


def test_settled_fenced_handoff_remains_historical_during_actor_refence(
    tmp_path: Path,
) -> None:
    """Settled typed evidence keeps its original mailbox generation forever.

    Generic ownership migration intentionally rejects a live fenced owner, so
    this exercises the same post-CAS refence boundary directly after creating
    the sidecar through its legal pending -> claimed -> settled lifecycle.
    """

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="settled-handoff-refence-test",
        ttl_seconds=300.0,
    )
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor settled mailbox handoff history",
        admission_grant=grant,
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    event_id = "settled-handoff:ownership-boundary"
    mailbox_id = _insert_actor_mailbox(
        database,
        key,
        event_id=event_id,
        ownership_generation=owner.generation,
        status="completed",
    )
    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=owner.generation,
        admission_fence_id=owner.admission_fence_id,
        admission_fence_generation=owner.admission_fence_generation,
    )
    handoff_repository = database.actor_v2_mailbox_handoffs
    handoff_repository.record_fenced_handoff(mailbox_id, request)
    target = MailboxHandoffTarget(
        "settled-handoff-refence-target",
        "settled-handoff-refence-incarnation",
    )
    claim = handoff_repository.claim_fenced_handoff(
        mailbox_id,
        worker_id="settled-handoff-refence-worker",
        target=target,
    )
    assert claim is not None
    handoff_repository.settle_fenced_claim(
        FencedMailboxHandoffReceipt(
            claim=claim,
            wake_receipt=FencedMailboxWakeReceipt(
                request=request,
                disposition=FencedMailboxWakeDisposition.ACCEPTED,
            ),
        )
    )

    target_generation = owner.generation + 1
    with database.connect() as conn:
        AgentRuntimeOwnershipRepository._refence_actor_state(
            conn,
            key,
            expected_generation=owner.generation,
            target_generation=target_generation,
            now=2.0,
            release_leases=True,
        )

    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT ownership_generation
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT ownership_generation, event_id, status
            FROM agent_session_mailbox
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()
        handoff = conn.execute(
            """
            SELECT ownership_generation, event_id, evidence_state, state
            FROM agent_session_mailbox_handoffs
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()
    assert aggregate is not None
    assert mailbox is not None
    assert handoff is not None
    assert tuple(aggregate) == (target_generation,)
    assert tuple(mailbox) == (owner.generation, event_id, "completed")
    assert tuple(handoff) == (owner.generation, event_id, "fenced", "settled")

    restarted = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted.initialize()


@pytest.mark.parametrize("run_status", ("running", "unknown"))
def test_live_or_unknown_review_witness_blocks_actor_to_legacy_migration(
    tmp_path: Path,
    run_status: str,
) -> None:
    """Neither live nor unknown review work can be refenced as quiescent."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns review execution",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    review_effect_id, _, _ = _insert_review_cancellation_evidence(
        database,
        key,
        target_generation=owner.generation,
        gate_generation=owner.generation,
        run_generation=owner.generation,
        run_status=run_status,
    )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match=f"{run_status} review execution witness",
    ):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=owner.generation,
            reason="attempt rollback after review lease expiry",
        )

    current = repository.get(key)
    assert current is not None
    assert current.mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert current.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert current.generation == owner.generation
    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, lease_until
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
        run = conn.execute(
            """
            SELECT ownership_generation, execution_status, claim_id
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert effect is not None
    assert run is not None
    assert tuple(effect) == ("cancelled", None)
    assert tuple(run) == (owner.generation, run_status, "expired-review-claim")


@pytest.mark.parametrize("run_status", ("running", "unknown"))
def test_live_or_unknown_model_witness_blocks_actor_to_legacy_migration(
    tmp_path: Path,
    run_status: str,
) -> None:
    """Generic model work cannot be refenced as a safe completed operation."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns generic model execution",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    effect_id, claim_id = _insert_model_execution_witness(
        database,
        key,
        ownership_generation=owner.generation,
        status=run_status,
    )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match=f"{run_status} model execution witness",
    ):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=owner.generation,
            reason="reject live generic model execution",
        )

    with database.connect() as conn:
        witness = conn.execute(
            """
            SELECT ownership_generation, execution_status
            FROM agent_model_execution_runs
            WHERE profile_id = ? AND session_id = ? AND effect_id = ? AND claim_id = ?
            """,
            (key.profile_id, key.session_id, effect_id, claim_id),
        ).fetchone()
    assert witness is not None
    assert tuple(witness) == (owner.generation, run_status)


@pytest.mark.parametrize("gate_status", ("requested", "cancelled", "blocked"))
def test_unresolved_model_execution_cancellation_gate_blocks_migration_boundary(
    tmp_path: Path,
    gate_status: str,
) -> None:
    """Every nonterminal v3 gate remains an ownership-migration barrier."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns model cancellation work",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    _insert_model_execution_cancellation_evidence(
        database,
        key,
        ownership_generation=owner.generation,
        gate_status=gate_status,
    )

    with database.connect() as conn:
        with pytest.raises(
            AgentRuntimeOwnershipMigrationConflict,
            match="unresolved model execution cancellation gate",
        ):
            AgentRuntimeOwnershipRepository._validate_model_execution_cancellation_migration_boundary(
                conn,
                key,
                expected_generation=owner.generation,
            )

    current = database.agent_runtime_ownership.get(key)
    assert current is not None
    assert current.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert current.generation == owner.generation


def test_terminal_model_execution_cancellation_refences_on_actor_abort(
    tmp_path: Path,
) -> None:
    """A quiescent generic gate follows a safe actor abort to its new fence."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns settled model cancellation work",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    target_effect_id, control_effect_id, operation_id = (
        _insert_model_execution_cancellation_evidence(
            database,
            key,
            ownership_generation=owner.generation,
            gate_status="terminal",
            terminal_witness=True,
        )
    )

    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="test settled generic cancellation migration",
    )
    restored = repository.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="return settled actor to service",
    )

    assert restored.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert restored.mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert restored.generation == owner.generation + 2
    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_session_operations
            WHERE profile_id = ? AND session_id = ? AND operation_id = ?
            """,
            (key.profile_id, key.session_id, operation_id),
        ).fetchone()
        target = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, target_effect_id),
        ).fetchone()
        control = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, control_effect_id),
        ).fetchone()
        gate = conn.execute(
            """
            SELECT ownership_generation, gate_status, target_execution_status
            FROM agent_model_execution_cancellation_gates
            WHERE profile_id = ? AND session_id = ? AND target_effect_id = ?
            """,
            (key.profile_id, key.session_id, target_effect_id),
        ).fetchone()
        witness = conn.execute(
            """
            SELECT ownership_generation, execution_status
            FROM agent_model_execution_runs
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, target_effect_id),
        ).fetchone()
    assert operation is not None
    assert target is not None
    assert control is not None
    assert gate is not None
    assert witness is not None
    assert tuple(operation) == (restored.generation, "superseded")
    assert tuple(target) == (restored.generation, "cancelled")
    assert tuple(control) == (restored.generation, "completed")
    assert tuple(gate) == (restored.generation, "terminal", "finished")
    assert tuple(witness) == (restored.generation, "finished")


@pytest.mark.parametrize(
    ("corruption", "message"),
    (
        ("control_payload", "model execution cancellation gate control identity changed"),
        ("operation", "model execution cancellation gate operation identity changed"),
        ("terminal_at", "terminal model execution cancellation gate changed state"),
    ),
)
def test_terminal_model_execution_cancellation_corruption_blocks_migration(
    tmp_path: Path,
    corruption: str,
    message: str,
) -> None:
    """A terminal gate is refenceable only with its full frozen evidence intact."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns terminal model cancellation work",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    target_effect_id, control_effect_id, operation_id = (
        _insert_model_execution_cancellation_evidence(
            database,
            key,
            ownership_generation=owner.generation,
            gate_status="terminal",
        )
    )
    with database.connect() as conn:
        if corruption == "control_payload":
            conn.execute(
                """
                UPDATE agent_effect_outbox
                SET payload_json = '{}'
                WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                """,
                (key.profile_id, key.session_id, control_effect_id),
            )
        elif corruption == "operation":
            conn.execute(
                """
                UPDATE agent_session_operations
                SET status = 'cancelled'
                WHERE profile_id = ? AND session_id = ? AND operation_id = ?
                """,
                (key.profile_id, key.session_id, operation_id),
            )
        else:
            conn.execute(
                """
                UPDATE agent_model_execution_cancellation_gates
                SET target_effect_terminal_at = 19.0
                WHERE profile_id = ? AND session_id = ? AND target_effect_id = ?
                """,
                (key.profile_id, key.session_id, target_effect_id),
            )

    with pytest.raises(AgentRuntimeOwnershipGenerationConflict, match=message):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=owner.generation,
            reason="reject corrupted generic cancellation evidence",
        )


@pytest.mark.parametrize("run_status", ("running", "unknown"))
def test_live_or_unknown_review_witness_blocks_abort_before_release_or_refence(
    tmp_path: Path,
    run_status: str,
) -> None:
    """Abort must retain all old-generation work without quiescence proof."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns review execution",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="test abort while remote review is running",
    )
    review_effect_id, _, _ = _insert_review_cancellation_evidence(
        database,
        key,
        target_generation=owner.generation,
        gate_generation=owner.generation,
        run_generation=owner.generation,
        run_status=run_status,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation, kind,
                occurred_at, status, attempt_count, available_at,
                claim_id, lease_owner, lease_until, created_at
            ) VALUES ('leased-mailbox:ownership-boundary', ?, ?, ?, 'TestEvent',
                      10.0, 'processing', 1, 10.0, 'actor-claim',
                      'actor-worker', 15.0, 10.0)
            """,
            (key.profile_id, key.session_id, owner.generation),
        )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match=f"{run_status} review execution witness",
    ):
        repository.abort_migration(
            key,
            expected_generation=migrating.generation,
            reason="abort must wait for remote review quiescence",
        )

    current = repository.get(key)
    assert current is not None
    assert current.status is AgentRuntimeOwnershipStatus.MIGRATING
    assert current.generation == migrating.generation
    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT ownership_generation
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT ownership_generation, status, claim_id, lease_owner, lease_until
            FROM agent_session_mailbox
            WHERE event_id = 'leased-mailbox:ownership-boundary'
            """
        ).fetchone()
        effect = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
        gate = conn.execute(
            """
            SELECT ownership_generation, gate_status
            FROM agent_review_cancellation_gates
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
        run = conn.execute(
            """
            SELECT ownership_generation, execution_status
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert aggregate is not None
    assert mailbox is not None
    assert effect is not None
    assert gate is not None
    assert run is not None
    assert tuple(aggregate) == (owner.generation,)
    assert tuple(mailbox) == (
        owner.generation,
        "processing",
        "actor-claim",
        "actor-worker",
        15.0,
    )
    assert tuple(effect) == (owner.generation, "cancelled")
    assert tuple(gate) == (owner.generation, "cancelled")
    assert tuple(run) == (owner.generation, run_status)


def test_quiescent_review_cancellation_evidence_refences_on_actor_abort(
    tmp_path: Path,
) -> None:
    """Safe cancellation evidence follows the actor state to the new generation."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns review execution",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    review_effect_id, cancellation_effect_id, review_operation_id = (
        _insert_review_cancellation_evidence(
            database,
            key,
            target_generation=owner.generation,
            gate_generation=owner.generation,
            run_generation=owner.generation,
        )
    )

    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="test quiescent review cancellation migration",
    )
    restored = repository.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="return quiescent actor to service",
    )

    assert restored.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert restored.mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert restored.generation == owner.generation + 2
    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT ownership_generation
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        operation = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            (review_operation_id,),
        ).fetchone()
        review_effect = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
        cancellation_effect = conn.execute(
            """
            SELECT ownership_generation, status
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, cancellation_effect_id),
        ).fetchone()
        gate = conn.execute(
            """
            SELECT ownership_generation, gate_status
            FROM agent_review_cancellation_gates
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
        run = conn.execute(
            """
            SELECT ownership_generation, execution_status
            FROM agent_review_execution_runs
            WHERE profile_id = ? AND session_id = ? AND review_effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert aggregate is not None
    assert operation is not None
    assert review_effect is not None
    assert cancellation_effect is not None
    assert gate is not None
    assert run is not None
    assert tuple(aggregate) == (restored.generation,)
    assert tuple(operation) == (restored.generation, "superseded")
    assert tuple(review_effect) == (restored.generation, "cancelled")
    assert tuple(cancellation_effect) == (restored.generation, "completed")
    assert tuple(gate) == (restored.generation, "cancelled")
    assert tuple(run) == (restored.generation, "cancelled")


@pytest.mark.parametrize("target_status", ("completed", "failed"))
def test_finished_review_witness_allows_a_settled_claim_to_refence(
    tmp_path: Path,
    target_status: str,
) -> None:
    """Normal effect settlement retains the claim without keeping a task alive."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns a completed review",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    review_effect_id, _, _ = _insert_review_cancellation_evidence(
        database,
        key,
        target_generation=owner.generation,
        gate_generation=None,
        run_generation=owner.generation,
        run_status="finished",
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET status = ?, claim_id = 'expired-review-claim',
                lease_owner = '', lease_until = NULL, completed_at = 20.0
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (target_status, key.profile_id, key.session_id, review_effect_id),
        )

    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="migrate after settled review task",
    )
    restored = repository.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="restore actor after settled review task",
    )

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT ownership_generation, status, claim_id, lease_owner, lease_until
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert effect is not None
    assert tuple(effect) == (
        restored.generation,
        target_status,
        "expired-review-claim",
        "",
        None,
    )


def test_finished_review_witness_does_not_block_a_retry_after_task_exit(
    tmp_path: Path,
) -> None:
    """A retry claim after a finished task is releasable during actor refencing."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns a retrying review",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    review_effect_id, _, _ = _insert_review_cancellation_evidence(
        database,
        key,
        target_generation=owner.generation,
        gate_generation=None,
        run_generation=owner.generation,
        run_status="finished",
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET status = 'processing', claim_id = 'retry-review-claim',
                lease_owner = 'retry-review-worker', lease_until = 100.0,
                completed_at = NULL
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        )

    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="migrate after retrying review task exited",
    )
    restored = repository.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="restore actor and release retry claim",
    )

    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT ownership_generation, status, claim_id, lease_owner, lease_until
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert effect is not None
    assert tuple(effect) == (restored.generation, "pending", "", "", None)


def test_cancelled_review_witness_blocks_a_reclaimed_target(
    tmp_path: Path,
) -> None:
    """Cancellation witnesses require a terminal target before leases can move."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns a cancelled review",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    review_effect_id, _, _ = _insert_review_cancellation_evidence(
        database,
        key,
        target_generation=owner.generation,
        gate_generation=owner.generation,
        run_generation=owner.generation,
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET status = 'processing', claim_id = 'reclaimed-review-claim',
                lease_owner = 'reclaimed-review-worker', lease_until = 100.0,
                completed_at = NULL
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        )

    with pytest.raises(
        AgentRuntimeOwnershipMigrationConflict,
        match="cancelled review execution witness awaits durable cancellation",
    ):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=owner.generation,
            reason="reject reclaimed cancellation target",
        )


@pytest.mark.parametrize(
    ("corruption", "exception_type", "message"),
    (
        ("operation", AgentRuntimeOwnershipGenerationConflict, "operation identity"),
        ("control_payload", AgentRuntimeOwnershipGenerationConflict, "control identity"),
        ("control_lease", AgentRuntimeOwnershipMigrationConflict, "control remains live"),
    ),
)
def test_review_cancellation_boundary_rejects_corrupted_control_evidence(
    tmp_path: Path,
    corruption: str,
    exception_type: type[Exception],
    message: str,
) -> None:
    """Ownership refencing shares the recovery reader's durable gate fence."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor owns cancellation evidence",
    ).ownership
    _insert_actor_aggregate(
        database,
        key,
        ownership_generation=owner.generation,
    )
    review_effect_id, cancellation_effect_id, review_operation_id = (
        _insert_review_cancellation_evidence(
            database,
            key,
            target_generation=owner.generation,
            gate_generation=owner.generation,
            run_generation=owner.generation,
        )
    )
    with database.connect() as conn:
        if corruption == "operation":
            conn.execute(
                """
                UPDATE agent_session_operations
                SET status = 'cancelled'
                WHERE operation_id = ?
                """,
                (review_operation_id,),
            )
        elif corruption == "control_payload":
            conn.execute(
                """
                UPDATE agent_effect_outbox
                SET payload_json = '{}'
                WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                """,
                (key.profile_id, key.session_id, cancellation_effect_id),
            )
        else:
            conn.execute(
                """
                UPDATE agent_effect_outbox
                SET lease_owner = 'stale-control-worker', lease_until = 100.0
                WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                """,
                (key.profile_id, key.session_id, cancellation_effect_id),
            )

    with pytest.raises(exception_type, match=message):
        repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=owner.generation,
            reason="reject corrupted review cancellation evidence",
        )
    with database.connect() as conn:
        target = conn.execute(
            """
            SELECT ownership_generation
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, review_effect_id),
        ).fetchone()
    assert target is not None and int(target["ownership_generation"]) == owner.generation


@pytest.mark.parametrize(
    ("path", "stale_table"),
    (
        ("activation", "agent_review_cancellation_gates"),
        ("activation", "agent_review_execution_runs"),
        ("abort_refence", "agent_review_cancellation_gates"),
        ("abort_refence", "agent_review_execution_runs"),
    ),
)
def test_stale_review_evidence_blocks_actor_activation_and_abort_refence(
    tmp_path: Path,
    path: str,
    stale_table: str,
) -> None:
    """Gate and witness rows must match the generation being activated or refenced."""

    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    stale_gate = stale_table == "agent_review_cancellation_gates"

    if path == "activation":
        source = repository.claim(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            reason="legacy activation baseline",
        ).ownership
        migrating = repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            expected_generation=source.generation,
            reason="prepare actor target with stale review evidence",
        )
        _insert_actor_aggregate(
            database,
            key,
            ownership_generation=migrating.generation,
        )
        _insert_review_cancellation_evidence(
            database,
            key,
            target_generation=migrating.generation,
            gate_generation=source.generation if stale_gate else None,
            run_generation=None if stale_gate else source.generation,
        )

        with pytest.raises(AgentRuntimeOwnershipGenerationConflict, match=stale_table):
            repository.complete_migration(
                key,
                expected_generation=migrating.generation,
                reason="reject stale actor review evidence",
            )

        current = repository.get(key)
        assert current is not None
        assert current.status is AgentRuntimeOwnershipStatus.MIGRATING
        assert current.generation == migrating.generation
        expected_stale_generation = source.generation
        expected_aggregate_generation = migrating.generation
    else:
        source = repository.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="actor refence baseline",
        ).ownership
        _insert_actor_aggregate(
            database,
            key,
            ownership_generation=source.generation,
        )
        migrating = repository.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=source.generation,
            reason="prepare abort with stale review evidence",
        )
        _insert_review_cancellation_evidence(
            database,
            key,
            target_generation=source.generation,
            gate_generation=migrating.generation if stale_gate else None,
            run_generation=None if stale_gate else migrating.generation,
        )

        with pytest.raises(AgentRuntimeOwnershipGenerationConflict, match=stale_table):
            repository.abort_migration(
                key,
                expected_generation=migrating.generation,
                reason="reject stale actor review evidence before refence",
            )

        current = repository.get(key)
        assert current is not None
        assert current.status is AgentRuntimeOwnershipStatus.MIGRATING
        assert current.generation == migrating.generation
        expected_stale_generation = migrating.generation
        expected_aggregate_generation = source.generation

    with database.connect() as conn:
        stale = conn.execute(
            f"""
            SELECT ownership_generation
            FROM {stale_table}
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        aggregate = conn.execute(
            """
            SELECT ownership_generation
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert stale is not None
    assert aggregate is not None
    assert int(stale["ownership_generation"]) == expected_stale_generation
    assert int(aggregate["ownership_generation"]) == expected_aggregate_generation


def test_abort_migration_is_generation_checked_and_audited(tmp_path: Path) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    claimed = repository.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy baseline",
    ).ownership
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=claimed.generation,
        reason="migration experiment",
    )

    with pytest.raises(AgentRuntimeOwnershipGenerationConflict):
        repository.abort_migration(
            key,
            expected_generation=claimed.generation,
            reason="stale abort",
        )
    aborted = repository.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="operator cancelled migration",
    )

    assert aborted.mode is AgentRuntimeOwnershipMode.LEGACY
    assert aborted.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert aborted.pending_mode is None
    assert aborted.generation == 3
    assert repository.list_events(key)[-1].event_type is (
        AgentRuntimeOwnershipEventType.MIGRATION_ABORTED
    )
    assert repository.list_events(key)[-1].reason == "operator cancelled migration"


def test_transactional_actor_validation_fences_mode_and_generation(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    actor_key = SessionKey("profile-a", "profile-a:group:room")
    legacy_key = SessionKey("profile-b", "profile-b:group:other")
    actor = repository.claim(
        actor_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor relay enabled",
    ).ownership
    repository.claim(
        legacy_key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy relay disabled",
    )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        verified = repository.require_actor_v2_in_transaction(
            conn,
            actor_key,
            expected_generation=actor.generation,
        )
        assert verified == actor
        with pytest.raises(AgentRuntimeOwnershipGenerationConflict):
            repository.require_actor_v2_in_transaction(
                conn,
                actor_key,
                expected_generation=actor.generation + 1,
            )
        with pytest.raises(AgentRuntimeOwnershipRequired):
            repository.require_actor_v2_in_transaction(conn, legacy_key)


def test_core_and_persistence_ownership_imports_do_not_load_agent_package() -> None:
    check = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import shinbot.core.dispatch.agent_ownership; "
                "import shinbot.persistence.repositories.agent_runtime_ownership; "
                "assert not any(name == 'shinbot.agent' or "
                "name.startswith('shinbot.agent.') for name in sys.modules)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert check.returncode == 0, check.stderr
