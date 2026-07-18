"""Fail-closed maintenance for inert pre-activation Actor v2 effects.

The future Actor v2 executor must never claim old Active Chat control or
workflow rows merely because a handler is now available.  This module provides
an explicit maintenance operation for a very small, independently auditable
set of historical contracts.  It never creates a mailbox event, invokes an
effect handler, wakes an actor, or changes aggregate/operation state.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError
from shinbot.persistence.canonical_json import validate_canonical_json_object

if TYPE_CHECKING:
    import sqlite3

    from shinbot.persistence import DatabaseManager


HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE = (
    "historical_effect_never_claimed_terminalized"
)
"""Stable outbox error code for a proven inert historical effect."""

_AUDIT_SCHEMA_VERSION = 1
_HISTORICAL_EFFECT_CONTRACTS = frozenset(
    (effect_kind, version)
    for effect_kind in (
        "run_active_chat_bootstrap",
        "run_active_chat_round",
        "active_chat_runtime_reconciliation",
        "stop_active_chat_runtime",
        "cancel_idle_review_planning",
        "idle_review_planning_cancellation_reconciliation",
    )
    for version in (1, 2)
)
_WORKFLOW_EFFECT_KINDS = frozenset(
    {"run_active_chat_bootstrap", "run_active_chat_round"}
)
_CONTROL_EFFECT_KINDS = frozenset(
    {"stop_active_chat_runtime", "cancel_idle_review_planning"}
)
_RECONCILIATION_EFFECT_KINDS = frozenset(
    {
        "active_chat_runtime_reconciliation",
        "idle_review_planning_cancellation_reconciliation",
    }
)
_TERMINAL_OPERATION_STATUSES = frozenset(
    {"completed", "failed", "superseded", "cancelled"}
)
_LIVE_RECEIPT_STATUSES = frozenset({"prepared", "executing", "unknown"})


class HistoricalEffectTerminalizationStatus(StrEnum):
    """Outcome of one explicit historical-effect maintenance request."""

    TERMINALIZED = "terminalized"
    ALREADY_TERMINALIZED = "already_terminalized"
    REJECTED = "rejected"
    NOT_FOUND = "not_found"


@dataclass(slots=True, frozen=True)
class HistoricalEffectTerminalization:
    """Immutable result for one maintenance attempt.

    ``REJECTED`` means the row was left entirely untouched.  Callers can use
    ``reason_code`` for diagnostics without treating a missing or unsafe row as
    a successful cleanup.
    """

    status: HistoricalEffectTerminalizationStatus
    key: SessionKey
    effect_id: str
    reason_code: str
    audit_id: str | None = None


class HistoricalEffectTerminalizer:
    """Terminalize only proven inert, never-claimed historical effect rows.

    This is intentionally not an executor lane and has no bulk/background
    runner.  A maintenance caller must name the exact session and effect id it
    inspected.  Each request obtains a SQLite write transaction, then proves
    the effect remains safe before changing its terminal status.
    """

    def __init__(
        self,
        database: DatabaseManager,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Bind maintenance to one initialized persistence domain."""

        self._database = database
        self._clock = clock or time.time

    @property
    def persistence_domain(self) -> object:
        """Return the database that owns this terminalizer's transaction."""

        return self._database

    async def terminalize(
        self,
        *,
        key: SessionKey,
        effect_id: str,
    ) -> HistoricalEffectTerminalization:
        """Terminalize one explicitly selected historical effect if proven inert.

        The effect must be in the fixed v1/v2 allowlist, still be a pristine
        pending outbox row, match its durable aggregate shape, and coexist with
        no live mailbox, routing, or external-action receipt work.  Any failed
        proof returns ``REJECTED`` without mutating durable actor state.
        """

        normalized_effect_id = _required_text(effect_id, field_name="effect_id")
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _nonnegative_finite(self._clock(), field_name="clock")
            try:
                ownership = (
                    self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                        conn,
                        key,
                    )
                )
            except AgentRuntimeOwnershipError:
                return _result(
                    HistoricalEffectTerminalizationStatus.REJECTED,
                    key=key,
                    effect_id=normalized_effect_id,
                    reason_code="actor_v2_ownership_not_active",
                )

            effect = _load_effect(conn, key=key, effect_id=normalized_effect_id)
            if effect is None:
                return _result(
                    HistoricalEffectTerminalizationStatus.NOT_FOUND,
                    key=key,
                    effect_id=normalized_effect_id,
                    reason_code="effect_not_found",
                )
            audit = _load_audit(conn, effect_seq=effect["effect_seq"])
            audit_result = _existing_audit_result(
                audit,
                effect=effect,
                key=key,
                effect_id=normalized_effect_id,
            )
            if audit_result is not None:
                return audit_result

            eligibility = _validate_eligibility(
                conn,
                effect=effect,
                key=key,
                active_ownership_generation=ownership.generation,
            )
            if eligibility.reason_code:
                return _result(
                    HistoricalEffectTerminalizationStatus.REJECTED,
                    key=key,
                    effect_id=normalized_effect_id,
                    reason_code=eligibility.reason_code,
                )

            audit_id = _audit_id(
                key=key,
                effect_id=normalized_effect_id,
                ownership_generation=int(effect["ownership_generation"]),
            )
            audit_evidence = _audit_evidence(effect=effect, eligibility=eligibility)
            updated = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = 'failed',
                    updated_at = ?,
                    completed_at = ?,
                    last_error = ?
                WHERE effect_seq = ?
                  AND profile_id = ?
                  AND session_id = ?
                  AND ownership_generation = ?
                  AND status = 'pending'
                  AND attempt_count = 0
                  AND claim_id = ''
                  AND lease_owner = ''
                  AND lease_until IS NULL
                  AND completed_at IS NULL
                  AND last_error = ''
                """,
                (
                    now,
                    now,
                    HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE,
                    effect["effect_seq"],
                    key.profile_id,
                    key.session_id,
                    effect["ownership_generation"],
                ),
            )
            if updated.rowcount != 1:
                return _result(
                    HistoricalEffectTerminalizationStatus.REJECTED,
                    key=key,
                    effect_id=normalized_effect_id,
                    reason_code="effect_changed_during_terminalization",
                )
            conn.execute(
                """
                INSERT INTO agent_historical_effect_terminalizations (
                    audit_id, effect_seq, profile_id, session_id,
                    ownership_generation, effect_id, idempotency_key,
                    operation_id, effect_kind, contract_version,
                    contract_signature, effect_payload_sha256, failure_code,
                    evidence_json, terminalized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    audit_id,
                    effect["effect_seq"],
                    key.profile_id,
                    key.session_id,
                    effect["ownership_generation"],
                    normalized_effect_id,
                    effect["idempotency_key"],
                    effect["operation_id"],
                    effect["kind"],
                    effect["contract_version"],
                    effect["contract_signature"],
                    eligibility.payload_sha256,
                    HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE,
                    _canonical_json(audit_evidence),
                    now,
                ),
            )
        return _result(
            HistoricalEffectTerminalizationStatus.TERMINALIZED,
            key=key,
            effect_id=normalized_effect_id,
            reason_code=HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE,
            audit_id=audit_id,
        )


@dataclass(slots=True, frozen=True)
class _Eligibility:
    """Validated data retained only until the enclosing transaction commits."""

    reason_code: str = ""
    payload_sha256: str = ""
    aggregate_state: str = ""
    aggregate_state_revision: int = 0
    operation_status: str = ""


def _validate_eligibility(
    conn: sqlite3.Connection,
    *,
    effect: sqlite3.Row,
    key: SessionKey,
    active_ownership_generation: int,
) -> _Eligibility:
    """Return a reason instead of modifying a row when any proof is absent."""

    static = _validate_effect_row(
        effect,
        key=key,
        active_ownership_generation=active_ownership_generation,
    )
    if static.reason_code:
        return static
    payload = _canonical_object(effect["payload_json"], field_name="effect.payload_json")
    if payload is None:
        return _Eligibility(reason_code="effect_payload_invalid")
    aggregate = _load_aggregate(conn, key=key)
    if aggregate is None:
        return _Eligibility(reason_code="aggregate_missing")
    aggregate_shape = _aggregate_shape(
        aggregate,
        expected_generation=int(effect["ownership_generation"]),
    )
    if aggregate_shape is None:
        return _Eligibility(reason_code="aggregate_shape_invalid")
    data, active_chat_state = aggregate_shape
    live_reason = _live_work_reason(
        conn,
        key=key,
        ownership_generation=int(effect["ownership_generation"]),
    )
    if live_reason:
        return _Eligibility(reason_code=live_reason)
    operation = _load_operation(conn, operation_id=effect["operation_id"])
    if operation is None:
        return _Eligibility(reason_code="operation_missing")

    effect_kind = effect["kind"]
    if effect_kind in _WORKFLOW_EFFECT_KINDS:
        reason = _validate_workflow_shape(
            effect=effect,
            payload=payload,
            aggregate=aggregate,
            data=data,
            active_chat_state=active_chat_state,
            operation=operation,
            key=key,
        )
    elif effect_kind in _CONTROL_EFFECT_KINDS:
        reason = _validate_control_shape(
            effect=effect,
            payload=payload,
            aggregate=aggregate,
            data=data,
            operation=operation,
            key=key,
        )
    elif effect_kind in _RECONCILIATION_EFFECT_KINDS:
        reason = _validate_reconciliation_shape(
            effect=effect,
            payload=payload,
            aggregate=aggregate,
            data=data,
            active_chat_state=active_chat_state,
            operation=operation,
            key=key,
        )
    else:
        return _Eligibility(reason_code="historical_effect_contract_not_allowlisted")
    if reason:
        return _Eligibility(reason_code=reason)
    return _Eligibility(
        payload_sha256=hashlib.sha256(effect["payload_json"].encode("utf-8")).hexdigest(),
        aggregate_state=aggregate["state"],
        aggregate_state_revision=int(aggregate["state_revision"]),
        operation_status=operation["status"],
    )


def _validate_effect_row(
    effect: sqlite3.Row,
    *,
    key: SessionKey,
    active_ownership_generation: int,
) -> _Eligibility:
    """Validate the pristine outbox ownership and contract identity."""

    if effect["status"] != "pending":
        return _Eligibility(reason_code="effect_status_not_pending")
    if not _is_exact_int(effect["attempt_count"], minimum=0):
        return _Eligibility(reason_code="effect_attempt_count_invalid")
    if effect["attempt_count"] != 0:
        return _Eligibility(reason_code="effect_already_attempted")
    if effect["claim_id"] != "" or effect["lease_owner"] != "":
        return _Eligibility(reason_code="effect_claim_evidence_present")
    if effect["lease_until"] is not None or effect["completed_at"] is not None:
        return _Eligibility(reason_code="effect_lease_or_terminal_evidence_present")
    if effect["last_error"] != "":
        return _Eligibility(reason_code="effect_error_evidence_present")
    if not _is_exact_int(effect["ownership_generation"], minimum=1):
        return _Eligibility(reason_code="effect_ownership_generation_invalid")
    if effect["ownership_generation"] != active_ownership_generation:
        return _Eligibility(reason_code="effect_ownership_generation_not_active")
    if effect["profile_id"] != key.profile_id or effect["session_id"] != key.session_id:
        return _Eligibility(reason_code="effect_session_identity_changed")
    for field_name in ("effect_id", "idempotency_key", "event_id", "operation_id", "kind"):
        if not _is_canonical_nonempty_text(effect[field_name]):
            return _Eligibility(reason_code=f"effect_{field_name}_invalid")
    if not _is_exact_int(effect["contract_version"], minimum=1):
        return _Eligibility(reason_code="effect_contract_version_invalid")
    if not _is_canonical_nonempty_text(effect["contract_signature"]):
        return _Eligibility(reason_code="effect_contract_signature_invalid")
    contract_ref = (effect["kind"], effect["contract_version"])
    if contract_ref not in _HISTORICAL_EFFECT_CONTRACTS:
        return _Eligibility(reason_code="historical_effect_contract_not_allowlisted")
    try:
        contract = builtin_effect_contract(
            effect["kind"],
            version=effect["contract_version"],
        )
    except (KeyError, TypeError, ValueError):
        return _Eligibility(reason_code="historical_effect_contract_unknown")
    if effect["contract_signature"] != contract.signature:
        return _Eligibility(reason_code="historical_effect_contract_signature_changed")
    return _Eligibility()


def _validate_workflow_shape(
    *,
    effect: sqlite3.Row,
    payload: dict[str, Any],
    aggregate: sqlite3.Row,
    data: Mapping[str, Any],
    active_chat_state: Mapping[str, Any],
    operation: sqlite3.Row,
    key: SessionKey,
) -> str:
    """Require an inert bootstrap/round to retain its exact operation fence."""

    effect_kind = effect["kind"]
    expected_operation_kind = (
        "active_chat_bootstrap"
        if effect_kind == "run_active_chat_bootstrap"
        else "active_chat_round"
    )
    if aggregate["state"] != "active_chat":
        return "workflow_aggregate_state_changed"
    if not _nonnegative_row_int_matches(
        aggregate["active_epoch"], payload.get("active_epoch")
    ):
        return "workflow_active_epoch_changed"
    if not _nonnegative_row_int_matches(
        aggregate["activity_generation"], payload.get("activity_generation")
    ):
        return "workflow_activity_generation_changed"
    if active_chat_state.get("active_epoch") != aggregate["active_epoch"]:
        return "workflow_active_chat_epoch_changed"
    if effect_kind == "run_active_chat_bootstrap":
        if active_chat_state.get("bootstrap_status") != "pending":
            return "bootstrap_status_changed"
        if active_chat_state.get("bootstrap_operation_id") != effect["operation_id"]:
            return "bootstrap_operation_changed"
    else:
        if active_chat_state.get("bootstrap_status") != "completed":
            return "round_bootstrap_status_changed"
        if aggregate["active_chat_round_operation_id"] != effect["operation_id"]:
            return "round_operation_changed"
        if active_chat_state.get("round_operation_id") != effect["operation_id"]:
            return "round_state_operation_changed"
        if not _positive_int_list_matches(
            active_chat_state.get("round_input_message_log_ids"),
            payload.get("message_log_ids"),
        ):
            return "round_input_selection_changed"
    operation_reason = _validate_pending_operation(
        operation,
        key=key,
        effect=effect,
        expected_kind=expected_operation_kind,
        payload=payload,
    )
    if operation_reason:
        return operation_reason
    fence = _operation_fence(data, effect["operation_id"])
    if fence is None:
        return "workflow_operation_fence_missing"
    if _canonical_json(fence) != effect["payload_json"]:
        return "workflow_operation_fence_changed"
    return _validate_workflow_fence_identity(
        fence,
        effect=effect,
        aggregate=aggregate,
        expected_operation_kind=expected_operation_kind,
    )


def _validate_workflow_fence_identity(
    fence: Mapping[str, Any],
    *,
    effect: sqlite3.Row,
    aggregate: sqlite3.Row,
    expected_operation_kind: str,
) -> str:
    """Check the non-payload identity stored beside a workflow operation."""

    expected_text = {
        "operation_id": effect["operation_id"],
        "operation_kind": expected_operation_kind,
        "source_event_id": effect["event_id"],
        "effect_id": effect["effect_id"],
        "effect_kind": effect["kind"],
        "idempotency_key": effect["idempotency_key"],
    }
    for field_name, expected in expected_text.items():
        if fence.get(field_name) != expected or not _is_canonical_nonempty_text(
            fence.get(field_name)
        ):
            return f"workflow_fence_{field_name}_changed"
    if not _is_exact_int(fence.get("ownership_generation"), minimum=1) or (
        fence["ownership_generation"] != effect["ownership_generation"]
    ):
        return "workflow_fence_ownership_generation_changed"
    if fence.get("plan_id") != aggregate["current_plan_id"] or not isinstance(
        fence.get("plan_id"), str
    ):
        return "workflow_fence_plan_changed"
    for field_name in (
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
    ):
        if not _is_exact_int(fence.get(field_name), minimum=0):
            return f"workflow_fence_{field_name}_invalid"
    if fence["active_epoch"] != aggregate["active_epoch"]:
        return "workflow_fence_active_epoch_changed"
    if fence["activity_generation"] != aggregate["activity_generation"]:
        return "workflow_fence_activity_generation_changed"
    version_reason = _validate_contract_snapshot(fence, effect=effect)
    if version_reason:
        return version_reason
    for field_name in ("completion_event_id", "failure_event_id"):
        if not _is_canonical_nonempty_text(fence.get(field_name)):
            return f"workflow_fence_{field_name}_invalid"
    return ""


def _validate_control_shape(
    *,
    effect: sqlite3.Row,
    payload: dict[str, Any],
    aggregate: sqlite3.Row,
    data: Mapping[str, Any],
    operation: sqlite3.Row,
    key: SessionKey,
) -> str:
    """Require an old stop/cancel effect to remain tied to a terminal operation."""

    effect_kind = effect["kind"]
    intent = _control_intent(data, effect_kind)
    if intent is None:
        return "control_intent_missing"
    if intent.get("status") != "requested":
        return "control_intent_status_changed"
    expected_state = "idle" if effect_kind == "stop_active_chat_runtime" else "active_chat"
    identity_reason = _validate_control_intent_identity(
        intent,
        effect=effect,
        expected_control_kind=effect_kind,
        expected_state=expected_state,
        expected_desired_state=(
            "stopped" if effect_kind == "stop_active_chat_runtime" else "cancelled"
        ),
        aggregate=aggregate,
    )
    if identity_reason:
        return identity_reason
    if not _terminal_operation_matches(
        operation,
        key=key,
        effect=effect,
        expected_kind="idle_review_planning",
        payload=payload,
    ):
        return "control_target_operation_changed"
    expected_fields = (
        {
            "operation_id",
            "plan_id",
            "outcome",
            "reason",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
        }
        if effect_kind == "stop_active_chat_runtime"
        else {
            "operation_id",
            "plan_id",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
            "completion_event_id",
            "failure_event_id",
            "superseded_by_event_id",
        }
    )
    if set(payload) != expected_fields:
        return "control_payload_shape_changed"
    for field_name in (
        "operation_id",
        "plan_id",
        "active_epoch",
        "activity_generation",
        "input_watermark",
        "input_ledger_sequence",
        "completion_event_id",
        "failure_event_id",
    ):
        if payload.get(field_name) != intent.get(field_name):
            return f"control_payload_{field_name}_changed"
    if effect_kind == "stop_active_chat_runtime":
        if not isinstance(payload["outcome"], str) or not isinstance(payload["reason"], str):
            return "control_payload_outcome_invalid"
    elif not _is_canonical_nonempty_text(payload["superseded_by_event_id"]):
        return "control_payload_superseded_by_event_id_invalid"
    return ""


def _validate_reconciliation_shape(
    *,
    effect: sqlite3.Row,
    payload: dict[str, Any],
    aggregate: sqlite3.Row,
    data: Mapping[str, Any],
    active_chat_state: Mapping[str, Any],
    operation: sqlite3.Row,
    key: SessionKey,
) -> str:
    """Require a reconciliation effect to remain fenced by its control intent."""

    effect_kind = effect["kind"]
    control_kind = (
        "stop_active_chat_runtime"
        if effect_kind == "active_chat_runtime_reconciliation"
        else "cancel_idle_review_planning"
    )
    intent = _control_intent(data, control_kind)
    if intent is None:
        return "reconciliation_control_intent_missing"
    if intent.get("status") != "reconciliation_requested":
        return "reconciliation_control_intent_status_changed"
    expected_state = "idle" if control_kind == "stop_active_chat_runtime" else "active_chat"
    identity_reason = _validate_control_intent_identity(
        intent,
        effect=effect,
        expected_control_kind=control_kind,
        expected_state=expected_state,
        expected_desired_state=(
            "stopped" if control_kind == "stop_active_chat_runtime" else "cancelled"
        ),
        aggregate=aggregate,
        reconciliation=True,
    )
    if identity_reason:
        return identity_reason
    if expected_state == "active_chat" and active_chat_state.get("active_epoch") != aggregate[
        "active_epoch"
    ]:
        return "reconciliation_active_chat_epoch_changed"
    operation_reason = _validate_pending_operation(
        operation,
        key=key,
        effect=effect,
        expected_kind=effect_kind,
        payload=payload,
    )
    if operation_reason:
        return operation_reason
    fence = _operation_fence(data, effect["operation_id"])
    if fence is None:
        return "reconciliation_operation_fence_missing"
    identity_reason = _validate_workflow_fence_identity(
        fence,
        effect=effect,
        aggregate=aggregate,
        expected_operation_kind=effect_kind,
    )
    if identity_reason:
        return identity_reason.replace("workflow_fence", "reconciliation_fence")
    expected_fields = {
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
    }
    if set(payload) != expected_fields:
        return "reconciliation_payload_shape_changed"
    for field_name in expected_fields:
        if payload.get(field_name) != fence.get(field_name):
            return f"reconciliation_payload_{field_name}_changed"
    if payload.get("control_effect_kind") != control_kind:
        return "reconciliation_control_kind_changed"
    if payload.get("control_effect_id") != intent.get("effect_id"):
        return "reconciliation_control_effect_changed"
    if payload.get("desired_state") != intent.get("desired_state"):
        return "reconciliation_desired_state_changed"
    if not _is_exact_int(payload.get("reconciliation_cycle"), minimum=1):
        return "reconciliation_cycle_invalid"
    if intent.get("reconciliation_cycle") != payload["reconciliation_cycle"]:
        return "reconciliation_cycle_changed"
    return ""


def _validate_pending_operation(
    operation: sqlite3.Row,
    *,
    key: SessionKey,
    effect: sqlite3.Row,
    expected_kind: str,
    payload: Mapping[str, Any],
) -> str:
    """Require an effect-owned operation that has never acquired a lease."""

    if (
        operation["profile_id"] != key.profile_id
        or operation["session_id"] != key.session_id
        or operation["ownership_generation"] != effect["ownership_generation"]
        or operation["kind"] != expected_kind
        or operation["status"] != "pending"
        or operation["lease_owner"] != ""
        or operation["lease_until"] is not None
    ):
        return "operation_not_pristine_pending"
    if not _nonnegative_row_int_matches(
        operation["input_watermark"],
        payload.get("input_watermark"),
    ):
        return "operation_input_watermark_changed"
    if not _nonnegative_row_int_matches(
        operation["input_ledger_sequence"],
        payload.get("input_ledger_sequence"),
    ):
        return "operation_input_ledger_sequence_changed"
    return ""


def _terminal_operation_matches(
    operation: sqlite3.Row,
    *,
    key: SessionKey,
    effect: sqlite3.Row,
    expected_kind: str,
    payload: Mapping[str, Any],
) -> bool:
    """Return whether a control targets an already terminal local operation."""

    return (
        operation["profile_id"] == key.profile_id
        and operation["session_id"] == key.session_id
        and operation["ownership_generation"] == effect["ownership_generation"]
        and operation["kind"] == expected_kind
        and operation["status"] in _TERMINAL_OPERATION_STATUSES
        and operation["lease_owner"] == ""
        and operation["lease_until"] is None
        and _optional_nonnegative_row_int_matches(
            operation["input_watermark"],
            payload.get("input_watermark"),
        )
        and _optional_nonnegative_row_int_matches(
            operation["input_ledger_sequence"],
            payload.get("input_ledger_sequence"),
        )
    )


def _validate_control_intent_identity(
    intent: Mapping[str, Any],
    *,
    effect: sqlite3.Row,
    expected_control_kind: str,
    expected_state: str,
    expected_desired_state: str,
    aggregate: sqlite3.Row,
    reconciliation: bool = False,
) -> str:
    """Prove an aggregate's control intent still names this exact outbox row."""

    prefix = "reconciliation_" if reconciliation else ""
    expected = {
        f"{prefix}effect_id": effect["effect_id"],
        f"{prefix}idempotency_key": effect["idempotency_key"],
        f"{prefix}operation_id": effect["operation_id"],
        f"{prefix}contract_version": effect["contract_version"],
        f"{prefix}contract_signature": effect["contract_signature"],
    }
    if reconciliation:
        expected["reconciliation_kind"] = effect["kind"]
        expected["reconciliation_causation_id"] = effect["event_id"]
    else:
        expected["effect_kind"] = effect["kind"]
        expected["ownership_generation"] = effect["ownership_generation"]
        expected["causation_id"] = effect["event_id"]
    for field_name, value in expected.items():
        if intent.get(field_name) != value:
            return f"control_intent_{field_name}_changed"
    if aggregate["state"] != expected_state or intent.get("expected_state") != expected_state:
        return "control_intent_aggregate_state_changed"
    if intent.get("effect_kind") != expected_control_kind:
        return "control_intent_effect_kind_changed"
    if intent.get("desired_state") != expected_desired_state:
        return "control_intent_desired_state_changed"
    if intent.get("ownership_generation") != effect["ownership_generation"]:
        return "control_intent_ownership_generation_changed"
    if intent.get("active_epoch") != aggregate["active_epoch"]:
        return "control_intent_active_epoch_changed"
    if intent.get("activity_generation") != aggregate["activity_generation"]:
        return "control_intent_activity_generation_changed"
    if intent.get("expected_active_epoch") != aggregate["active_epoch"]:
        return "control_intent_active_epoch_changed"
    if intent.get("expected_activity_generation") != aggregate["activity_generation"]:
        return "control_intent_activity_generation_changed"
    if intent.get("expected_current_plan_id") not in {None, aggregate["current_plan_id"]}:
        return "control_intent_plan_changed"
    if intent.get("plan_id") != aggregate["current_plan_id"]:
        return "control_intent_plan_changed"
    if not reconciliation:
        return ""
    for field_name in (
        "reconciliation_completion_event_id",
        "reconciliation_failure_event_id",
    ):
        if not _is_canonical_nonempty_text(intent.get(field_name)):
            return f"control_intent_{field_name}_invalid"
    return ""


def _validate_contract_snapshot(fence: Mapping[str, Any], *, effect: sqlite3.Row) -> str:
    """Allow old v1 fences to omit a snapshot, but never to contradict one."""

    has_version = "contract_version" in fence
    has_signature = "contract_signature" in fence
    if effect["contract_version"] == 1 and not has_version and not has_signature:
        return ""
    if not has_version or not has_signature:
        return "workflow_fence_contract_snapshot_incomplete"
    if not _is_exact_int(fence.get("contract_version"), minimum=1):
        return "workflow_fence_contract_snapshot_changed"
    if not _is_canonical_nonempty_text(fence.get("contract_signature")):
        return "workflow_fence_contract_snapshot_changed"
    if (
        fence["contract_version"] != effect["contract_version"]
        or fence["contract_signature"] != effect["contract_signature"]
    ):
        return "workflow_fence_contract_snapshot_changed"
    return ""


def _live_work_reason(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
) -> str:
    """Reject maintenance while any independently executable work remains live."""

    mailbox = conn.execute(
        """
        SELECT 1 FROM agent_session_mailbox
        WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
          AND status IN ('pending', 'processing')
        LIMIT 1
        """,
        (key.profile_id, key.session_id, ownership_generation),
    ).fetchone()
    if mailbox is not None:
        return "live_mailbox_work_present"
    route = conn.execute(
        """
        SELECT 1 FROM agent_route_outbox
        WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
          AND status IN ('pending', 'processing')
        LIMIT 1
        """,
        (key.profile_id, key.session_id, ownership_generation),
    ).fetchone()
    if route is not None:
        return "live_route_work_present"
    receipt = conn.execute(
        """
        SELECT 1 FROM agent_external_action_receipts
        WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
          AND status IN ('prepared', 'executing', 'unknown')
        LIMIT 1
        """,
        (key.profile_id, key.session_id, ownership_generation),
    ).fetchone()
    if receipt is not None:
        return "live_external_action_receipt_present"
    return ""


def _load_effect(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    effect_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM agent_effect_outbox
        WHERE profile_id = ? AND session_id = ? AND effect_id = ?
        """,
        (key.profile_id, key.session_id, effect_id),
    ).fetchone()


def _load_aggregate(conn: sqlite3.Connection, *, key: SessionKey) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM agent_session_aggregates
        WHERE profile_id = ? AND session_id = ?
        """,
        (key.profile_id, key.session_id),
    ).fetchone()


def _load_operation(conn: sqlite3.Connection, *, operation_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM agent_session_operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()


def _load_audit(conn: sqlite3.Connection, *, effect_seq: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM agent_historical_effect_terminalizations
        WHERE effect_seq = ?
        """,
        (effect_seq,),
    ).fetchone()


def _existing_audit_result(
    audit: sqlite3.Row | None,
    *,
    effect: sqlite3.Row,
    key: SessionKey,
    effect_id: str,
) -> HistoricalEffectTerminalization | None:
    """Recognize only an audit row that proves this exact prior terminalization."""

    if audit is None:
        return None
    expected = {
        "effect_seq": effect["effect_seq"],
        "profile_id": key.profile_id,
        "session_id": key.session_id,
        "ownership_generation": effect["ownership_generation"],
        "effect_id": effect_id,
        "idempotency_key": effect["idempotency_key"],
        "operation_id": effect["operation_id"],
        "effect_kind": effect["kind"],
        "contract_version": effect["contract_version"],
        "contract_signature": effect["contract_signature"],
        "failure_code": HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE,
    }
    if any(audit[field_name] != value for field_name, value in expected.items()):
        return _result(
            HistoricalEffectTerminalizationStatus.REJECTED,
            key=key,
            effect_id=effect_id,
            reason_code="historical_terminalization_audit_identity_changed",
        )
    if audit["audit_id"] != _audit_id(
        key=key,
        effect_id=effect_id,
        ownership_generation=int(effect["ownership_generation"]),
    ):
        return _result(
            HistoricalEffectTerminalizationStatus.REJECTED,
            key=key,
            effect_id=effect_id,
            reason_code="historical_terminalization_audit_id_changed",
        )
    if not isinstance(effect["payload_json"], str) or (
        audit["effect_payload_sha256"]
        != hashlib.sha256(effect["payload_json"].encode("utf-8")).hexdigest()
    ):
        return _result(
            HistoricalEffectTerminalizationStatus.REJECTED,
            key=key,
            effect_id=effect_id,
            reason_code="historical_terminalization_audit_payload_changed",
        )
    if (
        effect["status"] != "failed"
        or effect["attempt_count"] != 0
        or effect["completed_at"] is None
        or effect["last_error"] != HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE
    ):
        return _result(
            HistoricalEffectTerminalizationStatus.REJECTED,
            key=key,
            effect_id=effect_id,
            reason_code="historical_terminalization_outbox_state_changed",
        )
    return _result(
        HistoricalEffectTerminalizationStatus.ALREADY_TERMINALIZED,
        key=key,
        effect_id=effect_id,
        reason_code=HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE,
        audit_id=audit["audit_id"],
    )


def _aggregate_shape(
    aggregate: sqlite3.Row,
    *,
    expected_generation: int,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Decode only the aggregate fields used as terminalization evidence."""

    if (
        aggregate["ownership_generation"] != expected_generation
        or not _is_canonical_nonempty_text(aggregate["state"])
        or not _is_exact_int(aggregate["state_revision"], minimum=0)
        or not _is_exact_int(aggregate["active_epoch"], minimum=0)
        or not _is_exact_int(aggregate["activity_generation"], minimum=0)
        or not isinstance(aggregate["current_plan_id"], str)
    ):
        return None
    data = _canonical_object(aggregate["data_json"], field_name="aggregate.data_json")
    active_chat_state = _canonical_object(
        aggregate["active_chat_state_json"],
        field_name="aggregate.active_chat_state_json",
    )
    if data is None or active_chat_state is None:
        return None
    return data, active_chat_state


def _operation_fence(data: Mapping[str, Any], operation_id: str) -> dict[str, Any] | None:
    registry = data.get("operation_fences")
    if not isinstance(registry, dict):
        return None
    fence = registry.get(operation_id)
    return fence if isinstance(fence, dict) else None


def _control_intent(data: Mapping[str, Any], effect_kind: str) -> dict[str, Any] | None:
    intents = data.get("effect_control_intents")
    if not isinstance(intents, dict):
        return None
    intent = intents.get(effect_kind)
    return intent if isinstance(intent, dict) else None


def _canonical_object(value: object, *, field_name: str) -> dict[str, Any] | None:
    if not isinstance(value, str):
        return None
    validation = validate_canonical_json_object(value)
    if validation.violations or validation.payload is None:
        return None
    return validation.payload


def _canonical_json(value: Mapping[str, Any]) -> str:
    """Encode validated JSON-compatible evidence with its stable ordering."""

    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def _audit_evidence(
    *,
    effect: sqlite3.Row,
    eligibility: _Eligibility,
) -> dict[str, Any]:
    """Build the compact proof retained without copying an effect payload."""

    return {
        "aggregate_state": eligibility.aggregate_state,
        "aggregate_state_revision": eligibility.aggregate_state_revision,
        "audit_schema_version": _AUDIT_SCHEMA_VERSION,
        "effect_payload_sha256": eligibility.payload_sha256,
        "operation_status": eligibility.operation_status,
        "proof": "never_claimed_no_live_mailbox_route_or_receipt",
        "terminalization_scope": "historical_active_chat_control_allowlist",
    }


def _audit_id(*, key: SessionKey, effect_id: str, ownership_generation: int) -> str:
    identity = "\x00".join(
        (
            str(_AUDIT_SCHEMA_VERSION),
            key.profile_id,
            key.session_id,
            str(ownership_generation),
            effect_id,
        )
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"historical-effect-terminalization:v{_AUDIT_SCHEMA_VERSION}:{digest}"


def _result(
    status: HistoricalEffectTerminalizationStatus,
    *,
    key: SessionKey,
    effect_id: str,
    reason_code: str,
    audit_id: str | None = None,
) -> HistoricalEffectTerminalization:
    return HistoricalEffectTerminalization(
        status=status,
        key=key,
        effect_id=effect_id,
        reason_code=reason_code,
        audit_id=audit_id,
    )


def _required_text(value: object, *, field_name: str) -> str:
    if not _is_canonical_nonempty_text(value):
        raise ValueError(f"{field_name} must be canonical non-empty text")
    return value


def _is_canonical_nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _is_exact_int(value: object, *, minimum: int) -> bool:
    return type(value) is int and value >= minimum


def _nonnegative_row_int_matches(row_value: object, payload_value: object) -> bool:
    return (
        _is_exact_int(row_value, minimum=0)
        and _is_exact_int(payload_value, minimum=0)
        and row_value == payload_value
    )


def _optional_nonnegative_row_int_matches(
    row_value: object,
    payload_value: object,
) -> bool:
    if row_value is None or payload_value is None:
        return row_value is None and payload_value is None
    return _nonnegative_row_int_matches(row_value, payload_value)


def _positive_int_list_matches(left: object, right: object) -> bool:
    if not isinstance(left, list) or not isinstance(right, list) or left != right:
        return False
    return bool(left) and all(_is_exact_int(value, minimum=1) for value in left)


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite non-negative number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be a finite non-negative number")
    return normalized


__all__ = [
    "HISTORICAL_EFFECT_TERMINALIZATION_FAILURE_CODE",
    "HistoricalEffectTerminalization",
    "HistoricalEffectTerminalizationStatus",
    "HistoricalEffectTerminalizer",
]
