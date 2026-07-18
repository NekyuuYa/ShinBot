"""Durable receipts for actor-owned externally visible actions."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectStatus,
)
from shinbot.agent.runtime.session_actor.execution_binding import (
    require_live_execution_binding_in_transaction,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionKind,
    ExternalActionReceiptStatus,
    ExternalActionRequest,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError
from shinbot.core.dispatch.fenced_wake_target_lease import FencedActorExecutionBinding
from shinbot.persistence.records import MessageLogRecord

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


class ExternalActionStoreError(RuntimeError):
    """Base error raised by external-action receipt persistence."""


class ExternalActionConflict(ExternalActionStoreError):
    """Raised when a durable logical action changes identity or evidence."""


class ExternalActionClaimLost(ExternalActionStoreError):
    """Raised when an execution attempt no longer owns its receipt fence."""


class ExternalActionOwnershipLost(ExternalActionStoreError):
    """Raised when the actor ownership generation is no longer active."""


class ExternalActionEffectConflict(ExternalActionConflict):
    """Raised when a request does not match its durable parent effect."""


class ExternalActionEffectClaimLost(ExternalActionClaimLost):
    """Raised when the durable parent effect claim is stale or not executable."""


class ExternalActionMigrationBlocked(ExternalActionStoreError):
    """Raised when non-terminal action receipts prevent ownership migration."""


@dataclass(slots=True, frozen=True)
class ExternalActionReceipt:
    """Durable state of one runtime-owned logical external action."""

    receipt_seq: int
    idempotency_key: str
    effect_id: str
    operation_id: str
    action_ordinal: int
    key: SessionKey
    ownership_generation: int
    kind: ExternalActionKind
    contract_version: int
    request_digest: str
    request_json: str
    status: ExternalActionReceiptStatus
    attempt_count: int
    claim_id: str = ""
    lease_owner: str = ""
    lease_until: float | None = None
    platform_result_json: str = "{}"
    rejection_json: str = "{}"
    unknown_json: str = "{}"
    assistant_message_log_id: int | None = None
    prepared_at: float = 0.0
    execution_started_at: float | None = None
    settled_at: float | None = None
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate persisted receipt identities and lifecycle metadata."""

        if self.receipt_seq < 1:
            raise ValueError("receipt_seq must be at least one")
        if isinstance(self.action_ordinal, bool) or not isinstance(
            self.action_ordinal,
            int,
        ):
            raise TypeError("action_ordinal must be a non-negative integer")
        if self.action_ordinal < 0:
            raise ValueError("action_ordinal must be a non-negative integer")
        for field_name in (
            "idempotency_key",
            "effect_id",
            "operation_id",
            "request_digest",
            "request_json",
        ):
            if not str(getattr(self, field_name) or "").strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.ownership_generation < 1:
            raise ValueError("ownership_generation must be at least one")
        if self.contract_version < 1:
            raise ValueError("contract_version must be at least one")
        if self.attempt_count < 0:
            raise ValueError("attempt_count must not be negative")


@dataclass(slots=True, frozen=True)
class ClaimedExternalAction:
    """One uniquely fenced execution attempt for an external action."""

    receipt: ExternalActionReceipt
    claim_id: str
    worker_id: str
    attempt_count: int
    claimed_at: float
    lease_expires_at: float

    def __post_init__(self) -> None:
        """Validate claim identity and its receipt snapshot."""

        if not str(self.claim_id or "").strip():
            raise ValueError("claim_id must not be empty")
        if not str(self.worker_id or "").strip():
            raise ValueError("worker_id must not be empty")
        if self.attempt_count < 1:
            raise ValueError("attempt_count must be at least one")
        if self.receipt.claim_id != self.claim_id:
            raise ValueError("receipt snapshot claim_id does not match claim")
        if self.receipt.lease_owner != self.worker_id:
            raise ValueError("receipt snapshot lease_owner does not match claim")

    @property
    def key(self) -> SessionKey:
        """Return the actor key that owns this external action."""

        return self.receipt.key

    @property
    def idempotency_key(self) -> str:
        """Return the runtime-owned downstream idempotency key."""

        return self.receipt.idempotency_key


@dataclass(slots=True, frozen=True)
class ExternalActionTerminalResult:
    """A durable terminal receipt that forbids another adapter dispatch."""

    receipt: ExternalActionReceipt
    reason_code: str

    def __post_init__(self) -> None:
        """Validate terminal status and diagnostic identity."""

        if not isinstance(self.receipt, ExternalActionReceipt):
            raise TypeError("receipt must be ExternalActionReceipt")
        if not self.receipt.status.terminal:
            raise ValueError("terminal execution result requires a terminal receipt")
        normalized_reason = _required_text(
            self.reason_code,
            field_name="reason_code",
        )
        object.__setattr__(self, "reason_code", normalized_reason)


@dataclass(slots=True, frozen=True)
class ExternalActionOrderBlockedResult:
    """A durable predecessor receipt prevents this action from dispatching.

    The result deliberately preserves the follower receipt and, when present,
    its exact predecessor. Callers can surface the durable reason and release
    the parent effect without treating a blocked action as a dispatch failure.
    """

    receipt: ExternalActionReceipt
    predecessor: ExternalActionReceipt | None
    reason_code: str

    def __post_init__(self) -> None:
        """Validate the fixed follower/predecessor relationship."""

        if not isinstance(self.receipt, ExternalActionReceipt):
            raise TypeError("receipt must be ExternalActionReceipt")
        if self.receipt.action_ordinal < 1:
            raise ValueError("only non-initial actions can be order-blocked")
        if self.predecessor is not None:
            if self.predecessor.key != self.receipt.key:
                raise ValueError("predecessor belongs to a different session")
            if self.predecessor.operation_id != self.receipt.operation_id:
                raise ValueError("predecessor belongs to a different operation")
            if (
                self.predecessor.ownership_generation
                != self.receipt.ownership_generation
            ):
                raise ValueError("predecessor belongs to a different generation")
            if self.predecessor.action_ordinal != self.receipt.action_ordinal - 1:
                raise ValueError("predecessor action ordinal is not adjacent")
        object.__setattr__(
            self,
            "reason_code",
            _required_text(self.reason_code, field_name="reason_code"),
        )


class SQLiteExternalActionReceiptStore:
    """SQLite receipt store with ownership, lease, and ABA fencing."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        lease_seconds: float = 30.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Initialize the external-action receipt store."""

        normalized_lease = _positive_finite(
            lease_seconds,
            field_name="lease_seconds",
        )
        self._database = database
        self._lease_seconds = normalized_lease
        self._clock = clock or time.time

    async def prepare(
        self,
        request: ExternalActionRequest,
        *,
        effect_claim: ClaimedEffect,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ExternalActionReceipt:
        """Persist an exact logical action owned by a live durable effect claim."""

        identity = _request_identity(request)
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now()
            self._require_ownership(
                conn,
                request.key,
                expected_generation=request.ownership_generation,
            )
            require_live_execution_binding_in_transaction(
                self._database,
                conn,
                execution_binding,
                key=request.key,
                ownership_generation=request.ownership_generation,
            )
            self._validate_effect_claim(
                conn,
                request,
                effect_claim,
                now=now,
            )
            existing_ordinal = self._select_operation_ordinal_receipt(
                conn,
                request,
            )
            if (
                existing_ordinal is not None
                and str(existing_ordinal["idempotency_key"])
                != request.idempotency_key
            ):
                raise ExternalActionConflict(
                    "operation action ordinal is already bound to a different "
                    f"external action: {request.operation_id}:"
                    f"{request.intent.action_ordinal}"
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', 0,
                          '', '', NULL, '{}', '{}', '{}', NULL, ?, NULL, NULL, ?)
                ON CONFLICT(idempotency_key) DO NOTHING
                """,
                (*identity, now, now),
            )
            row = self._select_receipt(conn, request.idempotency_key)
            assert row is not None
            self._validate_request_identity(row, request, identity=identity)
            return _receipt_from_row(row)

    async def begin_execution(
        self,
        request: ExternalActionRequest,
        *,
        effect_claim: ClaimedEffect,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> (
        ClaimedExternalAction
        | ExternalActionOrderBlockedResult
        | ExternalActionTerminalResult
        | None
    ):
        """Claim a prepared or pre-dispatch-rejected action for execution.

        An expired ``executing`` attempt is atomically changed to ``unknown``
        and is never reclaimed automatically. A fresh live parent-effect claim
        also proves that a differently claimed nested execution lost ownership;
        that attempt is settled ``unknown`` instead of being dispatched again.
        """

        identity = _request_identity(request)
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now()
            self._require_ownership(
                conn,
                request.key,
                expected_generation=request.ownership_generation,
            )
            require_live_execution_binding_in_transaction(
                self._database,
                conn,
                execution_binding,
                key=request.key,
                ownership_generation=request.ownership_generation,
            )
            effect_lease_until = self._validate_effect_claim(
                conn,
                request,
                effect_claim,
                now=now,
            )
            normalized_worker = effect_claim.worker_id
            row = self._select_receipt(conn, request.idempotency_key)
            if row is None:
                raise ExternalActionConflict(
                    "external action must be prepared before execution"
                )
            self._validate_request_identity(row, request, identity=identity)
            status = ExternalActionReceiptStatus(str(row["status"]))
            if status is ExternalActionReceiptStatus.EXECUTING:
                if float(row["lease_until"] or 0.0) <= now:
                    self._expire_executing_row(
                        conn,
                        row,
                        now=now,
                        recovered_by=normalized_worker,
                    )
                    terminal_reason = "execution_lease_expired"
                elif (
                    str(row["claim_id"]) == effect_claim.claim_id
                    and str(row["lease_owner"]) == effect_claim.worker_id
                ):
                    return None
                else:
                    self._settle_reclaimed_executing_row(
                        conn,
                        row,
                        effect_claim=effect_claim,
                        now=now,
                    )
                    terminal_reason = "outer_effect_claim_reclaimed"
                terminal_row = self._select_receipt(
                    conn,
                    request.idempotency_key,
                )
                assert terminal_row is not None
                return ExternalActionTerminalResult(
                    receipt=_receipt_from_row(terminal_row),
                    reason_code=terminal_reason,
                )
            if status not in {
                ExternalActionReceiptStatus.PREPARED,
                ExternalActionReceiptStatus.REJECTED_BEFORE_DISPATCH,
            }:
                receipt = _receipt_from_row(row)
                if not receipt.status.terminal:
                    raise ExternalActionConflict(
                        f"unsupported external action status: {receipt.status.value}"
                    )
                return ExternalActionTerminalResult(
                    receipt=receipt,
                    reason_code=f"receipt_already_{receipt.status.value}",
                )
            order_block = self._execution_order_block(conn, row)
            if order_block is not None:
                follower = _receipt_from_row(row)
                predecessor, reason_code = order_block
                return ExternalActionOrderBlockedResult(
                    receipt=follower,
                    predecessor=predecessor,
                    reason_code=reason_code,
                )
            if str(row["claim_id"] or "") == effect_claim.claim_id:
                raise ExternalActionEffectClaimLost(
                    "a rejected action requires a fresh durable effect claim"
                )
            claim_id = effect_claim.claim_id
            lease_until = _nonnegative_finite(
                min(now + self._lease_seconds, effect_lease_until),
                field_name="lease_until",
            )
            attempt_count = int(row["attempt_count"]) + 1
            claimed = conn.execute(
                """
                UPDATE agent_external_action_receipts
                SET status = 'executing', attempt_count = ?, claim_id = ?,
                    lease_owner = ?, lease_until = ?,
                    execution_started_at = ?, settled_at = NULL, updated_at = ?
                WHERE receipt_seq = ?
                  AND ownership_generation = ?
                  AND status IN ('prepared', 'rejected_before_dispatch')
                """,
                (
                    attempt_count,
                    claim_id,
                    normalized_worker,
                    lease_until,
                    now,
                    now,
                    row["receipt_seq"],
                    request.ownership_generation,
                ),
            )
            if claimed.rowcount != 1:
                raise ExternalActionClaimLost("external action changed during claim")
            conn.execute(
                """
                INSERT INTO agent_external_action_attempts (
                    idempotency_key, attempt_count, claim_id, lease_owner,
                    claimed_at, lease_until, status, platform_result_json,
                    rejection_json, unknown_json, assistant_message_log_id,
                    settled_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'executing', '{}', '{}', '{}',
                          NULL, NULL)
                """,
                (
                    request.idempotency_key,
                    attempt_count,
                    claim_id,
                    normalized_worker,
                    now,
                    lease_until,
                ),
            )
            claimed_row = self._select_receipt(conn, request.idempotency_key)
            assert claimed_row is not None
            receipt = _receipt_from_row(claimed_row)
            return ClaimedExternalAction(
                receipt=receipt,
                claim_id=claim_id,
                worker_id=normalized_worker,
                attempt_count=attempt_count,
                claimed_at=now,
                lease_expires_at=lease_until,
            )

    async def renew_lease(
        self,
        claim: ClaimedExternalAction,
        *,
        effect_claim: ClaimedEffect,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ClaimedExternalAction:
        """Renew a live action claim without outliving its parent effect claim."""

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now()
            self._require_claim_ownership(conn, claim)
            require_live_execution_binding_in_transaction(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                ownership_generation=claim.receipt.ownership_generation,
            )
            effect_lease_until = self._validate_effect_claim_for_receipt(
                conn,
                claim.receipt,
                effect_claim,
                now=now,
            )
            if (
                claim.claim_id != effect_claim.claim_id
                or claim.worker_id != effect_claim.worker_id
            ):
                raise ExternalActionEffectClaimLost(
                    "action claim does not match its durable effect claim"
                )
            lease_until = _nonnegative_finite(
                min(now + self._lease_seconds, effect_lease_until),
                field_name="lease_until",
            )
            updated = conn.execute(
                """
                UPDATE agent_external_action_receipts
                SET lease_until = ?, updated_at = ?
                WHERE idempotency_key = ?
                  AND ownership_generation = ?
                  AND status = 'executing'
                  AND claim_id = ? AND lease_owner = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    lease_until,
                    now,
                    claim.idempotency_key,
                    claim.receipt.ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                    now,
                ),
            )
            if updated.rowcount != 1:
                raise ExternalActionClaimLost(
                    "external action lease expired or changed"
                )
            attempt = conn.execute(
                """
                UPDATE agent_external_action_attempts
                SET lease_until = ?
                WHERE idempotency_key = ? AND claim_id = ?
                  AND lease_owner = ? AND status = 'executing'
                """,
                (
                    lease_until,
                    claim.idempotency_key,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if attempt.rowcount != 1:
                raise ExternalActionConflict("receipt attempt journal is incomplete")
            row = self._select_receipt(conn, claim.idempotency_key)
            assert row is not None
            return replace(
                claim,
                receipt=_receipt_from_row(row),
                lease_expires_at=lease_until,
            )

    async def reject_before_dispatch(
        self,
        claim: ClaimedExternalAction,
        *,
        reason_code: str,
        reason_message: str = "",
        evidence: Mapping[str, Any] | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ExternalActionReceipt:
        """Record a fenced failure known to precede adapter dispatch."""

        rejection_json = _canonical_json_object(
            {
                "evidence": dict(evidence or {}),
                "reason_code": _required_text(
                    reason_code,
                    field_name="reason_code",
                ),
                "reason_message": str(reason_message or ""),
            },
            field_name="rejection",
        )
        return await self._settle_without_message(
            claim,
            target_status=ExternalActionReceiptStatus.REJECTED_BEFORE_DISPATCH,
            field_name="rejection_json",
            evidence_json=rejection_json,
            allowed_statuses=(ExternalActionReceiptStatus.EXECUTING,),
            require_active_ownership=True,
            execution_binding=execution_binding,
        )

    async def mark_unknown(
        self,
        claim: ClaimedExternalAction,
        *,
        reason_code: str,
        reason_message: str = "",
        evidence: Mapping[str, Any] | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ExternalActionReceipt:
        """Conservatively stop retry after dispatch may have begun."""

        unknown_json = _canonical_json_object(
            {
                "evidence": dict(evidence or {}),
                "reason_code": _required_text(
                    reason_code,
                    field_name="reason_code",
                ),
                "reason_message": str(reason_message or ""),
            },
            field_name="unknown evidence",
        )
        return await self._settle_without_message(
            claim,
            target_status=ExternalActionReceiptStatus.UNKNOWN,
            field_name="unknown_json",
            evidence_json=unknown_json,
            allowed_statuses=(ExternalActionReceiptStatus.EXECUTING,),
            require_active_ownership=False,
            execution_binding=execution_binding,
        )

    async def settle_succeeded(
        self,
        claim: ClaimedExternalAction,
        *,
        platform_result: Mapping[str, Any],
        assistant_message: MessageLogRecord | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ExternalActionReceipt:
        """Atomically persist success and, for replies, its assistant log.

        A late result may resolve the same claim from ``unknown`` to
        ``succeeded``. It may not settle a newer claim.
        """

        result_json = _canonical_json_object(
            platform_result,
            field_name="platform_result",
        )
        self._validate_assistant_message(claim.receipt, assistant_message)
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            require_live_execution_binding_in_transaction(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                ownership_generation=claim.receipt.ownership_generation,
            )
            now = self._now()
            row = self._owned_receipt_row(conn, claim)
            status = ExternalActionReceiptStatus(str(row["status"]))
            if status is ExternalActionReceiptStatus.SUCCEEDED:
                self._validate_duplicate_success(
                    conn,
                    row,
                    result_json=result_json,
                    assistant_message=assistant_message,
                )
                return _receipt_from_row(row)
            if status not in {
                ExternalActionReceiptStatus.EXECUTING,
                ExternalActionReceiptStatus.UNKNOWN,
            }:
                raise ExternalActionClaimLost(
                    f"cannot settle success from receipt status {status.value}"
                )
            assistant_message_log_id = None
            if assistant_message is not None:
                assistant_message_log_id = (
                    self._database.message_logs.insert_with_connection(
                        conn,
                        assistant_message,
                    )
                )
            attempt = conn.execute(
                """
                UPDATE agent_external_action_attempts
                SET status = 'succeeded', platform_result_json = ?,
                    assistant_message_log_id = ?, settled_at = ?
                WHERE idempotency_key = ? AND claim_id = ?
                  AND lease_owner = ? AND status IN ('executing', 'unknown')
                """,
                (
                    result_json,
                    assistant_message_log_id,
                    now,
                    claim.idempotency_key,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if attempt.rowcount != 1:
                raise ExternalActionClaimLost(
                    "external action attempt changed before success"
                )
            updated = conn.execute(
                """
                UPDATE agent_external_action_receipts
                SET status = 'succeeded', platform_result_json = ?,
                    assistant_message_log_id = ?, lease_until = NULL,
                    settled_at = ?, updated_at = ?
                WHERE receipt_seq = ? AND ownership_generation = ?
                  AND claim_id = ? AND lease_owner = ?
                  AND status IN ('executing', 'unknown')
                """,
                (
                    result_json,
                    assistant_message_log_id,
                    now,
                    now,
                    row["receipt_seq"],
                    claim.receipt.ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if updated.rowcount != 1:
                raise ExternalActionClaimLost(
                    "external action claim changed before success"
                )
            settled_row = self._select_receipt(conn, claim.idempotency_key)
            assert settled_row is not None
            return _receipt_from_row(settled_row)

    async def recover_expired(self, *, worker_id: str) -> int:
        """Change expired attempts to unknown, including stale generations."""

        recovered_by = _required_text(worker_id, field_name="worker_id")
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now()
            rows = conn.execute(
                """
                SELECT * FROM agent_external_action_receipts
                WHERE ownership_generation >= 1
                  AND status = 'executing'
                  AND COALESCE(lease_until, 0) <= ?
                ORDER BY receipt_seq
                """,
                (now,),
            ).fetchall()
            count = 0
            for row in rows:
                self._expire_executing_row(
                    conn,
                    row,
                    now=now,
                    recovered_by=recovered_by,
                )
                count += 1
            return count

    async def get(
        self,
        key: SessionKey,
        idempotency_key: str,
    ) -> ExternalActionReceipt | None:
        """Return one receipt only when it belongs to the exact profile key."""

        normalized_key = _required_text(
            idempotency_key,
            field_name="idempotency_key",
        )
        with self._database.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM agent_external_action_receipts
                WHERE profile_id = ? AND session_id = ? AND idempotency_key = ?
                """,
                (key.profile_id, key.session_id, normalized_key),
            ).fetchone()
        return _receipt_from_row(row) if row is not None else None

    async def _settle_without_message(
        self,
        claim: ClaimedExternalAction,
        *,
        target_status: ExternalActionReceiptStatus,
        field_name: str,
        evidence_json: str,
        allowed_statuses: tuple[ExternalActionReceiptStatus, ...],
        require_active_ownership: bool,
        execution_binding: FencedActorExecutionBinding | None,
    ) -> ExternalActionReceipt:
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if require_active_ownership:
                self._require_claim_ownership(conn, claim)
            require_live_execution_binding_in_transaction(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                ownership_generation=claim.receipt.ownership_generation,
            )
            now = self._now()
            row = self._owned_receipt_row(conn, claim)
            current = ExternalActionReceiptStatus(str(row["status"]))
            if current is target_status:
                if str(row[field_name]) != evidence_json:
                    raise ExternalActionConflict(
                        f"duplicate {target_status.value} changed evidence"
                    )
                self._validate_duplicate_nonmessage_attempt(
                    conn,
                    row,
                    target_status=target_status,
                    field_name=field_name,
                    evidence_json=evidence_json,
                )
                return _receipt_from_row(row)
            if current not in allowed_statuses:
                raise ExternalActionClaimLost(
                    f"cannot settle {target_status.value} from {current.value}"
                )
            placeholders = ",".join("?" for _status in allowed_statuses)
            values = tuple(status.value for status in allowed_statuses)
            attempt = conn.execute(
                f"""
                UPDATE agent_external_action_attempts
                SET status = ?, {field_name} = ?, settled_at = ?
                WHERE idempotency_key = ? AND claim_id = ? AND lease_owner = ?
                  AND status IN ({placeholders})
                """,
                (
                    target_status.value,
                    evidence_json,
                    now,
                    claim.idempotency_key,
                    claim.claim_id,
                    claim.worker_id,
                    *values,
                ),
            )
            if attempt.rowcount != 1:
                raise ExternalActionClaimLost(
                    "external action attempt changed before settlement"
                )
            updated = conn.execute(
                f"""
                UPDATE agent_external_action_receipts
                SET status = ?, {field_name} = ?, lease_until = NULL,
                    settled_at = ?, updated_at = ?
                WHERE receipt_seq = ? AND ownership_generation = ?
                  AND claim_id = ? AND lease_owner = ?
                  AND status IN ({placeholders})
                """,
                (
                    target_status.value,
                    evidence_json,
                    now,
                    now,
                    row["receipt_seq"],
                    claim.receipt.ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                    *values,
                ),
            )
            if updated.rowcount != 1:
                raise ExternalActionClaimLost(
                    "external action claim changed before settlement"
                )
            settled = self._select_receipt(conn, claim.idempotency_key)
            assert settled is not None
            return _receipt_from_row(settled)

    def _expire_executing_row(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        now: float,
        recovered_by: str,
    ) -> None:
        unknown_json = _canonical_json_object(
            {
                "evidence": {
                    "expired_lease_until": row["lease_until"],
                    "recovered_by": recovered_by,
                },
                "reason_code": "execution_lease_expired",
                "reason_message": (
                    "adapter dispatch may have begun before the execution lease expired"
                ),
            },
            field_name="unknown evidence",
        )
        self._settle_executing_unknown_row(
            conn,
            row,
            unknown_json=unknown_json,
            now=now,
        )

    def _settle_reclaimed_executing_row(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        effect_claim: ClaimedEffect,
        now: float,
    ) -> None:
        """Fence nested execution when a fresh live outer claim proves ABA."""

        unknown_json = _canonical_json_object(
            {
                "evidence": {
                    "current_effect_attempt_count": effect_claim.attempt_count,
                    "current_effect_claim_id": effect_claim.claim_id,
                    "current_effect_lease_until": effect_claim.lease_expires_at,
                    "current_effect_worker_id": effect_claim.worker_id,
                    "previous_action_attempt_count": row["attempt_count"],
                    "previous_action_claim_id": row["claim_id"],
                    "previous_action_lease_until": row["lease_until"],
                    "previous_action_worker_id": row["lease_owner"],
                },
                "reason_code": "outer_effect_claim_reclaimed",
                "reason_message": (
                    "a fresh durable effect claim replaced the outer owner "
                    "while adapter dispatch may have been in progress"
                ),
            },
            field_name="unknown evidence",
        )
        self._settle_executing_unknown_row(
            conn,
            row,
            unknown_json=unknown_json,
            now=now,
        )

    @staticmethod
    def _settle_executing_unknown_row(
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        unknown_json: str,
        now: float,
    ) -> None:
        """Atomically settle one exact executing receipt and attempt unknown."""

        attempt = conn.execute(
            """
            UPDATE agent_external_action_attempts
            SET status = 'unknown', unknown_json = ?, settled_at = ?
            WHERE idempotency_key = ? AND claim_id = ? AND lease_owner = ?
              AND status = 'executing'
            """,
            (
                unknown_json,
                now,
                row["idempotency_key"],
                row["claim_id"],
                row["lease_owner"],
            ),
        )
        if attempt.rowcount != 1:
            raise ExternalActionConflict(
                "executing action attempt journal is incomplete"
            )
        updated = conn.execute(
            """
            UPDATE agent_external_action_receipts
            SET status = 'unknown', unknown_json = ?, lease_until = NULL,
                settled_at = ?, updated_at = ?
            WHERE receipt_seq = ? AND ownership_generation = ?
              AND status = 'executing' AND claim_id = ? AND lease_owner = ?
            """,
            (
                unknown_json,
                now,
                now,
                row["receipt_seq"],
                row["ownership_generation"],
                row["claim_id"],
                row["lease_owner"],
            ),
        )
        if updated.rowcount != 1:
            raise ExternalActionClaimLost(
                "executing external action changed before unknown settlement"
            )

    def _require_claim_ownership(
        self,
        conn: sqlite3.Connection,
        claim: ClaimedExternalAction,
    ) -> None:
        self._require_ownership(
            conn,
            claim.key,
            expected_generation=claim.receipt.ownership_generation,
        )

    def _require_ownership(
        self,
        conn: sqlite3.Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        try:
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                key,
                expected_generation=expected_generation,
            )
        except AgentRuntimeOwnershipError as exc:
            raise ExternalActionOwnershipLost(
                "external action ownership generation is no longer active"
            ) from exc

    def _validate_effect_claim(
        self,
        conn: sqlite3.Connection,
        request: ExternalActionRequest,
        effect_claim: ClaimedEffect,
        *,
        now: float,
    ) -> float:
        request_json = _canonical_json_object(
            request.to_effect_payload(),
            field_name="external action request",
        )
        return self._validate_effect_claim_identity(
            conn,
            effect_claim,
            key=request.key,
            ownership_generation=request.ownership_generation,
            effect_id=request.effect_id,
            idempotency_key=request.idempotency_key,
            operation_id=request.operation_id,
            source_event_id=request.source_event_id,
            action_kind=request.intent.kind,
            contract_version=request.contract_version,
            request_json=request_json,
            now=now,
        )

    def _validate_effect_claim_for_receipt(
        self,
        conn: sqlite3.Connection,
        receipt: ExternalActionReceipt,
        effect_claim: ClaimedEffect,
        *,
        now: float,
    ) -> float:
        request = _json_object(receipt.request_json, field_name="request_json")
        return self._validate_effect_claim_identity(
            conn,
            effect_claim,
            key=receipt.key,
            ownership_generation=receipt.ownership_generation,
            effect_id=receipt.effect_id,
            idempotency_key=receipt.idempotency_key,
            operation_id=receipt.operation_id,
            source_event_id=_required_text(
                request.get("source_event_id"),
                field_name="request_json.source_event_id",
            ),
            action_kind=receipt.kind,
            contract_version=receipt.contract_version,
            request_json=receipt.request_json,
            now=now,
        )

    @staticmethod
    def _validate_effect_claim_identity(
        conn: sqlite3.Connection,
        effect_claim: ClaimedEffect,
        *,
        key: SessionKey,
        ownership_generation: int,
        effect_id: str,
        idempotency_key: str,
        operation_id: str,
        source_event_id: str,
        action_kind: ExternalActionKind,
        contract_version: int,
        request_json: str,
        now: float,
    ) -> float:
        effect = effect_claim.effect
        requested_identity = (
            effect_id,
            idempotency_key,
            key.profile_id,
            key.session_id,
            ownership_generation,
            source_event_id,
            operation_id,
            action_kind.value,
            contract_version,
            request_json,
        )
        claimed_identity = (
            effect.effect_id,
            effect.idempotency_key,
            effect.key.profile_id,
            effect.key.session_id,
            effect.ownership_generation,
            effect.source_event_id,
            effect.operation_id,
            effect.kind,
            effect.contract_version,
            _canonical_json_object(
                effect.payload,
                field_name="durable effect payload",
            ),
        )
        if claimed_identity != requested_identity:
            raise ExternalActionEffectConflict(
                "external action request does not match its durable effect"
            )
        row = conn.execute(
            """
            SELECT * FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, effect_id),
        ).fetchone()
        if row is None:
            raise ExternalActionEffectClaimLost(
                "durable external-action effect no longer exists"
            )
        persisted_identity = (
            str(row["effect_id"]),
            str(row["idempotency_key"]),
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["ownership_generation"]),
            str(row["event_id"]),
            str(row["operation_id"]),
            str(row["kind"]),
            int(row["contract_version"]),
            str(row["contract_signature"]),
            str(row["payload_json"]),
        )
        claim_identity = (
            effect.effect_id,
            effect.idempotency_key,
            effect.key.profile_id,
            effect.key.session_id,
            effect.ownership_generation,
            effect.source_event_id,
            effect.operation_id,
            effect.kind,
            effect.contract_version,
            effect.contract_signature,
            _canonical_json_object(
                effect.payload,
                field_name="durable effect payload",
            ),
        )
        if persisted_identity != claim_identity:
            raise ExternalActionEffectConflict(
                "durable external-action effect changed immutable identity"
            )
        lease_until = float(row["lease_until"] or 0.0)
        if (
            str(row["status"]) != DurableEffectStatus.PROCESSING.value
            or int(row["attempt_count"]) != effect_claim.attempt_count
            or str(row["claim_id"]) != effect_claim.claim_id
            or str(row["lease_owner"]) != effect_claim.worker_id
            or lease_until <= now
            or lease_until < effect_claim.lease_expires_at
        ):
            raise ExternalActionEffectClaimLost(
                "durable external-action effect claim is stale or expired"
            )
        operation = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation, status
            FROM agent_session_operations WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()
        if operation is None:
            raise ExternalActionEffectConflict(
                "external-action effect references a missing operation"
            )
        if (
            str(operation["profile_id"]) != key.profile_id
            or str(operation["session_id"]) != key.session_id
            or int(operation["ownership_generation"]) != ownership_generation
        ):
            raise ExternalActionEffectConflict(
                "external-action effect operation changed ownership identity"
            )
        operation_status = str(operation["status"])
        if operation_status not in {"running", "completed"}:
            raise ExternalActionEffectClaimLost(
                "external-action effect operation is no longer executable: "
                + operation_status
            )
        return lease_until

    @staticmethod
    def _select_receipt(
        conn: sqlite3.Connection,
        idempotency_key: str,
    ) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT * FROM agent_external_action_receipts
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()

    @staticmethod
    def _select_operation_ordinal_receipt(
        conn: sqlite3.Connection,
        request: ExternalActionRequest,
    ) -> sqlite3.Row | None:
        """Return the exact durable action slot for one request ordinal."""

        return conn.execute(
            """
            SELECT * FROM agent_external_action_receipts
            WHERE profile_id = ? AND session_id = ?
              AND ownership_generation = ? AND operation_id = ?
              AND action_ordinal = ?
            """,
            (
                request.key.profile_id,
                request.key.session_id,
                request.ownership_generation,
                request.operation_id,
                request.intent.action_ordinal,
            ),
        ).fetchone()

    @staticmethod
    def _execution_order_block(
        conn: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> tuple[ExternalActionReceipt | None, str] | None:
        """Return the unsatisfied adjacent predecessor for one receipt.

        The check runs inside the same immediate transaction that converts a
        prepared receipt into ``executing``. That makes receipt success the
        durable handoff between adjacent action effects, independent of the
        reducer or any executor-local ordering.
        """

        ordinal = int(row["action_ordinal"])
        if ordinal == 0:
            return None
        predecessor_row = conn.execute(
            """
            SELECT * FROM agent_external_action_receipts
            WHERE profile_id = ? AND session_id = ?
              AND ownership_generation = ? AND operation_id = ?
              AND action_ordinal = ?
            """,
            (
                row["profile_id"],
                row["session_id"],
                row["ownership_generation"],
                row["operation_id"],
                ordinal - 1,
            ),
        ).fetchone()
        if predecessor_row is None:
            return None, "predecessor_receipt_missing"
        predecessor = _receipt_from_row(predecessor_row)
        if predecessor.status is ExternalActionReceiptStatus.SUCCEEDED:
            return None
        return predecessor, f"predecessor_{predecessor.status.value}"

    @staticmethod
    def _validate_request_identity(
        row: sqlite3.Row,
        request: ExternalActionRequest,
        *,
        identity: tuple[object, ...],
    ) -> None:
        persisted = (
            str(row["idempotency_key"]),
            str(row["effect_id"]),
            str(row["operation_id"]),
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["ownership_generation"]),
            int(row["action_ordinal"]),
            str(row["action_kind"]),
            int(row["contract_version"]),
            str(row["request_digest"]),
            str(row["request_json"]),
        )
        persisted_without_generation = (*persisted[:5], *persisted[6:])
        identity_without_generation = (*identity[:5], *identity[6:])
        if persisted_without_generation != identity_without_generation:
            raise ExternalActionConflict(
                "runtime idempotency key is already bound to a different action request: "
                f"{request.idempotency_key}"
            )
        if int(row["ownership_generation"]) == request.ownership_generation:
            return
        status = ExternalActionReceiptStatus(str(row["status"]))
        if not status.terminal:
            raise ExternalActionConflict(
                "non-terminal external action receipt belongs to a different "
                "ownership generation"
            )

    @staticmethod
    def _owned_receipt_row(
        conn: sqlite3.Connection,
        claim: ClaimedExternalAction,
    ) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT * FROM agent_external_action_receipts
            WHERE idempotency_key = ?
            """,
            (claim.idempotency_key,),
        ).fetchone()
        if row is None:
            raise ExternalActionClaimLost("external action receipt no longer exists")
        if (
            int(row["ownership_generation"])
            != claim.receipt.ownership_generation
            or str(row["claim_id"]) != claim.claim_id
            or str(row["lease_owner"]) != claim.worker_id
            or int(row["attempt_count"]) != claim.attempt_count
        ):
            raise ExternalActionClaimLost(
                "external action is no longer owned by this claim"
            )
        return row

    @staticmethod
    def _validate_assistant_message(
        receipt: ExternalActionReceipt,
        message: MessageLogRecord | None,
    ) -> None:
        if receipt.kind is ExternalActionKind.SEND_REPLY:
            if message is None:
                raise ValueError("send_reply success requires an assistant message log")
            if message.role != "assistant":
                raise ValueError("send_reply message log role must be assistant")
            request = _json_object(receipt.request_json, field_name="request_json")
            if message.session_id != request["target_session_id"]:
                raise ValueError(
                    "assistant message session does not match the external target"
                )
            return
        if message is not None:
            raise ValueError("poke and reaction receipts cannot create assistant logs")

    @staticmethod
    def _validate_duplicate_success(
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        result_json: str,
        assistant_message: MessageLogRecord | None,
    ) -> None:
        if str(row["platform_result_json"]) != result_json:
            raise ExternalActionConflict("duplicate success changed platform result")
        message_log_id = row["assistant_message_log_id"]
        attempt = conn.execute(
            """
            SELECT * FROM agent_external_action_attempts
            WHERE idempotency_key = ? AND claim_id = ?
            """,
            (row["idempotency_key"], row["claim_id"]),
        ).fetchone()
        if (
            attempt is None
            or int(attempt["attempt_count"]) != int(row["attempt_count"])
            or str(attempt["lease_owner"]) != str(row["lease_owner"])
            or str(attempt["status"]) != ExternalActionReceiptStatus.SUCCEEDED.value
            or str(attempt["platform_result_json"]) != result_json
            or attempt["assistant_message_log_id"] != message_log_id
        ):
            raise ExternalActionConflict(
                "succeeded receipt disagrees with its attempt journal"
            )
        if assistant_message is None:
            if message_log_id is not None:
                raise ExternalActionConflict("duplicate success omitted assistant log")
            return
        if message_log_id is None:
            raise ExternalActionConflict("succeeded reply receipt is missing its log")
        persisted = conn.execute(
            "SELECT * FROM message_logs WHERE id = ?",
            (message_log_id,),
        ).fetchone()
        if persisted is None or _message_identity_from_row(persisted) != (
            _message_identity(assistant_message)
        ):
            raise ExternalActionConflict("duplicate success changed assistant log")

    @staticmethod
    def _validate_duplicate_nonmessage_attempt(
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        target_status: ExternalActionReceiptStatus,
        field_name: str,
        evidence_json: str,
    ) -> None:
        attempt = conn.execute(
            """
            SELECT * FROM agent_external_action_attempts
            WHERE idempotency_key = ? AND claim_id = ?
            """,
            (row["idempotency_key"], row["claim_id"]),
        ).fetchone()
        if (
            attempt is None
            or int(attempt["attempt_count"]) != int(row["attempt_count"])
            or str(attempt["lease_owner"]) != str(row["lease_owner"])
            or str(attempt["status"]) != target_status.value
            or str(attempt[field_name]) != evidence_json
        ):
            raise ExternalActionConflict(
                f"{target_status.value} receipt disagrees with its attempt journal"
            )

    def _now(self) -> float:
        return _nonnegative_finite(self._clock(), field_name="clock")


def validate_external_action_migration(
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    now: float | None = None,
) -> None:
    """Reconcile safely abandoned work, then reject live action receipts.

    ``succeeded``, ``unknown``, and ``abandoned_before_dispatch`` rows are
    historical evidence and retain the generation under which the action was
    attempted. The reconciliation can only terminally abandon a prepared or
    pre-dispatch-rejected receipt when its exact outer effect is already
    durably ``failed``; it never changes ``executing`` or ``unknown`` rows.
    """

    from shinbot.persistence.repositories.agent_external_action_reconciliation import (
        reconcile_abandoned_before_dispatch_receipts,
    )

    reconcile_abandoned_before_dispatch_receipts(conn, key, now=now)

    rows = conn.execute(
        """
        SELECT idempotency_key, status
        FROM agent_external_action_receipts
        WHERE profile_id = ? AND session_id = ?
          AND status IN ('prepared', 'executing', 'rejected_before_dispatch')
        ORDER BY receipt_seq
        """,
        (key.profile_id, key.session_id),
    ).fetchall()
    if rows:
        summary = ", ".join(
            f"{row['idempotency_key']}:{row['status']}" for row in rows
        )
        raise ExternalActionMigrationBlocked(
            "live external action receipts block ownership migration: " + summary
        )


def _request_identity(request: ExternalActionRequest) -> tuple[object, ...]:
    request_json = _canonical_json_object(
        request.to_effect_payload(),
        field_name="external action request",
    )
    return (
        request.idempotency_key,
        request.effect_id,
        request.operation_id,
        request.key.profile_id,
        request.key.session_id,
        request.ownership_generation,
        request.intent.action_ordinal,
        request.intent.kind.value,
        request.contract_version,
        request.request_digest,
        request_json,
    )


def _receipt_from_row(row: sqlite3.Row) -> ExternalActionReceipt:
    return ExternalActionReceipt(
        receipt_seq=int(row["receipt_seq"]),
        idempotency_key=str(row["idempotency_key"]),
        effect_id=str(row["effect_id"]),
        operation_id=str(row["operation_id"]),
        action_ordinal=int(row["action_ordinal"]),
        key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
        ownership_generation=int(row["ownership_generation"]),
        kind=ExternalActionKind(str(row["action_kind"])),
        contract_version=int(row["contract_version"]),
        request_digest=str(row["request_digest"]),
        request_json=str(row["request_json"]),
        status=ExternalActionReceiptStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        claim_id=str(row["claim_id"] or ""),
        lease_owner=str(row["lease_owner"] or ""),
        lease_until=_optional_float(row["lease_until"]),
        platform_result_json=str(row["platform_result_json"] or "{}"),
        rejection_json=str(row["rejection_json"] or "{}"),
        unknown_json=str(row["unknown_json"] or "{}"),
        assistant_message_log_id=(
            int(row["assistant_message_log_id"])
            if row["assistant_message_log_id"] is not None
            else None
        ),
        prepared_at=float(row["prepared_at"]),
        execution_started_at=_optional_float(row["execution_started_at"]),
        settled_at=_optional_float(row["settled_at"]),
        updated_at=float(row["updated_at"]),
    )


def _message_identity(record: MessageLogRecord) -> tuple[object, ...]:
    return (
        record.session_id,
        record.platform_msg_id,
        record.sender_id,
        record.sender_name,
        record.content_json,
        record.raw_text,
        record.role,
        int(record.is_read),
        int(record.is_mentioned),
        record.created_at,
    )


def _message_identity_from_row(row: sqlite3.Row) -> tuple[object, ...]:
    return (
        str(row["session_id"]),
        str(row["platform_msg_id"]),
        str(row["sender_id"]),
        str(row["sender_name"]),
        str(row["content_json"]),
        str(row["raw_text"]),
        str(row["role"]),
        int(row["is_read"]),
        int(row["is_mentioned"]),
        float(row["created_at"]),
    )


def _canonical_json_object(value: object, *, field_name: str) -> str:
    normalized = _normalize_json(value, path=field_name)
    if not isinstance(normalized, dict):
        raise TypeError(f"{field_name} must be a mapping")
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _normalize_json(value: object, *, path: str) -> Any:
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} keys must be strings")
            normalized[key] = _normalize_json(item, path=f"{path}.{key}")
        return normalized
    if isinstance(value, (list, tuple)):
        return [
            _normalize_json(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} numbers must be finite")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"{path} contains a non-JSON value")


def _json_object(value: str, *, field_name: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExternalActionConflict(f"{field_name} contains invalid JSON") from exc
    if not isinstance(loaded, dict):
        raise ExternalActionConflict(f"{field_name} must contain an object")
    return loaded


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be finite and non-negative") from exc
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _positive_finite(value: object, *, field_name: str) -> float:
    normalized = _nonnegative_finite(value, field_name=field_name)
    if normalized <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return normalized


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


__all__ = [
    "ClaimedExternalAction",
    "ExternalActionClaimLost",
    "ExternalActionConflict",
    "ExternalActionEffectClaimLost",
    "ExternalActionEffectConflict",
    "ExternalActionMigrationBlocked",
    "ExternalActionOrderBlockedResult",
    "ExternalActionOwnershipLost",
    "ExternalActionReceipt",
    "ExternalActionStoreError",
    "ExternalActionTerminalResult",
    "SQLiteExternalActionReceiptStore",
    "validate_external_action_migration",
]
