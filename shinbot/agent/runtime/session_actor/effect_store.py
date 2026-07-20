"""SQLite adapter for the durable session-actor effect executor."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    EffectContractAuthorityError,
    EffectExecutionContract,
    builtin_effect_contract_authority,
    resolved_outcome_fence_fields,
)
from shinbot.agent.runtime.session_actor.effect_execution_errors import (
    EffectExecutionCancelled,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    DurableEffectStatus,
    EffectClaimLost,
    EffectExpiryRecoveryResult,
    EffectQuarantineReason,
    EffectSettlementResult,
    EffectSettlementStatus,
    FencedEffectExecutionLeaseLost,
    completion_event_id,
    derived_effect_event_id,
    failure_event_id,
    quarantined_event_id,
    skipped_event_id,
)
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope
from shinbot.agent.runtime.session_actor.execution_binding import (
    require_live_execution_binding_in_transaction,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
    ExternalActionRequest,
    builtin_external_action_effect_contracts,
)
from shinbot.agent.runtime.session_actor.model_execution_cancellation_gate import (
    ModelExecutionCancellationBlocked,
    ModelExecutionCancellationGateError,
    cancel_claimed_model_execution_if_gated,
    is_model_execution_cancellation_target,
)
from shinbot.agent.runtime.session_actor.model_execution_witness import (
    MODEL_EXECUTION_UNKNOWN_EVENT_KIND,
    MODEL_EXECUTION_UNKNOWN_EVENT_SOURCE,
    MODEL_EXECUTION_WITNESSED_EFFECT_KINDS,
    ModelExecutionClaim,
    ModelExecutionUnknownNotice,
    mark_expired_model_execution_unknown,
)
from shinbot.agent.runtime.session_actor.review_execution_gate import (
    REVIEW_EXECUTION_UNKNOWN_EVENT_KIND,
    REVIEW_EXECUTION_UNKNOWN_EVENT_SOURCE,
    ReviewExecutionClaim,
    ReviewExecutionGateError,
    ReviewExecutionUnknownNotice,
    cancel_claimed_review_effect_if_gated,
    mark_expired_review_execution_unknown,
)
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionFenceError
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnership,
    AgentRuntimeOwnershipError,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)
from shinbot.persistence.canonical_json import (
    MAX_CANONICAL_JSON_BYTES,
    validate_canonical_json_object,
)
from shinbot.persistence.sqlite_raw import (
    RawSQLiteValue,
    bounded_raw_sqlite_projection,
    complete_truncated_raw_sqlite_value,
    decode_raw_sqlite_values,
    raw_sqlite_values,
)

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


class EffectStoreConflict(RuntimeError):
    """Raised when a durable effect or completion id changes identity."""


_EXTERNAL_ACTION_EFFECT_KINDS = tuple(kind.value for kind in ExternalActionKind)
_EXTERNAL_ACTION_CONTRACT_SIGNATURES = {
    (contract.effect_kind, contract.version): contract.signature
    for contract in builtin_external_action_effect_contracts()
}
_SQLITE_INT64_MAX = (1 << 63) - 1
_MALFORMED_EFFECT_KIND = "__malformed_persisted_effect__"
_MALFORMED_EFFECT_SIGNATURE = "store-quarantine-v1"
_EFFECT_SCRUB_CURSOR = "claimable"
_EFFECT_SCRUB_PAGE_SIZE = 64
_EFFECT_SCRUB_MAX_VALIDATION_ROWS = 8
_EFFECT_SCRUB_BYTE_BUDGET = (2 * MAX_CANONICAL_JSON_BYTES) + 131_072
_MAX_INLINE_CLAIM_QUARANTINES = 1
_EFFECT_METADATA_FIELD_BYTE_LIMIT = 65_536
_EFFECT_EVIDENCE_PREFIX_BYTES = 192
_MODEL_WORKFLOW_EFFECT_KINDS = (
    "run_active_reply_workflow",
    "run_active_chat_bootstrap",
    "run_active_chat_round",
    "run_idle_review_planning",
    "run_review_workflow",
)
_MODEL_WORKFLOW_EFFECT_KINDS_SQL = ", ".join(
    f"'{effect_kind}'" for effect_kind in _MODEL_WORKFLOW_EFFECT_KINDS
)
_GENERIC_MODEL_WORKFLOW_EFFECT_KINDS_SQL = ", ".join(
    f"'{effect_kind}'" for effect_kind in sorted(MODEL_EXECUTION_WITNESSED_EFFECT_KINDS)
)
_EFFECT_ROW_EVIDENCE_FIELDS = (
    "effect_seq",
    "effect_id",
    "idempotency_key",
    "profile_id",
    "session_id",
    "ownership_generation",
    "event_id",
    "operation_id",
    "kind",
    "contract_version",
    "contract_signature",
    "payload_json",
    "status",
    "attempt_count",
    "available_at",
    "claim_id",
    "lease_owner",
    "lease_until",
    "created_at",
    "updated_at",
    "completed_at",
    "last_error",
)
_EFFECT_RAW_BYTE_LIMITS = {
    field_name: (
        MAX_CANONICAL_JSON_BYTES
        if field_name == "payload_json"
        else _EFFECT_METADATA_FIELD_BYTE_LIMIT
    )
    for field_name in _EFFECT_ROW_EVIDENCE_FIELDS
}
_EFFECT_RAW_PROJECTION = bounded_raw_sqlite_projection(
    "effect",
    _EFFECT_ROW_EVIDENCE_FIELDS,
    byte_limits=_EFFECT_RAW_BYTE_LIMITS,
)
_SOURCE_TRACE_RAW_PROJECTION = bounded_raw_sqlite_projection(
    "source",
    ("trace_id",),
    byte_limits={"trace_id": _EFFECT_METADATA_FIELD_BYTE_LIMIT},
    output_prefix="source_",
)
_EFFECT_METADATA_PROJECTION = bounded_raw_sqlite_projection(
    "effect",
    _EFFECT_ROW_EVIDENCE_FIELDS,
    byte_limits=dict.fromkeys(
        _EFFECT_ROW_EVIDENCE_FIELDS,
        _EFFECT_EVIDENCE_PREFIX_BYTES,
    ),
)
_SOURCE_TRACE_METADATA_PROJECTION = bounded_raw_sqlite_projection(
    "source",
    ("trace_id",),
    byte_limits={"trace_id": _EFFECT_EVIDENCE_PREFIX_BYTES},
    output_prefix="source_",
)
_EFFECT_SCRUB_HEADER_FIELDS = (
    "ownership_generation",
    "status",
    "available_at",
    "lease_until",
)
_EFFECT_SCRUB_HEADER_PROJECTION = bounded_raw_sqlite_projection(
    "effect",
    _EFFECT_SCRUB_HEADER_FIELDS,
    byte_limits=dict.fromkeys(
        _EFFECT_SCRUB_HEADER_FIELDS,
        _EFFECT_EVIDENCE_PREFIX_BYTES,
    ),
    output_prefix="scrub_",
)
_EFFECT_SCRUB_PAGE_SQL = """
    SELECT effect.effect_seq AS scrub_effect_seq
    FROM agent_effect_outbox AS effect NOT INDEXED
    WHERE effect.effect_seq > ?
    ORDER BY effect.effect_seq
    LIMIT ?
"""


class SQLiteDurableEffectStore:
    """Lease-fenced SQLite outbox with atomic mailbox settlement."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        lease_seconds: float = 30.0,
        clock: Callable[[], float] | None = None,
        contract_authority: EffectContractAuthority | None = None,
    ) -> None:
        """Initialize the durable effect store.

        Args:
            database: Initialized ShinBot database manager.
            lease_seconds: Claim duration before another worker may reclaim it.
            clock: Injectable wall clock for tests.
            contract_authority: Immutable policy snapshot allowed to settle
                effects. Omitting it uses the complete built-in Actor v2 graph.
        """

        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        self._database = database
        self._lease_seconds = float(lease_seconds)
        self._clock = clock or time.time
        if contract_authority is not None and not isinstance(
            contract_authority,
            EffectContractAuthority,
        ):
            raise TypeError("contract_authority must be an EffectContractAuthority")
        self._contract_authority = contract_authority or builtin_effect_contract_authority()
        self._quarantine_notifications: deque[EffectSettlementResult] = deque()

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the immutable contract authority used by this store."""

        return self._contract_authority

    @property
    def persistence_domain(self) -> object:
        """Return the DatabaseManager that owns this transaction domain."""

        return self._database

    async def claim_next(
        self,
        *,
        worker_id: str,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ClaimedEffect | None:
        """Claim the oldest available or expired effect with a fresh claim id."""

        normalized_worker_id = str(worker_id or "").strip()
        if not normalized_worker_id:
            raise ValueError("worker_id must not be empty")
        filter_sql, filter_params, priority_sql, priority_params = _contract_filter_sql(
            effect_contracts,
            excluded_effect_contracts,
            table_alias="effect",
        )
        action_order_sql, action_order_params = _external_action_order_gate_sql("effect")
        binding_filter_sql, binding_params = _effect_execution_binding_filter(
            execution_binding,
            table_alias="effect",
        )
        cancellation_gate_sql = _model_work_cancellation_gate_sql("effect")
        claimed: ClaimedEffect | None = None
        quarantine_notifications: list[EffectSettlementResult] = []
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
            )
            now = _nonnegative_finite(self._clock(), field_name="clock")
            lease_until = _nonnegative_finite(
                now + self._lease_seconds,
                field_name="lease_until",
            )
            scrub_notifications, scrub_streamed_oversized = (
                self._quarantine_malformed_claimable_rows(conn, now=now)
                if execution_binding is None
                else ((), False)
            )
            quarantine_notifications.extend(scrub_notifications)
            inline_quarantines = 0
            while effect_contracts != () and not scrub_streamed_oversized:
                if inline_quarantines >= _MAX_INLINE_CLAIM_QUARANTINES:
                    break
                row = conn.execute(
                    f"""
                    SELECT effect.effect_seq AS selected_effect_seq,
                           {_EFFECT_RAW_PROJECTION},
                           ownership.generation AS active_ownership_generation,
                           {_SOURCE_TRACE_RAW_PROJECTION}
                    FROM agent_effect_outbox AS effect
                    JOIN agent_session_runtime_ownership AS ownership
                      ON ownership.profile_id = effect.profile_id
                     AND ownership.session_id = effect.session_id
                     AND ownership.mode = 'actor_v2'
                     AND ownership.status = 'active'
                     AND ownership.generation = effect.ownership_generation
                    LEFT JOIN agent_session_mailbox AS source
                      ON source.profile_id = effect.profile_id
                     AND source.session_id = effect.session_id
                     AND source.event_id = effect.event_id
                    WHERE (
                        (
                            effect.status = 'pending'
                            AND effect.available_at <= ?
                        ) OR (
                            effect.status = 'processing'
                            AND COALESCE(effect.lease_until, 0) <= ?
                        )
                    )
                    AND effect.ownership_generation >= 1
                    {binding_filter_sql}
                    {filter_sql}
                    {action_order_sql}
                    {cancellation_gate_sql}
                    ORDER BY {priority_sql} effect.effect_seq ASC
                    LIMIT 1
                    """,
                    (
                        now,
                        now,
                        *binding_params,
                        *filter_params,
                        *action_order_params,
                        *priority_params,
                    ),
                ).fetchone()
                if row is None:
                    break
                raw_values, decoded, decoding_violations = _raw_effect_row(
                    conn,
                    row,
                    effect_seq=int(row["selected_effect_seq"]),
                )
                violations = (
                    *decoding_violations,
                    *_claim_row_violations(decoded),
                )
                if violations:
                    if execution_binding is not None:
                        raise EffectStoreConflict(
                            "scoped fenced effect executor cannot quarantine malformed work"
                        )
                    quarantine_notifications.append(
                        self._quarantine_malformed_effect_row(
                            conn,
                            raw_values,
                            decoded,
                        now=now,
                        violations=violations,
                        execution_binding=execution_binding,
                        )
                    )
                    inline_quarantines += 1
                    continue
                try:
                    effect = _effect_from_row(decoded)
                except (
                    EffectStoreConflict,
                    OverflowError,
                    RecursionError,
                    TypeError,
                    ValueError,
                ) as exc:
                    if execution_binding is not None:
                        raise EffectStoreConflict(
                            "scoped fenced effect executor cannot decode its durable effect"
                        ) from exc
                    quarantine_notifications.append(
                        self._quarantine_malformed_effect_row(
                            conn,
                            raw_values,
                            decoded,
                        now=now,
                        violations=("effect_envelope_decode_failed",),
                        execution_binding=execution_binding,
                        )
                    )
                    inline_quarantines += 1
                    continue
                ownership_generation = effect.ownership_generation
                _require_effect_ownership(
                    self._database,
                    conn,
                    effect.key,
                    expected_generation=ownership_generation,
                )
                _require_effect_execution_binding(
                    self._database,
                    conn,
                    execution_binding,
                    key=effect.key,
                    expected_generation=ownership_generation,
                )
                claim_id = uuid.uuid4().hex
                attempt_count = int(decoded["attempt_count"]) + 1
                updated = conn.execute(
                    f"""
                    UPDATE agent_effect_outbox
                    SET status = 'processing',
                        attempt_count = ?,
                        claim_id = ?,
                        lease_owner = ?,
                        lease_until = ?,
                        updated_at = ?,
                        last_error = ''
                    WHERE effect_seq = ?
                      AND ownership_generation = ?
                      AND (
                          (status = 'pending' AND available_at <= ?)
                          OR (
                              status = 'processing'
                              AND COALESCE(lease_until, 0) <= ?
                          )
                      )
                      AND kind = ?
                      AND contract_version = ?
                      AND contract_signature = ?
                      {_model_work_cancellation_gate_sql("agent_effect_outbox")}
                    """,
                    (
                        attempt_count,
                        claim_id,
                        normalized_worker_id,
                        lease_until,
                        now,
                        decoded["effect_seq"],
                        ownership_generation,
                        now,
                        now,
                        effect.kind,
                        effect.contract_version,
                        effect.contract_signature,
                    ),
                )
                if updated.rowcount != 1:
                    break
                claimed = ClaimedEffect(
                    claim_id=claim_id,
                    effect=effect,
                    worker_id=normalized_worker_id,
                    attempt_count=attempt_count,
                    claimed_at=now,
                    lease_expires_at=lease_until,
                )
                break
        self._quarantine_notifications.extend(quarantine_notifications)
        return claimed

    async def drain_quarantine_notifications(
        self,
    ) -> tuple[EffectSettlementResult, ...]:
        """Return store-owned mailbox events committed during maintenance scans."""

        notifications = tuple(self._quarantine_notifications)
        self._quarantine_notifications.clear()
        return notifications

    def _quarantine_malformed_claimable_rows(
        self,
        conn: sqlite3.Connection,
        *,
        now: float,
    ) -> tuple[tuple[EffectSettlementResult, ...], bool]:
        cursor_row = conn.execute(
            """
            SELECT last_effect_seq
            FROM agent_effect_scrub_state
            WHERE cursor_name = ?
            """,
            (_EFFECT_SCRUB_CURSOR,),
        ).fetchone()
        if cursor_row is None or not _is_integer_at_least(
            cursor_row["last_effect_seq"],
            0,
        ):
            raise EffectStoreConflict("effect scrub cursor is missing or invalid")
        cursor = int(cursor_row["last_effect_seq"])
        candidate_effect_seqs = self._select_scrub_sequence_page(
            conn,
            after_effect_seq=cursor,
        )
        if not candidate_effect_seqs and cursor > 0:
            candidate_effect_seqs = self._select_scrub_sequence_page(
                conn,
                after_effect_seq=0,
            )
        quarantined: list[EffectSettlementResult] = []
        validated_rows = 0
        materialized_bytes = 0
        streamed_oversized = False
        next_cursor = 0 if not candidate_effect_seqs else cursor
        for effect_seq in candidate_effect_seqs:
            header_row = self._select_effect_scrub_header(
                conn,
                effect_seq=effect_seq,
            )
            if header_row is None or not _scrub_header_is_claimable(
                header_row,
                now=now,
            ):
                next_cursor = effect_seq
                continue
            if validated_rows >= _EFFECT_SCRUB_MAX_VALIDATION_ROWS:
                break
            metadata_row = self._select_effect_metadata_for_scrub(
                conn,
                effect_seq=effect_seq,
            )
            if metadata_row is None:
                next_cursor = effect_seq
                continue
            planned_bytes, has_oversized_field = _effect_row_materialization_plan(metadata_row)
            if validated_rows and (
                has_oversized_field
                or materialized_bytes + planned_bytes > _EFFECT_SCRUB_BYTE_BUDGET
            ):
                break
            row = self._select_effect_row_for_scrub(conn, effect_seq=effect_seq)
            if row is None:
                next_cursor = effect_seq
                continue
            raw_values, decoded, decoding_violations = _raw_effect_row(
                conn,
                row,
                effect_seq=effect_seq,
            )
            validated_rows += 1
            if has_oversized_field:
                materialized_bytes = _EFFECT_SCRUB_BYTE_BUDGET
                streamed_oversized = True
            else:
                materialized_bytes += planned_bytes
            next_cursor = effect_seq
            violations = (
                *decoding_violations,
                *_claim_row_violations(decoded),
            )
            if not violations:
                continue
            quarantined.append(
                self._quarantine_malformed_effect_row(
                    conn,
                    raw_values,
                    decoded,
                    now=now,
                    violations=violations,
                )
            )
        updated_cursor = conn.execute(
            """
            UPDATE agent_effect_scrub_state
            SET last_effect_seq = ?, updated_at = ?
            WHERE cursor_name = ? AND last_effect_seq = ?
            """,
            (next_cursor, now, _EFFECT_SCRUB_CURSOR, cursor),
        )
        if updated_cursor.rowcount != 1:
            raise EffectStoreConflict("effect scrub cursor changed concurrently")
        return tuple(quarantined), streamed_oversized

    @staticmethod
    def _select_scrub_sequence_page(
        conn: sqlite3.Connection,
        *,
        after_effect_seq: int,
    ) -> tuple[int, ...]:
        cursor = conn.execute(
            _EFFECT_SCRUB_PAGE_SQL,
            (after_effect_seq, _EFFECT_SCRUB_PAGE_SIZE),
        )
        return tuple(
            int(row["scrub_effect_seq"]) for row in cursor.fetchmany(_EFFECT_SCRUB_PAGE_SIZE)
        )

    @staticmethod
    def _select_effect_scrub_header(
        conn: sqlite3.Connection,
        *,
        effect_seq: int,
    ) -> sqlite3.Row | None:
        return conn.execute(
            f"""
            SELECT {_EFFECT_SCRUB_HEADER_PROJECTION},
                   ownership.generation AS active_ownership_generation
            FROM agent_effect_outbox AS effect
            JOIN agent_session_runtime_ownership AS ownership
              ON ownership.profile_id = effect.profile_id
             AND ownership.session_id = effect.session_id
             AND ownership.mode = 'actor_v2'
             AND ownership.status = 'active'
            WHERE effect.effect_seq = ?
            """,
            (effect_seq,),
        ).fetchone()

    @staticmethod
    def _select_effect_metadata_for_scrub(
        conn: sqlite3.Connection,
        *,
        effect_seq: int,
    ) -> sqlite3.Row | None:
        return conn.execute(
            f"""
            SELECT effect.effect_seq AS selected_effect_seq,
                   {_EFFECT_METADATA_PROJECTION},
                   ownership.generation AS active_ownership_generation,
                   {_SOURCE_TRACE_METADATA_PROJECTION}
            FROM agent_effect_outbox AS effect
            JOIN agent_session_runtime_ownership AS ownership
              ON ownership.profile_id = effect.profile_id
             AND ownership.session_id = effect.session_id
             AND ownership.mode = 'actor_v2'
             AND ownership.status = 'active'
            LEFT JOIN agent_session_mailbox AS source
              ON source.profile_id = effect.profile_id
             AND source.session_id = effect.session_id
             AND source.event_id = effect.event_id
            WHERE effect.effect_seq = ?
            """,
            (effect_seq,),
        ).fetchone()

    @staticmethod
    def _select_effect_row_for_scrub(
        conn: sqlite3.Connection,
        *,
        effect_seq: int,
    ) -> sqlite3.Row | None:
        return conn.execute(
            f"""
            SELECT effect.effect_seq AS selected_effect_seq,
                   {_EFFECT_RAW_PROJECTION},
                   ownership.generation AS active_ownership_generation,
                   {_SOURCE_TRACE_RAW_PROJECTION}
            FROM agent_effect_outbox AS effect
            JOIN agent_session_runtime_ownership AS ownership
              ON ownership.profile_id = effect.profile_id
             AND ownership.session_id = effect.session_id
             AND ownership.mode = 'actor_v2'
             AND ownership.status = 'active'
            LEFT JOIN agent_session_mailbox AS source
              ON source.profile_id = effect.profile_id
             AND source.session_id = effect.session_id
             AND source.event_id = effect.event_id
            WHERE effect.effect_seq = ?
            """,
            (effect_seq,),
        ).fetchone()

    def _quarantine_malformed_effect_row(
        self,
        conn: sqlite3.Connection,
        raw_values: dict[str, RawSQLiteValue],
        row: dict[str, object],
        *,
        now: float,
        violations: tuple[str, ...] | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult:
        violations = violations or _claim_row_violations(row)
        if not violations:
            raise EffectStoreConflict("raw effect quarantine requires a malformed durable row")
        profile_id = row["profile_id"]
        session_id = row["session_id"]
        if not _is_canonical_nonempty_text(profile_id) or not (
            _is_canonical_nonempty_text(session_id)
        ):
            raise EffectStoreConflict(
                "malformed effect row cannot be assigned to a canonical session"
            )
        key = SessionKey(profile_id, session_id)
        ownership_generation = _persistable_ownership_generation(row["active_ownership_generation"])
        ownership = _require_effect_ownership(
            self._database,
            conn,
            key,
            expected_generation=ownership_generation,
        )
        _require_effect_execution_binding(
            self._database,
            conn,
            execution_binding,
            key=key,
            expected_generation=ownership_generation,
        )
        handoff_request = _wake_request_for_ownership(ownership)
        conn.execute("SAVEPOINT malformed_effect_quarantine_candidate")
        try:
            evidence = {
                field_name: raw_values[field_name].evidence()
                for field_name in (*_EFFECT_ROW_EVIDENCE_FIELDS, "source_trace_id")
            }
            evidence_digest = hashlib.sha256(_json_dumps(evidence).encode("utf-8")).hexdigest()
            effect_seq = int(row["effect_seq"])
            effect_id = (
                str(row["effect_id"])
                if _is_canonical_nonempty_text(row["effect_id"])
                else f"malformed-effect:{effect_seq}:{evidence_digest[:16]}"
            )
            idempotency_key = (
                str(row["idempotency_key"])
                if _is_canonical_nonempty_text(row["idempotency_key"])
                else f"malformed-idempotency:{effect_seq}:{evidence_digest[:16]}"
            )
            source_event_id = (
                str(row["event_id"])
                if _is_canonical_nonempty_text(row["event_id"])
                else f"malformed-source:{effect_seq}:{evidence_digest[:16]}"
            )
            operation_id = (
                str(row["operation_id"]) if _is_canonical_text(row["operation_id"]) else ""
            )
            attempt_count = (
                int(row["attempt_count"])
                if _is_integer_at_least(row["attempt_count"], 0)
                else 0
            )
            created_at = (
                float(row["created_at"])
                if _is_nonnegative_finite_number(row["created_at"])
                else now
            )
            failure_message = "durable effect row failed validation: " + ", ".join(violations)
            event_id = derived_effect_event_id(
                key=key,
                effect_id=effect_id,
                outcome="quarantined",
            )
            trace_id = (
                str(row["source_trace_id"])
                if isinstance(row["source_trace_id"], str)
                else ""
            )
            envelope = SessionEventEnvelope(
                event_id=event_id,
                key=key,
                kind="EffectQuarantined",
                ownership_generation=ownership_generation,
                payload={
                    "attempt_count": attempt_count,
                    "contract_signature": _MALFORMED_EFFECT_SIGNATURE,
                    "contract_version": 1,
                    "effect_id": effect_id,
                    "effect_kind": _MALFORMED_EFFECT_KIND,
                    "failure_code": EffectQuarantineReason.MALFORMED_EFFECT_ROW.value,
                    "failure_message": failure_message,
                    "idempotency_key": idempotency_key,
                    "operation_id": operation_id,
                    "raw_row": evidence,
                    "reason_code": EffectQuarantineReason.MALFORMED_EFFECT_ROW.value,
                    "reason_message": failure_message,
                    "violations": list(violations),
                },
                source="effect_store",
                occurred_at=now,
                causation_id=source_event_id,
                correlation_id=operation_id or effect_id,
                trace_id=trace_id,
                available_at=now,
                created_at=now,
            )
            mailbox_id = self._insert_event(
                conn,
                envelope,
                payload_json=_json_dumps(envelope.payload),
                now=now,
                handoff_request=handoff_request,
            )
            sentinel_payload_json = _json_dumps(
                {"quarantine_event_id": event_id, "quarantined": True}
            )
            error = f"{EffectQuarantineReason.MALFORMED_EFFECT_ROW.value}: {failure_message}"
            updated = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET effect_id = ?,
                    idempotency_key = ?,
                    ownership_generation = ?,
                    event_id = ?,
                    operation_id = ?,
                    kind = ?,
                    contract_version = 1,
                    contract_signature = ?,
                    payload_json = ?,
                    status = 'failed',
                    attempt_count = ?,
                    available_at = ?,
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    created_at = ?,
                    updated_at = ?,
                    completed_at = ?,
                    last_error = ?
                WHERE effect_seq = ?
                  AND status = ?
                """,
                (
                    effect_id,
                    idempotency_key,
                    ownership_generation,
                    source_event_id,
                    operation_id,
                    _MALFORMED_EFFECT_KIND,
                    _MALFORMED_EFFECT_SIGNATURE,
                    sentinel_payload_json,
                    attempt_count,
                    now,
                    created_at,
                    now,
                    now,
                    error,
                    effect_seq,
                    row["status"],
                ),
            )
            if updated.rowcount != 1:
                raise EffectClaimLost("malformed effect changed during quarantine")
            final_ownership = _final_effect_ownership(self._database, conn, ownership)
            result = EffectSettlementResult(
                status=EffectSettlementStatus.COMMITTED,
                effect_id=effect_id,
                event_id=event_id,
                key=key,
                wake_request=_wake_request_for_ownership(final_ownership),
                mailbox_id=mailbox_id,
            )
        except Exception:
            conn.execute("ROLLBACK TO SAVEPOINT malformed_effect_quarantine_candidate")
            conn.execute("RELEASE SAVEPOINT malformed_effect_quarantine_candidate")
            raise
        conn.execute("RELEASE SAVEPOINT malformed_effect_quarantine_candidate")
        return result

    async def renew_lease(
        self,
        claim: ClaimedEffect,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ClaimedEffect:
        """Extend a current, unexpired effect lease without changing its fence."""

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.effect.ownership_generation
            )
            _require_effect_ownership(
                self._database,
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                expected_generation=ownership_generation,
            )
            now = _nonnegative_finite(self._clock(), field_name="clock")
            cancelled = self._cancel_review_claim_if_gated(
                conn,
                claim=claim,
                now=now,
                evidence="review_gate_lease_renewal",
            )
            if cancelled is not None:
                raise EffectExecutionCancelled("review_cancellation_gate:" + claim.effect.effect_id)
            lease_until = _nonnegative_finite(
                now + self._lease_seconds,
                field_name="lease_until",
            )
            renewed = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET lease_until = ?, updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    lease_until,
                    now,
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.effect.effect_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                    now,
                ),
            )
            if renewed.rowcount != 1:
                raise EffectClaimLost("effect lease is expired or no longer owned")
        return replace(claim, lease_expires_at=lease_until)

    async def complete_with_event(
        self,
        claim: ClaimedEffect,
        completion_envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...] | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult:
        """Atomically complete an effect and insert its mailbox completion.

        ``outcome_fence_fields`` remains a compatibility assertion for older
        callers. The store always resolves the projection from its sealed
        contract authority and rejects a caller value that differs.
        """

        return self._settle_with_event(
            claim,
            completion_envelope,
            target_status=DurableEffectStatus.COMPLETED,
            error="",
            outcome_fence_fields=outcome_fence_fields,
            execution_binding=execution_binding,
        )

    async def release_for_retry(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        available_at: float,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult | None:
        """Return a current claim to pending at an explicit retry time."""

        return await self._release(
            claim,
            error=error,
            available_at=max(
                _nonnegative_finite(self._clock(), field_name="clock"),
                _nonnegative_finite(available_at, field_name="available_at"),
            ),
            execution_binding=execution_binding,
        )

    async def defer_without_attempt(
        self,
        claim: ClaimedEffect,
        *,
        reason: str,
        available_at: float,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult | None:
        """Release a live claim without spending one bounded retry attempt.

        This is reserved for a durable condition that has already been proven
        transient by the handler, such as waiting for a fenced control target
        to acknowledge cancellation.  It never writes a mailbox event.
        """

        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("deferred effect reason must not be empty")
        normalized_available_at = max(
            _nonnegative_finite(self._clock(), field_name="clock"),
            _nonnegative_finite(available_at, field_name="available_at"),
        )
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.effect.ownership_generation
            )
            _require_effect_ownership(
                self._database,
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                expected_generation=ownership_generation,
            )
            now = _nonnegative_finite(self._clock(), field_name="clock")
            cancelled = self._cancel_review_claim_if_gated(
                conn,
                claim=claim,
                now=now,
                evidence="review_gate_deferred_release",
            )
            if cancelled is not None:
                return cancelled
            deferred = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = 'pending',
                    attempt_count = CASE
                        WHEN attempt_count > 0 THEN attempt_count - 1
                        ELSE 0
                    END,
                    available_at = ?,
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    updated_at = ?,
                    last_error = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                """,
                (
                    normalized_available_at,
                    now,
                    normalized_reason,
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.effect.effect_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if deferred.rowcount != 1:
                raise EffectClaimLost("effect is not owned by this claim")
        return None

    async def fail_with_event(
        self,
        claim: ClaimedEffect,
        failure_envelope: SessionEventEnvelope,
        *,
        error: str,
        outcome_fence_fields: tuple[str, ...] | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult:
        """Atomically fail an effect and insert its terminal failure event.

        ``outcome_fence_fields`` remains a compatibility assertion for older
        callers. The store always resolves the projection from its sealed
        contract authority and rejects a caller value that differs.
        """

        return self._settle_with_event(
            claim,
            failure_envelope,
            target_status=DurableEffectStatus.FAILED,
            error=error,
            outcome_fence_fields=outcome_fence_fields,
            execution_binding=execution_binding,
        )

    async def quarantine(
        self,
        claim: ClaimedEffect,
        *,
        reason: EffectQuarantineReason,
        message: str,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult:
        """Atomically terminalize unsupported work with a store-owned event."""

        if not isinstance(reason, EffectQuarantineReason):
            raise TypeError("effect quarantine reason is invalid")
        if not isinstance(message, str):
            raise TypeError("effect quarantine message must be a string")
        effect = claim.effect
        event_id = quarantined_event_id(effect)
        result: EffectSettlementResult
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(effect.ownership_generation)
            ownership = _require_effect_ownership(
                self._database,
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                expected_generation=ownership_generation,
            )
            handoff_request = _wake_request_for_ownership(ownership)
            now = _nonnegative_finite(self._clock(), field_name="clock")
            conn.execute("SAVEPOINT effect_quarantine_candidate")
            try:
                cancelled = self._cancel_review_claim_if_gated(
                    conn,
                    claim=claim,
                    now=now,
                    evidence="review_gate_quarantine",
                )
                if cancelled is not None:
                    _final_effect_ownership(self._database, conn, ownership)
                    result = cancelled
                else:
                    row = conn.execute(
                        """
                        SELECT * FROM agent_effect_outbox
                        WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                        """,
                        (claim.key.profile_id, claim.key.session_id, effect.effect_id),
                    ).fetchone()
                    if row is None:
                        raise EffectClaimLost("durable effect no longer exists")
                    self._validate_effect_identity(row, effect)
                    if int(row["attempt_count"]) != claim.attempt_count:
                        raise EffectClaimLost("effect quarantine attempt count changed")
                    envelope = SessionEventEnvelope(
                        event_id=event_id,
                        key=claim.key,
                        kind="EffectQuarantined",
                        ownership_generation=ownership_generation,
                        payload={
                            "attempt_count": claim.attempt_count,
                            "contract_signature": effect.contract_signature,
                            "contract_version": effect.contract_version,
                            "effect_id": effect.effect_id,
                            "effect_kind": effect.kind,
                            "failure_code": reason.value,
                            "failure_message": message,
                            "idempotency_key": effect.idempotency_key,
                            "operation_id": effect.operation_id,
                            "reason_code": reason.value,
                            "reason_message": message,
                        },
                        source="effect_store",
                        occurred_at=now,
                        causation_id=effect.source_event_id,
                        correlation_id=effect.operation_id or effect.effect_id,
                        trace_id=effect.trace_id,
                        available_at=now,
                        created_at=now,
                    )
                    payload_json = _json_dumps(envelope.payload)
                    persisted_status = DurableEffectStatus(str(row["status"]))
                    if persisted_status is DurableEffectStatus.FAILED:
                        if str(row["claim_id"]) != claim.claim_id:
                            raise EffectClaimLost("a different claim already terminalized this effect")
                        existing = conn.execute(
                            """
                            SELECT mailbox_id, kind, source, ownership_generation, payload_json,
                                   causation_id, correlation_id, trace_id
                            FROM agent_session_mailbox
                            WHERE profile_id = ? AND session_id = ? AND event_id = ?
                            """,
                            (claim.key.profile_id, claim.key.session_id, event_id),
                        ).fetchone()
                        expected_identity = (
                            "EffectQuarantined",
                            "effect_store",
                            ownership_generation,
                            payload_json,
                            effect.source_event_id,
                            effect.operation_id or effect.effect_id,
                            effect.trace_id,
                        )
                        if existing is None or tuple(existing)[1:] != expected_identity:
                            raise EffectStoreConflict(
                                "persisted effect quarantine changed diagnostic identity"
                            )
                        mailbox_id = int(existing["mailbox_id"])
                        final_ownership = _final_effect_ownership(
                            self._database,
                            conn,
                            ownership,
                        )
                        result = EffectSettlementResult(
                            status=EffectSettlementStatus.ALREADY_COMMITTED,
                            effect_id=effect.effect_id,
                            event_id=event_id,
                            key=claim.key,
                            wake_request=_wake_request_for_ownership(final_ownership),
                            mailbox_id=mailbox_id,
                        )
                    else:
                        self._validate_live_claim(row, claim, now=now)
                        mailbox_id = self._insert_event(
                            conn,
                            envelope,
                            payload_json=payload_json,
                            now=now,
                            handoff_request=handoff_request,
                        )
                        error = f"{reason.value}: {message}"
                        settled = conn.execute(
                            """
                            UPDATE agent_effect_outbox
                            SET status = 'failed',
                                lease_owner = '',
                                lease_until = NULL,
                                updated_at = ?,
                                completed_at = ?,
                                last_error = ?
                            WHERE profile_id = ?
                              AND session_id = ?
                              AND effect_id = ?
                              AND ownership_generation = ?
                              AND status = 'processing'
                              AND claim_id = ?
                              AND lease_owner = ?
                              AND COALESCE(lease_until, 0) > ?
                            """,
                            (
                                now,
                                now,
                                error,
                                claim.key.profile_id,
                                claim.key.session_id,
                                effect.effect_id,
                                ownership_generation,
                                claim.claim_id,
                                claim.worker_id,
                                now,
                            ),
                        )
                        if settled.rowcount != 1:
                            raise EffectClaimLost("effect claim changed during quarantine")
                        final_ownership = _final_effect_ownership(
                            self._database,
                            conn,
                            ownership,
                        )
                        result = EffectSettlementResult(
                            status=EffectSettlementStatus.COMMITTED,
                            effect_id=effect.effect_id,
                            event_id=event_id,
                            key=claim.key,
                            wake_request=_wake_request_for_ownership(final_ownership),
                            mailbox_id=mailbox_id,
                        )
            except Exception:
                conn.execute("ROLLBACK TO SAVEPOINT effect_quarantine_candidate")
                conn.execute("RELEASE SAVEPOINT effect_quarantine_candidate")
                raise
            conn.execute("RELEASE SAVEPOINT effect_quarantine_candidate")
        return result

    async def release(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult | None:
        """Immediately release a live claim so shutdown never strands it."""

        return await self._release(
            claim,
            error=error,
            available_at=self._clock(),
            execution_binding=execution_binding,
        )

    async def recover_expired(
        self,
        *,
        worker_id: str,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> int:
        """Recover reclaimable leases and publish unrecoverable model evidence."""

        if execution_binding is not None:
            raise ValueError(
                "scoped fenced effect execution requires an explicit recovery controller"
            )
        result = await self._recover_expired(
            worker_id=worker_id,
            execution_binding=None,
        )
        self._quarantine_notifications.extend(result.notifications)
        return result.recovered_count

    async def recover_expired_fenced(
        self,
        *,
        worker_id: str,
        execution_binding: FencedActorExecutionBinding,
    ) -> EffectExpiryRecoveryResult:
        """Recover only one live target's expired effect history.

        The caller must retain the matching target-lease capability for every
        durable mutation in this pass. Recovery notifications are returned to
        the caller instead of joining the process-wide maintenance queue, so a
        scoped executor cannot consume or wake another target's mailbox debt.
        """

        if not isinstance(execution_binding, FencedActorExecutionBinding):
            raise TypeError("execution_binding must be a FencedActorExecutionBinding")
        return await self._recover_expired(
            worker_id=worker_id,
            execution_binding=execution_binding,
        )

    async def _recover_expired(
        self,
        *,
        worker_id: str,
        execution_binding: FencedActorExecutionBinding | None,
    ) -> EffectExpiryRecoveryResult:
        """Run one bounded expiry-maintenance transaction under one scope."""

        if not str(worker_id or "").strip():
            raise ValueError("worker_id must not be empty")
        binding_filter_sql, binding_params = _effect_execution_binding_filter(
            execution_binding,
            table_alias="effect",
        )
        recovery_notifications: list[EffectSettlementResult] = []
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
            )
            now = _nonnegative_finite(self._clock(), field_name="clock")
            expired_review_runs = conn.execute(
                f"""
                SELECT effect.profile_id, effect.session_id,
                       effect.ownership_generation, effect.effect_id, effect.event_id,
                       effect.operation_id, effect.kind, effect.contract_version,
                       effect.contract_signature, effect.attempt_count,
                       effect.claim_id, effect.lease_owner,
                       review_run.execution_status
                FROM agent_effect_outbox AS effect
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = effect.profile_id
                 AND ownership.session_id = effect.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = effect.ownership_generation
                JOIN agent_review_execution_runs AS review_run
                  ON review_run.profile_id = effect.profile_id
                 AND review_run.session_id = effect.session_id
                 AND review_run.ownership_generation = effect.ownership_generation
                 AND review_run.review_effect_id = effect.effect_id
                 AND review_run.claim_id = effect.claim_id
                 AND review_run.worker_id = effect.lease_owner
                WHERE effect.ownership_generation >= 1
                  {binding_filter_sql}
                  AND effect.kind = 'run_review_workflow'
                  AND effect.status = 'processing'
                  AND COALESCE(effect.lease_until, 0) <= ?
                  AND review_run.execution_status IN ('running', 'finished')
                ORDER BY review_run.run_seq
                """,
                (*binding_params, now),
            ).fetchall()
            for row in expired_review_runs:
                key = SessionKey(str(row["profile_id"]), str(row["session_id"]))
                generation = int(row["ownership_generation"])
                _require_effect_execution_binding(
                    self._database,
                    conn,
                    execution_binding,
                    key=key,
                    expected_generation=generation,
                )
                ownership = _require_effect_ownership(
                    self._database,
                    conn,
                    key,
                    expected_generation=generation,
                )
                handoff_request = _wake_request_for_ownership(ownership)
                reason = (
                    "review_execution_lease_expired_before_handler_terminal"
                    if str(row["execution_status"]) == "running"
                    else "review_execution_completed_without_effect_settlement"
                )
                conn.execute("SAVEPOINT expired_review_notice_candidate")
                try:
                    marked = mark_expired_review_execution_unknown(
                        conn,
                        key=key,
                        ownership_generation=generation,
                        review_effect_id=str(row["effect_id"]),
                        claim_id=str(row["claim_id"]),
                        worker_id=str(row["lease_owner"]),
                        now=now,
                        reason=reason,
                    )
                    if marked:
                        updated = conn.execute(
                            """
                            UPDATE agent_effect_outbox
                            SET last_error = ?, updated_at = ?
                            WHERE profile_id = ?
                              AND session_id = ?
                              AND effect_id = ?
                              AND ownership_generation = ?
                              AND status = 'processing'
                              AND claim_id = ?
                              AND lease_owner = ?
                              AND COALESCE(lease_until, 0) <= ?
                            """,
                            (
                                reason,
                                now,
                                key.profile_id,
                                key.session_id,
                                str(row["effect_id"]),
                                generation,
                                str(row["claim_id"]),
                                str(row["lease_owner"]),
                                now,
                            ),
                        )
                        if updated.rowcount != 1:
                            raise EffectStoreConflict(
                                "expired review effect changed before unknown evidence"
                            )
                        notice = ReviewExecutionUnknownNotice(
                            claim=ReviewExecutionClaim(
                                key=key,
                                ownership_generation=generation,
                                review_effect_id=str(row["effect_id"]),
                                review_operation_id=str(row["operation_id"]),
                                review_effect_kind=str(row["kind"]),
                                review_contract_version=int(row["contract_version"]),
                                review_contract_signature=str(row["contract_signature"]),
                                claim_id=str(row["claim_id"]),
                                worker_id=str(row["lease_owner"]),
                            ),
                            attempt_count=int(row["attempt_count"]),
                            unknown_at=now,
                            unknown_reason=reason,
                        )
                        envelope = SessionEventEnvelope(
                            event_id=notice.event_id,
                            key=key,
                            kind=REVIEW_EXECUTION_UNKNOWN_EVENT_KIND,
                            ownership_generation=generation,
                            payload=notice.to_payload(),
                            source=REVIEW_EXECUTION_UNKNOWN_EVENT_SOURCE,
                            occurred_at=now,
                            causation_id=str(row["event_id"]),
                            correlation_id=str(row["operation_id"]),
                            available_at=now,
                            created_at=now,
                        )
                        mailbox_id = self._insert_event(
                            conn,
                            envelope,
                            payload_json=_json_dumps(envelope.payload),
                            now=now,
                            handoff_request=handoff_request,
                        )
                        final_ownership = _final_effect_ownership(
                            self._database,
                            conn,
                            ownership,
                        )
                        recovery_notifications.append(
                            EffectSettlementResult(
                                status=EffectSettlementStatus.COMMITTED,
                                effect_id=str(row["effect_id"]),
                                event_id=envelope.event_id,
                                key=key,
                                wake_request=_wake_request_for_ownership(final_ownership),
                                mailbox_id=mailbox_id,
                            )
                        )
                except Exception:
                    conn.execute("ROLLBACK TO SAVEPOINT expired_review_notice_candidate")
                    conn.execute("RELEASE SAVEPOINT expired_review_notice_candidate")
                    raise
                conn.execute("RELEASE SAVEPOINT expired_review_notice_candidate")
            expired_model_runs = conn.execute(
                f"""
                SELECT effect.profile_id, effect.session_id,
                       effect.ownership_generation, effect.effect_id, effect.event_id,
                       effect.operation_id, effect.kind, effect.contract_version,
                       effect.contract_signature, effect.attempt_count,
                       effect.claim_id, effect.lease_owner,
                       model_run.execution_status
                FROM agent_effect_outbox AS effect
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = effect.profile_id
                 AND ownership.session_id = effect.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = effect.ownership_generation
                JOIN agent_model_execution_runs AS model_run
                  ON model_run.profile_id = effect.profile_id
                 AND model_run.session_id = effect.session_id
                 AND model_run.ownership_generation = effect.ownership_generation
                 AND model_run.effect_id = effect.effect_id
                 AND model_run.claim_id = effect.claim_id
                 AND model_run.worker_id = effect.lease_owner
                WHERE effect.ownership_generation >= 1
                  {binding_filter_sql}
                  AND effect.kind IN ({_GENERIC_MODEL_WORKFLOW_EFFECT_KINDS_SQL})
                  AND effect.status = 'processing'
                  AND COALESCE(effect.lease_until, 0) <= ?
                  AND model_run.execution_status IN ('running', 'finished')
                ORDER BY model_run.run_seq
                """,
                (*binding_params, now),
            ).fetchall()
            for row in expired_model_runs:
                key = SessionKey(str(row["profile_id"]), str(row["session_id"]))
                generation = int(row["ownership_generation"])
                _require_effect_execution_binding(
                    self._database,
                    conn,
                    execution_binding,
                    key=key,
                    expected_generation=generation,
                )
                ownership = _require_effect_ownership(
                    self._database,
                    conn,
                    key,
                    expected_generation=generation,
                )
                handoff_request = _wake_request_for_ownership(ownership)
                reason = (
                    "model_execution_lease_expired_before_handler_terminal"
                    if str(row["execution_status"]) == "running"
                    else "model_execution_completed_without_effect_settlement"
                )
                conn.execute("SAVEPOINT expired_model_notice_candidate")
                try:
                    marked = mark_expired_model_execution_unknown(
                        conn,
                        key=key,
                        ownership_generation=generation,
                        effect_id=str(row["effect_id"]),
                        claim_id=str(row["claim_id"]),
                        worker_id=str(row["lease_owner"]),
                        now=now,
                        reason=reason,
                    )
                    if marked:
                        updated = conn.execute(
                            """
                            UPDATE agent_effect_outbox
                            SET last_error = ?, updated_at = ?
                            WHERE profile_id = ?
                              AND session_id = ?
                              AND effect_id = ?
                              AND ownership_generation = ?
                              AND status = 'processing'
                              AND claim_id = ?
                              AND lease_owner = ?
                              AND COALESCE(lease_until, 0) <= ?
                            """,
                            (
                                reason,
                                now,
                                key.profile_id,
                                key.session_id,
                                str(row["effect_id"]),
                                generation,
                                str(row["claim_id"]),
                                str(row["lease_owner"]),
                                now,
                            ),
                        )
                        if updated.rowcount != 1:
                            raise EffectStoreConflict(
                                "expired model effect changed before unknown evidence"
                            )
                        notice = ModelExecutionUnknownNotice(
                            claim=ModelExecutionClaim(
                                key=key,
                                ownership_generation=generation,
                                effect_id=str(row["effect_id"]),
                                operation_id=str(row["operation_id"]),
                                effect_kind=str(row["kind"]),
                                contract_version=int(row["contract_version"]),
                                contract_signature=str(row["contract_signature"]),
                                claim_id=str(row["claim_id"]),
                                worker_id=str(row["lease_owner"]),
                            ),
                            attempt_count=int(row["attempt_count"]),
                            unknown_at=now,
                            unknown_reason=reason,
                        )
                        envelope = SessionEventEnvelope(
                            event_id=notice.event_id,
                            key=key,
                            kind=MODEL_EXECUTION_UNKNOWN_EVENT_KIND,
                            ownership_generation=generation,
                            payload=notice.to_payload(),
                            source=MODEL_EXECUTION_UNKNOWN_EVENT_SOURCE,
                            occurred_at=now,
                            causation_id=str(row["event_id"]),
                            correlation_id=str(row["operation_id"]),
                            available_at=now,
                            created_at=now,
                        )
                        mailbox_id = self._insert_event(
                            conn,
                            envelope,
                            payload_json=_json_dumps(envelope.payload),
                            now=now,
                            handoff_request=handoff_request,
                        )
                        final_ownership = _final_effect_ownership(
                            self._database,
                            conn,
                            ownership,
                        )
                        recovery_notifications.append(
                            EffectSettlementResult(
                                status=EffectSettlementStatus.COMMITTED,
                                effect_id=str(row["effect_id"]),
                                event_id=envelope.event_id,
                                key=key,
                                wake_request=_wake_request_for_ownership(final_ownership),
                                mailbox_id=mailbox_id,
                            )
                        )
                except Exception:
                    conn.execute("ROLLBACK TO SAVEPOINT expired_model_notice_candidate")
                    conn.execute("RELEASE SAVEPOINT expired_model_notice_candidate")
                    raise
                conn.execute("RELEASE SAVEPOINT expired_model_notice_candidate")
            rows = conn.execute(
                f"""
                SELECT effect.effect_seq, effect.profile_id, effect.session_id,
                       effect.ownership_generation
                FROM agent_effect_outbox AS effect
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = effect.profile_id
                 AND ownership.session_id = effect.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = effect.ownership_generation
                WHERE effect.ownership_generation >= 1
                  {binding_filter_sql}
                  AND effect.status = 'processing'
                  AND COALESCE(effect.lease_until, 0) <= ?
                  {_model_work_cancellation_gate_sql("effect")}
                ORDER BY effect.effect_seq
                """,
                (*binding_params, now),
            ).fetchall()
            recovered_count = 0
            for row in rows:
                key = SessionKey(str(row["profile_id"]), str(row["session_id"]))
                generation = int(row["ownership_generation"])
                _require_effect_execution_binding(
                    self._database,
                    conn,
                    execution_binding,
                    key=key,
                    expected_generation=generation,
                )
                _require_effect_ownership(
                    self._database,
                    conn,
                    key,
                    expected_generation=generation,
                )
                recovered = conn.execute(
                    """
                    UPDATE agent_effect_outbox
                    SET status = 'pending',
                        available_at = MIN(available_at, ?),
                        claim_id = '',
                        lease_owner = '',
                        lease_until = NULL,
                        updated_at = ?,
                        last_error = CASE
                            WHEN last_error = '' THEN 'effect_lease_recovered'
                            ELSE last_error
                        END
                    WHERE effect_seq = ?
                      AND ownership_generation = ?
                      AND status = 'processing'
                      AND COALESCE(lease_until, 0) <= ?
                    """,
                    (now, now, row["effect_seq"], generation, now),
                )
                recovered_count += int(recovered.rowcount)
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
            )
        return EffectExpiryRecoveryResult(
            recovered_count=recovered_count,
            notifications=tuple(recovery_notifications),
        )

    async def next_available_at(
        self,
        *,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> float | None:
        """Return the next pending availability or processing lease expiry."""

        filter_sql, filter_params, _priority_sql, _priority_params = _contract_filter_sql(
            effect_contracts,
            excluded_effect_contracts,
            table_alias="agent_effect_outbox",
        )
        action_order_sql, action_order_params = _external_action_order_gate_sql(
            "agent_effect_outbox"
        )
        binding_filter_sql, binding_params = _effect_execution_binding_filter(
            execution_binding,
            table_alias="agent_effect_outbox",
        )
        if effect_contracts == ():
            return None
        with self._database.connect() as conn:
            if execution_binding is not None:
                conn.execute("BEGIN IMMEDIATE")
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
            )
            row = conn.execute(
                f"""
                SELECT MIN(
                    CASE
                        WHEN agent_effect_outbox.status = 'pending'
                        THEN agent_effect_outbox.available_at
                        ELSE agent_effect_outbox.lease_until
                    END
                ) AS next_available_at
                FROM agent_effect_outbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = agent_effect_outbox.profile_id
                 AND ownership.session_id = agent_effect_outbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = agent_effect_outbox.ownership_generation
                WHERE agent_effect_outbox.ownership_generation >= 1
                  AND agent_effect_outbox.status IN ('pending', 'processing')
                {binding_filter_sql}
                {filter_sql}
                {action_order_sql}
                {_model_work_cancellation_gate_sql("agent_effect_outbox")}
                """,
                (*binding_params, *filter_params, *action_order_params),
            ).fetchone()
        if row is None or row["next_available_at"] is None:
            return None
        return float(row["next_available_at"])

    @staticmethod
    def _review_execution_claim(
        claim: ClaimedEffect,
    ) -> ReviewExecutionClaim | None:
        """Project one review effect lease into the durable witness identity."""

        effect = claim.effect
        if effect.kind != "run_review_workflow":
            return None
        try:
            return ReviewExecutionClaim(
                key=claim.key,
                ownership_generation=effect.ownership_generation,
                review_effect_id=effect.effect_id,
                review_operation_id=effect.operation_id,
                review_effect_kind=effect.kind,
                review_contract_version=effect.contract_version,
                review_contract_signature=effect.contract_signature,
                claim_id=claim.claim_id,
                worker_id=claim.worker_id,
            )
        except (TypeError, ValueError) as exc:
            raise EffectStoreConflict(
                "review effect claim cannot be projected into an execution fence"
            ) from exc

    @staticmethod
    def _model_execution_claim(
        claim: ClaimedEffect,
    ) -> ModelExecutionClaim | None:
        """Project an opted-in v3 model lease into its witness identity."""

        effect = claim.effect
        if not is_model_execution_cancellation_target(
            effect_kind=effect.kind,
            contract_version=effect.contract_version,
        ):
            return None
        try:
            return ModelExecutionClaim(
                key=claim.key,
                ownership_generation=effect.ownership_generation,
                effect_id=effect.effect_id,
                operation_id=effect.operation_id,
                effect_kind=effect.kind,
                contract_version=effect.contract_version,
                contract_signature=effect.contract_signature,
                claim_id=claim.claim_id,
                worker_id=claim.worker_id,
            )
        except (TypeError, ValueError) as exc:
            raise EffectStoreConflict(
                "model effect claim cannot be projected into a cancellation fence"
            ) from exc

    @classmethod
    def _cancel_review_claim_if_gated(
        cls,
        conn: sqlite3.Connection,
        *,
        claim: ClaimedEffect,
        now: float,
        evidence: str,
    ) -> EffectSettlementResult | None:
        """Apply review and generic-model cancellation fences before mutation."""

        review_claim = cls._review_execution_claim(claim)
        if review_claim is not None:
            try:
                cancellation_effect_id = cancel_claimed_review_effect_if_gated(
                    conn,
                    claim=review_claim,
                    now=now,
                    evidence=evidence,
                )
            except ReviewExecutionGateError as exc:
                raise EffectStoreConflict(
                    "review cancellation gate changed while mutating effect claim"
                ) from exc
            if cancellation_effect_id is not None:
                return EffectSettlementResult(
                    status=EffectSettlementStatus.CANCELLED,
                    effect_id=claim.effect.effect_id,
                    event_id="",
                    key=claim.key,
                )
        model_claim = cls._model_execution_claim(claim)
        if model_claim is None:
            return None
        try:
            cancellation_effect_id = cancel_claimed_model_execution_if_gated(
                conn,
                claim=model_claim,
                now=now,
                evidence=evidence,
            )
        except ModelExecutionCancellationBlocked as exc:
            raise EffectClaimLost(
                "model execution cancellation gate is blocked"
            ) from exc
        except ModelExecutionCancellationGateError as exc:
            raise EffectStoreConflict(
                "model execution cancellation gate changed while mutating effect claim"
            ) from exc
        if cancellation_effect_id is None:
            return None
        return EffectSettlementResult(
            status=EffectSettlementStatus.CANCELLED,
            effect_id=claim.effect.effect_id,
            event_id="",
            key=claim.key,
        )

    async def _release(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        available_at: float,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult | None:
        normalized_available_at = _nonnegative_finite(
            available_at,
            field_name="available_at",
        )
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.effect.ownership_generation
            )
            _require_effect_ownership(
                self._database,
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                expected_generation=ownership_generation,
            )
            now = _nonnegative_finite(self._clock(), field_name="clock")
            cancelled = self._cancel_review_claim_if_gated(
                conn,
                claim=claim,
                now=now,
                evidence="review_gate_claim_release",
            )
            if cancelled is not None:
                return cancelled
            released = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = 'pending',
                    available_at = ?,
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    updated_at = ?,
                    last_error = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                """,
                (
                    normalized_available_at,
                    now,
                    str(error or ""),
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.effect.effect_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if released.rowcount != 1:
                raise EffectClaimLost("effect is not owned by this claim")
        return None

    def _settle_with_event(
        self,
        claim: ClaimedEffect,
        envelope: SessionEventEnvelope,
        *,
        target_status: DurableEffectStatus,
        error: str,
        outcome_fence_fields: tuple[str, ...] | None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> EffectSettlementResult:
        if envelope.key != claim.key:
            raise ValueError("completion event key does not match effect ownership")
        contract, resolved_fence_fields = self._resolve_settlement_contract(
            claim.effect,
            caller_fence_fields=outcome_fence_fields,
        )
        self._validate_settlement_contract(claim.effect, envelope)
        if target_status not in {
            DurableEffectStatus.COMPLETED,
            DurableEffectStatus.FAILED,
        }:
            raise ValueError("effect settlement target must be terminal")
        if target_status is DurableEffectStatus.FAILED:
            self._validate_failure_envelope(
                claim,
                envelope,
                outcome_fence_fields=resolved_fence_fields,
            )
        else:
            self._validate_completion_envelope(
                claim,
                envelope,
                contract=contract,
                outcome_fence_fields=resolved_fence_fields,
            )
        payload_json = _json_dumps(envelope.payload)
        result: EffectSettlementResult
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.effect.ownership_generation
            )
            if envelope.ownership_generation != ownership_generation:
                raise EffectStoreConflict("effect settlement ownership_generation changed identity")
            ownership = _require_effect_ownership(
                self._database,
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            _require_effect_execution_binding(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                expected_generation=ownership_generation,
            )
            handoff_request = _wake_request_for_ownership(ownership)
            now = _nonnegative_finite(self._clock(), field_name="clock")
            conn.execute("SAVEPOINT effect_settlement_candidate")
            try:
                row = conn.execute(
                    """
                    SELECT * FROM agent_effect_outbox
                    WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                    """,
                    (
                        claim.key.profile_id,
                        claim.key.session_id,
                        claim.effect.effect_id,
                    ),
                ).fetchone()
                if row is None:
                    raise EffectClaimLost("durable effect no longer exists")
                self._validate_effect_identity(row, claim.effect)
                persisted_status = DurableEffectStatus(str(row["status"]))
                if persisted_status == target_status:
                    if str(row["claim_id"]) != claim.claim_id:
                        raise EffectClaimLost("a different claim already settled this effect")
                    existing_skip: tuple[str, int] | None = None
                    if target_status == DurableEffectStatus.COMPLETED:
                        existing_skip = self._load_existing_skip(
                            conn,
                            claim.effect,
                            envelope,
                            outcome_fence_fields=resolved_fence_fields,
                        )
                    if existing_skip is not None:
                        skipped_event_id, mailbox_id = existing_skip
                        final_ownership = _final_effect_ownership(
                            self._database,
                            conn,
                            ownership,
                        )
                        result = EffectSettlementResult(
                            status=EffectSettlementStatus.PRECONDITION_SKIPPED,
                            effect_id=claim.effect.effect_id,
                            event_id=skipped_event_id,
                            key=claim.key,
                            wake_request=_wake_request_for_ownership(final_ownership),
                            mailbox_id=mailbox_id,
                        )
                    else:
                        self._validate_persisted_event(conn, envelope, payload_json)
                        mailbox_id = self._mailbox_id_for_event(
                            conn,
                            key=claim.key,
                            event_id=envelope.event_id,
                        )
                        final_ownership = _final_effect_ownership(
                            self._database,
                            conn,
                            ownership,
                        )
                        result = EffectSettlementResult(
                            status=EffectSettlementStatus.ALREADY_COMMITTED,
                            effect_id=claim.effect.effect_id,
                            event_id=envelope.event_id,
                            key=claim.key,
                            wake_request=_wake_request_for_ownership(final_ownership),
                            mailbox_id=mailbox_id,
                        )
                else:
                    cancelled = self._cancel_review_claim_if_gated(
                        conn,
                        claim=claim,
                        now=now,
                        evidence="review_gate_settlement",
                    )
                    if cancelled is not None:
                        _final_effect_ownership(self._database, conn, ownership)
                        result = cancelled
                    else:
                        self._validate_live_claim(row, claim, now=now)
                        settled_envelope = envelope
                        settlement_status = EffectSettlementStatus.COMMITTED
                        actual_operation_status = None
                        if target_status == DurableEffectStatus.COMPLETED:
                            actual_operation_status = self._precondition_failure_status(
                                conn,
                                claim.effect,
                            )
                        if actual_operation_status is not None:
                            settled_envelope = self._skipped_envelope(
                                claim.effect,
                                intended=envelope,
                                actual_operation_status=actual_operation_status,
                                now=now,
                                outcome_fence_fields=resolved_fence_fields,
                            )
                            payload_json = _json_dumps(settled_envelope.payload)
                            settlement_status = EffectSettlementStatus.PRECONDITION_SKIPPED
                        mailbox_id = self._insert_event(
                            conn,
                            settled_envelope,
                            payload_json=payload_json,
                            now=now,
                            handoff_request=handoff_request,
                        )
                        settled = conn.execute(
                            """
                            UPDATE agent_effect_outbox
                            SET status = ?,
                                lease_owner = '',
                                lease_until = NULL,
                                updated_at = ?,
                                completed_at = ?,
                                last_error = ?
                            WHERE profile_id = ?
                              AND session_id = ?
                              AND effect_id = ?
                              AND ownership_generation = ?
                              AND status = 'processing'
                              AND claim_id = ?
                              AND lease_owner = ?
                              AND COALESCE(lease_until, 0) > ?
                            """,
                            (
                                target_status.value,
                                now,
                                now,
                                str(error or ""),
                                claim.key.profile_id,
                                claim.key.session_id,
                                claim.effect.effect_id,
                                ownership_generation,
                                claim.claim_id,
                                claim.worker_id,
                                now,
                            ),
                        )
                        if settled.rowcount != 1:
                            raise EffectClaimLost("effect claim changed during settlement")
                        final_ownership = _final_effect_ownership(
                            self._database,
                            conn,
                            ownership,
                        )
                        result = EffectSettlementResult(
                            status=settlement_status,
                            effect_id=claim.effect.effect_id,
                            event_id=settled_envelope.event_id,
                            key=claim.key,
                            wake_request=_wake_request_for_ownership(final_ownership),
                            mailbox_id=mailbox_id,
                        )
            except Exception:
                conn.execute("ROLLBACK TO SAVEPOINT effect_settlement_candidate")
                conn.execute("RELEASE SAVEPOINT effect_settlement_candidate")
                raise
            conn.execute("RELEASE SAVEPOINT effect_settlement_candidate")
        return result

    def _resolve_settlement_contract(
        self,
        effect: DurableEffectEnvelope,
        *,
        caller_fence_fields: tuple[str, ...] | None,
    ) -> tuple[EffectExecutionContract, tuple[str, ...]]:
        """Resolve exact settlement policy from the immutable contract authority."""

        try:
            contract = self._contract_authority.resolve(
                effect_kind=effect.kind,
                version=effect.contract_version,
                signature=effect.contract_signature,
            )
        except EffectContractAuthorityError as exc:
            raise EffectStoreConflict(
                "effect settlement contract is not authorized: " + str(exc)
            ) from exc
        declared_fields = contract.outcome_fence_fields
        if declared_fields is not None:
            missing_fields = tuple(
                field_name for field_name in declared_fields if field_name not in effect.payload
            )
            if missing_fields:
                raise EffectStoreConflict(
                    "explicit effect contract payload is missing outcome fences: "
                    + ", ".join(missing_fields)
                )
        resolved_fields = resolved_outcome_fence_fields(contract)
        if caller_fence_fields is None:
            return contract, resolved_fields
        if isinstance(caller_fence_fields, str) or not isinstance(
            caller_fence_fields,
            tuple,
        ):
            raise EffectStoreConflict(
                "settlement outcome_fence_fields must be a tuple when supplied"
            )
        if caller_fence_fields != resolved_fields:
            raise EffectStoreConflict(
                "settlement outcome_fence_fields differ from the sealed contract"
            )
        return contract, resolved_fields

    @staticmethod
    def _validate_completion_envelope(
        claim: ClaimedEffect,
        envelope: SessionEventEnvelope,
        *,
        contract: EffectExecutionContract,
        outcome_fence_fields: tuple[str, ...],
    ) -> None:
        """Reject forged successful outcomes before changing the durable outbox."""

        effect = claim.effect
        if envelope.kind != contract.completion_event_kind:
            raise EffectStoreConflict("effect completion changed event kind")
        if envelope.event_id != completion_event_id(effect):
            raise EffectStoreConflict("effect completion changed event id")
        if envelope.source != contract.completion_source:
            raise EffectStoreConflict("effect completion changed source")
        if envelope.causation_id != effect.source_event_id:
            raise EffectStoreConflict("effect completion changed causation id")
        if envelope.correlation_id != (effect.operation_id or effect.effect_id):
            raise EffectStoreConflict("effect completion changed correlation id")
        if envelope.trace_id != effect.trace_id:
            raise EffectStoreConflict("effect completion changed trace id")
        if envelope.ownership_generation != effect.ownership_generation:
            raise EffectStoreConflict("effect completion changed ownership generation")

        expected_identity: dict[str, object] = {
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "idempotency_key": effect.idempotency_key,
            "operation_id": effect.operation_id,
            "contract_version": effect.contract_version,
            "contract_signature": effect.contract_signature,
        }
        for field_name, expected in expected_identity.items():
            if field_name not in envelope.payload or envelope.payload[field_name] != expected:
                raise EffectStoreConflict("effect completion changed " + field_name)
        if envelope.payload.get("attempt_count") != claim.attempt_count:
            raise EffectStoreConflict("effect completion changed attempt count")
        if (
            isinstance(envelope.payload.get("attempt_count"), bool)
            or not isinstance(envelope.payload.get("attempt_count"), int)
            or int(envelope.payload["attempt_count"]) < 1
        ):
            raise EffectStoreConflict("effect completion has an invalid attempt")

        SQLiteDurableEffectStore._validate_completion_fences(
            effect,
            envelope,
            outcome_fence_fields=outcome_fence_fields,
        )

    @staticmethod
    def _validate_completion_fences(
        effect: DurableEffectEnvelope,
        envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...],
    ) -> None:
        """Require a successful outcome to retain every persisted fence value."""

        for field_name, expected in effect.outcome_fence_payload(outcome_fence_fields).items():
            if field_name not in envelope.payload or envelope.payload[field_name] != expected:
                raise EffectStoreConflict("effect completion changed fence " + field_name)

    @staticmethod
    def _precondition_failure_status(
        conn: sqlite3.Connection,
        effect: DurableEffectEnvelope,
    ) -> str | None:
        raw_expected = effect.payload.get("enqueue_only_if_operation_status")
        if raw_expected is None:
            return None
        if not isinstance(raw_expected, list) or not raw_expected:
            raise EffectStoreConflict("enqueue_only_if_operation_status must be a non-empty list")
        expected = {str(value or "").strip() for value in raw_expected}
        if "" in expected:
            raise EffectStoreConflict("enqueue_only_if_operation_status contains an empty status")
        disposition = str(effect.payload.get("terminal_operation_disposition") or "").strip()
        if disposition != "skip":
            raise EffectStoreConflict(
                "operation-status effect preconditions require skip disposition"
            )
        if not effect.operation_id:
            raise EffectStoreConflict("operation-status effect preconditions require operation_id")
        row = conn.execute(
            """
            SELECT status FROM agent_session_operations
            WHERE operation_id = ? AND profile_id = ? AND session_id = ?
            """,
            (
                effect.operation_id,
                effect.key.profile_id,
                effect.key.session_id,
            ),
        ).fetchone()
        actual = str(row["status"]) if row is not None else "missing"
        return None if actual in expected else actual

    @staticmethod
    def _skipped_envelope(
        effect: DurableEffectEnvelope,
        *,
        intended: SessionEventEnvelope,
        actual_operation_status: str,
        now: float,
        outcome_fence_fields: tuple[str, ...],
    ) -> SessionEventEnvelope:
        raw_expected = effect.payload["enqueue_only_if_operation_status"]
        assert isinstance(raw_expected, list)
        return SessionEventEnvelope(
            event_id=skipped_event_id(effect),
            key=effect.key,
            kind="EffectSkipped",
            ownership_generation=effect.ownership_generation,
            payload={
                **effect.outcome_fence_payload(outcome_fence_fields),
                "effect_id": effect.effect_id,
                "effect_kind": effect.kind,
                "contract_version": effect.contract_version,
                "contract_signature": effect.contract_signature,
                "operation_id": effect.operation_id,
                "idempotency_key": effect.idempotency_key,
                "reason": "operation_status_precondition_not_met",
                "expected_operation_statuses": [str(value) for value in raw_expected],
                "actual_operation_status": actual_operation_status,
                "intended_event_id": intended.event_id,
                "intended_event_kind": intended.kind,
            },
            source="effect_executor",
            occurred_at=now,
            causation_id=effect.source_event_id,
            correlation_id=effect.operation_id or effect.effect_id,
            trace_id=effect.trace_id,
            available_at=now,
            created_at=now,
        )

    @staticmethod
    def _load_existing_skip(
        conn: sqlite3.Connection,
        effect: DurableEffectEnvelope,
        intended: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...],
    ) -> tuple[str, int] | None:
        event_id = skipped_event_id(effect)
        row = conn.execute(
            """
            SELECT mailbox_id, kind, ownership_generation, payload_json
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (effect.key.profile_id, effect.key.session_id, event_id),
        ).fetchone()
        if row is None:
            return None
        if str(row["kind"]) != "EffectSkipped":
            raise EffectStoreConflict("deterministic skipped event id changed identity")
        if int(row["ownership_generation"]) != effect.ownership_generation:
            raise EffectStoreConflict(
                "deterministic skipped event ownership generation changed identity"
            )
        payload = _json_object(str(row["payload_json"]))
        expected_identity = {
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "contract_version": effect.contract_version,
            "contract_signature": effect.contract_signature,
            "operation_id": effect.operation_id,
            "idempotency_key": effect.idempotency_key,
            "intended_event_id": intended.event_id,
            "intended_event_kind": intended.kind,
        }
        if any(payload.get(key) != value for key, value in expected_identity.items()):
            raise EffectStoreConflict("persisted skipped event changed identity")
        for field_name, expected in effect.outcome_fence_payload(outcome_fence_fields).items():
            if payload.get(field_name) != expected:
                raise EffectStoreConflict("persisted skipped event changed fence " + field_name)
        return event_id, int(row["mailbox_id"])

    @staticmethod
    def _validate_effect_identity(
        row: sqlite3.Row,
        effect: DurableEffectEnvelope,
    ) -> None:
        persisted = (
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["ownership_generation"]),
            str(row["kind"]),
            int(row["contract_version"]),
            str(row["contract_signature"]),
            str(row["idempotency_key"]),
            str(row["event_id"]),
            str(row["operation_id"]),
            str(row["payload_json"]),
        )
        requested = (
            effect.key.profile_id,
            effect.key.session_id,
            effect.ownership_generation,
            effect.kind,
            effect.contract_version,
            effect.contract_signature,
            effect.idempotency_key,
            effect.source_event_id,
            effect.operation_id,
            _json_dumps(effect.payload),
        )
        if persisted != requested:
            raise EffectStoreConflict(
                f"effect id {effect.effect_id!r} is already used by different work"
            )

    @staticmethod
    def _validate_settlement_contract(
        effect: DurableEffectEnvelope,
        envelope: SessionEventEnvelope,
    ) -> None:
        if envelope.payload.get("contract_version") != effect.contract_version:
            raise EffectStoreConflict("effect settlement contract_version changed identity")
        if envelope.payload.get("contract_signature") != effect.contract_signature:
            raise EffectStoreConflict("effect settlement contract_signature changed identity")

    @staticmethod
    def _validate_failure_envelope(
        claim: ClaimedEffect,
        envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...],
    ) -> None:
        """Reject forged terminal failures before changing the durable outbox."""

        effect = claim.effect
        if envelope.kind != "EffectFailed":
            raise EffectStoreConflict("terminal effect failure changed event kind")
        if envelope.event_id != failure_event_id(effect):
            raise EffectStoreConflict("terminal effect failure changed event id")
        if envelope.source != "effect_executor":
            raise EffectStoreConflict("terminal effect failure changed source")
        if envelope.causation_id != effect.source_event_id:
            raise EffectStoreConflict("terminal effect failure changed causation id")
        if envelope.correlation_id != (effect.operation_id or effect.effect_id):
            raise EffectStoreConflict("terminal effect failure changed correlation id")
        if envelope.trace_id != effect.trace_id:
            raise EffectStoreConflict("terminal effect failure changed trace id")
        if envelope.ownership_generation != effect.ownership_generation:
            raise EffectStoreConflict("terminal effect failure changed ownership generation")

        expected_identity = {
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "idempotency_key": effect.idempotency_key,
            "operation_id": effect.operation_id,
            "contract_signature": effect.contract_signature,
        }
        for field_name, expected in expected_identity.items():
            if envelope.payload.get(field_name) != expected:
                raise EffectStoreConflict("terminal effect failure changed " + field_name)
        if envelope.payload.get("contract_version") != effect.contract_version:
            raise EffectStoreConflict("terminal effect failure changed contract_version")
        if envelope.payload.get("attempt_count") != claim.attempt_count:
            raise EffectStoreConflict("terminal effect failure changed attempt count")
        if (
            isinstance(envelope.payload.get("attempt_count"), bool)
            or not isinstance(envelope.payload.get("attempt_count"), int)
            or int(envelope.payload["attempt_count"]) < 1
        ):
            raise EffectStoreConflict("terminal effect failure has an invalid attempt")

        for field_name, expected in effect.outcome_fence_payload(outcome_fence_fields).items():
            if field_name not in envelope.payload or envelope.payload[field_name] != expected:
                raise EffectStoreConflict("terminal effect failure changed fence " + field_name)
        for field_name in ("action_ordinal", "request_digest"):
            if (
                field_name in effect.payload
                and envelope.payload.get(field_name) != effect.payload[field_name]
            ):
                raise EffectStoreConflict("terminal effect failure changed action " + field_name)
        failure_code = envelope.payload.get("failure_code")
        if not isinstance(failure_code, str) or not failure_code.strip():
            raise EffectStoreConflict("terminal effect failure has no failure code")
        if not isinstance(envelope.payload.get("failure_message"), str):
            raise EffectStoreConflict("terminal effect failure has an invalid message")

    @staticmethod
    def _validate_live_claim(
        row: sqlite3.Row,
        claim: ClaimedEffect,
        *,
        now: float,
    ) -> None:
        if (
            str(row["status"]) != DurableEffectStatus.PROCESSING.value
            or str(row["claim_id"]) != claim.claim_id
            or str(row["lease_owner"]) != claim.worker_id
            or int(row["attempt_count"]) != claim.attempt_count
            or float(row["lease_until"] or 0.0) <= now
        ):
            raise EffectClaimLost("effect lease is expired or no longer owned")

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        envelope: SessionEventEnvelope,
        *,
        payload_json: str,
        now: float,
        handoff_request: FencedMailboxWakeRequest,
    ) -> int:
        """Insert one mailbox event and immutable handoff evidence atomically.

        Sidecar evidence is written only for a newly created mailbox row. A
        duplicate event can be historical work whose sidecar is missing or
        blocked/unknown, and must never be promoted from a current owner read.
        """

        if not isinstance(handoff_request, FencedMailboxWakeRequest):
            raise TypeError("handoff_request must be a FencedMailboxWakeRequest")
        inserted = conn.execute(
            """
            INSERT OR IGNORE INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at,
                payload_json, causation_id, correlation_id, trace_id,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, NULL, '')
            """,
            (
                envelope.event_id,
                envelope.key.profile_id,
                envelope.key.session_id,
                _persistable_ownership_generation(envelope.ownership_generation),
                envelope.kind,
                envelope.source,
                envelope.occurred_at or now,
                payload_json,
                envelope.causation_id,
                envelope.correlation_id,
                envelope.trace_id,
                envelope.available_at or now,
                envelope.created_at or now,
            ),
        )
        mailbox_id = self._mailbox_id_for_event(
            conn,
            key=envelope.key,
            event_id=envelope.event_id,
        )
        if inserted.rowcount != 1:
            self._validate_persisted_event(conn, envelope, payload_json)
            return mailbox_id
        if handoff_request.has_admission_fence:
            try:
                self._database.actor_v2_mailbox_handoffs.record_fenced_handoff_in_transaction(
                    conn,
                    mailbox_id,
                    handoff_request,
                )
            except (ActorV2AdmissionFenceError, AgentRuntimeOwnershipError) as exc:
                raise EffectClaimLost(
                    "effect ownership generation is no longer active"
                ) from exc
        else:
            self._database.actor_v2_mailbox_handoffs.record_unfenced_legacy_handoff_in_transaction(
                conn,
                mailbox_id,
            )
        return mailbox_id

    @staticmethod
    def _mailbox_id_for_event(
        conn: sqlite3.Connection,
        *,
        key: SessionKey,
        event_id: str,
    ) -> int:
        """Read the immutable id for one already persisted mailbox event."""

        row = conn.execute(
            """
            SELECT mailbox_id
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, event_id),
        ).fetchone()
        if row is None:
            raise EffectStoreConflict("settled effect is missing its mailbox event")
        mailbox_id = int(row["mailbox_id"])
        if mailbox_id < 1:
            raise EffectStoreConflict("settled effect has an invalid mailbox id")
        return mailbox_id

    @staticmethod
    def _validate_persisted_event(
        conn: sqlite3.Connection,
        envelope: SessionEventEnvelope,
        payload_json: str,
    ) -> None:
        row = conn.execute(
            """
            SELECT kind, source, ownership_generation, payload_json,
                   causation_id, correlation_id, trace_id
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (
                envelope.key.profile_id,
                envelope.key.session_id,
                envelope.event_id,
            ),
        ).fetchone()
        if row is None:
            raise EffectStoreConflict("settled effect is missing its mailbox event")
        persisted = (
            str(row["kind"]),
            str(row["source"]),
            int(row["ownership_generation"]),
            str(row["payload_json"]),
            str(row["causation_id"]),
            str(row["correlation_id"]),
            str(row["trace_id"]),
        )
        requested = (
            envelope.kind,
            envelope.source,
            envelope.ownership_generation,
            payload_json,
            envelope.causation_id,
            envelope.correlation_id,
            envelope.trace_id,
        )
        if persisted != requested:
            raise EffectStoreConflict(
                f"mailbox event id {envelope.event_id!r} is already used by different work"
            )


def _effect_from_row(row: dict[str, object]) -> DurableEffectEnvelope:
    payload = _json_object(str(row["payload_json"] or "{}"))
    return DurableEffectEnvelope(
        effect_id=str(row["effect_id"]),
        key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
        kind=str(row["kind"]),
        idempotency_key=str(row["idempotency_key"]),
        ownership_generation=int(row["ownership_generation"]),
        contract_version=int(row["contract_version"]),
        contract_signature=str(row["contract_signature"]),
        payload=payload,
        source_event_id=str(row["event_id"]),
        operation_id=str(row["operation_id"]),
        trace_id=str(row["source_trace_id"]),
        available_at=float(row["available_at"]),
        created_at=float(row["created_at"]),
    )


def _json_object(value: str) -> dict[str, Any]:
    validation = validate_canonical_json_object(value)
    if validation.payload is None or validation.violations:
        reason = ", ".join(validation.violations) or "payload_json_invalid"
        raise EffectStoreConflict(f"durable effect payload is invalid: {reason}")
    return validation.payload


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _raw_effect_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    effect_seq: int,
) -> tuple[
    dict[str, RawSQLiteValue],
    dict[str, object],
    tuple[str, ...],
]:
    raw_values = raw_sqlite_values(row, _EFFECT_ROW_EVIDENCE_FIELDS)
    raw_values["source_trace_id"] = raw_sqlite_values(
        row,
        ("trace_id",),
        output_prefix="source_",
    )["trace_id"]
    for field_name, raw_value in tuple(raw_values.items()):
        if not raw_value.projection_truncated:
            continue
        expression = (
            "source.trace_id" if field_name == "source_trace_id" else f"effect.{field_name}"
        )
        raw_values[field_name] = complete_truncated_raw_sqlite_value(
            raw_value,
            chunk_reader=lambda offset, length, expression=expression: _read_effect_raw_chunk(
                conn,
                effect_seq=effect_seq,
                expression=expression,
                offset=offset,
                length=length,
            ),
        )
    decoded, violations = decode_raw_sqlite_values(raw_values)
    if decoded["source_trace_id"] is None:
        decoded["source_trace_id"] = ""
    active_generation = row["active_ownership_generation"]
    if not isinstance(active_generation, int) or isinstance(active_generation, bool):
        violations = (*violations, "active_ownership_generation_invalid")
    decoded["active_ownership_generation"] = active_generation
    return raw_values, decoded, violations


def _read_effect_raw_chunk(
    conn: sqlite3.Connection,
    *,
    effect_seq: int,
    expression: str,
    offset: int,
    length: int,
) -> object:
    row = conn.execute(
        f"""
        SELECT substr(CAST({expression} AS BLOB), ?, ?) AS raw_chunk
        FROM agent_effect_outbox AS effect
        LEFT JOIN agent_session_mailbox AS source
          ON source.profile_id = effect.profile_id
         AND source.session_id = effect.session_id
         AND source.event_id = effect.event_id
        WHERE effect.effect_seq = ?
        """,
        (offset, length, effect_seq),
    ).fetchone()
    return None if row is None else row["raw_chunk"]


def _effect_row_materialization_plan(row: sqlite3.Row) -> tuple[int, bool]:
    raw_values = raw_sqlite_values(row, _EFFECT_ROW_EVIDENCE_FIELDS)
    raw_values["source_trace_id"] = raw_sqlite_values(
        row,
        ("trace_id",),
        output_prefix="source_",
    )["trace_id"]
    total = 0
    has_oversized_field = False
    for field_name, raw_value in raw_values.items():
        byte_limit = (
            _EFFECT_METADATA_FIELD_BYTE_LIMIT
            if field_name == "source_trace_id"
            else _EFFECT_RAW_BYTE_LIMITS[field_name]
        )
        if raw_value.logical_byte_length > byte_limit:
            has_oversized_field = True
        total += raw_value.logical_byte_length
    return total, has_oversized_field


def _scrub_header_is_claimable(row: sqlite3.Row, *, now: float) -> bool:
    raw_values = raw_sqlite_values(
        row,
        _EFFECT_SCRUB_HEADER_FIELDS,
        output_prefix="scrub_",
    )
    decoded, _violations = decode_raw_sqlite_values(raw_values)
    active_generation = row["active_ownership_generation"]
    if not _is_integer_at_least(active_generation, 1):
        return False
    ownership_generation = decoded["ownership_generation"]
    if isinstance(ownership_generation, int) and not isinstance(
        ownership_generation,
        bool,
    ):
        if ownership_generation != active_generation:
            return False
    status = decoded["status"]
    if status == "pending":
        available_at = decoded["available_at"]
        return not _is_nonnegative_finite_number(available_at) or float(available_at) <= now
    if status == "processing":
        lease_until = decoded["lease_until"]
        return (
            lease_until is None
            or not _is_nonnegative_finite_number(lease_until)
            or float(lease_until) <= now
        )
    return False


def _effect_row_violations(row: dict[str, object]) -> tuple[str, ...]:
    violations: list[str] = []
    for field_name in (
        "effect_id",
        "idempotency_key",
        "profile_id",
        "session_id",
        "event_id",
        "kind",
        "contract_signature",
    ):
        if not _is_canonical_nonempty_text(row[field_name]):
            violations.append(f"{field_name}_invalid")
    for field_name in ("operation_id", "claim_id", "lease_owner"):
        if not _is_canonical_text(row[field_name]):
            violations.append(f"{field_name}_invalid")
    if not isinstance(row["last_error"], str):
        violations.append("last_error_not_text")
    if not isinstance(row["status"], str) or row["status"] not in {
        "pending",
        "processing",
        "completed",
        "failed",
        "cancelled",
    }:
        violations.append("status_invalid")
    for field_name, minimum in (
        ("effect_seq", 1),
        ("ownership_generation", 1),
        ("contract_version", 1),
        ("attempt_count", 0),
    ):
        if not _is_integer_at_least(row[field_name], minimum):
            violations.append(f"{field_name}_not_integer")
    if _is_integer_at_least(row["attempt_count"], _SQLITE_INT64_MAX) and row["status"] in {
        "pending",
        "processing",
    }:
        violations.append("attempt_count_not_claimable")
    for field_name in ("available_at", "created_at", "updated_at"):
        if not _is_nonnegative_finite_number(row[field_name]):
            violations.append(f"{field_name}_invalid")
    for field_name in ("lease_until", "completed_at"):
        value = row[field_name]
        if value is not None and not _is_nonnegative_finite_number(value):
            violations.append(f"{field_name}_invalid")
    if not isinstance(row["source_trace_id"], str):
        violations.append("source_trace_id_invalid")
    payload_json = row["payload_json"]
    if not isinstance(payload_json, str):
        violations.append("payload_json_not_text")
    else:
        violations.extend(validate_canonical_json_object(payload_json).violations)
    return tuple(violations)


def _claim_row_violations(row: dict[str, object]) -> tuple[str, ...]:
    structural = _effect_row_violations(row)
    if structural:
        return structural
    if not _is_trusted_external_action_contract(row):
        return ()
    return _external_action_effect_violations(row)


def _is_trusted_external_action_contract(row: dict[str, object]) -> bool:
    identity = (row["kind"], row["contract_version"])
    expected_signature = _EXTERNAL_ACTION_CONTRACT_SIGNATURES.get(identity)
    return expected_signature is not None and row["contract_signature"] == (expected_signature)


def _external_action_effect_violations(row: dict[str, object]) -> tuple[str, ...]:
    validation = validate_canonical_json_object(str(row["payload_json"]))
    if validation.payload is None:
        return validation.violations or ("external_action_payload_invalid",)
    payload = validation.payload
    violations: list[str] = []
    operation_id = row["operation_id"]
    if not _is_canonical_nonempty_text(operation_id):
        violations.append("external_action_operation_id_invalid")
    elif payload.get("operation_id") != operation_id:
        violations.append("external_action_operation_id_mismatch")
    if payload.get("source_event_id") != row["event_id"]:
        violations.append("external_action_source_event_id_mismatch")

    action_ordinal = payload.get("action_ordinal")
    if (
        not isinstance(action_ordinal, int)
        or isinstance(action_ordinal, bool)
        or action_ordinal < 0
        or action_ordinal > _SQLITE_INT64_MAX
    ):
        violations.append("external_action_action_ordinal_invalid")
    request_digest = payload.get("request_digest")
    if (
        not isinstance(request_digest, str)
        or len(request_digest) != 64
        or any(character not in "0123456789abcdef" for character in request_digest)
    ):
        violations.append("external_action_request_digest_invalid")
    for field_name in (
        "instance_id",
        "target_session_id",
        "tool_call_id",
    ):
        if not _is_canonical_nonempty_text(payload.get(field_name)):
            violations.append(f"external_action_{field_name}_invalid")
    action_payload = payload.get("payload")
    if not isinstance(action_payload, dict):
        violations.append("external_action_payload_invalid")
    if violations:
        return tuple(violations)

    try:
        kind = ExternalActionKind(str(row["kind"]))
        intent = ExternalActionIntent(
            kind=kind,
            tool_call_id=str(payload["tool_call_id"]),
            action_ordinal=int(action_ordinal),
            payload=dict(action_payload),
        )
        request = ExternalActionRequest(
            key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
            ownership_generation=int(row["ownership_generation"]),
            operation_id=str(operation_id),
            source_event_id=str(row["event_id"]),
            instance_id=str(payload["instance_id"]),
            target_session_id=str(payload["target_session_id"]),
            intent=intent,
            contract_version=int(row["contract_version"]),
        )
    except (KeyError, TypeError, ValueError):
        return ("external_action_request_invalid",)
    if request.effect_id != row["effect_id"]:
        violations.append("external_action_effect_id_mismatch")
    if request.idempotency_key != row["idempotency_key"]:
        violations.append("external_action_idempotency_key_mismatch")
    if request.request_digest != request_digest:
        violations.append("external_action_request_digest_mismatch")
    return tuple(violations)


def _is_canonical_nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _is_canonical_text(value: object) -> bool:
    return isinstance(value, str) and value == value.strip()


def _is_integer_at_least(value: object, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _is_nonnegative_finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0
    )


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be finite and non-negative") from exc
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _persistable_ownership_generation(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("ownership_generation must be at least one")
    try:
        generation = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("ownership_generation must be at least one") from exc
    if generation < 1:
        raise ValueError("ownership_generation must be at least one")
    return generation


def _effect_execution_binding_filter(
    execution_binding: FencedActorExecutionBinding | None,
    *,
    table_alias: str,
) -> tuple[str, tuple[object, ...]]:
    """Build the exact one-session predicate for a scoped target executor."""

    if execution_binding is None:
        return "", ()
    if not isinstance(execution_binding, FencedActorExecutionBinding):
        raise TypeError("execution_binding must be a FencedActorExecutionBinding")
    request = execution_binding.request
    return (
        f"""
        AND {table_alias}.profile_id = ?
        AND {table_alias}.session_id = ?
        AND {table_alias}.ownership_generation = ?
        """,
        (
            request.key.profile_id,
            request.key.session_id,
            request.ownership_generation,
        ),
    )


def _require_effect_execution_binding(
    database: DatabaseManager,
    conn: sqlite3.Connection,
    execution_binding: FencedActorExecutionBinding | None,
    *,
    key: SessionKey | None = None,
    expected_generation: int | None = None,
) -> None:
    """Require a live target lease for one exact effect owner in this transaction."""

    if execution_binding is None:
        return
    if not isinstance(execution_binding, FencedActorExecutionBinding):
        raise TypeError("execution_binding must be a FencedActorExecutionBinding")
    try:
        require_live_execution_binding_in_transaction(
            database,
            conn,
            execution_binding,
            key=key,
            ownership_generation=expected_generation,
        )
    except FencedWakeTargetLeaseError as exc:
        raise FencedEffectExecutionLeaseLost(
            "fenced effect executor lost its target lease authority"
        ) from exc


def _require_effect_ownership(
    database: DatabaseManager,
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    expected_generation: int,
    expected_admission_fence_id: str | None = None,
    expected_admission_fence_generation: int | None = None,
) -> AgentRuntimeOwnership:
    """Require one current Actor v2 owner without leaking fence internals."""

    try:
        return database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            key,
            expected_generation=expected_generation,
            expected_admission_fence_id=expected_admission_fence_id,
            expected_admission_fence_generation=expected_admission_fence_generation,
        )
    except (ActorV2AdmissionFenceError, AgentRuntimeOwnershipError) as exc:
        raise EffectClaimLost("effect ownership generation is no longer active") from exc


def _final_effect_ownership(
    database: DatabaseManager,
    conn: sqlite3.Connection,
    ownership: AgentRuntimeOwnership,
) -> AgentRuntimeOwnership:
    """Revalidate an exact owner and admission fence before committing writes."""

    return _require_effect_ownership(
        database,
        conn,
        ownership.key,
        expected_generation=ownership.generation,
        expected_admission_fence_id=ownership.admission_fence_id,
        expected_admission_fence_generation=ownership.admission_fence_generation,
    )


def _wake_request_for_ownership(
    ownership: AgentRuntimeOwnership,
) -> FencedMailboxWakeRequest:
    """Project a final validated Actor v2 owner into one typed wake request."""

    if not ownership.actor_v2_active:
        raise EffectClaimLost("effect ownership generation is no longer active")
    return FencedMailboxWakeRequest(
        key=ownership.key,
        ownership_generation=ownership.generation,
        admission_fence_id=ownership.admission_fence_id,
        admission_fence_generation=ownership.admission_fence_generation,
    )


def _external_action_order_gate_sql(
    effect_alias: str,
) -> tuple[str, tuple[object, ...]]:
    """Build the durable claim predicate for ordered external-action effects.

    Action effects are intentionally filtered before a worker receives an
    outbox lease. The immediate predecessor must already have a succeeded
    receipt. This makes actions from separate operations independently
    claimable while preventing a follower from reaching adapter code early.
    Invalid legacy or tampered action payloads evaluate to false and remain
    pending for diagnosis instead of being treated as ordinal zero.
    """

    kinds = ", ".join("?" for _kind in _EXTERNAL_ACTION_EFFECT_KINDS)
    trusted_contract_sql, trusted_contract_params = _trusted_external_action_contract_sql(
        effect_alias
    )
    payload_json = f"{effect_alias}.payload_json"
    payload_is_bounded_json = f"""
        CASE
            WHEN typeof({payload_json}) != 'text' THEN 0
            WHEN length(CAST({payload_json} AS BLOB))
                 > {MAX_CANONICAL_JSON_BYTES} THEN 0
            WHEN json_valid({payload_json}) THEN 1
            ELSE 0
        END
    """
    ordinal = f"""
        CASE
            WHEN ({payload_is_bounded_json}) = 1 THEN CASE
                WHEN json_type({payload_json}, '$.action_ordinal') = 'integer'
                THEN CAST(json_extract({payload_json}, '$.action_ordinal') AS INTEGER)
                ELSE -1
            END
            ELSE -1
        END
    """
    operation_matches = f"""
        CASE
            WHEN ({payload_is_bounded_json}) = 1 THEN CASE
                WHEN json_type({payload_json}, '$.operation_id') = 'text'
                 AND json_extract({payload_json}, '$.operation_id')
                     = {effect_alias}.operation_id
                THEN 1
                ELSE 0
            END
            ELSE 0
        END
    """
    payload_is_ordered_action = f"""
        {effect_alias}.operation_id != ''
        AND ({operation_matches}) = 1
        AND ({ordinal}) >= 0
    """
    sql = f"""
        AND (
            {effect_alias}.kind NOT IN ({kinds})
            OR NOT ({trusted_contract_sql})
            OR (
                ({payload_is_ordered_action}) = 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM agent_external_action_receipts AS occupied_slot
                    WHERE occupied_slot.profile_id = {effect_alias}.profile_id
                      AND occupied_slot.session_id = {effect_alias}.session_id
                      AND occupied_slot.ownership_generation
                          = {effect_alias}.ownership_generation
                      AND occupied_slot.operation_id = {effect_alias}.operation_id
                      AND occupied_slot.action_ordinal = {ordinal}
                      AND occupied_slot.idempotency_key != {effect_alias}.idempotency_key
                )
                AND (
                    {ordinal} = 0
                    OR EXISTS (
                        SELECT 1
                        FROM agent_external_action_receipts AS predecessor
                        WHERE predecessor.profile_id = {effect_alias}.profile_id
                          AND predecessor.session_id = {effect_alias}.session_id
                          AND predecessor.ownership_generation
                              = {effect_alias}.ownership_generation
                          AND predecessor.operation_id = {effect_alias}.operation_id
                          AND predecessor.action_ordinal = {ordinal} - 1
                          AND predecessor.status = 'succeeded'
                    )
                )
            )
        )
    """
    return sql, (
        *_EXTERNAL_ACTION_EFFECT_KINDS,
        *trusted_contract_params,
    )


def _review_cancellation_gate_sql(effect_alias: str) -> str:
    """Return the durable claim/recovery fence for model execution.

    The gate is written in the same actor transaction that supersedes the
    review.  Every claim path rechecks the full immutable review effect
    identity so a pending or recovered target cannot start after that commit.
    A durable running witness independently blocks lease recovery and any
    second worker. An unknown review witness additionally blocks every model
    workflow for that session until explicit reconciliation exists. Generic
    model witnesses apply the same no-replay rule to active reply, active chat,
    and idle planning. A missing local task or expired lease is not a
    distributed proof that a prior model task has stopped.
    """

    return f"""
        AND NOT EXISTS (
            SELECT 1
            FROM agent_review_cancellation_gates AS review_gate
            WHERE review_gate.profile_id = {effect_alias}.profile_id
              AND review_gate.session_id = {effect_alias}.session_id
              AND review_gate.ownership_generation
                  = {effect_alias}.ownership_generation
              AND review_gate.review_effect_id = {effect_alias}.effect_id
              AND review_gate.review_operation_id = {effect_alias}.operation_id
              AND review_gate.review_effect_kind = {effect_alias}.kind
              AND review_gate.review_contract_version
                  = {effect_alias}.contract_version
              AND review_gate.review_contract_signature
                  = {effect_alias}.contract_signature
        )
        AND (
            {effect_alias}.kind != 'run_review_workflow'
            OR NOT EXISTS (
                SELECT 1
                FROM agent_review_execution_runs AS review_run
                WHERE review_run.profile_id = {effect_alias}.profile_id
                  AND review_run.session_id = {effect_alias}.session_id
                  AND review_run.ownership_generation
                      = {effect_alias}.ownership_generation
                  AND review_run.review_effect_id = {effect_alias}.effect_id
                  AND review_run.execution_status IN ('running', 'finished', 'unknown')
            )
        )
        AND (
            {effect_alias}.kind NOT IN ({_MODEL_WORKFLOW_EFFECT_KINDS_SQL})
            OR NOT EXISTS (
                SELECT 1
                FROM agent_review_execution_runs AS unknown_review_run
                WHERE unknown_review_run.profile_id = {effect_alias}.profile_id
                  AND unknown_review_run.session_id = {effect_alias}.session_id
                  AND unknown_review_run.ownership_generation
                      = {effect_alias}.ownership_generation
                  AND unknown_review_run.execution_status = 'unknown'
            )
        )
        AND (
            {effect_alias}.kind NOT IN ({_GENERIC_MODEL_WORKFLOW_EFFECT_KINDS_SQL})
            OR NOT EXISTS (
                SELECT 1
                FROM agent_model_execution_runs AS model_run
                WHERE model_run.profile_id = {effect_alias}.profile_id
                  AND model_run.session_id = {effect_alias}.session_id
                  AND model_run.ownership_generation
                      = {effect_alias}.ownership_generation
                  AND model_run.effect_id = {effect_alias}.effect_id
                  AND model_run.execution_status IN ('running', 'finished', 'unknown')
            )
        )
        AND (
            {effect_alias}.kind NOT IN ({_MODEL_WORKFLOW_EFFECT_KINDS_SQL})
            OR NOT EXISTS (
                SELECT 1
                FROM agent_model_execution_runs AS unknown_model_run
                WHERE unknown_model_run.profile_id = {effect_alias}.profile_id
                  AND unknown_model_run.session_id = {effect_alias}.session_id
                  AND unknown_model_run.ownership_generation
                      = {effect_alias}.ownership_generation
                  AND unknown_model_run.execution_status = 'unknown'
            )
        )
    """


def _model_work_cancellation_gate_sql(effect_alias: str) -> str:
    """Return all durable cancellation fences that prevent model-work replay."""

    return (
        _review_cancellation_gate_sql(effect_alias)
        + f"""
        AND NOT EXISTS (
            SELECT 1
            FROM agent_model_execution_cancellation_gates AS model_gate
            WHERE model_gate.profile_id = {effect_alias}.profile_id
              AND model_gate.session_id = {effect_alias}.session_id
              AND model_gate.ownership_generation
                  = {effect_alias}.ownership_generation
              AND model_gate.target_effect_id = {effect_alias}.effect_id
              AND model_gate.target_operation_id = {effect_alias}.operation_id
              AND model_gate.target_effect_kind = {effect_alias}.kind
              AND model_gate.target_contract_version
                  = {effect_alias}.contract_version
              AND model_gate.target_contract_signature
                  = {effect_alias}.contract_signature
        )
        """
    )


def _trusted_external_action_contract_sql(
    effect_alias: str,
) -> tuple[str, tuple[object, ...]]:
    predicates = tuple(
        (
            f"({effect_alias}.kind = ? "
            f"AND {effect_alias}.contract_version = ? "
            f"AND {effect_alias}.contract_signature = ?)"
        )
        for _identity in _EXTERNAL_ACTION_CONTRACT_SIGNATURES
    )
    params = tuple(
        item
        for (kind, version), signature in _EXTERNAL_ACTION_CONTRACT_SIGNATURES.items()
        for item in (kind, version, signature)
    )
    return " OR ".join(predicates), params


def _contract_filter_sql(
    effect_contracts: tuple[tuple[str, int], ...] | None,
    excluded_effect_contracts: tuple[tuple[str, int], ...],
    *,
    table_alias: str,
) -> tuple[str, tuple[object, ...], str, tuple[object, ...]]:
    if effect_contracts is not None:
        predicate = " OR ".join(
            (f"({table_alias}.kind = ? AND {table_alias}.contract_version = ?)")
            for _contract in effect_contracts
        )
        filter_sql = f"AND ({predicate})"
        cases = " ".join(
            (f"WHEN {table_alias}.kind = ? AND {table_alias}.contract_version = ? THEN {index}")
            for index, _contract in enumerate(effect_contracts)
        )
        priority_sql = f"CASE {cases} ELSE {len(effect_contracts)} END,"
        params = tuple(item for contract in effect_contracts for item in contract)
        return filter_sql, params, priority_sql, params
    if excluded_effect_contracts:
        predicate = " OR ".join(
            (f"({table_alias}.kind = ? AND {table_alias}.contract_version = ?)")
            for _contract in excluded_effect_contracts
        )
        return (
            f"AND NOT ({predicate})",
            tuple(item for contract in excluded_effect_contracts for item in contract),
            "",
            (),
        )
    return "", (), "", ()


__all__ = ["EffectStoreConflict", "SQLiteDurableEffectStore"]
