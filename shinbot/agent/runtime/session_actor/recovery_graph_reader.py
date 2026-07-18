"""Transaction-bound raw authority reader for session-actor recovery."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.events import ClaimedSessionEvent
from shinbot.agent.runtime.session_actor.recovery import (
    MAX_RECOVERY_GRAPH_EDGES,
    MAX_RECOVERY_GRAPH_NODES,
    MAX_RECOVERY_INVARIANTS,
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
    RecoveryAggregateFence,
    RecoveryCertificate,
    RecoveryContractDecodeError,
    RecoveryDeliveryEnvelopeIdentity,
    RecoveryDeliveryPayload,
    RecoveryGraphEdge,
    RecoveryGraphNode,
    RecoveryInvariant,
    RecoveryInvariantSeverity,
    RecoverySubject,
    RecoveryV1Policy,
    RecoveryWorkClassification,
    build_recovery_certificate,
    canonical_recovery_digest,
    decode_recovery_delivery_payload,
)
from shinbot.agent.runtime.session_actor.recovery_commit import (
    RecoveryDeliveryClaimLost,
)
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionFenceError
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipError,
    AgentRuntimeOwnershipMode,
)
from shinbot.persistence.canonical_json import validate_canonical_json_object
from shinbot.persistence.sqlite_raw import (
    RawSQLiteValue,
    RawSQLiteValueTruncatedError,
    bounded_raw_sqlite_projection,
    raw_sqlite_values,
)

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


MAX_RECOVERY_SOURCE_ROWS = 8
MAX_RECOVERY_RAW_FIELD_BYTES = 8_192
MAX_RECOVERY_TOTAL_RAW_BYTES = 1_048_576
MAX_RECOVERY_TOTAL_SOURCE_ROWS = 64

_AGGREGATE_SEMANTIC_VOLATILE_COLUMNS = frozenset({"event_sequence", "updated_at"})
_MODEL_EXECUTION_OPERATION_KINDS = {
    "run_active_reply_workflow": "active_reply",
    "run_active_chat_bootstrap": "active_chat_bootstrap",
    "run_active_chat_round": "active_chat_round",
    "run_idle_review_planning": "idle_review_planning",
}

_AGGREGATE_COLUMNS = (
    "profile_id",
    "session_id",
    "ownership_generation",
    "state",
    "state_revision",
    "event_sequence",
    "activity_generation",
    "active_epoch",
    "review_plan_json",
    "current_plan_id",
    "review_plan_revision",
    "active_reply_resume_json",
    "active_chat_state_json",
    "review_operation_id",
    "active_reply_operation_id",
    "active_chat_round_operation_id",
    "idle_planning_operation_id",
    "data_json",
    "updated_at",
)
_OWNERSHIP_COLUMNS = (
    "profile_id",
    "session_id",
    "legacy_session_id",
    "mode",
    "status",
    "pending_mode",
    "generation",
    "selection_reason",
    "migration_reason",
    "requested_by",
    "created_at",
    "updated_at",
)
_MAILBOX_COLUMNS = (
    "event_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "kind",
    "source",
    "occurred_at",
    "payload_json",
    "causation_id",
    "correlation_id",
    "trace_id",
    "status",
    "attempt_count",
    "available_at",
    "claim_id",
    "lease_owner",
    "lease_until",
    "created_at",
    "handled_at",
    "last_error",
)
_RECOVERY_CASE_COLUMNS = (
    "case_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "certificate_version",
    "policy_version",
    "work_graph_digest",
    "latest_certificate_digest",
    "status",
    "next_delivery_cycle",
    "delivery_count",
    "last_event_id",
    "last_error",
    "created_at",
    "updated_at",
)
_DELIVERY_IMMUTABLE_MAILBOX_COLUMNS = (
    "event_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "kind",
    "source",
    "occurred_at",
    "payload_json",
    "causation_id",
    "correlation_id",
    "trace_id",
    "created_at",
)
_OPERATION_COLUMNS = (
    "operation_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "kind",
    "status",
    "launched_by_event_id",
    "state_revision",
    "active_epoch",
    "activity_generation",
    "input_watermark",
    "input_ledger_sequence",
    "started_at",
    "lease_owner",
    "lease_until",
    "superseded_at",
    "finished_at",
    "failure_code",
    "failure_message",
    "metadata_json",
)
_EFFECT_COLUMNS = (
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
_REVIEW_CANCELLATION_GATE_COLUMNS = (
    "gate_seq",
    "profile_id",
    "session_id",
    "ownership_generation",
    "cancellation_effect_id",
    "request_event_id",
    "review_operation_id",
    "review_effect_id",
    "review_effect_kind",
    "review_contract_version",
    "review_contract_signature",
    "gate_status",
    "target_effect_status",
    "target_effect_claim_id",
    "target_effect_attempt_count",
    "target_effect_terminal_at",
    "created_at",
    "updated_at",
)
_REVIEW_EXECUTION_RUN_COLUMNS = (
    "run_seq",
    "profile_id",
    "session_id",
    "ownership_generation",
    "review_effect_id",
    "review_operation_id",
    "review_effect_kind",
    "review_contract_version",
    "review_contract_signature",
    "claim_id",
    "worker_id",
    "execution_status",
    "started_at",
    "finished_at",
    "unknown_at",
    "unknown_reason",
)
_MODEL_EXECUTION_RUN_COLUMNS = (
    "run_seq",
    "profile_id",
    "session_id",
    "ownership_generation",
    "effect_id",
    "operation_id",
    "effect_kind",
    "contract_version",
    "contract_signature",
    "claim_id",
    "worker_id",
    "execution_status",
    "started_at",
    "finished_at",
    "unknown_at",
    "unknown_reason",
)
_MODEL_EXECUTION_CANCELLATION_GATE_COLUMNS = (
    "gate_seq",
    "profile_id",
    "session_id",
    "ownership_generation",
    "cancellation_effect_id",
    "request_event_id",
    "target_operation_id",
    "target_effect_id",
    "target_effect_kind",
    "target_contract_version",
    "target_contract_signature",
    "target_effect_status",
    "target_claim_id",
    "target_worker_id",
    "target_effect_attempt_count",
    "target_execution_status",
    "gate_status",
    "target_effect_terminal_at",
    "blocker_code",
    "created_at",
    "updated_at",
)
_SCHEDULE_COLUMNS = (
    "plan_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "plan_revision",
    "status",
    "trigger",
    "outcome",
    "source",
    "requested_delay_seconds",
    "applied_delay_seconds",
    "scheduled_from",
    "next_review_at",
    "reason",
    "fallback_reason",
    "mention_sensitivity",
    "active_reply_threshold_json",
    "model_execution_id",
    "prompt_signature",
    "expected_active_epoch",
    "expected_activity_generation",
    "committed_state_revision",
    "available_at",
    "claim_owner",
    "claim_until",
    "attempt_count",
    "delivery_cycle",
    "last_error",
    "created_at",
    "updated_at",
)
_SCHEDULE_EVENT_COLUMNS = (
    "schedule_event_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "event_id",
    "plan_id",
    "previous_plan_id",
    "event_type",
    "trigger",
    "outcome",
    "source",
    "requested_delay_seconds",
    "applied_delay_seconds",
    "scheduled_from",
    "next_review_at",
    "reason",
    "fallback_reason",
    "model_execution_id",
    "prompt_signature",
    "expected_active_epoch",
    "expected_activity_generation",
    "committed_state_revision",
    "operation_id",
    "trace_id",
    "metadata_json",
    "created_at",
)
_TRANSITION_COLUMNS = (
    "transition_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "event_id",
    "from_state",
    "to_state",
    "trigger",
    "disposition",
    "state_revision",
    "event_sequence",
    "operation_id",
    "plan_id",
    "trace_id",
    "metadata_json",
    "created_at",
)
_CONSUMPTION_COLUMNS = (
    "consumption_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "kind",
    "selection",
    "idempotency_key",
    "operation_id",
    "source_event_id",
    "input_watermark",
    "input_ledger_sequence",
    "explicit_message_log_ids_json",
    "canonical_json",
    "reason",
    "trace_id",
    "occurred_at",
    "committed_at",
)
_LEDGER_COLUMNS = (
    "profile_id",
    "session_id",
    "ledger_sequence",
    "message_log_id",
    "ownership_generation",
    "source_event_id",
    "actor_event_id",
    "eligible_for_work",
    "review_consumption_id",
    "chat_consumption_id",
    "high_priority_consumption_id",
    "canonical_json",
    "updated_at",
)
_RECEIPT_COLUMNS = (
    "idempotency_key",
    "effect_id",
    "operation_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "action_ordinal",
    "action_kind",
    "contract_version",
    "request_digest",
    "request_json",
    "status",
    "attempt_count",
    "claim_id",
    "lease_owner",
    "lease_until",
    "platform_result_json",
    "rejection_json",
    "unknown_json",
    "assistant_message_log_id",
    "prepared_at",
    "execution_started_at",
    "settled_at",
    "updated_at",
)
_ATTEMPT_COLUMNS = (
    "idempotency_key",
    "attempt_count",
    "claim_id",
    "lease_owner",
    "claimed_at",
    "lease_until",
    "status",
    "platform_result_json",
    "rejection_json",
    "unknown_json",
    "assistant_message_log_id",
    "settled_at",
)
_ROUTE_COLUMNS = (
    "delivery_id",
    "routing_job_id",
    "profile_id",
    "session_id",
    "message_log_id",
    "route_rule_id",
    "version",
    "ownership_generation",
    "event_id",
    "payload_json",
    "payload_digest",
    "trace_id",
    "correlation_id",
    "causation_id",
    "status",
    "attempt_count",
    "available_at",
    "claim_id",
    "lease_owner",
    "lease_until",
    "created_at",
    "updated_at",
    "completed_at",
    "failed_at",
    "last_error_code",
    "last_error_message",
)

_JSON_FIELDS_BY_TABLE: Mapping[str, frozenset[str]] = {
    "agent_session_aggregates": frozenset(
        {
            "review_plan_json",
            "active_reply_resume_json",
            "active_chat_state_json",
            "data_json",
        }
    ),
    "agent_session_mailbox": frozenset({"payload_json"}),
    "agent_session_operations": frozenset({"metadata_json"}),
    "agent_effect_outbox": frozenset({"payload_json"}),
    "agent_review_schedules": frozenset({"active_reply_threshold_json"}),
    "agent_review_schedule_events": frozenset({"metadata_json"}),
    "agent_state_transitions": frozenset({"metadata_json"}),
    "agent_message_ledger_consumptions": frozenset({"canonical_json"}),
    "agent_message_ledger": frozenset({"canonical_json"}),
    "agent_external_action_receipts": frozenset(
        {
            "request_json",
            "platform_result_json",
            "rejection_json",
            "unknown_json",
        }
    ),
    "agent_external_action_attempts": frozenset(
        {"platform_result_json", "rejection_json", "unknown_json"}
    ),
    "agent_route_outbox": frozenset({"payload_json"}),
}


class RecoveryGraphReadError(RuntimeError):
    """Raised when raw durable authority cannot form a safe certificate."""

    def __init__(
        self,
        code: str,
        *,
        evidence: Mapping[str, object],
    ) -> None:
        """Store bounded evidence suitable for the findings ledger."""

        self.code = _required_text(code, field_name="recovery finding code")
        self.evidence = dict(evidence)
        super().__init__(self.code)


class RecoveryGraphNotEligible(RuntimeError):
    """Raised when valid authority no longer permits recovery for this subject."""

    def __init__(self, reason_code: str) -> None:
        """Expose a stable non-corruption reason for case supersession."""

        self.reason_code = _required_text(
            reason_code,
            field_name="recovery graph ineligibility reason",
        )
        super().__init__(self.reason_code)


@dataclass(slots=True, frozen=True)
class ValidatedClaimedRecoveryDelivery:
    """Typed recovery payload and physical mailbox key proven in one transaction."""

    mailbox_id: int
    delivery: RecoveryDeliveryPayload


@dataclass(slots=True, frozen=True)
class RecoveryCaseSnapshot:
    """Raw-validated recovery case authority read inside one transaction."""

    case_id: str
    profile_id: str
    session_id: str
    ownership_generation: int
    certificate_version: int
    policy_version: int
    work_graph_digest: str
    latest_certificate_digest: str
    status: str
    next_delivery_cycle: int
    delivery_count: int
    last_event_id: str
    last_error: str
    created_at: float
    updated_at: float


@dataclass(slots=True, frozen=True)
class _RecoveryGraphSnapshot:
    """One reader-owned graph plus its deterministic policy decision."""

    certificate: RecoveryCertificate
    key: SessionKey
    ownership_generation: int


@dataclass(slots=True, frozen=True)
class _RawSourceRow:
    """One bounded raw SQLite row used by the graph projection."""

    row_id: int
    values: Mapping[str, RawSQLiteValue]


@dataclass(slots=True, frozen=True)
class _ReviewExecutionLiveness:
    """Classify durable review cancellation evidence without inferring quiescence."""

    blocking_reason_codes: tuple[str, ...]
    blocking_node_identities: tuple[str, ...]
    waiting_reason_codes: tuple[str, ...]
    waiting_node_identities: tuple[str, ...]
    invariants: tuple[RecoveryInvariant, ...]


@dataclass(slots=True, frozen=True)
class _ModelExecutionLiveness:
    """Classify non-review model execution evidence without replaying it."""

    blocking_reason_codes: tuple[str, ...]
    blocking_node_identities: tuple[str, ...]
    waiting_reason_codes: tuple[str, ...]
    waiting_node_identities: tuple[str, ...]
    invariants: tuple[RecoveryInvariant, ...]


@dataclass(slots=True)
class _RecoveryReadBudget:
    """Bound raw authority materialized while reconstructing one graph."""

    raw_bytes: int = 0
    source_rows: int = 0

    def consume(
        self,
        *,
        table: str,
        row_id: int,
        values: Mapping[str, RawSQLiteValue],
    ) -> None:
        """Account for one raw row before it enters certificate authority."""

        row_bytes = sum(value.logical_byte_length for value in values.values())
        next_rows = self.source_rows + 1
        next_bytes = self.raw_bytes + row_bytes
        if next_rows > MAX_RECOVERY_TOTAL_SOURCE_ROWS:
            raise RecoveryGraphReadError(
                "recovery_authority_total_row_limit_exceeded",
                evidence={
                    "maximum_rows": MAX_RECOVERY_TOTAL_SOURCE_ROWS,
                    "observed_rows": next_rows,
                    "row_id": row_id,
                    "table": table,
                },
            )
        if next_bytes > MAX_RECOVERY_TOTAL_RAW_BYTES:
            raise RecoveryGraphReadError(
                "recovery_authority_total_byte_limit_exceeded",
                evidence={
                    "maximum_bytes": MAX_RECOVERY_TOTAL_RAW_BYTES,
                    "observed_bytes": next_bytes,
                    "row_id": row_id,
                    "row_bytes": row_bytes,
                    "table": table,
                },
            )
        self.source_rows = next_rows
        self.raw_bytes = next_bytes


class RecoveryGraphAuthority(Protocol):
    """Read-only recovery authority bound to one persistence domain."""

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain whose authority this reader observes."""

    @property
    def policy(self) -> RecoveryV1Policy:
        """Return the exact pure policy used to classify the graph."""

    def rebuild_certificate(
        self,
        conn: sqlite3.Connection,
        *,
        key: SessionKey,
        ownership_generation: int,
    ) -> RecoveryCertificate:
        """Rebuild authority inside the caller-owned transaction."""

    def load_delivery(
        self,
        conn: sqlite3.Connection,
        *,
        profile_id: str,
        session_id: str,
        event_id: str,
    ) -> tuple[RecoveryDeliveryPayload, str, int] | None:
        """Read one exact typed recovery delivery and its mailbox identity."""

    def validate_claimed_delivery(
        self,
        conn: sqlite3.Connection,
        *,
        claim: ClaimedSessionEvent,
        commit_now: float,
    ) -> ValidatedClaimedRecoveryDelivery:
        """Validate one processing delivery against its durable claim fence."""

    def load_case_snapshot(
        self,
        conn: sqlite3.Connection,
        *,
        case_id: str,
    ) -> RecoveryCaseSnapshot | None:
        """Load one exact recovery case without SQLite affinity coercion."""

    def validate_delivery_mailbox(
        self,
        conn: sqlite3.Connection,
        *,
        payload: RecoveryDeliveryPayload,
        payload_json: str,
        now: float,
        allow_missing: bool,
    ) -> bool:
        """Verify one immutable typed delivery envelope."""


class SQLiteRecoveryGraphReader:
    """Rebuild recovery authority without scanner, case, or mailbox writes."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        policy: RecoveryV1Policy | None = None,
    ) -> None:
        """Bind one reader to a persistence domain and policy instance."""

        self._database = database
        self._policy = policy or RecoveryV1Policy()

    @property
    def persistence_domain(self) -> object:
        """Return the exact database domain observed by this reader."""

        return self._database

    @property
    def policy(self) -> RecoveryV1Policy:
        """Return the pure policy used for every graph build."""

        return self._policy

    def rebuild_certificate(
        self,
        conn: sqlite3.Connection,
        *,
        key: SessionKey,
        ownership_generation: int,
    ) -> RecoveryCertificate:
        """Rebuild authority after revalidating Actor v2 ownership."""

        _require_reader_transaction(conn)
        try:
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                key,
                expected_generation=ownership_generation,
            )
        except (AgentRuntimeOwnershipError, ActorV2AdmissionFenceError) as exc:
            raise RecoveryGraphNotEligible("actor_v2_ownership_changed") from exc
        return _read_recovery_graph_snapshot(
            conn,
            key=key,
            ownership_generation=ownership_generation,
            policy=self._policy,
        ).certificate

    def load_delivery(
        self,
        conn: sqlite3.Connection,
        *,
        profile_id: str,
        session_id: str,
        event_id: str,
    ) -> tuple[RecoveryDeliveryPayload, str, int] | None:
        """Read and decode one exact recovery delivery mailbox row."""

        _require_reader_transaction(conn)
        rows = _read_delivery_mailbox_rows(
            conn,
            profile_id=profile_id,
            session_id=session_id,
            event_id=event_id,
        )
        if not rows:
            return None
        payload, status = _decode_delivery_mailbox_row(rows[0])
        return payload, status, rows[0].row_id

    def validate_claimed_delivery(
        self,
        conn: sqlite3.Connection,
        *,
        claim: ClaimedSessionEvent,
        commit_now: float,
    ) -> ValidatedClaimedRecoveryDelivery:
        """Validate raw processing state before a recovery commit is materialized."""

        _require_reader_transaction(conn)
        if not isinstance(claim, ClaimedSessionEvent):
            raise TypeError("claim must be a ClaimedSessionEvent")
        observed_at = _nonnegative_finite(commit_now, field_name="commit_now")
        envelope = claim.envelope
        rows = _read_delivery_mailbox_rows(
            conn,
            profile_id=envelope.key.profile_id,
            session_id=envelope.key.session_id,
            event_id=envelope.event_id,
            columns=_MAILBOX_COLUMNS,
        )
        if not rows:
            raise RecoveryGraphReadError(
                "recovery_delivery_disappeared",
                evidence={"event_id": envelope.event_id},
            )
        row = rows[0]
        for field_name, (storage_class, expected_value) in {
            "event_id": ("text", envelope.event_id),
            "profile_id": ("text", envelope.key.profile_id),
            "session_id": ("text", envelope.key.session_id),
            "ownership_generation": ("integer", envelope.ownership_generation),
            "kind": ("text", envelope.kind),
            "source": ("text", envelope.source),
            "occurred_at": ("real", envelope.occurred_at),
            "status": ("text", "processing"),
            "attempt_count": ("integer", claim.attempt_count),
            "available_at": ("real", envelope.available_at),
            "claim_id": ("text", claim.claim_id),
            "lease_owner": ("text", claim.worker_id),
            "lease_until": ("real", claim.lease_expires_at),
            "created_at": ("real", envelope.created_at),
            "last_error": ("text", ""),
        }.items():
            _validate_claim_raw_value(
                row,
                field_name=field_name,
                table="agent_session_mailbox",
                storage_class=storage_class,
                expected_value=expected_value,
            )
        _require_null_value(
            row,
            field_name="handled_at",
            table="agent_session_mailbox",
        )
        lease_until = _nonnegative_time_value(
            row,
            "lease_until",
            table="agent_session_mailbox",
        )
        if lease_until <= observed_at:
            raise RecoveryDeliveryClaimLost(
                "recovery_delivery_lease_expired",
            )
        available_at = _nonnegative_time_value(
            row,
            "available_at",
            table="agent_session_mailbox",
        )
        if available_at > claim.claimed_at:
            raise RecoveryDeliveryClaimLost("recovery_delivery_not_available_at_claim")
        payload, _status = _decode_delivery_mailbox_row(row)
        return ValidatedClaimedRecoveryDelivery(
            mailbox_id=row.row_id,
            delivery=payload,
        )

    def load_case_snapshot(
        self,
        conn: sqlite3.Connection,
        *,
        case_id: str,
    ) -> RecoveryCaseSnapshot | None:
        """Read one complete typed recovery case from raw SQLite authority."""

        _require_reader_transaction(conn)
        normalized_case_id = _required_text(case_id, field_name="case_id")
        rows = _read_recovery_case_rows(conn, case_id=normalized_case_id)
        if not rows:
            return None
        return _decode_recovery_case_snapshot(rows[0])

    def validate_delivery_mailbox(
        self,
        conn: sqlite3.Connection,
        *,
        payload: RecoveryDeliveryPayload,
        payload_json: str,
        now: float,
        allow_missing: bool,
    ) -> bool:
        """Verify immutable delivery storage and typed payload authority."""

        _require_reader_transaction(conn)
        return _validate_delivery_mailbox(
            conn,
            payload=payload,
            payload_json=payload_json,
            now=now,
            allow_missing=allow_missing,
        )


def _read_recovery_graph_snapshot(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    policy: RecoveryV1Policy,
) -> _RecoveryGraphSnapshot:
    budget = _RecoveryReadBudget()
    aggregate_rows = _read_source_rows(
        conn,
        table="agent_session_aggregates",
        columns=_AGGREGATE_COLUMNS,
        where_sql=_raw_session_scope(),
        parameters=_raw_session_scope_parameters(
            key,
            ownership_generation=ownership_generation,
        ),
        order_sql="source.rowid ASC",
        maximum_rows=1,
        budget=budget,
    )
    if len(aggregate_rows) != 1:
        raise RecoveryGraphReadError(
            "aggregate_authority_missing",
            evidence={
                "ownership_generation": ownership_generation,
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
        )
    aggregate = aggregate_rows[0]
    _validate_source_json("agent_session_aggregates", aggregate)
    aggregate_data = _source_json_object(
        aggregate,
        "data_json",
        table="agent_session_aggregates",
    )
    active_chat_state = _source_json_object(
        aggregate,
        "active_chat_state_json",
        table="agent_session_aggregates",
    )
    active_reply_resume = _source_json_object(
        aggregate,
        "active_reply_resume_json",
        table="agent_session_aggregates",
    )
    aggregate_profile = _text_value(
        aggregate,
        "profile_id",
        table="agent_session_aggregates",
    )
    aggregate_session = _text_value(
        aggregate,
        "session_id",
        table="agent_session_aggregates",
    )
    aggregate_generation = _positive_int_value(
        aggregate,
        "ownership_generation",
        table="agent_session_aggregates",
    )
    if (
        aggregate_profile != key.profile_id
        or aggregate_session != key.session_id
        or aggregate_generation != ownership_generation
    ):
        raise RecoveryGraphReadError(
            "aggregate_authority_conflict",
            evidence={
                "ownership_generation": ownership_generation,
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
        )
    state = _text_value(aggregate, "state", table="agent_session_aggregates")
    if state == "idle":
        raise RecoveryGraphNotEligible("aggregate_idle")
    aggregate_fence = RecoveryAggregateFence(
        state=state,
        state_revision=_nonnegative_int_value(
            aggregate,
            "state_revision",
            table="agent_session_aggregates",
        ),
        event_sequence=_nonnegative_int_value(
            aggregate,
            "event_sequence",
            table="agent_session_aggregates",
        ),
        activity_generation=_nonnegative_int_value(
            aggregate,
            "activity_generation",
            table="agent_session_aggregates",
        ),
        active_epoch=_nonnegative_int_value(
            aggregate,
            "active_epoch",
            table="agent_session_aggregates",
        ),
        current_plan_id=_text_value(
            aggregate,
            "current_plan_id",
            table="agent_session_aggregates",
            required=False,
        ),
        review_plan_revision=_nonnegative_int_value(
            aggregate,
            "review_plan_revision",
            table="agent_session_aggregates",
        ),
    )
    top_operation_ids = {
        "review": _text_value(
            aggregate,
            "review_operation_id",
            table="agent_session_aggregates",
            required=False,
        ),
        "active_reply": _text_value(
            aggregate,
            "active_reply_operation_id",
            table="agent_session_aggregates",
            required=False,
        ),
        "active_chat_round": _text_value(
            aggregate,
            "active_chat_round_operation_id",
            table="agent_session_aggregates",
            required=False,
        ),
        "idle_review_planning": _text_value(
            aggregate,
            "idle_planning_operation_id",
            table="agent_session_aggregates",
            required=False,
        ),
    }
    operation_roles, unexpected_operation_ids = _state_operation_shape(
        state=state,
        top_operation_ids=top_operation_ids,
        active_chat_state=active_chat_state,
    )
    operation_ids = tuple(operation_roles)
    current_plan_id = aggregate_fence.current_plan_id

    ownership_rows = _read_source_rows(
        conn,
        table="agent_session_runtime_ownership",
        columns=_OWNERSHIP_COLUMNS,
        where_sql=(
            _raw_session_scope(include_generation=False)
            + " AND CAST(source.generation AS BLOB) = ?"
        ),
        parameters=(
            *_raw_session_scope_parameters(key),
            str(ownership_generation).encode("ascii"),
        ),
        order_sql="source.rowid ASC",
        maximum_rows=1,
        budget=budget,
    )
    if len(ownership_rows) != 1:
        raise RecoveryGraphReadError(
            "ownership_authority_missing",
            evidence={"profile_id": key.profile_id, "session_id": key.session_id},
        )
    ownership = ownership_rows[0]
    if (
        _text_value(
            ownership,
            "mode",
            table="agent_session_runtime_ownership",
        )
        != AgentRuntimeOwnershipMode.ACTOR_V2.value
        or _text_value(
            ownership,
            "status",
            table="agent_session_runtime_ownership",
        )
        != "active"
        or _positive_int_value(
            ownership,
            "generation",
            table="agent_session_runtime_ownership",
        )
        != ownership_generation
    ):
        raise RecoveryGraphReadError(
            "ownership_authority_conflict",
            evidence={
                "ownership_generation": ownership_generation,
                "profile_id": key.profile_id,
                "session_id": key.session_id,
            },
        )

    mailbox_rows = _read_source_rows(
        conn,
        table="agent_session_mailbox",
        columns=_MAILBOX_COLUMNS,
        where_sql=(
            _raw_session_scope() + " "
            "AND CAST(source.status AS BLOB) IN (?, ?) "
            "AND NOT (CAST(source.kind AS BLOB) = ? "
            "AND CAST(source.source AS BLOB) = ?)"
        ),
        parameters=(
            *_raw_session_scope_parameters(
                key,
                ownership_generation=ownership_generation,
            ),
            b"pending",
            b"processing",
            RECOVERY_DELIVERY_EVENT_KIND.encode("utf-8", errors="strict"),
            RECOVERY_DELIVERY_EVENT_SOURCE.encode("utf-8", errors="strict"),
        ),
        order_sql="source.rowid ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )
    # A cancelled review outbox is not proof that its model task stopped.  The
    # gate and task-start witness live outside the aggregate's current
    # operation roots, so they must be projected before normal reachability.
    review_gate_rows = _read_review_cancellation_gate_rows(
        conn,
        key=key,
        budget=budget,
    )
    review_execution_run_rows = _read_review_execution_run_rows(
        conn,
        key=key,
        budget=budget,
    )
    model_cancellation_gate_rows = _read_model_execution_cancellation_gate_rows(
        conn,
        key=key,
        budget=budget,
    )
    model_execution_run_rows = _read_model_execution_run_rows(
        conn,
        key=key,
        budget=budget,
    )
    current_review_gate_rows = tuple(
        row
        for row in review_gate_rows
        if _positive_int_value(
            row,
            "ownership_generation",
            table="agent_review_cancellation_gates",
        )
        == ownership_generation
    )
    current_review_execution_run_rows = tuple(
        row
        for row in review_execution_run_rows
        if _positive_int_value(
            row,
            "ownership_generation",
            table="agent_review_execution_runs",
        )
        == ownership_generation
    )
    current_model_cancellation_gate_rows = tuple(
        row
        for row in model_cancellation_gate_rows
        if _positive_int_value(
            row,
            "ownership_generation",
            table="agent_model_execution_cancellation_gates",
        )
        == ownership_generation
    )
    current_model_execution_run_rows = tuple(
        row
        for row in model_execution_run_rows
        if _positive_int_value(
            row,
            "ownership_generation",
            table="agent_model_execution_runs",
        )
        == ownership_generation
    )
    review_identity_operation_ids = _review_liveness_operation_ids(
        review_gate_rows=current_review_gate_rows,
        review_execution_run_rows=current_review_execution_run_rows,
    )
    model_identity_operation_ids = _model_liveness_operation_ids(
        model_cancellation_gate_rows=current_model_cancellation_gate_rows,
        model_execution_run_rows=current_model_execution_run_rows,
    )
    review_identity_effect_ids = _review_liveness_effect_ids(
        review_gate_rows=current_review_gate_rows,
        review_execution_run_rows=current_review_execution_run_rows,
    )
    model_identity_effect_ids = _model_liveness_effect_ids(
        model_cancellation_gate_rows=current_model_cancellation_gate_rows,
        model_execution_run_rows=current_model_execution_run_rows,
    )
    rooted_operation_rows = _read_operation_rows(
        conn,
        key=key,
        ownership_generation=ownership_generation,
        operation_ids=operation_ids,
        budget=budget,
    )
    rooted_operation_ids = {
        _text_value(row, "operation_id", table="agent_session_operations")
        for row in rooted_operation_rows
    }
    operation_rows = _merge_source_rows(
        rooted_operation_rows,
        _read_operation_rows_by_ids(
            conn,
            key=key,
            ownership_generation=ownership_generation,
            operation_ids=tuple(
                operation_id
                for operation_id in (
                    *review_identity_operation_ids,
                    *model_identity_operation_ids,
                )
                if operation_id not in rooted_operation_ids
            ),
            budget=budget,
        ),
    )
    rooted_effect_rows = _read_effect_rows(
        conn,
        key=key,
        ownership_generation=ownership_generation,
        operation_ids=operation_ids,
        budget=budget,
    )
    rooted_effect_ids = {
        _text_value(row, "effect_id", table="agent_effect_outbox") for row in rooted_effect_rows
    }
    effect_rows = _merge_source_rows(
        rooted_effect_rows,
        _read_effect_rows_by_ids(
            conn,
            key=key,
            ownership_generation=ownership_generation,
            effect_ids=tuple(
                effect_id
                for effect_id in (
                    *review_identity_effect_ids,
                    *model_identity_effect_ids,
                )
                if effect_id not in rooted_effect_ids
            ),
            budget=budget,
        ),
    )
    schedule_rows = _read_schedule_rows(
        conn,
        key=key,
        ownership_generation=ownership_generation,
        current_plan_id=current_plan_id,
        budget=budget,
    )
    receipt_rows = _read_receipt_rows(
        conn,
        key=key,
        ownership_generation=ownership_generation,
        operation_ids=operation_ids,
        budget=budget,
    )
    schedule_event_rows = _read_schedule_event_rows(
        conn,
        key=key,
        ownership_generation=ownership_generation,
        current_plan_id=current_plan_id,
        operation_ids=operation_ids,
        budget=budget,
    )
    transition_rows = _read_transition_rows(
        conn,
        key=key,
        ownership_generation=ownership_generation,
        event_sequence=aggregate_fence.event_sequence,
        budget=budget,
    )
    consumption_rows = _read_consumption_rows(
        conn,
        key=key,
        ownership_generation=ownership_generation,
        operation_ids=operation_ids,
        budget=budget,
    )
    attempt_rows = _read_attempt_rows(
        conn,
        receipt_rows=receipt_rows,
        budget=budget,
    )
    ledger_rows = _read_ledger_rows(
        conn,
        key=key,
        ownership_generation=ownership_generation,
        consumption_rows=consumption_rows,
        budget=budget,
    )
    route_rows = _read_source_rows(
        conn,
        table="agent_route_outbox",
        columns=_ROUTE_COLUMNS,
        where_sql=(_raw_session_scope() + " AND CAST(source.status AS BLOB) IN (?, ?)"),
        parameters=(
            *_raw_session_scope_parameters(
                key,
                ownership_generation=ownership_generation,
            ),
            b"pending",
            b"processing",
        ),
        order_sql="source.outbox_seq ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )
    for table_name, rows in (
        ("agent_session_mailbox", mailbox_rows),
        ("agent_session_operations", operation_rows),
        ("agent_effect_outbox", effect_rows),
        ("agent_review_cancellation_gates", review_gate_rows),
        ("agent_review_execution_runs", review_execution_run_rows),
        ("agent_model_execution_cancellation_gates", model_cancellation_gate_rows),
        ("agent_model_execution_runs", model_execution_run_rows),
        ("agent_review_schedules", schedule_rows),
        ("agent_review_schedule_events", schedule_event_rows),
        ("agent_state_transitions", transition_rows),
        ("agent_message_ledger_consumptions", consumption_rows),
        ("agent_external_action_attempts", attempt_rows),
        ("agent_message_ledger", ledger_rows),
        ("agent_external_action_receipts", receipt_rows),
        ("agent_route_outbox", route_rows),
    ):
        for row in rows:
            _validate_source_json(table_name, row)

    aggregate_node = _node_from_row(
        identity="aggregate",
        kind="aggregate",
        authority="agent_session_aggregates",
        status=state,
        row=aggregate,
        semantic_excluded_fields=_AGGREGATE_SEMANTIC_VOLATILE_COLUMNS,
    )
    ownership_node = _node_from_row(
        identity="ownership",
        kind="ownership",
        authority="agent_session_runtime_ownership",
        status="active",
        row=ownership,
    )
    nodes: list[RecoveryGraphNode] = [aggregate_node, ownership_node]
    edges: list[RecoveryGraphEdge] = [
        RecoveryGraphEdge(
            identity="edge:aggregate:ownership",
            source="aggregate",
            target="ownership",
            relation="owned_by",
        )
    ]
    mailbox_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_session_mailbox",
        kind="mailbox",
        id_column="event_id",
        rows=mailbox_rows,
    )
    operation_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_session_operations",
        kind="operation",
        id_column="operation_id",
        rows=operation_rows,
    )
    effect_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_effect_outbox",
        kind="effect",
        id_column="effect_id",
        rows=effect_rows,
    )
    review_gate_nodes = _append_review_cancellation_gate_nodes(
        nodes,
        edges,
        rows=review_gate_rows,
    )
    review_execution_run_nodes = _append_review_execution_run_nodes(
        nodes,
        edges,
        rows=review_execution_run_rows,
    )
    model_cancellation_gate_nodes = _append_model_execution_cancellation_gate_nodes(
        nodes,
        edges,
        rows=model_cancellation_gate_rows,
    )
    model_execution_run_nodes = _append_model_execution_run_nodes(
        nodes,
        edges,
        rows=model_execution_run_rows,
    )
    schedule_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_review_schedules",
        kind="review_schedule",
        id_column="plan_id",
        rows=schedule_rows,
    )
    schedule_event_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_review_schedule_events",
        kind="review_schedule_event",
        id_column="schedule_event_id",
        rows=schedule_event_rows,
        status_column=None,
        static_status="recorded",
    )
    transition_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_state_transitions",
        kind="state_transition",
        id_column="transition_id",
        rows=transition_rows,
        status_column=None,
        static_status="committed",
    )
    consumption_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_message_ledger_consumptions",
        kind="message_consumption",
        id_column="consumption_id",
        rows=consumption_rows,
        status_column=None,
        static_status="committed",
    )
    ledger_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_message_ledger",
        kind="message_ledger_entry",
        id_column="source_event_id",
        rows=ledger_rows,
        status_column=None,
        static_status="recorded",
    )
    receipt_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_external_action_receipts",
        kind="external_action_receipt",
        id_column="idempotency_key",
        rows=receipt_rows,
    )
    attempt_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_external_action_attempts",
        kind="external_action_attempt",
        id_column="claim_id",
        rows=attempt_rows,
    )
    route_nodes = _append_row_nodes(
        nodes,
        edges,
        table="agent_route_outbox",
        kind="route_delivery",
        id_column="delivery_id",
        rows=route_rows,
    )
    classification, invariants = _classify_work(
        key=key,
        aggregate_node_id=aggregate_node.identity,
        aggregate_state=state,
        aggregate_data=aggregate_data,
        active_chat_state=active_chat_state,
        active_reply_resume=active_reply_resume,
        ownership_generation=ownership_generation,
        operation_roles=operation_roles,
        unexpected_operation_ids=unexpected_operation_ids,
        operation_ids=operation_ids,
        operation_rows=operation_rows,
        operation_nodes=operation_nodes,
        mailbox_nodes=mailbox_nodes,
        effect_rows=effect_rows,
        effect_nodes=effect_nodes,
        schedule_rows=schedule_rows,
        schedule_nodes=schedule_nodes,
        receipt_rows=receipt_rows,
        receipt_nodes=receipt_nodes,
        route_nodes=route_nodes,
        schedule_event_rows=schedule_event_rows,
        schedule_event_nodes=schedule_event_nodes,
        transition_rows=transition_rows,
        transition_nodes=transition_nodes,
        aggregate_fence=aggregate_fence,
        consumption_rows=consumption_rows,
        consumption_nodes=consumption_nodes,
        ledger_rows=ledger_rows,
        ledger_nodes=ledger_nodes,
        attempt_rows=attempt_rows,
        attempt_nodes=attempt_nodes,
        review_gate_rows=review_gate_rows,
        review_gate_nodes=review_gate_nodes,
        review_execution_run_rows=review_execution_run_rows,
        review_execution_run_nodes=review_execution_run_nodes,
        model_cancellation_gate_rows=model_cancellation_gate_rows,
        model_cancellation_gate_nodes=model_cancellation_gate_nodes,
        model_execution_run_rows=model_execution_run_rows,
        model_execution_run_nodes=model_execution_run_nodes,
    )
    _append_reachable_edges(
        edges,
        ownership_generation=ownership_generation,
        operation_rows=operation_rows,
        operation_nodes=operation_nodes,
        effect_rows=effect_rows,
        effect_nodes=effect_nodes,
        schedule_event_rows=schedule_event_rows,
        schedule_event_nodes=schedule_event_nodes,
        schedule_nodes=schedule_nodes,
        consumption_rows=consumption_rows,
        consumption_nodes=consumption_nodes,
        ledger_rows=ledger_rows,
        ledger_nodes=ledger_nodes,
        receipt_rows=receipt_rows,
        receipt_nodes=receipt_nodes,
        attempt_rows=attempt_rows,
        attempt_nodes=attempt_nodes,
        review_gate_rows=review_gate_rows,
        review_gate_nodes=review_gate_nodes,
        review_execution_run_rows=review_execution_run_rows,
        review_execution_run_nodes=review_execution_run_nodes,
        model_cancellation_gate_rows=model_cancellation_gate_rows,
        model_cancellation_gate_nodes=model_cancellation_gate_nodes,
        model_execution_run_rows=model_execution_run_rows,
        model_execution_run_nodes=model_execution_run_nodes,
    )
    _validate_graph_limits(nodes=nodes, edges=edges, invariants=invariants)
    decision = policy.decide(classification)
    certificate = build_recovery_certificate(
        subject=RecoverySubject(
            profile_id=key.profile_id,
            session_id=key.session_id,
            ownership_generation=ownership_generation,
        ),
        aggregate_fence=aggregate_fence,
        nodes=nodes,
        edges=edges,
        invariants=invariants,
        decision=decision,
        policy_version=policy.policy_version,
    )
    return _RecoveryGraphSnapshot(
        certificate=certificate,
        key=key,
        ownership_generation=ownership_generation,
    )


def _read_operation_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_ids: Sequence[str],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    where_sql, parameters = _live_or_referenced_where(
        base=_raw_session_scope(),
        base_parameters=_raw_session_scope_parameters(
            key,
            ownership_generation=ownership_generation,
        ),
        status_column="status",
        live_statuses=("pending", "running"),
        identity_column="operation_id",
        identities=operation_ids,
    )
    return _read_source_rows(
        conn,
        table="agent_session_operations",
        columns=_OPERATION_COLUMNS,
        where_sql=where_sql,
        parameters=parameters,
        order_sql="source.operation_id ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_effect_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_ids: Sequence[str],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    where_sql, parameters = _live_or_referenced_where(
        base=_raw_session_scope(),
        base_parameters=_raw_session_scope_parameters(
            key,
            ownership_generation=ownership_generation,
        ),
        status_column="status",
        live_statuses=("pending", "processing"),
        identity_column="operation_id",
        identities=operation_ids,
    )
    return _read_source_rows(
        conn,
        table="agent_effect_outbox",
        columns=_EFFECT_COLUMNS,
        where_sql=where_sql,
        parameters=parameters,
        order_sql="source.effect_seq ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_review_cancellation_gate_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read unresolved cancellation gates across the stable session.

    ``cancelled`` was an intermediate status in an earlier implementation.
    Historical rows in that state may already have complete terminal proof, but
    cannot be rewritten by this read-only recovery path.  Exclude only rows
    whose exact target, control payload, and lack of a running witness prove
    that they are equivalent to a modern ``terminal`` gate.  Every missing or
    mismatched relationship remains authority and is classified fail-closed.
    """

    unresolved_sql, unresolved_parameters = _unresolved_review_cancellation_gate_where()

    return _read_source_rows(
        conn,
        table="agent_review_cancellation_gates",
        columns=_REVIEW_CANCELLATION_GATE_COLUMNS,
        where_sql=_raw_session_scope(include_generation=False) + " AND " + unresolved_sql,
        parameters=(
            *_raw_session_scope_parameters(key),
            *unresolved_parameters,
        ),
        order_sql="source.gate_seq ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _unresolved_review_cancellation_gate_where() -> tuple[str, tuple[object, ...]]:
    """Build the strict predicate for legacy non-terminal gate evidence.

    The expression returns every requested gate and every cancelled gate that
    lacks at least one durable proof required for terminalization.  It uses
    only immutable outbox identity plus the exact control payload fence; a
    terminal effect or an expired lease alone never qualifies.
    """

    control_contract = builtin_effect_contract("cancel_review_workflow", version=2)
    return (
        """
        (
            CAST(source.gate_status AS BLOB) = ?
            OR (
                CAST(source.gate_status AS BLOB) = ?
                AND NOT (
                    typeof(source.gate_seq) = 'integer'
                    AND typeof(source.profile_id) = 'text'
                    AND typeof(source.session_id) = 'text'
                    AND typeof(source.ownership_generation) = 'integer'
                    AND typeof(source.cancellation_effect_id) = 'text'
                    AND typeof(source.request_event_id) = 'text'
                    AND typeof(source.review_operation_id) = 'text'
                    AND typeof(source.review_effect_id) = 'text'
                    AND typeof(source.review_effect_kind) = 'text'
                    AND typeof(source.review_contract_version) = 'integer'
                    AND typeof(source.review_contract_signature) = 'text'
                    AND typeof(source.gate_status) = 'text'
                    AND typeof(source.target_effect_status) = 'text'
                    AND typeof(source.target_effect_claim_id) = 'text'
                    AND typeof(source.target_effect_attempt_count) = 'integer'
                    AND typeof(source.target_effect_terminal_at) IN ('integer', 'real')
                    AND typeof(source.created_at) IN ('integer', 'real')
                    AND typeof(source.updated_at) IN ('integer', 'real')
                    AND source.updated_at >= source.created_at
                    AND CAST(source.target_effect_status AS BLOB) = ?
                    AND EXISTS (
                        SELECT 1
                        FROM agent_effect_outbox AS target
                        WHERE CAST(target.profile_id AS BLOB)
                                  = CAST(source.profile_id AS BLOB)
                          AND CAST(target.session_id AS BLOB)
                                  = CAST(source.session_id AS BLOB)
                          AND target.ownership_generation
                                  = source.ownership_generation
                          AND CAST(target.effect_id AS BLOB)
                                  = CAST(source.review_effect_id AS BLOB)
                          AND CAST(target.operation_id AS BLOB)
                                  = CAST(source.review_operation_id AS BLOB)
                          AND CAST(target.kind AS BLOB)
                                  = CAST(source.review_effect_kind AS BLOB)
                          AND target.contract_version
                                  = source.review_contract_version
                          AND CAST(target.contract_signature AS BLOB)
                                  = CAST(source.review_contract_signature AS BLOB)
                          AND CAST(target.status AS BLOB) = ?
                          AND target.attempt_count
                                  = source.target_effect_attempt_count
                          AND CAST(target.claim_id AS BLOB) = ?
                          AND CAST(target.lease_owner AS BLOB) = ?
                          AND target.lease_until IS NULL
                          AND target.completed_at
                                  = source.target_effect_terminal_at
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM agent_session_operations AS operation
                        WHERE CAST(operation.profile_id AS BLOB)
                                  = CAST(source.profile_id AS BLOB)
                          AND CAST(operation.session_id AS BLOB)
                                  = CAST(source.session_id AS BLOB)
                          AND operation.ownership_generation
                                  = source.ownership_generation
                          AND CAST(operation.operation_id AS BLOB)
                                  = CAST(source.review_operation_id AS BLOB)
                          AND CAST(operation.kind AS BLOB) = ?
                          AND CAST(operation.status AS BLOB) = ?
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM agent_effect_outbox AS control
                        WHERE CAST(control.profile_id AS BLOB)
                                  = CAST(source.profile_id AS BLOB)
                          AND CAST(control.session_id AS BLOB)
                                  = CAST(source.session_id AS BLOB)
                          AND control.ownership_generation
                                  = source.ownership_generation
                          AND CAST(control.effect_id AS BLOB)
                                  = CAST(source.cancellation_effect_id AS BLOB)
                          AND CAST(control.idempotency_key AS BLOB)
                                  = CAST(source.cancellation_effect_id AS BLOB)
                          AND CAST(control.event_id AS BLOB)
                                  = CAST(source.request_event_id AS BLOB)
                          AND CAST(control.operation_id AS BLOB)
                                  = CAST(source.review_operation_id AS BLOB)
                          AND CAST(control.kind AS BLOB) = ?
                          AND control.contract_version = ?
                          AND CAST(control.contract_signature AS BLOB) = ?
                          AND CAST(control.status AS BLOB) IN (?, ?, ?)
                          AND CAST(control.lease_owner AS BLOB) = ?
                          AND control.lease_until IS NULL
                          AND control.completed_at IS NOT NULL
                          AND typeof(control.payload_json) = 'text'
                          AND length(CAST(control.payload_json AS BLOB)) <= ?
                          AND json_valid(control.payload_json)
                          AND json_type(control.payload_json) = 'object'
                          AND json_type(
                              control.payload_json,
                              '$.operation_id'
                          ) = 'text'
                          AND CAST(
                              json_extract(
                                  control.payload_json,
                                  '$.operation_id'
                              ) AS BLOB
                          ) = CAST(source.review_operation_id AS BLOB)
                          AND json_type(
                              control.payload_json,
                              '$.cancelled_operation_fence'
                          ) = 'object'
                          AND json_type(
                              control.payload_json,
                              '$.cancelled_operation_fence.operation_id'
                          ) = 'text'
                          AND CAST(
                              json_extract(
                                  control.payload_json,
                                  '$.cancelled_operation_fence.operation_id'
                              ) AS BLOB
                          ) = CAST(source.review_operation_id AS BLOB)
                          AND json_type(
                              control.payload_json,
                              '$.cancelled_operation_fence.effect_id'
                          ) = 'text'
                          AND CAST(
                              json_extract(
                                  control.payload_json,
                                  '$.cancelled_operation_fence.effect_id'
                              ) AS BLOB
                          ) = CAST(source.review_effect_id AS BLOB)
                          AND json_type(
                              control.payload_json,
                              '$.cancelled_operation_fence.effect_kind'
                          ) = 'text'
                          AND CAST(
                              json_extract(
                                  control.payload_json,
                                  '$.cancelled_operation_fence.effect_kind'
                              ) AS BLOB
                          ) = CAST(source.review_effect_kind AS BLOB)
                          AND json_type(
                              control.payload_json,
                              '$.cancelled_operation_fence.contract_version'
                          ) = 'integer'
                          AND json_extract(
                              control.payload_json,
                              '$.cancelled_operation_fence.contract_version'
                          ) = source.review_contract_version
                          AND json_type(
                              control.payload_json,
                              '$.cancelled_operation_fence.contract_signature'
                          ) = 'text'
                          AND CAST(
                              json_extract(
                                  control.payload_json,
                                  '$.cancelled_operation_fence.contract_signature'
                              ) AS BLOB
                          ) = CAST(source.review_contract_signature AS BLOB)
                          AND json_type(
                              control.payload_json,
                              '$.cancelled_operation_fence.ownership_generation'
                          ) = 'integer'
                          AND json_extract(
                              control.payload_json,
                              '$.cancelled_operation_fence.ownership_generation'
                          ) = source.ownership_generation
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM agent_review_execution_runs AS witness
                        WHERE CAST(witness.profile_id AS BLOB)
                                  = CAST(source.profile_id AS BLOB)
                          AND CAST(witness.session_id AS BLOB)
                                  = CAST(source.session_id AS BLOB)
                          AND witness.ownership_generation
                                  = source.ownership_generation
                          AND CAST(witness.review_effect_id AS BLOB)
                                  = CAST(source.review_effect_id AS BLOB)
                          AND CAST(witness.execution_status AS BLOB) IN (?, ?)
                    )
                )
            )
        )
        """,
        (
            b"requested",
            b"cancelled",
            b"cancelled",
            b"cancelled",
            b"",
            b"",
            b"review",
            b"superseded",
            b"cancel_review_workflow",
            control_contract.version,
            control_contract.signature.encode("utf-8", errors="strict"),
            b"completed",
            b"failed",
            b"cancelled",
            b"",
            MAX_RECOVERY_RAW_FIELD_BYTES,
            b"running",
            b"unknown",
        ),
    )


def _read_review_execution_run_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read durable review tasks whose model result is not safely replayable."""

    return _read_source_rows(
        conn,
        table="agent_review_execution_runs",
        columns=_REVIEW_EXECUTION_RUN_COLUMNS,
        where_sql=(
            _raw_session_scope(include_generation=False)
            + " AND CAST(source.execution_status AS BLOB) IN (?, ?)"
        ),
        parameters=(
            *_raw_session_scope_parameters(key),
            b"running",
            b"unknown",
        ),
        order_sql="source.run_seq ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_model_execution_run_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read non-review model executions that cannot safely be replayed."""

    return _read_source_rows(
        conn,
        table="agent_model_execution_runs",
        columns=_MODEL_EXECUTION_RUN_COLUMNS,
        where_sql=(
            _raw_session_scope(include_generation=False)
            + " AND CAST(source.execution_status AS BLOB) IN (?, ?)"
        ),
        parameters=(
            *_raw_session_scope_parameters(key),
            b"running",
            b"unknown",
        ),
        order_sql="source.run_seq ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_model_execution_cancellation_gate_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read every unresolved generic model-cancellation declaration.

    A terminal gate is historical evidence only after its control effect is
    terminal too. A still-pending control must remain reachable so recovery
    does not lose the completion that clears the aggregate's control intent.
    Requested, cancelled, and blocked gates remain live authority because their
    target may still be executing or need an operator-visible reconciliation
    outcome.
    """

    return _read_source_rows(
        conn,
        table="agent_model_execution_cancellation_gates",
        columns=_MODEL_EXECUTION_CANCELLATION_GATE_COLUMNS,
        where_sql=(
            _raw_session_scope(include_generation=False)
            + """
            AND (
                NOT (
                    typeof(source.gate_status) = 'text'
                    AND CAST(source.gate_status AS BLOB) = ?
                )
                OR EXISTS (
                    SELECT 1
                    FROM agent_effect_outbox AS control
                    WHERE CAST(control.profile_id AS BLOB)
                              = CAST(source.profile_id AS BLOB)
                      AND CAST(control.session_id AS BLOB)
                              = CAST(source.session_id AS BLOB)
                      AND control.ownership_generation
                              = source.ownership_generation
                      AND CAST(control.effect_id AS BLOB)
                              = CAST(source.cancellation_effect_id AS BLOB)
                      AND CAST(control.status AS BLOB) IN (?, ?)
                )
            )
            """
        ),
        parameters=(
            *_raw_session_scope_parameters(key),
            b"terminal",
            b"pending",
            b"processing",
        ),
        order_sql="source.gate_seq ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _review_liveness_operation_ids(
    *,
    review_gate_rows: Sequence[_RawSourceRow],
    review_execution_run_rows: Sequence[_RawSourceRow],
) -> tuple[str, ...]:
    """Return every old review operation required to validate liveness evidence."""

    operation_ids: list[str] = []
    for row in review_gate_rows:
        operation_ids.append(
            _text_value(
                row,
                "review_operation_id",
                table="agent_review_cancellation_gates",
            )
        )
    for row in review_execution_run_rows:
        operation_ids.append(
            _text_value(
                row,
                "review_operation_id",
                table="agent_review_execution_runs",
            )
        )
    return tuple(dict.fromkeys(operation_ids))


def _review_liveness_effect_ids(
    *,
    review_gate_rows: Sequence[_RawSourceRow],
    review_execution_run_rows: Sequence[_RawSourceRow],
) -> tuple[str, ...]:
    """Return cancellation control and review target effects needed for validation."""

    effect_ids: list[str] = []
    for row in review_gate_rows:
        effect_ids.extend(
            (
                _text_value(
                    row,
                    "cancellation_effect_id",
                    table="agent_review_cancellation_gates",
                ),
                _text_value(
                    row,
                    "review_effect_id",
                    table="agent_review_cancellation_gates",
                ),
            )
        )
    for row in review_execution_run_rows:
        effect_ids.append(
            _text_value(
                row,
                "review_effect_id",
                table="agent_review_execution_runs",
            )
        )
    return tuple(dict.fromkeys(effect_ids))


def _model_liveness_operation_ids(
    *,
    model_cancellation_gate_rows: Sequence[_RawSourceRow],
    model_execution_run_rows: Sequence[_RawSourceRow],
) -> tuple[str, ...]:
    """Return model operations needed to validate generic liveness evidence."""

    operation_ids: list[str] = []
    for row in model_cancellation_gate_rows:
        operation_ids.append(
            _text_value(
                row,
                "target_operation_id",
                table="agent_model_execution_cancellation_gates",
            )
        )
    for row in model_execution_run_rows:
        operation_ids.append(
            _text_value(
                row,
                "operation_id",
                table="agent_model_execution_runs",
            )
        )
    return tuple(dict.fromkeys(operation_ids))


def _model_liveness_effect_ids(
    *,
    model_cancellation_gate_rows: Sequence[_RawSourceRow],
    model_execution_run_rows: Sequence[_RawSourceRow],
) -> tuple[str, ...]:
    """Return model effects needed to validate generic liveness evidence."""

    effect_ids: list[str] = []
    for row in model_cancellation_gate_rows:
        effect_ids.extend(
            (
                _text_value(
                    row,
                    "cancellation_effect_id",
                    table="agent_model_execution_cancellation_gates",
                ),
                _text_value(
                    row,
                    "target_effect_id",
                    table="agent_model_execution_cancellation_gates",
                ),
            )
        )
    for row in model_execution_run_rows:
        effect_ids.append(
            _text_value(
                row,
                "effect_id",
                table="agent_model_execution_runs",
            )
        )
    return tuple(dict.fromkeys(effect_ids))


def _read_operation_rows_by_ids(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_ids: Sequence[str],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read exact operation rows referenced only by review liveness evidence."""

    return _read_rows_by_text_identities(
        conn,
        table="agent_session_operations",
        columns=_OPERATION_COLUMNS,
        identity_column="operation_id",
        identities=operation_ids,
        key=key,
        ownership_generation=ownership_generation,
        order_sql="source.operation_id ASC",
        budget=budget,
    )


def _read_effect_rows_by_ids(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    effect_ids: Sequence[str],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read exact outbox rows referenced only by review liveness evidence."""

    return _read_rows_by_text_identities(
        conn,
        table="agent_effect_outbox",
        columns=_EFFECT_COLUMNS,
        identity_column="effect_id",
        identities=effect_ids,
        key=key,
        ownership_generation=ownership_generation,
        order_sql="source.effect_seq ASC",
        budget=budget,
    )


def _read_rows_by_text_identities(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: tuple[str, ...],
    identity_column: str,
    identities: Sequence[str],
    key: SessionKey,
    ownership_generation: int,
    order_sql: str,
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read a bounded exact-id projection without treating aggregate roots as complete."""

    if not identities:
        return ()
    normalized = tuple(dict.fromkeys(identities))
    return _read_source_rows(
        conn,
        table=table,
        columns=columns,
        where_sql=(
            _raw_session_scope()
            + f" AND CAST(source.{identity_column} AS BLOB) IN ("
            + ", ".join("?" for _identity in normalized)
            + ")"
        ),
        parameters=(
            *_raw_session_scope_parameters(
                key,
                ownership_generation=ownership_generation,
            ),
            *(identity.encode("utf-8", errors="strict") for identity in normalized),
        ),
        order_sql=order_sql,
        maximum_rows=len(normalized),
        budget=budget,
    )


def _merge_source_rows(
    *groups: Sequence[_RawSourceRow],
) -> tuple[_RawSourceRow, ...]:
    """Merge independently rooted raw projections while preserving read order."""

    rows: list[_RawSourceRow] = []
    seen_row_ids: set[int] = set()
    for group in groups:
        for row in group:
            if row.row_id in seen_row_ids:
                continue
            seen_row_ids.add(row.row_id)
            rows.append(row)
    return tuple(rows)


def _read_schedule_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    current_plan_id: str,
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    where_sql = _raw_session_scope() + " AND (CAST(source.status AS BLOB) IN (?, ?)"
    parameters: list[object] = [
        *_raw_session_scope_parameters(
            key,
            ownership_generation=ownership_generation,
        ),
        b"scheduled",
        b"claimed",
    ]
    if current_plan_id:
        where_sql += " OR CAST(source.plan_id AS BLOB) = ?"
        parameters.append(current_plan_id.encode("utf-8", errors="strict"))
    where_sql += ")"
    return _read_source_rows(
        conn,
        table="agent_review_schedules",
        columns=_SCHEDULE_COLUMNS,
        where_sql=where_sql,
        parameters=tuple(parameters),
        order_sql="source.plan_revision ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_receipt_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_ids: Sequence[str],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    where_sql, parameters = _live_or_referenced_where(
        base=_raw_session_scope(),
        base_parameters=_raw_session_scope_parameters(
            key,
            ownership_generation=ownership_generation,
        ),
        status_column="status",
        live_statuses=("prepared", "executing", "unknown"),
        identity_column="operation_id",
        identities=operation_ids,
    )
    return _read_source_rows(
        conn,
        table="agent_external_action_receipts",
        columns=_RECEIPT_COLUMNS,
        where_sql=where_sql,
        parameters=parameters,
        order_sql="source.receipt_seq ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_schedule_event_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    current_plan_id: str,
    operation_ids: Sequence[str],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read the bounded schedule journal reachable from current work."""

    roots: list[str] = []
    parameters: list[object] = [
        *_raw_session_scope_parameters(
            key,
            ownership_generation=ownership_generation,
        )
    ]
    if current_plan_id:
        roots.append("CAST(source.plan_id AS BLOB) = ?")
        parameters.append(current_plan_id.encode("utf-8", errors="strict"))
        roots.append("CAST(source.previous_plan_id AS BLOB) = ?")
        parameters.append(current_plan_id.encode("utf-8", errors="strict"))
    if operation_ids:
        roots.append(
            "CAST(source.operation_id AS BLOB) IN ("
            + ", ".join("?" for _operation_id in operation_ids)
            + ")"
        )
        parameters.extend(
            operation_id.encode("utf-8", errors="strict") for operation_id in operation_ids
        )
    if not roots:
        return ()
    return _read_source_rows(
        conn,
        table="agent_review_schedule_events",
        columns=_SCHEDULE_EVENT_COLUMNS,
        where_sql=_raw_session_scope() + " AND (" + " OR ".join(roots) + ")",
        parameters=tuple(parameters),
        order_sql="source.schedule_event_seq ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_consumption_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_ids: Sequence[str],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read consumptions that can change the referenced work's inputs."""

    if not operation_ids:
        return ()
    return _read_source_rows(
        conn,
        table="agent_message_ledger_consumptions",
        columns=_CONSUMPTION_COLUMNS,
        where_sql=(
            _raw_session_scope()
            + " AND CAST(source.operation_id AS BLOB) IN ("
            + ", ".join("?" for _operation_id in operation_ids)
            + ")"
        ),
        parameters=(
            *_raw_session_scope_parameters(
                key,
                ownership_generation=ownership_generation,
            ),
            *(operation_id.encode("utf-8") for operation_id in operation_ids),
        ),
        order_sql="source.committed_at ASC, source.consumption_id ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_transition_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    event_sequence: int,
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read the exact transition journal tail for the aggregate fence."""

    if event_sequence == 0:
        return ()
    return _read_source_rows(
        conn,
        table="agent_state_transitions",
        columns=_TRANSITION_COLUMNS,
        where_sql=(_raw_session_scope() + " AND CAST(source.event_sequence AS BLOB) = ?"),
        parameters=(
            *_raw_session_scope_parameters(
                key,
                ownership_generation=ownership_generation,
            ),
            str(event_sequence).encode("ascii"),
        ),
        order_sql="source.transition_seq ASC",
        maximum_rows=1,
        budget=budget,
    )


def _read_attempt_rows(
    conn: sqlite3.Connection,
    *,
    receipt_rows: Sequence[_RawSourceRow],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read every durable dispatch attempt for reachable action receipts."""

    idempotency_keys = tuple(
        _text_value(
            row,
            "idempotency_key",
            table="agent_external_action_receipts",
        )
        for row in receipt_rows
    )
    if not idempotency_keys:
        return ()
    return _read_source_rows(
        conn,
        table="agent_external_action_attempts",
        columns=_ATTEMPT_COLUMNS,
        where_sql=(
            "CAST(source.idempotency_key AS BLOB) IN ("
            + ", ".join("?" for _idempotency_key in idempotency_keys)
            + ")"
        ),
        parameters=tuple(idempotency_key.encode("utf-8") for idempotency_key in idempotency_keys),
        order_sql="source.idempotency_key ASC, source.attempt_count ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _read_ledger_rows(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    consumption_rows: Sequence[_RawSourceRow],
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    """Read ledger rows whose consumption pointers affect recovery safety."""

    consumption_ids = tuple(
        _text_value(
            row,
            "consumption_id",
            table="agent_message_ledger_consumptions",
        )
        for row in consumption_rows
    )
    if not consumption_ids:
        return ()
    placeholders = ", ".join("?" for _consumption_id in consumption_ids)
    encoded_ids = tuple(consumption_id.encode("utf-8") for consumption_id in consumption_ids)
    return _read_source_rows(
        conn,
        table="agent_message_ledger",
        columns=_LEDGER_COLUMNS,
        where_sql=(
            _raw_session_scope() + " AND ("
            f"CAST(source.review_consumption_id AS BLOB) IN ({placeholders}) "
            f"OR CAST(source.chat_consumption_id AS BLOB) IN ({placeholders}) "
            "OR CAST(source.high_priority_consumption_id AS BLOB) "
            f"IN ({placeholders}))"
        ),
        parameters=(
            *_raw_session_scope_parameters(
                key,
                ownership_generation=ownership_generation,
            ),
            *encoded_ids,
            *encoded_ids,
            *encoded_ids,
        ),
        order_sql="source.ledger_sequence ASC",
        maximum_rows=MAX_RECOVERY_SOURCE_ROWS,
        budget=budget,
    )


def _validate_delivery_mailbox(
    conn: sqlite3.Connection,
    *,
    payload: RecoveryDeliveryPayload,
    payload_json: str,
    now: float,
    allow_missing: bool,
) -> bool:
    """Validate one immutable mailbox envelope using raw SQLite storage."""

    if len(payload_json.encode("utf-8", errors="strict")) > MAX_RECOVERY_RAW_FIELD_BYTES:
        raise RecoveryGraphReadError(
            "recovery_delivery_payload_too_large",
            evidence={
                "event_id": payload.event_id,
                "maximum_bytes": MAX_RECOVERY_RAW_FIELD_BYTES,
            },
        )
    rows = _read_delivery_mailbox_rows(
        conn,
        profile_id=payload.certificate.subject.profile_id,
        session_id=payload.certificate.subject.session_id,
        event_id=payload.event_id,
    )
    if not rows:
        if allow_missing:
            return False
        raise RecoveryGraphReadError(
            "recovery_delivery_disappeared",
            evidence={"event_id": payload.event_id},
        )
    row = rows[0]
    expected = {
        "event_id": ("text", payload.event_id),
        "profile_id": ("text", payload.certificate.subject.profile_id),
        "session_id": ("text", payload.certificate.subject.session_id),
        "ownership_generation": (
            "integer",
            payload.certificate.subject.ownership_generation,
        ),
        "kind": ("text", RECOVERY_DELIVERY_EVENT_KIND),
        "source": ("text", RECOVERY_DELIVERY_EVENT_SOURCE),
        "occurred_at": ("real", now),
        "payload_json": ("text", payload_json),
        "causation_id": ("text", payload.case_id),
        "correlation_id": ("text", payload.case_id),
        "trace_id": ("text", payload.event_id),
        "created_at": ("real", now),
    }
    for field_name, (storage_class, expected_value) in expected.items():
        _validate_exact_raw_value(
            row,
            field_name=field_name,
            table="agent_session_mailbox",
            storage_class=storage_class,
            expected_value=expected_value,
        )
    decoded, _status = _decode_delivery_mailbox_row(row)
    if decoded.to_record() != payload.to_record():
        raise RecoveryGraphReadError(
            "recovery_delivery_payload_conflict",
            evidence={"event_id": payload.event_id},
        )
    return True


def _read_delivery_mailbox_rows(
    conn: sqlite3.Connection,
    *,
    profile_id: str,
    session_id: str,
    event_id: str,
    columns: tuple[str, ...] = (*_DELIVERY_IMMUTABLE_MAILBOX_COLUMNS, "status"),
) -> tuple[_RawSourceRow, ...]:
    """Read one recovery mailbox logical key without SQLite TEXT coercion."""

    projection = bounded_raw_sqlite_projection(
        "source",
        columns,
        byte_limits=dict.fromkeys(columns, MAX_RECOVERY_RAW_FIELD_BYTES),
    )
    rows = conn.execute(
        f"""
        SELECT source.mailbox_id AS source_row_id,
               {projection}
        FROM agent_session_mailbox AS source
        WHERE CAST(source.profile_id AS BLOB) = ?
          AND CAST(source.session_id AS BLOB) = ?
          AND CAST(source.event_id AS BLOB) = ?
        ORDER BY source.mailbox_id ASC
        LIMIT 2
        """,
        (
            profile_id.encode("utf-8", errors="strict"),
            session_id.encode("utf-8", errors="strict"),
            event_id.encode("utf-8", errors="strict"),
        ),
    ).fetchall()
    if len(rows) > 1:
        raise RecoveryGraphReadError(
            "recovery_delivery_logical_key_duplicate",
            evidence={
                "event_id": event_id,
                "profile_id": profile_id,
                "session_id": session_id,
            },
        )
    projected: list[_RawSourceRow] = []
    for row in rows:
        row_id = _required_positive_int(
            row["source_row_id"],
            field_name="agent_session_mailbox.rowid",
        )
        values = raw_sqlite_values(row, columns)
        for field_name, value in values.items():
            if value.projection_truncated:
                raise RecoveryGraphReadError(
                    "recovery_delivery_field_too_large",
                    evidence={
                        "event_id": event_id,
                        "field": field_name,
                        "row_id": row_id,
                        "value": _truncated_value_evidence(value),
                    },
                )
        projected.append(_RawSourceRow(row_id=row_id, values=values))
    return tuple(projected)


def _read_recovery_case_rows(
    conn: sqlite3.Connection,
    *,
    case_id: str,
) -> tuple[_RawSourceRow, ...]:
    """Read one recovery case logical key without SQLite TEXT coercion."""

    projection = bounded_raw_sqlite_projection(
        "source",
        _RECOVERY_CASE_COLUMNS,
        byte_limits=dict.fromkeys(
            _RECOVERY_CASE_COLUMNS,
            MAX_RECOVERY_RAW_FIELD_BYTES,
        ),
    )
    rows = conn.execute(
        f"""
        SELECT source.rowid AS source_row_id,
               {projection}
        FROM agent_session_recovery_cases AS source
        WHERE CAST(source.case_id AS BLOB) = ?
        ORDER BY source.rowid ASC
        LIMIT 2
        """,
        (case_id.encode("utf-8", errors="strict"),),
    ).fetchall()
    if len(rows) > 1:
        raise RecoveryGraphReadError(
            "recovery_case_logical_key_duplicate",
            evidence={"case_id": case_id},
        )
    projected: list[_RawSourceRow] = []
    for row in rows:
        row_id = _required_positive_int(
            row["source_row_id"],
            field_name="agent_session_recovery_cases.rowid",
        )
        values = raw_sqlite_values(row, _RECOVERY_CASE_COLUMNS)
        for field_name, value in values.items():
            if value.projection_truncated:
                raise RecoveryGraphReadError(
                    "recovery_case_field_too_large",
                    evidence={
                        "case_id": case_id,
                        "field": field_name,
                        "row_id": row_id,
                        "value": _truncated_value_evidence(value),
                    },
                )
        projected.append(_RawSourceRow(row_id=row_id, values=values))
    return tuple(projected)


def _decode_recovery_case_snapshot(row: _RawSourceRow) -> RecoveryCaseSnapshot:
    """Decode one raw case row with all persistent fences intact."""

    table = "agent_session_recovery_cases"
    case_id = _text_value(row, "case_id", table=table)
    case_digest = _recovery_case_digest(row, case_id=case_id, table=table)
    certificate_version = _positive_int_value(row, "certificate_version", table=table)
    if certificate_version != 1:
        raise RecoveryGraphReadError(
            "recovery_case_certificate_version_unsupported",
            evidence={
                "case_id": case_id,
                "certificate_version": certificate_version,
                "row_id": row.row_id,
            },
        )
    profile_id = _text_value(row, "profile_id", table=table)
    session_id = _text_value(row, "session_id", table=table)
    ownership_generation = _positive_int_value(
        row,
        "ownership_generation",
        table=table,
    )
    policy_version = _positive_int_value(row, "policy_version", table=table)
    work_graph_digest = _sha256_digest_value(
        row,
        "work_graph_digest",
        table=table,
    )
    latest_certificate_digest = _sha256_digest_value(
        row,
        "latest_certificate_digest",
        table=table,
    )
    status = _text_value(row, "status", table=table)
    if status not in {
        "open",
        "applied",
        "superseded",
        "delivery_exhausted",
        "scanner_blocked",
    }:
        raise RecoveryGraphReadError(
            "recovery_case_status_invalid",
            evidence={"case_id": case_id, "row_id": row.row_id, "status": status},
        )
    next_delivery_cycle = _nonnegative_int_value(
        row,
        "next_delivery_cycle",
        table=table,
    )
    delivery_count = _nonnegative_int_value(row, "delivery_count", table=table)
    last_event_id = _text_value(
        row,
        "last_event_id",
        table=table,
        required=False,
    )
    last_error = _text_value(row, "last_error", table=table, required=False)
    created_at = _nonnegative_time_value(row, "created_at", table=table)
    updated_at = _nonnegative_time_value(row, "updated_at", table=table)
    if updated_at < created_at:
        raise RecoveryGraphReadError(
            "recovery_case_time_invalid",
            evidence={"case_id": case_id, "row_id": row.row_id},
        )
    if next_delivery_cycle != delivery_count:
        raise RecoveryGraphReadError(
            "recovery_case_delivery_progress_invalid",
            evidence={"case_id": case_id, "row_id": row.row_id},
        )
    if delivery_count == 0:
        if last_event_id:
            raise RecoveryGraphReadError(
                "recovery_case_delivery_progress_invalid",
                evidence={"case_id": case_id, "row_id": row.row_id},
            )
    else:
        expected_event_id = (
            f"recovery-requested:v{certificate_version}:{case_digest}:{delivery_count - 1}"
        )
        if last_event_id != expected_event_id:
            raise RecoveryGraphReadError(
                "recovery_case_delivery_progress_invalid",
                evidence={"case_id": case_id, "row_id": row.row_id},
            )
    if status == "scanner_blocked" and not last_error:
        raise RecoveryGraphReadError(
            "recovery_case_blocker_reason_missing",
            evidence={"case_id": case_id, "row_id": row.row_id},
        )
    return RecoveryCaseSnapshot(
        case_id=case_id,
        profile_id=profile_id,
        session_id=session_id,
        ownership_generation=ownership_generation,
        certificate_version=certificate_version,
        policy_version=policy_version,
        work_graph_digest=work_graph_digest,
        latest_certificate_digest=latest_certificate_digest,
        status=status,
        next_delivery_cycle=next_delivery_cycle,
        delivery_count=delivery_count,
        last_event_id=last_event_id,
        last_error=last_error,
        created_at=created_at,
        updated_at=updated_at,
    )


def _recovery_case_digest(
    row: _RawSourceRow,
    *,
    case_id: str,
    table: str,
) -> str:
    """Return the v1 case digest after validating its persisted identity."""

    prefix = "recovery-case:v1:"
    if len(case_id) != len(prefix) + 64 or not case_id.startswith(prefix):
        raise RecoveryGraphReadError(
            "recovery_case_identity_invalid",
            evidence={"case_id": case_id, "row_id": row.row_id, "table": table},
        )
    digest = case_id[len(prefix) :]
    if any(character not in "0123456789abcdef" for character in digest):
        raise RecoveryGraphReadError(
            "recovery_case_identity_invalid",
            evidence={"case_id": case_id, "row_id": row.row_id, "table": table},
        )
    return digest


def _sha256_digest_value(
    row: _RawSourceRow,
    field_name: str,
    *,
    table: str,
) -> str:
    """Read one lowercase SHA-256 digest with its raw TEXT fence intact."""

    value = _text_value(row, field_name, table=table)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise RecoveryGraphReadError(
            "recovery_case_digest_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
            },
        )
    return value


def _validate_exact_raw_value(
    row: _RawSourceRow,
    *,
    field_name: str,
    table: str,
    storage_class: str,
    expected_value: object,
) -> None:
    """Require one immutable raw SQLite field to exactly match expected data."""

    value = row.values[field_name]
    if value.storage_class != storage_class:
        raise RecoveryGraphReadError(
            "recovery_delivery_storage_class_conflict",
            evidence={
                "expected_storage_class": storage_class,
                "field": field_name,
                "row_id": row.row_id,
                "storage_class": value.storage_class,
                "table": table,
            },
        )
    try:
        actual_value = value.decode()
    except (RawSQLiteValueTruncatedError, UnicodeDecodeError) as exc:
        raise RecoveryGraphReadError(
            "recovery_delivery_raw_value_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
                "value": _truncated_value_evidence(value),
            },
        ) from exc
    if actual_value != expected_value:
        raise RecoveryGraphReadError(
            "recovery_delivery_immutable_value_conflict",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
            },
        )


def _validate_claim_raw_value(
    row: _RawSourceRow,
    *,
    field_name: str,
    table: str,
    storage_class: str,
    expected_value: object,
) -> None:
    """Validate a raw claim field without treating a valid ABA change as corruption."""

    value = row.values[field_name]
    if value.storage_class != storage_class:
        raise RecoveryGraphReadError(
            "recovery_delivery_claim_storage_class_conflict",
            evidence={
                "expected_storage_class": storage_class,
                "field": field_name,
                "row_id": row.row_id,
                "storage_class": value.storage_class,
                "table": table,
            },
        )
    try:
        actual_value = value.decode()
    except (RawSQLiteValueTruncatedError, UnicodeDecodeError) as exc:
        raise RecoveryGraphReadError(
            "recovery_delivery_claim_raw_value_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
                "value": _truncated_value_evidence(value),
            },
        ) from exc
    if actual_value != expected_value:
        raise RecoveryDeliveryClaimLost(f"recovery_delivery_claim_{field_name}_changed")


def _decode_delivery_mailbox_row(
    row: _RawSourceRow,
) -> tuple[RecoveryDeliveryPayload, str]:
    """Decode one raw mailbox row into a verified typed recovery delivery."""

    table = "agent_session_mailbox"
    event_id = _text_value(row, "event_id", table=table)
    profile_id = _text_value(row, "profile_id", table=table)
    session_id = _text_value(row, "session_id", table=table)
    ownership_generation = _positive_int_value(
        row,
        "ownership_generation",
        table=table,
    )
    kind = _text_value(row, "kind", table=table)
    source = _text_value(row, "source", table=table)
    payload_json = _text_value(row, "payload_json", table=table)
    validation = validate_canonical_json_object(payload_json)
    if validation.violations or validation.payload is None:
        raise RecoveryGraphReadError(
            "recovery_delivery_payload_invalid",
            evidence={
                "event_id": event_id,
                "row_id": row.row_id,
                "violations": list(validation.violations),
            },
        )
    try:
        envelope = RecoveryDeliveryEnvelopeIdentity(
            event_id=event_id,
            profile_id=profile_id,
            session_id=session_id,
            ownership_generation=ownership_generation,
            kind=kind,
            source=source,
        )
        payload = decode_recovery_delivery_payload(
            validation.payload,
            envelope=envelope,
        )
    except (RecoveryContractDecodeError, TypeError, ValueError) as exc:
        raise RecoveryGraphReadError(
            "recovery_delivery_payload_invalid",
            evidence={"event_id": event_id, "row_id": row.row_id},
        ) from exc
    for field_name, expected_value in (
        ("causation_id", payload.case_id),
        ("correlation_id", payload.case_id),
        ("trace_id", payload.event_id),
    ):
        actual_value = _text_value(row, field_name, table=table)
        if actual_value != expected_value:
            raise RecoveryGraphReadError(
                "recovery_delivery_envelope_conflict",
                evidence={
                    "event_id": event_id,
                    "field": field_name,
                    "row_id": row.row_id,
                },
            )
    status = _text_value(row, "status", table=table)
    if status not in {"pending", "processing", "completed", "failed"}:
        raise RecoveryGraphReadError(
            "recovery_delivery_status_invalid",
            evidence={"event_id": event_id, "row_id": row.row_id},
        )
    return payload, status


def _read_source_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: tuple[str, ...],
    where_sql: str,
    parameters: Sequence[object],
    order_sql: str,
    maximum_rows: int,
    budget: _RecoveryReadBudget,
) -> tuple[_RawSourceRow, ...]:
    if maximum_rows < 1:
        raise ValueError("maximum_rows must be positive")
    byte_limits = dict.fromkeys(columns, MAX_RECOVERY_RAW_FIELD_BYTES)
    projection = bounded_raw_sqlite_projection(
        "source",
        columns,
        byte_limits=byte_limits,
    )
    rows = conn.execute(
        f"""
        SELECT source.rowid AS source_row_id,
               {projection}
        FROM {table} AS source
        WHERE {where_sql}
        ORDER BY {order_sql}
        LIMIT ?
        """,
        (*parameters, maximum_rows + 1),
    ).fetchall()
    if len(rows) > maximum_rows:
        raise RecoveryGraphReadError(
            "recovery_authority_row_limit_exceeded",
            evidence={
                "maximum_rows": maximum_rows,
                "observed_rows": len(rows),
                "table": table,
            },
        )
    projected: list[_RawSourceRow] = []
    for row in rows:
        row_id = _required_positive_int(
            row["source_row_id"],
            field_name=f"{table}.rowid",
        )
        values = raw_sqlite_values(row, columns)
        for field_name, value in values.items():
            if value.projection_truncated:
                raise RecoveryGraphReadError(
                    "recovery_authority_field_too_large",
                    evidence={
                        "field": field_name,
                        "row_id": row_id,
                        "table": table,
                        "value": _truncated_value_evidence(value),
                    },
                )
        budget.consume(table=table, row_id=row_id, values=values)
        projected.append(_RawSourceRow(row_id=row_id, values=values))
    return tuple(projected)


def _live_or_referenced_where(
    *,
    base: str,
    base_parameters: Sequence[object],
    status_column: str,
    live_statuses: Sequence[str],
    identity_column: str,
    identities: Sequence[str],
) -> tuple[str, tuple[object, ...]]:
    placeholders = ", ".join("?" for _status in live_statuses)
    where_sql = f"{base} AND (CAST(source.{status_column} AS BLOB) IN ({placeholders})"
    parameters: list[object] = [
        *base_parameters,
        *(status.encode("utf-8", errors="strict") for status in live_statuses),
    ]
    if identities:
        identity_placeholders = ", ".join("?" for _identity in identities)
        where_sql += f" OR CAST(source.{identity_column} AS BLOB) IN ({identity_placeholders})"
        parameters.extend(identity.encode("utf-8", errors="strict") for identity in identities)
    where_sql += ")"
    return where_sql, tuple(parameters)


def _raw_session_scope(*, include_generation: bool = True) -> str:
    """Return a storage-aware SQL predicate for one actor authority scope."""

    scope = "CAST(source.profile_id AS BLOB) = ? AND CAST(source.session_id AS BLOB) = ?"
    if include_generation:
        scope += " AND CAST(source.ownership_generation AS BLOB) = ?"
    return scope


def _raw_session_scope_parameters(
    key: SessionKey,
    *,
    ownership_generation: int | None = None,
) -> tuple[object, ...]:
    """Encode one canonical actor key for :func:`_raw_session_scope`."""

    parameters: list[object] = [
        key.profile_id.encode("utf-8", errors="strict"),
        key.session_id.encode("utf-8", errors="strict"),
    ]
    if ownership_generation is not None:
        parameters.append(str(ownership_generation).encode("ascii"))
    return tuple(parameters)


def _validate_source_json(table: str, row: _RawSourceRow) -> None:
    for field_name in _JSON_FIELDS_BY_TABLE.get(table, frozenset()):
        value = row.values[field_name]
        if value.storage_class != "text":
            raise RecoveryGraphReadError(
                "recovery_authority_json_storage_class_invalid",
                evidence={
                    "field": field_name,
                    "row_id": row.row_id,
                    "storage_class": value.storage_class,
                    "table": table,
                },
            )
        try:
            decoded = value.decode()
        except (RawSQLiteValueTruncatedError, UnicodeDecodeError) as exc:
            raise RecoveryGraphReadError(
                "recovery_authority_json_invalid_utf8",
                evidence={
                    "field": field_name,
                    "row_id": row.row_id,
                    "table": table,
                    "value": _truncated_value_evidence(value),
                },
            ) from exc
        if not isinstance(decoded, str):
            raise RecoveryGraphReadError(
                "recovery_authority_json_storage_class_invalid",
                evidence={
                    "field": field_name,
                    "row_id": row.row_id,
                    "storage_class": value.storage_class,
                    "table": table,
                },
            )
        validation = validate_canonical_json_object(decoded)
        if validation.violations:
            raise RecoveryGraphReadError(
                "recovery_authority_json_invalid",
                evidence={
                    "field": field_name,
                    "row_id": row.row_id,
                    "table": table,
                    "violations": list(validation.violations),
                },
            )


def _source_json_object(
    row: _RawSourceRow,
    field_name: str,
    *,
    table: str,
) -> Mapping[str, object]:
    """Return a previously validated canonical JSON object without coercion."""

    value = _text_value(row, field_name, table=table)
    validation = validate_canonical_json_object(value)
    if validation.violations or validation.payload is None:
        raise RecoveryGraphReadError(
            "recovery_authority_json_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
                "violations": list(validation.violations),
            },
        )
    return validation.payload


def _json_text(value: object) -> str:
    """Return canonical JSON text only when it is safe as an authority key."""

    if not isinstance(value, str) or value != value.strip():
        return ""
    return value


def _json_nonnegative_int(value: object) -> int | None:
    """Read an exact non-negative JSON integer without affinity coercion."""

    if type(value) is not int or value < 0:
        return None
    return value


def _optional_nonnegative_int_value(
    row: _RawSourceRow,
    field_name: str,
    *,
    table: str,
) -> int | None:
    """Read one nullable integer authority field without affinity coercion."""

    value = row.values[field_name]
    if value.storage_class == "null":
        return None
    return _nonnegative_int_value(row, field_name, table=table)


def _optional_nonnegative_time_value(
    row: _RawSourceRow,
    field_name: str,
    *,
    table: str,
) -> float | None:
    """Read one nullable finite timestamp without SQLite affinity coercion."""

    value = row.values[field_name]
    if value.storage_class == "null":
        return None
    if value.storage_class not in {"integer", "real"}:
        raise RecoveryGraphReadError(
            "recovery_authority_time_storage_class_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "storage_class": value.storage_class,
                "table": table,
            },
        )
    decoded = value.decode()
    if (
        isinstance(decoded, bool)
        or not isinstance(decoded, (int, float))
        or not math.isfinite(float(decoded))
        or float(decoded) < 0
    ):
        raise RecoveryGraphReadError(
            "recovery_authority_time_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
            },
        )
    return float(decoded)


def _nonnegative_time_value(
    row: _RawSourceRow,
    field_name: str,
    *,
    table: str,
) -> float:
    """Read one required finite timestamp without SQLite affinity coercion."""

    result = _optional_nonnegative_time_value(row, field_name, table=table)
    if result is not None:
        return result
    raise RecoveryGraphReadError(
        "recovery_authority_time_missing",
        evidence={
            "field": field_name,
            "row_id": row.row_id,
            "table": table,
        },
    )


def _require_null_value(
    row: _RawSourceRow,
    *,
    field_name: str,
    table: str,
) -> None:
    """Require one nullable authority field to remain a raw SQLite NULL."""

    value = row.values[field_name]
    if value.storage_class == "null":
        return
    raise RecoveryGraphReadError(
        "recovery_authority_null_storage_class_invalid",
        evidence={
            "field": field_name,
            "row_id": row.row_id,
            "storage_class": value.storage_class,
            "table": table,
        },
    )


def _state_operation_shape(
    *,
    state: str,
    top_operation_ids: Mapping[str, str],
    active_chat_state: Mapping[str, object],
) -> tuple[dict[str, str], tuple[str, ...]]:
    """Return the only operation roots valid for one non-idle state shape."""

    expected_by_state = {
        "review": ("review",),
        "active_reply": ("active_reply",),
        "active_chat_settling": ("idle_review_planning",),
        "active_chat": ("active_chat_round",),
    }
    expected_kinds = expected_by_state.get(state, ())
    roles: dict[str, str] = {}
    unexpected: list[str] = []
    for operation_kind, operation_id in top_operation_ids.items():
        if not operation_id:
            continue
        if operation_kind not in expected_kinds:
            unexpected.append(operation_id)
            continue
        previous = roles.setdefault(operation_id, operation_kind)
        if previous != operation_kind:
            unexpected.append(operation_id)
    bootstrap_operation_id = _json_text(active_chat_state.get("bootstrap_operation_id"))
    if state == "active_chat" and bootstrap_operation_id:
        previous = roles.setdefault(bootstrap_operation_id, "active_chat_bootstrap")
        if previous != "active_chat_bootstrap":
            unexpected.append(bootstrap_operation_id)
    return roles, tuple(dict.fromkeys(unexpected))


def _node_from_row(
    *,
    identity: str,
    kind: str,
    authority: str,
    status: str,
    row: _RawSourceRow,
    semantic_excluded_fields: frozenset[str] = frozenset(),
) -> RecoveryGraphNode:
    facts: dict[str, object] = {"raw_digest": _row_digest(authority, row)}
    if semantic_excluded_fields:
        facts["semantic_digest"] = _row_digest(
            authority,
            row,
            excluded_fields=semantic_excluded_fields,
        )
    return RecoveryGraphNode(
        identity=identity,
        kind=kind,
        authority=authority,
        status=status,
        facts=facts,
    )


def _append_row_nodes(
    nodes: list[RecoveryGraphNode],
    edges: list[RecoveryGraphEdge],
    *,
    table: str,
    kind: str,
    id_column: str,
    rows: Sequence[_RawSourceRow],
    status_column: str | None = "status",
    static_status: str = "",
) -> dict[str, str]:
    """Append deterministic graph nodes for one raw persistence projection."""

    if status_column is None:
        status = _required_text(static_status, field_name="static_status")
    identities: dict[str, str] = {}
    for row in rows:
        record_id = _text_value(row, id_column, table=table)
        if status_column is not None:
            status = _text_value(row, status_column, table=table)
        node_identity = f"{kind}:{record_id}"
        if record_id in identities:
            raise RecoveryGraphReadError(
                "recovery_authority_duplicate_identity",
                evidence={
                    "field": id_column,
                    "record_id": record_id,
                    "table": table,
                },
            )
        nodes.append(
            _node_from_row(
                identity=node_identity,
                kind=kind,
                authority=table,
                status=status,
                row=row,
            )
        )
        edges.append(
            RecoveryGraphEdge(
                identity=f"edge:aggregate:{node_identity}",
                source="aggregate",
                target=node_identity,
                relation="observes",
            )
        )
        identities[record_id] = node_identity
    return identities


def _review_cancellation_gate_identity(row: _RawSourceRow) -> str:
    """Return the generation-scoped durable identity for one cancellation gate."""

    return ":".join(
        (
            str(
                _positive_int_value(
                    row,
                    "ownership_generation",
                    table="agent_review_cancellation_gates",
                )
            ),
            _text_value(
                row,
                "cancellation_effect_id",
                table="agent_review_cancellation_gates",
            ),
        )
    )


def _append_review_cancellation_gate_nodes(
    nodes: list[RecoveryGraphNode],
    edges: list[RecoveryGraphEdge],
    *,
    rows: Sequence[_RawSourceRow],
) -> dict[str, str]:
    """Append generation-scoped nodes for every unresolved cancellation gate."""

    identities: dict[str, str] = {}
    for row in rows:
        record_id = _review_cancellation_gate_identity(row)
        if record_id in identities:
            raise RecoveryGraphReadError(
                "recovery_authority_duplicate_identity",
                evidence={
                    "field": "ownership_generation:cancellation_effect_id",
                    "record_id": record_id,
                    "table": "agent_review_cancellation_gates",
                },
            )
        node_identity = f"review_cancellation_gate:{record_id}"
        nodes.append(
            _node_from_row(
                identity=node_identity,
                kind="review_cancellation_gate",
                authority="agent_review_cancellation_gates",
                status=_text_value(
                    row,
                    "gate_status",
                    table="agent_review_cancellation_gates",
                ),
                row=row,
            )
        )
        edges.append(
            RecoveryGraphEdge(
                identity=f"edge:aggregate:{node_identity}",
                source="aggregate",
                target=node_identity,
                relation="observes",
            )
        )
        identities[record_id] = node_identity
    return identities


def _review_execution_run_identity(row: _RawSourceRow) -> str:
    """Return the composite durable identity for one review task witness."""

    return ":".join(
        (
            str(
                _positive_int_value(
                    row,
                    "ownership_generation",
                    table="agent_review_execution_runs",
                )
            ),
            _text_value(
                row,
                "review_effect_id",
                table="agent_review_execution_runs",
            ),
            _text_value(
                row,
                "claim_id",
                table="agent_review_execution_runs",
            ),
        )
    )


def _append_review_execution_run_nodes(
    nodes: list[RecoveryGraphNode],
    edges: list[RecoveryGraphEdge],
    *,
    rows: Sequence[_RawSourceRow],
) -> dict[str, str]:
    """Append composite-key nodes for durable running review task witnesses."""

    identities: dict[str, str] = {}
    for row in rows:
        record_id = _review_execution_run_identity(row)
        if record_id in identities:
            raise RecoveryGraphReadError(
                "recovery_authority_duplicate_identity",
                evidence={
                    "field": "review_effect_id:claim_id",
                    "record_id": record_id,
                    "table": "agent_review_execution_runs",
                },
            )
        node_identity = f"review_execution_run:{record_id}"
        nodes.append(
            _node_from_row(
                identity=node_identity,
                kind="review_execution_run",
                authority="agent_review_execution_runs",
                status=_text_value(
                    row,
                    "execution_status",
                    table="agent_review_execution_runs",
                ),
                row=row,
            )
        )
        edges.append(
            RecoveryGraphEdge(
                identity=f"edge:aggregate:{node_identity}",
                source="aggregate",
                target=node_identity,
                relation="observes",
            )
        )
        identities[record_id] = node_identity
    return identities


def _model_execution_cancellation_gate_identity(row: _RawSourceRow) -> str:
    """Return the generation-scoped durable identity for one generic gate."""

    return ":".join(
        (
            str(
                _positive_int_value(
                    row,
                    "ownership_generation",
                    table="agent_model_execution_cancellation_gates",
                )
            ),
            _text_value(
                row,
                "cancellation_effect_id",
                table="agent_model_execution_cancellation_gates",
            ),
        )
    )


def _append_model_execution_cancellation_gate_nodes(
    nodes: list[RecoveryGraphNode],
    edges: list[RecoveryGraphEdge],
    *,
    rows: Sequence[_RawSourceRow],
) -> dict[str, str]:
    """Append graph nodes for unresolved generic model cancellation gates."""

    identities: dict[str, str] = {}
    for row in rows:
        record_id = _model_execution_cancellation_gate_identity(row)
        if record_id in identities:
            raise RecoveryGraphReadError(
                "recovery_authority_duplicate_identity",
                evidence={
                    "field": "ownership_generation:cancellation_effect_id",
                    "record_id": record_id,
                    "table": "agent_model_execution_cancellation_gates",
                },
            )
        node_identity = f"model_execution_cancellation_gate:{record_id}"
        nodes.append(
            _node_from_row(
                identity=node_identity,
                kind="model_execution_cancellation_gate",
                authority="agent_model_execution_cancellation_gates",
                status=_text_value(
                    row,
                    "gate_status",
                    table="agent_model_execution_cancellation_gates",
                ),
                row=row,
            )
        )
        edges.append(
            RecoveryGraphEdge(
                identity=f"edge:aggregate:{node_identity}",
                source="aggregate",
                target=node_identity,
                relation="observes",
            )
        )
        identities[record_id] = node_identity
    return identities


def _model_execution_run_identity(row: _RawSourceRow) -> str:
    """Return the composite durable identity for one non-review model witness."""

    return ":".join(
        (
            str(
                _positive_int_value(
                    row,
                    "ownership_generation",
                    table="agent_model_execution_runs",
                )
            ),
            _text_value(
                row,
                "effect_id",
                table="agent_model_execution_runs",
            ),
            _text_value(
                row,
                "claim_id",
                table="agent_model_execution_runs",
            ),
        )
    )


def _append_model_execution_run_nodes(
    nodes: list[RecoveryGraphNode],
    edges: list[RecoveryGraphEdge],
    *,
    rows: Sequence[_RawSourceRow],
) -> dict[str, str]:
    """Append composite-key nodes for generic model execution witnesses."""

    identities: dict[str, str] = {}
    for row in rows:
        record_id = _model_execution_run_identity(row)
        if record_id in identities:
            raise RecoveryGraphReadError(
                "recovery_authority_duplicate_identity",
                evidence={
                    "field": "effect_id:claim_id",
                    "record_id": record_id,
                    "table": "agent_model_execution_runs",
                },
            )
        node_identity = f"model_execution_run:{record_id}"
        nodes.append(
            _node_from_row(
                identity=node_identity,
                kind="model_execution_run",
                authority="agent_model_execution_runs",
                status=_text_value(
                    row,
                    "execution_status",
                    table="agent_model_execution_runs",
                ),
                row=row,
            )
        )
        edges.append(
            RecoveryGraphEdge(
                identity=f"edge:aggregate:{node_identity}",
                source="aggregate",
                target=node_identity,
                relation="observes",
            )
        )
        identities[record_id] = node_identity
    return identities


def _append_reachable_edges(
    edges: list[RecoveryGraphEdge],
    *,
    ownership_generation: int,
    operation_rows: Sequence[_RawSourceRow],
    operation_nodes: Mapping[str, str],
    effect_rows: Sequence[_RawSourceRow],
    effect_nodes: Mapping[str, str],
    schedule_event_rows: Sequence[_RawSourceRow],
    schedule_event_nodes: Mapping[str, str],
    schedule_nodes: Mapping[str, str],
    consumption_rows: Sequence[_RawSourceRow],
    consumption_nodes: Mapping[str, str],
    ledger_rows: Sequence[_RawSourceRow],
    ledger_nodes: Mapping[str, str],
    receipt_rows: Sequence[_RawSourceRow],
    receipt_nodes: Mapping[str, str],
    attempt_rows: Sequence[_RawSourceRow],
    attempt_nodes: Mapping[str, str],
    review_gate_rows: Sequence[_RawSourceRow],
    review_gate_nodes: Mapping[str, str],
    review_execution_run_rows: Sequence[_RawSourceRow],
    review_execution_run_nodes: Mapping[str, str],
    model_cancellation_gate_rows: Sequence[_RawSourceRow],
    model_cancellation_gate_nodes: Mapping[str, str],
    model_execution_run_rows: Sequence[_RawSourceRow],
    model_execution_run_nodes: Mapping[str, str],
) -> None:
    """Connect source rows through their durable foreign-key identities."""

    existing = {edge.identity for edge in edges}

    def link(*, source: str, target: str, relation: str) -> None:
        identity = "edge:recovery:" + canonical_recovery_digest(
            {"relation": relation, "source": source, "target": target}
        )
        if identity in existing:
            return
        existing.add(identity)
        edges.append(
            RecoveryGraphEdge(
                identity=identity,
                source=source,
                target=target,
                relation=relation,
            )
        )

    for row in effect_rows:
        operation_id = _text_value(
            row,
            "operation_id",
            table="agent_effect_outbox",
            required=False,
        )
        effect_id = _text_value(row, "effect_id", table="agent_effect_outbox")
        if operation_id and operation_id in operation_nodes:
            link(
                source=operation_nodes[operation_id],
                target=effect_nodes[effect_id],
                relation="emits",
            )

    for row in review_gate_rows:
        if (
            _positive_int_value(
                row,
                "ownership_generation",
                table="agent_review_cancellation_gates",
            )
            != ownership_generation
        ):
            continue
        cancellation_effect_id = _text_value(
            row,
            "cancellation_effect_id",
            table="agent_review_cancellation_gates",
        )
        gate_node = review_gate_nodes[_review_cancellation_gate_identity(row)]
        review_operation_id = _text_value(
            row,
            "review_operation_id",
            table="agent_review_cancellation_gates",
        )
        if review_operation_id in operation_nodes:
            link(
                source=gate_node,
                target=operation_nodes[review_operation_id],
                relation="cancels_operation",
            )
        review_effect_id = _text_value(
            row,
            "review_effect_id",
            table="agent_review_cancellation_gates",
        )
        if review_effect_id in effect_nodes:
            link(
                source=gate_node,
                target=effect_nodes[review_effect_id],
                relation="cancels_effect",
            )
        if cancellation_effect_id in effect_nodes:
            link(
                source=gate_node,
                target=effect_nodes[cancellation_effect_id],
                relation="awaits_cancellation_control",
            )

    for row in review_execution_run_rows:
        if (
            _positive_int_value(
                row,
                "ownership_generation",
                table="agent_review_execution_runs",
            )
            != ownership_generation
        ):
            continue
        run_node = review_execution_run_nodes[_review_execution_run_identity(row)]
        review_effect_id = _text_value(
            row,
            "review_effect_id",
            table="agent_review_execution_runs",
        )
        if review_effect_id in effect_nodes:
            link(
                source=run_node,
                target=effect_nodes[review_effect_id],
                relation="executes",
            )
        review_operation_id = _text_value(
            row,
            "review_operation_id",
            table="agent_review_execution_runs",
        )
        if review_operation_id in operation_nodes:
            link(
                source=run_node,
                target=operation_nodes[review_operation_id],
                relation="executes_for_operation",
            )

    for row in model_cancellation_gate_rows:
        if (
            _positive_int_value(
                row,
                "ownership_generation",
                table="agent_model_execution_cancellation_gates",
            )
            != ownership_generation
        ):
            continue
        cancellation_effect_id = _text_value(
            row,
            "cancellation_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        gate_node = model_cancellation_gate_nodes[
            _model_execution_cancellation_gate_identity(row)
        ]
        target_operation_id = _text_value(
            row,
            "target_operation_id",
            table="agent_model_execution_cancellation_gates",
        )
        if target_operation_id in operation_nodes:
            link(
                source=gate_node,
                target=operation_nodes[target_operation_id],
                relation="cancels_operation",
            )
        target_effect_id = _text_value(
            row,
            "target_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        if target_effect_id in effect_nodes:
            link(
                source=gate_node,
                target=effect_nodes[target_effect_id],
                relation="cancels_effect",
            )
        if cancellation_effect_id in effect_nodes:
            link(
                source=gate_node,
                target=effect_nodes[cancellation_effect_id],
                relation="awaits_cancellation_control",
            )

    for row in model_execution_run_rows:
        if (
            _positive_int_value(
                row,
                "ownership_generation",
                table="agent_model_execution_runs",
            )
            != ownership_generation
        ):
            continue
        run_node = model_execution_run_nodes[_model_execution_run_identity(row)]
        effect_id = _text_value(
            row,
            "effect_id",
            table="agent_model_execution_runs",
        )
        if effect_id in effect_nodes:
            link(
                source=run_node,
                target=effect_nodes[effect_id],
                relation="executes",
            )
        operation_id = _text_value(
            row,
            "operation_id",
            table="agent_model_execution_runs",
        )
        if operation_id in operation_nodes:
            link(
                source=run_node,
                target=operation_nodes[operation_id],
                relation="executes_for_operation",
            )

    for row in schedule_event_rows:
        event_id = _text_value(
            row,
            "schedule_event_id",
            table="agent_review_schedule_events",
        )
        event_node = schedule_event_nodes[event_id]
        for field_name, relation in (
            ("plan_id", "records"),
            ("previous_plan_id", "supersedes"),
        ):
            plan_id = _text_value(
                row,
                field_name,
                table="agent_review_schedule_events",
                required=False,
            )
            if plan_id and plan_id in schedule_nodes:
                link(
                    source=event_node,
                    target=schedule_nodes[plan_id],
                    relation=relation,
                )
        operation_id = _text_value(
            row,
            "operation_id",
            table="agent_review_schedule_events",
            required=False,
        )
        if operation_id and operation_id in operation_nodes:
            link(
                source=operation_nodes[operation_id],
                target=event_node,
                relation="journals",
            )

    for row in consumption_rows:
        consumption_id = _text_value(
            row,
            "consumption_id",
            table="agent_message_ledger_consumptions",
        )
        operation_id = _text_value(
            row,
            "operation_id",
            table="agent_message_ledger_consumptions",
        )
        if operation_id in operation_nodes:
            link(
                source=operation_nodes[operation_id],
                target=consumption_nodes[consumption_id],
                relation="consumes",
            )

    for row in ledger_rows:
        ledger_id = _text_value(
            row,
            "source_event_id",
            table="agent_message_ledger",
        )
        ledger_node = ledger_nodes[ledger_id]
        for field_name, relation in (
            ("review_consumption_id", "marks_review_input"),
            ("chat_consumption_id", "marks_chat_input"),
            ("high_priority_consumption_id", "marks_high_priority_input"),
        ):
            consumption_id = _text_value(
                row,
                field_name,
                table="agent_message_ledger",
                required=False,
            )
            if consumption_id and consumption_id in consumption_nodes:
                link(
                    source=consumption_nodes[consumption_id],
                    target=ledger_node,
                    relation=relation,
                )

    for row in receipt_rows:
        idempotency_key = _text_value(
            row,
            "idempotency_key",
            table="agent_external_action_receipts",
        )
        effect_id = _text_value(
            row,
            "effect_id",
            table="agent_external_action_receipts",
        )
        if effect_id in effect_nodes:
            link(
                source=effect_nodes[effect_id],
                target=receipt_nodes[idempotency_key],
                relation="dispatches",
            )

    for row in attempt_rows:
        claim_id = _text_value(
            row,
            "claim_id",
            table="agent_external_action_attempts",
        )
        idempotency_key = _text_value(
            row,
            "idempotency_key",
            table="agent_external_action_attempts",
        )
        if idempotency_key in receipt_nodes:
            link(
                source=receipt_nodes[idempotency_key],
                target=attempt_nodes[claim_id],
                relation="attempts",
            )


def _validate_graph_limits(
    *,
    nodes: Sequence[RecoveryGraphNode],
    edges: Sequence[RecoveryGraphEdge],
    invariants: Sequence[RecoveryInvariant],
) -> None:
    """Reject a projection that cannot fit the bounded recovery contract."""

    limits = (
        ("nodes", len(nodes), MAX_RECOVERY_GRAPH_NODES),
        ("edges", len(edges), MAX_RECOVERY_GRAPH_EDGES),
        ("invariants", len(invariants), MAX_RECOVERY_INVARIANTS),
    )
    for field_name, observed, maximum in limits:
        if observed > maximum:
            raise RecoveryGraphReadError(
                f"recovery_graph_{field_name}_limit_exceeded",
                evidence={
                    "maximum": maximum,
                    "observed": observed,
                    "projection": field_name,
                },
            )


def _classify_work(
    *,
    key: SessionKey,
    aggregate_node_id: str,
    aggregate_state: str,
    aggregate_data: Mapping[str, object],
    active_chat_state: Mapping[str, object],
    active_reply_resume: Mapping[str, object],
    ownership_generation: int,
    operation_roles: Mapping[str, str],
    unexpected_operation_ids: Sequence[str],
    operation_ids: Sequence[str],
    operation_rows: Sequence[_RawSourceRow],
    operation_nodes: Mapping[str, str],
    mailbox_nodes: Mapping[str, str],
    effect_rows: Sequence[_RawSourceRow],
    effect_nodes: Mapping[str, str],
    schedule_rows: Sequence[_RawSourceRow],
    schedule_nodes: Mapping[str, str],
    receipt_rows: Sequence[_RawSourceRow],
    receipt_nodes: Mapping[str, str],
    route_nodes: Mapping[str, str],
    schedule_event_rows: Sequence[_RawSourceRow],
    schedule_event_nodes: Mapping[str, str],
    transition_rows: Sequence[_RawSourceRow],
    transition_nodes: Mapping[str, str],
    aggregate_fence: RecoveryAggregateFence,
    consumption_rows: Sequence[_RawSourceRow],
    consumption_nodes: Mapping[str, str],
    ledger_rows: Sequence[_RawSourceRow],
    ledger_nodes: Mapping[str, str],
    attempt_rows: Sequence[_RawSourceRow],
    attempt_nodes: Mapping[str, str],
    review_gate_rows: Sequence[_RawSourceRow],
    review_gate_nodes: Mapping[str, str],
    review_execution_run_rows: Sequence[_RawSourceRow],
    review_execution_run_nodes: Mapping[str, str],
    model_cancellation_gate_rows: Sequence[_RawSourceRow],
    model_cancellation_gate_nodes: Mapping[str, str],
    model_execution_run_rows: Sequence[_RawSourceRow],
    model_execution_run_nodes: Mapping[str, str],
) -> tuple[RecoveryWorkClassification, tuple[RecoveryInvariant, ...]]:
    blocking_reasons: list[str] = []
    blocking_nodes: list[str] = []
    waiting_reasons: list[str] = []
    waiting_nodes: list[str] = []
    orphaned_nodes: list[str] = []
    invariants: list[RecoveryInvariant] = []

    review_liveness = _classify_review_execution_liveness(
        key=key,
        ownership_generation=ownership_generation,
        operation_rows=operation_rows,
        operation_nodes=operation_nodes,
        effect_rows=effect_rows,
        effect_nodes=effect_nodes,
        review_gate_rows=review_gate_rows,
        review_gate_nodes=review_gate_nodes,
        review_execution_run_rows=review_execution_run_rows,
        review_execution_run_nodes=review_execution_run_nodes,
    )
    blocking_reasons.extend(review_liveness.blocking_reason_codes)
    blocking_nodes.extend(review_liveness.blocking_node_identities)
    waiting_reasons.extend(review_liveness.waiting_reason_codes)
    waiting_nodes.extend(review_liveness.waiting_node_identities)
    invariants.extend(review_liveness.invariants)

    model_liveness = _classify_model_execution_liveness(
        key=key,
        ownership_generation=ownership_generation,
        operation_rows=operation_rows,
        operation_nodes=operation_nodes,
        effect_rows=effect_rows,
        effect_nodes=effect_nodes,
        model_cancellation_gate_rows=model_cancellation_gate_rows,
        model_cancellation_gate_nodes=model_cancellation_gate_nodes,
        model_execution_run_rows=model_execution_run_rows,
        model_execution_run_nodes=model_execution_run_nodes,
    )
    blocking_reasons.extend(model_liveness.blocking_reason_codes)
    blocking_nodes.extend(model_liveness.blocking_node_identities)
    waiting_reasons.extend(model_liveness.waiting_reason_codes)
    waiting_nodes.extend(model_liveness.waiting_node_identities)
    invariants.extend(model_liveness.invariants)

    # Outbound authority is never inferred during recovery. The one exception
    # is a completed active-chat exit control in ``active_chat_settling``: its
    # state-specific materializer can prove that the process has no planner
    # result to replay and settle through a fixed fallback policy.
    for field_name in (
        "pending_outbound_actions",
        "outbound_continuation",
        "outbound_blocked",
        "review_cancellation_blocked",
    ):
        value = aggregate_data.get(field_name)
        if value not in (None, {}, [], ""):
            code = f"aggregate_{field_name}_requires_state_materializer"
            blocking_reasons.append(code)
            blocking_nodes.append(aggregate_node_id)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_aggregates",
                    node_identity=aggregate_node_id,
                )
            )

    raw_bootstrap_operation_id = active_chat_state.get("bootstrap_operation_id")
    bootstrap_status = _json_text(active_chat_state.get("bootstrap_status"))
    if aggregate_state == "active_chat" and (
        raw_bootstrap_operation_id is not None
        and (
            not isinstance(raw_bootstrap_operation_id, str)
            or raw_bootstrap_operation_id != raw_bootstrap_operation_id.strip()
        )
    ):
        code = "active_chat_bootstrap_operation_id_invalid"
        blocking_reasons.append(code)
        blocking_nodes.append(aggregate_node_id)
        invariants.append(
            RecoveryInvariant(
                identity=f"invariant:{code}",
                code=code,
                severity=RecoveryInvariantSeverity.BLOCKING,
                authority="agent_session_aggregates",
                node_identity=aggregate_node_id,
            )
        )

    for operation_id in unexpected_operation_ids:
        code = "aggregate_state_has_unexpected_operation"
        blocking_reasons.append(code)
        blocking_nodes.append(aggregate_node_id)
        invariants.append(
            RecoveryInvariant(
                identity=f"invariant:{code}:{operation_id}",
                code=code,
                severity=RecoveryInvariantSeverity.BLOCKING,
                authority="agent_session_aggregates",
                node_identity=aggregate_node_id,
                details={"operation_id": operation_id},
            )
        )

    operation_fences = aggregate_data.get("operation_fences")
    if operation_ids and not isinstance(operation_fences, Mapping):
        code = "aggregate_operation_fences_missing"
        blocking_reasons.append(code)
        blocking_nodes.append(aggregate_node_id)
        invariants.append(
            RecoveryInvariant(
                identity=f"invariant:{code}",
                code=code,
                severity=RecoveryInvariantSeverity.BLOCKING,
                authority="agent_session_aggregates",
                node_identity=aggregate_node_id,
            )
        )
        operation_fences = {}

    if aggregate_state == "active_chat_settling":
        settling_metadata_code = _settling_recovery_metadata_code(
            aggregate_data=aggregate_data,
            aggregate_fence=aggregate_fence,
            ownership_generation=ownership_generation,
            operation_roles=operation_roles,
            operation_fences=operation_fences,
        )
        if settling_metadata_code:
            blocking_reasons.append(settling_metadata_code)
            blocking_nodes.append(aggregate_node_id)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{settling_metadata_code}",
                    code=settling_metadata_code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_aggregates",
                    node_identity=aggregate_node_id,
                )
            )
    elif aggregate_state == "active_chat":
        active_chat_metadata_code = _active_chat_recovery_metadata_code(
            aggregate_data=aggregate_data,
            aggregate_fence=aggregate_fence,
            ownership_generation=ownership_generation,
            operation_roles=operation_roles,
        )
        if active_chat_metadata_code:
            blocking_reasons.append(active_chat_metadata_code)
            blocking_nodes.append(aggregate_node_id)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{active_chat_metadata_code}",
                    code=active_chat_metadata_code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_aggregates",
                    node_identity=aggregate_node_id,
                )
            )
        idle_exit = aggregate_data.get("idle_exit")
        if idle_exit not in (None, {}, [], ""):
            code = "aggregate_idle_exit_requires_state_materializer"
            blocking_reasons.append(code)
            blocking_nodes.append(aggregate_node_id)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_aggregates",
                    node_identity=aggregate_node_id,
                )
            )
    else:
        for field_name in ("effect_control_intents", "idle_exit"):
            value = aggregate_data.get(field_name)
            if value not in (None, {}, [], ""):
                code = f"aggregate_{field_name}_requires_state_materializer"
                blocking_reasons.append(code)
                blocking_nodes.append(aggregate_node_id)
                invariants.append(
                    RecoveryInvariant(
                        identity=f"invariant:{code}",
                        code=code,
                        severity=RecoveryInvariantSeverity.BLOCKING,
                        authority="agent_session_aggregates",
                        node_identity=aggregate_node_id,
                    )
                )

    if mailbox_nodes:
        waiting_reasons.append("pending_mailbox")
        waiting_nodes.extend(mailbox_nodes.values())
    live_effect_nodes = [
        effect_nodes[_text_value(row, "effect_id", table="agent_effect_outbox")]
        for row in effect_rows
        if _text_value(row, "status", table="agent_effect_outbox") in {"pending", "processing"}
    ]
    if live_effect_nodes:
        waiting_reasons.append("live_effect")
        waiting_nodes.extend(live_effect_nodes)
    for row in effect_rows:
        effect_id = _text_value(row, "effect_id", table="agent_effect_outbox")
        operation_id = _text_value(
            row,
            "operation_id",
            table="agent_effect_outbox",
            required=False,
        )
        if operation_id and operation_id not in operation_nodes:
            code = "effect_references_missing_operation"
            blocking_reasons.append(code)
            blocking_nodes.append(effect_nodes[effect_id])
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{effect_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_effect_outbox",
                    node_identity=effect_nodes[effect_id],
                    details={"operation_id": operation_id},
                )
            )
    live_schedule_nodes = [
        schedule_nodes[_text_value(row, "plan_id", table="agent_review_schedules")]
        for row in schedule_rows
        if _text_value(row, "status", table="agent_review_schedules") in {"scheduled", "claimed"}
    ]
    if live_schedule_nodes:
        waiting_reasons.append("live_review_schedule")
        waiting_nodes.extend(live_schedule_nodes)
    if route_nodes:
        waiting_reasons.append("pending_route_delivery")
        waiting_nodes.extend(route_nodes.values())

    attempts_by_receipt: dict[str, list[_RawSourceRow]] = {}
    for row in attempt_rows:
        idempotency_key = _text_value(
            row,
            "idempotency_key",
            table="agent_external_action_attempts",
        )
        claim_id = _text_value(
            row,
            "claim_id",
            table="agent_external_action_attempts",
        )
        node_identity = attempt_nodes[claim_id]
        attempts_by_receipt.setdefault(idempotency_key, []).append(row)
        if idempotency_key not in receipt_nodes:
            code = "external_action_attempt_references_missing_receipt"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{claim_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_external_action_attempts",
                    node_identity=node_identity,
                    details={"idempotency_key": idempotency_key},
                )
            )
        attempt_status = _text_value(
            row,
            "status",
            table="agent_external_action_attempts",
        )
        if attempt_status in {"executing", "unknown"}:
            code = f"external_action_attempt_{attempt_status}"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{claim_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_external_action_attempts",
                    node_identity=node_identity,
                )
            )

    for row in receipt_rows:
        receipt_id = _text_value(
            row,
            "idempotency_key",
            table="agent_external_action_receipts",
        )
        status = _text_value(row, "status", table="agent_external_action_receipts")
        node_identity = receipt_nodes[receipt_id]
        attempt_count = _nonnegative_int_value(
            row,
            "attempt_count",
            table="agent_external_action_receipts",
        )
        attempts = attempts_by_receipt.get(receipt_id, [])
        effect_id = _text_value(
            row,
            "effect_id",
            table="agent_external_action_receipts",
        )
        if effect_id not in effect_nodes:
            code = "external_action_receipt_references_missing_effect"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{receipt_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_external_action_receipts",
                    node_identity=node_identity,
                    details={"effect_id": effect_id},
                )
            )
        if status == "prepared" and attempts:
            code = "prepared_external_action_has_attempt"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{receipt_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_external_action_receipts",
                    node_identity=node_identity,
                )
            )
        if status != "prepared" and not attempts:
            code = "external_action_receipt_attempt_missing"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{receipt_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_external_action_receipts",
                    node_identity=node_identity,
                )
            )
        if attempt_count < len(attempts):
            code = "external_action_attempt_count_conflict"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{receipt_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_external_action_receipts",
                    node_identity=node_identity,
                )
            )
        if status in {"executing", "unknown"}:
            code = f"external_action_{status}"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{receipt_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_external_action_receipts",
                    node_identity=node_identity,
                )
            )
        elif status == "prepared":
            waiting_reasons.append("prepared_external_action")
            waiting_nodes.append(node_identity)

    ledger_consumption_ids: set[str] = set()
    for row in ledger_rows:
        ledger_id = _text_value(
            row,
            "source_event_id",
            table="agent_message_ledger",
        )
        node_identity = ledger_nodes[ledger_id]
        for field_name in (
            "review_consumption_id",
            "chat_consumption_id",
            "high_priority_consumption_id",
        ):
            consumption_id = _text_value(
                row,
                field_name,
                table="agent_message_ledger",
                required=False,
            )
            if not consumption_id:
                continue
            ledger_consumption_ids.add(consumption_id)
            if consumption_id not in consumption_nodes:
                code = "message_ledger_references_missing_consumption"
                blocking_reasons.append(code)
                blocking_nodes.append(node_identity)
                invariants.append(
                    RecoveryInvariant(
                        identity=f"invariant:{code}:{ledger_id}:{field_name}",
                        code=code,
                        severity=RecoveryInvariantSeverity.BLOCKING,
                        authority="agent_message_ledger",
                        node_identity=node_identity,
                        details={"consumption_id": consumption_id},
                    )
                )

    for row in consumption_rows:
        consumption_id = _text_value(
            row,
            "consumption_id",
            table="agent_message_ledger_consumptions",
        )
        operation_id = _text_value(
            row,
            "operation_id",
            table="agent_message_ledger_consumptions",
        )
        node_identity = consumption_nodes[consumption_id]
        if operation_id not in operation_nodes:
            code = "message_consumption_references_missing_operation"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{consumption_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_message_ledger_consumptions",
                    node_identity=node_identity,
                    details={"operation_id": operation_id},
                )
            )
        if consumption_id not in ledger_consumption_ids:
            canonical = _source_json_object(
                row,
                "canonical_json",
                table="agent_message_ledger_consumptions",
            )
            if _json_text(canonical.get("selection")) == "explicit_ids":
                code = "message_consumption_ledger_projection_missing"
                blocking_reasons.append(code)
                blocking_nodes.append(node_identity)
                invariants.append(
                    RecoveryInvariant(
                        identity=f"invariant:{code}:{consumption_id}",
                        code=code,
                        severity=RecoveryInvariantSeverity.BLOCKING,
                        authority="agent_message_ledger_consumptions",
                        node_identity=node_identity,
                    )
                )

    if schedule_event_rows and not schedule_event_nodes:
        raise AssertionError("schedule event rows must produce graph nodes")

    if aggregate_fence.event_sequence > 0 and len(transition_rows) != 1:
        code = "aggregate_transition_tail_missing"
        blocking_reasons.append(code)
        blocking_nodes.append(aggregate_node_id)
        invariants.append(
            RecoveryInvariant(
                identity=f"invariant:{code}:{aggregate_fence.event_sequence}",
                code=code,
                severity=RecoveryInvariantSeverity.BLOCKING,
                authority="agent_state_transitions",
                node_identity=aggregate_node_id,
            )
        )
    for row in transition_rows:
        transition_id = _text_value(
            row,
            "transition_id",
            table="agent_state_transitions",
        )
        node_identity = transition_nodes[transition_id]
        if (
            _text_value(row, "to_state", table="agent_state_transitions") != aggregate_fence.state
            or _nonnegative_int_value(
                row,
                "state_revision",
                table="agent_state_transitions",
            )
            != aggregate_fence.state_revision
            or _nonnegative_int_value(
                row,
                "event_sequence",
                table="agent_state_transitions",
            )
            != aggregate_fence.event_sequence
        ):
            code = "aggregate_transition_tail_conflict"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{transition_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_state_transitions",
                    node_identity=node_identity,
                )
            )

    operation_rows_by_id = {
        _text_value(row, "operation_id", table="agent_session_operations"): row
        for row in operation_rows
    }
    operation_statuses = {
        operation_id: _text_value(
            row,
            "status",
            table="agent_session_operations",
        )
        for operation_id, row in operation_rows_by_id.items()
    }
    for operation_id in operation_ids:
        node_identity = operation_nodes.get(operation_id)
        if node_identity is None:
            code = "aggregate_references_missing_operation"
            blocking_reasons.append(code)
            blocking_nodes.append(aggregate_node_id)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{operation_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_aggregates",
                    node_identity=aggregate_node_id,
                    details={"operation_id": operation_id},
                )
            )
            continue
        operation_row = operation_rows_by_id[operation_id]
        expected_kind = operation_roles.get(operation_id)
        actual_kind = _text_value(
            operation_row,
            "kind",
            table="agent_session_operations",
        )
        raw_fence = operation_fences.get(operation_id) if operation_fences else None
        if not isinstance(raw_fence, Mapping):
            code = "aggregate_operation_fence_missing"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{operation_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_aggregates",
                    node_identity=node_identity,
                    details={"operation_id": operation_id},
                )
            )
        else:
            operation_watermark = _optional_nonnegative_int_value(
                operation_row,
                "input_watermark",
                table="agent_session_operations",
            )
            operation_sequence = _optional_nonnegative_int_value(
                operation_row,
                "input_ledger_sequence",
                table="agent_session_operations",
            )
            fence_watermark = _json_nonnegative_int(raw_fence.get("input_watermark"))
            fence_sequence = _json_nonnegative_int(raw_fence.get("input_ledger_sequence"))
            fence_conflict = (
                _json_text(raw_fence.get("operation_id")) != operation_id
                or _json_nonnegative_int(raw_fence.get("ownership_generation"))
                != ownership_generation
                or _json_text(raw_fence.get("operation_kind")) not in {"", actual_kind}
                or (operation_watermark, operation_sequence) != (fence_watermark, fence_sequence)
            )
            if fence_conflict:
                code = "aggregate_operation_fence_conflict"
                blocking_reasons.append(code)
                blocking_nodes.append(node_identity)
                invariants.append(
                    RecoveryInvariant(
                        identity=f"invariant:{code}:{operation_id}",
                        code=code,
                        severity=RecoveryInvariantSeverity.BLOCKING,
                        authority="agent_session_aggregates",
                        node_identity=node_identity,
                        details={"operation_id": operation_id},
                    )
                )
        if expected_kind is None or actual_kind != expected_kind:
            code = "aggregate_operation_kind_conflict"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{operation_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_operations",
                    node_identity=node_identity,
                    details={
                        "expected_kind": expected_kind or "",
                        "operation_id": operation_id,
                    },
                )
            )
        operation_status = operation_statuses[operation_id]
        lease_owner = _text_value(
            operation_row,
            "lease_owner",
            table="agent_session_operations",
            required=False,
        )
        lease_until = _optional_nonnegative_time_value(
            operation_row,
            "lease_until",
            table="agent_session_operations",
        )
        if operation_status == "running" and (lease_owner or lease_until is not None):
            if bool(lease_owner) != (lease_until is not None):
                code = "running_operation_lease_incomplete"
                blocking_reasons.append(code)
                blocking_nodes.append(node_identity)
                invariants.append(
                    RecoveryInvariant(
                        identity=f"invariant:{code}:{operation_id}",
                        code=code,
                        severity=RecoveryInvariantSeverity.BLOCKING,
                        authority="agent_session_operations",
                        node_identity=node_identity,
                    )
                )
            else:
                waiting_reasons.append("running_operation_lease")
                waiting_nodes.append(node_identity)
        elif operation_status == "pending" and (lease_owner or lease_until is not None):
            code = "pending_operation_has_lease"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{operation_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_operations",
                    node_identity=node_identity,
                )
            )
        elif operation_status in {"pending", "running"}:
            orphaned_nodes.append(node_identity)
        else:
            code = "aggregate_references_terminal_operation"
            blocking_reasons.append(code)
            blocking_nodes.append(node_identity)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{operation_id}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_operations",
                    node_identity=node_identity,
                    details={"operation_id": operation_id},
                )
            )

    requires_operation = aggregate_state in {
        "review",
        "active_reply",
        "active_chat_settling",
    }
    if requires_operation and not operation_ids:
        code = "aggregate_state_requires_operation"
        blocking_reasons.append(code)
        blocking_nodes.append(aggregate_node_id)
        invariants.append(
            RecoveryInvariant(
                identity=f"invariant:{code}:{aggregate_state}",
                code=code,
                severity=RecoveryInvariantSeverity.BLOCKING,
                authority="agent_session_aggregates",
                node_identity=aggregate_node_id,
                details={"state": aggregate_state},
            )
        )
    elif aggregate_state == "active_chat" and not operation_ids:
        # A completed bootstrap with no round is the one known quiescent
        # non-idle shape. Unknown bootstrap state remains a blocker.
        if bootstrap_status != "completed":
            code = "active_chat_unrecognized_quiescent_shape"
            blocking_reasons.append(code)
            blocking_nodes.append(aggregate_node_id)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{code}:{bootstrap_status}",
                    code=code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_aggregates",
                    node_identity=aggregate_node_id,
                    details={"bootstrap_status": bootstrap_status},
                )
            )
    elif not operation_ids:
        code = "aggregate_state_unrecognized_without_operation"
        blocking_reasons.append(code)
        blocking_nodes.append(aggregate_node_id)
        invariants.append(
            RecoveryInvariant(
                identity=f"invariant:{code}:{aggregate_state}",
                code=code,
                severity=RecoveryInvariantSeverity.BLOCKING,
                authority="agent_session_aggregates",
                node_identity=aggregate_node_id,
                details={"state": aggregate_state},
            )
        )

    if operation_ids:
        state_shape_code = _state_recovery_shape_code(
            aggregate_state=aggregate_state,
            aggregate_data=aggregate_data,
            active_chat_state=active_chat_state,
            active_reply_resume=active_reply_resume,
            aggregate_fence=aggregate_fence,
            ownership_generation=ownership_generation,
            operation_roles=operation_roles,
            operation_rows=operation_rows,
            effect_rows=effect_rows,
            receipt_rows=receipt_rows,
        )
        if state_shape_code:
            blocking_reasons.append(state_shape_code)
            blocking_nodes.append(aggregate_node_id)
            invariants.append(
                RecoveryInvariant(
                    identity=f"invariant:{state_shape_code}",
                    code=state_shape_code,
                    severity=RecoveryInvariantSeverity.BLOCKING,
                    authority="agent_session_aggregates",
                    node_identity=aggregate_node_id,
                )
            )

    return (
        RecoveryWorkClassification(
            blocking_reason_codes=tuple(dict.fromkeys(blocking_reasons)),
            blocking_node_identities=tuple(dict.fromkeys(blocking_nodes)),
            waiting_reason_codes=tuple(dict.fromkeys(waiting_reasons)),
            waiting_node_identities=tuple(dict.fromkeys(waiting_nodes)),
            orphaned_node_identities=tuple(dict.fromkeys(orphaned_nodes)),
        ),
        tuple(invariants),
    )


def _classify_model_execution_liveness(
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_rows: Sequence[_RawSourceRow],
    operation_nodes: Mapping[str, str],
    effect_rows: Sequence[_RawSourceRow],
    effect_nodes: Mapping[str, str],
    model_cancellation_gate_rows: Sequence[_RawSourceRow],
    model_cancellation_gate_nodes: Mapping[str, str],
    model_execution_run_rows: Sequence[_RawSourceRow],
    model_execution_run_nodes: Mapping[str, str],
) -> _ModelExecutionLiveness:
    """Classify non-review model liveness without assuming lease expiry is safe.

    A generic model witness protects workflows that have no cancellation gate.
    A v3 gate may cancel its exact outbox target while a remote witness is
    still running; that is a durable wait, not a stale claim conflict.
    ``unknown`` remains a hard recovery blocker until an explicit
    reconciliation protocol exists.
    """

    blocking_reasons: list[str] = []
    blocking_nodes: list[str] = []
    waiting_reasons: list[str] = []
    waiting_nodes: list[str] = []
    invariants: list[RecoveryInvariant] = []
    operation_by_id = {
        _text_value(row, "operation_id", table="agent_session_operations"): row
        for row in operation_rows
    }
    effect_by_id = {
        _text_value(row, "effect_id", table="agent_effect_outbox"): row
        for row in effect_rows
    }
    runs_by_effect_id: dict[str, list[_RawSourceRow]] = {}
    gates_by_effect_id: dict[str, list[_RawSourceRow]] = {}

    def block(
        code: str,
        *,
        node_identity: str,
        authority: str,
        token: str,
        details: Mapping[str, object] | None = None,
    ) -> None:
        blocking_reasons.append(code)
        blocking_nodes.append(node_identity)
        invariants.append(
            RecoveryInvariant(
                identity=f"invariant:{code}:{token}",
                code=code,
                severity=RecoveryInvariantSeverity.BLOCKING,
                authority=authority,
                node_identity=node_identity,
                details=dict(details or {}),
            )
        )

    def wait(code: str, *, node_identity: str) -> None:
        waiting_reasons.append(code)
        waiting_nodes.append(node_identity)

    current_gate_rows: list[_RawSourceRow] = []
    for row in model_cancellation_gate_rows:
        gate_identity = _model_execution_cancellation_gate_identity(row)
        node_identity = model_cancellation_gate_nodes[gate_identity]
        gate_generation = _positive_int_value(
            row,
            "ownership_generation",
            table="agent_model_execution_cancellation_gates",
        )
        if (
            _text_value(
                row,
                "profile_id",
                table="agent_model_execution_cancellation_gates",
            )
            != key.profile_id
            or _text_value(
                row,
                "session_id",
                table="agent_model_execution_cancellation_gates",
            )
            != key.session_id
        ):
            block(
                "model_execution_cancellation_gate_scope_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=gate_identity,
            )
        elif gate_generation != ownership_generation:
            block(
                "model_execution_cancellation_gate_generation_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=gate_identity,
                details={
                    "expected_generation": ownership_generation,
                    "observed_generation": gate_generation,
                },
            )
        else:
            current_gate_rows.append(row)

    for row in current_gate_rows:
        target_effect_id = _text_value(
            row,
            "target_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        gates_by_effect_id.setdefault(target_effect_id, []).append(row)

    current_rows: list[_RawSourceRow] = []
    for row in model_execution_run_rows:
        run_identity = _model_execution_run_identity(row)
        node_identity = model_execution_run_nodes[run_identity]
        run_generation = _positive_int_value(
            row,
            "ownership_generation",
            table="agent_model_execution_runs",
        )
        if (
            _text_value(row, "profile_id", table="agent_model_execution_runs")
            != key.profile_id
            or _text_value(row, "session_id", table="agent_model_execution_runs")
            != key.session_id
        ):
            block(
                "model_execution_witness_scope_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_runs",
                token=run_identity,
            )
        elif run_generation != ownership_generation:
            block(
                "model_execution_witness_generation_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_runs",
                token=run_identity,
                details={
                    "expected_generation": ownership_generation,
                    "observed_generation": run_generation,
                },
            )
        else:
            current_rows.append(row)

    for row in current_rows:
        effect_id = _text_value(row, "effect_id", table="agent_model_execution_runs")
        runs_by_effect_id.setdefault(effect_id, []).append(row)
    for effect_id, runs in runs_by_effect_id.items():
        if len(runs) < 2:
            continue
        for row in runs:
            run_identity = _model_execution_run_identity(row)
            block(
                "model_execution_witness_multiple_running",
                node_identity=model_execution_run_nodes[run_identity],
                authority="agent_model_execution_runs",
                token=run_identity,
                details={"effect_id": effect_id},
            )

    for row in current_rows:
        run_identity = _model_execution_run_identity(row)
        node_identity = model_execution_run_nodes[run_identity]
        effect_id = _text_value(row, "effect_id", table="agent_model_execution_runs")
        operation_id = _text_value(row, "operation_id", table="agent_model_execution_runs")
        effect_kind = _text_value(row, "effect_kind", table="agent_model_execution_runs")
        _positive_int_value(row, "contract_version", table="agent_model_execution_runs")
        _text_value(row, "contract_signature", table="agent_model_execution_runs")
        claim_id = _text_value(row, "claim_id", table="agent_model_execution_runs")
        worker_id = _text_value(row, "worker_id", table="agent_model_execution_runs")
        execution_status = _text_value(
            row,
            "execution_status",
            table="agent_model_execution_runs",
        )
        _nonnegative_time_value(row, "started_at", table="agent_model_execution_runs")
        finished_at = _optional_nonnegative_time_value(
            row,
            "finished_at",
            table="agent_model_execution_runs",
        )
        unknown_at = _optional_nonnegative_time_value(
            row,
            "unknown_at",
            table="agent_model_execution_runs",
        )
        unknown_reason = _text_value(
            row,
            "unknown_reason",
            table="agent_model_execution_runs",
            required=False,
        )
        if execution_status == "unknown":
            if finished_at is not None or unknown_at is None or not unknown_reason:
                block(
                    "model_execution_witness_unknown_evidence_conflict",
                    node_identity=node_identity,
                    authority="agent_model_execution_runs",
                    token=run_identity,
                )
        elif (
            execution_status != "running"
            or finished_at is not None
            or unknown_at is not None
            or unknown_reason
        ):
            block(
                "model_execution_witness_status_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_runs",
                token=run_identity,
            )
        expected_operation_kind = _MODEL_EXECUTION_OPERATION_KINDS.get(effect_kind)
        if expected_operation_kind is None:
            block(
                "model_execution_witness_kind_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_runs",
                token=run_identity,
                details={"effect_kind": effect_kind},
            )

        effect = effect_by_id.get(effect_id)
        if effect is None:
            block(
                "model_execution_witness_target_missing",
                node_identity=node_identity,
                authority="agent_model_execution_runs",
                token=run_identity,
                details={"effect_id": effect_id},
            )
        elif not _model_execution_run_matches_effect(
            row,
            effect,
            ownership_generation=ownership_generation,
        ):
            block(
                "model_execution_witness_target_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_runs",
                token=run_identity,
                details={"effect_id": effect_id},
            )
        else:
            effect_status = _text_value(effect, "status", table="agent_effect_outbox")
            if effect_status == "processing":
                if (
                    _text_value(
                        effect,
                        "claim_id",
                        table="agent_effect_outbox",
                        required=False,
                    )
                    != claim_id
                    or _text_value(
                        effect,
                        "lease_owner",
                        table="agent_effect_outbox",
                        required=False,
                    )
                    != worker_id
                    or _optional_nonnegative_time_value(
                        effect,
                        "lease_until",
                        table="agent_effect_outbox",
                    )
                    is None
                ):
                    block(
                        "model_execution_witness_claim_conflict",
                        node_identity=node_identity,
                        authority="agent_effect_outbox",
                        token=run_identity,
                    )
            elif effect_status == "cancelled":
                gates = gates_by_effect_id.get(effect_id, [])
                if not (
                    len(gates) == 1
                    and _text_value(
                        gates[0],
                        "gate_status",
                        table="agent_model_execution_cancellation_gates",
                    )
                    == "cancelled"
                ):
                    block(
                        "model_execution_witness_cancelled_without_gate",
                        node_identity=node_identity,
                        authority="agent_model_execution_runs",
                        token=run_identity,
                    )
            else:
                block(
                    "model_execution_witness_target_status_conflict",
                    node_identity=node_identity,
                    authority="agent_effect_outbox",
                    token=run_identity,
                    details={"status": effect_status},
                )

        operation = operation_by_id.get(operation_id)
        if operation is None:
            block(
                "model_execution_witness_operation_missing",
                node_identity=node_identity,
                authority="agent_model_execution_runs",
                token=run_identity,
                details={"operation_id": operation_id},
            )
        elif (
            _positive_int_value(
                operation,
                "ownership_generation",
                table="agent_session_operations",
            )
            != ownership_generation
            or _text_value(operation, "kind", table="agent_session_operations")
            != expected_operation_kind
        ):
            block(
                "model_execution_witness_operation_conflict",
                node_identity=node_identity,
                authority="agent_session_operations",
                token=run_identity,
                details={"operation_id": operation_id},
            )

        if execution_status == "unknown":
            block(
                "model_execution_witness_unknown",
                node_identity=node_identity,
                authority="agent_model_execution_runs",
                token=run_identity,
            )
        else:
            wait("model_execution_witness_running", node_identity=node_identity)

    for row in current_gate_rows:
        gate_identity = _model_execution_cancellation_gate_identity(row)
        node_identity = model_cancellation_gate_nodes[gate_identity]
        cancellation_effect_id = _text_value(
            row,
            "cancellation_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        target_effect_id = _text_value(
            row,
            "target_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        target_operation_id = _text_value(
            row,
            "target_operation_id",
            table="agent_model_execution_cancellation_gates",
        )
        target_effect_kind = _text_value(
            row,
            "target_effect_kind",
            table="agent_model_execution_cancellation_gates",
        )
        target_contract_version = _positive_int_value(
            row,
            "target_contract_version",
            table="agent_model_execution_cancellation_gates",
        )
        target_contract_signature = _text_value(
            row,
            "target_contract_signature",
            table="agent_model_execution_cancellation_gates",
        )
        target_effect_status = _text_value(
            row,
            "target_effect_status",
            table="agent_model_execution_cancellation_gates",
        )
        target_execution_status = _text_value(
            row,
            "target_execution_status",
            table="agent_model_execution_cancellation_gates",
        )
        gate_status = _text_value(
            row,
            "gate_status",
            table="agent_model_execution_cancellation_gates",
        )
        target_claim_id = _text_value(
            row,
            "target_claim_id",
            table="agent_model_execution_cancellation_gates",
            required=False,
        )
        target_worker_id = _text_value(
            row,
            "target_worker_id",
            table="agent_model_execution_cancellation_gates",
            required=False,
        )
        target_terminal_at = _optional_nonnegative_time_value(
            row,
            "target_effect_terminal_at",
            table="agent_model_execution_cancellation_gates",
        )
        blocker_code = _text_value(
            row,
            "blocker_code",
            table="agent_model_execution_cancellation_gates",
            required=False,
        )
        created_at = _nonnegative_time_value(
            row,
            "created_at",
            table="agent_model_execution_cancellation_gates",
        )
        updated_at = _nonnegative_time_value(
            row,
            "updated_at",
            table="agent_model_execution_cancellation_gates",
        )
        expected_operation_kind = _MODEL_EXECUTION_OPERATION_KINDS.get(
            target_effect_kind
        )
        if updated_at < created_at:
            block(
                "model_execution_cancellation_gate_time_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=cancellation_effect_id,
            )
        try:
            target_contract = builtin_effect_contract(
                target_effect_kind,
                version=target_contract_version,
            )
        except KeyError:
            target_contract = None
        if (
            expected_operation_kind is None
            or target_contract is None
            or target_contract.signature != target_contract_signature
        ):
            block(
                "model_execution_cancellation_gate_target_contract_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=cancellation_effect_id,
            )

        effect = effect_by_id.get(target_effect_id)
        if effect is None:
            block(
                "model_execution_cancellation_gate_target_missing",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=cancellation_effect_id,
                details={"target_effect_id": target_effect_id},
            )
        elif not _model_execution_cancellation_gate_matches_effect(
            row,
            effect,
            ownership_generation=ownership_generation,
        ):
            block(
                "model_execution_cancellation_gate_target_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=cancellation_effect_id,
                details={"target_effect_id": target_effect_id},
            )
        else:
            actual_status = _text_value(effect, "status", table="agent_effect_outbox")
            actual_attempt_count = _nonnegative_int_value(
                effect,
                "attempt_count",
                table="agent_effect_outbox",
            )
            expected_attempt_count = _nonnegative_int_value(
                row,
                "target_effect_attempt_count",
                table="agent_model_execution_cancellation_gates",
            )
            effect_claim_id = _text_value(
                effect,
                "claim_id",
                table="agent_effect_outbox",
                required=False,
            )
            effect_lease_owner = _text_value(
                effect,
                "lease_owner",
                table="agent_effect_outbox",
                required=False,
            )
            effect_lease_until = _optional_nonnegative_time_value(
                effect,
                "lease_until",
                table="agent_effect_outbox",
            )
            completed_at = _optional_nonnegative_time_value(
                effect,
                "completed_at",
                table="agent_effect_outbox",
            )
            if (
                actual_status != target_effect_status
                or actual_attempt_count != expected_attempt_count
            ):
                block(
                    "model_execution_cancellation_gate_target_state_conflict",
                    node_identity=node_identity,
                    authority="agent_model_execution_cancellation_gates",
                    token=cancellation_effect_id,
                )
            if gate_status == "requested":
                if (
                    actual_status != "processing"
                    or target_execution_status not in {"none", "running"}
                    or target_terminal_at is not None
                    or blocker_code
                    or not target_claim_id
                    or not target_worker_id
                    or effect_claim_id != target_claim_id
                    or effect_lease_owner != target_worker_id
                    or effect_lease_until is None
                ):
                    block(
                        "model_execution_cancellation_gate_requested_state_conflict",
                        node_identity=node_identity,
                        authority="agent_model_execution_cancellation_gates",
                        token=cancellation_effect_id,
                    )
            elif gate_status == "cancelled":
                if (
                    actual_status != "cancelled"
                    or target_execution_status != "running"
                    or target_terminal_at is None
                    or completed_at != target_terminal_at
                    or blocker_code
                    or not target_claim_id
                    or not target_worker_id
                    or effect_claim_id
                    or effect_lease_owner
                    or effect_lease_until is not None
                ):
                    block(
                        "model_execution_cancellation_gate_cancelled_state_conflict",
                        node_identity=node_identity,
                        authority="agent_model_execution_cancellation_gates",
                        token=cancellation_effect_id,
                    )
            elif gate_status == "blocked":
                if (
                    actual_status != "processing"
                    or target_execution_status != "unknown"
                    or target_terminal_at is not None
                    or not blocker_code
                    or not target_claim_id
                    or not target_worker_id
                    or effect_claim_id != target_claim_id
                    or effect_lease_owner != target_worker_id
                    or effect_lease_until is None
                ):
                    block(
                        "model_execution_cancellation_gate_blocked_state_conflict",
                        node_identity=node_identity,
                        authority="agent_model_execution_cancellation_gates",
                        token=cancellation_effect_id,
                    )
            elif gate_status == "terminal":
                if (
                    actual_status not in {"completed", "failed", "cancelled"}
                    or target_execution_status not in {"none", "finished"}
                    or target_terminal_at is None
                    or completed_at != target_terminal_at
                    or blocker_code
                    or effect_lease_owner
                    or effect_lease_until is not None
                    or (
                        actual_status == "cancelled"
                        and effect_claim_id
                    )
                    or (
                        actual_status in {"completed", "failed"}
                        and effect_claim_id != target_claim_id
                    )
                ):
                    block(
                        "model_execution_cancellation_gate_terminal_state_conflict",
                        node_identity=node_identity,
                        authority="agent_model_execution_cancellation_gates",
                        token=cancellation_effect_id,
                    )
            else:
                block(
                    "model_execution_cancellation_gate_status_conflict",
                    node_identity=node_identity,
                    authority="agent_model_execution_cancellation_gates",
                    token=cancellation_effect_id,
                    details={"status": gate_status},
                )

        operation = operation_by_id.get(target_operation_id)
        if operation is None:
            block(
                "model_execution_cancellation_gate_operation_missing",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=cancellation_effect_id,
                details={"target_operation_id": target_operation_id},
            )
        elif (
            _positive_int_value(
                operation,
                "ownership_generation",
                table="agent_session_operations",
            )
            != ownership_generation
            or _text_value(operation, "kind", table="agent_session_operations")
            != expected_operation_kind
            or _text_value(operation, "status", table="agent_session_operations")
            != "superseded"
        ):
            block(
                "model_execution_cancellation_gate_operation_conflict",
                node_identity=node_identity,
                authority="agent_session_operations",
                token=cancellation_effect_id,
                details={"target_operation_id": target_operation_id},
            )

        control_effect = effect_by_id.get(cancellation_effect_id)
        if control_effect is None:
            block(
                "model_execution_cancellation_gate_control_missing",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=cancellation_effect_id,
            )
        elif not _model_execution_cancellation_control_matches_gate(
            row,
            control_effect,
            ownership_generation=ownership_generation,
        ):
            block(
                "model_execution_cancellation_gate_control_conflict",
                node_identity=node_identity,
                authority="agent_effect_outbox",
                token=cancellation_effect_id,
            )
        else:
            control_status = _text_value(
                control_effect,
                "status",
                table="agent_effect_outbox",
            )
            if control_status in {"pending", "processing"}:
                wait("model_execution_cancellation_control_live", node_identity=node_identity)
            elif control_status not in {"completed", "failed", "cancelled"}:
                block(
                    "model_execution_cancellation_gate_control_status_conflict",
                    node_identity=node_identity,
                    authority="agent_effect_outbox",
                    token=cancellation_effect_id,
                    details={"status": control_status},
                )
            elif gate_status in {"requested", "cancelled"}:
                block(
                    "model_execution_cancellation_gate_unresolved_after_control_terminal",
                    node_identity=node_identity,
                    authority="agent_model_execution_cancellation_gates",
                    token=cancellation_effect_id,
                )

        runs = runs_by_effect_id.get(target_effect_id, [])
        if target_execution_status == "none":
            if runs:
                block(
                    "model_execution_cancellation_gate_witness_conflict",
                    node_identity=node_identity,
                    authority="agent_model_execution_runs",
                    token=cancellation_effect_id,
                )
        elif target_execution_status in {"running", "unknown"}:
            if (
                len(runs) != 1
                or _text_value(
                    runs[0],
                    "execution_status",
                    table="agent_model_execution_runs",
                )
                != target_execution_status
                or not _model_execution_cancellation_gate_matches_run(row, runs[0])
            ):
                block(
                    "model_execution_cancellation_gate_witness_conflict",
                    node_identity=node_identity,
                    authority="agent_model_execution_runs",
                    token=cancellation_effect_id,
                )
        elif target_execution_status == "finished":
            if runs:
                block(
                    "model_execution_cancellation_gate_witness_conflict",
                    node_identity=node_identity,
                    authority="agent_model_execution_runs",
                    token=cancellation_effect_id,
                )
        else:
            block(
                "model_execution_cancellation_gate_execution_status_conflict",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=cancellation_effect_id,
                details={"status": target_execution_status},
            )

        if gate_status in {"requested", "cancelled"}:
            wait(
                f"model_execution_cancellation_gate_{gate_status}",
                node_identity=node_identity,
            )
        elif gate_status == "blocked":
            block(
                "model_execution_cancellation_gate_blocked",
                node_identity=node_identity,
                authority="agent_model_execution_cancellation_gates",
                token=cancellation_effect_id,
                details={"blocker_code": blocker_code},
            )

    return _ModelExecutionLiveness(
        blocking_reason_codes=tuple(dict.fromkeys(blocking_reasons)),
        blocking_node_identities=tuple(dict.fromkeys(blocking_nodes)),
        waiting_reason_codes=tuple(dict.fromkeys(waiting_reasons)),
        waiting_node_identities=tuple(dict.fromkeys(waiting_nodes)),
        invariants=tuple(invariants),
    )


def _model_execution_run_matches_effect(
    run: _RawSourceRow,
    effect: _RawSourceRow,
    *,
    ownership_generation: int,
) -> bool:
    """Return whether one generic witness retains its immutable outbox fence."""

    return (
        _positive_int_value(
            effect,
            "ownership_generation",
            table="agent_effect_outbox",
        )
        == ownership_generation
        and _text_value(run, "effect_id", table="agent_model_execution_runs")
        == _text_value(effect, "effect_id", table="agent_effect_outbox")
        and _text_value(run, "operation_id", table="agent_model_execution_runs")
        == _text_value(effect, "operation_id", table="agent_effect_outbox")
        and _text_value(run, "effect_kind", table="agent_model_execution_runs")
        == _text_value(effect, "kind", table="agent_effect_outbox")
        and _positive_int_value(
            run,
            "contract_version",
            table="agent_model_execution_runs",
        )
        == _positive_int_value(
            effect,
            "contract_version",
            table="agent_effect_outbox",
        )
        and _text_value(run, "contract_signature", table="agent_model_execution_runs")
        == _text_value(effect, "contract_signature", table="agent_effect_outbox")
    )


def _model_execution_cancellation_gate_matches_effect(
    gate: _RawSourceRow,
    effect: _RawSourceRow,
    *,
    ownership_generation: int,
) -> bool:
    """Return whether a generic gate retains its exact target outbox fence."""

    return (
        _positive_int_value(
            effect,
            "ownership_generation",
            table="agent_effect_outbox",
        )
        == ownership_generation
        and _text_value(
            gate,
            "target_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(effect, "effect_id", table="agent_effect_outbox")
        and _text_value(
            gate,
            "target_operation_id",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(effect, "operation_id", table="agent_effect_outbox")
        and _text_value(
            gate,
            "target_effect_kind",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(effect, "kind", table="agent_effect_outbox")
        and _positive_int_value(
            gate,
            "target_contract_version",
            table="agent_model_execution_cancellation_gates",
        )
        == _positive_int_value(
            effect,
            "contract_version",
            table="agent_effect_outbox",
        )
        and _text_value(
            gate,
            "target_contract_signature",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(effect, "contract_signature", table="agent_effect_outbox")
    )


def _model_execution_cancellation_gate_matches_run(
    gate: _RawSourceRow,
    run: _RawSourceRow,
) -> bool:
    """Return whether one witness matches the generic gate's frozen claim."""

    return (
        _text_value(
            gate,
            "target_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(run, "effect_id", table="agent_model_execution_runs")
        and _text_value(
            gate,
            "target_operation_id",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(run, "operation_id", table="agent_model_execution_runs")
        and _text_value(
            gate,
            "target_effect_kind",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(run, "effect_kind", table="agent_model_execution_runs")
        and _positive_int_value(
            gate,
            "target_contract_version",
            table="agent_model_execution_cancellation_gates",
        )
        == _positive_int_value(
            run,
            "contract_version",
            table="agent_model_execution_runs",
        )
        and _text_value(
            gate,
            "target_contract_signature",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(run, "contract_signature", table="agent_model_execution_runs")
        and _text_value(
            gate,
            "target_claim_id",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(run, "claim_id", table="agent_model_execution_runs")
        and _text_value(
            gate,
            "target_worker_id",
            table="agent_model_execution_cancellation_gates",
        )
        == _text_value(run, "worker_id", table="agent_model_execution_runs")
    )


def _model_execution_cancellation_control_matches_gate(
    gate: _RawSourceRow,
    effect: _RawSourceRow,
    *,
    ownership_generation: int,
) -> bool:
    """Return whether a v3 control effect retains the gate's exact target fence."""

    control_contract = builtin_effect_contract("cancel_model_execution", version=3)
    if (
        _positive_int_value(
            effect,
            "ownership_generation",
            table="agent_effect_outbox",
        )
        != ownership_generation
        or _text_value(effect, "kind", table="agent_effect_outbox")
        != "cancel_model_execution"
        or _positive_int_value(
            effect,
            "contract_version",
            table="agent_effect_outbox",
        )
        != control_contract.version
        or _text_value(effect, "contract_signature", table="agent_effect_outbox")
        != control_contract.signature
        or _text_value(effect, "effect_id", table="agent_effect_outbox")
        != _text_value(
            gate,
            "cancellation_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        or _text_value(effect, "idempotency_key", table="agent_effect_outbox")
        != _text_value(
            gate,
            "cancellation_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        or _text_value(effect, "event_id", table="agent_effect_outbox")
        != _text_value(
            gate,
            "request_event_id",
            table="agent_model_execution_cancellation_gates",
        )
        or _text_value(effect, "operation_id", table="agent_effect_outbox")
        != _text_value(
            gate,
            "target_operation_id",
            table="agent_model_execution_cancellation_gates",
        )
    ):
        return False
    payload = _source_json_object(
        effect,
        "payload_json",
        table="agent_effect_outbox",
    )
    fence = payload.get("cancelled_model_effect_fence")
    if not isinstance(fence, Mapping):
        return False
    return (
        _json_text(payload.get("operation_id"))
        == _text_value(
            gate,
            "target_operation_id",
            table="agent_model_execution_cancellation_gates",
        )
        and _json_text(fence.get("operation_id"))
        == _text_value(
            gate,
            "target_operation_id",
            table="agent_model_execution_cancellation_gates",
        )
        and _json_text(fence.get("effect_id"))
        == _text_value(
            gate,
            "target_effect_id",
            table="agent_model_execution_cancellation_gates",
        )
        and _json_text(fence.get("effect_kind"))
        == _text_value(
            gate,
            "target_effect_kind",
            table="agent_model_execution_cancellation_gates",
        )
        and _json_nonnegative_int(fence.get("contract_version"))
        == _positive_int_value(
            gate,
            "target_contract_version",
            table="agent_model_execution_cancellation_gates",
        )
        and _json_text(fence.get("contract_signature"))
        == _text_value(
            gate,
            "target_contract_signature",
            table="agent_model_execution_cancellation_gates",
        )
        and _json_nonnegative_int(fence.get("ownership_generation"))
        == ownership_generation
    )


def _classify_review_execution_liveness(
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_rows: Sequence[_RawSourceRow],
    operation_nodes: Mapping[str, str],
    effect_rows: Sequence[_RawSourceRow],
    effect_nodes: Mapping[str, str],
    review_gate_rows: Sequence[_RawSourceRow],
    review_gate_nodes: Mapping[str, str],
    review_execution_run_rows: Sequence[_RawSourceRow],
    review_execution_run_nodes: Mapping[str, str],
) -> _ReviewExecutionLiveness:
    """Classify cancellation and execution evidence without guessing task state.

    The review task may be running in another process after its outbox target
    has been cancelled. A running witness is therefore a durable wait signal,
    not an expired lease to reclaim. Any broken relationship is more dangerous
    than a delayed recovery and remains an operator-visible blocker.
    """

    blocking_reasons: list[str] = []
    blocking_nodes: list[str] = []
    waiting_reasons: list[str] = []
    waiting_nodes: list[str] = []
    invariants: list[RecoveryInvariant] = []

    operation_by_id = {
        _text_value(row, "operation_id", table="agent_session_operations"): row
        for row in operation_rows
    }
    effect_by_id = {
        _text_value(row, "effect_id", table="agent_effect_outbox"): row for row in effect_rows
    }
    runs_by_effect_id: dict[str, list[_RawSourceRow]] = {}
    gates_by_effect_id: dict[str, list[_RawSourceRow]] = {}

    def block(
        code: str,
        *,
        node_identity: str,
        authority: str,
        token: str,
        details: Mapping[str, object] | None = None,
    ) -> None:
        blocking_reasons.append(code)
        blocking_nodes.append(node_identity)
        invariants.append(
            RecoveryInvariant(
                identity=f"invariant:{code}:{token}",
                code=code,
                severity=RecoveryInvariantSeverity.BLOCKING,
                authority=authority,
                node_identity=node_identity,
                details=dict(details or {}),
            )
        )

    def wait(code: str, *, node_identity: str) -> None:
        waiting_reasons.append(code)
        waiting_nodes.append(node_identity)

    current_review_gate_rows: list[_RawSourceRow] = []
    for row in review_gate_rows:
        gate_identity = _review_cancellation_gate_identity(row)
        node_identity = review_gate_nodes[gate_identity]
        gate_generation = _positive_int_value(
            row,
            "ownership_generation",
            table="agent_review_cancellation_gates",
        )
        if (
            _text_value(
                row,
                "profile_id",
                table="agent_review_cancellation_gates",
            )
            != key.profile_id
            or _text_value(
                row,
                "session_id",
                table="agent_review_cancellation_gates",
            )
            != key.session_id
        ):
            block(
                "review_cancellation_gate_scope_conflict",
                node_identity=node_identity,
                authority="agent_review_cancellation_gates",
                token=gate_identity,
            )
        elif gate_generation != ownership_generation:
            block(
                "review_cancellation_gate_generation_conflict",
                node_identity=node_identity,
                authority="agent_review_cancellation_gates",
                token=gate_identity,
                details={
                    "expected_generation": ownership_generation,
                    "observed_generation": gate_generation,
                },
            )
        else:
            current_review_gate_rows.append(row)

    current_review_execution_run_rows: list[_RawSourceRow] = []
    for row in review_execution_run_rows:
        run_identity = _review_execution_run_identity(row)
        node_identity = review_execution_run_nodes[run_identity]
        run_generation = _positive_int_value(
            row,
            "ownership_generation",
            table="agent_review_execution_runs",
        )
        if (
            _text_value(
                row,
                "profile_id",
                table="agent_review_execution_runs",
            )
            != key.profile_id
            or _text_value(
                row,
                "session_id",
                table="agent_review_execution_runs",
            )
            != key.session_id
        ):
            block(
                "review_execution_witness_scope_conflict",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
            )
        elif run_generation != ownership_generation:
            block(
                "review_execution_witness_generation_conflict",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
                details={
                    "expected_generation": ownership_generation,
                    "observed_generation": run_generation,
                },
            )
        else:
            current_review_execution_run_rows.append(row)

    for row in current_review_gate_rows:
        review_effect_id = _text_value(
            row,
            "review_effect_id",
            table="agent_review_cancellation_gates",
        )
        gates_by_effect_id.setdefault(review_effect_id, []).append(row)
    for row in current_review_execution_run_rows:
        review_effect_id = _text_value(
            row,
            "review_effect_id",
            table="agent_review_execution_runs",
        )
        runs_by_effect_id.setdefault(review_effect_id, []).append(row)

    for review_effect_id, runs in runs_by_effect_id.items():
        if len(runs) < 2:
            continue
        for row in runs:
            run_identity = _review_execution_run_identity(row)
            block(
                "review_execution_witness_multiple_running",
                node_identity=review_execution_run_nodes[run_identity],
                authority="agent_review_execution_runs",
                token=run_identity,
                details={"review_effect_id": review_effect_id},
            )

    for row in current_review_execution_run_rows:
        run_identity = _review_execution_run_identity(row)
        node_identity = review_execution_run_nodes[run_identity]
        review_effect_id = _text_value(
            row,
            "review_effect_id",
            table="agent_review_execution_runs",
        )
        review_operation_id = _text_value(
            row,
            "review_operation_id",
            table="agent_review_execution_runs",
        )
        review_effect_kind = _text_value(
            row,
            "review_effect_kind",
            table="agent_review_execution_runs",
        )
        _positive_int_value(
            row,
            "review_contract_version",
            table="agent_review_execution_runs",
        )
        _text_value(
            row,
            "review_contract_signature",
            table="agent_review_execution_runs",
        )
        claim_id = _text_value(
            row,
            "claim_id",
            table="agent_review_execution_runs",
        )
        worker_id = _text_value(
            row,
            "worker_id",
            table="agent_review_execution_runs",
        )
        execution_status = _text_value(
            row,
            "execution_status",
            table="agent_review_execution_runs",
        )
        run_generation = _positive_int_value(
            row,
            "ownership_generation",
            table="agent_review_execution_runs",
        )
        _nonnegative_time_value(
            row,
            "started_at",
            table="agent_review_execution_runs",
        )
        finished_at = _optional_nonnegative_time_value(
            row,
            "finished_at",
            table="agent_review_execution_runs",
        )
        unknown_at = _optional_nonnegative_time_value(
            row,
            "unknown_at",
            table="agent_review_execution_runs",
        )
        unknown_reason = _text_value(
            row,
            "unknown_reason",
            table="agent_review_execution_runs",
            required=False,
        )
        if (
            _text_value(
                row,
                "profile_id",
                table="agent_review_execution_runs",
            )
            != key.profile_id
            or _text_value(
                row,
                "session_id",
                table="agent_review_execution_runs",
            )
            != key.session_id
            or run_generation != ownership_generation
        ):
            block(
                "review_execution_witness_scope_conflict",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
            )
        if execution_status == "unknown":
            if finished_at is not None or unknown_at is None or not unknown_reason:
                block(
                    "review_execution_witness_unknown_evidence_conflict",
                    node_identity=node_identity,
                    authority="agent_review_execution_runs",
                    token=run_identity,
                )
        elif execution_status != "running" or finished_at is not None:
            block(
                "review_execution_witness_status_conflict",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
            )
        if review_effect_kind != "run_review_workflow":
            block(
                "review_execution_witness_kind_conflict",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
            )

        effect = effect_by_id.get(review_effect_id)
        if effect is None:
            block(
                "review_execution_witness_target_missing",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
                details={"review_effect_id": review_effect_id},
            )
        elif not _review_execution_run_matches_effect(
            row,
            effect,
            ownership_generation=ownership_generation,
        ):
            block(
                "review_execution_witness_target_conflict",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
                details={"review_effect_id": review_effect_id},
            )
        else:
            effect_status = _text_value(
                effect,
                "status",
                table="agent_effect_outbox",
            )
            if effect_status == "processing":
                if (
                    _text_value(
                        effect,
                        "claim_id",
                        table="agent_effect_outbox",
                        required=False,
                    )
                    != claim_id
                    or _text_value(
                        effect,
                        "lease_owner",
                        table="agent_effect_outbox",
                        required=False,
                    )
                    != worker_id
                    or _optional_nonnegative_time_value(
                        effect,
                        "lease_until",
                        table="agent_effect_outbox",
                    )
                    is None
                ):
                    block(
                        "review_execution_witness_claim_conflict",
                        node_identity=node_identity,
                        authority="agent_review_execution_runs",
                        token=run_identity,
                    )
            elif effect_status == "cancelled":
                if (
                    _text_value(
                        effect,
                        "claim_id",
                        table="agent_effect_outbox",
                        required=False,
                    )
                    or _text_value(
                        effect,
                        "lease_owner",
                        table="agent_effect_outbox",
                        required=False,
                    )
                    or _optional_nonnegative_time_value(
                        effect,
                        "lease_until",
                        table="agent_effect_outbox",
                    )
                    is not None
                ):
                    block(
                        "cancelled_review_effect_retains_lease",
                        node_identity=node_identity,
                        authority="agent_effect_outbox",
                        token=run_identity,
                    )
                if review_effect_id not in gates_by_effect_id:
                    block(
                        "review_execution_witness_cancelled_without_gate",
                        node_identity=node_identity,
                        authority="agent_review_execution_runs",
                        token=run_identity,
                    )
            else:
                block(
                    "review_execution_witness_target_status_conflict",
                    node_identity=node_identity,
                    authority="agent_effect_outbox",
                    token=run_identity,
                    details={"status": effect_status},
                )

        operation = operation_by_id.get(review_operation_id)
        if operation is None:
            block(
                "review_execution_witness_operation_missing",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
                details={"review_operation_id": review_operation_id},
            )
        elif (
            _positive_int_value(
                operation,
                "ownership_generation",
                table="agent_session_operations",
            )
            != ownership_generation
            or _text_value(operation, "kind", table="agent_session_operations") != "review"
        ):
            block(
                "review_execution_witness_operation_conflict",
                node_identity=node_identity,
                authority="agent_session_operations",
                token=run_identity,
                details={"review_operation_id": review_operation_id},
            )

        if execution_status == "unknown":
            block(
                "review_execution_witness_unknown",
                node_identity=node_identity,
                authority="agent_review_execution_runs",
                token=run_identity,
            )
        else:
            # A running witness is live authority until its outbox lease stops
            # being renewed. Lease expiry then creates an explicit unknown
            # blocker rather than permission to replay the model call.
            wait("review_execution_witness_running", node_identity=node_identity)

    for row in current_review_gate_rows:
        cancellation_effect_id = _text_value(
            row,
            "cancellation_effect_id",
            table="agent_review_cancellation_gates",
        )
        node_identity = review_gate_nodes[_review_cancellation_gate_identity(row)]
        review_effect_id = _text_value(
            row,
            "review_effect_id",
            table="agent_review_cancellation_gates",
        )
        review_operation_id = _text_value(
            row,
            "review_operation_id",
            table="agent_review_cancellation_gates",
        )
        gate_status = _text_value(
            row,
            "gate_status",
            table="agent_review_cancellation_gates",
        )
        target_effect_status = _text_value(
            row,
            "target_effect_status",
            table="agent_review_cancellation_gates",
        )
        gate_generation = _positive_int_value(
            row,
            "ownership_generation",
            table="agent_review_cancellation_gates",
        )
        created_at = _nonnegative_time_value(
            row,
            "created_at",
            table="agent_review_cancellation_gates",
        )
        updated_at = _nonnegative_time_value(
            row,
            "updated_at",
            table="agent_review_cancellation_gates",
        )
        target_terminal_at = _optional_nonnegative_time_value(
            row,
            "target_effect_terminal_at",
            table="agent_review_cancellation_gates",
        )
        if (
            _text_value(
                row,
                "profile_id",
                table="agent_review_cancellation_gates",
            )
            != key.profile_id
            or _text_value(
                row,
                "session_id",
                table="agent_review_cancellation_gates",
            )
            != key.session_id
            or gate_generation != ownership_generation
            or updated_at < created_at
        ):
            block(
                "review_cancellation_gate_scope_conflict",
                node_identity=node_identity,
                authority="agent_review_cancellation_gates",
                token=cancellation_effect_id,
            )

        effect = effect_by_id.get(review_effect_id)
        if effect is None:
            block(
                "review_cancellation_gate_target_missing",
                node_identity=node_identity,
                authority="agent_review_cancellation_gates",
                token=cancellation_effect_id,
                details={"review_effect_id": review_effect_id},
            )
        elif not _review_cancellation_gate_matches_effect(
            row,
            effect,
            ownership_generation=ownership_generation,
        ):
            block(
                "review_cancellation_gate_target_conflict",
                node_identity=node_identity,
                authority="agent_review_cancellation_gates",
                token=cancellation_effect_id,
                details={"review_effect_id": review_effect_id},
            )
        else:
            actual_status = _text_value(effect, "status", table="agent_effect_outbox")
            actual_attempt_count = _nonnegative_int_value(
                effect,
                "attempt_count",
                table="agent_effect_outbox",
            )
            expected_attempt_count = _nonnegative_int_value(
                row,
                "target_effect_attempt_count",
                table="agent_review_cancellation_gates",
            )
            if actual_status != target_effect_status or (
                actual_attempt_count != expected_attempt_count
            ):
                block(
                    "review_cancellation_gate_target_state_conflict",
                    node_identity=node_identity,
                    authority="agent_review_cancellation_gates",
                    token=cancellation_effect_id,
                )
            gate_claim_id = _text_value(
                row,
                "target_effect_claim_id",
                table="agent_review_cancellation_gates",
                required=False,
            )
            effect_claim_id = _text_value(
                effect,
                "claim_id",
                table="agent_effect_outbox",
                required=False,
            )
            effect_lease_owner = _text_value(
                effect,
                "lease_owner",
                table="agent_effect_outbox",
                required=False,
            )
            effect_lease_until = _optional_nonnegative_time_value(
                effect,
                "lease_until",
                table="agent_effect_outbox",
            )
            if gate_status == "requested":
                if (
                    actual_status != "processing"
                    or target_terminal_at is not None
                    or gate_claim_id != effect_claim_id
                    or not effect_claim_id
                    or not effect_lease_owner
                    or effect_lease_until is None
                ):
                    block(
                        "review_cancellation_gate_requested_state_conflict",
                        node_identity=node_identity,
                        authority="agent_review_cancellation_gates",
                        token=cancellation_effect_id,
                    )
            elif gate_status == "cancelled":
                completed_at = _optional_nonnegative_time_value(
                    effect,
                    "completed_at",
                    table="agent_effect_outbox",
                )
                if (
                    actual_status != "cancelled"
                    or target_terminal_at is None
                    or completed_at != target_terminal_at
                    or effect_claim_id
                    or effect_lease_owner
                    or effect_lease_until is not None
                ):
                    block(
                        "review_cancellation_gate_cancelled_state_conflict",
                        node_identity=node_identity,
                        authority="agent_review_cancellation_gates",
                        token=cancellation_effect_id,
                    )
            else:
                block(
                    "review_cancellation_gate_status_conflict",
                    node_identity=node_identity,
                    authority="agent_review_cancellation_gates",
                    token=cancellation_effect_id,
                )

        operation = operation_by_id.get(review_operation_id)
        if operation is None:
            block(
                "review_cancellation_gate_operation_missing",
                node_identity=node_identity,
                authority="agent_review_cancellation_gates",
                token=cancellation_effect_id,
                details={"review_operation_id": review_operation_id},
            )
        elif (
            _positive_int_value(
                operation,
                "ownership_generation",
                table="agent_session_operations",
            )
            != ownership_generation
            or _text_value(operation, "kind", table="agent_session_operations") != "review"
            or _text_value(operation, "status", table="agent_session_operations") != "superseded"
        ):
            block(
                "review_cancellation_gate_operation_conflict",
                node_identity=node_identity,
                authority="agent_session_operations",
                token=cancellation_effect_id,
                details={"review_operation_id": review_operation_id},
            )

        control_effect = effect_by_id.get(cancellation_effect_id)
        if control_effect is None:
            block(
                "review_cancellation_gate_control_missing",
                node_identity=node_identity,
                authority="agent_review_cancellation_gates",
                token=cancellation_effect_id,
            )
            continue
        if not _review_cancellation_control_matches_gate(
            row,
            control_effect,
            ownership_generation=ownership_generation,
        ):
            block(
                "review_cancellation_gate_control_conflict",
                node_identity=node_identity,
                authority="agent_effect_outbox",
                token=cancellation_effect_id,
            )
            continue
        control_status = _text_value(
            control_effect,
            "status",
            table="agent_effect_outbox",
        )
        if control_status in {"pending", "processing"}:
            wait("review_cancellation_control_live", node_identity=node_identity)
        elif control_status not in {"completed", "failed", "cancelled"}:
            block(
                "review_cancellation_gate_control_status_conflict",
                node_identity=node_identity,
                authority="agent_effect_outbox",
                token=cancellation_effect_id,
                details={"status": control_status},
            )
        elif gate_status == "requested":
            block(
                "review_cancellation_gate_unresolved_after_control_terminal",
                node_identity=node_identity,
                authority="agent_review_cancellation_gates",
                token=cancellation_effect_id,
            )

        if gate_status == "requested":
            wait("review_cancellation_gate_requested", node_identity=node_identity)

    return _ReviewExecutionLiveness(
        blocking_reason_codes=tuple(dict.fromkeys(blocking_reasons)),
        blocking_node_identities=tuple(dict.fromkeys(blocking_nodes)),
        waiting_reason_codes=tuple(dict.fromkeys(waiting_reasons)),
        waiting_node_identities=tuple(dict.fromkeys(waiting_nodes)),
        invariants=tuple(invariants),
    )


def _review_execution_run_matches_effect(
    run: _RawSourceRow,
    effect: _RawSourceRow,
    *,
    ownership_generation: int,
) -> bool:
    """Return whether one running witness preserves its review outbox fence."""

    return (
        _positive_int_value(
            effect,
            "ownership_generation",
            table="agent_effect_outbox",
        )
        == ownership_generation
        and _text_value(run, "review_effect_id", table="agent_review_execution_runs")
        == _text_value(effect, "effect_id", table="agent_effect_outbox")
        and _text_value(
            run,
            "review_operation_id",
            table="agent_review_execution_runs",
        )
        == _text_value(effect, "operation_id", table="agent_effect_outbox")
        and _text_value(
            run,
            "review_effect_kind",
            table="agent_review_execution_runs",
        )
        == _text_value(effect, "kind", table="agent_effect_outbox")
        and _positive_int_value(
            run,
            "review_contract_version",
            table="agent_review_execution_runs",
        )
        == _positive_int_value(
            effect,
            "contract_version",
            table="agent_effect_outbox",
        )
        and _text_value(
            run,
            "review_contract_signature",
            table="agent_review_execution_runs",
        )
        == _text_value(effect, "contract_signature", table="agent_effect_outbox")
    )


def _review_cancellation_gate_matches_effect(
    gate: _RawSourceRow,
    effect: _RawSourceRow,
    *,
    ownership_generation: int,
) -> bool:
    """Return whether a gate still names the exact durable review target."""

    review_contract_version = _positive_int_value(
        gate,
        "review_contract_version",
        table="agent_review_cancellation_gates",
    )
    review_contract_signature = _text_value(
        gate,
        "review_contract_signature",
        table="agent_review_cancellation_gates",
    )
    try:
        review_contract = builtin_effect_contract(
            "run_review_workflow",
            version=review_contract_version,
        )
    except KeyError:
        return False
    return (
        _text_value(
            gate,
            "review_effect_kind",
            table="agent_review_cancellation_gates",
        )
        == "run_review_workflow"
        and review_contract.signature == review_contract_signature
        and _positive_int_value(
            effect,
            "ownership_generation",
            table="agent_effect_outbox",
        )
        == ownership_generation
        and _text_value(
            gate,
            "review_effect_id",
            table="agent_review_cancellation_gates",
        )
        == _text_value(effect, "effect_id", table="agent_effect_outbox")
        and _text_value(
            gate,
            "review_operation_id",
            table="agent_review_cancellation_gates",
        )
        == _text_value(effect, "operation_id", table="agent_effect_outbox")
        and _text_value(
            gate,
            "review_effect_kind",
            table="agent_review_cancellation_gates",
        )
        == _text_value(effect, "kind", table="agent_effect_outbox")
        and review_contract_version
        == _positive_int_value(
            effect,
            "contract_version",
            table="agent_effect_outbox",
        )
        and review_contract_signature
        == _text_value(effect, "contract_signature", table="agent_effect_outbox")
    )


def _review_cancellation_control_matches_gate(
    gate: _RawSourceRow,
    effect: _RawSourceRow,
    *,
    ownership_generation: int,
) -> bool:
    """Return whether a control outbox row is the gate's exact cancellation effect."""

    control_contract = builtin_effect_contract(
        "cancel_review_workflow",
        version=2,
    )
    if (
        _positive_int_value(
            effect,
            "ownership_generation",
            table="agent_effect_outbox",
        )
        != ownership_generation
        or _text_value(effect, "kind", table="agent_effect_outbox") != "cancel_review_workflow"
        or _positive_int_value(
            effect,
            "contract_version",
            table="agent_effect_outbox",
        )
        != control_contract.version
        or _text_value(effect, "contract_signature", table="agent_effect_outbox")
        != control_contract.signature
        or _text_value(effect, "effect_id", table="agent_effect_outbox")
        != _text_value(
            gate,
            "cancellation_effect_id",
            table="agent_review_cancellation_gates",
        )
        or _text_value(effect, "idempotency_key", table="agent_effect_outbox")
        != _text_value(
            gate,
            "cancellation_effect_id",
            table="agent_review_cancellation_gates",
        )
        or _text_value(effect, "event_id", table="agent_effect_outbox")
        != _text_value(
            gate,
            "request_event_id",
            table="agent_review_cancellation_gates",
        )
        or _text_value(effect, "operation_id", table="agent_effect_outbox")
        != _text_value(
            gate,
            "review_operation_id",
            table="agent_review_cancellation_gates",
        )
    ):
        return False
    payload = _source_json_object(
        effect,
        "payload_json",
        table="agent_effect_outbox",
    )
    fence = payload.get("cancelled_operation_fence")
    if not isinstance(fence, Mapping):
        return False
    return (
        _json_text(payload.get("operation_id"))
        == _text_value(
            gate,
            "review_operation_id",
            table="agent_review_cancellation_gates",
        )
        and _json_text(fence.get("operation_id"))
        == _text_value(
            gate,
            "review_operation_id",
            table="agent_review_cancellation_gates",
        )
        and _json_text(fence.get("effect_id"))
        == _text_value(
            gate,
            "review_effect_id",
            table="agent_review_cancellation_gates",
        )
        and _json_text(fence.get("effect_kind"))
        == _text_value(
            gate,
            "review_effect_kind",
            table="agent_review_cancellation_gates",
        )
        and _json_nonnegative_int(fence.get("contract_version"))
        == _positive_int_value(
            gate,
            "review_contract_version",
            table="agent_review_cancellation_gates",
        )
        and _json_text(fence.get("contract_signature"))
        == _text_value(
            gate,
            "review_contract_signature",
            table="agent_review_cancellation_gates",
        )
        and _json_nonnegative_int(fence.get("ownership_generation")) == ownership_generation
    )


def _state_recovery_shape_code(
    *,
    aggregate_state: str,
    aggregate_data: Mapping[str, object],
    active_chat_state: Mapping[str, object],
    active_reply_resume: Mapping[str, object],
    aggregate_fence: RecoveryAggregateFence,
    ownership_generation: int,
    operation_roles: Mapping[str, str],
    operation_rows: Sequence[_RawSourceRow],
    effect_rows: Sequence[_RawSourceRow],
    receipt_rows: Sequence[_RawSourceRow],
) -> str:
    """Return a blocker unless one exact no-replay materializer shape exists.

    The generic graph policy can identify an orphaned operation, but that alone
    is insufficient authority to discard a lost model result. This projection
    accepts only operation/effect pairs that the reducer writes atomically and
    whose effects are terminal. It deliberately excludes every external-action
    receipt, unrelated live operation, partial active-chat shape, and stale
    active-reply resume path.
    """

    if len(operation_roles) != 1:
        return "recovery_orphan_root_cardinality_invalid"
    operation_id, operation_kind = next(iter(operation_roles.items()))
    operation_by_id = {
        _text_value(row, "operation_id", table="agent_session_operations"): row
        for row in operation_rows
    }
    root_row = operation_by_id.get(operation_id)
    if root_row is None:
        return "recovery_expected_operation_missing"
    for observed_id, row in operation_by_id.items():
        if observed_id == operation_id:
            continue
        status = _text_value(row, "status", table="agent_session_operations")
        if status in {"pending", "running"}:
            return "aggregate_unreferenced_live_operation"

    raw_fences = aggregate_data.get("operation_fences")
    if not isinstance(raw_fences, Mapping) or set(raw_fences) != {operation_id}:
        return "aggregate_operation_fence_shape_invalid"
    fence = raw_fences.get(operation_id)
    if not isinstance(fence, Mapping):
        return "aggregate_operation_fence_missing"
    if (
        _json_text(fence.get("operation_id")) != operation_id
        or _json_text(fence.get("operation_kind")) != operation_kind
        or _json_nonnegative_int(fence.get("ownership_generation")) != ownership_generation
        or _json_nonnegative_int(fence.get("active_epoch")) != aggregate_fence.active_epoch
        or _json_nonnegative_int(fence.get("activity_generation"))
        != aggregate_fence.activity_generation
        or _json_nonnegative_int(fence.get("input_watermark")) is None
        or _json_nonnegative_int(fence.get("input_ledger_sequence")) is None
    ):
        return "aggregate_operation_fence_incomplete"
    if _text_value(root_row, "kind", table="agent_session_operations") != operation_kind:
        return "aggregate_operation_kind_conflict"

    expected_effects = _expected_recovery_effects(
        aggregate_state=aggregate_state,
        operation_id=operation_id,
        operation_kind=operation_kind,
        aggregate_data=aggregate_data,
        aggregate_fence=aggregate_fence,
        ownership_generation=ownership_generation,
        fence=fence,
    )
    if isinstance(expected_effects, str):
        return expected_effects
    effects_for_operation = tuple(
        row
        for row in effect_rows
        if _text_value(row, "operation_id", table="agent_effect_outbox") == operation_id
    )
    if len(effects_for_operation) != len(expected_effects):
        return "recovery_expected_effect_conflict"
    by_effect_id = {
        _text_value(row, "effect_id", table="agent_effect_outbox"): row
        for row in effects_for_operation
    }
    if len(by_effect_id) != len(expected_effects):
        return "recovery_expected_effect_conflict"
    for effect_id, expected in expected_effects.items():
        row = by_effect_id.get(effect_id)
        if row is None:
            return "recovery_expected_effect_missing"
        if (
            _text_value(row, "kind", table="agent_effect_outbox") != expected["kind"]
            or _text_value(row, "idempotency_key", table="agent_effect_outbox")
            != expected["idempotency_key"]
            or _nonnegative_int_value(
                row,
                "contract_version",
                table="agent_effect_outbox",
            )
            != expected["contract_version"]
            or _text_value(row, "contract_signature", table="agent_effect_outbox")
            != expected["contract_signature"]
            or _text_value(row, "status", table="agent_effect_outbox")
            not in {"completed", "failed"}
        ):
            return "recovery_expected_effect_conflict"
    if receipt_rows:
        return "recovery_operation_has_external_action_receipt"

    if aggregate_state == "review":
        if active_reply_resume or active_chat_state:
            return "review_recovery_state_shape_invalid"
    elif aggregate_state == "active_reply":
        if active_reply_resume or active_chat_state:
            return "active_reply_resume_recovery_shape_invalid"
    elif aggregate_state == "active_chat":
        active_shape_code = _active_chat_operation_shape_code(
            active_chat_state=active_chat_state,
            operation_id=operation_id,
            operation_kind=operation_kind,
            fence=fence,
        )
        if active_shape_code:
            return active_shape_code
    elif aggregate_state == "active_chat_settling":
        if active_reply_resume:
            return "active_chat_settling_recovery_shape_invalid"
    else:
        return "recovery_state_materializer_unsupported"
    return ""


def _expected_recovery_effects(
    *,
    aggregate_state: str,
    operation_id: str,
    operation_kind: str,
    aggregate_data: Mapping[str, object],
    aggregate_fence: RecoveryAggregateFence,
    ownership_generation: int,
    fence: Mapping[str, object],
) -> dict[str, dict[str, object]] | str:
    """Return the exact terminal effect set required by one recovery state."""

    expected_kind_by_operation = {
        "review": "run_review_workflow",
        "active_reply": "run_active_reply_workflow",
        "active_chat_bootstrap": "run_active_chat_bootstrap",
        "active_chat_round": "run_active_chat_round",
    }
    if aggregate_state != "active_chat_settling":
        expected_kind = expected_kind_by_operation.get(operation_kind)
        if expected_kind is None:
            return "recovery_state_materializer_unsupported"
        effect_id = _json_text(fence.get("effect_id"))
        idempotency_key = _json_text(fence.get("idempotency_key"))
        contract_signature = _json_text(fence.get("contract_signature"))
        contract_version = _json_nonnegative_int(fence.get("contract_version"))
        if (
            not effect_id
            or not idempotency_key
            or _json_text(fence.get("effect_kind")) != expected_kind
            or not contract_signature
            or contract_version is None
            or contract_version < 1
            or _json_text(fence.get("plan_id")) != aggregate_fence.current_plan_id
        ):
            return "aggregate_operation_fence_incomplete"
        return {
            effect_id: {
                "kind": expected_kind,
                "idempotency_key": idempotency_key,
                "contract_version": contract_version,
                "contract_signature": contract_signature,
            }
        }

    if operation_kind != "idle_review_planning":
        return "active_chat_settling_recovery_shape_invalid"
    idle_exit = aggregate_data.get("idle_exit")
    if not isinstance(idle_exit, Mapping):
        return "aggregate_idle_exit_requires_state_materializer"
    successor_plan_id = _json_text(idle_exit.get("plan_id"))
    if (
        _json_text(fence.get("plan_id")) != successor_plan_id
        or not successor_plan_id
        or successor_plan_id == aggregate_fence.current_plan_id
        or _json_nonnegative_int(fence.get("ownership_generation")) != ownership_generation
    ):
        return "active_chat_settling_idle_exit_fence_conflict"
    expected: dict[str, dict[str, object]] = {}
    for prefix, kind in (
        ("planner", "run_idle_review_planning"),
        ("deadline", "enqueue_idle_review_planning_deadline"),
    ):
        effect_id = _json_text(idle_exit.get(f"{prefix}_effect_id"))
        idempotency_key = _json_text(idle_exit.get(f"{prefix}_idempotency_key"))
        contract_signature = _json_text(idle_exit.get(f"{prefix}_contract_signature"))
        contract_version = _json_nonnegative_int(idle_exit.get(f"{prefix}_contract_version"))
        if (
            not effect_id
            or not idempotency_key
            or not contract_signature
            or contract_version is None
            or contract_version < 1
        ):
            return "active_chat_settling_idle_exit_fence_conflict"
        expected[effect_id] = {
            "kind": kind,
            "idempotency_key": idempotency_key,
            "contract_version": contract_version,
            "contract_signature": contract_signature,
        }
    return expected


def _active_chat_operation_shape_code(
    *,
    active_chat_state: Mapping[str, object],
    operation_id: str,
    operation_kind: str,
    fence: Mapping[str, object],
) -> str:
    """Validate the mutually exclusive bootstrap and round recovery shapes."""

    bootstrap_operation_id = _json_text(active_chat_state.get("bootstrap_operation_id"))
    round_operation_id = _json_text(active_chat_state.get("round_operation_id"))
    if operation_kind == "active_chat_bootstrap":
        if (
            bootstrap_operation_id != operation_id
            or _json_text(active_chat_state.get("bootstrap_status")) != "pending"
            or round_operation_id
        ):
            return "active_chat_bootstrap_recovery_shape_invalid"
        return ""
    if operation_kind != "active_chat_round":
        return "active_chat_recovery_shape_invalid"
    pending_ids = _json_positive_int_tuple(active_chat_state.get("pending_message_log_ids"))
    selected_ids = _json_positive_int_tuple(fence.get("message_log_ids"))
    if (
        bootstrap_operation_id
        or _json_text(active_chat_state.get("bootstrap_status")) != "completed"
        or round_operation_id != operation_id
        or not pending_ids
        or selected_ids != pending_ids
    ):
        return "active_chat_round_recovery_shape_invalid"
    return ""


def _json_positive_int_tuple(value: object) -> tuple[int, ...] | None:
    """Decode a non-empty, canonical list of positive message identifiers."""

    if not isinstance(value, list) or not value:
        return None
    if any(type(item) is not int or item < 1 for item in value):
        return None
    values = tuple(value)
    if tuple(sorted(set(values))) != values:
        return None
    return values


def _active_chat_recovery_metadata_code(
    *,
    aggregate_data: Mapping[str, object],
    aggregate_fence: RecoveryAggregateFence,
    ownership_generation: int,
    operation_roles: Mapping[str, str],
) -> str:
    """Allow only a completed round-due control beside an active chat round."""

    control_intents = aggregate_data.get("effect_control_intents")
    if control_intents in (None, {}, []):
        return ""
    if not isinstance(control_intents, Mapping) or set(control_intents) != {
        "enqueue_active_chat_round_due"
    }:
        return "aggregate_effect_control_intents_requires_state_materializer"
    round_operations = tuple(
        operation_id
        for operation_id, operation_kind in operation_roles.items()
        if operation_kind == "active_chat_round"
    )
    if len(round_operations) != 1:
        return "active_chat_round_control_without_round"
    round_intent = control_intents.get("enqueue_active_chat_round_due")
    if not isinstance(round_intent, Mapping):
        return "aggregate_effect_control_intents_requires_state_materializer"
    if (
        _json_text(round_intent.get("status")) != "completed"
        or _json_text(round_intent.get("effect_kind")) != "enqueue_active_chat_round_due"
        or _json_text(round_intent.get("operation_id"))
        or _json_text(round_intent.get("plan_id")) != aggregate_fence.current_plan_id
        or _json_nonnegative_int(round_intent.get("ownership_generation")) != ownership_generation
        or _json_nonnegative_int(round_intent.get("active_epoch")) != aggregate_fence.active_epoch
        or _json_nonnegative_int(round_intent.get("activity_generation"))
        != aggregate_fence.activity_generation
    ):
        return "active_chat_round_control_fence_invalid"
    return ""


def _settling_recovery_metadata_code(
    *,
    aggregate_data: Mapping[str, object],
    aggregate_fence: RecoveryAggregateFence,
    ownership_generation: int,
    operation_roles: Mapping[str, str],
    operation_fences: Mapping[str, object],
) -> str:
    """Return a blocker unless settling metadata supports no-replay recovery.

    A completed exit-control record is durable evidence that the actor entered
    settling; it is not an outstanding external action. The planner result is
    still unknown after a crash, so only this narrow shape may reach the
    materializer, which terminalizes the planner and creates its fixed fallback
    review plan. Any other control intent remains operator-visible authority.
    """

    idle_operations = tuple(
        operation_id
        for operation_id, operation_kind in operation_roles.items()
        if operation_kind == "idle_review_planning"
    )
    if len(idle_operations) != 1:
        return "aggregate_settling_operation_shape_invalid"
    operation_id = idle_operations[0]
    idle_exit = aggregate_data.get("idle_exit")
    if not isinstance(idle_exit, Mapping):
        return "aggregate_idle_exit_requires_state_materializer"
    successor_plan_id = _json_text(idle_exit.get("plan_id"))
    if (
        _json_text(idle_exit.get("operation_id")) != operation_id
        or not successor_plan_id
        or successor_plan_id == aggregate_fence.current_plan_id
        or _json_nonnegative_int(idle_exit.get("ownership_generation")) != ownership_generation
    ):
        return "aggregate_idle_exit_fence_invalid"
    operation_fence = operation_fences.get(operation_id)
    if not isinstance(operation_fence, Mapping):
        return "aggregate_operation_fence_missing"
    if (
        _json_text(operation_fence.get("operation_id")) != operation_id
        or _json_text(operation_fence.get("operation_kind")) != "idle_review_planning"
        or _json_text(operation_fence.get("plan_id")) != successor_plan_id
        or _json_nonnegative_int(operation_fence.get("ownership_generation"))
        != ownership_generation
        or _json_nonnegative_int(operation_fence.get("active_epoch"))
        != aggregate_fence.active_epoch
        or _json_nonnegative_int(operation_fence.get("activity_generation"))
        != aggregate_fence.activity_generation
    ):
        return "aggregate_idle_exit_fence_invalid"
    control_intents = aggregate_data.get("effect_control_intents")
    if control_intents in (None, {}, []):
        return ""
    if not isinstance(control_intents, Mapping) or set(control_intents) != {
        "enqueue_active_chat_exit_request"
    }:
        return "aggregate_effect_control_intents_requires_state_materializer"
    exit_intent = control_intents.get("enqueue_active_chat_exit_request")
    if not isinstance(exit_intent, Mapping):
        return "aggregate_effect_control_intents_requires_state_materializer"
    if (
        _json_text(exit_intent.get("status")) != "completed"
        or _json_text(exit_intent.get("effect_kind")) != "enqueue_active_chat_exit_request"
        or _json_text(exit_intent.get("operation_id"))
        or _json_text(exit_intent.get("plan_id")) != aggregate_fence.current_plan_id
        or _json_nonnegative_int(exit_intent.get("ownership_generation")) != ownership_generation
        or _json_nonnegative_int(exit_intent.get("active_epoch")) != aggregate_fence.active_epoch
        or _json_nonnegative_int(exit_intent.get("activity_generation"))
        != aggregate_fence.activity_generation
    ):
        return "aggregate_effect_control_intents_requires_state_materializer"
    return ""


def _row_digest(
    table: str,
    row: _RawSourceRow,
    *,
    excluded_fields: frozenset[str] = frozenset(),
) -> str:
    """Return raw-row evidence, optionally omitting defined semantic noise."""

    digest = hashlib.sha256()
    digest.update(table.encode("utf-8"))
    for field_name in sorted(row.values):
        if field_name in excluded_fields:
            continue
        evidence = row.values[field_name].evidence(prefix_bytes=0)
        digest.update(b"\0")
        digest.update(field_name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(
            json.dumps(
                evidence,
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        )
    return digest.hexdigest()


def _text_value(
    row: _RawSourceRow,
    field_name: str,
    *,
    table: str,
    required: bool = True,
) -> str:
    value = row.values[field_name]
    if value.storage_class != "text":
        raise RecoveryGraphReadError(
            "recovery_authority_text_storage_class_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "storage_class": value.storage_class,
                "table": table,
            },
        )
    try:
        decoded = value.decode()
    except (RawSQLiteValueTruncatedError, UnicodeDecodeError) as exc:
        raise RecoveryGraphReadError(
            "recovery_authority_text_invalid_utf8",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
                "value": _truncated_value_evidence(value),
            },
        ) from exc
    if not isinstance(decoded, str):
        raise AssertionError("TEXT raw SQLite values must decode to str")
    if decoded != decoded.strip():
        raise RecoveryGraphReadError(
            "recovery_authority_text_not_canonical",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
            },
        )
    if required and not decoded:
        raise RecoveryGraphReadError(
            "recovery_authority_required_text_empty",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
            },
        )
    return decoded


def _nonnegative_int_value(
    row: _RawSourceRow,
    field_name: str,
    *,
    table: str,
) -> int:
    value = row.values[field_name]
    if value.storage_class != "integer":
        raise RecoveryGraphReadError(
            "recovery_authority_integer_storage_class_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "storage_class": value.storage_class,
                "table": table,
            },
        )
    decoded = value.decode()
    if type(decoded) is not int or decoded < 0:
        raise RecoveryGraphReadError(
            "recovery_authority_integer_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
            },
        )
    return decoded


def _positive_int_value(
    row: _RawSourceRow,
    field_name: str,
    *,
    table: str,
) -> int:
    result = _nonnegative_int_value(row, field_name, table=table)
    if result < 1:
        raise RecoveryGraphReadError(
            "recovery_authority_positive_integer_invalid",
            evidence={
                "field": field_name,
                "row_id": row.row_id,
                "table": table,
            },
        )
    return result


def _truncated_value_evidence(value: RawSQLiteValue) -> dict[str, object]:
    raw = value.raw
    prefix = raw if isinstance(raw, bytes) else b""
    return {
        "byte_length": value.byte_length,
        "prefix_base64": base64.b64encode(prefix).decode("ascii"),
        "storage_class": value.storage_class,
    }


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized or normalized != value:
        raise ValueError(f"{field_name} must be non-empty canonical text")
    return normalized


def _required_positive_int(value: object, *, field_name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite non-negative number")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{field_name} must be a finite non-negative number")
    return result


def _require_reader_transaction(conn: sqlite3.Connection) -> None:
    if not conn.in_transaction:
        raise ValueError("recovery graph reader requires a caller-owned transaction")


__all__ = [
    "MAX_RECOVERY_RAW_FIELD_BYTES",
    "MAX_RECOVERY_SOURCE_ROWS",
    "RecoveryCaseSnapshot",
    "RecoveryDeliveryClaimLost",
    "RecoveryGraphAuthority",
    "RecoveryGraphNotEligible",
    "RecoveryGraphReadError",
    "SQLiteRecoveryGraphReader",
    "ValidatedClaimedRecoveryDelivery",
]
