"""Transaction-bound coordination for typed session-actor recovery commits."""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, fields, is_dataclass
from enum import StrEnum

from shinbot.agent.runtime.session_actor.aggregate import AgentSessionAggregate
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.json_validation import (
    DurableJSONValidationError,
    validate_durable_json,
)
from shinbot.agent.runtime.session_actor.recovery import (
    MAX_RECOVERY_TEXT_BYTES,
    RECOVERY_CERTIFICATE_SCHEMA,
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
    RECOVERY_DELIVERY_SCHEMA,
    RecoveryCertificate,
    RecoveryDecisionKind,
    RecoveryDeliveryEnvelopeIdentity,
)
from shinbot.agent.runtime.session_actor.recovery_commit import (
    RecoveryCommitIntent,
    RecoveryCommitIntentMismatch,
    RecoveryMaterializationBlocked,
    RecoveryMaterializer,
)
from shinbot.agent.runtime.session_actor.recovery_graph_reader import (
    MAX_RECOVERY_RAW_FIELD_BYTES,
    RecoveryCaseSnapshot,
    RecoveryGraphAuthority,
    RecoveryGraphNotEligible,
    RecoveryGraphReadError,
    ValidatedClaimedRecoveryDelivery,
)


class RecoveryCommitDisposition(StrEnum):
    """One terminal or retryable result of commit-time recovery proof."""

    APPLIED = "applied"
    ALREADY_TERMINAL = "already_terminal"
    BLOCKED = "blocked"
    REFRESHED = "refreshed"
    SUPERSEDED = "superseded"


MAX_RECOVERY_MATERIALIZATION_METADATA_BYTES = MAX_RECOVERY_TEXT_BYTES


class RecoveryCommitAuthorityError(RuntimeError):
    """Raised when a claimed recovery delivery lacks coherent durable authority."""

    def __init__(self, code: str) -> None:
        """Expose a stable failure code without carrying raw authority data."""

        self.code = _required_text(code, field_name="recovery commit authority code")
        super().__init__(self.code)


@dataclass(slots=True, frozen=True)
class RecoveryCaseSettlement:
    """One case status CAS deferred until the mailbox is terminal."""

    case: RecoveryCaseSnapshot
    status: str
    last_error: str

    def __post_init__(self) -> None:
        """Validate the bounded set of coordinator-owned case terminal states."""

        if self.status not in {"applied", "superseded", "scanner_blocked"}:
            raise ValueError("unsupported recovery case settlement status")
        normalized_error = str(self.last_error or "").strip()
        if self.status == "scanner_blocked" and not normalized_error:
            raise ValueError("scanner_blocked recovery settlement requires an error")
        _validate_bounded_text(
            normalized_error,
            field_name="recovery case settlement error",
            allow_empty=True,
        )
        object.__setattr__(self, "last_error", normalized_error)


@dataclass(slots=True, frozen=True)
class PreparedRecoveryCommit:
    """Raw authority proven before normal SQLite aggregate decoding occurs."""

    intent: RecoveryCommitIntent
    delivery: ValidatedClaimedRecoveryDelivery
    case: RecoveryCaseSnapshot
    certificate: RecoveryCertificate | None
    disposition: RecoveryCommitDisposition
    reason_code: str


@dataclass(slots=True, frozen=True)
class RecoveryCommitResolution:
    """Final transition and optional post-mailbox case settlement."""

    mailbox_id: int
    transition: SessionTransition
    disposition: RecoveryCommitDisposition
    reason_code: str
    case_settlement: RecoveryCaseSettlement | None = None


class SQLiteRecoveryCommitCoordinator:
    """Revalidate typed recovery authority before materializing one transition.

    The coordinator never opens a transaction and never writes aggregates,
    journals, effects, or mailboxes. The SQLite store calls ``prepare`` before
    its normal mailbox/aggregate decoder, calls ``resolve`` after loading the
    current aggregate in that same transaction, then calls ``finalize_case``
    only after the mailbox completion update has succeeded.
    """

    def __init__(
        self,
        authority: RecoveryGraphAuthority,
        *,
        materializers: Mapping[str, RecoveryMaterializer] | None = None,
    ) -> None:
        """Bind one coordinator to raw authority and pure state materializers."""

        if authority is None:
            raise TypeError("authority must implement RecoveryGraphAuthority")
        self._authority = authority
        self._materializers = {
            _required_text(state, field_name="materializer state"): materializer
            for state, materializer in (materializers or {}).items()
        }

    @property
    def persistence_domain(self) -> object:
        """Return the exact durable domain observed by the raw authority reader."""

        return self._authority.persistence_domain

    def prepare(
        self,
        conn: sqlite3.Connection,
        *,
        claim: ClaimedSessionEvent,
        intent: RecoveryCommitIntent,
        provisional_transition: SessionTransition,
        commit_now: float,
    ) -> PreparedRecoveryCommit:
        """Prove raw mailbox, case, ownership, and graph authority in one transaction."""

        if not conn.in_transaction:
            raise ValueError("recovery commit preparation requires an open transaction")
        if not isinstance(claim, ClaimedSessionEvent):
            raise TypeError("claim must be a ClaimedSessionEvent")
        if not isinstance(intent, RecoveryCommitIntent):
            raise TypeError("intent must be a RecoveryCommitIntent")
        delivery = self._authority.validate_claimed_delivery(
            conn,
            claim=claim,
            commit_now=_nonnegative_finite(commit_now, field_name="commit_now"),
        )
        authoritative_intent = RecoveryCommitIntent.from_delivery(
            envelope=_delivery_envelope_from_claim(claim),
            payload=delivery.delivery,
        )
        case = self._authority.load_case_snapshot(
            conn,
            case_id=delivery.delivery.case_id,
        )
        if case is None:
            raise RecoveryCommitAuthorityError("recovery_case_disappeared")
        _validate_case_delivery_fence(case, delivery=delivery)
        if case.status != "open":
            return PreparedRecoveryCommit(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                certificate=None,
                disposition=RecoveryCommitDisposition.ALREADY_TERMINAL,
                reason_code=f"recovery_case_{case.status}",
            )
        try:
            _validate_intent_claim_identity(intent, claim)
            intent.validate_delivery(delivery.delivery)
        except RecoveryCommitIntentMismatch as exc:
            return self._blocked_preparation(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                reason_code=exc.code,
            )
        except RecoveryCommitAuthorityError as exc:
            return self._blocked_preparation(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                reason_code=exc.code,
            )
        try:
            _validate_provisional_transition(
                provisional_transition,
                intent=intent,
                claim=claim,
            )
        except RecoveryCommitAuthorityError as exc:
            return self._blocked_preparation(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                reason_code=exc.code,
            )
        except TypeError:
            return self._blocked_preparation(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                reason_code="recovery_provisional_transition_invalid",
            )
        try:
            certificate = self._authority.rebuild_certificate(
                conn,
                key=claim.key,
                ownership_generation=claim.envelope.ownership_generation,
            )
        except RecoveryGraphNotEligible as exc:
            return PreparedRecoveryCommit(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                certificate=None,
                disposition=RecoveryCommitDisposition.SUPERSEDED,
                reason_code=exc.reason_code,
            )
        except RecoveryGraphReadError as exc:
            return PreparedRecoveryCommit(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                certificate=None,
                disposition=RecoveryCommitDisposition.BLOCKED,
                reason_code=exc.code,
            )
        if certificate.case_identity.case_id != delivery.delivery.case_id:
            return PreparedRecoveryCommit(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                certificate=certificate,
                disposition=RecoveryCommitDisposition.SUPERSEDED,
                reason_code="recovery_semantic_graph_changed",
            )
        if certificate.certificate_digest != authoritative_intent.certificate_digest:
            return PreparedRecoveryCommit(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                certificate=certificate,
                disposition=RecoveryCommitDisposition.REFRESHED,
                reason_code="recovery_certificate_fence_changed",
            )
        if certificate.decision.kind is not RecoveryDecisionKind.RECOVER_ORPHANED_WORK:
            return PreparedRecoveryCommit(
                intent=authoritative_intent,
                delivery=delivery,
                case=case,
                certificate=certificate,
                disposition=RecoveryCommitDisposition.SUPERSEDED,
                reason_code="recovery_decision_changed",
            )
        return PreparedRecoveryCommit(
            intent=authoritative_intent,
            delivery=delivery,
            case=case,
            certificate=certificate,
            disposition=RecoveryCommitDisposition.APPLIED,
            reason_code="recovery_materialization_authorized",
        )

    def resolve(
        self,
        prepared: PreparedRecoveryCommit,
        *,
        aggregate: AgentSessionAggregate,
        transition_validator: Callable[[SessionTransition], None] | None = None,
    ) -> RecoveryCommitResolution:
        """Materialize one proven recovery or create its fenced no-op transition.

        Args:
            prepared: Raw authority prepared in the caller's write transaction.
            aggregate: Current aggregate decoded by the durable store.
            transition_validator: Optional store-owned, no-write contract check
                for an applied materializer result. A rejected result is
                converted into a durable blocker only after raw proof succeeds.
        """

        if not isinstance(prepared, PreparedRecoveryCommit):
            raise TypeError("prepared must be a PreparedRecoveryCommit")
        if not isinstance(aggregate, AgentSessionAggregate):
            raise TypeError("aggregate must be an AgentSessionAggregate")
        if transition_validator is not None and not callable(transition_validator):
            raise TypeError("transition_validator must be callable")
        _validate_aggregate_identity(aggregate, intent=prepared.intent)
        if prepared.certificate is not None:
            _validate_aggregate_fence(
                aggregate,
                certificate=prepared.certificate,
            )
        if prepared.disposition is RecoveryCommitDisposition.ALREADY_TERMINAL:
            return self._no_op_resolution(prepared, aggregate=aggregate)
        if prepared.disposition is RecoveryCommitDisposition.SUPERSEDED:
            return self._no_op_resolution(
                prepared,
                aggregate=aggregate,
                settlement=RecoveryCaseSettlement(
                    case=prepared.case,
                    status="superseded",
                    last_error=prepared.reason_code,
                ),
            )
        if prepared.disposition is RecoveryCommitDisposition.BLOCKED:
            return self._no_op_resolution(
                prepared,
                aggregate=aggregate,
                settlement=RecoveryCaseSettlement(
                    case=prepared.case,
                    status="scanner_blocked",
                    last_error=prepared.reason_code,
                ),
            )
        if prepared.disposition is RecoveryCommitDisposition.REFRESHED:
            return self._no_op_resolution(prepared, aggregate=aggregate)
        if prepared.certificate is None:
            raise RecoveryCommitAuthorityError("recovery_certificate_missing")
        materializer = self._materializers.get(aggregate.state)
        if materializer is None:
            return self.block(
                prepared,
                aggregate=aggregate,
                reason_code="recovery_materializer_missing",
            )
        try:
            materialized = materializer.materialize(
                aggregate=aggregate,
                intent=prepared.intent,
                certificate=prepared.certificate,
            )
        except Exception:
            return self.block(
                prepared,
                aggregate=aggregate,
                reason_code="recovery_materializer_failed",
            )
        if isinstance(materialized, RecoveryMaterializationBlocked):
            return self.block(
                prepared,
                aggregate=aggregate,
                reason_code=materialized.code,
            )
        if not isinstance(materialized, SessionTransition):
            return self.block(
                prepared,
                aggregate=aggregate,
                reason_code="recovery_materializer_result_invalid",
            )
        try:
            _validate_materialized_transition(materialized)
        except RecoveryCommitAuthorityError as exc:
            return self.block(
                prepared,
                aggregate=aggregate,
                reason_code=exc.code,
            )
        if transition_validator is not None:
            try:
                transition_validator(materialized)
            except Exception:
                return self.block(
                    prepared,
                    aggregate=aggregate,
                    reason_code="recovery_materialized_transition_invalid",
                )
        return RecoveryCommitResolution(
            mailbox_id=prepared.delivery.mailbox_id,
            transition=materialized,
            disposition=RecoveryCommitDisposition.APPLIED,
            reason_code="recovery_materialized",
            case_settlement=RecoveryCaseSettlement(
                case=prepared.case,
                status="applied",
                last_error="",
            ),
        )

    def block(
        self,
        prepared: PreparedRecoveryCommit,
        *,
        aggregate: AgentSessionAggregate,
        reason_code: str,
    ) -> RecoveryCommitResolution:
        """Settle a proven open case as a durable no-op blocker.

        Callers may use this after a materializer or transition contract rejects
        behavior, but only after :meth:`prepare` has already proven the raw
        delivery and case. It deliberately does not manufacture a blocker for
        unreadable raw authority.
        """

        if not isinstance(prepared, PreparedRecoveryCommit):
            raise TypeError("prepared must be a PreparedRecoveryCommit")
        if not isinstance(aggregate, AgentSessionAggregate):
            raise TypeError("aggregate must be an AgentSessionAggregate")
        _validate_aggregate_identity(aggregate, intent=prepared.intent)
        if prepared.case.status != "open":
            raise RecoveryCommitAuthorityError("recovery_case_not_open_for_block")
        normalized_reason = _required_text(
            reason_code,
            field_name="recovery materialization blocker code",
        )
        return self._no_op_resolution(
            prepared,
            aggregate=aggregate,
            disposition=RecoveryCommitDisposition.BLOCKED,
            reason_code=normalized_reason,
            settlement=RecoveryCaseSettlement(
                case=prepared.case,
                status="scanner_blocked",
                last_error=normalized_reason,
            ),
        )

    @staticmethod
    def _blocked_preparation(
        *,
        intent: RecoveryCommitIntent,
        delivery: ValidatedClaimedRecoveryDelivery,
        case: RecoveryCaseSnapshot,
        reason_code: str,
    ) -> PreparedRecoveryCommit:
        """Return a blocker only after raw delivery and case proof succeeds."""

        return PreparedRecoveryCommit(
            intent=intent,
            delivery=delivery,
            case=case,
            certificate=None,
            disposition=RecoveryCommitDisposition.BLOCKED,
            reason_code=_required_text(
                reason_code,
                field_name="recovery preparation blocker code",
            ),
        )

    def finalize_case(
        self,
        conn: sqlite3.Connection,
        resolution: RecoveryCommitResolution,
        *,
        commit_now: float,
    ) -> None:
        """CAS the case only after the store has terminalized its mailbox."""

        if not conn.in_transaction:
            raise ValueError("recovery case finalization requires an open transaction")
        settlement = resolution.case_settlement
        if settlement is None:
            return
        case = settlement.case
        updated_at = _next_monotonic_time(case.updated_at, commit_now)
        updated = conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET status = ?, last_error = ?, updated_at = ?
            WHERE case_id = ?
              AND profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND certificate_version = ?
              AND policy_version = ?
              AND work_graph_digest = ?
              AND latest_certificate_digest = ?
              AND status = ?
              AND next_delivery_cycle = ?
              AND delivery_count = ?
              AND last_event_id = ?
              AND last_error = ?
              AND created_at = ?
              AND updated_at = ?
            """,
            (
                settlement.status,
                settlement.last_error,
                updated_at,
                case.case_id,
                case.profile_id,
                case.session_id,
                case.ownership_generation,
                case.certificate_version,
                case.policy_version,
                case.work_graph_digest,
                case.latest_certificate_digest,
                case.status,
                case.next_delivery_cycle,
                case.delivery_count,
                case.last_event_id,
                case.last_error,
                case.created_at,
                case.updated_at,
            ),
        )
        if updated.rowcount != 1:
            raise RecoveryCommitAuthorityError("recovery_case_changed_during_commit")

    @staticmethod
    def _no_op_resolution(
        prepared: PreparedRecoveryCommit,
        *,
        aggregate: AgentSessionAggregate,
        disposition: RecoveryCommitDisposition | None = None,
        reason_code: str | None = None,
        settlement: RecoveryCaseSettlement | None = None,
    ) -> RecoveryCommitResolution:
        resolved_disposition = disposition or prepared.disposition
        resolved_reason = reason_code or prepared.reason_code
        transition = SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition=f"recovery_{resolved_disposition.value}",
            result={
                "recovery": {
                    "case_id": prepared.case.case_id,
                    "outcome": resolved_disposition.value,
                    "reason_code": resolved_reason,
                }
            },
            reason="recovery_requested",
        )
        return RecoveryCommitResolution(
            mailbox_id=prepared.delivery.mailbox_id,
            transition=transition,
            disposition=resolved_disposition,
            reason_code=resolved_reason,
            case_settlement=settlement,
        )


def _validate_intent_claim_identity(
    intent: RecoveryCommitIntent,
    claim: ClaimedSessionEvent,
) -> None:
    """Reject a carrier intent attached to another mailbox claim."""

    envelope = claim.envelope
    expected = intent.envelope
    if (
        expected.event_id != envelope.event_id
        or expected.profile_id != claim.key.profile_id
        or expected.session_id != claim.key.session_id
        or expected.ownership_generation != envelope.ownership_generation
        or expected.kind != RECOVERY_DELIVERY_EVENT_KIND
        or expected.source != RECOVERY_DELIVERY_EVENT_SOURCE
        or envelope.kind != RECOVERY_DELIVERY_EVENT_KIND
        or envelope.source != RECOVERY_DELIVERY_EVENT_SOURCE
    ):
        raise RecoveryCommitAuthorityError("recovery_intent_claim_identity_changed")


def _delivery_envelope_from_claim(
    claim: ClaimedSessionEvent,
) -> RecoveryDeliveryEnvelopeIdentity:
    """Build the only intent envelope that raw claimed-delivery proof permits."""

    envelope = claim.envelope
    return RecoveryDeliveryEnvelopeIdentity(
        event_id=envelope.event_id,
        profile_id=claim.key.profile_id,
        session_id=claim.key.session_id,
        ownership_generation=envelope.ownership_generation,
        kind=envelope.kind,
        source=envelope.source,
    )


def _validate_aggregate_identity(
    aggregate: AgentSessionAggregate,
    *,
    intent: RecoveryCommitIntent,
) -> None:
    """Require a coordinator result to remain scoped to its raw delivery."""

    if (
        aggregate.profile_id != intent.envelope.profile_id
        or aggregate.session_id != intent.envelope.session_id
    ):
        raise RecoveryCommitAuthorityError("recovery_aggregate_key_changed")
    if aggregate.ownership_generation != intent.envelope.ownership_generation:
        raise RecoveryCommitAuthorityError("recovery_aggregate_generation_changed")


def _validate_provisional_transition(
    transition: SessionTransition,
    *,
    intent: RecoveryCommitIntent,
    claim: ClaimedSessionEvent,
) -> None:
    """Require the reducer's recovery carrier to contain no durable behavior.

    The carrier is intentionally replaced after raw authority proof. Rejecting
    all journal metadata and side effects here prevents an untrusted reducer
    output from influencing a typed recovery commit before that replacement.
    """

    if not isinstance(transition, SessionTransition):
        raise TypeError("provisional_transition must be a SessionTransition")
    if transition.aggregate.key != claim.key:
        raise RecoveryCommitAuthorityError("recovery_provisional_key_changed")
    if transition.recovery_commit_intent != intent:
        raise RecoveryCommitAuthorityError("recovery_provisional_intent_changed")
    if (
        transition.disposition != "recovery_commit_pending"
        or transition.reason != "recovery_requested"
    ):
        raise RecoveryCommitAuthorityError("recovery_provisional_protocol_invalid")
    if (
        transition.caused_operation_id
        or transition.caused_plan_id
        or transition.effects
        or transition.operations
        or transition.message_ledger_mutations
        or transition.review_schedules
        or transition.review_schedule_events
        or transition.result
    ):
        raise RecoveryCommitAuthorityError("recovery_provisional_transition_not_empty")


def _validate_aggregate_fence(
    aggregate: AgentSessionAggregate,
    *,
    certificate: RecoveryCertificate,
) -> None:
    """Require the decoded aggregate to equal the raw certificate fence."""

    fence = certificate.aggregate_fence
    observed = (
        aggregate.state,
        aggregate.state_revision,
        aggregate.event_sequence,
        aggregate.activity_generation,
        aggregate.active_epoch,
        aggregate.current_plan_id,
        aggregate.review_plan_revision,
    )
    expected = (
        fence.state,
        fence.state_revision,
        fence.event_sequence,
        fence.activity_generation,
        fence.active_epoch,
        fence.current_plan_id,
        fence.review_plan_revision,
    )
    if observed != expected:
        raise RecoveryCommitAuthorityError("recovery_certificate_aggregate_fence_changed")


def _validate_materialized_transition(transition: SessionTransition) -> None:
    """Reject authority carriers from every materializer-persisted field."""

    if transition.recovery_commit_intent is not None:
        raise RecoveryCommitAuthorityError("recovery_materializer_returned_intent")
    if not isinstance(transition.result, dict):
        raise RecoveryCommitAuthorityError("recovery_materializer_result_invalid")
    try:
        validate_durable_json(transition.result, path="recovery_materializer.result")
    except DurableJSONValidationError as exc:
        raise RecoveryCommitAuthorityError("recovery_materializer_result_invalid") from exc
    try:
        persistence_carrier = asdict(transition)
    except Exception as exc:
        raise RecoveryCommitAuthorityError(
            "recovery_materializer_persistence_invalid"
        ) from exc
    try:
        validate_durable_json(
            persistence_carrier,
            path="recovery_materializer.persistence",
        )
        persistence_json = json.dumps(
            persistence_carrier,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        persistence_size = len(persistence_json.encode("utf-8", errors="strict"))
    except (DurableJSONValidationError, TypeError, UnicodeEncodeError, ValueError) as exc:
        raise RecoveryCommitAuthorityError(
            "recovery_materializer_persistence_invalid"
        ) from exc
    if persistence_size > MAX_RECOVERY_RAW_FIELD_BYTES:
        raise RecoveryCommitAuthorityError(
            "recovery_materializer_persistence_too_large"
        )
    if _contains_recovery_authority_record(persistence_carrier):
        raise RecoveryCommitAuthorityError(
            "recovery_materializer_persistence_leaks_authority"
        )
    try:
        metadata_json = json.dumps(
            {"result": transition.result},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        metadata_size = len(metadata_json.encode("utf-8", errors="strict"))
    except (TypeError, UnicodeEncodeError, ValueError) as exc:
        raise RecoveryCommitAuthorityError("recovery_materializer_result_invalid") from exc
    if metadata_size > MAX_RECOVERY_MATERIALIZATION_METADATA_BYTES:
        raise RecoveryCommitAuthorityError("recovery_materializer_result_too_large")


def _contains_recovery_authority_record(value: object) -> bool:
    """Detect a full certificate or delivery record in nested durable values."""

    stack = [value]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, RecoveryCertificate):
            return True
        if isinstance(current, Mapping):
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
            schema = current.get("schema")
            if schema in {RECOVERY_CERTIFICATE_SCHEMA, RECOVERY_DELIVERY_SCHEMA}:
                return True
            stack.extend(current.values())
        elif isinstance(current, (list, tuple)):
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
            stack.extend(current)
        elif isinstance(current, str):
            try:
                decoded = json.loads(current)
            except (TypeError, ValueError):
                continue
            if isinstance(decoded, (Mapping, list, tuple)):
                stack.append(decoded)
        elif is_dataclass(current) and not isinstance(current, type):
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
            stack.extend(getattr(current, field.name) for field in fields(current))
    return False


def _validate_case_delivery_fence(
    case: RecoveryCaseSnapshot,
    *,
    delivery: ValidatedClaimedRecoveryDelivery,
) -> None:
    """Compare every case-owned delivery fence before materialization."""

    payload = delivery.delivery
    certificate = payload.certificate
    if (
        case.case_id != payload.case_id
        or case.profile_id != certificate.subject.profile_id
        or case.session_id != certificate.subject.session_id
        or case.ownership_generation != certificate.subject.ownership_generation
        or case.certificate_version != certificate.version
        or case.policy_version != certificate.policy_version
        or case.work_graph_digest != certificate.work_graph_digest
        or case.latest_certificate_digest != certificate.certificate_digest
        or case.delivery_count != payload.delivery_cycle + 1
        or case.next_delivery_cycle != payload.delivery_cycle + 1
        or case.last_event_id != payload.event_id
    ):
        raise RecoveryCommitAuthorityError("recovery_case_delivery_fence_changed")
    if case.status == "open" and case.last_error:
        raise RecoveryCommitAuthorityError("recovery_open_case_has_error")


def _required_text(value: object, *, field_name: str) -> str:
    """Normalize one coordinator configuration identifier."""

    return _validate_bounded_text(value, field_name=field_name, allow_empty=False)


def _validate_bounded_text(
    value: object,
    *,
    field_name: str,
    allow_empty: bool,
) -> str:
    """Validate a small UTF-8 value that the raw reader can always reload."""

    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if normalized != value or (not allow_empty and not normalized):
        raise ValueError(f"{field_name} must be non-empty canonical text")
    try:
        encoded = normalized.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field_name} must contain valid UTF-8 text") from exc
    if len(encoded) > MAX_RECOVERY_TEXT_BYTES:
        raise ValueError(
            f"{field_name} exceeds the maximum recovery text byte size "
            f"of {MAX_RECOVERY_TEXT_BYTES}"
        )
    return normalized


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    """Validate one transaction timestamp before monotonic case updates."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite non-negative number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be a finite non-negative number")
    return normalized


def _next_monotonic_time(previous: float, candidate: float) -> float:
    """Return a finite timestamp strictly after a case semantic update."""

    normalized_previous = _nonnegative_finite(previous, field_name="case.updated_at")
    normalized_candidate = _nonnegative_finite(candidate, field_name="commit_now")
    if normalized_candidate > normalized_previous:
        return normalized_candidate
    result = math.nextafter(normalized_previous, math.inf)
    if not math.isfinite(result):
        raise RecoveryCommitAuthorityError("recovery_case_time_exhausted")
    return result


__all__ = [
    "PreparedRecoveryCommit",
    "RecoveryCaseSettlement",
    "RecoveryCommitAuthorityError",
    "RecoveryCommitDisposition",
    "RecoveryCommitResolution",
    "SQLiteRecoveryCommitCoordinator",
]
