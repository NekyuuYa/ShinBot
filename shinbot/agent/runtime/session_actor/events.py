"""Typed mailbox events and declarative session-actor transitions."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.message_ledger import (
    MessageLedgerMutation,
)


class MailboxEventStatus(StrEnum):
    """Durable processing state for one mailbox event."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class SessionOperationStatus(StrEnum):
    """Lifecycle state for durable long-running Agent work."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SUPERSEDED = "superseded"
    CANCELLED = "cancelled"


class ReviewScheduleStatus(StrEnum):
    """Lifecycle state for one durable review schedule."""

    SCHEDULED = "scheduled"
    CLAIMED = "claimed"
    COMPLETED = "completed"
    FAILED = "failed"
    SUPERSEDED = "superseded"


@dataclass(slots=True, frozen=True)
class SessionEventEnvelope:
    """One durable event addressed to a profile-scoped session actor."""

    event_id: str
    key: SessionKey
    kind: str
    ownership_generation: int = 0
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = ""
    occurred_at: float = 0.0
    causation_id: str = ""
    correlation_id: str = ""
    trace_id: str = ""
    available_at: float = 0.0
    created_at: float = 0.0

    def __post_init__(self) -> None:
        """Normalize required identifiers."""

        event_id = str(self.event_id or "").strip()
        kind = str(self.kind or "").strip()
        if not event_id:
            raise ValueError("event_id must not be empty")
        if not kind:
            raise ValueError("event kind must not be empty")
        _nonnegative_integer(
            self.ownership_generation,
            field_name="ownership_generation",
        )
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "kind", kind)
        for field_name in ("occurred_at", "available_at", "created_at"):
            object.__setattr__(
                self,
                field_name,
                _nonnegative_finite(getattr(self, field_name), field_name=field_name),
            )


@dataclass(slots=True, frozen=True)
class EventEnqueueResult:
    """Result of durably and idempotently submitting one mailbox event."""

    event_id: str
    key: SessionKey
    inserted: bool
    status: MailboxEventStatus = MailboxEventStatus.PENDING

    @property
    def duplicate(self) -> bool:
        """Return whether the event already existed in the durable mailbox."""

        return not self.inserted


@dataclass(slots=True, frozen=True)
class ClaimedSessionEvent:
    """Lease for one mailbox event claimed by a session actor."""

    claim_id: str
    envelope: SessionEventEnvelope
    worker_id: str
    attempt_count: int = 1
    claimed_at: float = 0.0
    lease_expires_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate claim identity and attempt count."""

        if not str(self.claim_id or "").strip():
            raise ValueError("claim_id must not be empty")
        if not str(self.worker_id or "").strip():
            raise ValueError("worker_id must not be empty")
        _positive_integer(self.attempt_count, field_name="attempt_count")
        object.__setattr__(
            self,
            "claimed_at",
            _nonnegative_finite(self.claimed_at, field_name="claimed_at"),
        )
        object.__setattr__(
            self,
            "lease_expires_at",
            _nonnegative_finite(
                self.lease_expires_at,
                field_name="lease_expires_at",
            ),
        )

    @property
    def key(self) -> SessionKey:
        """Return the destination actor key."""

        return self.envelope.key


@dataclass(slots=True, frozen=True)
class SessionEffect:
    """Durable work requested by a committed actor transition."""

    effect_id: str
    kind: str
    contract_version: int = 1
    contract_signature: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""
    operation_id: str = ""
    available_at: float = 0.0
    available_after_seconds: float | None = None

    def __post_init__(self) -> None:
        """Validate durable effect identity."""

        if not str(self.effect_id or "").strip():
            raise ValueError("effect_id must not be empty")
        if not str(self.kind or "").strip():
            raise ValueError("effect kind must not be empty")
        _positive_integer(
            self.contract_version,
            field_name="effect contract_version",
        )
        contract_signature = str(self.contract_signature or "").strip()
        if not contract_signature:
            raise ValueError("effect contract_signature must not be empty")
        object.__setattr__(self, "contract_signature", contract_signature)
        if not str(self.idempotency_key or "").strip():
            object.__setattr__(self, "idempotency_key", self.effect_id)
        object.__setattr__(
            self,
            "available_at",
            _nonnegative_finite(self.available_at, field_name="available_at"),
        )
        if self.available_after_seconds is not None:
            delay = _nonnegative_finite(
                self.available_after_seconds,
                field_name="available_after_seconds",
            )
            if self.available_at != 0:
                raise ValueError(
                    "available_at and available_after_seconds are mutually exclusive"
                )
            object.__setattr__(self, "available_after_seconds", delay)


@dataclass(slots=True, frozen=True)
class SessionOperation:
    """Durable mutation for one long-running Agent operation."""

    operation_id: str
    kind: str
    status: SessionOperationStatus = SessionOperationStatus.PENDING
    launched_by_event_id: str = ""
    state_revision: int | None = None
    active_epoch: int | None = None
    activity_generation: int | None = None
    input_watermark: int | None = None
    input_ledger_sequence: int | None = None
    started_at: float | None = None
    lease_owner: str = ""
    lease_until: float | None = None
    superseded_at: float | None = None
    finished_at: float | None = None
    failure_code: str = ""
    failure_message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate operation identity and monotonic guards."""

        if not str(self.operation_id or "").strip():
            raise ValueError("operation_id must not be empty")
        if not str(self.kind or "").strip():
            raise ValueError("operation kind must not be empty")
        for name in (
            "state_revision",
            "active_epoch",
            "activity_generation",
            "input_watermark",
            "input_ledger_sequence",
        ):
            value = getattr(self, name)
            _optional_nonnegative_integer(value, field_name=name)
        if self.input_watermark is None and self.input_ledger_sequence is not None:
            raise ValueError(
                "input_ledger_sequence requires a captured input_watermark"
            )
        for field_name in (
            "started_at",
            "lease_until",
            "superseded_at",
            "finished_at",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _nonnegative_finite(value, field_name=field_name),
                )

    def to_record(self) -> dict[str, object]:
        """Return a persistence mapping consumed by session actor stores."""

        return asdict(self)


@dataclass(slots=True, frozen=True)
class SessionReviewSchedule:
    """Declarative current-plan mutation committed with a session transition.

    ``scheduled_from`` and ``next_review_at`` are assigned by the durable
    store from the commit clock. Reducers provide only the applied delay so a
    model decision cannot be anchored to a stale pre-commit timestamp.
    """

    plan_id: str
    plan_revision: int
    applied_delay_seconds: float
    status: ReviewScheduleStatus = ReviewScheduleStatus.SCHEDULED
    trigger: str = ""
    outcome: str = ""
    source: str = ""
    requested_delay_seconds: float | None = None
    reason: str = ""
    fallback_reason: str = ""
    mention_sensitivity: str = "normal"
    active_reply_threshold: dict[str, Any] = field(default_factory=dict)
    model_execution_id: str = ""
    prompt_signature: str = ""
    expected_active_epoch: int | None = None
    expected_activity_generation: int | None = None
    committed_state_revision: int | None = None
    available_at: float | None = None
    claim_owner: str = ""
    claim_until: float | None = None
    attempt_count: int = 0
    last_error: str = ""
    created_at: float | None = None
    updated_at: float | None = None
    scheduled_from: float | None = field(default=None, init=False)
    next_review_at: float | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Validate plan identity, revision, and timing."""

        if not str(self.plan_id or "").strip():
            raise ValueError("plan_id must not be empty")
        _positive_integer(self.plan_revision, field_name="plan_revision")
        object.__setattr__(
            self,
            "applied_delay_seconds",
            _nonnegative_finite(
                self.applied_delay_seconds,
                field_name="applied_delay_seconds",
            ),
        )
        _nonnegative_integer(self.attempt_count, field_name="attempt_count")
        for field_name in (
            "requested_delay_seconds",
            "available_at",
            "claim_until",
            "created_at",
            "updated_at",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _nonnegative_finite(value, field_name=field_name),
                )
        for field_name in (
            "expected_active_epoch",
            "expected_activity_generation",
            "committed_state_revision",
        ):
            value = getattr(self, field_name)
            _optional_nonnegative_integer(value, field_name=field_name)

    def to_record(self) -> dict[str, object]:
        """Return a persistence mapping consumed by session actor stores."""

        return asdict(self)


@dataclass(slots=True, frozen=True)
class SessionReviewScheduleEvent:
    """Append-only explanation of one review scheduling decision."""

    schedule_event_id: str
    event_type: str
    plan_id: str = ""
    previous_plan_id: str = ""
    trigger: str = ""
    outcome: str = ""
    source: str = ""
    requested_delay_seconds: float | None = None
    applied_delay_seconds: float | None = None
    scheduled_from: float | None = None
    next_review_at: float | None = None
    reason: str = ""
    fallback_reason: str = ""
    model_execution_id: str = ""
    prompt_signature: str = ""
    expected_active_epoch: int | None = None
    expected_activity_generation: int | None = None
    committed_state_revision: int | None = None
    operation_id: str = ""
    trace_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float | None = None

    def __post_init__(self) -> None:
        """Validate append-only schedule event identity."""

        if not str(self.schedule_event_id or "").strip():
            raise ValueError("schedule_event_id must not be empty")
        if not str(self.event_type or "").strip():
            raise ValueError("schedule event type must not be empty")
        for field_name in (
            "requested_delay_seconds",
            "applied_delay_seconds",
            "scheduled_from",
            "next_review_at",
            "created_at",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _nonnegative_finite(value, field_name=field_name),
                )
        for field_name in (
            "expected_active_epoch",
            "expected_activity_generation",
            "committed_state_revision",
        ):
            value = getattr(self, field_name)
            _optional_nonnegative_integer(value, field_name=field_name)

    def to_record(self) -> dict[str, object]:
        """Return a persistence mapping consumed by session actor stores."""

        return asdict(self)


@dataclass(slots=True, frozen=True)
class SessionTransition:
    """Atomic state transition and effects produced by an event handler."""

    aggregate: AgentSessionAggregate
    disposition: str
    caused_operation_id: str = ""
    caused_plan_id: str = ""
    effects: tuple[SessionEffect, ...] = ()
    operations: tuple[SessionOperation, ...] = ()
    message_ledger_mutations: tuple[MessageLedgerMutation, ...] = ()
    review_schedules: tuple[SessionReviewSchedule, ...] = ()
    review_schedule_events: tuple[SessionReviewScheduleEvent, ...] = ()
    result: dict[str, Any] = field(default_factory=dict)
    reason: str = ""

    def __post_init__(self) -> None:
        """Normalize the explicit transition journal identity."""

        disposition = _normalized_text(
            self.disposition,
            field_name="transition disposition",
            required=True,
        )
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(
            self,
            "caused_operation_id",
            _normalized_text(
                self.caused_operation_id,
                field_name="caused_operation_id",
            ),
        )
        object.__setattr__(
            self,
            "caused_plan_id",
            _normalized_text(
                self.caused_plan_id,
                field_name="caused_plan_id",
            ),
        )


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    """Return one normalized durable timing value or reject it."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a JSON number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _nonnegative_integer(value: object, *, field_name: str) -> int:
    """Return one exact non-negative JSON integer without coercion."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must not be negative")
    return value


def _positive_integer(value: object, *, field_name: str) -> int:
    result = _nonnegative_integer(value, field_name=field_name)
    if result < 1:
        raise ValueError(f"{field_name} must be at least one")
    return result


def _optional_nonnegative_integer(
    value: object,
    *,
    field_name: str,
) -> int | None:
    if value is None:
        return None
    return _nonnegative_integer(value, field_name=field_name)


def _normalized_text(
    value: object,
    *,
    field_name: str,
    required: bool = False,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if required and not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


__all__ = [
    "ClaimedSessionEvent",
    "EventEnqueueResult",
    "MailboxEventStatus",
    "ReviewScheduleStatus",
    "SessionEffect",
    "SessionEventEnvelope",
    "SessionOperation",
    "SessionOperationStatus",
    "SessionReviewSchedule",
    "SessionReviewScheduleEvent",
    "SessionTransition",
]
