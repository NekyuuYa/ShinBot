"""Pure state reduction for the durable Agent session actor.

This module intentionally has no production runtime wiring.  It defines the
first actor-owned workflow slice: settling every active-chat exit through one
fenced idle-review-planning operation.
"""

from __future__ import annotations

import json
import math
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from enum import StrEnum
from typing import Any, ClassVar, Protocol

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    derived_effect_event_id,
)
from shinbot.agent.runtime.session_actor.events import (
    ReviewScheduleStatus,
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionOperationStatus,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
    ExternalActionReceiptStatus,
    builtin_external_action_effect_contract,
    materialize_external_action_effects,
)
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    ConsumeMessageLedgerEntries,
    MessageLedgerConsumptionKind,
    MessageLedgerConsumptionSelection,
    MessageWatermarkDisposition,
    append_message_ledger_entry_from_payload,
    classify_message_watermark,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    ActiveChatBootstrapCompletionResult,
    ActiveChatBootstrapDisposition,
    ActiveChatRoundCompletionResult,
    ActiveChatRoundOutcome,
    ActiveReplyCompletionResult,
    ReviewCompletionResult,
    ReviewNextReviewOutcome,
    ReviewNextReviewOutcomeKind,
    WorkflowCompletionCodecError,
)


class AgentSessionState(StrEnum):
    """Authoritative states owned by one session actor."""

    IDLE = "idle"
    REVIEW = "review"
    ACTIVE_REPLY = "active_reply"
    ACTIVE_CHAT = "active_chat"
    ACTIVE_CHAT_SETTLING = "active_chat_settling"


class AgentSessionEventKind(StrEnum):
    """Mailbox event kinds understood by the initial session reducer slice."""

    EXIT_REQUESTED = "ExitRequested"
    IDLE_REVIEW_PLANNING_COMPLETED = "IdleReviewPlanningCompleted"
    IDLE_REVIEW_PLANNING_DEADLINE_REACHED = (
        "IdleReviewPlanningDeadlineReached"
    )
    MESSAGE_RECEIVED = "MessageReceived"
    REVIEW_DUE = "ReviewDue"
    MANUAL_REVIEW_REQUESTED = "ManualReviewRequested"
    ACTIVE_REPLY_COMPLETED = "ActiveReplyCompleted"
    REVIEW_CANCELLATION_COMPLETED = "ReviewCancellationCompleted"
    REVIEW_COMPLETED = "ReviewCompleted"
    ACTIVE_CHAT_BOOTSTRAP_COMPLETED = "ActiveChatBootstrapCompleted"
    ACTIVE_CHAT_ROUND_DUE = "ActiveChatRoundDue"
    ACTIVE_CHAT_ROUND_COMPLETED = "ActiveChatRoundCompleted"
    ACTIVE_CHAT_TICK = "ActiveChatTick"
    EXTERNAL_ACTION_COMPLETED = "ExternalActionCompleted"
    EFFECT_FAILED = "EffectFailed"
    ACTIVE_CHAT_RUNTIME_STOPPED = "ActiveChatRuntimeStopped"
    IDLE_REVIEW_PLANNING_CANCELLATION_COMPLETED = (
        "IdleReviewPlanningCancellationCompleted"
    )
    ACTIVE_CHAT_RUNTIME_RECONCILED = "ActiveChatRuntimeReconciled"
    IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILED = (
        "IdleReviewPlanningCancellationReconciled"
    )


class AgentSessionEffectKind(StrEnum):
    """Durable effect kinds emitted by active-chat exit reduction."""

    RUN_IDLE_REVIEW_PLANNING = "run_idle_review_planning"
    ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE = (
        "enqueue_idle_review_planning_deadline"
    )
    CANCEL_IDLE_REVIEW_PLANNING = "cancel_idle_review_planning"
    STOP_ACTIVE_CHAT_RUNTIME = "stop_active_chat_runtime"
    RUN_REVIEW_WORKFLOW = "run_review_workflow"
    RUN_ACTIVE_REPLY_WORKFLOW = "run_active_reply_workflow"
    CANCEL_REVIEW_WORKFLOW = "cancel_review_workflow"
    ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST = "enqueue_active_chat_exit_request"
    ENQUEUE_ACTIVE_CHAT_ROUND_DUE = "enqueue_active_chat_round_due"
    RUN_ACTIVE_CHAT_BOOTSTRAP = "run_active_chat_bootstrap"
    RUN_ACTIVE_CHAT_ROUND = "run_active_chat_round"
    ACTIVE_CHAT_RUNTIME_RECONCILIATION = "active_chat_runtime_reconciliation"
    IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILIATION = (
        "idle_review_planning_cancellation_reconciliation"
    )


_CONTROL_INTENTS_DATA_KEY = "effect_control_intents"
_DELIVERY_CONTEXT_DATA_KEY = "delivery_context"
_PENDING_HIGH_PRIORITY_DATA_KEY = "pending_high_priority_message_log_ids"
_PENDING_OUTBOUND_DATA_KEY = "pending_outbound_actions"
_OUTBOUND_BLOCKED_DATA_KEY = "outbound_blocked"
_OUTBOUND_CONTINUATION_DATA_KEY = "outbound_continuation"
_REVIEW_CANCELLATION_BLOCKER_DATA_KEY = "review_cancellation_blocked"


class IdleReviewScheduleOutcomeKind(StrEnum):
    """Typed, exhaustive outcomes for an active-chat exit schedule."""

    PLANNED = "planned"
    DEFAULTED = "defaulted"
    FAILED = "failed"
    BYPASSED = "bypassed"
    SUPERSEDED = "superseded"


@dataclass(slots=True, frozen=True, kw_only=True)
class _SettledScheduleOutcome:
    """Shared fields for outcomes that commit a new review schedule."""

    applied_delay_seconds: float
    reason: str
    requested_delay_seconds: float | None = None
    fallback_reason: str = ""
    mention_sensitivity: str = "normal"
    active_reply_threshold: dict[str, Any] = field(default_factory=dict)
    model_execution_id: str = ""
    prompt_signature: str = ""
    source: str = "idle_review_planning"

    kind: ClassVar[IdleReviewScheduleOutcomeKind]

    def __post_init__(self) -> None:
        """Validate the relative timing carried into the atomic commit."""

        if not _is_nonnegative_finite(self.applied_delay_seconds):
            raise ValueError("applied_delay_seconds must be finite and non-negative")
        if self.requested_delay_seconds is not None and not _is_nonnegative_finite(
            self.requested_delay_seconds
        ):
            raise ValueError("requested_delay_seconds must be finite and non-negative")
        object.__setattr__(self, "reason", str(self.reason or "").strip())
        object.__setattr__(
            self,
            "active_reply_threshold",
            dict(self.active_reply_threshold),
        )

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-compatible representation for journals and effects."""

        payload = asdict(self)
        payload["kind"] = self.kind.value
        return payload


@dataclass(slots=True, frozen=True, kw_only=True)
class Planned(_SettledScheduleOutcome):
    """A planner supplied a usable delay which the policy accepted."""

    kind: ClassVar[IdleReviewScheduleOutcomeKind] = (
        IdleReviewScheduleOutcomeKind.PLANNED
    )

    def __post_init__(self) -> None:
        """Require the model-requested delay for a planned outcome."""

        super(Planned, self).__post_init__()
        if self.requested_delay_seconds is None:
            raise ValueError("a planned outcome requires requested_delay_seconds")


@dataclass(slots=True, frozen=True, kw_only=True)
class Defaulted(_SettledScheduleOutcome):
    """The planner requested the configured default review delay."""

    kind: ClassVar[IdleReviewScheduleOutcomeKind] = (
        IdleReviewScheduleOutcomeKind.DEFAULTED
    )


@dataclass(slots=True, frozen=True, kw_only=True)
class Failed(_SettledScheduleOutcome):
    """Planning failed and the reducer applied a bounded fallback delay."""

    failure_code: str = "idle_review_planning_failed"
    failure_message: str = ""
    kind: ClassVar[IdleReviewScheduleOutcomeKind] = IdleReviewScheduleOutcomeKind.FAILED


@dataclass(slots=True, frozen=True, kw_only=True)
class Bypassed(_SettledScheduleOutcome):
    """The caller intentionally bypassed model planning."""

    kind: ClassVar[IdleReviewScheduleOutcomeKind] = (
        IdleReviewScheduleOutcomeKind.BYPASSED
    )


@dataclass(slots=True, frozen=True, kw_only=True)
class Superseded:
    """A completion or pending exit lost its fencing race."""

    reason: str
    operation_id: str
    plan_id: str
    expected_active_epoch: int | None
    expected_activity_generation: int | None
    actual_active_epoch: int
    actual_activity_generation: int
    actual_state: str
    active_operation_id: str
    kind: ClassVar[IdleReviewScheduleOutcomeKind] = (
        IdleReviewScheduleOutcomeKind.SUPERSEDED
    )

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-compatible representation for the stale journal."""

        payload = asdict(self)
        payload["kind"] = self.kind.value
        return payload


type SettledScheduleOutcome = Planned | Defaulted | Failed | Bypassed
type IdleReviewScheduleOutcome = SettledScheduleOutcome | Superseded


class ReducerIdFactory(Protocol):
    """Create deterministic durable identifiers without performing I/O."""

    def create(self, *, key: SessionKey, seed: str, purpose: str) -> str:
        """Create one stable identifier for the supplied logical seed."""


@dataclass(slots=True, frozen=True)
class DeterministicReducerIdFactory:
    """UUID5 identifier factory scoped by profile, session, seed, and purpose."""

    namespace: uuid.UUID = uuid.UUID("05ad8d62-9722-5410-9255-47a1bc9038f7")

    def create(self, *, key: SessionKey, seed: str, purpose: str) -> str:
        """Create a stable identifier that is globally safe across sessions."""

        normalized_seed = str(seed or "").strip()
        normalized_purpose = str(purpose or "").strip()
        if not normalized_seed:
            raise ValueError("deterministic id seed must not be empty")
        if not normalized_purpose:
            raise ValueError("deterministic id purpose must not be empty")
        identity = json.dumps(
            [key.profile_id, key.session_id, normalized_seed, normalized_purpose],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        digest = uuid.uuid5(self.namespace, identity).hex
        return f"{normalized_purpose}:{digest}"


@dataclass(slots=True, frozen=True)
class IdleExitReducerConfig:
    """Pure policy inputs for active-chat exit settlement."""

    default_review_delay_seconds: float = 900.0
    minimum_review_delay_seconds: float = 0.0
    maximum_review_delay_seconds: float = 604_800.0
    planning_deadline_seconds: float = 30.0
    control_reconciliation_max_cycles: int = 2
    busy_review_retry_seconds: float = 30.0
    provisional_active_chat_interest: float = 15.0
    provisional_active_chat_half_life_seconds: float = 20.0
    active_chat_semantic_wait_seconds: float = 1.5
    active_chat_tick_interval_seconds: float = 5.0
    active_chat_idle_interest_threshold: float = 5.0
    active_chat_max_interest: float = 100.0

    def __post_init__(self) -> None:
        """Validate delay bounds once at reducer construction."""

        values = {
            "default_review_delay_seconds": self.default_review_delay_seconds,
            "minimum_review_delay_seconds": self.minimum_review_delay_seconds,
            "maximum_review_delay_seconds": self.maximum_review_delay_seconds,
            "planning_deadline_seconds": self.planning_deadline_seconds,
            "busy_review_retry_seconds": self.busy_review_retry_seconds,
            "provisional_active_chat_interest": (
                self.provisional_active_chat_interest
            ),
            "provisional_active_chat_half_life_seconds": (
                self.provisional_active_chat_half_life_seconds
            ),
            "active_chat_semantic_wait_seconds": (
                self.active_chat_semantic_wait_seconds
            ),
            "active_chat_tick_interval_seconds": (
                self.active_chat_tick_interval_seconds
            ),
            "active_chat_idle_interest_threshold": (
                self.active_chat_idle_interest_threshold
            ),
            "active_chat_max_interest": self.active_chat_max_interest,
        }
        for name, value in values.items():
            if not _is_nonnegative_finite(value):
                raise ValueError(f"{name} must be finite and non-negative")
        if self.maximum_review_delay_seconds < self.minimum_review_delay_seconds:
            raise ValueError("maximum review delay cannot be below minimum review delay")
        if not (
            self.minimum_review_delay_seconds
            <= self.default_review_delay_seconds
            <= self.maximum_review_delay_seconds
        ):
            raise ValueError("default review delay must be within the configured bounds")
        if self.planning_deadline_seconds <= 0:
            raise ValueError("planning_deadline_seconds must be positive")
        if self.provisional_active_chat_half_life_seconds <= 0:
            raise ValueError(
                "provisional_active_chat_half_life_seconds must be positive"
            )
        if self.active_chat_tick_interval_seconds <= 0:
            raise ValueError("active_chat_tick_interval_seconds must be positive")
        if self.active_chat_max_interest <= 0:
            raise ValueError("active_chat_max_interest must be positive")
        if self.active_chat_idle_interest_threshold > self.active_chat_max_interest:
            raise ValueError(
                "active_chat_idle_interest_threshold cannot exceed active_chat_max_interest"
            )
        if (
            not isinstance(self.control_reconciliation_max_cycles, int)
            or isinstance(self.control_reconciliation_max_cycles, bool)
            or self.control_reconciliation_max_cycles < 1
        ):
            raise ValueError("control_reconciliation_max_cycles must be at least one")

    def apply_delay(self, delay_seconds: float) -> float:
        """Clamp a relative delay to the configured review policy bounds."""

        if not _is_nonnegative_finite(delay_seconds):
            raise ValueError("review delay must be finite and non-negative")
        return min(
            self.maximum_review_delay_seconds,
            max(self.minimum_review_delay_seconds, float(delay_seconds)),
        )


class AgentSessionReducer:
    """Synchronously reduce actor events into atomic durable transitions.

    The reducer never awaits and never reads a clock, database, model, adapter,
    or tool.  Event timestamps and deterministic IDs are its only sources of
    time and identity.
    """

    def __init__(
        self,
        *,
        config: IdleExitReducerConfig | None = None,
        id_factory: ReducerIdFactory | None = None,
    ) -> None:
        """Initialize the pure reducer and its deterministic policy inputs."""

        self._config = config or IdleExitReducerConfig()
        self._ids = id_factory or DeterministicReducerIdFactory()

    def __call__(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Reduce one mailbox event using the session actor handler contract."""

        return self.reduce(aggregate, event)

    def reduce(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Return a declarative transition for one mailbox event."""

        if aggregate.key != event.key:
            raise ValueError("session event key does not match aggregate ownership")
        if event.kind == AgentSessionEventKind.EXIT_REQUESTED:
            return self._request_exit(aggregate, event)
        if event.kind == AgentSessionEventKind.MESSAGE_RECEIVED:
            return self._receive_message(aggregate, event)
        if event.kind == AgentSessionEventKind.REVIEW_DUE:
            return self._review_due(aggregate, event)
        if event.kind == AgentSessionEventKind.ACTIVE_REPLY_COMPLETED:
            return self._active_reply_completed(aggregate, event)
        if event.kind == AgentSessionEventKind.REVIEW_CANCELLATION_COMPLETED:
            return self._review_cancellation_completed(aggregate, event)
        if event.kind == AgentSessionEventKind.REVIEW_COMPLETED:
            return self._review_completed(aggregate, event)
        if event.kind == AgentSessionEventKind.ACTIVE_CHAT_BOOTSTRAP_COMPLETED:
            return self._active_chat_bootstrap_completed(aggregate, event)
        if event.kind == AgentSessionEventKind.ACTIVE_CHAT_ROUND_DUE:
            return self._active_chat_round_due(aggregate, event)
        if event.kind == AgentSessionEventKind.ACTIVE_CHAT_ROUND_COMPLETED:
            return self._active_chat_round_completed(aggregate, event)
        if event.kind == AgentSessionEventKind.ACTIVE_CHAT_TICK:
            return self._active_chat_tick(aggregate, event)
        if event.kind == AgentSessionEventKind.EXTERNAL_ACTION_COMPLETED:
            return self._external_action_completed(aggregate, event)
        if event.kind == AgentSessionEventKind.EFFECT_FAILED:
            return self._effect_failed(aggregate, event)
        if event.kind in {
            AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_STOPPED,
            AgentSessionEventKind.IDLE_REVIEW_PLANNING_CANCELLATION_COMPLETED,
        }:
            return self._control_completed(aggregate, event)
        if event.kind in {
            AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_RECONCILED,
            AgentSessionEventKind.IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILED,
        }:
            return self._control_reconciled(aggregate, event)
        if event.kind in {
            AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED,
            AgentSessionEventKind.IDLE_REVIEW_PLANNING_DEADLINE_REACHED,
        }:
            return self._settle_or_supersede(aggregate, event)
        return self._ignored(
            aggregate,
            event,
            disposition="ignored_unsupported_event",
            reason=f"unsupported_event:{event.kind}",
        )

    def resolve_schedule_outcome(
        self,
        event: SessionEventEnvelope,
        *,
        deadline_reached: bool = False,
    ) -> SettledScheduleOutcome:
        """Resolve serialized planner output into one exhaustive typed outcome."""

        values = _planner_outcome_values(event.payload)
        source = _text(event.payload.get("source")) or event.source or "idle_review_planning"
        model_execution_id = _text(event.payload.get("model_execution_id"))
        prompt_signature = _text(event.payload.get("prompt_signature"))
        if deadline_reached:
            return Failed(
                applied_delay_seconds=self._config.default_review_delay_seconds,
                reason="idle_review_planning_deadline_reached",
                fallback_reason="planner_deadline_reached",
                failure_code="planner_deadline_reached",
                failure_message=_text(event.payload.get("failure_message")),
                model_execution_id=model_execution_id,
                prompt_signature=prompt_signature,
                source=source,
            )

        raw_kind = _text(values.get("kind") or values.get("outcome_kind")).lower()
        try:
            kind = IdleReviewScheduleOutcomeKind(raw_kind)
        except ValueError:
            return self._invalid_outcome(
                event,
                failure_message=f"unsupported planner outcome: {raw_kind or '<empty>'}",
            )

        requested = _optional_delay(values.get("requested_delay_seconds"))
        if kind is IdleReviewScheduleOutcomeKind.PLANNED:
            if requested is None:
                return self._invalid_outcome(
                    event,
                    failure_message="planned outcome omitted a valid requested delay",
                )
            applied = self._config.apply_delay(requested)
        elif kind is IdleReviewScheduleOutcomeKind.BYPASSED:
            bypass_delay = event.payload.get(
                "bypass_delay_seconds",
                event.payload.get("applied_delay_seconds"),
            )
            if bypass_delay is None:
                applied = self._config.default_review_delay_seconds
            else:
                parsed_bypass_delay = _optional_delay(bypass_delay)
                if parsed_bypass_delay is None:
                    return self._invalid_outcome(
                        event,
                        failure_message="invalid trusted bypass delay",
                    )
                applied = self._config.apply_delay(parsed_bypass_delay)
            requested = None
        else:
            applied = self._config.default_review_delay_seconds
            requested = None

        common: dict[str, Any] = {
            "applied_delay_seconds": applied,
            "requested_delay_seconds": requested,
            "reason": _text(values.get("reason")) or f"idle_review_{kind.value}",
            "fallback_reason": "",
            "mention_sensitivity": (
                _text(values.get("mention_sensitivity")) or "normal"
            ),
            "active_reply_threshold": _mapping(
                values.get("active_reply_threshold")
            ),
            "model_execution_id": model_execution_id,
            "prompt_signature": prompt_signature,
            "source": source,
        }
        if kind is IdleReviewScheduleOutcomeKind.PLANNED:
            return Planned(**common)
        if kind is IdleReviewScheduleOutcomeKind.DEFAULTED:
            common["fallback_reason"] = (
                common["fallback_reason"] or "planner_requested_default"
            )
            return Defaulted(**common)
        if kind is IdleReviewScheduleOutcomeKind.FAILED:
            common["fallback_reason"] = (
                common["fallback_reason"] or "idle_review_planning_failed"
            )
            return Failed(
                **common,
                failure_code=(
                    _text(event.payload.get("failure_code"))
                    or "idle_review_planning_failed"
                ),
                failure_message=_text(event.payload.get("failure_message")),
            )
        if kind is IdleReviewScheduleOutcomeKind.BYPASSED:
            common["fallback_reason"] = (
                common["fallback_reason"] or "idle_review_planning_bypassed"
            )
            return Bypassed(**common)
        return self._invalid_outcome(
            event,
            failure_message="superseded is not a settlement completion outcome",
        )

    def _request_exit(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        exit_effect_kind = AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
        exit_intent = _control_intent(aggregate.data, exit_effect_kind)
        mismatch = (
            self._active_chat_exit_request_control_mismatch(
                aggregate,
                event,
                intent=exit_intent,
                expected_event_field="completion_event_id",
            )
            if exit_intent
            else _active_chat_exit_request_mismatch(aggregate, event)
        )
        if mismatch:
            return self._ignored(
                aggregate,
                event,
                disposition="ignored_stale_active_chat_exit_request",
                reason=",".join(mismatch),
            )
        if aggregate.state != AgentSessionState.ACTIVE_CHAT:
            return self._ignored(
                aggregate,
                event,
                disposition="ignored_exit_outside_active_chat",
                reason=f"exit_requested_from:{aggregate.state}",
            )

        event_time = _event_time(aggregate, event)

        operation_id = self._payload_or_generated_id(
            event,
            field_name="operation_id",
            seed=event.event_id,
            purpose="idle-planning-operation",
        )
        plan_id = self._payload_or_generated_id(
            event,
            field_name="plan_id",
            seed=operation_id,
            purpose="idle-review-plan",
        )
        completion_event_id = self._payload_or_generated_id(
            event,
            field_name="completion_event_id",
            seed=operation_id,
            purpose="idle-planning-completion-event",
        )
        deadline_event_id = self._payload_or_generated_id(
            event,
            field_name="deadline_event_id",
            seed=operation_id,
            purpose="idle-planning-deadline-event",
        )
        planner_effect_id = self._payload_or_generated_id(
            event,
            field_name="planner_effect_id",
            seed=operation_id,
            purpose="run-idle-planning-effect",
        )
        planner_failure_event_id = self._payload_or_generated_id(
            event,
            field_name="planner_failure_event_id",
            seed=planner_effect_id,
            purpose="run-idle-planning-failure-event",
        )
        planner_idempotency_key = (
            _text(event.payload.get("planner_idempotency_key"))
            or planner_effect_id
        )
        deadline_effect_id = self._payload_or_generated_id(
            event,
            field_name="deadline_effect_id",
            seed=operation_id,
            purpose="idle-planning-deadline-effect",
        )
        deadline_failure_event_id = self._payload_or_generated_id(
            event,
            field_name="deadline_failure_event_id",
            seed=deadline_effect_id,
            purpose="idle-planning-deadline-failure-event",
        )
        deadline_idempotency_key = (
            _text(event.payload.get("deadline_idempotency_key"))
            or deadline_effect_id
        )
        deadline_delay_seconds = self._config.planning_deadline_seconds
        input_watermark = _message_watermark(aggregate.data)
        trigger = _text(event.payload.get("trigger")) or "active_chat_exit"
        source = _text(event.payload.get("source")) or event.source or "session_actor"
        fence = {
            "operation_id": operation_id,
            "plan_id": plan_id,
            "active_epoch": aggregate.active_epoch,
            "activity_generation": aggregate.activity_generation,
            "input_watermark": input_watermark,
        }
        operation_fence = {
            **fence,
            "operation_kind": "idle_review_planning",
            "source_event_id": event.event_id,
            "ownership_generation": aggregate.ownership_generation,
            "input_ledger_sequence": None,
        }
        pending_exit = {
            **fence,
            "ownership_generation": aggregate.ownership_generation,
            "requested_at": event.occurred_at,
            "deadline_delay_seconds": deadline_delay_seconds,
            "trigger": trigger,
            "source": source,
            "requested_by_event_id": event.event_id,
            "planner_effect_id": planner_effect_id,
            "planner_idempotency_key": planner_idempotency_key,
            "planner_failure_event_id": planner_failure_event_id,
            "deadline_effect_id": deadline_effect_id,
            "deadline_idempotency_key": deadline_idempotency_key,
            "deadline_failure_event_id": deadline_failure_event_id,
            "completion_event_id": completion_event_id,
            "deadline_event_id": deadline_event_id,
        }
        data = _with_completed_control_intent(
            aggregate.data,
            effect_kind=exit_effect_kind,
            intent=exit_intent,
            event=event,
        )
        data = _with_operation_fence(
            data,
            operation_id,
            operation_fence,
        )
        data["idle_exit"] = pending_exit
        target = aggregate.advance(
            state=AgentSessionState.ACTIVE_CHAT_SETTLING.value,
            idle_planning_operation_id=operation_id,
            data=data,
            updated_at=event_time,
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="idle_review_planning",
            status=SessionOperationStatus.PENDING,
            launched_by_event_id=event.event_id,
            state_revision=target.state_revision,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            started_at=event.occurred_at,
            metadata={
                **pending_exit,
                "correlation_id": event.correlation_id,
                "trace_id": event.trace_id,
            },
        )
        planner_effect = _durable_effect(
            effect_id=planner_effect_id,
            kind=AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
            idempotency_key=planner_idempotency_key,
            operation_id=operation_id,
            available_after_seconds=0.0,
            payload={
                **operation_fence,
                "completion_event_id": completion_event_id,
                "failure_event_id": planner_failure_event_id,
                "planning_input": _mapping(event.payload.get("planning_input")),
                "trigger": trigger,
                "source": source,
            },
        )
        deadline_effect = _durable_effect(
            effect_id=deadline_effect_id,
            kind=AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
            idempotency_key=deadline_idempotency_key,
            operation_id=operation_id,
            available_after_seconds=deadline_delay_seconds,
            payload={
                **operation_fence,
                "deadline_event_id": deadline_event_id,
                "failure_event_id": deadline_failure_event_id,
                "deadline_delay_seconds": deadline_delay_seconds,
                "trigger": trigger,
                "source": source,
                # The executor must re-read the operation before enqueueing the
                # deadline event. A terminal operation makes this effect a no-op.
                "enqueue_only_if_operation_status": ["pending", "running"],
                "terminal_operation_disposition": "skip",
            },
        )
        return SessionTransition(
            aggregate=target,
            disposition="active_chat_exit_settling",
            caused_operation_id=operation_id,
            caused_plan_id=plan_id,
            effects=(planner_effect, deadline_effect),
            operations=(operation,),
            result={
                **fence,
                "deadline_delay_seconds": deadline_delay_seconds,
                "deadline_effect_contract": "skip_when_operation_terminal",
            },
            reason=trigger,
        )

    def _active_chat_exit_request_control_mismatch(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        intent: Mapping[str, Any],
        expected_event_field: str,
    ) -> tuple[str, ...]:
        """Validate an actor-owned exit request before changing chat state."""

        effect_kind = AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
        mismatch = list(
            self._control_effect_event_mismatch(
                aggregate,
                event,
                intent=intent,
                effect_kind=effect_kind,
                expected_event_field=expected_event_field,
            )
        )
        expected_watermark = _optional_nonnegative_int(
            intent.get("expected_message_watermark")
        )
        if (
            expected_watermark is None
            or expected_watermark != _message_watermark(aggregate.data)
        ):
            mismatch.append("message_watermark_changed")
        if _optional_nonnegative_int(event.payload.get("expected_message_watermark")) != (
            expected_watermark
        ):
            mismatch.append("expected_message_watermark_changed")
        if _optional_nonnegative_int(event.payload.get("expected_active_epoch")) != (
            _optional_nonnegative_int(intent.get("expected_active_epoch"))
        ):
            mismatch.append("expected_active_epoch_changed")
        if expected_event_field == "failure_event_id":
            if not _text(event.payload.get("failure_code")):
                mismatch.append("failure_code_missing")
            if not isinstance(event.payload.get("failure_message"), str):
                mismatch.append("failure_message_invalid")
        return tuple(dict.fromkeys(mismatch))

    def _receive_message(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        response_profile = event.payload.get("response_profile", "")
        if not isinstance(response_profile, str):
            raise TypeError("MessageReceived response_profile must be a string")
        append = append_message_ledger_entry_from_payload(
            event.payload,
            key=event.key,
            ownership_generation=event.ownership_generation,
            source_event_id=event.event_id,
            event_source=event.source,
            occurred_at=event.occurred_at,
            event_created_at=event.created_at,
            causation_id=event.causation_id,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            response_profile=response_profile,
        )
        data = _with_message_delivery(aggregate.data, append)
        if not append.eligible_for_work:
            return self._message_recorded(
                aggregate,
                event,
                append=append,
                data=data,
                disposition="message_recorded_suppressed",
                reason=append.suppression_reason,
            )
        if aggregate.state in {
            AgentSessionState.IDLE,
            AgentSessionState.REVIEW,
        } and append.priority.should_wake_active_reply:
            if _review_cancellation_blocks_active_reply(data):
                return self._message_recorded(
                    aggregate,
                    event,
                    append=append,
                    data=data,
                    disposition="message_recorded_active_reply_blocked",
                    reason="review_cancellation_blocked",
                )
            if _has_unsettled_pending_outbound(data):
                return self._message_recorded(
                    aggregate,
                    event,
                    append=append,
                    data=data,
                    disposition="message_recorded_waiting_outbound",
                    reason="high_priority_message_waiting_for_external_actions",
                )
            return self._start_active_reply(
                aggregate,
                event,
                append=append,
                data=data,
            )
        if aggregate.state == AgentSessionState.ACTIVE_CHAT:
            return self._record_active_chat_message(
                aggregate,
                event,
                append=append,
                data=data,
            )
        if aggregate.state != AgentSessionState.ACTIVE_CHAT_SETTLING:
            return self._message_recorded(
                aggregate,
                event,
                append=append,
                data=data,
                disposition="message_recorded",
                reason=f"message_received_in:{aggregate.state}",
            )

        operation_id = aggregate.idle_planning_operation_id
        plan_id = self._pending_plan_id(aggregate, operation_id)
        pending = _mapping(aggregate.data.get("idle_exit"))
        operation_fence = _operation_fence(aggregate.data, operation_id)
        input_watermark = _optional_nonnegative_int(
            operation_fence.get("input_watermark")
        )
        input_ledger_sequence = _optional_nonnegative_int(
            operation_fence.get("input_ledger_sequence")
        )
        if input_watermark is not None and (
            classify_message_watermark(
                append.message_log_id,
                input_watermark=input_watermark,
            )
            is MessageWatermarkDisposition.CAPTURED_OR_LATE
        ):
            return self._message_recorded(
                aggregate,
                event,
                append=append,
                data=data,
                disposition="late_snapshot_message_recorded",
                reason="message_within_idle_planning_input_watermark",
            )
        data = _without_operation_fence(
            _without_idle_exit(data),
            operation_id,
        )
        cancel_effect_id = ""
        cancel_idempotency_key = ""
        cancel_completion_event_id = ""
        cancel_failure_event_id = ""
        if operation_id:
            cancel_effect_id = self._payload_or_generated_id(
                event,
                field_name="cancel_effect_id",
                seed=operation_id,
                purpose="cancel-idle-planning-effect",
            )
            cancel_idempotency_key = (
                _text(event.payload.get("cancel_idempotency_key"))
                or cancel_effect_id
            )
            cancel_completion_event_id = self._payload_or_generated_id(
                event,
                field_name="cancel_completion_event_id",
                seed=cancel_effect_id,
                purpose="cancel-idle-planning-completion-event",
            )
            cancel_failure_event_id = self._payload_or_generated_id(
                event,
                field_name="cancel_failure_event_id",
                seed=cancel_effect_id,
                purpose="cancel-idle-planning-failure-event",
            )
            data = _with_control_intent(
                data,
                effect_kind=AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
                intent={
                    "desired_state": "cancelled",
                    "status": "requested",
                    "effect_id": cancel_effect_id,
                    "effect_kind": (
                        AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
                    ),
                    "idempotency_key": cancel_idempotency_key,
                    "completion_event_id": cancel_completion_event_id,
                    "failure_event_id": cancel_failure_event_id,
                    "operation_id": operation_id,
                    "plan_id": plan_id,
                    "active_epoch": aggregate.active_epoch,
                    "activity_generation": aggregate.activity_generation,
                    "input_watermark": input_watermark,
                    "input_ledger_sequence": input_ledger_sequence,
                    "ownership_generation": aggregate.ownership_generation,
                    "causation_id": event.event_id,
                    "expected_state": AgentSessionState.ACTIVE_CHAT.value,
                    "expected_active_epoch": aggregate.active_epoch,
                    "expected_activity_generation": (
                        aggregate.activity_generation + 1
                    ),
                },
            )
        target = aggregate.advance(
            state=AgentSessionState.ACTIVE_CHAT.value,
            activity_generation=aggregate.activity_generation + 1,
            idle_planning_operation_id="",
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        operations: tuple[SessionOperation, ...] = ()
        effects: tuple[SessionEffect, ...] = ()
        schedule_events: tuple[SessionReviewScheduleEvent, ...] = ()
        if operation_id:
            operations = (
                SessionOperation(
                    operation_id=operation_id,
                    kind="idle_review_planning",
                    status=SessionOperationStatus.SUPERSEDED,
                    state_revision=aggregate.state_revision,
                    active_epoch=aggregate.active_epoch,
                    activity_generation=aggregate.activity_generation,
                    superseded_at=event.occurred_at,
                    metadata={
                        "idle_exit": pending,
                        "reason": "message_received_while_settling",
                        "superseded_by_event_id": event.event_id,
                    },
                ),
            )
            effects = (
                _durable_effect(
                    effect_id=cancel_effect_id,
                    kind=AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
                    idempotency_key=cancel_idempotency_key,
                    operation_id=operation_id,
                    payload={
                        "operation_id": operation_id,
                        "plan_id": plan_id,
                        "active_epoch": aggregate.active_epoch,
                        "activity_generation": aggregate.activity_generation,
                        "input_watermark": input_watermark,
                        "input_ledger_sequence": input_ledger_sequence,
                        "completion_event_id": cancel_completion_event_id,
                        "failure_event_id": cancel_failure_event_id,
                        "superseded_by_event_id": event.event_id,
                    },
                ),
            )
            schedule_events = (
                self._superseded_schedule_event(
                    aggregate,
                    event,
                    Superseded(
                        reason="message_received_while_settling",
                        operation_id=operation_id,
                        plan_id=plan_id,
                        expected_active_epoch=aggregate.active_epoch,
                        expected_activity_generation=aggregate.activity_generation,
                        actual_active_epoch=aggregate.active_epoch,
                        actual_activity_generation=target.activity_generation,
                        actual_state=target.state,
                        active_operation_id="",
                    ),
                ),
            )
        return SessionTransition(
            aggregate=target,
            disposition="active_chat_exit_cancelled",
            caused_operation_id=operation_id,
            caused_plan_id=plan_id,
            effects=effects,
            operations=operations,
            message_ledger_mutations=(append,),
            review_schedule_events=schedule_events,
            result={
                "operation_id": operation_id,
                "plan_id": plan_id,
                "activity_generation": target.activity_generation,
                "outcome": IdleReviewScheduleOutcomeKind.SUPERSEDED.value,
                "deadline_effect_contract": "skip_when_operation_terminal",
            },
            reason="message_received_while_settling",
        )

    def _message_recorded(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        append: AppendMessageLedgerEntry,
        data: Mapping[str, Any],
        disposition: str,
        reason: str,
    ) -> SessionTransition:
        normalized_data = dict(data)
        state_changed = normalized_data != dict(aggregate.data)
        target = aggregate.advance(
            state_changed=state_changed,
            data=normalized_data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition=disposition,
            message_ledger_mutations=(append,),
            result={"message_log_id": append.message_log_id},
            reason=reason,
        )

    def _start_active_reply(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        append: AppendMessageLedgerEntry,
        data: Mapping[str, Any],
    ) -> SessionTransition:
        review_operation_id = aggregate.review_operation_id
        cancel_effect_id = ""
        cancel_completion_event_id = ""
        cancel_failure_event_id = ""
        if aggregate.state == AgentSessionState.REVIEW and review_operation_id:
            cancel_effect_id = self._ids.create(
                key=event.key,
                seed=f"{review_operation_id}:{event.event_id}",
                purpose="cancel-review-effect",
            )
            cancel_completion_event_id = self._ids.create(
                key=event.key,
                seed=cancel_effect_id,
                purpose="cancel-review-completion-event",
            )
            cancel_failure_event_id = self._ids.create(
                key=event.key,
                seed=cancel_effect_id,
                purpose="cancel-review-failure-event",
            )
        operation_id = self._ids.create(
            key=event.key,
            seed=event.event_id,
            purpose="active-reply-operation",
        )
        effect_id = self._ids.create(
            key=event.key,
            seed=operation_id,
            purpose="run-active-reply-effect",
        )
        completion_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-reply-completion-event",
        )
        failure_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-reply-failure-event",
        )
        activity_generation = aggregate.activity_generation + 1
        input_watermark = _message_watermark(data)
        delivery_context = _mapping(data.get(_DELIVERY_CONTEXT_DATA_KEY))
        operation_fence = {
            "operation_id": operation_id,
            "operation_kind": "active_reply",
            "source_event_id": cancel_completion_event_id or event.event_id,
            "effect_id": effect_id,
            "effect_kind": AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW,
            **_effect_contract_snapshot(
                AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW
            ),
            "idempotency_key": effect_id,
            "completion_event_id": completion_event_id,
            "failure_event_id": failure_event_id,
            "ownership_generation": aggregate.ownership_generation,
            "plan_id": aggregate.current_plan_id,
            "active_epoch": aggregate.active_epoch,
            "activity_generation": activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": None,
            "instance_id": _text(delivery_context.get("instance_id")),
            "target_session_id": _text(
                delivery_context.get("target_session_id")
            ),
            "message_log_ids": [append.message_log_id],
            "response_profile": append.response_profile,
            "sender_id": append.sender_id,
        }
        next_data = _with_operation_fence(data, operation_id, operation_fence)
        operations: list[SessionOperation] = []
        effects: list[SessionEffect] = []
        resume: dict[str, Any] = {}
        awaiting_review_cancellation = False
        if aggregate.state == AgentSessionState.REVIEW:
            resume = {
                "kind": "resume_interrupted_review",
                "plan_id": aggregate.current_plan_id,
                "plan_revision": aggregate.review_plan_revision,
                "interrupted_operation_id": review_operation_id,
                "interrupted_by_event_id": event.event_id,
            }
            if review_operation_id:
                review_fence = _operation_fence(
                    aggregate.data,
                    review_operation_id,
                )
                cancel_contract = builtin_effect_contract(
                    AgentSessionEffectKind.CANCEL_REVIEW_WORKFLOW
                )
                operations.append(
                    SessionOperation(
                        operation_id=review_operation_id,
                        kind="review",
                        status=SessionOperationStatus.SUPERSEDED,
                        superseded_at=event.occurred_at,
                        metadata={
                            "reason": "high_priority_message_interrupted_review",
                            "superseded_by_event_id": event.event_id,
                        },
                    )
                )
                cancel_input_watermark = _optional_nonnegative_int(
                    review_fence.get("input_watermark")
                )
                cancel_input_ledger_sequence = _optional_nonnegative_int(
                    review_fence.get("input_ledger_sequence")
                )
                next_data = _without_operation_fence(
                    next_data,
                    review_operation_id,
                )
                next_data = _with_control_intent(
                    next_data,
                    effect_kind=AgentSessionEffectKind.CANCEL_REVIEW_WORKFLOW,
                    intent={
                        "desired_state": "cancelled",
                        "status": "requested",
                        "effect_id": cancel_effect_id,
                        "effect_kind": (
                            AgentSessionEffectKind.CANCEL_REVIEW_WORKFLOW
                        ),
                        "idempotency_key": cancel_effect_id,
                        "contract_version": cancel_contract.version,
                        "contract_signature": cancel_contract.signature,
                        "completion_event_id": cancel_completion_event_id,
                        "failure_event_id": cancel_failure_event_id,
                        "operation_id": review_operation_id,
                        "plan_id": aggregate.current_plan_id,
                        "active_epoch": aggregate.active_epoch,
                        "activity_generation": activity_generation,
                        "input_watermark": cancel_input_watermark,
                        "input_ledger_sequence": cancel_input_ledger_sequence,
                        "ownership_generation": aggregate.ownership_generation,
                        "causation_id": event.event_id,
                        "expected_state": AgentSessionState.ACTIVE_REPLY.value,
                        "expected_current_plan_id": aggregate.current_plan_id,
                        "expected_active_epoch": aggregate.active_epoch,
                        "expected_activity_generation": activity_generation,
                        "cancelled_operation_fence": dict(review_fence),
                        "pending_active_reply": {
                            "operation_id": operation_id,
                            "effect_id": effect_id,
                            "effect_kind": (
                                AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW
                            ),
                            "idempotency_key": effect_id,
                            "source_event_id": (
                                cancel_completion_event_id or event.event_id
                            ),
                            "completion_event_id": completion_event_id,
                            "failure_event_id": failure_event_id,
                            "message_log_ids": [append.message_log_id],
                            "response_profile": append.response_profile,
                            "sender_id": append.sender_id,
                        },
                    },
                )
                effects.append(
                    _durable_effect(
                        effect_id=cancel_effect_id,
                        kind=AgentSessionEffectKind.CANCEL_REVIEW_WORKFLOW,
                        idempotency_key=cancel_effect_id,
                        operation_id=review_operation_id,
                        payload={
                            "operation_id": review_operation_id,
                            "plan_id": aggregate.current_plan_id,
                            "active_epoch": aggregate.active_epoch,
                            "activity_generation": activity_generation,
                            "input_watermark": cancel_input_watermark,
                            "input_ledger_sequence": cancel_input_ledger_sequence,
                            "expected_active_epoch": aggregate.active_epoch,
                            "expected_activity_generation": activity_generation,
                            "completion_event_id": cancel_completion_event_id,
                            "failure_event_id": cancel_failure_event_id,
                            "superseded_by_event_id": event.event_id,
                            "cancelled_operation_fence": dict(review_fence),
                        },
                    )
                )
                awaiting_review_cancellation = True
        target = aggregate.advance(
            state=AgentSessionState.ACTIVE_REPLY.value,
            activity_generation=activity_generation,
            review_operation_id="",
            active_reply_operation_id=operation_id,
            active_reply_resume=resume,
            data=next_data,
            updated_at=_event_time(aggregate, event),
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="active_reply",
            status=SessionOperationStatus.PENDING,
            launched_by_event_id=event.event_id,
            state_revision=target.state_revision,
            active_epoch=aggregate.active_epoch,
            activity_generation=activity_generation,
            input_watermark=input_watermark,
            started_at=event.occurred_at,
            metadata={
                "message_log_ids": [append.message_log_id],
                "priority": append.priority.to_record(),
                "resume": resume,
                "launch_status": (
                    "awaiting_review_cancellation"
                    if awaiting_review_cancellation
                    else "ready"
                ),
            },
        )
        effect = _durable_effect(
            effect_id=effect_id,
            kind=AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW,
            idempotency_key=effect_id,
            operation_id=operation_id,
            payload=operation_fence,
        )
        if not awaiting_review_cancellation:
            effects.append(effect)
        operations.append(operation)
        return SessionTransition(
            aggregate=target,
            disposition=(
                "review_interrupted_active_reply_waiting_cancellation"
                if aggregate.state == AgentSessionState.REVIEW
                else "active_reply_started"
            ),
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=tuple(effects),
            operations=tuple(operations),
            message_ledger_mutations=(append,),
            result={
                "message_log_id": append.message_log_id,
                "operation_id": operation_id,
                "resume": resume,
                "awaiting_review_cancellation": awaiting_review_cancellation,
            },
            reason="high_priority_message",
        )

    def _start_due_active_reply(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        pending_message_log_ids: tuple[int, ...],
    ) -> SessionTransition:
        """Run pending priority work before resuming the exact due review."""

        return self._start_queued_active_reply(
            aggregate,
            event,
            data=aggregate.data,
            pending_message_log_ids=pending_message_log_ids,
            resume={
                "kind": "resume_due_review",
                "plan_id": aggregate.current_plan_id,
                "plan_revision": aggregate.review_plan_revision,
                "review_due_event_id": event.event_id,
            },
            trigger="review_due_pending_high_priority",
            disposition="review_due_active_reply_started",
            reason="pending_high_priority_before_review",
        )

    def _start_queued_active_reply(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        data: Mapping[str, Any],
        pending_message_log_ids: tuple[int, ...],
        resume: Mapping[str, Any],
        trigger: str,
        disposition: str,
        reason: str,
    ) -> SessionTransition:
        """Start one actor-owned reply from durable queued priority input."""

        if not pending_message_log_ids:
            raise ValueError("queued active reply requires at least one message")
        if _has_unsettled_pending_outbound(data):
            raise ValueError("queued active reply cannot bypass pending outbound actions")
        operation_id = self._ids.create(
            key=event.key,
            seed=event.event_id,
            purpose="active-reply-operation",
        )
        effect_id = self._ids.create(
            key=event.key,
            seed=operation_id,
            purpose="run-active-reply-effect",
        )
        completion_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-reply-completion-event",
        )
        failure_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-reply-failure-event",
        )
        activity_generation = aggregate.activity_generation + 1
        input_watermark = _message_watermark(data)
        delivery_context = _mapping(
            data.get(_DELIVERY_CONTEXT_DATA_KEY)
        )
        operation_fence = {
            "operation_id": operation_id,
            "operation_kind": "active_reply",
            "source_event_id": event.event_id,
            "effect_id": effect_id,
            "effect_kind": AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW,
            **_effect_contract_snapshot(
                AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW
            ),
            "idempotency_key": effect_id,
            "completion_event_id": completion_event_id,
            "failure_event_id": failure_event_id,
            "ownership_generation": aggregate.ownership_generation,
            "plan_id": aggregate.current_plan_id,
            "active_epoch": aggregate.active_epoch,
            "activity_generation": activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": None,
            "instance_id": _text(delivery_context.get("instance_id")),
            "target_session_id": _text(
                delivery_context.get("target_session_id")
            ),
        }
        next_data = _with_operation_fence(
            data,
            operation_id,
            operation_fence,
        )
        normalized_resume = dict(resume)
        target = aggregate.advance(
            state=AgentSessionState.ACTIVE_REPLY.value,
            activity_generation=activity_generation,
            active_reply_operation_id=operation_id,
            active_reply_resume=normalized_resume,
            data=next_data,
            updated_at=_event_time(aggregate, event),
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="active_reply",
            status=SessionOperationStatus.PENDING,
            launched_by_event_id=event.event_id,
            state_revision=target.state_revision,
            active_epoch=aggregate.active_epoch,
            activity_generation=activity_generation,
            input_watermark=input_watermark,
            started_at=event.occurred_at,
            metadata={
                "message_log_ids": list(pending_message_log_ids),
                "resume": normalized_resume,
                "trigger": trigger,
            },
        )
        effect = _durable_effect(
            effect_id=effect_id,
            kind=AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW,
            idempotency_key=effect_id,
            operation_id=operation_id,
            payload={
                **operation_fence,
                "message_log_ids": list(pending_message_log_ids),
            },
        )
        return SessionTransition(
            aggregate=target,
            disposition=disposition,
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=(effect,),
            operations=(operation,),
            result={
                "message_log_ids": list(pending_message_log_ids),
                "operation_id": operation_id,
                "resume": normalized_resume,
            },
            reason=reason,
        )

    def _record_active_chat_message(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        append: AppendMessageLedgerEntry,
        data: Mapping[str, Any],
    ) -> SessionTransition:
        active_chat_state = dict(aggregate.active_chat_state)
        data = _supersede_active_chat_exit_request(
            data,
            event_id=event.event_id,
        )
        active_chat_state.pop("exit_blocker", None)
        active_chat_state["exit_requested"] = False
        pending_ids = list(
            _positive_int_tuple(
                active_chat_state.get("pending_message_log_ids"),
                field_name="active_chat_state.pending_message_log_ids",
                allow_empty=True,
            )
        )
        if append.message_log_id not in pending_ids:
            pending_ids.append(append.message_log_id)
        event_time = _event_time(aggregate, event)
        interest_value = _active_chat_interest_after_message(
            active_chat_state,
            append=append,
            now=event_time,
            config=self._config,
        )
        active_chat_state.update(
            {
                "pending_message_log_ids": pending_ids,
                "last_message_log_id": append.message_log_id,
                "last_message_at": event.occurred_at,
                "last_priority": append.priority.to_record(),
                "interest_value": interest_value,
                "updated_at": event_time,
            }
        )
        effects: tuple[SessionEffect, ...] = ()
        if (
            _text(active_chat_state.get("bootstrap_status")) == "completed"
            and not aggregate.active_chat_round_operation_id
            and not _has_unsettled_pending_outbound(data)
        ):
            active_chat_state, scheduled_effect, round_intent = (
                self._schedule_active_chat_round_due(
                aggregate,
                event,
                active_chat_state=active_chat_state,
                input_watermark=_message_watermark(data),
                delay_seconds=self._config.active_chat_semantic_wait_seconds,
                )
            )
            data = _with_control_intent(
                data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
                intent=round_intent,
            )
            effects = (scheduled_effect,)
        target = aggregate.advance(
            active_chat_state=active_chat_state,
            data=data,
            updated_at=event_time,
        )
        return SessionTransition(
            aggregate=target,
            disposition="active_chat_message_buffered",
            effects=effects,
            message_ledger_mutations=(append,),
            result={
                "message_log_id": append.message_log_id,
                "round_schedule_id": _text(
                    active_chat_state.get("round_schedule_id")
                ),
            },
            reason="message_received_in_active_chat",
        )

    def _schedule_active_chat_round_due(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        active_chat_state: Mapping[str, Any],
        input_watermark: int,
        delay_seconds: float,
        retry_cycle: int = 0,
    ) -> tuple[dict[str, Any], SessionEffect, dict[str, Any]]:
        """Debounce buffered active-chat input through a durable control effect."""

        revision = (
            _optional_nonnegative_int(
                active_chat_state.get("round_schedule_revision")
            )
            or 0
        ) + 1
        active_epoch = _optional_nonnegative_int(
            active_chat_state.get("active_epoch")
        )
        if active_epoch is None or active_epoch != aggregate.active_epoch:
            raise ValueError("active-chat schedule has an invalid active epoch")
        if retry_cycle < 0:
            raise ValueError("active-chat round retry cycle must not be negative")
        schedule_id = self._ids.create(
            key=event.key,
            seed=f"{active_epoch}:{revision}:{event.event_id}",
            purpose="active-chat-round-schedule",
        )
        effect_id = self._ids.create(
            key=event.key,
            seed=schedule_id,
            purpose="enqueue-active-chat-round-due-effect",
        )
        due_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-chat-round-due-event",
        )
        failure_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-chat-round-due-failure-event",
        )
        contract = builtin_effect_contract(
            AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE
        )
        pending_message_log_ids = list(
            _positive_int_tuple(
                active_chat_state.get("pending_message_log_ids"),
                field_name="active_chat_state.pending_message_log_ids",
                allow_empty=True,
            )
        )
        updated_state = dict(active_chat_state)
        updated_state.update(
            {
                "round_schedule_revision": revision,
                "round_schedule_id": schedule_id,
                "round_due_at": _event_time(aggregate, event) + delay_seconds,
                "round_schedule_effect_id": effect_id,
                "round_due_event_id": due_event_id,
                "round_schedule_failure_event_id": failure_event_id,
                "round_schedule_source_event_id": event.event_id,
                "round_schedule_input_watermark": input_watermark,
                "round_schedule_contract_version": contract.version,
                "round_schedule_contract_signature": contract.signature,
            }
        )
        updated_state.pop("round_schedule_blocker", None)
        effect = _durable_effect(
            effect_id=effect_id,
            kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
            idempotency_key=effect_id,
            operation_id="",
            available_after_seconds=delay_seconds,
            payload={
                "plan_id": aggregate.current_plan_id,
                "schedule_id": schedule_id,
                "schedule_revision": revision,
                "active_epoch": active_epoch,
                "activity_generation": aggregate.activity_generation,
                "input_watermark": input_watermark,
                "input_ledger_sequence": None,
                "completion_event_id": due_event_id,
                "due_event_id": due_event_id,
                "failure_event_id": failure_event_id,
            },
        )
        intent = {
            "desired_state": "round_due",
            "status": "requested",
            "effect_id": effect_id,
            "effect_kind": AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
            "idempotency_key": effect_id,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
            "completion_event_id": due_event_id,
            "failure_event_id": failure_event_id,
            "operation_id": "",
            "plan_id": aggregate.current_plan_id,
            "active_epoch": active_epoch,
            "activity_generation": aggregate.activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": None,
            "ownership_generation": aggregate.ownership_generation,
            "causation_id": event.event_id,
            "expected_state": AgentSessionState.ACTIVE_CHAT.value,
            "expected_current_plan_id": aggregate.current_plan_id,
            "expected_active_epoch": active_epoch,
            "expected_activity_generation": aggregate.activity_generation,
            "expected_message_watermark": input_watermark,
            "schedule_id": schedule_id,
            "schedule_revision": revision,
            "pending_message_log_ids": pending_message_log_ids,
            "retry_cycle": retry_cycle,
        }
        return updated_state, effect, intent

    def _active_chat_bootstrap_completed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        active_state = _mapping(aggregate.active_chat_state)
        operation_id = _text(active_state.get("bootstrap_operation_id"))
        mismatch = self._workflow_completion_mismatch(
            aggregate,
            event,
            operation_id=operation_id,
            expected_state=AgentSessionState.ACTIVE_CHAT,
            expected_operation_kind="active_chat_bootstrap",
            expected_effect_kind=AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=operation_id,
                disposition="active_chat_bootstrap_completion_stale",
                mismatch=mismatch,
            )
        fence = _operation_fence(aggregate.data, operation_id)
        failure_code = ""
        failure_message = ""
        try:
            result = _active_chat_bootstrap_workflow_result(event.payload)
        except (TypeError, ValueError) as exc:
            result = ActiveChatBootstrapCompletionResult(
                disposition=ActiveChatBootstrapDisposition.WATCH,
                reason="invalid_bootstrap_completion_fallback",
            )
            failure_code = "invalid_active_chat_bootstrap_completion"
            failure_message = str(exc)
        input_watermark, input_ledger_sequence = _captured_input_boundary(fence)
        status = (
            SessionOperationStatus.FAILED
            if failure_code
            else SessionOperationStatus.COMPLETED
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="active_chat_bootstrap",
            status=status,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            finished_at=event.occurred_at,
            failure_code=failure_code,
            failure_message=failure_message,
            metadata={
                "completion_event_id": event.event_id,
                "disposition": result.disposition.value,
                "reason": result.reason,
            },
        )
        data = _without_operation_fence(aggregate.data, operation_id)
        next_state = dict(active_state)
        next_state.update(
            {
                "bootstrap_status": "completed",
                "bootstrap_operation_id": "",
                "bootstrap_disposition": result.disposition.value,
                "bootstrap_reason": result.reason,
            }
        )
        effects: tuple[SessionEffect, ...] = ()
        if result.disposition is ActiveChatBootstrapDisposition.EXIT_SOON:
            exit_effect, exit_intent = self._enqueue_active_chat_exit_request(
                aggregate,
                event,
                trigger="active_chat_bootstrap_exit",
                input_watermark=_message_watermark(data),
            )
            data = _with_control_intent(
                data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
                intent=exit_intent,
            )
            effects = (exit_effect,)
            next_state["bootstrap_status"] = "exit_requested"
        elif (
            _positive_int_tuple(
                next_state.get("pending_message_log_ids"),
                field_name="active_chat_state.pending_message_log_ids",
                allow_empty=True,
            )
            and not aggregate.active_chat_round_operation_id
        ):
            next_state, scheduled_effect, round_intent = (
                self._schedule_active_chat_round_due(
                aggregate,
                event,
                active_chat_state=next_state,
                input_watermark=_message_watermark(data),
                delay_seconds=self._config.active_chat_semantic_wait_seconds,
                )
            )
            data = _with_control_intent(
                data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
                intent=round_intent,
            )
            effects = (scheduled_effect,)
        target = aggregate.advance(
            active_chat_state=next_state,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition=(
                "active_chat_bootstrap_completed"
                if not failure_code
                else "active_chat_bootstrap_completion_rejected"
            ),
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=effects,
            operations=(operation,),
            result={
                "disposition": result.disposition.value,
                "round_schedule_id": _text(next_state.get("round_schedule_id")),
            },
            reason=failure_code or result.reason,
        )

    def _enqueue_active_chat_exit_request(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        trigger: str,
        input_watermark: int,
        retry_cycle: int = 0,
    ) -> tuple[SessionEffect, dict[str, Any]]:
        """Create one durable, actor-owned request for the idle-exit path."""

        if retry_cycle < 0:
            raise ValueError("active-chat exit retry cycle must not be negative")

        effect_id = self._ids.create(
            key=event.key,
            seed=f"{event.event_id}:{trigger}:{input_watermark}:{retry_cycle}",
            purpose="enqueue-active-chat-exit-request-effect",
        )
        exit_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-chat-exit-request-event",
        )
        failure_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-chat-exit-request-failure-event",
        )
        contract = builtin_effect_contract(
            AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
        )
        effect = _durable_effect(
            effect_id=effect_id,
            kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
            idempotency_key=effect_id,
            operation_id="",
            payload={
                "plan_id": aggregate.current_plan_id,
                "active_epoch": aggregate.active_epoch,
                "activity_generation": aggregate.activity_generation,
                "input_watermark": input_watermark,
                "input_ledger_sequence": None,
                "exit_event_id": exit_event_id,
                "completion_event_id": exit_event_id,
                "failure_event_id": failure_event_id,
                "trigger": trigger,
                "expected_active_epoch": aggregate.active_epoch,
                "expected_message_watermark": input_watermark,
            },
        )
        intent = {
            "desired_state": "idle_review_planning",
            "status": "requested",
            "effect_id": effect_id,
            "effect_kind": AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
            "idempotency_key": effect_id,
            "contract_version": contract.version,
            "contract_signature": contract.signature,
            "completion_event_id": exit_event_id,
            "failure_event_id": failure_event_id,
            "operation_id": "",
            "plan_id": aggregate.current_plan_id,
            "active_epoch": aggregate.active_epoch,
            "activity_generation": aggregate.activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": None,
            "ownership_generation": aggregate.ownership_generation,
            "causation_id": event.event_id,
            "expected_state": AgentSessionState.ACTIVE_CHAT.value,
            "expected_current_plan_id": aggregate.current_plan_id,
            "expected_active_epoch": aggregate.active_epoch,
            "expected_activity_generation": aggregate.activity_generation,
            "expected_message_watermark": input_watermark,
            "trigger": trigger,
            "retry_cycle": retry_cycle,
        }
        return effect, intent

    def _active_chat_round_due(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        active_state = _mapping(aggregate.active_chat_state)
        intent = _control_intent(
            aggregate.data,
            AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
        )
        mismatch = (
            self._active_chat_round_due_control_mismatch(
                aggregate,
                event,
                active_state=active_state,
                intent=intent,
                expected_event_field="completion_event_id",
            )
            if intent
            else _active_chat_round_due_mismatch(aggregate, event, active_state)
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=aggregate.active_chat_round_operation_id,
                disposition="active_chat_round_due_stale",
                mismatch=mismatch,
            )
        pending_ids = _positive_int_tuple(
            active_state.get("pending_message_log_ids"),
            field_name="active_chat_state.pending_message_log_ids",
            allow_empty=True,
        )
        if not pending_ids:
            data = _with_completed_control_intent(
                aggregate.data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
                intent=intent,
                event=event,
            )
            target = aggregate.advance(
                active_chat_state={
                    **active_state,
                    "round_schedule_id": "",
                    "round_due_at": None,
                    "round_schedule_effect_id": "",
                    "round_due_event_id": "",
                    "round_schedule_failure_event_id": "",
                },
                data=data,
                updated_at=_event_time(aggregate, event),
            )
            return SessionTransition(
                aggregate=target,
                disposition="active_chat_round_due_empty",
                caused_plan_id=aggregate.current_plan_id,
                reason="no_buffered_active_chat_messages",
            )
        schedule_id = _text(active_state.get("round_schedule_id"))
        operation_id = self._ids.create(
            key=event.key,
            seed=schedule_id,
            purpose="active-chat-round-operation",
        )
        effect_id = self._ids.create(
            key=event.key,
            seed=operation_id,
            purpose="run-active-chat-round-effect",
        )
        completion_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-chat-round-completion-event",
        )
        failure_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-chat-round-failure-event",
        )
        input_watermark = _message_watermark(aggregate.data)
        delivery_context = _mapping(
            aggregate.data.get(_DELIVERY_CONTEXT_DATA_KEY)
        )
        operation_fence = {
            "operation_id": operation_id,
            "operation_kind": "active_chat_round",
            "source_event_id": event.event_id,
            "effect_id": effect_id,
            "effect_kind": AgentSessionEffectKind.RUN_ACTIVE_CHAT_ROUND,
            "idempotency_key": effect_id,
            "completion_event_id": completion_event_id,
            "failure_event_id": failure_event_id,
            "ownership_generation": aggregate.ownership_generation,
            "plan_id": aggregate.current_plan_id,
            "active_epoch": aggregate.active_epoch,
            "activity_generation": aggregate.activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": None,
            "instance_id": _text(delivery_context.get("instance_id")),
            "target_session_id": _text(
                delivery_context.get("target_session_id")
            ),
            "round_schedule_id": schedule_id,
            "message_log_ids": list(pending_ids),
        }
        round_contract = builtin_effect_contract(
            AgentSessionEffectKind.RUN_ACTIVE_CHAT_ROUND
        )
        operation_fence.update(
            {
                "contract_version": round_contract.version,
                "contract_signature": round_contract.signature,
            }
        )
        data = _with_completed_control_intent(
            aggregate.data,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
            intent=intent,
            event=event,
        )
        data = _with_operation_fence(
            data,
            operation_id,
            operation_fence,
        )
        next_state = {
            **active_state,
            "round_schedule_id": "",
            "round_due_at": None,
            "round_schedule_effect_id": "",
            "round_due_event_id": "",
            "round_schedule_failure_event_id": "",
            "round_schedule_contract_version": 0,
            "round_schedule_contract_signature": "",
            "round_operation_id": operation_id,
            "round_input_message_log_ids": list(pending_ids),
        }
        target = aggregate.advance(
            active_chat_round_operation_id=operation_id,
            active_chat_state=next_state,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="active_chat_round",
            status=SessionOperationStatus.PENDING,
            launched_by_event_id=event.event_id,
            state_revision=target.state_revision,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            started_at=event.occurred_at,
            metadata={
                "schedule_id": schedule_id,
                "message_log_ids": list(pending_ids),
            },
        )
        effect = _durable_effect(
            effect_id=effect_id,
            kind=AgentSessionEffectKind.RUN_ACTIVE_CHAT_ROUND,
            idempotency_key=effect_id,
            operation_id=operation_id,
            payload=operation_fence,
        )
        return SessionTransition(
            aggregate=target,
            disposition="active_chat_round_started",
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=(effect,),
            operations=(operation,),
            result={
                "message_log_ids": list(pending_ids),
                "round_schedule_id": schedule_id,
            },
            reason="active_chat_round_due",
        )

    def _active_chat_round_due_control_mismatch(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        active_state: Mapping[str, Any],
        intent: Mapping[str, Any],
        expected_event_field: str,
    ) -> tuple[str, ...]:
        """Validate a round timer against its durable control intent."""

        effect_kind = AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE
        mismatch = list(
            self._control_effect_event_mismatch(
                aggregate,
                event,
                intent=intent,
                effect_kind=effect_kind,
                expected_event_field=expected_event_field,
            )
        )
        if aggregate.state != AgentSessionState.ACTIVE_CHAT:
            mismatch.append("state_changed")
        if _text(active_state.get("bootstrap_status")) != "completed":
            mismatch.append("bootstrap_not_completed")
        if aggregate.active_chat_round_operation_id:
            mismatch.append("round_already_running")
        if _has_unsettled_pending_outbound(aggregate.data):
            mismatch.append("outbound_actions_pending")

        expected_text = {
            "round_schedule_id": _text(intent.get("schedule_id")),
            "round_schedule_effect_id": _text(intent.get("effect_id")),
            "round_due_event_id": _text(intent.get("completion_event_id")),
            "round_schedule_source_event_id": _text(intent.get("causation_id")),
            "round_schedule_contract_signature": _text(
                intent.get("contract_signature")
            ),
        }
        for field_name, expected in expected_text.items():
            if not expected or _text(active_state.get(field_name)) != expected:
                mismatch.append(f"{field_name}_changed")

        expected_revision = _optional_nonnegative_int(
            intent.get("schedule_revision")
        )
        if expected_revision is None or _optional_nonnegative_int(
            active_state.get("round_schedule_revision")
        ) != expected_revision:
            mismatch.append("round_schedule_revision_changed")
        if _optional_nonnegative_int(
            active_state.get("round_schedule_contract_version")
        ) != _optional_nonnegative_int(intent.get("contract_version")):
            mismatch.append("round_schedule_contract_version_changed")

        expected_watermark = _optional_nonnegative_int(
            intent.get("expected_message_watermark")
        )
        if (
            expected_watermark is None
            or _optional_nonnegative_int(
                active_state.get("round_schedule_input_watermark")
            )
            != expected_watermark
            or _message_watermark(aggregate.data) != expected_watermark
        ):
            mismatch.append("message_watermark_changed")
        if _text(event.payload.get("schedule_id")) != _text(
            intent.get("schedule_id")
        ):
            mismatch.append("schedule_id_changed")
        if _optional_nonnegative_int(
            event.payload.get("schedule_revision")
        ) != expected_revision:
            mismatch.append("schedule_revision_changed")

        try:
            pending_message_log_ids = _positive_int_tuple(
                intent.get("pending_message_log_ids"),
                field_name="round_due_intent.pending_message_log_ids",
                allow_empty=True,
            )
            active_pending_ids = _positive_int_tuple(
                active_state.get("pending_message_log_ids"),
                field_name="active_chat_state.pending_message_log_ids",
                allow_empty=True,
            )
        except (TypeError, ValueError):
            mismatch.append("pending_message_log_ids_invalid")
        else:
            if pending_message_log_ids != active_pending_ids:
                mismatch.append("pending_message_log_ids_changed")
        if expected_event_field == "failure_event_id":
            if not _text(event.payload.get("failure_code")):
                mismatch.append("failure_code_missing")
            if not isinstance(event.payload.get("failure_message"), str):
                mismatch.append("failure_message_invalid")
        return tuple(dict.fromkeys(mismatch))

    def _active_chat_round_completed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        operation_id = aggregate.active_chat_round_operation_id
        mismatch = self._workflow_completion_mismatch(
            aggregate,
            event,
            operation_id=operation_id,
            expected_state=AgentSessionState.ACTIVE_CHAT,
            expected_operation_kind="active_chat_round",
            expected_effect_kind=AgentSessionEffectKind.RUN_ACTIVE_CHAT_ROUND,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=operation_id,
                disposition="active_chat_round_completion_stale",
                mismatch=mismatch,
            )
        fence = _operation_fence(aggregate.data, operation_id)
        failure_code = ""
        failure_message = ""
        try:
            result = _active_chat_round_workflow_result(event.payload)
            input_watermark, _ = _captured_input_boundary(fence)
            _validate_consumed_ids(
                result.consumed_message_log_ids,
                input_watermark=input_watermark,
            )
            action_effects = self._materialize_completion_actions(
                aggregate,
                event,
                operation_id=operation_id,
                fence=fence,
                intents=result.external_action_intents,
            )
        except (TypeError, ValueError) as exc:
            result = ActiveChatRoundCompletionResult(
                outcome=ActiveChatRoundOutcome.RETRY,
                interest_delta=0.0,
                reason="invalid_active_chat_round_completion",
            )
            action_effects = ()
            failure_code = "invalid_active_chat_round_completion"
            failure_message = str(exc)
        input_watermark, input_ledger_sequence = _captured_input_boundary(fence)
        consumed_ids = result.consumed_message_log_ids
        consumptions: tuple[ConsumeMessageLedgerEntries, ...] = ()
        if consumed_ids:
            consumptions = (
                self._message_consumption(
                    aggregate,
                    event,
                    operation_id=operation_id,
                    input_watermark=input_watermark,
                    input_ledger_sequence=input_ledger_sequence,
                    message_log_ids=consumed_ids,
                    kind=MessageLedgerConsumptionKind.CHAT,
                ),
            )
        status = (
            SessionOperationStatus.FAILED
            if failure_code or result.outcome is ActiveChatRoundOutcome.RETRY
            else SessionOperationStatus.COMPLETED
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="active_chat_round",
            status=status,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            finished_at=event.occurred_at,
            failure_code=(
                failure_code
                or (
                    "active_chat_round_retry"
                    if result.outcome is ActiveChatRoundOutcome.RETRY
                    else ""
                )
            ),
            failure_message=failure_message,
            metadata={
                "completion_event_id": event.event_id,
                "outcome": result.outcome.value,
                "reason": result.reason,
                "consumed_message_log_ids": list(consumed_ids),
                "external_action_effect_ids": [
                    effect.effect_id for effect in action_effects
                ],
            },
        )
        data = _without_operation_fence(aggregate.data, operation_id)
        active_state = dict(aggregate.active_chat_state)
        pending_ids = list(
            _positive_int_tuple(
                active_state.get("pending_message_log_ids"),
                field_name="active_chat_state.pending_message_log_ids",
                allow_empty=True,
            )
        )
        consumed_set = set(consumed_ids)
        remaining_ids = [item for item in pending_ids if item not in consumed_set]
        current_interest = _finite_nonnegative_float(
            active_state.get("interest_value"),
            field_name="active_chat_state.interest_value",
            default=self._config.provisional_active_chat_interest,
        )
        next_interest = min(
            self._config.active_chat_max_interest,
            max(0.0, current_interest + result.interest_delta),
        )
        next_state: dict[str, Any] = {
            **active_state,
            "round_operation_id": "",
            "round_input_message_log_ids": [],
            "pending_message_log_ids": remaining_ids,
            "interest_value": next_interest,
            "updated_at": _event_time(aggregate, event),
        }
        effects = action_effects
        disposition = "active_chat_round_completed"
        if action_effects:
            data = _with_pending_outbound_actions(
                data,
                action_effects,
                source_event_id=event.event_id,
            )
            data = _with_outbound_continuation(
                data,
                kind="active_chat_round",
                source_operation_id=operation_id,
                outcome=result.outcome.value,
            )
            next_state["outbound_status"] = "waiting"
            disposition = "active_chat_round_waiting_outbound"
        elif result.outcome is ActiveChatRoundOutcome.EXIT and not remaining_ids:
            exit_effect, exit_intent = self._enqueue_active_chat_exit_request(
                aggregate,
                event,
                trigger="active_chat_round_exit",
                input_watermark=_message_watermark(data),
            )
            data = _with_control_intent(
                data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
                intent=exit_intent,
            )
            effects = (*effects, exit_effect)
            next_state["exit_requested"] = True
            disposition = "active_chat_round_exit_requested"
        elif remaining_ids:
            delay = (
                self._config.busy_review_retry_seconds
                if result.outcome is ActiveChatRoundOutcome.RETRY
                else self._config.active_chat_semantic_wait_seconds
            )
            next_state, scheduled_effect, round_intent = (
                self._schedule_active_chat_round_due(
                aggregate,
                event,
                active_chat_state=next_state,
                input_watermark=_message_watermark(data),
                delay_seconds=delay,
                )
            )
            data = _with_control_intent(
                data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
                intent=round_intent,
            )
            effects = (*effects, scheduled_effect)
            if result.outcome is ActiveChatRoundOutcome.RETRY:
                disposition = "active_chat_round_retry_scheduled"
        target = aggregate.advance(
            active_chat_round_operation_id="",
            active_chat_state=next_state,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition=disposition,
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=effects,
            operations=(operation,),
            message_ledger_mutations=consumptions,
            result={
                "outcome": result.outcome.value,
                "remaining_message_log_ids": remaining_ids,
                "round_schedule_id": _text(next_state.get("round_schedule_id")),
            },
            reason=failure_code or result.reason,
        )

    def _active_chat_tick(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        active_state = _mapping(aggregate.active_chat_state)
        mismatch = _active_chat_tick_mismatch(aggregate, event, active_state)
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=aggregate.active_chat_round_operation_id,
                disposition="active_chat_tick_stale",
                mismatch=mismatch,
            )
        event_time = _event_time(aggregate, event)
        current_interest = _finite_nonnegative_float(
            active_state.get("interest_value"),
            field_name="active_chat_state.interest_value",
            default=self._config.provisional_active_chat_interest,
        )
        half_life = _finite_positive_float(
            active_state.get("decay_half_life_seconds"),
            field_name="active_chat_state.decay_half_life_seconds",
            default=self._config.provisional_active_chat_half_life_seconds,
        )
        updated_at = _finite_nonnegative_float(
            active_state.get("updated_at"),
            field_name="active_chat_state.updated_at",
            default=aggregate.updated_at,
        )
        elapsed = max(0.0, event_time - updated_at)
        interest_value = current_interest * (0.5 ** (elapsed / half_life))
        pending_ids = _positive_int_tuple(
            active_state.get("pending_message_log_ids"),
            field_name="active_chat_state.pending_message_log_ids",
            allow_empty=True,
        )
        next_state = {
            **active_state,
            "interest_value": interest_value,
            "updated_at": event_time,
            "tick_count": (
                _optional_nonnegative_int(active_state.get("tick_count")) or 0
            )
            + 1,
        }
        data = dict(aggregate.data)
        effects: tuple[SessionEffect, ...] = ()
        disposition = "active_chat_tick_applied"
        if (
            interest_value <= self._config.active_chat_idle_interest_threshold
            and not pending_ids
            and not aggregate.active_chat_round_operation_id
            and not _has_unsettled_pending_outbound(data)
            and _text(active_state.get("bootstrap_status")) == "completed"
            and not _active_chat_exit_request_blocked(data)
            and not _active_chat_exit_request_pending(data)
        ):
            exit_effect, exit_intent = self._enqueue_active_chat_exit_request(
                aggregate,
                event,
                trigger="active_chat_decay_tick",
                input_watermark=_message_watermark(data),
            )
            data = _with_control_intent(
                data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
                intent=exit_intent,
            )
            effects = (exit_effect,)
            next_state["exit_requested"] = True
            disposition = "active_chat_tick_exit_requested"
        target = aggregate.advance(
            active_chat_state=next_state,
            data=data,
            updated_at=event_time,
        )
        return SessionTransition(
            aggregate=target,
            disposition=disposition,
            caused_plan_id=aggregate.current_plan_id,
            effects=effects,
            result={
                "interest_value": interest_value,
                "tick_count": next_state["tick_count"],
            },
            reason="active_chat_interest_decay",
        )

    def _external_action_completed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        pending = _pending_outbound_actions(aggregate.data)
        effect_id = _text(event.payload.get("effect_id"))
        expected = _mapping(pending.get(effect_id))
        mismatch = _external_action_completion_mismatch(
            aggregate,
            event,
            expected=expected,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=_text(event.payload.get("operation_id")),
                disposition="external_action_completion_stale",
                mismatch=mismatch,
            )
        try:
            receipt_status = ExternalActionReceiptStatus(
                _text(event.payload.get("receipt_status"))
            )
        except ValueError:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=_text(expected.get("operation_id")),
                disposition="external_action_completion_stale",
                mismatch=("receipt_status_invalid",),
            )
        updated_pending = dict(pending)
        updated = dict(expected)
        updated["status"] = receipt_status.value
        updated["completion_event_id"] = event.event_id
        updated["completed_at"] = event.occurred_at
        updated_pending[effect_id] = updated
        data = dict(aggregate.data)
        data[_PENDING_OUTBOUND_DATA_KEY] = updated_pending
        if receipt_status is not ExternalActionReceiptStatus.SUCCEEDED:
            data["outbound_blocked_reason"] = receipt_status.value
            data[_OUTBOUND_BLOCKED_DATA_KEY] = {
                "completion_event_id": event.event_id,
                "effect_id": effect_id,
                "kind": "receipt_terminal",
                "operation_id": _text(expected.get("operation_id")),
                "receipt_status": receipt_status.value,
            }
            target = aggregate.advance(
                data=data,
                updated_at=_event_time(aggregate, event),
            )
            return SessionTransition(
                aggregate=target,
                disposition="external_action_terminal_blocked",
                caused_operation_id=_text(expected.get("operation_id")),
                caused_plan_id=aggregate.current_plan_id,
                result={
                    "effect_id": effect_id,
                    "receipt_status": receipt_status.value,
                },
                reason=f"external_action_{receipt_status.value}",
            )
        if not _all_pending_outbound_succeeded(updated_pending):
            target = aggregate.advance(
                data=data,
                updated_at=_event_time(aggregate, event),
            )
            return SessionTransition(
                aggregate=target,
                disposition="external_action_completed_waiting_for_predecessors",
                caused_operation_id=_text(expected.get("operation_id")),
                caused_plan_id=aggregate.current_plan_id,
                result={"effect_id": effect_id, "receipt_status": receipt_status.value},
                reason="pending_external_actions_remain",
            )
        data.pop(_PENDING_OUTBOUND_DATA_KEY, None)
        data.pop("outbound_blocked_reason", None)
        data.pop(_OUTBOUND_BLOCKED_DATA_KEY, None)
        if (
            aggregate.state == AgentSessionState.ACTIVE_CHAT
            and _text(aggregate.active_chat_state.get("bootstrap_status"))
            == "waiting_outbound"
        ):
            return self._start_active_chat_bootstrap_after_outbound(
                aggregate,
                event,
                data=data,
                completed_effect_id=effect_id,
            )
        continuation = _outbound_continuation(data)
        if (
            aggregate.state == AgentSessionState.ACTIVE_CHAT
            and _text(continuation.get("kind")) == "active_chat_round"
        ):
            return self._resume_active_chat_round_after_outbound(
                aggregate,
                event,
                data=data,
                completed_effect_id=effect_id,
                continuation=continuation,
            )
        if aggregate.state == AgentSessionState.IDLE:
            return self._resume_idle_after_outbound(
                aggregate,
                event,
                data=data,
                completed_effect_id=effect_id,
                completed_operation_id=_text(expected.get("operation_id")),
            )
        target = aggregate.advance(
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="external_action_completed",
            caused_operation_id=_text(expected.get("operation_id")),
            caused_plan_id=aggregate.current_plan_id,
            result={"effect_id": effect_id, "receipt_status": receipt_status.value},
            reason="all_external_actions_succeeded",
        )

    def _resume_idle_after_outbound(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        data: Mapping[str, Any],
        completed_effect_id: str,
        completed_operation_id: str,
    ) -> SessionTransition:
        """Release queued priority work only after all visible actions settle."""

        pending_priority_ids = _positive_int_tuple(
            data.get(_PENDING_HIGH_PRIORITY_DATA_KEY),
            field_name=_PENDING_HIGH_PRIORITY_DATA_KEY,
            allow_empty=True,
        )
        resume = _mapping(aggregate.active_reply_resume)
        if pending_priority_ids and not _review_cancellation_blocks_active_reply(data):
            return self._start_queued_active_reply(
                aggregate,
                event,
                data=data,
                pending_message_log_ids=pending_priority_ids,
                resume=resume,
                trigger="external_actions_completed_pending_high_priority",
                disposition="external_actions_completed_active_reply_started",
                reason="external_actions_completed_pending_high_priority",
            )
        if _text(resume.get("kind")) in {
            "resume_due_review",
            "resume_interrupted_review",
        }:
            next_data, operation, effect = self._build_review_work(
                aggregate,
                event,
                data=data,
            )
            target = aggregate.advance(
                state=AgentSessionState.REVIEW.value,
                review_operation_id=operation.operation_id,
                active_reply_resume={},
                data=next_data,
                updated_at=_event_time(aggregate, event),
            )
            return SessionTransition(
                aggregate=target,
                disposition="external_actions_completed_review_resumed",
                caused_operation_id=operation.operation_id,
                caused_plan_id=aggregate.current_plan_id,
                effects=(effect,),
                operations=(operation,),
                result={
                    "completed_effect_id": completed_effect_id,
                    "completed_operation_id": completed_operation_id,
                    "review_operation_id": operation.operation_id,
                },
                reason="all_external_actions_succeeded",
            )
        target = aggregate.advance(
            data=dict(data),
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="external_actions_completed_idle",
            caused_operation_id=completed_operation_id,
            caused_plan_id=aggregate.current_plan_id,
            result={
                "completed_effect_id": completed_effect_id,
                "completed_operation_id": completed_operation_id,
            },
            reason="all_external_actions_succeeded",
        )

    def _resume_active_chat_round_after_outbound(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        data: Mapping[str, Any],
        completed_effect_id: str,
        continuation: Mapping[str, Any],
    ) -> SessionTransition:
        """Resume a completed chat round only after its replies are visible."""

        outcome = ActiveChatRoundOutcome(_text(continuation.get("outcome")))
        next_data = _without_outbound_continuation(data)
        active_state = dict(aggregate.active_chat_state)
        pending_ids = _positive_int_tuple(
            active_state.get("pending_message_log_ids"),
            field_name="active_chat_state.pending_message_log_ids",
            allow_empty=True,
        )
        active_state.update(
            {
                "outbound_gate_completed_effect_id": completed_effect_id,
                "outbound_status": "completed",
                "updated_at": _event_time(aggregate, event),
            }
        )
        effects: tuple[SessionEffect, ...] = ()
        disposition = "external_actions_completed_active_chat_round"
        if outcome is ActiveChatRoundOutcome.EXIT and not pending_ids:
            exit_effect, exit_intent = self._enqueue_active_chat_exit_request(
                aggregate,
                event,
                trigger="active_chat_round_exit_after_outbound",
                input_watermark=_message_watermark(next_data),
            )
            next_data = _with_control_intent(
                next_data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
                intent=exit_intent,
            )
            effects = (exit_effect,)
            active_state["exit_requested"] = True
            disposition = "external_actions_completed_active_chat_exit_requested"
        elif pending_ids:
            delay = (
                self._config.busy_review_retry_seconds
                if outcome is ActiveChatRoundOutcome.RETRY
                else self._config.active_chat_semantic_wait_seconds
            )
            active_state, scheduled_effect, round_intent = (
                self._schedule_active_chat_round_due(
                aggregate,
                event,
                active_chat_state=active_state,
                input_watermark=_message_watermark(next_data),
                delay_seconds=delay,
                )
            )
            next_data = _with_control_intent(
                next_data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
                intent=round_intent,
            )
            effects = (scheduled_effect,)
            disposition = (
                "external_actions_completed_active_chat_retry_scheduled"
                if outcome is ActiveChatRoundOutcome.RETRY
                else "external_actions_completed_active_chat_round_scheduled"
            )
        target = aggregate.advance(
            active_chat_state=active_state,
            data=next_data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition=disposition,
            caused_operation_id=_text(continuation.get("source_operation_id")),
            caused_plan_id=aggregate.current_plan_id,
            effects=effects,
            result={
                "completed_effect_id": completed_effect_id,
                "outcome": outcome.value,
                "round_schedule_id": _text(active_state.get("round_schedule_id")),
            },
            reason="all_external_actions_succeeded",
        )

    def _start_active_chat_bootstrap_after_outbound(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        data: Mapping[str, Any],
        completed_effect_id: str,
    ) -> SessionTransition:
        """Start the first post-review model turn only after replies are visible."""

        active_state = dict(aggregate.active_chat_state)
        active_epoch = aggregate.active_epoch
        operation_id = self._ids.create(
            key=event.key,
            seed=f"{active_epoch}:{completed_effect_id}",
            purpose="active-chat-bootstrap-operation",
        )
        effect_id = self._ids.create(
            key=event.key,
            seed=operation_id,
            purpose="run-active-chat-bootstrap-effect",
        )
        completion_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-chat-bootstrap-completion-event",
        )
        failure_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="active-chat-bootstrap-failure-event",
        )
        input_watermark = _message_watermark(data)
        delivery_context = _mapping(data.get(_DELIVERY_CONTEXT_DATA_KEY))
        fence = {
            "operation_id": operation_id,
            "operation_kind": "active_chat_bootstrap",
            "source_event_id": event.event_id,
            "effect_id": effect_id,
            "effect_kind": AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP,
            **_effect_contract_snapshot(
                AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP
            ),
            "idempotency_key": effect_id,
            "completion_event_id": completion_event_id,
            "failure_event_id": failure_event_id,
            "ownership_generation": aggregate.ownership_generation,
            "plan_id": aggregate.current_plan_id,
            "active_epoch": active_epoch,
            "activity_generation": aggregate.activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": None,
            "instance_id": _text(delivery_context.get("instance_id")),
            "target_session_id": _text(
                delivery_context.get("target_session_id")
            ),
        }
        next_data = _with_operation_fence(data, operation_id, fence)
        active_state.update(
            {
                "bootstrap_status": "pending",
                "bootstrap_operation_id": operation_id,
                "outbound_gate_completed_effect_id": completed_effect_id,
            }
        )
        target = aggregate.advance(
            active_chat_state=active_state,
            data=next_data,
            updated_at=_event_time(aggregate, event),
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="active_chat_bootstrap",
            status=SessionOperationStatus.PENDING,
            launched_by_event_id=event.event_id,
            state_revision=target.state_revision,
            active_epoch=active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            started_at=event.occurred_at,
            metadata={"outbound_gate_completed_effect_id": completed_effect_id},
        )
        effect = _durable_effect(
            effect_id=effect_id,
            kind=AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP,
            idempotency_key=effect_id,
            operation_id=operation_id,
            payload=fence,
        )
        return SessionTransition(
            aggregate=target,
            disposition="external_actions_completed_bootstrap_started",
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=(effect,),
            operations=(operation,),
            result={"bootstrap_operation_id": operation_id},
            reason="all_external_actions_succeeded",
        )

    def _review_due(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        mismatch = _review_due_mismatch(aggregate, event)
        if mismatch:
            target = aggregate.advance(
                state_changed=False,
                updated_at=_event_time(aggregate, event),
            )
            plan_id = _text(event.payload.get("plan_id"))
            return SessionTransition(
                aggregate=target,
                disposition="review_due_superseded",
                caused_plan_id=plan_id,
                review_schedule_events=(
                    SessionReviewScheduleEvent(
                        schedule_event_id=self._ids.create(
                            key=event.key,
                            seed=event.event_id,
                            purpose="review-due-superseded-event",
                        ),
                        event_type="due_superseded",
                        plan_id=plan_id,
                        previous_plan_id=aggregate.current_plan_id,
                        trigger="review_due",
                        outcome="superseded",
                        source=event.source,
                        reason=",".join(mismatch),
                        committed_state_revision=aggregate.state_revision,
                        trace_id=event.trace_id,
                    ),
                ),
                result={"mismatch": list(mismatch)},
                reason=",".join(mismatch),
            )
        pending_priority_ids = _positive_int_tuple(
            aggregate.data.get(_PENDING_HIGH_PRIORITY_DATA_KEY),
            field_name=_PENDING_HIGH_PRIORITY_DATA_KEY,
            allow_empty=True,
        )
        if _has_unsettled_pending_outbound(aggregate.data):
            return self._defer_review_due(
                aggregate,
                event,
                blocking_reason="outbound_actions_pending",
                blocking_operation_id=_pending_outbound_operation_id(
                    aggregate.data
                ),
            )
        if aggregate.state == AgentSessionState.IDLE and pending_priority_ids:
            if _review_cancellation_blocks_active_reply(aggregate.data):
                return self._start_review(aggregate, event)
            return self._start_due_active_reply(
                aggregate,
                event,
                pending_message_log_ids=pending_priority_ids,
            )
        if aggregate.state == AgentSessionState.IDLE:
            return self._start_review(aggregate, event)
        if (
            aggregate.state == AgentSessionState.REVIEW
            and aggregate.review_operation_id
        ):
            return self._ignored(
                aggregate,
                event,
                disposition="review_already_running",
                reason="review_due_while_review_running",
            )
        return self._defer_review_due(aggregate, event)

    def _start_review(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        data, operation, effect = self._build_review_work(
            aggregate,
            event,
            data=aggregate.data,
        )
        target = aggregate.advance(
            state=AgentSessionState.REVIEW.value,
            review_operation_id=operation.operation_id,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="review_started",
            caused_operation_id=operation.operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=(effect,),
            operations=(operation,),
            result={
                "operation_id": operation.operation_id,
                "plan_id": aggregate.current_plan_id,
                "input_watermark": operation.input_watermark,
            },
            reason="review_schedule_due",
        )

    def _build_review_work(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        data: Mapping[str, Any],
    ) -> tuple[dict[str, Any], SessionOperation, SessionEffect]:
        """Create one fenced review operation without advancing the aggregate."""

        operation_id = self._ids.create(
            key=event.key,
            seed=event.event_id,
            purpose="review-operation",
        )
        effect_id = self._ids.create(
            key=event.key,
            seed=operation_id,
            purpose="run-review-effect",
        )
        completion_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="review-completion-event",
        )
        failure_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose="review-failure-event",
        )
        input_watermark = _message_watermark(data)
        delivery_context = _mapping(
            data.get(_DELIVERY_CONTEXT_DATA_KEY)
        )
        operation_fence = {
            "operation_id": operation_id,
            "operation_kind": "review",
            "source_event_id": event.event_id,
            "effect_id": effect_id,
            "effect_kind": AgentSessionEffectKind.RUN_REVIEW_WORKFLOW,
            **_effect_contract_snapshot(
                AgentSessionEffectKind.RUN_REVIEW_WORKFLOW
            ),
            "idempotency_key": effect_id,
            "completion_event_id": completion_event_id,
            "failure_event_id": failure_event_id,
            "ownership_generation": aggregate.ownership_generation,
            "plan_id": aggregate.current_plan_id,
            "plan_revision": aggregate.review_plan_revision,
            "active_epoch": aggregate.active_epoch,
            "activity_generation": aggregate.activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": None,
            "instance_id": _text(delivery_context.get("instance_id")),
            "target_session_id": _text(
                delivery_context.get("target_session_id")
            ),
        }
        next_data = _with_operation_fence(
            data,
            operation_id,
            operation_fence,
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="review",
            status=SessionOperationStatus.PENDING,
            launched_by_event_id=event.event_id,
            state_revision=aggregate.state_revision + 1,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            started_at=event.occurred_at,
            metadata={
                "plan_id": aggregate.current_plan_id,
                "plan_revision": aggregate.review_plan_revision,
            },
        )
        effect = _durable_effect(
            effect_id=effect_id,
            kind=AgentSessionEffectKind.RUN_REVIEW_WORKFLOW,
            idempotency_key=effect_id,
            operation_id=operation_id,
            payload={
                **operation_fence,
                "review_plan": dict(aggregate.review_plan),
            },
        )
        return next_data, operation, effect

    def _defer_review_due(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        blocking_reason: str = "",
        blocking_operation_id: str = "",
    ) -> SessionTransition:
        """Reschedule a due review without treating it as consumed work."""

        normalized_reason = _text(blocking_reason) or (
            f"session_busy:{aggregate.state}"
        )
        normalized_operation_id = _text(blocking_operation_id)
        plan = dict(aggregate.review_plan)
        retry_at = _event_time(aggregate, event) + self._config.busy_review_retry_seconds
        attempt_count = (_optional_nonnegative_int(event.payload.get("attempt_count")) or 0) + 1
        applied_delay = _optional_delay(plan.get("applied_delay_seconds"))
        if applied_delay is None:
            applied_delay = self._config.default_review_delay_seconds
        schedule = SessionReviewSchedule(
            plan_id=aggregate.current_plan_id,
            plan_revision=aggregate.review_plan_revision,
            applied_delay_seconds=applied_delay,
            status=ReviewScheduleStatus.SCHEDULED,
            trigger=_text(plan.get("trigger")) or "review_due",
            outcome=_text(plan.get("kind") or plan.get("outcome")) or "planned",
            source=_text(plan.get("source")) or event.source,
            requested_delay_seconds=_optional_delay(
                plan.get("requested_delay_seconds")
            ),
            reason=_text(plan.get("reason")),
            fallback_reason=_text(plan.get("fallback_reason")),
            mention_sensitivity=_text(plan.get("mention_sensitivity")) or "normal",
            active_reply_threshold=_mapping(plan.get("active_reply_threshold")),
            model_execution_id=_text(plan.get("model_execution_id")),
            prompt_signature=_text(plan.get("prompt_signature")),
            expected_active_epoch=_optional_nonnegative_int(
                plan.get("expected_active_epoch")
            ),
            expected_activity_generation=_optional_nonnegative_int(
                plan.get("expected_activity_generation")
            ),
            committed_state_revision=_optional_nonnegative_int(
                plan.get("committed_state_revision")
            ),
            available_at=retry_at,
            attempt_count=attempt_count,
            last_error=f"review_due_deferred:{normalized_reason}",
        )
        target = aggregate.advance(
            state_changed=False,
            updated_at=_event_time(aggregate, event),
        )
        schedule_event = SessionReviewScheduleEvent(
            schedule_event_id=self._ids.create(
                key=event.key,
                seed=event.event_id,
                purpose="review-due-deferred-event",
            ),
            event_type="deferred",
            plan_id=aggregate.current_plan_id,
            previous_plan_id=aggregate.current_plan_id,
            trigger="review_due",
            outcome="deferred",
            source=event.source,
            reason=normalized_reason,
            expected_active_epoch=aggregate.active_epoch,
            expected_activity_generation=aggregate.activity_generation,
            committed_state_revision=aggregate.state_revision,
            operation_id=(
                normalized_operation_id
                or aggregate.active_reply_operation_id
                or aggregate.active_chat_round_operation_id
                or aggregate.idle_planning_operation_id
                or aggregate.review_operation_id
            ),
            trace_id=event.trace_id,
            metadata={"retry_at": retry_at, "attempt_count": attempt_count},
        )
        return SessionTransition(
            aggregate=target,
            disposition="review_due_deferred",
            caused_operation_id=schedule_event.operation_id,
            caused_plan_id=aggregate.current_plan_id,
            review_schedules=(schedule,),
            review_schedule_events=(schedule_event,),
            result={"retry_at": retry_at, "attempt_count": attempt_count},
            reason=normalized_reason,
        )

    def _review_cancellation_completed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Release an interrupted active reply only after review cancellation.

        The control effect intentionally owns the former review operation while
        the active-reply operation remains pending in the aggregate.  A trusted
        completion is the sole point at which its model effect may enter the
        outbox, so an in-flight review can never overlap the reply workflow.
        """

        effect_kind = AgentSessionEffectKind.CANCEL_REVIEW_WORKFLOW
        intent = _control_intent(aggregate.data, effect_kind)
        mismatch = self._control_effect_event_mismatch(
            aggregate,
            event,
            intent=intent,
            effect_kind=effect_kind,
            expected_event_field="completion_event_id",
        )
        if mismatch:
            return self._ignored(
                aggregate,
                event,
                disposition="review_cancellation_completion_stale",
                reason=",".join(mismatch),
            )

        effect, pending_mismatch = self._pending_active_reply_effect(
            aggregate,
            intent=intent,
        )
        if pending_mismatch:
            completion = _effect_completion_record(event)
            failure = {
                **completion,
                "failure_code": "review_cancellation_release_rejected",
                "failure_message": ",".join(pending_mismatch),
            }
            return self._block_review_cancellation(
                aggregate,
                event,
                intent=intent,
                failure=failure,
                disposition="review_cancellation_completed_active_reply_blocked",
            )

        assert effect is not None
        completion = _effect_completion_record(event)
        operation = SessionOperation(
            operation_id=effect.operation_id,
            kind="active_reply",
            status=SessionOperationStatus.PENDING,
            metadata={
                "launch_status": "released_after_review_cancellation",
                "review_cancellation_completion_event_id": event.event_id,
            },
        )
        target = aggregate.advance(
            data=_with_control_intent(
                aggregate.data,
                effect_kind=effect_kind,
                intent={
                    **intent,
                    "status": "completed",
                    "completion": completion,
                    "pending_active_reply": {
                        **_mapping(intent.get("pending_active_reply")),
                        "status": "released",
                    },
                },
            ),
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="review_cancellation_completed_active_reply_released",
            caused_operation_id=effect.operation_id,
            caused_plan_id=_text(intent.get("plan_id")),
            effects=(effect,),
            operations=(operation,),
            result={
                "completion": completion,
                "active_reply_effect_id": effect.effect_id,
                "active_reply_operation_id": effect.operation_id,
            },
            reason="review_cancellation_completed",
        )

    def _pending_active_reply_effect(
        self,
        aggregate: AgentSessionAggregate,
        *,
        intent: Mapping[str, Any],
    ) -> tuple[SessionEffect | None, tuple[str, ...]]:
        """Rebuild one saved active-reply effect from its stamped operation fence."""

        pending = _mapping(intent.get("pending_active_reply"))
        operation_id = _text(pending.get("operation_id"))
        mismatch: list[str] = []
        if not operation_id:
            mismatch.append("pending_active_reply_operation_id_missing")
        elif aggregate.active_reply_operation_id != operation_id:
            mismatch.append("pending_active_reply_operation_changed")
        if aggregate.state != AgentSessionState.ACTIVE_REPLY:
            mismatch.append("pending_active_reply_state_changed")

        fence = _mapping(
            _mapping(aggregate.data.get("operation_fences")).get(operation_id)
        )
        if not fence:
            mismatch.append("pending_active_reply_fence_missing")

        expected_text = {
            "operation_id": operation_id,
            "operation_kind": "active_reply",
            "effect_id": _text(pending.get("effect_id")),
            "effect_kind": AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW,
            "idempotency_key": _text(pending.get("idempotency_key")),
            "source_event_id": _text(pending.get("source_event_id")),
            "completion_event_id": _text(pending.get("completion_event_id")),
            "failure_event_id": _text(pending.get("failure_event_id")),
        }
        for field_name, expected in expected_text.items():
            if not expected:
                mismatch.append(f"pending_active_reply_{field_name}_missing")
            elif _text(fence.get(field_name)) != expected:
                mismatch.append(f"pending_active_reply_{field_name}_changed")

        expected_ints = {
            "ownership_generation": aggregate.ownership_generation,
            "active_epoch": aggregate.active_epoch,
            "activity_generation": aggregate.activity_generation,
        }
        for field_name, expected in expected_ints.items():
            if _optional_nonnegative_int(fence.get(field_name)) != expected:
                mismatch.append(f"pending_active_reply_{field_name}_changed")
        if _text(fence.get("plan_id")) != aggregate.current_plan_id:
            mismatch.append("pending_active_reply_plan_id_changed")

        try:
            input_watermark, input_ledger_sequence = _captured_input_boundary(fence)
        except ValueError:
            input_watermark = 0
            input_ledger_sequence = 0
            mismatch.append("pending_active_reply_input_boundary_uncaptured")

        try:
            message_log_ids = _positive_int_tuple(
                pending.get("message_log_ids"),
                field_name="pending_active_reply.message_log_ids",
                allow_empty=False,
            )
        except (TypeError, ValueError):
            message_log_ids = ()
            mismatch.append("pending_active_reply_message_log_ids_invalid")
        try:
            fenced_message_log_ids = _positive_int_tuple(
                fence.get("message_log_ids"),
                field_name="operation_fence.message_log_ids",
                allow_empty=False,
            )
        except (TypeError, ValueError):
            fenced_message_log_ids = ()
            mismatch.append("pending_active_reply_fenced_message_log_ids_invalid")
        if message_log_ids and message_log_ids != fenced_message_log_ids:
            mismatch.append("pending_active_reply_message_log_ids_changed")
        if any(message_log_id > input_watermark for message_log_id in message_log_ids):
            mismatch.append("pending_active_reply_message_log_ids_outside_watermark")

        for field_name in ("response_profile", "sender_id"):
            pending_value = pending.get(field_name)
            if not isinstance(pending_value, str):
                mismatch.append(f"pending_active_reply_{field_name}_invalid")
            elif fence.get(field_name) != pending_value:
                mismatch.append(f"pending_active_reply_{field_name}_changed")
        instance_id = _text(fence.get("instance_id"))
        target_session_id = _text(fence.get("target_session_id"))
        if not instance_id:
            mismatch.append("pending_active_reply_instance_id_missing")
        if not target_session_id:
            mismatch.append("pending_active_reply_target_session_id_missing")
        elif instance_id and not target_session_id.startswith(f"{instance_id}:"):
            mismatch.append("pending_active_reply_target_session_id_changed")

        if mismatch:
            return None, tuple(dict.fromkeys(mismatch))
        effect = _durable_effect(
            effect_id=_text(pending.get("effect_id")),
            kind=AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW,
            idempotency_key=_text(pending.get("idempotency_key")),
            operation_id=operation_id,
            payload={
                **fence,
                "input_watermark": input_watermark,
                "input_ledger_sequence": input_ledger_sequence,
                "message_log_ids": list(message_log_ids),
                "response_profile": pending["response_profile"],
                "sender_id": pending["sender_id"],
            },
        )
        return effect, ()

    def _block_review_cancellation(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        intent: Mapping[str, Any],
        failure: Mapping[str, Any],
        disposition: str,
    ) -> SessionTransition:
        """Terminalize the held reply and retain unread input for a later review."""

        pending = _mapping(intent.get("pending_active_reply"))
        active_reply_operation_id = (
            aggregate.active_reply_operation_id
            or _text(pending.get("operation_id"))
        )
        active_reply_fence = _mapping(
            _mapping(aggregate.data.get("operation_fences")).get(
                active_reply_operation_id
            )
        )
        failure_code = _text(failure.get("failure_code")) or (
            "review_cancellation_effect_failed"
        )
        failure_message = failure.get("failure_message")
        if not isinstance(failure_message, str):
            failure_message = ""
        failure_event_id = _text(failure.get("event_id")) or event.event_id
        blocker = {
            "effect_id": _text(intent.get("effect_id")),
            "failure_code": failure_code,
            "failure_event_id": failure_event_id,
            "kind": "effect_failed",
            "operation_id": active_reply_operation_id,
        }
        updated_intent = {
            **intent,
            "status": "failed",
            "last_failure": dict(failure),
            "pending_active_reply": {
                **pending,
                "status": "blocked",
            },
            "blocker": blocker,
        }
        data = _without_operation_fence(aggregate.data, active_reply_operation_id)
        data = _with_control_intent(
            data,
            effect_kind=AgentSessionEffectKind.CANCEL_REVIEW_WORKFLOW,
            intent=updated_intent,
        )
        data[_REVIEW_CANCELLATION_BLOCKER_DATA_KEY] = {
            **blocker,
            "review_operation_id": _text(intent.get("operation_id")),
        }
        data["outbound_blocked_reason"] = "review_cancellation_effect_failed"
        data[_OUTBOUND_BLOCKED_DATA_KEY] = blocker
        deferred_resume = self._deferred_active_reply_resume(
            aggregate.active_reply_resume,
            event=event,
            failure=failure,
        )
        target = aggregate.advance(
            state=AgentSessionState.IDLE.value,
            review_operation_id="",
            active_reply_operation_id="",
            active_reply_resume=deferred_resume,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        operations: tuple[SessionOperation, ...] = ()
        if active_reply_operation_id:
            operations = (
                SessionOperation(
                    operation_id=active_reply_operation_id,
                    kind="active_reply",
                    status=SessionOperationStatus.FAILED,
                    active_epoch=_optional_nonnegative_int(
                        active_reply_fence.get("active_epoch")
                    ),
                    activity_generation=_optional_nonnegative_int(
                        active_reply_fence.get("activity_generation")
                    ),
                    input_watermark=_optional_nonnegative_int(
                        active_reply_fence.get("input_watermark")
                    ),
                    input_ledger_sequence=_optional_nonnegative_int(
                        active_reply_fence.get("input_ledger_sequence")
                    ),
                    finished_at=event.occurred_at,
                    failure_code=failure_code,
                    failure_message=failure_message,
                    metadata={
                        "launch_status": "blocked_by_review_cancellation_failure",
                        "review_cancellation_effect_id": _text(
                            intent.get("effect_id")
                        ),
                        "review_cancellation_failure": dict(failure),
                        "resume": dict(aggregate.active_reply_resume),
                    },
                ),
            )
        review_schedules, review_schedule_events = self._defer_active_reply_resume(
            aggregate,
            event,
            target=target,
            operation_id=active_reply_operation_id,
            resume=deferred_resume,
            failure_code=failure_code,
        )
        return SessionTransition(
            aggregate=target,
            disposition=disposition,
            caused_operation_id=active_reply_operation_id,
            caused_plan_id=_text(intent.get("plan_id")),
            operations=operations,
            review_schedules=review_schedules,
            review_schedule_events=review_schedule_events,
            result={
                "failure": dict(failure),
                "active_reply_operation_id": active_reply_operation_id,
                "blocker": blocker,
                "review_retry_scheduled": bool(review_schedules),
            },
            reason=failure_code,
        )

    def _active_reply_completed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        operation_id = aggregate.active_reply_operation_id
        mismatch = self._workflow_completion_mismatch(
            aggregate,
            event,
            operation_id=operation_id,
            expected_state=AgentSessionState.ACTIVE_REPLY,
            expected_operation_kind="active_reply",
            expected_effect_kind=AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=operation_id,
                disposition="active_reply_completion_stale",
                mismatch=mismatch,
            )

        fence = _operation_fence(aggregate.data, operation_id)
        try:
            result = _active_reply_workflow_result(event.payload)
            consumed_ids = result.consumed_message_log_ids
            input_watermark, _ = _captured_input_boundary(fence)
            _validate_consumed_ids(
                consumed_ids,
                input_watermark=input_watermark,
            )
            action_effects = self._materialize_completion_actions(
                aggregate,
                event,
                operation_id=operation_id,
                fence=fence,
                intents=result.external_action_intents,
            )
        except (TypeError, ValueError) as exc:
            return self._finish_active_reply(
                aggregate,
                event,
                operation_id=operation_id,
                fence=fence,
                consumed_ids=(),
                action_effects=(),
                status=SessionOperationStatus.FAILED,
                failure_code="invalid_active_reply_completion",
                failure_message=str(exc),
            )
        return self._finish_active_reply(
            aggregate,
            event,
            operation_id=operation_id,
            fence=fence,
            consumed_ids=consumed_ids,
            action_effects=action_effects,
            status=SessionOperationStatus.COMPLETED,
        )

    def _review_completed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        operation_id = aggregate.review_operation_id
        mismatch = self._workflow_completion_mismatch(
            aggregate,
            event,
            operation_id=operation_id,
            expected_state=AgentSessionState.REVIEW,
            expected_operation_kind="review",
            expected_effect_kind=AgentSessionEffectKind.RUN_REVIEW_WORKFLOW,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=operation_id,
                disposition="review_completion_stale",
                mismatch=mismatch,
            )

        fence = _operation_fence(aggregate.data, operation_id)
        try:
            result = _review_workflow_result(event.payload)
            consumed_ids = result.consumed_message_log_ids
            input_watermark, _ = _captured_input_boundary(fence)
            _validate_consumed_ids(
                consumed_ids,
                input_watermark=input_watermark,
            )
            action_effects = self._materialize_completion_actions(
                aggregate,
                event,
                operation_id=operation_id,
                fence=fence,
                intents=result.external_action_intents,
            )
            enter_active_chat = result.enter_active_chat
            next_outcome = (
                None
                if enter_active_chat
                else self._review_next_schedule_outcome(
                    event,
                    result.next_review_outcome,
                )
            )
        except (TypeError, ValueError) as exc:
            return self._finish_review(
                aggregate,
                event,
                operation_id=operation_id,
                fence=fence,
                consumed_ids=(),
                action_effects=(),
                status=SessionOperationStatus.FAILED,
                enter_active_chat=False,
                next_outcome=Failed(
                    applied_delay_seconds=(
                        self._config.default_review_delay_seconds
                    ),
                    reason="invalid_review_completion",
                    fallback_reason="invalid_review_completion",
                    failure_code="invalid_review_completion",
                    failure_message=str(exc),
                    source="session_actor",
                ),
                failure_code="invalid_review_completion",
                failure_message=str(exc),
            )
        return self._finish_review(
            aggregate,
            event,
            operation_id=operation_id,
            fence=fence,
            consumed_ids=consumed_ids,
            action_effects=action_effects,
            status=SessionOperationStatus.COMPLETED,
            enter_active_chat=enter_active_chat,
            next_outcome=next_outcome,
        )

    def _finish_review(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation_id: str,
        fence: Mapping[str, Any],
        consumed_ids: tuple[int, ...],
        action_effects: tuple[SessionEffect, ...],
        status: SessionOperationStatus,
        enter_active_chat: bool,
        next_outcome: SettledScheduleOutcome | None,
        failure_code: str = "",
        failure_message: str = "",
        terminal_event_metadata: Mapping[str, Any] | None = None,
    ) -> SessionTransition:
        input_watermark, input_ledger_sequence = _captured_input_boundary(fence)
        consumptions: tuple[ConsumeMessageLedgerEntries, ...] = ()
        if consumed_ids:
            consumptions = (
                self._message_consumption(
                    aggregate,
                    event,
                    operation_id=operation_id,
                    input_watermark=input_watermark,
                    input_ledger_sequence=input_ledger_sequence,
                    message_log_ids=consumed_ids,
                    kind=MessageLedgerConsumptionKind.REVIEW,
                ),
            )
        operation_metadata: dict[str, Any] = {
            "completion_event_id": event.event_id,
            "consumed_message_log_ids": list(consumed_ids),
            "external_action_effect_ids": [
                effect.effect_id for effect in action_effects
            ],
            "enter_active_chat": enter_active_chat,
        }
        if terminal_event_metadata is not None:
            operation_metadata.pop("completion_event_id", None)
            operation_metadata.update(dict(terminal_event_metadata))
        operation = SessionOperation(
            operation_id=operation_id,
            kind="review",
            status=status,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            finished_at=event.occurred_at,
            failure_code=failure_code,
            failure_message=failure_message,
            metadata=operation_metadata,
        )
        data = _without_operation_fence(aggregate.data, operation_id)
        if enter_active_chat:
            return self._finish_review_into_active_chat(
                aggregate,
                event,
                operation=operation,
                data=data,
                action_effects=action_effects,
                consumptions=consumptions,
            )
        if next_outcome is None:
            raise ValueError("idle review completion requires a schedule outcome")
        return self._finish_review_into_idle(
            aggregate,
            event,
            operation=operation,
            data=data,
            action_effects=action_effects,
            consumptions=consumptions,
            outcome=next_outcome,
        )

    def _finish_review_into_active_chat(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation: SessionOperation,
        data: Mapping[str, Any],
        action_effects: tuple[SessionEffect, ...],
        consumptions: tuple[ConsumeMessageLedgerEntries, ...],
    ) -> SessionTransition:
        active_epoch = aggregate.active_epoch + 1
        event_time = _event_time(aggregate, event)
        if action_effects:
            pending_data = _with_pending_outbound_actions(
                data,
                action_effects,
                source_event_id=event.event_id,
            )
            active_chat_state = {
                "active_epoch": active_epoch,
                "interest_value": self._config.provisional_active_chat_interest,
                "decay_half_life_seconds": (
                    self._config.provisional_active_chat_half_life_seconds
                ),
                "entered_at": event_time,
                "updated_at": event_time,
                "tick_count": 0,
                "pending_message_log_ids": [],
                "bootstrap_status": "waiting_outbound",
                "bootstrap_operation_id": "",
                "round_schedule_revision": 0,
                "round_schedule_id": "",
                "round_due_at": None,
            }
            target = aggregate.advance(
                state=AgentSessionState.ACTIVE_CHAT.value,
                active_epoch=active_epoch,
                review_operation_id="",
                active_chat_state=active_chat_state,
                data=pending_data,
                updated_at=event_time,
            )
            completed_schedule = self._current_review_schedule(
                aggregate,
                status=ReviewScheduleStatus.COMPLETED,
                committed_state_revision=target.state_revision,
            )
            schedule_event = SessionReviewScheduleEvent(
                schedule_event_id=self._ids.create(
                    key=event.key,
                    seed=event.event_id,
                    purpose="review-completed-event",
                ),
                event_type="completed",
                plan_id=aggregate.current_plan_id,
                previous_plan_id=aggregate.current_plan_id,
                trigger="review_completed",
                outcome="active_chat_waiting_outbound",
                source=event.source,
                reason="review_waiting_for_external_actions",
                expected_active_epoch=active_epoch,
                expected_activity_generation=aggregate.activity_generation,
                committed_state_revision=target.state_revision,
                operation_id=operation.operation_id,
                trace_id=event.trace_id,
            )
            return SessionTransition(
                aggregate=target,
                disposition="review_completed_active_chat_waiting_outbound",
                caused_operation_id=operation.operation_id,
                caused_plan_id=aggregate.current_plan_id,
                effects=action_effects,
                operations=(operation,),
                message_ledger_mutations=consumptions,
                review_schedules=(completed_schedule,),
                review_schedule_events=(schedule_event,),
                result={
                    "active_epoch": active_epoch,
                    "pending_outbound_effect_ids": [
                        effect.effect_id for effect in action_effects
                    ],
                },
                reason="review_waiting_for_external_actions",
            )
        bootstrap_operation_id = self._ids.create(
            key=event.key,
            seed=f"{operation.operation_id}:{active_epoch}",
            purpose="active-chat-bootstrap-operation",
        )
        bootstrap_effect_id = self._ids.create(
            key=event.key,
            seed=bootstrap_operation_id,
            purpose="run-active-chat-bootstrap-effect",
        )
        bootstrap_completion_event_id = self._ids.create(
            key=event.key,
            seed=bootstrap_effect_id,
            purpose="active-chat-bootstrap-completion-event",
        )
        bootstrap_failure_event_id = self._ids.create(
            key=event.key,
            seed=bootstrap_effect_id,
            purpose="active-chat-bootstrap-failure-event",
        )
        input_watermark = _message_watermark(data)
        delivery_context = _mapping(data.get(_DELIVERY_CONTEXT_DATA_KEY))
        bootstrap_fence = {
            "operation_id": bootstrap_operation_id,
            "operation_kind": "active_chat_bootstrap",
            "source_event_id": event.event_id,
            "effect_id": bootstrap_effect_id,
            "effect_kind": AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP,
            **_effect_contract_snapshot(
                AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP
            ),
            "idempotency_key": bootstrap_effect_id,
            "completion_event_id": bootstrap_completion_event_id,
            "failure_event_id": bootstrap_failure_event_id,
            "ownership_generation": aggregate.ownership_generation,
            "plan_id": aggregate.current_plan_id,
            "active_epoch": active_epoch,
            "activity_generation": aggregate.activity_generation,
            "input_watermark": input_watermark,
            "input_ledger_sequence": None,
            "instance_id": _text(delivery_context.get("instance_id")),
            "target_session_id": _text(
                delivery_context.get("target_session_id")
            ),
        }
        next_data = _with_operation_fence(
            data,
            bootstrap_operation_id,
            bootstrap_fence,
        )
        active_chat_state = {
            "active_epoch": active_epoch,
            "interest_value": self._config.provisional_active_chat_interest,
            "decay_half_life_seconds": (
                self._config.provisional_active_chat_half_life_seconds
            ),
            "entered_at": event_time,
            "updated_at": event_time,
            "tick_count": 0,
            "pending_message_log_ids": [],
            "bootstrap_status": "pending",
            "bootstrap_operation_id": bootstrap_operation_id,
            "round_schedule_revision": 0,
            "round_schedule_id": "",
            "round_due_at": None,
        }
        target = aggregate.advance(
            state=AgentSessionState.ACTIVE_CHAT.value,
            active_epoch=active_epoch,
            review_operation_id="",
            active_chat_state=active_chat_state,
            data=next_data,
            updated_at=event_time,
        )
        bootstrap_operation = SessionOperation(
            operation_id=bootstrap_operation_id,
            kind="active_chat_bootstrap",
            status=SessionOperationStatus.PENDING,
            launched_by_event_id=event.event_id,
            state_revision=target.state_revision,
            active_epoch=active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            started_at=event.occurred_at,
            metadata={
                "review_operation_id": operation.operation_id,
                "review_plan_id": aggregate.current_plan_id,
            },
        )
        bootstrap_effect = _durable_effect(
            effect_id=bootstrap_effect_id,
            kind=AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP,
            idempotency_key=bootstrap_effect_id,
            operation_id=bootstrap_operation_id,
            payload={
                **bootstrap_fence,
                "review_completion": _mapping(event.payload.get("workflow_result")),
            },
        )
        completed_schedule = self._current_review_schedule(
            aggregate,
            status=ReviewScheduleStatus.COMPLETED,
            committed_state_revision=target.state_revision,
        )
        schedule_event = SessionReviewScheduleEvent(
            schedule_event_id=self._ids.create(
                key=event.key,
                seed=event.event_id,
                purpose="review-completed-event",
            ),
            event_type="completed",
            plan_id=aggregate.current_plan_id,
            previous_plan_id=aggregate.current_plan_id,
            trigger="review_completed",
            outcome="active_chat_started",
            source=event.source,
            reason="review_entered_active_chat",
            expected_active_epoch=active_epoch,
            expected_activity_generation=aggregate.activity_generation,
            committed_state_revision=target.state_revision,
            operation_id=operation.operation_id,
            trace_id=event.trace_id,
        )
        return SessionTransition(
            aggregate=target,
            disposition="review_completed_active_chat_started",
            caused_operation_id=operation.operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=(*action_effects, bootstrap_effect),
            operations=(operation, bootstrap_operation),
            message_ledger_mutations=consumptions,
            review_schedules=(completed_schedule,),
            review_schedule_events=(schedule_event,),
            result={
                "active_epoch": active_epoch,
                "bootstrap_operation_id": bootstrap_operation_id,
                "external_action_effect_ids": [
                    effect.effect_id for effect in action_effects
                ],
            },
            reason="review_workflow_completed",
        )

    def _finish_review_into_idle(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation: SessionOperation,
        data: Mapping[str, Any],
        action_effects: tuple[SessionEffect, ...],
        consumptions: tuple[ConsumeMessageLedgerEntries, ...],
        outcome: SettledScheduleOutcome,
    ) -> SessionTransition:
        plan_id = self._ids.create(
            key=event.key,
            seed=operation.operation_id,
            purpose="post-review-plan",
        )
        plan_revision = aggregate.review_plan_revision + 1
        outcome_payload = outcome.to_payload()
        review_plan = {
            "plan_id": plan_id,
            "plan_revision": plan_revision,
            "trigger": "review_completed",
            **outcome_payload,
            "expected_active_epoch": aggregate.active_epoch,
            "expected_activity_generation": aggregate.activity_generation,
        }
        next_data = (
            _with_pending_outbound_actions(
                data,
                action_effects,
                source_event_id=event.event_id,
            )
            if action_effects
            else dict(data)
        )
        target = aggregate.advance(
            state=AgentSessionState.IDLE.value,
            current_plan_id=plan_id,
            review_plan_revision=plan_revision,
            review_plan=review_plan,
            review_operation_id="",
            active_reply_resume={},
            data=next_data,
            updated_at=_event_time(aggregate, event),
        )
        schedule = SessionReviewSchedule(
            plan_id=plan_id,
            plan_revision=plan_revision,
            applied_delay_seconds=outcome.applied_delay_seconds,
            status=ReviewScheduleStatus.SCHEDULED,
            trigger="review_completed",
            outcome=outcome.kind.value,
            source=outcome.source,
            requested_delay_seconds=outcome.requested_delay_seconds,
            reason=outcome.reason,
            fallback_reason=outcome.fallback_reason,
            mention_sensitivity=outcome.mention_sensitivity,
            active_reply_threshold=outcome.active_reply_threshold,
            model_execution_id=outcome.model_execution_id,
            prompt_signature=outcome.prompt_signature,
            expected_active_epoch=aggregate.active_epoch,
            expected_activity_generation=aggregate.activity_generation,
            committed_state_revision=target.state_revision,
        )
        schedule_event = SessionReviewScheduleEvent(
            schedule_event_id=self._ids.create(
                key=event.key,
                seed=event.event_id,
                purpose="post-review-schedule-event",
            ),
            event_type="scheduled",
            plan_id=plan_id,
            previous_plan_id=aggregate.current_plan_id,
            trigger="review_completed",
            outcome=outcome.kind.value,
            source=outcome.source,
            requested_delay_seconds=outcome.requested_delay_seconds,
            applied_delay_seconds=outcome.applied_delay_seconds,
            reason=outcome.reason,
            fallback_reason=outcome.fallback_reason,
            model_execution_id=outcome.model_execution_id,
            prompt_signature=outcome.prompt_signature,
            expected_active_epoch=aggregate.active_epoch,
            expected_activity_generation=aggregate.activity_generation,
            committed_state_revision=target.state_revision,
            operation_id=operation.operation_id,
            trace_id=event.trace_id,
            metadata={"schedule_outcome": outcome_payload},
        )
        return SessionTransition(
            aggregate=target,
            disposition=(
                "review_completed_idle_waiting_outbound"
                if action_effects
                else "review_completed_idle_scheduled"
            ),
            caused_operation_id=operation.operation_id,
            caused_plan_id=plan_id,
            effects=action_effects,
            operations=(operation,),
            message_ledger_mutations=consumptions,
            review_schedules=(schedule,),
            review_schedule_events=(schedule_event,),
            result={
                "plan_id": plan_id,
                "schedule_outcome": outcome_payload,
                "external_action_effect_ids": [
                    effect.effect_id for effect in action_effects
                ],
            },
            reason=(
                "review_waiting_for_external_actions"
                if action_effects
                else outcome.reason
            ),
        )

    def _review_next_schedule_outcome(
        self,
        event: SessionEventEnvelope,
        wire_outcome: ReviewNextReviewOutcome | None,
    ) -> SettledScheduleOutcome:
        if wire_outcome is None:
            raise TypeError(
                "idle ReviewCompleted requires next_review_outcome"
            )
        normalized_payload = {
            "outcome": {
                "kind": wire_outcome.kind.value,
                "requested_delay_seconds": (
                    wire_outcome.requested_delay_seconds
                ),
                "reason": wire_outcome.reason,
            },
            "source": "review_workflow",
            "model_execution_id": event.payload.get("model_execution_id", ""),
            "prompt_signature": event.payload.get("prompt_signature", ""),
        }
        outcome = self.resolve_schedule_outcome(
            replace(event, payload=normalized_payload)
        )
        if wire_outcome.kind is ReviewNextReviewOutcomeKind.BYPASSED:
            return Bypassed(
                applied_delay_seconds=self._config.apply_delay(
                    wire_outcome.applied_delay_seconds
                ),
                reason=wire_outcome.reason,
                fallback_reason=wire_outcome.fallback_reason,
                source="review_workflow",
            )
        return outcome

    def _current_review_schedule(
        self,
        aggregate: AgentSessionAggregate,
        *,
        status: ReviewScheduleStatus,
        committed_state_revision: int,
    ) -> SessionReviewSchedule:
        plan = dict(aggregate.review_plan)
        applied_delay = _optional_delay(plan.get("applied_delay_seconds"))
        if applied_delay is None:
            applied_delay = self._config.default_review_delay_seconds
        return SessionReviewSchedule(
            plan_id=aggregate.current_plan_id,
            plan_revision=aggregate.review_plan_revision,
            applied_delay_seconds=applied_delay,
            status=status,
            trigger=_text(plan.get("trigger")),
            outcome=_text(plan.get("kind") or plan.get("outcome")),
            source=_text(plan.get("source")),
            requested_delay_seconds=_optional_delay(
                plan.get("requested_delay_seconds")
            ),
            reason=_text(plan.get("reason")),
            fallback_reason=_text(plan.get("fallback_reason")),
            mention_sensitivity=(
                _text(plan.get("mention_sensitivity")) or "normal"
            ),
            active_reply_threshold=_mapping(
                plan.get("active_reply_threshold")
            ),
            model_execution_id=_text(plan.get("model_execution_id")),
            prompt_signature=_text(plan.get("prompt_signature")),
            expected_active_epoch=_optional_nonnegative_int(
                plan.get("expected_active_epoch")
            ),
            expected_activity_generation=_optional_nonnegative_int(
                plan.get("expected_activity_generation")
            ),
            committed_state_revision=committed_state_revision,
        )

    @staticmethod
    def _deferred_active_reply_resume(
        resume: Mapping[str, Any],
        *,
        event: SessionEventEnvelope,
        failure: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Retain an interrupted review handoff after a reply effect failed."""

        if not resume:
            return {}
        deferred = dict(resume)
        deferred.update(
            {
                "status": "deferred",
                "deferred_by_event_id": event.event_id,
                "failure_event_id": event.event_id,
                "failure_code": _text(failure.get("failure_code")),
                "failure_message": _text(failure.get("failure_message")),
                "defer_attempt_count": (
                    _optional_nonnegative_int(
                        resume.get("defer_attempt_count")
                    )
                    or 0
                )
                + 1,
            }
        )
        return deferred

    def _defer_active_reply_resume(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        target: AgentSessionAggregate,
        operation_id: str,
        resume: Mapping[str, Any],
        failure_code: str,
    ) -> tuple[
        tuple[SessionReviewSchedule, ...],
        tuple[SessionReviewScheduleEvent, ...],
    ]:
        """Return one delayed schedule for a failed reply's review handoff."""

        if not resume or not aggregate.current_plan_id:
            return (), ()
        retry_at = _event_time(aggregate, event) + self._config.busy_review_retry_seconds
        schedule = replace(
            self._current_review_schedule(
                aggregate,
                status=ReviewScheduleStatus.SCHEDULED,
                committed_state_revision=target.state_revision,
            ),
            trigger="active_reply_effect_failed",
            source=event.source or "effect_executor",
            reason="active_reply_effect_failed",
            fallback_reason="active_reply_effect_failed",
            available_at=retry_at,
            attempt_count=(
                _optional_nonnegative_int(resume.get("defer_attempt_count"))
                or 1
            ),
            last_error=f"active_reply_effect_failed:{failure_code}",
        )
        schedule_event = SessionReviewScheduleEvent(
            schedule_event_id=self._ids.create(
                key=event.key,
                seed=f"{event.event_id}:{operation_id}",
                purpose="active-reply-effect-failure-deferred-event",
            ),
            event_type="deferred",
            plan_id=aggregate.current_plan_id,
            previous_plan_id=aggregate.current_plan_id,
            trigger="active_reply_effect_failed",
            outcome="deferred",
            source=event.source or "effect_executor",
            applied_delay_seconds=schedule.applied_delay_seconds,
            reason="active_reply_effect_failed",
            fallback_reason="active_reply_effect_failed",
            expected_active_epoch=aggregate.active_epoch,
            expected_activity_generation=aggregate.activity_generation,
            committed_state_revision=target.state_revision,
            operation_id=operation_id,
            trace_id=event.trace_id,
            metadata={"resume": dict(resume), "retry_at": retry_at},
        )
        return (schedule,), (schedule_event,)

    def _finish_active_reply(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation_id: str,
        fence: Mapping[str, Any],
        consumed_ids: tuple[int, ...],
        action_effects: tuple[SessionEffect, ...],
        status: SessionOperationStatus,
        failure_code: str = "",
        failure_message: str = "",
        resume_review: bool = True,
        retained_resume: Mapping[str, Any] | None = None,
        terminal_event_metadata: Mapping[str, Any] | None = None,
    ) -> SessionTransition:
        input_watermark, input_ledger_sequence = _captured_input_boundary(fence)
        consumptions: tuple[ConsumeMessageLedgerEntries, ...] = ()
        if consumed_ids:
            _validate_consumed_ids(
                consumed_ids,
                input_watermark=input_watermark,
            )
            consumptions = tuple(
                self._message_consumption(
                    aggregate,
                    event,
                    operation_id=operation_id,
                    input_watermark=input_watermark,
                    input_ledger_sequence=input_ledger_sequence,
                    message_log_ids=consumed_ids,
                    kind=kind,
                )
                for kind in (
                    MessageLedgerConsumptionKind.CHAT,
                    MessageLedgerConsumptionKind.HIGH_PRIORITY,
                )
            )
        data = _without_pending_high_priority_ids(
            _without_operation_fence(aggregate.data, operation_id),
            consumed_ids,
        )
        operation_metadata: dict[str, Any] = {
            "completion_event_id": event.event_id,
            "consumed_message_log_ids": list(consumed_ids),
            "external_action_effect_ids": [
                effect.effect_id for effect in action_effects
            ],
        }
        if terminal_event_metadata is not None:
            operation_metadata.pop("completion_event_id", None)
            operation_metadata.update(dict(terminal_event_metadata))
        terminal_operation = SessionOperation(
            operation_id=operation_id,
            kind="active_reply",
            status=status,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            finished_at=event.occurred_at,
            failure_code=failure_code,
            failure_message=failure_message,
            metadata=operation_metadata,
        )
        resume = dict(aggregate.active_reply_resume)
        operations: tuple[SessionOperation, ...] = (terminal_operation,)
        effects = action_effects
        review_operation_id = ""
        disposition = (
            "active_reply_completed"
            if status is SessionOperationStatus.COMPLETED
            else "active_reply_completion_rejected"
        )
        state = AgentSessionState.IDLE
        if action_effects:
            data = _with_pending_outbound_actions(
                data,
                action_effects,
                source_event_id=event.event_id,
            )
            disposition = (
                "active_reply_completed_waiting_outbound"
                if status is SessionOperationStatus.COMPLETED
                else "active_reply_rejected_waiting_outbound"
            )
        elif resume_review and _text(resume.get("kind")) in {
            "resume_due_review",
            "resume_interrupted_review",
        }:
            data, review_operation, review_effect = self._build_review_work(
                aggregate,
                event,
                data=data,
            )
            operations = (*operations, review_operation)
            effects = (*effects, review_effect)
            review_operation_id = review_operation.operation_id
            disposition = (
                "active_reply_completed_review_resumed"
                if status is SessionOperationStatus.COMPLETED
                else "active_reply_rejected_review_resumed"
            )
            state = AgentSessionState.REVIEW
        if retained_resume is not None:
            next_resume = dict(retained_resume)
        else:
            next_resume = resume if action_effects else {}
        target = aggregate.advance(
            state=state.value,
            review_operation_id=review_operation_id,
            active_reply_operation_id="",
            active_reply_resume=next_resume,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition=disposition,
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=effects,
            operations=operations,
            message_ledger_mutations=consumptions,
            result={
                "consumed_message_log_ids": list(consumed_ids),
                "external_action_effect_ids": [
                    effect.effect_id for effect in action_effects
                ],
                "review_operation_id": review_operation_id,
                "status": status.value,
            },
            reason=failure_code or "active_reply_workflow_completed",
        )

    def _workflow_completion_mismatch(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation_id: str,
        expected_state: AgentSessionState,
        expected_operation_kind: str,
        expected_effect_kind: AgentSessionEffectKind,
    ) -> tuple[str, ...]:
        registry = _mapping(aggregate.data.get("operation_fences"))
        fence = _mapping(registry.get(operation_id))
        if not operation_id or not fence:
            return ("operation_fence_changed",)
        mismatch = list(
            _effect_event_provenance_mismatch(
                aggregate,
                event,
                expected_event_id=_text(fence.get("completion_event_id")),
                expected_effect_kind=expected_effect_kind,
                expected_effect_id=_text(fence.get("effect_id")),
                expected_idempotency_key=_text(fence.get("idempotency_key")),
                expected_operation_id=operation_id,
                expected_plan_id=_text(fence.get("plan_id")),
                expected_active_epoch=_optional_nonnegative_int(
                    fence.get("active_epoch")
                ),
                expected_activity_generation=_optional_nonnegative_int(
                    fence.get("activity_generation")
                ),
                expected_input_watermark=_optional_nonnegative_int(
                    fence.get("input_watermark")
                ),
                expected_input_ledger_sequence=_optional_nonnegative_int(
                    fence.get("input_ledger_sequence")
                ),
                expected_causation_id=_text(fence.get("source_event_id")),
                expected_ownership_generation=_optional_nonnegative_int(
                    fence.get("ownership_generation")
                ),
                expected_contract_version=_optional_nonnegative_int(
                    fence.get("contract_version")
                ),
                expected_contract_signature=_text(fence.get("contract_signature")),
            )
        )
        if aggregate.state != expected_state:
            mismatch.append("state_changed")
        if _text(fence.get("operation_kind")) != expected_operation_kind:
            mismatch.append("operation_kind_changed")
        if _optional_nonnegative_int(fence.get("input_ledger_sequence")) is None:
            mismatch.append("input_ledger_sequence_uncaptured")
        return tuple(dict.fromkeys(mismatch))

    def _workflow_effect_failure_mismatch(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation_id: str,
        expected_state: AgentSessionState,
        expected_operation_kind: str,
        expected_effect_kind: AgentSessionEffectKind,
    ) -> tuple[str, ...]:
        """Validate one terminal workflow failure against its operation fence."""

        registry = _mapping(aggregate.data.get("operation_fences"))
        fence = _mapping(registry.get(operation_id))
        if not operation_id or not fence:
            return ("operation_fence_changed",)
        mismatch = list(
            _effect_event_provenance_mismatch(
                aggregate,
                event,
                expected_event_id=_text(fence.get("failure_event_id")),
                expected_effect_kind=expected_effect_kind,
                expected_effect_id=_text(fence.get("effect_id")),
                expected_idempotency_key=_text(fence.get("idempotency_key")),
                expected_operation_id=operation_id,
                expected_plan_id=_text(fence.get("plan_id")),
                expected_active_epoch=_optional_nonnegative_int(
                    fence.get("active_epoch")
                ),
                expected_activity_generation=_optional_nonnegative_int(
                    fence.get("activity_generation")
                ),
                expected_input_watermark=_optional_nonnegative_int(
                    fence.get("input_watermark")
                ),
                expected_input_ledger_sequence=_optional_nonnegative_int(
                    fence.get("input_ledger_sequence")
                ),
                expected_causation_id=_text(fence.get("source_event_id")),
                expected_ownership_generation=_optional_nonnegative_int(
                    fence.get("ownership_generation")
                ),
                expected_contract_version=_optional_nonnegative_int(
                    fence.get("contract_version")
                ),
                expected_contract_signature=_text(fence.get("contract_signature")),
            )
        )
        if aggregate.state != expected_state:
            mismatch.append("state_changed")
        if _text(fence.get("operation_kind")) != expected_operation_kind:
            mismatch.append("operation_kind_changed")
        if _optional_nonnegative_int(fence.get("input_ledger_sequence")) is None:
            mismatch.append("input_ledger_sequence_uncaptured")
        if not _text(event.payload.get("failure_code")):
            mismatch.append("failure_code_missing")
        if not isinstance(event.payload.get("failure_message"), str):
            mismatch.append("failure_message_invalid")
        return tuple(dict.fromkeys(mismatch))

    def _stale_workflow_completion(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation_id: str,
        disposition: str,
        mismatch: tuple[str, ...],
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(
                state_changed=False,
                updated_at=_event_time(aggregate, event),
            ),
            disposition=disposition,
            caused_operation_id=(
                _text(event.payload.get("operation_id")) or operation_id
            ),
            caused_plan_id=_text(event.payload.get("plan_id")),
            result={"mismatch": list(mismatch)},
            reason=",".join(mismatch),
        )

    def _message_consumption(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation_id: str,
        input_watermark: int,
        input_ledger_sequence: int,
        message_log_ids: tuple[int, ...],
        kind: MessageLedgerConsumptionKind,
    ) -> ConsumeMessageLedgerEntries:
        consumption_id = self._ids.create(
            key=event.key,
            seed=f"{operation_id}:{kind.value}",
            purpose="message-consumption",
        )
        return ConsumeMessageLedgerEntries(
            key=event.key,
            kind=kind,
            selection=MessageLedgerConsumptionSelection.EXPLICIT_IDS,
            consumption_id=consumption_id,
            idempotency_key=f"{operation_id}:{kind.value}:messages",
            operation_id=operation_id,
            source_event_id=event.event_id,
            ownership_generation=aggregate.ownership_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            explicit_message_log_ids=message_log_ids,
            reason=f"{kind.value}_workflow_completed",
            trace_id=event.trace_id,
            occurred_at=event.occurred_at,
        )

    @staticmethod
    def _materialize_completion_actions(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        operation_id: str,
        fence: Mapping[str, Any],
        intents: tuple[ExternalActionIntent, ...],
    ) -> tuple[SessionEffect, ...]:
        if not intents:
            return ()
        instance_id = _text(fence.get("instance_id"))
        target_session_id = _text(fence.get("target_session_id"))
        if not instance_id or not target_session_id:
            raise ValueError("operation omitted trusted external-action target context")
        return materialize_external_action_effects(
            key=event.key,
            ownership_generation=aggregate.ownership_generation,
            operation_id=operation_id,
            source_event_id=event.event_id,
            instance_id=instance_id,
            target_session_id=target_session_id,
            intents=intents,
        )

    def _settle_or_supersede(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        mismatch = self._fence_mismatch(aggregate, event)
        if mismatch:
            return self._stale_completion(aggregate, event, mismatch)
        deadline_reached = (
            event.kind
            == AgentSessionEventKind.IDLE_REVIEW_PLANNING_DEADLINE_REACHED
        )
        outcome = self.resolve_schedule_outcome(
            event,
            deadline_reached=deadline_reached,
        )
        return self._settle_exit(aggregate, event, outcome)

    def _effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        effect_kind = _text(event.payload.get("effect_kind"))
        try:
            action_kind = ExternalActionKind(effect_kind)
        except ValueError:
            action_kind = None
        if action_kind is not None:
            return self._external_action_effect_failed(
                aggregate,
                event,
                effect_kind=action_kind.value,
            )
        if effect_kind == AgentSessionEffectKind.CANCEL_REVIEW_WORKFLOW:
            return self._review_cancellation_effect_failed(aggregate, event)
        if effect_kind == AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST:
            return self._active_chat_exit_request_effect_failed(aggregate, event)
        if effect_kind == AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE:
            return self._active_chat_round_due_effect_failed(aggregate, event)
        if effect_kind == AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW:
            return self._active_reply_effect_failed(aggregate, event)
        if effect_kind == AgentSessionEffectKind.RUN_REVIEW_WORKFLOW:
            return self._review_effect_failed(aggregate, event)
        if effect_kind == AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP:
            return self._active_chat_bootstrap_effect_failed(aggregate, event)
        if effect_kind == AgentSessionEffectKind.RUN_ACTIVE_CHAT_ROUND:
            return self._active_chat_round_effect_failed(aggregate, event)
        if effect_kind in {
            AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING,
            AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE,
        }:
            return self._planning_effect_failed(
                aggregate,
                event,
                effect_kind=effect_kind,
            )
        if effect_kind in {
            AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING,
            AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
        }:
            return self._control_effect_failed(
                aggregate,
                event,
                effect_kind=effect_kind,
            )
        if effect_kind in {
            AgentSessionEffectKind.ACTIVE_CHAT_RUNTIME_RECONCILIATION,
            AgentSessionEffectKind.IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILIATION,
        }:
            return self._reconciliation_effect_failed(
                aggregate,
                event,
                effect_kind=effect_kind,
            )
        return self._ignored(
            aggregate,
            event,
            disposition="ignored_unrelated_effect_failure",
            reason=f"unrelated_effect_failed:{effect_kind or 'unknown'}",
        )

    def _review_cancellation_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Fail closed when an interrupted review cannot be cancelled."""

        effect_kind = AgentSessionEffectKind.CANCEL_REVIEW_WORKFLOW
        intent = _control_intent(aggregate.data, effect_kind)
        mismatch = self._control_effect_failure_mismatch(
            aggregate,
            event,
            intent=intent,
            effect_kind=effect_kind,
        )
        if mismatch:
            return self._ignored(
                aggregate,
                event,
                disposition="review_cancellation_effect_failure_stale",
                reason=",".join(mismatch),
            )
        return self._block_review_cancellation(
            aggregate,
            event,
            intent=intent,
            failure=_effect_failure_record(event),
            disposition="review_cancellation_effect_failed_blocked",
        )

    def _active_chat_exit_request_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Retry a failed exit request only through its fenced control intent."""

        effect_kind = AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
        intent = _control_intent(aggregate.data, effect_kind)
        mismatch = self._active_chat_exit_request_control_mismatch(
            aggregate,
            event,
            intent=intent,
            expected_event_field="failure_event_id",
        )
        if mismatch:
            return self._ignored(
                aggregate,
                event,
                disposition="active_chat_exit_request_failure_stale",
                reason=",".join(mismatch),
            )

        failure = _effect_failure_record(event)
        cycle = _optional_nonnegative_int(intent.get("retry_cycle")) or 0
        active_state = dict(aggregate.active_chat_state)
        if cycle + 1 < self._config.control_reconciliation_max_cycles:
            retry_effect, retry_intent = self._enqueue_active_chat_exit_request(
                aggregate,
                event,
                trigger=_text(intent.get("trigger")) or "active_chat_exit_retry",
                input_watermark=_message_watermark(aggregate.data),
                retry_cycle=cycle + 1,
            )
            retry_intent["prior_failure"] = failure
            data = _with_control_intent(
                aggregate.data,
                effect_kind=effect_kind,
                intent=retry_intent,
            )
            target = aggregate.advance(
                active_chat_state={
                    **active_state,
                    "exit_requested": True,
                    "exit_last_failure": failure,
                },
                data=data,
                updated_at=_event_time(aggregate, event),
            )
            return SessionTransition(
                aggregate=target,
                disposition="active_chat_exit_request_retry_scheduled",
                caused_plan_id=aggregate.current_plan_id,
                effects=(retry_effect,),
                result={
                    "failure": failure,
                    "retry_cycle": cycle + 1,
                    "effect_id": retry_effect.effect_id,
                },
                reason="active_chat_exit_request_effect_failed",
            )

        failed_intent = {
            **intent,
            "status": "failed",
            "last_failure": failure,
            "retry_cycle": cycle,
        }
        data = _with_control_intent(
            aggregate.data,
            effect_kind=effect_kind,
            intent=failed_intent,
        )
        target = aggregate.advance(
            active_chat_state={
                **active_state,
                "exit_requested": False,
                "exit_blocker": {
                    "effect_id": _text(intent.get("effect_id")),
                    "failure_event_id": event.event_id,
                    "failure_code": _text(failure.get("failure_code")),
                },
            },
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="active_chat_exit_request_failed_blocked",
            caused_plan_id=aggregate.current_plan_id,
            result={"failure": failure, "retry_cycle": cycle},
            reason="active_chat_exit_request_retries_exhausted",
        )

    def _active_chat_round_due_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Retry one failed round timer or retain an explicit input blocker."""

        effect_kind = AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE
        intent = _control_intent(aggregate.data, effect_kind)
        active_state = _mapping(aggregate.active_chat_state)
        mismatch = self._active_chat_round_due_control_mismatch(
            aggregate,
            event,
            active_state=active_state,
            intent=intent,
            expected_event_field="failure_event_id",
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=aggregate.active_chat_round_operation_id,
                disposition="active_chat_round_due_failure_stale",
                mismatch=mismatch,
            )

        failure = _effect_failure_record(event)
        cycle = _optional_nonnegative_int(intent.get("retry_cycle")) or 0
        pending_ids = _positive_int_tuple(
            active_state.get("pending_message_log_ids"),
            field_name="active_chat_state.pending_message_log_ids",
            allow_empty=True,
        )
        if (
            pending_ids
            and cycle + 1 < self._config.control_reconciliation_max_cycles
        ):
            next_state, retry_effect, retry_intent = (
                self._schedule_active_chat_round_due(
                    aggregate,
                    event,
                    active_chat_state=active_state,
                    input_watermark=_message_watermark(aggregate.data),
                    delay_seconds=self._config.busy_review_retry_seconds,
                    retry_cycle=cycle + 1,
                )
            )
            retry_intent["prior_failure"] = failure
            data = _with_control_intent(
                aggregate.data,
                effect_kind=effect_kind,
                intent=retry_intent,
            )
            target = aggregate.advance(
                active_chat_state=next_state,
                data=data,
                updated_at=_event_time(aggregate, event),
            )
            return SessionTransition(
                aggregate=target,
                disposition="active_chat_round_due_retry_scheduled",
                caused_plan_id=aggregate.current_plan_id,
                effects=(retry_effect,),
                result={
                    "failure": failure,
                    "retry_cycle": cycle + 1,
                    "round_schedule_id": _text(next_state.get("round_schedule_id")),
                },
                reason="active_chat_round_due_effect_failed",
            )

        failed_intent = {
            **intent,
            "status": "failed",
            "last_failure": failure,
            "retry_cycle": cycle,
        }
        next_state = {
            **active_state,
            "round_schedule_id": "",
            "round_due_at": None,
            "round_schedule_effect_id": "",
            "round_due_event_id": "",
            "round_schedule_failure_event_id": "",
            "round_schedule_contract_version": 0,
            "round_schedule_contract_signature": "",
            "round_schedule_blocker": {
                "effect_id": _text(intent.get("effect_id")),
                "failure_event_id": event.event_id,
                "failure_code": _text(failure.get("failure_code")),
            },
        }
        data = _with_control_intent(
            aggregate.data,
            effect_kind=effect_kind,
            intent=failed_intent,
        )
        target = aggregate.advance(
            active_chat_state=next_state,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="active_chat_round_due_failed_blocked",
            caused_plan_id=aggregate.current_plan_id,
            result={
                "failure": failure,
                "retry_cycle": cycle,
                "remaining_message_log_ids": list(pending_ids),
            },
            reason="active_chat_round_due_retries_exhausted",
        )

    def _active_reply_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Terminalize one active-reply workflow without bypassing review work."""

        operation_id = aggregate.active_reply_operation_id
        mismatch = self._workflow_effect_failure_mismatch(
            aggregate,
            event,
            operation_id=operation_id,
            expected_state=AgentSessionState.ACTIVE_REPLY,
            expected_operation_kind="active_reply",
            expected_effect_kind=AgentSessionEffectKind.RUN_ACTIVE_REPLY_WORKFLOW,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=operation_id,
                disposition="active_reply_effect_failure_stale",
                mismatch=mismatch,
            )

        fence = _operation_fence(aggregate.data, operation_id)
        failure = _workflow_effect_failure_record(event)
        failure_code = _text(failure.get("failure_code")) or (
            "active_reply_workflow_effect_failed"
        )
        failure_message = _text(failure.get("failure_message"))
        resume = self._deferred_active_reply_resume(
            aggregate.active_reply_resume,
            event=event,
            failure=failure,
        )
        transition = self._finish_active_reply(
            aggregate,
            event,
            operation_id=operation_id,
            fence=fence,
            consumed_ids=(),
            action_effects=(),
            status=SessionOperationStatus.FAILED,
            failure_code=failure_code,
            failure_message=failure_message,
            resume_review=False,
            retained_resume=resume,
            terminal_event_metadata={
                "failure_event_id": event.event_id,
                "effect_failure": failure,
            },
        )
        schedules, schedule_events = self._defer_active_reply_resume(
            aggregate,
            event,
            target=transition.aggregate,
            operation_id=operation_id,
            resume=resume,
            failure_code=failure_code,
        )
        disposition = (
            "active_reply_effect_failed_review_deferred"
            if schedules
            else "active_reply_effect_failed"
        )
        return replace(
            transition,
            disposition=disposition,
            review_schedules=schedules,
            review_schedule_events=schedule_events,
            result={
                **transition.result,
                "failure": failure,
                "resume_deferred": bool(schedules),
            },
            reason=failure_code,
        )

    def _review_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Apply the review completion fallback after terminal effect failure."""

        operation_id = aggregate.review_operation_id
        mismatch = self._workflow_effect_failure_mismatch(
            aggregate,
            event,
            operation_id=operation_id,
            expected_state=AgentSessionState.REVIEW,
            expected_operation_kind="review",
            expected_effect_kind=AgentSessionEffectKind.RUN_REVIEW_WORKFLOW,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=operation_id,
                disposition="review_effect_failure_stale",
                mismatch=mismatch,
            )

        fence = _operation_fence(aggregate.data, operation_id)
        failure = _workflow_effect_failure_record(event)
        failure_code = _text(failure.get("failure_code")) or (
            "review_workflow_effect_failed"
        )
        failure_message = _text(failure.get("failure_message"))
        transition = self._finish_review(
            aggregate,
            event,
            operation_id=operation_id,
            fence=fence,
            consumed_ids=(),
            action_effects=(),
            status=SessionOperationStatus.FAILED,
            enter_active_chat=False,
            next_outcome=Failed(
                applied_delay_seconds=self._config.default_review_delay_seconds,
                reason="review_workflow_effect_failed",
                fallback_reason="review_workflow_effect_failed",
                failure_code=failure_code,
                failure_message=failure_message,
                source=event.source or "effect_executor",
            ),
            failure_code=failure_code,
            failure_message=failure_message,
            terminal_event_metadata={
                "failure_event_id": event.event_id,
                "effect_failure": failure,
            },
        )
        return replace(
            transition,
            disposition="review_effect_failed_idle_scheduled",
            result={**transition.result, "failure": failure},
            reason=failure_code,
        )

    def _active_chat_bootstrap_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Fail bootstrap closed and enter the normal fenced active-chat exit."""

        active_state = _mapping(aggregate.active_chat_state)
        operation_id = _text(active_state.get("bootstrap_operation_id"))
        mismatch = self._workflow_effect_failure_mismatch(
            aggregate,
            event,
            operation_id=operation_id,
            expected_state=AgentSessionState.ACTIVE_CHAT,
            expected_operation_kind="active_chat_bootstrap",
            expected_effect_kind=AgentSessionEffectKind.RUN_ACTIVE_CHAT_BOOTSTRAP,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=operation_id,
                disposition="active_chat_bootstrap_effect_failure_stale",
                mismatch=mismatch,
            )

        fence = _operation_fence(aggregate.data, operation_id)
        input_watermark, input_ledger_sequence = _captured_input_boundary(fence)
        failure = _workflow_effect_failure_record(event)
        failure_code = _text(failure.get("failure_code")) or (
            "active_chat_bootstrap_effect_failed"
        )
        failure_message = _text(failure.get("failure_message"))
        operation = SessionOperation(
            operation_id=operation_id,
            kind="active_chat_bootstrap",
            status=SessionOperationStatus.FAILED,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            finished_at=event.occurred_at,
            failure_code=failure_code,
            failure_message=failure_message,
            metadata={
                "failure_event_id": event.event_id,
                "effect_failure": failure,
                "disposition": "effect_failed_exit_requested",
            },
        )
        data = _without_operation_fence(aggregate.data, operation_id)
        next_state = dict(active_state)
        next_state.update(
            {
                "bootstrap_status": "exit_requested",
                "bootstrap_operation_id": "",
                "bootstrap_disposition": "effect_failed",
                "bootstrap_reason": failure_code,
                "bootstrap_failure_event_id": event.event_id,
            }
        )
        exit_effect, exit_intent = self._enqueue_active_chat_exit_request(
            aggregate,
            event,
            trigger="active_chat_bootstrap_effect_failed",
            input_watermark=_message_watermark(data),
        )
        data = _with_control_intent(
            data,
            effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
            intent=exit_intent,
        )
        target = aggregate.advance(
            active_chat_state=next_state,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="active_chat_bootstrap_effect_failed_exit_requested",
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=(exit_effect,),
            operations=(operation,),
            result={"failure": failure},
            reason=failure_code,
        )

    def _active_chat_round_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        """Terminalize one failed round and retry only through its timer lane."""

        operation_id = aggregate.active_chat_round_operation_id
        mismatch = self._workflow_effect_failure_mismatch(
            aggregate,
            event,
            operation_id=operation_id,
            expected_state=AgentSessionState.ACTIVE_CHAT,
            expected_operation_kind="active_chat_round",
            expected_effect_kind=AgentSessionEffectKind.RUN_ACTIVE_CHAT_ROUND,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=operation_id,
                disposition="active_chat_round_effect_failure_stale",
                mismatch=mismatch,
            )

        fence = _operation_fence(aggregate.data, operation_id)
        input_watermark, input_ledger_sequence = _captured_input_boundary(fence)
        failure = _workflow_effect_failure_record(event)
        failure_code = _text(failure.get("failure_code")) or (
            "active_chat_round_effect_failed"
        )
        failure_message = _text(failure.get("failure_message"))
        operation = SessionOperation(
            operation_id=operation_id,
            kind="active_chat_round",
            status=SessionOperationStatus.FAILED,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            finished_at=event.occurred_at,
            failure_code=failure_code,
            failure_message=failure_message,
            metadata={
                "failure_event_id": event.event_id,
                "effect_failure": failure,
                "outcome": ActiveChatRoundOutcome.RETRY.value,
                "consumed_message_log_ids": [],
                "external_action_effect_ids": [],
            },
        )
        data = _without_operation_fence(aggregate.data, operation_id)
        active_state = dict(aggregate.active_chat_state)
        pending_ids = _positive_int_tuple(
            active_state.get("pending_message_log_ids"),
            field_name="active_chat_state.pending_message_log_ids",
            allow_empty=True,
        )
        next_state: dict[str, Any] = {
            **active_state,
            "round_operation_id": "",
            "round_input_message_log_ids": [],
            "round_last_failure_event_id": event.event_id,
            "round_last_failure_code": failure_code,
            "updated_at": _event_time(aggregate, event),
        }
        effects: tuple[SessionEffect, ...] = ()
        if pending_ids:
            next_state, retry_effect, round_intent = (
                self._schedule_active_chat_round_due(
                aggregate,
                event,
                active_chat_state=next_state,
                input_watermark=_message_watermark(data),
                delay_seconds=self._config.busy_review_retry_seconds,
                )
            )
            data = _with_control_intent(
                data,
                effect_kind=AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
                intent=round_intent,
            )
            effects = (retry_effect,)
        target = aggregate.advance(
            active_chat_round_operation_id="",
            active_chat_state=next_state,
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition=(
                "active_chat_round_effect_failed_retry_scheduled"
                if effects
                else "active_chat_round_effect_failed"
            ),
            caused_operation_id=operation_id,
            caused_plan_id=aggregate.current_plan_id,
            effects=effects,
            operations=(operation,),
            result={
                "failure": failure,
                "remaining_message_log_ids": list(pending_ids),
                "round_schedule_id": _text(next_state.get("round_schedule_id")),
            },
            reason=failure_code,
        )

    def _external_action_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        effect_kind: str,
    ) -> SessionTransition:
        """Persist a terminal action-effect failure without inventing a receipt.

        A receipt can prove whether adapter I/O started; an exhausted outer
        effect cannot. Keeping the receipt status intact is therefore essential:
        ownership migration may later reconcile a proven pre-dispatch receipt,
        while an ``unknown`` or executing receipt must remain visible to an
        operator. The actor merely records the exact terminal effect evidence
        and keeps every later model transition behind the existing outbound
        gate.
        """

        pending = _pending_outbound_actions(aggregate.data)
        effect_id = _text(event.payload.get("effect_id"))
        expected = _mapping(pending.get(effect_id))
        mismatch = _external_action_effect_failure_mismatch(
            aggregate,
            event,
            expected=expected,
            effect_kind=effect_kind,
        )
        if mismatch:
            return self._stale_workflow_completion(
                aggregate,
                event,
                operation_id=_text(event.payload.get("operation_id")),
                disposition="external_action_effect_failure_stale",
                mismatch=mismatch,
            )

        failure = _external_action_effect_failure_record(event)
        updated_pending = dict(pending)
        updated = dict(expected)
        updated["status"] = "effect_failed"
        updated["effect_failure"] = failure
        updated_pending[effect_id] = updated
        data = dict(aggregate.data)
        data[_PENDING_OUTBOUND_DATA_KEY] = updated_pending
        data["outbound_blocked_reason"] = "effect_failed"
        data[_OUTBOUND_BLOCKED_DATA_KEY] = {
            "effect_id": effect_id,
            "failure_code": failure["failure_code"],
            "failure_event_id": failure["event_id"],
            "kind": "effect_failed",
            "operation_id": failure["operation_id"],
        }
        target = aggregate.advance(
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="external_action_effect_failed_blocked",
            caused_operation_id=_text(expected.get("operation_id")),
            caused_plan_id=aggregate.current_plan_id,
            result={
                "effect_id": effect_id,
                "failure": failure,
            },
            reason="external_action_effect_failed",
        )

    def _planning_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        effect_kind: str,
    ) -> SessionTransition:
        pending = _mapping(aggregate.data.get("idle_exit"))
        mismatch = self._planning_effect_failure_mismatch(
            aggregate,
            event,
            pending=pending,
            effect_kind=effect_kind,
        )
        if mismatch:
            return self._stale_completion(aggregate, event, mismatch)

        normalized_payload = dict(event.payload)
        normalized_payload.update(
            {
                "operation_id": _text(pending.get("operation_id")),
                "plan_id": _text(pending.get("plan_id")),
                "active_epoch": _optional_nonnegative_int(
                    pending.get("active_epoch")
                ),
                "activity_generation": _optional_nonnegative_int(
                    pending.get("activity_generation")
                ),
            }
        )
        normalized = replace(event, payload=normalized_payload)
        deadline_effect_failed = (
            effect_kind
            == AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE
        )
        failure_code = _text(event.payload.get("failure_code")) or (
            "planner_deadline_effect_failed"
            if deadline_effect_failed
            else "idle_review_planning_effect_failed"
        )
        outcome = Failed(
            applied_delay_seconds=self._config.default_review_delay_seconds,
            reason=(
                "idle_review_planning_deadline_effect_failed"
                if deadline_effect_failed
                else "idle_review_planning_effect_failed"
            ),
            fallback_reason=(
                "planner_deadline_effect_failed"
                if deadline_effect_failed
                else "idle_review_planning_failed"
            ),
            failure_code=failure_code,
            failure_message=_text(event.payload.get("failure_message")),
            source=event.source or "effect_executor",
        )
        return self._settle_exit(aggregate, normalized, outcome)

    def _planning_effect_failure_mismatch(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        pending: Mapping[str, Any],
        effect_kind: str,
    ) -> tuple[str, ...]:
        planner_failed = (
            effect_kind == AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING
        )
        effect_prefix = "planner" if planner_failed else "deadline"
        expected_effect_id = _text(pending.get(f"{effect_prefix}_effect_id"))
        expected_idempotency_key = _text(
            pending.get(f"{effect_prefix}_idempotency_key")
        )
        expected_operation_id = _text(pending.get("operation_id"))
        operation_fence = _mapping(
            _mapping(aggregate.data.get("operation_fences")).get(
                expected_operation_id
            )
        )
        expected_plan_id = _text(pending.get("plan_id"))
        expected_active_epoch = _optional_nonnegative_int(
            pending.get("active_epoch")
        )
        expected_activity_generation = _optional_nonnegative_int(
            pending.get("activity_generation")
        )
        mismatch = list(
            _effect_event_provenance_mismatch(
                aggregate,
                event,
                expected_event_id=_text(
                    pending.get(f"{effect_prefix}_failure_event_id")
                ),
                expected_effect_kind=effect_kind,
                expected_effect_id=expected_effect_id,
                expected_idempotency_key=expected_idempotency_key,
                expected_operation_id=expected_operation_id,
                expected_plan_id=expected_plan_id,
                expected_active_epoch=expected_active_epoch,
                expected_activity_generation=expected_activity_generation,
                expected_input_watermark=_optional_nonnegative_int(
                    operation_fence.get("input_watermark")
                ),
                expected_input_ledger_sequence=_optional_nonnegative_int(
                    operation_fence.get("input_ledger_sequence")
                ),
                expected_causation_id=_text(
                    pending.get("requested_by_event_id")
                ),
                expected_ownership_generation=_optional_nonnegative_int(
                    pending.get("ownership_generation")
                ),
            )
        )
        if aggregate.state != AgentSessionState.ACTIVE_CHAT_SETTLING:
            mismatch.append("state_changed")
        if not operation_fence:
            mismatch.append("operation_fence_changed")
        operation_id = _text(event.payload.get("operation_id"))
        if operation_id != aggregate.idle_planning_operation_id:
            mismatch.append("operation_id_changed")
        if expected_active_epoch != aggregate.active_epoch:
            mismatch.append("active_epoch_changed")
        if expected_activity_generation != aggregate.activity_generation:
            mismatch.append("activity_generation_changed")
        return tuple(dict.fromkeys(mismatch))

    def _control_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        effect_kind: str,
    ) -> SessionTransition:
        intent = _control_intent(aggregate.data, effect_kind)
        mismatch = self._control_effect_failure_mismatch(
            aggregate,
            event,
            intent=intent,
            effect_kind=effect_kind,
        )
        if mismatch:
            return self._ignored(
                aggregate,
                event,
                disposition="ignored_unrelated_control_effect_failure",
                reason=",".join(mismatch),
            )

        failure = _effect_failure_record(event)
        return self._start_control_reconciliation(
            aggregate,
            event,
            control_effect_kind=effect_kind,
            intent={**intent, "last_failure": failure},
            cycle=1,
            prior_operations=(),
            failure=failure,
        )

    def _control_completed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        effect_kind = (
            AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
            if event.kind == AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_STOPPED
            else AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
        )
        intent = _control_intent(aggregate.data, effect_kind)
        mismatch = self._control_effect_event_mismatch(
            aggregate,
            event,
            intent=intent,
            effect_kind=effect_kind,
            expected_event_field="completion_event_id",
        )
        if mismatch:
            return self._ignored(
                aggregate,
                event,
                disposition="ignored_unrelated_control_effect_completion",
                reason=",".join(mismatch),
            )
        completion = _effect_completion_record(event)
        target = aggregate.advance(
            data=_with_control_intent(
                aggregate.data,
                effect_kind=effect_kind,
                intent={
                    **intent,
                    "status": "completed",
                    "completion": completion,
                },
            ),
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition=(
                "active_chat_runtime_stopped"
                if effect_kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
                else "idle_planning_cancellation_completed"
            ),
            caused_operation_id=_text(intent.get("operation_id")),
            caused_plan_id=_text(intent.get("plan_id")),
            result={"completion": completion},
            reason=f"control_effect_completed:{effect_kind}",
        )

    def _control_reconciled(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        control_effect_kind = (
            AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
            if event.kind == AgentSessionEventKind.ACTIVE_CHAT_RUNTIME_RECONCILED
            else AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
        )
        reconciliation_kind = _reconciliation_effect_kind(control_effect_kind)
        intent = _control_intent(aggregate.data, control_effect_kind)
        mismatch = self._reconciliation_event_mismatch(
            aggregate,
            event,
            intent=intent,
            reconciliation_kind=reconciliation_kind,
            expected_event_field="reconciliation_completion_event_id",
        )
        if mismatch:
            return self._ignored(
                aggregate,
                event,
                disposition="ignored_unrelated_control_reconciliation",
                reason=",".join(mismatch),
            )
        completion = _effect_completion_record(event)
        target = aggregate.advance(
            data=_with_control_intent(
                aggregate.data,
                effect_kind=control_effect_kind,
                intent={
                    **intent,
                    "status": "reconciled",
                    "reconciliation_completion": completion,
                },
            ),
            updated_at=_event_time(aggregate, event),
        )
        operation_id = _text(intent.get("reconciliation_operation_id"))
        operation = SessionOperation(
            operation_id=operation_id,
            kind=reconciliation_kind,
            status=SessionOperationStatus.COMPLETED,
            finished_at=event.occurred_at,
            metadata={"completion": completion},
        )
        return SessionTransition(
            aggregate=target,
            disposition=(
                "active_chat_runtime_reconciled"
                if control_effect_kind
                == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
                else "idle_planning_cancellation_reconciled"
            ),
            caused_operation_id=operation_id,
            caused_plan_id=_text(intent.get("plan_id")),
            operations=(operation,),
            result={"completion": completion},
            reason=f"control_reconciled:{control_effect_kind}",
        )

    def _reconciliation_effect_failed(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        effect_kind: str,
    ) -> SessionTransition:
        control_effect_kind = _control_effect_kind_for_reconciliation(effect_kind)
        intent = _control_intent(aggregate.data, control_effect_kind)
        mismatch = self._reconciliation_event_mismatch(
            aggregate,
            event,
            intent=intent,
            reconciliation_kind=effect_kind,
            expected_event_field="reconciliation_failure_event_id",
        )
        if mismatch:
            return self._ignored(
                aggregate,
                event,
                disposition="ignored_unrelated_reconciliation_failure",
                reason=",".join(mismatch),
            )

        failure = _effect_failure_record(event)
        cycle = _optional_nonnegative_int(intent.get("reconciliation_cycle")) or 1
        failed_operation = SessionOperation(
            operation_id=_text(intent.get("reconciliation_operation_id")),
            kind=effect_kind,
            status=SessionOperationStatus.FAILED,
            finished_at=event.occurred_at,
            failure_code=_text(event.payload.get("failure_code")),
            failure_message=_text(event.payload.get("failure_message")),
            metadata={"failure": failure, "reconciliation_cycle": cycle},
        )
        failures = list(intent.get("reconciliation_failures") or ())
        failures.append(failure)
        updated_intent = {
            **intent,
            "last_reconciliation_failure": failure,
            "reconciliation_failures": failures,
        }
        if cycle < self._config.control_reconciliation_max_cycles:
            return self._start_control_reconciliation(
                aggregate,
                event,
                control_effect_kind=control_effect_kind,
                intent=updated_intent,
                cycle=cycle + 1,
                prior_operations=(failed_operation,),
                failure=failure,
            )

        updated_intent["status"] = "reconciliation_failed"
        target = aggregate.advance(
            data=_with_control_intent(
                aggregate.data,
                effect_kind=control_effect_kind,
                intent=updated_intent,
            ),
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="control_reconciliation_exhausted",
            caused_operation_id=failed_operation.operation_id,
            caused_plan_id=_text(intent.get("plan_id")),
            operations=(failed_operation,),
            result={
                "failure": failure,
                "reconciliation_cycle": cycle,
                "reconciliation_exhausted": True,
            },
            reason=f"control_reconciliation_failed:{control_effect_kind}",
        )

    def _start_control_reconciliation(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        control_effect_kind: str,
        intent: Mapping[str, Any],
        cycle: int,
        prior_operations: tuple[SessionOperation, ...],
        failure: Mapping[str, Any],
    ) -> SessionTransition:
        reconciliation_kind = _reconciliation_effect_kind(control_effect_kind)
        seed = f"{_text(intent.get('effect_id'))}:{cycle}"
        operation_id = self._ids.create(
            key=event.key,
            seed=seed,
            purpose=f"{reconciliation_kind}-operation",
        )
        effect_id = self._ids.create(
            key=event.key,
            seed=seed,
            purpose=f"{reconciliation_kind}-effect",
        )
        completion_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose=f"{reconciliation_kind}-completion-event",
        )
        failure_event_id = self._ids.create(
            key=event.key,
            seed=effect_id,
            purpose=f"{reconciliation_kind}-failure-event",
        )
        desired_state = _text(intent.get("desired_state"))
        updated_intent = {
            **intent,
            "status": "reconciliation_requested",
            "reconciliation_cycle": cycle,
            "reconciliation_kind": reconciliation_kind,
            "reconciliation_operation_id": operation_id,
            "reconciliation_effect_id": effect_id,
            "reconciliation_idempotency_key": effect_id,
            "reconciliation_completion_event_id": completion_event_id,
            "reconciliation_failure_event_id": failure_event_id,
            "reconciliation_causation_id": event.event_id,
        }
        target = aggregate.advance(
            data=_with_control_intent(
                aggregate.data,
                effect_kind=control_effect_kind,
                intent=updated_intent,
            ),
            updated_at=_event_time(aggregate, event),
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind=reconciliation_kind,
            status=SessionOperationStatus.PENDING,
            launched_by_event_id=event.event_id,
            state_revision=target.state_revision,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            started_at=event.occurred_at,
            metadata={
                "desired_state": desired_state,
                "failed_effect": dict(failure),
                "control_effect_kind": _text(control_effect_kind),
                "reconciliation_cycle": cycle,
            },
        )
        effect = _durable_effect(
            effect_id=effect_id,
            kind=reconciliation_kind,
            idempotency_key=effect_id,
            operation_id=operation_id,
            payload={
                "completion_event_id": completion_event_id,
                "failure_event_id": failure_event_id,
                "plan_id": _text(intent.get("plan_id")),
                "active_epoch": _optional_nonnegative_int(
                    intent.get("active_epoch")
                ),
                "activity_generation": _optional_nonnegative_int(
                    intent.get("activity_generation")
                ),
                "input_watermark": _optional_nonnegative_int(
                    intent.get("input_watermark")
                ),
                "input_ledger_sequence": None,
                "desired_state": desired_state,
                "control_effect_kind": _text(control_effect_kind),
                "control_effect_id": _text(intent.get("effect_id")),
                "reconciliation_cycle": cycle,
            },
        )
        disposition = (
            "active_chat_runtime_reconciliation_required"
            if control_effect_kind == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
            else "idle_planning_cancellation_reconciliation_required"
        )
        return SessionTransition(
            aggregate=target,
            disposition=disposition,
            caused_operation_id=operation_id,
            caused_plan_id=_text(intent.get("plan_id")),
            effects=(effect,),
            operations=(*prior_operations, operation),
            result={
                "desired_state": desired_state,
                "failed_effect": dict(failure),
                "reconciliation_operation_id": operation_id,
                "reconciliation_effect_id": effect_id,
                "reconciliation_cycle": cycle,
            },
            reason=f"control_effect_failed:{control_effect_kind}",
        )

    def _control_effect_failure_mismatch(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        intent: Mapping[str, Any],
        effect_kind: str,
    ) -> tuple[str, ...]:
        return self._control_effect_event_mismatch(
            aggregate,
            event,
            intent=intent,
            effect_kind=effect_kind,
            expected_event_field="failure_event_id",
        )

    @staticmethod
    def _control_effect_event_mismatch(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        intent: Mapping[str, Any],
        effect_kind: str,
        expected_event_field: str,
    ) -> tuple[str, ...]:
        if not intent:
            return ("control_intent_changed",)
        if _text(intent.get("status")) != "requested":
            return ("control_intent_status_changed",)
        mismatch = list(
            _effect_event_provenance_mismatch(
                aggregate,
                event,
                expected_event_id=_text(intent.get(expected_event_field)),
                expected_effect_kind=effect_kind,
                expected_effect_id=_text(intent.get("effect_id")),
                expected_idempotency_key=_text(intent.get("idempotency_key")),
                expected_operation_id=_text(intent.get("operation_id")),
                expected_plan_id=_text(intent.get("plan_id")),
                expected_active_epoch=_optional_nonnegative_int(
                    intent.get("active_epoch")
                ),
                expected_activity_generation=_optional_nonnegative_int(
                    intent.get("activity_generation")
                ),
                expected_input_watermark=_optional_nonnegative_int(
                    intent.get("input_watermark")
                ),
                expected_input_ledger_sequence=_optional_nonnegative_int(
                    intent.get("input_ledger_sequence")
                ),
                expected_causation_id=_text(intent.get("causation_id")),
                expected_ownership_generation=_optional_nonnegative_int(
                    intent.get("ownership_generation")
                ),
                expected_contract_version=_optional_nonnegative_int(
                    intent.get("contract_version")
                ),
                expected_contract_signature=_text(intent.get("contract_signature")),
            )
        )
        mismatch.extend(_control_runtime_fence_mismatch(aggregate, intent))
        return tuple(dict.fromkeys(mismatch))

    @staticmethod
    def _reconciliation_event_mismatch(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        intent: Mapping[str, Any],
        reconciliation_kind: str,
        expected_event_field: str,
    ) -> tuple[str, ...]:
        if not intent:
            return ("control_intent_changed",)
        if _text(intent.get("status")) != "reconciliation_requested":
            return ("control_intent_status_changed",)
        mismatch = list(
            _effect_event_provenance_mismatch(
                aggregate,
                event,
                expected_event_id=_text(intent.get(expected_event_field)),
                expected_effect_kind=reconciliation_kind,
                expected_effect_id=_text(intent.get("reconciliation_effect_id")),
                expected_idempotency_key=_text(
                    intent.get("reconciliation_idempotency_key")
                ),
                expected_operation_id=_text(
                    intent.get("reconciliation_operation_id")
                ),
                expected_plan_id=_text(intent.get("plan_id")),
                expected_active_epoch=_optional_nonnegative_int(
                    intent.get("active_epoch")
                ),
                expected_activity_generation=_optional_nonnegative_int(
                    intent.get("activity_generation")
                ),
                expected_input_watermark=_optional_nonnegative_int(
                    intent.get("input_watermark")
                ),
                expected_input_ledger_sequence=None,
                expected_causation_id=_text(
                    intent.get("reconciliation_causation_id")
                ),
                expected_ownership_generation=_optional_nonnegative_int(
                    intent.get("ownership_generation")
                ),
            )
        )
        mismatch.extend(_control_runtime_fence_mismatch(aggregate, intent))
        return tuple(dict.fromkeys(mismatch))

    def _settle_exit(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        outcome: SettledScheduleOutcome,
    ) -> SessionTransition:
        operation_id = _text(event.payload.get("operation_id"))
        plan_id = self._pending_plan_id(aggregate, operation_id)
        pending = _mapping(aggregate.data.get("idle_exit"))
        trigger = _text(pending.get("trigger")) or "active_chat_exit"
        plan_revision = aggregate.review_plan_revision + 1
        outcome_payload = outcome.to_payload()
        review_plan = {
            "plan_id": plan_id,
            "plan_revision": plan_revision,
            "trigger": trigger,
            **outcome_payload,
            "expected_active_epoch": aggregate.active_epoch,
            "expected_activity_generation": aggregate.activity_generation,
        }
        stop_effect_id = self._payload_or_generated_id(
            event,
            field_name="stop_effect_id",
            seed=operation_id,
            purpose="stop-active-chat-effect",
        )
        stop_idempotency_key = (
            _text(event.payload.get("stop_idempotency_key")) or stop_effect_id
        )
        stop_completion_event_id = self._payload_or_generated_id(
            event,
            field_name="stop_completion_event_id",
            seed=stop_effect_id,
            purpose="stop-active-chat-completion-event",
        )
        stop_failure_event_id = self._payload_or_generated_id(
            event,
            field_name="stop_failure_event_id",
            seed=stop_effect_id,
            purpose="stop-active-chat-failure-event",
        )
        operation_fence = _operation_fence(aggregate.data, operation_id)
        input_watermark = _optional_nonnegative_int(
            operation_fence.get("input_watermark")
        )
        input_ledger_sequence = _optional_nonnegative_int(
            operation_fence.get("input_ledger_sequence")
        )
        data = _with_control_intent(
            _without_operation_fence(
                _without_idle_exit(aggregate.data),
                operation_id,
            ),
            effect_kind=AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
            intent={
                "desired_state": "stopped",
                "status": "requested",
                "effect_id": stop_effect_id,
                "effect_kind": AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
                "idempotency_key": stop_idempotency_key,
                "completion_event_id": stop_completion_event_id,
                "failure_event_id": stop_failure_event_id,
                "operation_id": operation_id,
                "plan_id": plan_id,
                "active_epoch": aggregate.active_epoch,
                "activity_generation": aggregate.activity_generation,
                "input_watermark": input_watermark,
                "input_ledger_sequence": input_ledger_sequence,
                "ownership_generation": aggregate.ownership_generation,
                "causation_id": event.event_id,
                "expected_state": AgentSessionState.IDLE.value,
                "expected_active_epoch": aggregate.active_epoch,
                "expected_activity_generation": aggregate.activity_generation,
                "expected_current_plan_id": plan_id,
            },
        )
        target = aggregate.advance(
            state=AgentSessionState.IDLE.value,
            current_plan_id=plan_id,
            review_plan_revision=plan_revision,
            review_plan=review_plan,
            active_reply_resume={},
            active_chat_state={},
            active_chat_round_operation_id="",
            idle_planning_operation_id="",
            data=data,
            updated_at=_event_time(aggregate, event),
        )
        operation_status = (
            SessionOperationStatus.FAILED
            if isinstance(outcome, Failed)
            else SessionOperationStatus.COMPLETED
        )
        operation = SessionOperation(
            operation_id=operation_id,
            kind="idle_review_planning",
            status=operation_status,
            state_revision=aggregate.state_revision,
            active_epoch=aggregate.active_epoch,
            activity_generation=aggregate.activity_generation,
            finished_at=event.occurred_at,
            failure_code=(outcome.failure_code if isinstance(outcome, Failed) else ""),
            failure_message=(
                outcome.failure_message if isinstance(outcome, Failed) else ""
            ),
            metadata={
                "idle_exit": pending,
                "schedule_outcome": outcome_payload,
                "deadline_effect_contract": "skip_when_operation_terminal",
            },
        )
        schedule = SessionReviewSchedule(
            plan_id=plan_id,
            plan_revision=plan_revision,
            applied_delay_seconds=outcome.applied_delay_seconds,
            status=ReviewScheduleStatus.SCHEDULED,
            trigger=trigger,
            outcome=outcome.kind.value,
            source=outcome.source,
            requested_delay_seconds=outcome.requested_delay_seconds,
            reason=outcome.reason,
            fallback_reason=outcome.fallback_reason,
            mention_sensitivity=outcome.mention_sensitivity,
            active_reply_threshold=outcome.active_reply_threshold,
            model_execution_id=outcome.model_execution_id,
            prompt_signature=outcome.prompt_signature,
            expected_active_epoch=aggregate.active_epoch,
            expected_activity_generation=aggregate.activity_generation,
            committed_state_revision=target.state_revision,
        )
        schedule_event = SessionReviewScheduleEvent(
            schedule_event_id=self._payload_or_generated_id(
                event,
                field_name="schedule_event_id",
                seed=event.event_id,
                purpose="idle-review-schedule-event",
            ),
            event_type="scheduled",
            plan_id=plan_id,
            previous_plan_id=aggregate.current_plan_id,
            trigger=trigger,
            outcome=outcome.kind.value,
            source=outcome.source,
            requested_delay_seconds=outcome.requested_delay_seconds,
            applied_delay_seconds=outcome.applied_delay_seconds,
            reason=outcome.reason,
            fallback_reason=outcome.fallback_reason,
            model_execution_id=outcome.model_execution_id,
            prompt_signature=outcome.prompt_signature,
            expected_active_epoch=aggregate.active_epoch,
            expected_activity_generation=aggregate.activity_generation,
            committed_state_revision=target.state_revision,
            operation_id=operation_id,
            trace_id=event.trace_id,
            metadata={"schedule_outcome": outcome_payload},
        )
        stop_effect = _durable_effect(
            effect_id=stop_effect_id,
            kind=AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME,
            idempotency_key=stop_idempotency_key,
            operation_id=operation_id,
            payload={
                "operation_id": operation_id,
                "plan_id": plan_id,
                "outcome": outcome.kind.value,
                "reason": outcome.reason,
                "active_epoch": aggregate.active_epoch,
                "activity_generation": aggregate.activity_generation,
                "input_watermark": input_watermark,
                "input_ledger_sequence": input_ledger_sequence,
                "completion_event_id": stop_completion_event_id,
                "failure_event_id": stop_failure_event_id,
            },
        )
        return SessionTransition(
            aggregate=target,
            disposition="active_chat_exit_committed",
            caused_operation_id=operation_id,
            caused_plan_id=plan_id,
            effects=(stop_effect,),
            operations=(operation,),
            review_schedules=(schedule,),
            review_schedule_events=(schedule_event,),
            result={
                "operation_id": operation_id,
                "plan_id": plan_id,
                "schedule_outcome": outcome_payload,
                "deadline_effect_contract": "skip_when_operation_terminal",
            },
            reason=outcome.reason,
        )

    def _stale_completion(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        mismatch: tuple[str, ...],
    ) -> SessionTransition:
        operation_id = _text(event.payload.get("operation_id"))
        plan_id = _text(event.payload.get("plan_id"))
        if not plan_id:
            seed = operation_id or event.event_id
            plan_id = self._ids.create(
                key=event.key,
                seed=seed,
                purpose="idle-review-plan",
            )
        outcome = Superseded(
            reason=",".join(mismatch),
            operation_id=operation_id,
            plan_id=plan_id,
            expected_active_epoch=_optional_nonnegative_int(
                event.payload.get("active_epoch")
            ),
            expected_activity_generation=_optional_nonnegative_int(
                event.payload.get("activity_generation")
            ),
            actual_active_epoch=aggregate.active_epoch,
            actual_activity_generation=aggregate.activity_generation,
            actual_state=aggregate.state,
            active_operation_id=aggregate.idle_planning_operation_id,
        )
        target = aggregate.advance(
            state_changed=False,
            updated_at=_event_time(aggregate, event),
        )
        return SessionTransition(
            aggregate=target,
            disposition="superseded",
            caused_operation_id=operation_id,
            caused_plan_id=plan_id,
            review_schedule_events=(
                self._superseded_schedule_event(aggregate, event, outcome),
            ),
            result={"schedule_outcome": outcome.to_payload()},
            reason=outcome.reason,
        )

    def _superseded_schedule_event(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        outcome: Superseded,
    ) -> SessionReviewScheduleEvent:
        return SessionReviewScheduleEvent(
            schedule_event_id=self._payload_or_generated_id(
                event,
                field_name="schedule_event_id",
                seed=event.event_id,
                purpose="idle-review-superseded-event",
            ),
            event_type="superseded",
            plan_id=outcome.plan_id,
            previous_plan_id=aggregate.current_plan_id,
            trigger="active_chat_exit",
            outcome=outcome.kind.value,
            source=event.source or "session_actor",
            reason=outcome.reason,
            expected_active_epoch=outcome.expected_active_epoch,
            expected_activity_generation=outcome.expected_activity_generation,
            committed_state_revision=aggregate.state_revision,
            operation_id=outcome.operation_id,
            trace_id=event.trace_id,
            metadata={"schedule_outcome": outcome.to_payload()},
        )

    def _fence_mismatch(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> tuple[str, ...]:
        pending = _mapping(aggregate.data.get("idle_exit"))
        completion = (
            event.kind == AgentSessionEventKind.IDLE_REVIEW_PLANNING_COMPLETED
        )
        effect_prefix = "planner" if completion else "deadline"
        expected_event_id = _text(
            pending.get("completion_event_id" if completion else "deadline_event_id")
        )
        expected_operation_id = _text(pending.get("operation_id"))
        operation_fence = _mapping(
            _mapping(aggregate.data.get("operation_fences")).get(
                expected_operation_id
            )
        )
        expected_plan_id = _text(pending.get("plan_id"))
        expected_active_epoch = _optional_nonnegative_int(
            pending.get("active_epoch")
        )
        expected_activity_generation = _optional_nonnegative_int(
            pending.get("activity_generation")
        )
        expected_effect_id = _text(pending.get(f"{effect_prefix}_effect_id"))
        expected_idempotency_key = _text(
            pending.get(f"{effect_prefix}_idempotency_key")
        )
        expected_effect_kind = (
            AgentSessionEffectKind.RUN_IDLE_REVIEW_PLANNING
            if completion
            else AgentSessionEffectKind.ENQUEUE_IDLE_REVIEW_PLANNING_DEADLINE
        )
        mismatch = list(
            _effect_event_provenance_mismatch(
                aggregate,
                event,
                expected_event_id=expected_event_id,
                expected_effect_kind=expected_effect_kind,
                expected_effect_id=expected_effect_id,
                expected_idempotency_key=expected_idempotency_key,
                expected_operation_id=expected_operation_id,
                expected_plan_id=expected_plan_id,
                expected_active_epoch=expected_active_epoch,
                expected_activity_generation=expected_activity_generation,
                expected_input_watermark=_optional_nonnegative_int(
                    operation_fence.get("input_watermark")
                ),
                expected_input_ledger_sequence=_optional_nonnegative_int(
                    operation_fence.get("input_ledger_sequence")
                ),
                expected_causation_id=_text(
                    pending.get("requested_by_event_id")
                ),
                expected_ownership_generation=_optional_nonnegative_int(
                    pending.get("ownership_generation")
                ),
            )
        )
        if aggregate.state != AgentSessionState.ACTIVE_CHAT_SETTLING:
            mismatch.append("state_changed")
        if not operation_fence:
            mismatch.append("operation_fence_changed")
        if expected_operation_id != aggregate.idle_planning_operation_id:
            mismatch.append("operation_id_changed")
        if expected_active_epoch != aggregate.active_epoch:
            mismatch.append("active_epoch_changed")
        if expected_activity_generation != aggregate.activity_generation:
            mismatch.append("activity_generation_changed")
        return tuple(dict.fromkeys(mismatch))

    def _pending_plan_id(
        self,
        aggregate: AgentSessionAggregate,
        operation_id: str,
    ) -> str:
        pending = _mapping(aggregate.data.get("idle_exit"))
        plan_id = _text(pending.get("plan_id"))
        pending_operation_id = _text(pending.get("operation_id"))
        if plan_id and (
            not pending_operation_id or pending_operation_id == operation_id
        ):
            return plan_id
        return self._ids.create(
            key=aggregate.key,
            seed=operation_id or "missing-idle-planning-operation",
            purpose="idle-review-plan",
        )

    def _payload_or_generated_id(
        self,
        event: SessionEventEnvelope,
        *,
        field_name: str,
        seed: str,
        purpose: str,
    ) -> str:
        supplied = _text(event.payload.get(field_name))
        if supplied:
            return supplied
        return self._ids.create(key=event.key, seed=seed, purpose=purpose)

    def _invalid_outcome(
        self,
        event: SessionEventEnvelope,
        *,
        failure_message: str,
    ) -> Failed:
        return Failed(
            applied_delay_seconds=self._config.default_review_delay_seconds,
            reason="invalid_idle_review_planning_outcome",
            fallback_reason="invalid_planner_outcome",
            failure_code="invalid_planner_outcome",
            failure_message=failure_message,
            model_execution_id=_text(event.payload.get("model_execution_id")),
            prompt_signature=_text(event.payload.get("prompt_signature")),
            source=(
                _text(event.payload.get("source"))
                or event.source
                or "idle_review_planning"
            ),
        )

    def _ignored(
        self,
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
        *,
        disposition: str,
        reason: str,
    ) -> SessionTransition:
        operation_id = (
            _text(event.payload.get("operation_id"))
            or aggregate.idle_planning_operation_id
        )
        plan_id = _text(event.payload.get("plan_id"))
        if not plan_id and operation_id:
            plan_id = self._pending_plan_id(aggregate, operation_id)
        return SessionTransition(
            aggregate=aggregate.advance(
                state_changed=False,
                updated_at=_event_time(aggregate, event),
            ),
            disposition=disposition,
            caused_operation_id=operation_id,
            caused_plan_id=plan_id,
            result={"event_kind": event.kind},
            reason=reason,
        )


def _reconciliation_effect_kind(control_effect_kind: str) -> AgentSessionEffectKind:
    normalized = _text(control_effect_kind)
    if normalized == AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME:
        return AgentSessionEffectKind.ACTIVE_CHAT_RUNTIME_RECONCILIATION
    if normalized == AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING:
        return (
            AgentSessionEffectKind.IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILIATION
        )
    raise ValueError(f"unsupported control effect kind: {normalized or '<empty>'}")


def _active_chat_exit_request_mismatch(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
) -> tuple[str, ...]:
    """Fence control-lane exit requests against later active-chat activity."""

    actor_generated = event.source == "effect_executor" or any(
        field in event.payload
        for field in ("expected_active_epoch", "expected_message_watermark")
    )
    if not actor_generated:
        return ()
    mismatch: list[str] = []
    contract = builtin_effect_contract(
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
        version=1,
    )
    if event.source != contract.completion_source:
        mismatch.append("source_changed")
    required_text = {
        "completion_event_id": event.event_id,
        "effect_kind": contract.effect_kind,
        "contract_signature": contract.signature,
    }
    for field_name, expected in required_text.items():
        if _text(event.payload.get(field_name)) != expected:
            mismatch.append(f"{field_name}_changed")
    if _optional_nonnegative_int(event.payload.get("contract_version")) != contract.version:
        mismatch.append("contract_version_changed")
    attempt_count = _optional_nonnegative_int(event.payload.get("attempt_count"))
    if attempt_count is None or attempt_count < 1:
        mismatch.append("attempt_count_invalid")
    expected_epoch = _optional_nonnegative_int(
        event.payload.get("expected_active_epoch")
    )
    if expected_epoch is None or expected_epoch != aggregate.active_epoch:
        mismatch.append("active_epoch_changed")
    expected_watermark = _optional_nonnegative_int(
        event.payload.get("expected_message_watermark")
    )
    if expected_watermark is None or expected_watermark != _message_watermark(
        aggregate.data
    ):
        mismatch.append("message_watermark_changed")
    return tuple(dict.fromkeys(mismatch))


def _active_chat_round_due_mismatch(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
    active_state: Mapping[str, Any],
) -> tuple[str, ...]:
    """Validate a debounced round timer without relying on wall-clock order."""

    mismatch: list[str] = []
    if aggregate.state != AgentSessionState.ACTIVE_CHAT:
        mismatch.append("state_changed")
    if _text(active_state.get("bootstrap_status")) != "completed":
        mismatch.append("bootstrap_not_completed")
    if aggregate.active_chat_round_operation_id:
        mismatch.append("round_already_running")
    if _has_unsettled_pending_outbound(aggregate.data):
        mismatch.append("outbound_actions_pending")
    expected_event_id = _text(active_state.get("round_due_event_id"))
    if not expected_event_id or event.event_id != expected_event_id:
        mismatch.append("event_id_changed")
    contract = builtin_effect_contract(
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_ROUND_DUE,
        version=1,
    )
    if event.source != contract.completion_source:
        mismatch.append("source_changed")
    expected_fields = {
        "effect_id": _text(active_state.get("round_schedule_effect_id")),
        "effect_kind": contract.effect_kind,
        "idempotency_key": _text(active_state.get("round_schedule_effect_id")),
        "contract_signature": contract.signature,
        "schedule_id": _text(active_state.get("round_schedule_id")),
    }
    for field_name, expected in expected_fields.items():
        if not expected or _text(event.payload.get(field_name)) != expected:
            mismatch.append(f"{field_name}_changed")
    expected_revision = _optional_nonnegative_int(
        active_state.get("round_schedule_revision")
    )
    if _optional_nonnegative_int(event.payload.get("schedule_revision")) != expected_revision:
        mismatch.append("schedule_revision_changed")
    if _optional_nonnegative_int(event.payload.get("active_epoch")) != aggregate.active_epoch:
        mismatch.append("active_epoch_changed")
    expected_watermark = _optional_nonnegative_int(
        active_state.get("round_schedule_input_watermark")
    )
    if _optional_nonnegative_int(event.payload.get("input_watermark")) != expected_watermark:
        mismatch.append("input_watermark_changed")
    if _optional_nonnegative_int(event.payload.get("contract_version")) != contract.version:
        mismatch.append("contract_version_changed")
    attempt_count = _optional_nonnegative_int(event.payload.get("attempt_count"))
    if attempt_count is None or attempt_count < 1:
        mismatch.append("attempt_count_invalid")
    expected_causation = _text(active_state.get("round_schedule_source_event_id"))
    if not expected_causation or event.causation_id != expected_causation:
        mismatch.append("causation_id_changed")
    if event.ownership_generation != aggregate.ownership_generation:
        mismatch.append("ownership_generation_changed")
    return tuple(dict.fromkeys(mismatch))


def _active_chat_tick_mismatch(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
    active_state: Mapping[str, Any],
) -> tuple[str, ...]:
    """Reject timer ticks from another epoch or an obsolete message snapshot."""

    mismatch: list[str] = []
    if aggregate.state != AgentSessionState.ACTIVE_CHAT:
        mismatch.append("state_changed")
    if _optional_nonnegative_int(event.payload.get("active_epoch")) != aggregate.active_epoch:
        mismatch.append("active_epoch_changed")
    expected_watermark = _optional_nonnegative_int(
        event.payload.get("expected_message_watermark")
    )
    if expected_watermark is None or expected_watermark != _message_watermark(
        aggregate.data
    ):
        mismatch.append("message_watermark_changed")
    if _optional_nonnegative_int(event.payload.get("ownership_generation")) not in {
        None,
        aggregate.ownership_generation,
    }:
        mismatch.append("ownership_generation_changed")
    if _optional_nonnegative_int(active_state.get("active_epoch")) != aggregate.active_epoch:
        mismatch.append("active_state_epoch_changed")
    return tuple(dict.fromkeys(mismatch))


def _active_chat_interest_after_message(
    active_state: Mapping[str, Any],
    *,
    append: AppendMessageLedgerEntry,
    now: float,
    config: IdleExitReducerConfig,
) -> float:
    """Apply deterministic decay and priority deltas without scheduler state."""

    current = _finite_nonnegative_float(
        active_state.get("interest_value"),
        field_name="active_chat_state.interest_value",
        default=config.provisional_active_chat_interest,
    )
    half_life = _finite_positive_float(
        active_state.get("decay_half_life_seconds"),
        field_name="active_chat_state.decay_half_life_seconds",
        default=config.provisional_active_chat_half_life_seconds,
    )
    updated_at = _finite_nonnegative_float(
        active_state.get("updated_at"),
        field_name="active_chat_state.updated_at",
        default=now,
    )
    decayed = current * (0.5 ** (max(0.0, now - updated_at) / half_life))
    delta = 1.0
    if append.priority.mention:
        delta += 8.0
    if append.priority.reply_to_bot:
        delta += 5.0
    if append.priority.poke_to_bot:
        delta += 5.0
    return min(config.active_chat_max_interest, max(0.0, decayed + delta))


def _control_effect_kind_for_reconciliation(
    reconciliation_kind: str,
) -> AgentSessionEffectKind:
    normalized = _text(reconciliation_kind)
    if normalized == AgentSessionEffectKind.ACTIVE_CHAT_RUNTIME_RECONCILIATION:
        return AgentSessionEffectKind.STOP_ACTIVE_CHAT_RUNTIME
    if (
        normalized
        == AgentSessionEffectKind.IDLE_REVIEW_PLANNING_CANCELLATION_RECONCILIATION
    ):
        return AgentSessionEffectKind.CANCEL_IDLE_REVIEW_PLANNING
    raise ValueError(f"unsupported reconciliation effect kind: {normalized or '<empty>'}")


def _message_watermark(data: Mapping[str, Any]) -> int:
    """Return the greatest message-log id observed by this actor."""

    raw = data.get("message_watermark", 0)
    watermark = _optional_nonnegative_int(raw)
    if watermark is None:
        raise ValueError("aggregate message_watermark must be a non-negative integer")
    return watermark


def _with_message_watermark(
    data: Mapping[str, Any],
    message_log_id: int,
) -> dict[str, Any]:
    """Advance the coarse message watermark without moving it backwards."""

    if (
        not isinstance(message_log_id, int)
        or isinstance(message_log_id, bool)
        or message_log_id < 1
    ):
        raise ValueError("message_log_id must be a positive integer")
    updated = dict(data)
    updated["message_watermark"] = max(_message_watermark(data), message_log_id)
    return updated


def _with_message_delivery(
    data: Mapping[str, Any],
    append: AppendMessageLedgerEntry,
) -> dict[str, Any]:
    """Record actor identity plus the immutable adapter transport target.

    ``SessionKey.session_id`` is intentionally bot-scoped for actor ownership;
    it is not necessarily a session identifier accepted by an adapter.  Visible
    actions therefore retain the ingress ``base_session_id`` as their transport
    target and never reconstruct it from the actor key.
    """

    updated = _with_message_watermark(data, append.message_log_id)
    updated[_DELIVERY_CONTEXT_DATA_KEY] = {
        "instance_id": append.instance_id,
        "target_session_id": append.base_session_id,
    }
    pending_ids = _positive_int_tuple(
        updated.get(_PENDING_HIGH_PRIORITY_DATA_KEY),
        field_name=_PENDING_HIGH_PRIORITY_DATA_KEY,
        allow_empty=True,
    )
    if (
        append.eligible_for_work
        and append.priority.should_wake_active_reply
        and append.message_log_id not in pending_ids
    ):
        pending_ids = (*pending_ids, append.message_log_id)
    if pending_ids:
        updated[_PENDING_HIGH_PRIORITY_DATA_KEY] = list(pending_ids)
    else:
        updated.pop(_PENDING_HIGH_PRIORITY_DATA_KEY, None)
    return updated


def _operation_fence(
    data: Mapping[str, Any],
    operation_id: str,
) -> dict[str, Any]:
    """Load one pending operation fence from the aggregate registry."""

    normalized_id = _text(operation_id)
    if not normalized_id:
        raise ValueError("operation_id must not be empty")
    raw_registry = data.get("operation_fences")
    if not isinstance(raw_registry, Mapping):
        raise ValueError("aggregate operation_fences must be an object")
    raw_fence = raw_registry.get(normalized_id)
    if not isinstance(raw_fence, Mapping):
        raise ValueError(f"operation fence {normalized_id!r} is missing")
    return {str(key): value for key, value in raw_fence.items()}


def _with_operation_fence(
    data: Mapping[str, Any],
    operation_id: str,
    fence: Mapping[str, Any],
) -> dict[str, Any]:
    """Register the sole pending input fence for one workflow operation."""

    normalized_id = _text(operation_id)
    if not normalized_id:
        raise ValueError("operation_id must not be empty")
    raw_registry = data.get("operation_fences")
    if raw_registry is None:
        registry: dict[str, Any] = {}
    elif isinstance(raw_registry, Mapping):
        registry = {str(key): value for key, value in raw_registry.items()}
    else:
        raise ValueError("aggregate operation_fences must be an object")
    normalized_fence = {str(key): value for key, value in fence.items()}
    existing = registry.get(normalized_id)
    if existing is not None and existing != normalized_fence:
        raise ValueError(f"operation fence {normalized_id!r} already exists")
    registry[normalized_id] = normalized_fence
    updated = dict(data)
    updated["operation_fences"] = registry
    return updated


def _without_operation_fence(
    data: Mapping[str, Any],
    operation_id: str,
) -> dict[str, Any]:
    """Remove a terminal operation from the aggregate fence registry."""

    normalized_id = _text(operation_id)
    if not normalized_id:
        return dict(data)
    raw_registry = data.get("operation_fences")
    if raw_registry is None:
        return dict(data)
    if not isinstance(raw_registry, Mapping):
        raise ValueError("aggregate operation_fences must be an object")
    registry = {str(key): value for key, value in raw_registry.items()}
    registry.pop(normalized_id, None)
    updated = dict(data)
    if registry:
        updated["operation_fences"] = registry
    else:
        updated.pop("operation_fences", None)
    return updated


def _review_due_mismatch(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
) -> tuple[str, ...]:
    """Validate the complete durable identity of a due-review delivery."""

    mismatch: list[str] = []
    if event.payload.get("version") != 1:
        mismatch.append("version_changed")
    if _text(event.payload.get("event_id")) != event.event_id:
        mismatch.append("event_id_changed")
    session_key = event.payload.get("session_key")
    if not isinstance(session_key, Mapping):
        mismatch.append("session_key_missing")
    else:
        if _text(session_key.get("profile_id")) != aggregate.key.profile_id:
            mismatch.append("profile_id_changed")
        if _text(session_key.get("session_id")) != aggregate.key.session_id:
            mismatch.append("session_id_changed")
    payload_generation = _optional_nonnegative_int(
        event.payload.get("ownership_generation")
    )
    if (
        payload_generation is None
        or payload_generation != event.ownership_generation
        or payload_generation != aggregate.ownership_generation
    ):
        mismatch.append("ownership_generation_changed")
    plan_id = _text(event.payload.get("plan_id"))
    if not plan_id:
        mismatch.append("plan_id_missing")
    elif plan_id != aggregate.current_plan_id:
        mismatch.append("plan_id_changed")
    plan_revision = _optional_nonnegative_int(event.payload.get("plan_revision"))
    if plan_revision is None or plan_revision < 1:
        mismatch.append("plan_revision_missing")
    elif plan_revision != aggregate.review_plan_revision:
        mismatch.append("plan_revision_changed")
    return tuple(dict.fromkeys(mismatch))


def _control_runtime_fence_mismatch(
    aggregate: AgentSessionAggregate,
    intent: Mapping[str, Any],
) -> tuple[str, ...]:
    mismatch: list[str] = []
    expected_state = _text(intent.get("expected_state"))
    if not expected_state:
        mismatch.append("expected_state_missing")
    elif aggregate.state != expected_state:
        mismatch.append("state_changed")
    if "expected_current_plan_id" in intent and (
        aggregate.current_plan_id != _text(intent.get("expected_current_plan_id"))
    ):
        mismatch.append("current_plan_id_changed")
    expected_active_epoch = _optional_nonnegative_int(
        intent.get("expected_active_epoch")
    )
    if expected_active_epoch is None:
        mismatch.append("expected_active_epoch_missing")
    elif aggregate.active_epoch != expected_active_epoch:
        mismatch.append("active_epoch_changed")
    expected_activity_generation = _optional_nonnegative_int(
        intent.get("expected_activity_generation")
    )
    if expected_activity_generation is None:
        mismatch.append("expected_activity_generation_missing")
    elif aggregate.activity_generation != expected_activity_generation:
        mismatch.append("activity_generation_changed")
    return tuple(mismatch)


def _effect_completion_record(event: SessionEventEnvelope) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "effect_id": _text(event.payload.get("effect_id")),
        "effect_kind": _text(event.payload.get("effect_kind")),
        "idempotency_key": _text(event.payload.get("idempotency_key")),
        "operation_id": _text(event.payload.get("operation_id")),
        "attempt_count": _optional_nonnegative_int(event.payload.get("attempt_count")),
        "causation_id": event.causation_id,
        "source": event.source,
        "occurred_at": event.occurred_at,
    }


def _effect_failure_record(event: SessionEventEnvelope) -> dict[str, Any]:
    return {
        **_effect_completion_record(event),
        "failure_code": _text(event.payload.get("failure_code")),
        "failure_message": _text(event.payload.get("failure_message")),
    }


def _workflow_effect_failure_record(
    event: SessionEventEnvelope,
) -> dict[str, Any]:
    """Project verified workflow-effect failure provenance for operation audit."""

    return {
        **_effect_failure_record(event),
        "contract_signature": _text(event.payload.get("contract_signature")),
        "contract_version": _strict_nonnegative_int(
            event.payload.get("contract_version")
        ),
        "ownership_generation": event.ownership_generation,
        "plan_id": _text(event.payload.get("plan_id")),
        "active_epoch": _optional_nonnegative_int(
            event.payload.get("active_epoch")
        ),
        "activity_generation": _optional_nonnegative_int(
            event.payload.get("activity_generation")
        ),
        "input_watermark": _optional_nonnegative_int(
            event.payload.get("input_watermark")
        ),
        "input_ledger_sequence": _optional_nonnegative_int(
            event.payload.get("input_ledger_sequence")
        ),
    }


def _external_action_effect_failure_record(
    event: SessionEventEnvelope,
) -> dict[str, Any]:
    """Project verified action-effect failure evidence into aggregate state."""

    return {
        **_effect_failure_record(event),
        "action_ordinal": _strict_nonnegative_int(
            event.payload.get("action_ordinal")
        ),
        "contract_signature": _text(event.payload.get("contract_signature")),
        "contract_version": _strict_nonnegative_int(
            event.payload.get("contract_version")
        ),
        "ownership_generation": event.ownership_generation,
        "request_digest": _text(event.payload.get("request_digest")),
    }


def _effect_event_provenance_mismatch(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
    *,
    expected_event_id: str,
    expected_effect_kind: str,
    expected_effect_id: str,
    expected_idempotency_key: str,
    expected_operation_id: str,
    expected_plan_id: str,
    expected_active_epoch: int | None,
    expected_activity_generation: int | None,
    expected_input_watermark: int | None,
    expected_input_ledger_sequence: int | None,
    expected_causation_id: str,
    expected_ownership_generation: int | None,
    expected_contract_version: int | None = None,
    expected_contract_signature: str = "",
) -> tuple[str, ...]:
    """Validate complete executor provenance, treating omissions as stale."""

    effect_kind = _text(expected_effect_kind)
    mismatch: list[str] = []
    declared_contract_signature = _text(expected_contract_signature)
    if expected_contract_version is None:
        if declared_contract_signature:
            return ("expected_contract_snapshot_incomplete",)
        # Aggregates written before contract snapshots existed can only have
        # created v1 effects. Never let an inbound completion choose a newer
        # registered contract on their behalf.
        resolved_contract_version = 1
    else:
        if expected_contract_version < 1:
            return ("expected_contract_version_invalid",)
        if not declared_contract_signature:
            return ("expected_contract_snapshot_incomplete",)
        resolved_contract_version = expected_contract_version
    try:
        contract = builtin_effect_contract(
            effect_kind,
            version=resolved_contract_version,
        )
    except KeyError:
        return ("contract_version_unknown",)
    if (
        declared_contract_signature
        and declared_contract_signature != contract.signature
    ):
        mismatch.append("expected_contract_signature_changed")
    expected_signature = declared_contract_signature or contract.signature

    if not expected_event_id:
        mismatch.append("expected_event_id_missing")
    elif event.event_id != expected_event_id:
        mismatch.append("event_id_changed")
    if not event.source:
        mismatch.append("source_missing")
    elif event.source != contract.completion_source:
        mismatch.append("source_changed")
    if not expected_causation_id:
        mismatch.append("expected_causation_id_missing")
    elif not event.causation_id:
        mismatch.append("causation_id_missing")
    elif event.causation_id != expected_causation_id:
        mismatch.append("causation_id_changed")

    if expected_ownership_generation is None:
        mismatch.append("expected_ownership_generation_missing")
    elif (
        event.ownership_generation != expected_ownership_generation
        or aggregate.ownership_generation != expected_ownership_generation
    ):
        mismatch.append("ownership_generation_changed")

    text_fields = {
        "effect_id": expected_effect_id,
        "effect_kind": effect_kind,
        "idempotency_key": expected_idempotency_key,
        "operation_id": expected_operation_id,
        "plan_id": expected_plan_id,
        "contract_signature": expected_signature,
    }
    for field_name, expected in text_fields.items():
        if field_name not in event.payload:
            mismatch.append(f"{field_name}_missing")
        elif _text(event.payload.get(field_name)) != expected:
            mismatch.append(f"{field_name}_changed")

    integer_fields = {
        "active_epoch": expected_active_epoch,
        "activity_generation": expected_activity_generation,
        "contract_version": resolved_contract_version,
    }
    for field_name, expected in integer_fields.items():
        if field_name not in event.payload:
            mismatch.append(f"{field_name}_missing")
        elif _optional_nonnegative_int(event.payload.get(field_name)) != expected:
            mismatch.append(f"{field_name}_changed")

    if "input_watermark" not in event.payload:
        mismatch.append("input_watermark_missing")
    else:
        supplied_watermark = event.payload.get("input_watermark")
        if expected_input_watermark is None:
            if supplied_watermark is not None:
                mismatch.append("input_watermark_changed")
        elif _optional_nonnegative_int(supplied_watermark) != expected_input_watermark:
            mismatch.append("input_watermark_changed")

    if "input_ledger_sequence" not in event.payload:
        mismatch.append("input_ledger_sequence_missing")
    else:
        supplied_sequence = event.payload.get("input_ledger_sequence")
        if expected_input_ledger_sequence is None:
            if supplied_sequence is not None:
                mismatch.append("input_ledger_sequence_changed")
        elif (
            _optional_nonnegative_int(supplied_sequence)
            != expected_input_ledger_sequence
        ):
            mismatch.append("input_ledger_sequence_changed")

    if "attempt_count" not in event.payload:
        mismatch.append("attempt_count_missing")
    else:
        attempt_count = _optional_nonnegative_int(event.payload.get("attempt_count"))
        if attempt_count is None or attempt_count < 1:
            mismatch.append("attempt_count_invalid")
    return tuple(dict.fromkeys(mismatch))


def _planner_outcome_values(payload: Mapping[str, Any]) -> dict[str, Any]:
    allowed_fields = {
        "active_reply_threshold",
        "kind",
        "mention_sensitivity",
        "outcome_kind",
        "reason",
        "requested_delay_seconds",
    }
    nested = payload.get("outcome")
    if isinstance(nested, Mapping):
        values = {str(key): value for key, value in nested.items()}
    elif nested is not None:
        values = dict(payload)
        values["kind"] = nested
    else:
        values = dict(payload)
    return {key: value for key, value in values.items() if key in allowed_fields}


def _durable_effect(
    *,
    effect_id: str,
    kind: str,
    idempotency_key: str,
    operation_id: str,
    payload: dict[str, Any],
    available_at: float = 0.0,
    available_after_seconds: float | None = None,
) -> SessionEffect:
    """Build an effect pinned to the current built-in durable contract."""

    contract = builtin_effect_contract(kind)
    return SessionEffect(
        effect_id=effect_id,
        kind=kind,
        contract_version=contract.version,
        contract_signature=contract.signature,
        idempotency_key=idempotency_key,
        operation_id=operation_id,
        payload=payload,
        available_at=available_at,
        available_after_seconds=available_after_seconds,
    )


def _effect_contract_snapshot(effect_kind: str) -> dict[str, Any]:
    """Return the exact contract identity a new operation expects later."""

    contract = builtin_effect_contract(effect_kind)
    return {
        "contract_version": contract.version,
        "contract_signature": contract.signature,
    }


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def _positive_int_tuple(
    value: object,
    *,
    field_name: str,
    allow_empty: bool,
) -> tuple[int, ...]:
    if value is None and allow_empty:
        return ()
    if not isinstance(value, (list, tuple)):
        raise TypeError(f"{field_name} must be a list")
    normalized: list[int] = []
    seen: set[int] = set()
    for item in value:
        if not isinstance(item, int) or isinstance(item, bool) or item < 1:
            raise ValueError(f"{field_name} items must be positive integers")
        if item in seen:
            raise ValueError(f"{field_name} must not contain duplicates")
        normalized.append(item)
        seen.add(item)
    if not normalized and not allow_empty:
        raise ValueError(f"{field_name} must not be empty")
    return tuple(normalized)


def _active_reply_workflow_result(
    payload: Mapping[str, Any],
) -> ActiveReplyCompletionResult:
    return ActiveReplyCompletionResult.from_payload(
        _workflow_result_payload(payload)
    )


def _active_chat_bootstrap_workflow_result(
    payload: Mapping[str, Any],
) -> ActiveChatBootstrapCompletionResult:
    return ActiveChatBootstrapCompletionResult.from_payload(
        _workflow_result_payload(payload)
    )


def _active_chat_round_workflow_result(
    payload: Mapping[str, Any],
) -> ActiveChatRoundCompletionResult:
    return ActiveChatRoundCompletionResult.from_payload(
        _workflow_result_payload(payload)
    )


def _review_workflow_result(
    payload: Mapping[str, Any],
) -> ReviewCompletionResult:
    return ReviewCompletionResult.from_payload(_workflow_result_payload(payload))


def _workflow_result_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = payload.get("workflow_result")
    if not isinstance(raw, Mapping):
        raise WorkflowCompletionCodecError(
            "effect completion omitted versioned workflow_result"
        )
    return {str(key): value for key, value in raw.items()}


def _captured_input_boundary(fence: Mapping[str, Any]) -> tuple[int, int]:
    input_watermark = _optional_nonnegative_int(fence.get("input_watermark"))
    input_ledger_sequence = _optional_nonnegative_int(
        fence.get("input_ledger_sequence")
    )
    if input_watermark is None or input_ledger_sequence is None:
        raise ValueError("workflow operation has no committed input boundary")
    return input_watermark, input_ledger_sequence


def _validate_consumed_ids(
    message_log_ids: tuple[int, ...],
    *,
    input_watermark: int,
) -> None:
    outside = [item for item in message_log_ids if item > input_watermark]
    if outside:
        raise ValueError("consumed message ids exceed the operation watermark")


def _without_pending_high_priority_ids(
    data: Mapping[str, Any],
    consumed_ids: tuple[int, ...],
) -> dict[str, Any]:
    updated = dict(data)
    pending = _positive_int_tuple(
        updated.get(_PENDING_HIGH_PRIORITY_DATA_KEY),
        field_name=_PENDING_HIGH_PRIORITY_DATA_KEY,
        allow_empty=True,
    )
    consumed = set(consumed_ids)
    remaining = [item for item in pending if item not in consumed]
    if remaining:
        updated[_PENDING_HIGH_PRIORITY_DATA_KEY] = remaining
    else:
        updated.pop(_PENDING_HIGH_PRIORITY_DATA_KEY, None)
    return updated


def _pending_outbound_actions(data: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Return validated actor-owned external actions still awaiting a receipt.

    The aggregate retains enough immutable action identity to reject a mailbox
    completion whose payload was produced for another effect, operation, or
    canonical platform request.  Receipt status is the only mutable member.
    """

    raw = data.get(_PENDING_OUTBOUND_DATA_KEY)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError("pending_outbound_actions must be an object")
    pending: dict[str, dict[str, Any]] = {}
    ordinals: set[tuple[str, int]] = set()
    allowed_statuses = {
        "pending",
        "effect_failed",
        *(status.value for status in ExternalActionReceiptStatus),
    }
    for raw_effect_id, raw_entry in raw.items():
        effect_id = _text(raw_effect_id)
        if not effect_id:
            raise ValueError("pending_outbound_actions contains an empty effect id")
        if not isinstance(raw_entry, Mapping):
            raise ValueError(f"pending outbound action {effect_id!r} must be an object")
        entry = {str(key): value for key, value in raw_entry.items()}
        if _text(entry.get("effect_id")) != effect_id:
            raise ValueError(
                f"pending outbound action {effect_id!r} changed its effect id"
            )
        try:
            action_kind = ExternalActionKind(_text(entry.get("effect_kind")))
        except ValueError as exc:
            raise ValueError(
                f"pending outbound action {effect_id!r} has an invalid effect kind"
            ) from exc
        operation_id = _text(entry.get("operation_id"))
        idempotency_key = _text(entry.get("idempotency_key"))
        source_event_id = _text(entry.get("source_event_id"))
        request_digest = _text(entry.get("request_digest"))
        action_ordinal = _strict_nonnegative_int(entry.get("action_ordinal"))
        status = _text(entry.get("status"))
        if not operation_id:
            raise ValueError(f"pending outbound action {effect_id!r} has no operation")
        if not idempotency_key:
            raise ValueError(
                f"pending outbound action {effect_id!r} has no idempotency key"
            )
        if not source_event_id:
            raise ValueError(
                f"pending outbound action {effect_id!r} has no source event"
            )
        if not _is_sha256_digest(request_digest):
            raise ValueError(
                f"pending outbound action {effect_id!r} has an invalid request digest"
            )
        if action_ordinal is None:
            raise ValueError(
                f"pending outbound action {effect_id!r} has an invalid action ordinal"
            )
        if status not in allowed_statuses:
            raise ValueError(
                f"pending outbound action {effect_id!r} has an invalid receipt status"
            )
        ordinal_key = (operation_id, action_ordinal)
        if ordinal_key in ordinals:
            raise ValueError(
                "pending outbound actions reuse one operation action ordinal: "
                f"{operation_id}:{action_ordinal}"
            )
        ordinals.add(ordinal_key)
        entry["effect_kind"] = action_kind.value
        entry["action_ordinal"] = action_ordinal
        entry["status"] = status
        if status == "effect_failed":
            entry["effect_failure"] = _validated_pending_action_effect_failure(
                entry,
                effect_id=effect_id,
                action_kind=action_kind,
            )
        elif "effect_failure" in entry:
            raise ValueError(
                f"pending outbound action {effect_id!r} has failure evidence "
                "without an effect_failed status"
            )
        pending[effect_id] = entry
    return pending


def _validated_pending_action_effect_failure(
    entry: Mapping[str, Any],
    *,
    effect_id: str,
    action_kind: ExternalActionKind,
) -> dict[str, Any]:
    """Validate the durable evidence that exhausted one action effect."""

    raw = entry.get("effect_failure")
    if not isinstance(raw, Mapping):
        raise ValueError(
            f"pending outbound action {effect_id!r} has no effect failure evidence"
        )
    failure = {str(key): value for key, value in raw.items()}
    contract = builtin_external_action_effect_contract(action_kind)
    expected_text = {
        "effect_id": effect_id,
        "effect_kind": action_kind.value,
        "idempotency_key": _text(entry.get("idempotency_key")),
        "operation_id": _text(entry.get("operation_id")),
        "request_digest": _text(entry.get("request_digest")),
        "contract_signature": contract.signature,
        "causation_id": _text(entry.get("source_event_id")),
        "source": contract.completion_source,
    }
    for field_name, expected in expected_text.items():
        if _text(failure.get(field_name)) != expected:
            raise ValueError(
                f"pending outbound action {effect_id!r} failure changed "
                f"{field_name}"
            )
    if not _text(failure.get("event_id")):
        raise ValueError(
            f"pending outbound action {effect_id!r} failure has no event id"
        )
    if _strict_nonnegative_int(failure.get("action_ordinal")) != _strict_nonnegative_int(
        entry.get("action_ordinal")
    ):
        raise ValueError(
            f"pending outbound action {effect_id!r} failure changed action ordinal"
        )
    if _strict_nonnegative_int(failure.get("contract_version")) != contract.version:
        raise ValueError(
            f"pending outbound action {effect_id!r} failure changed contract version"
        )
    attempt_count = _strict_nonnegative_int(failure.get("attempt_count"))
    if attempt_count is None or attempt_count < 1:
        raise ValueError(
            f"pending outbound action {effect_id!r} failure has an invalid attempt"
        )
    ownership_generation = _strict_nonnegative_int(
        failure.get("ownership_generation")
    )
    if ownership_generation is None or ownership_generation < 1:
        raise ValueError(
            f"pending outbound action {effect_id!r} failure has an invalid ownership"
        )
    if not _text(failure.get("failure_code")):
        raise ValueError(
            f"pending outbound action {effect_id!r} failure has no failure code"
        )
    if not isinstance(failure.get("failure_message"), str):
        raise ValueError(
            f"pending outbound action {effect_id!r} failure has an invalid message"
        )
    return failure


def _with_pending_outbound_actions(
    data: Mapping[str, Any],
    effects: tuple[SessionEffect, ...],
    *,
    source_event_id: str,
) -> dict[str, Any]:
    """Persist one accepted action batch before the actor starts later work.

    A batch may only be created when no earlier batch remains unresolved.  That
    makes the aggregate-level gate independent from outbox worker timing and
    lets a completion prove exactly which model decision it is releasing.
    """

    if _pending_outbound_actions(data):
        raise ValueError("cannot create external actions while another batch is pending")
    normalized_source_event_id = _text(source_event_id)
    if not normalized_source_event_id:
        raise ValueError("pending outbound actions require a source event id")
    if not effects:
        raise ValueError("pending outbound actions require at least one effect")

    pending: dict[str, dict[str, Any]] = {}
    ordinals: set[tuple[str, int]] = set()
    for effect in effects:
        effect_id = _text(effect.effect_id)
        if effect_id in pending:
            raise ValueError(f"duplicate pending outbound effect id: {effect_id!r}")
        try:
            action_kind = ExternalActionKind(effect.kind)
        except ValueError as exc:
            raise ValueError(
                f"pending outbound effect {effect_id!r} is not an external action"
            ) from exc
        payload = _mapping(effect.payload)
        operation_id = _text(effect.operation_id)
        if not operation_id or _text(payload.get("operation_id")) != operation_id:
            raise ValueError(
                f"pending outbound effect {effect_id!r} changed its operation id"
            )
        action_ordinal = _strict_nonnegative_int(payload.get("action_ordinal"))
        if action_ordinal is None:
            raise ValueError(
                f"pending outbound effect {effect_id!r} has an invalid action ordinal"
            )
        ordinal_key = (operation_id, action_ordinal)
        if ordinal_key in ordinals:
            raise ValueError(
                "external action effects reuse one operation action ordinal: "
                f"{operation_id}:{action_ordinal}"
            )
        ordinals.add(ordinal_key)
        request_digest = _text(payload.get("request_digest"))
        if not _is_sha256_digest(request_digest):
            raise ValueError(
                f"pending outbound effect {effect_id!r} has an invalid request digest"
            )
        idempotency_key = _text(effect.idempotency_key)
        if not idempotency_key:
            raise ValueError(
                f"pending outbound effect {effect_id!r} has no idempotency key"
            )
        contract = builtin_external_action_effect_contract(action_kind)
        if (
            effect.contract_version != contract.version
            or effect.contract_signature != contract.signature
        ):
            raise ValueError(
                f"pending outbound effect {effect_id!r} changed its action contract"
            )
        pending[effect_id] = {
            "action_ordinal": action_ordinal,
            "effect_id": effect_id,
            "effect_kind": action_kind.value,
            "idempotency_key": idempotency_key,
            "operation_id": operation_id,
            "request_digest": request_digest,
            "source_event_id": normalized_source_event_id,
            "status": "pending",
        }

    updated = dict(data)
    updated[_PENDING_OUTBOUND_DATA_KEY] = pending
    updated.pop("outbound_blocked_reason", None)
    updated.pop(_OUTBOUND_BLOCKED_DATA_KEY, None)
    return updated


def _external_action_completion_mismatch(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
    *,
    expected: Mapping[str, Any],
) -> tuple[str, ...]:
    """Reject action completion evidence that does not match the accepted intent."""

    mismatch = list(
        _external_action_effect_provenance_mismatch(
            aggregate,
            event,
            expected=expected,
            outcome="completed",
        )
    )
    if _mapping(expected.get("effect_failure")):
        mismatch.append("effect_failure_recorded")

    receipt_status = _text(event.payload.get("receipt_status"))
    completion_statuses = {
        ExternalActionReceiptStatus.SUCCEEDED.value,
        ExternalActionReceiptStatus.REJECTED_BEFORE_DISPATCH.value,
        ExternalActionReceiptStatus.ABANDONED_BEFORE_DISPATCH.value,
        ExternalActionReceiptStatus.UNKNOWN.value,
    }
    if receipt_status not in completion_statuses:
        mismatch.append("receipt_status_invalid")
    expected_status = _text(expected.get("status"))
    if expected_status != "pending" and receipt_status != expected_status:
        mismatch.append("receipt_status_changed")
    return tuple(dict.fromkeys(mismatch))


def _external_action_effect_failure_mismatch(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
    *,
    expected: Mapping[str, Any],
    effect_kind: str,
) -> tuple[str, ...]:
    """Reject a terminal action-effect failure without exact accepted identity."""

    mismatch = list(
        _external_action_effect_provenance_mismatch(
            aggregate,
            event,
            expected=expected,
            outcome="failed",
        )
    )
    if _text(expected.get("effect_kind")) != effect_kind:
        mismatch.append("effect_kind_changed")
    if _text(expected.get("status")) != "pending":
        mismatch.append("receipt_status_changed")
    if _mapping(expected.get("effect_failure")):
        mismatch.append("effect_failure_recorded")
    if not _text(event.payload.get("failure_code")):
        mismatch.append("failure_code_missing")
    if "failure_message" not in event.payload:
        mismatch.append("failure_message_missing")
    return tuple(dict.fromkeys(mismatch))


def _external_action_effect_provenance_mismatch(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
    *,
    expected: Mapping[str, Any],
    outcome: str,
) -> tuple[str, ...]:
    """Validate the immutable provenance shared by action outcomes."""

    if not expected:
        return ("pending_external_action_missing",)
    mismatch: list[str] = []
    effect_id = _text(expected.get("effect_id"))
    try:
        action_kind = ExternalActionKind(_text(expected.get("effect_kind")))
        contract = builtin_external_action_effect_contract(action_kind)
    except (KeyError, ValueError):
        return ("pending_external_action_contract_invalid",)

    expected_event_id = derived_effect_event_id(
        key=aggregate.key,
        effect_id=effect_id,
        outcome=outcome,
    )
    if event.event_id != expected_event_id:
        mismatch.append("event_id_changed")
    if event.source != contract.completion_source:
        mismatch.append("source_changed")
    expected_causation_id = _text(expected.get("source_event_id"))
    if not event.causation_id:
        mismatch.append("causation_id_missing")
    elif event.causation_id != expected_causation_id:
        mismatch.append("causation_id_changed")
    expected_correlation_id = (
        _text(expected.get("operation_id")) or effect_id
    )
    if event.correlation_id != expected_correlation_id:
        mismatch.append("correlation_id_changed")
    if (
        event.ownership_generation != aggregate.ownership_generation
        or aggregate.ownership_generation < 1
    ):
        mismatch.append("ownership_generation_changed")

    expected_text = {
        "effect_id": effect_id,
        "effect_kind": action_kind.value,
        "idempotency_key": _text(expected.get("idempotency_key")),
        "operation_id": _text(expected.get("operation_id")),
        "request_digest": _text(expected.get("request_digest")),
        "contract_signature": contract.signature,
    }
    for field_name, expected_value in expected_text.items():
        if field_name not in event.payload:
            mismatch.append(f"{field_name}_missing")
        elif _text(event.payload.get(field_name)) != expected_value:
            mismatch.append(f"{field_name}_changed")

    expected_ordinal = _strict_nonnegative_int(expected.get("action_ordinal"))
    if expected_ordinal is None:
        mismatch.append("action_ordinal_expected_invalid")
    elif _strict_nonnegative_int(event.payload.get("action_ordinal")) != expected_ordinal:
        mismatch.append("action_ordinal_changed")
    if _strict_nonnegative_int(event.payload.get("contract_version")) != contract.version:
        mismatch.append("contract_version_changed")
    attempt_count = _strict_nonnegative_int(event.payload.get("attempt_count"))
    if attempt_count is None or attempt_count < 1:
        mismatch.append("attempt_count_invalid")
    return tuple(dict.fromkeys(mismatch))


def _all_pending_outbound_succeeded(
    pending: Mapping[str, Mapping[str, Any]],
) -> bool:
    """Return whether every accepted action has durable success evidence."""

    return bool(pending) and all(
        _text(entry.get("status")) == ExternalActionReceiptStatus.SUCCEEDED.value
        for entry in pending.values()
    )


def _has_unsettled_pending_outbound(data: Mapping[str, Any]) -> bool:
    """Return whether the actor must wait for accepted external actions."""

    return bool(_pending_outbound_actions(data))


def _review_cancellation_blocks_active_reply(data: Mapping[str, Any]) -> bool:
    """Return whether an exhausted review cancellation blocks new replies.

    A superseded review may still have a model task running after its control
    effect exhausts retries.  Keep later high-priority input unread for a
    review, but never launch another active-reply workflow into that unknown
    cancellation tail.
    """

    return _REVIEW_CANCELLATION_BLOCKER_DATA_KEY in data


def _pending_outbound_operation_id(data: Mapping[str, Any]) -> str:
    """Return one stable blocker operation for review-schedule diagnostics."""

    pending = _pending_outbound_actions(data)
    if not pending:
        return ""
    first_effect_id = min(pending)
    return _text(pending[first_effect_id].get("operation_id"))


def _outbound_continuation(data: Mapping[str, Any]) -> dict[str, Any]:
    """Read the actor-owned continuation that follows an action batch."""

    raw = data.get(_OUTBOUND_CONTINUATION_DATA_KEY)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError("outbound_continuation must be an object")
    continuation = {str(key): value for key, value in raw.items()}
    kind = _text(continuation.get("kind"))
    if kind != "active_chat_round":
        raise ValueError("outbound_continuation has an unsupported kind")
    if not _text(continuation.get("source_operation_id")):
        raise ValueError("outbound_continuation has no source operation")
    try:
        outcome = ActiveChatRoundOutcome(_text(continuation.get("outcome")))
    except ValueError as exc:
        raise ValueError("outbound_continuation has an invalid active-chat outcome") from exc
    continuation["outcome"] = outcome.value
    return continuation


def _with_outbound_continuation(
    data: Mapping[str, Any],
    *,
    kind: str,
    source_operation_id: str,
    outcome: str,
) -> dict[str, Any]:
    """Persist one deterministic post-action continuation for a round."""

    if _outbound_continuation(data):
        raise ValueError("cannot replace an unresolved outbound continuation")
    normalized_kind = _text(kind)
    normalized_operation_id = _text(source_operation_id)
    if normalized_kind != "active_chat_round":
        raise ValueError("unsupported outbound continuation kind")
    if not normalized_operation_id:
        raise ValueError("outbound continuation requires a source operation")
    try:
        normalized_outcome = ActiveChatRoundOutcome(_text(outcome)).value
    except ValueError as exc:
        raise ValueError("outbound continuation requires a valid round outcome") from exc
    updated = dict(data)
    updated[_OUTBOUND_CONTINUATION_DATA_KEY] = {
        "kind": normalized_kind,
        "outcome": normalized_outcome,
        "source_operation_id": normalized_operation_id,
    }
    return updated


def _without_outbound_continuation(data: Mapping[str, Any]) -> dict[str, Any]:
    """Remove an action continuation once the actor has consumed it."""

    updated = dict(data)
    updated.pop(_OUTBOUND_CONTINUATION_DATA_KEY, None)
    return updated


def _control_intent(
    data: Mapping[str, Any],
    effect_kind: str,
) -> dict[str, Any]:
    intents = _mapping(data.get(_CONTROL_INTENTS_DATA_KEY))
    return _mapping(intents.get(_text(effect_kind)))


def _with_control_intent(
    data: Mapping[str, Any],
    *,
    effect_kind: str,
    intent: Mapping[str, Any],
) -> dict[str, Any]:
    updated = dict(data)
    intents = _mapping(updated.get(_CONTROL_INTENTS_DATA_KEY))
    intents[_text(effect_kind)] = dict(intent)
    updated[_CONTROL_INTENTS_DATA_KEY] = intents
    return updated


def _with_completed_control_intent(
    data: Mapping[str, Any],
    *,
    effect_kind: str,
    intent: Mapping[str, Any],
    event: SessionEventEnvelope,
) -> dict[str, Any]:
    """Record one verified control completion without requiring legacy state."""

    if not intent:
        return dict(data)
    return _with_control_intent(
        data,
        effect_kind=effect_kind,
        intent={
            **intent,
            "status": "completed",
            "completion": _effect_completion_record(event),
        },
    )


def _supersede_active_chat_exit_request(
    data: Mapping[str, Any],
    *,
    event_id: str,
) -> dict[str, Any]:
    """Invalidate a pending exit when new active-chat input arrives."""

    effect_kind = AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST
    intent = _control_intent(data, effect_kind)
    if _text(intent.get("status")) not in {"requested", "failed"}:
        return dict(data)
    return _with_control_intent(
        data,
        effect_kind=effect_kind,
        intent={
            **intent,
            "status": "superseded",
            "superseded_by_event_id": event_id,
        },
    )


def _active_chat_exit_request_pending(data: Mapping[str, Any]) -> bool:
    """Return whether a live exit-control effect already owns this epoch."""

    intent = _control_intent(
        data,
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
    )
    return _text(intent.get("status")) == "requested"


def _active_chat_exit_request_blocked(data: Mapping[str, Any]) -> bool:
    """Return whether exhausted exit retries require new input or intervention."""

    intent = _control_intent(
        data,
        AgentSessionEffectKind.ENQUEUE_ACTIVE_CHAT_EXIT_REQUEST,
    )
    return _text(intent.get("status")) == "failed"


def _without_idle_exit(value: Mapping[str, Any]) -> dict[str, Any]:
    data = dict(value)
    data.pop("idle_exit", None)
    return data


def _text(value: object) -> str:
    return str(value or "").strip()


def _is_nonnegative_finite(value: object) -> bool:
    if isinstance(value, bool):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and numeric >= 0.0


def _optional_delay(value: object) -> float | None:
    if value is None or not _is_nonnegative_finite(value):
        return None
    return float(value)


def _finite_nonnegative_float(
    value: object,
    *,
    field_name: str,
    default: float,
) -> float:
    """Read a finite non-negative aggregate numeric field or a safe default."""

    if value is None:
        return default
    if not _is_nonnegative_finite(value):
        raise ValueError(f"{field_name} must be finite and non-negative")
    return float(value)


def _finite_positive_float(
    value: object,
    *,
    field_name: str,
    default: float,
) -> float:
    """Read a finite positive aggregate numeric field or a safe default."""

    numeric = _finite_nonnegative_float(
        value,
        field_name=field_name,
        default=default,
    )
    if numeric <= 0:
        raise ValueError(f"{field_name} must be positive")
    return numeric


def _optional_nonnegative_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    if numeric < 0:
        return None
    return numeric


def _strict_nonnegative_int(value: object) -> int | None:
    """Return a JSON integer without coercing strings or booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _is_sha256_digest(value: str) -> bool:
    """Return whether *value* is the canonical lowercase SHA-256 hex digest."""

    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _event_time(
    aggregate: AgentSessionAggregate,
    event: SessionEventEnvelope,
) -> float:
    occurred_at = float(event.occurred_at)
    if not math.isfinite(occurred_at) or occurred_at < 0:
        raise ValueError("event occurred_at must be finite and non-negative")
    return max(aggregate.updated_at, occurred_at)


__all__ = [
    "AgentSessionEffectKind",
    "AgentSessionEventKind",
    "AgentSessionReducer",
    "AgentSessionState",
    "Bypassed",
    "Defaulted",
    "DeterministicReducerIdFactory",
    "Failed",
    "IdleExitReducerConfig",
    "IdleReviewScheduleOutcome",
    "IdleReviewScheduleOutcomeKind",
    "Planned",
    "ReducerIdFactory",
    "SettledScheduleOutcome",
    "Superseded",
]
