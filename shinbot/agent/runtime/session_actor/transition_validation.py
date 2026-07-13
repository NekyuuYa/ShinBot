"""Pure protocol validation shared by session actors and durable stores."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from shinbot.agent.runtime.session_actor.aggregate import AgentSessionAggregate
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    validate_effect_declaration,
)
from shinbot.agent.runtime.session_actor.events import (
    ReviewScheduleStatus,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.json_validation import (
    DurableJSONValidationError,
    validate_durable_json,
)

_MISSING = object()
_SCHEDULE_OUTCOME_FIELDS = frozenset(
    {
        "active_reply_threshold",
        "applied_delay_seconds",
        "fallback_reason",
        "kind",
        "mention_sensitivity",
        "model_execution_id",
        "prompt_signature",
        "reason",
        "requested_delay_seconds",
        "source",
    }
)
_FAILED_SCHEDULE_OUTCOME_DIAGNOSTIC_FIELDS = frozenset(
    {
        "failure_code",
        "failure_message",
    }
)
_FAILED_SCHEDULE_OUTCOME_FIELDS = (
    _SCHEDULE_OUTCOME_FIELDS | _FAILED_SCHEDULE_OUTCOME_DIAGNOSTIC_FIELDS
)


class ReviewPlanTransitionValidationError(ValueError):
    """Raised when a new review plan lacks one coherent durable declaration."""


def validate_session_transition(
    current: AgentSessionAggregate,
    transition: SessionTransition,
    *,
    effect_contract_authority: EffectContractAuthority,
) -> None:
    """Validate the pure, cross-store shape of one actor transition.

    Durable stores additionally validate aggregate diffs, leases, database
    uniqueness, and existing records. This function intentionally preserves the
    actor-level rules that can be checked before a persistence transaction, so
    actors and commit-time recovery materializers share one invariant boundary.
    """

    if not isinstance(current, AgentSessionAggregate):
        raise TypeError("current must be an AgentSessionAggregate")
    if not isinstance(transition, SessionTransition):
        raise TypeError("session event handler must return SessionTransition")
    if not isinstance(effect_contract_authority, EffectContractAuthority):
        raise TypeError("effect_contract_authority must be an EffectContractAuthority")
    next_aggregate = transition.aggregate
    if next_aggregate.key != current.key:
        raise ValueError("a session transition cannot change actor ownership")
    if next_aggregate.event_sequence != current.event_sequence + 1:
        raise ValueError("a session transition must advance event_sequence exactly once")
    if next_aggregate.state_revision not in {
        current.state_revision,
        current.state_revision + 1,
    }:
        raise ValueError("a session transition may advance state_revision at most once")
    effect_ids = [effect.effect_id for effect in transition.effects]
    if len(effect_ids) != len(set(effect_ids)):
        raise ValueError("a session transition contains duplicate effect ids")
    for effect in transition.effects:
        validate_effect_declaration(effect, authority=effect_contract_authority)
    operation_ids = [operation.operation_id for operation in transition.operations]
    if len(operation_ids) != len(set(operation_ids)):
        raise ValueError("a session transition contains duplicate operation ids")
    plan_ids = [schedule.plan_id for schedule in transition.review_schedules]
    if len(plan_ids) != len(set(plan_ids)):
        raise ValueError("a session transition contains duplicate review plan ids")
    validate_review_plan_transition(current, transition)
    if transition.review_schedules:
        if len(transition.review_schedules) != 1:
            raise ValueError("a session transition may replace at most one review plan")
        schedule = transition.review_schedules[0]
        if not transition.caused_plan_id:
            raise ValueError("a review schedule transition must identify caused_plan_id")
        if schedule.plan_id != transition.caused_plan_id:
            raise ValueError("review schedule does not match caused_plan_id")
        if schedule.plan_id != next_aggregate.current_plan_id:
            raise ValueError("review schedule does not match aggregate current_plan_id")
        if schedule.plan_revision != next_aggregate.review_plan_revision:
            raise ValueError(
                "review schedule revision does not match aggregate plan revision"
            )
    schedule_event_ids = [
        event.schedule_event_id for event in transition.review_schedule_events
    ]
    if len(schedule_event_ids) != len(set(schedule_event_ids)):
        raise ValueError("a session transition contains duplicate schedule event ids")


def validate_review_plan_transition(
    current: AgentSessionAggregate,
    transition: SessionTransition,
) -> None:
    """Validate one new plan, schedule, and scheduled journal as one protocol.

    Transitions which do not advance the aggregate plan fence are intentionally
    ignored here. Existing-plan immutability and database lifecycle checks stay
    with the durable store.

    Args:
        current: Aggregate snapshot used to reduce the mailbox event.
        transition: Declarative transition returned by the event handler.

    Raises:
        ReviewPlanTransitionValidationError: If a plan advance is malformed or
            lacks exactly one matching schedule and scheduled journal.
    """

    target = transition.aggregate
    current_revision = _nonnegative_integer(
        current.review_plan_revision,
        field_name="current.review_plan_revision",
    )
    target_revision = _nonnegative_integer(
        target.review_plan_revision,
        field_name="target.review_plan_revision",
    )
    current_plan_id = _plan_id(
        current.current_plan_id,
        field_name="current.current_plan_id",
        allow_empty=current_revision == 0,
    )
    target_plan_id = _plan_id(
        target.current_plan_id,
        field_name="target.current_plan_id",
        allow_empty=target_revision == 0,
    )
    plan_id_changed = target_plan_id != current_plan_id
    plan_revision_changed = target_revision != current_revision
    plan_advanced = plan_id_changed or plan_revision_changed
    if not plan_advanced:
        for journal in transition.review_schedule_events:
            event_type = _exact_text(
                journal.event_type,
                field_name="review_schedule_event.event_type",
            )
            if event_type != event_type.strip():
                raise ReviewPlanTransitionValidationError(
                    "review schedule event type must not contain surrounding whitespace"
                )
            if event_type == "scheduled":
                raise ReviewPlanTransitionValidationError(
                    "a scheduled review journal requires the plan fence to advance"
                )
        return
    if plan_id_changed != plan_revision_changed:
        raise ReviewPlanTransitionValidationError(
            "review plan id and revision must advance together"
        )
    if target_revision != current_revision + 1:
        raise ReviewPlanTransitionValidationError(
            "review plan revision must advance by exactly one"
        )
    if len(transition.review_schedules) != 1:
        raise ReviewPlanTransitionValidationError(
            "advancing the review plan requires exactly one schedule"
        )
    if len(transition.review_schedule_events) != 1:
        raise ReviewPlanTransitionValidationError(
            "advancing the review plan requires exactly one scheduled journal"
        )
    if transition.caused_plan_id != target_plan_id:
        raise ReviewPlanTransitionValidationError(
            "new review plan does not match transition caused_plan_id"
        )

    _validate_new_plan_identity(
        target.review_plan,
        plan_id=target_plan_id,
        plan_revision=target_revision,
    )
    schedule = transition.review_schedules[0]
    journal = transition.review_schedule_events[0]
    _validate_new_schedule_identity(
        schedule,
        plan_id=target_plan_id,
        plan_revision=target_revision,
    )
    _validate_new_journal_identity(
        journal,
        current_plan_id=current_plan_id,
        plan_id=target_plan_id,
        plan_revision=target_revision,
    )

    plan_semantics = review_plan_semantics(
        target.review_plan,
        default_committed_state_revision=target.state_revision,
    )
    schedule_semantics = review_schedule_semantics(
        schedule,
        default_committed_state_revision=target.state_revision,
    )
    mismatched = tuple(
        field_name
        for field_name, value in plan_semantics.items()
        if schedule_semantics[field_name] != value
    )
    if mismatched:
        raise ReviewPlanTransitionValidationError(
            "new review plan payload does not match schedule semantics: "
            + ", ".join(mismatched)
        )

    journal_semantics = review_schedule_event_semantics(
        journal,
        default_committed_state_revision=target.state_revision,
    )
    mismatched = tuple(
        field_name
        for field_name, value in journal_semantics.items()
        if schedule_semantics[field_name] != value
    )
    if mismatched:
        raise ReviewPlanTransitionValidationError(
            "new review schedule journal does not match schedule semantics: "
            + ", ".join(mismatched)
        )
    if journal_semantics["outcome"] == "failed":
        _validate_failed_schedule_outcome_diagnostics(
            target.review_plan,
            journal,
        )


def review_plan_semantics(
    review_plan: Mapping[str, object],
    *,
    default_committed_state_revision: int,
) -> dict[str, object]:
    """Return strict comparable policy fields from an aggregate review plan."""

    _validate_json(review_plan, path="review_plan")
    kind = _mapping_text(review_plan, "kind")
    legacy_outcome = _mapping_text(review_plan, "outcome")
    if kind and legacy_outcome and kind != legacy_outcome:
        raise ReviewPlanTransitionValidationError(
            "review plan kind and outcome aliases do not match"
        )
    return {
        "applied_delay_seconds": _mapping_number(
            review_plan,
            "applied_delay_seconds",
            required=True,
        ),
        "trigger": _mapping_text(review_plan, "trigger"),
        "outcome": kind or legacy_outcome,
        "source": _mapping_text(review_plan, "source"),
        "requested_delay_seconds": _mapping_number(
            review_plan,
            "requested_delay_seconds",
        ),
        "reason": _mapping_text(review_plan, "reason"),
        "fallback_reason": _mapping_text(review_plan, "fallback_reason"),
        "mention_sensitivity": (
            _mapping_text(review_plan, "mention_sensitivity") or "normal"
        ),
        "active_reply_threshold": _mapping_json_object(
            review_plan,
            "active_reply_threshold",
        ),
        "model_execution_id": _mapping_text(review_plan, "model_execution_id"),
        "prompt_signature": _mapping_text(review_plan, "prompt_signature"),
        "expected_active_epoch": _mapping_optional_integer(
            review_plan,
            "expected_active_epoch",
        ),
        "expected_activity_generation": _mapping_optional_integer(
            review_plan,
            "expected_activity_generation",
        ),
        "committed_state_revision": _mapping_defaulted_integer(
            review_plan,
            "committed_state_revision",
            default=default_committed_state_revision,
        ),
    }


def review_schedule_semantics(
    schedule: SessionReviewSchedule,
    *,
    default_committed_state_revision: int,
) -> dict[str, object]:
    """Return strict comparable policy fields from one review schedule."""

    return {
        "applied_delay_seconds": _finite_number(
            schedule.applied_delay_seconds,
            field_name="review_schedule.applied_delay_seconds",
        ),
        "trigger": _exact_text(schedule.trigger, field_name="review_schedule.trigger"),
        "outcome": _exact_text(schedule.outcome, field_name="review_schedule.outcome"),
        "source": _exact_text(schedule.source, field_name="review_schedule.source"),
        "requested_delay_seconds": _optional_number(
            schedule.requested_delay_seconds,
            field_name="review_schedule.requested_delay_seconds",
        ),
        "reason": _exact_text(schedule.reason, field_name="review_schedule.reason"),
        "fallback_reason": _exact_text(
            schedule.fallback_reason,
            field_name="review_schedule.fallback_reason",
        ),
        "mention_sensitivity": _exact_text(
            schedule.mention_sensitivity,
            field_name="review_schedule.mention_sensitivity",
        )
        or "normal",
        "active_reply_threshold": _json_object(
            schedule.active_reply_threshold,
            field_name="review_schedule.active_reply_threshold",
        ),
        "model_execution_id": _exact_text(
            schedule.model_execution_id,
            field_name="review_schedule.model_execution_id",
        ),
        "prompt_signature": _exact_text(
            schedule.prompt_signature,
            field_name="review_schedule.prompt_signature",
        ),
        "expected_active_epoch": _optional_integer(
            schedule.expected_active_epoch,
            field_name="review_schedule.expected_active_epoch",
        ),
        "expected_activity_generation": _optional_integer(
            schedule.expected_activity_generation,
            field_name="review_schedule.expected_activity_generation",
        ),
        "committed_state_revision": _defaulted_integer(
            schedule.committed_state_revision,
            default=default_committed_state_revision,
            field_name="review_schedule.committed_state_revision",
        ),
    }


def review_schedule_event_semantics(
    event: SessionReviewScheduleEvent,
    *,
    default_committed_state_revision: int,
) -> dict[str, object]:
    """Return common strict policy fields from a scheduled journal event."""

    semantics: dict[str, object] = {
        "applied_delay_seconds": _required_optional_number(
            event.applied_delay_seconds,
            field_name="review_schedule_event.applied_delay_seconds",
        ),
        "trigger": _exact_text(
            event.trigger,
            field_name="review_schedule_event.trigger",
        ),
        "outcome": _exact_text(
            event.outcome,
            field_name="review_schedule_event.outcome",
        ),
        "source": _exact_text(
            event.source,
            field_name="review_schedule_event.source",
        ),
        "requested_delay_seconds": _optional_number(
            event.requested_delay_seconds,
            field_name="review_schedule_event.requested_delay_seconds",
        ),
        "reason": _exact_text(
            event.reason,
            field_name="review_schedule_event.reason",
        ),
        "fallback_reason": _exact_text(
            event.fallback_reason,
            field_name="review_schedule_event.fallback_reason",
        ),
        "model_execution_id": _exact_text(
            event.model_execution_id,
            field_name="review_schedule_event.model_execution_id",
        ),
        "prompt_signature": _exact_text(
            event.prompt_signature,
            field_name="review_schedule_event.prompt_signature",
        ),
        "expected_active_epoch": _optional_integer(
            event.expected_active_epoch,
            field_name="review_schedule_event.expected_active_epoch",
        ),
        "expected_activity_generation": _optional_integer(
            event.expected_activity_generation,
            field_name="review_schedule_event.expected_activity_generation",
        ),
        "committed_state_revision": _defaulted_integer(
            event.committed_state_revision,
            default=default_committed_state_revision,
            field_name="review_schedule_event.committed_state_revision",
        ),
    }
    outcome = _schedule_outcome_metadata(event)
    outcome_semantics: dict[str, object] = {
        "active_reply_threshold": _json_object(
            outcome["active_reply_threshold"],
            field_name=(
                "review_schedule_event.metadata.schedule_outcome."
                "active_reply_threshold"
            ),
        ),
        "applied_delay_seconds": _finite_number(
            outcome["applied_delay_seconds"],
            field_name=(
                "review_schedule_event.metadata.schedule_outcome."
                "applied_delay_seconds"
            ),
        ),
        "fallback_reason": _exact_text(
            outcome["fallback_reason"],
            field_name=(
                "review_schedule_event.metadata.schedule_outcome.fallback_reason"
            ),
        ),
        "mention_sensitivity": _exact_text(
            outcome["mention_sensitivity"],
            field_name=(
                "review_schedule_event.metadata.schedule_outcome.mention_sensitivity"
            ),
        )
        or "normal",
        "model_execution_id": _exact_text(
            outcome["model_execution_id"],
            field_name=(
                "review_schedule_event.metadata.schedule_outcome.model_execution_id"
            ),
        ),
        "outcome": _exact_text(
            outcome["kind"],
            field_name="review_schedule_event.metadata.schedule_outcome.kind",
        ),
        "prompt_signature": _exact_text(
            outcome["prompt_signature"],
            field_name=(
                "review_schedule_event.metadata.schedule_outcome.prompt_signature"
            ),
        ),
        "reason": _exact_text(
            outcome["reason"],
            field_name="review_schedule_event.metadata.schedule_outcome.reason",
        ),
        "requested_delay_seconds": _optional_number(
            outcome["requested_delay_seconds"],
            field_name=(
                "review_schedule_event.metadata.schedule_outcome."
                "requested_delay_seconds"
            ),
        ),
        "source": _exact_text(
            outcome["source"],
            field_name="review_schedule_event.metadata.schedule_outcome.source",
        ),
    }
    overlapping_fields = tuple(
        field_name
        for field_name in outcome_semantics
        if field_name in semantics
        and semantics[field_name] != outcome_semantics[field_name]
    )
    if overlapping_fields:
        raise ReviewPlanTransitionValidationError(
            "review schedule outcome metadata does not match journal semantics: "
            + ", ".join(overlapping_fields)
        )
    semantics.update(
        {
            "active_reply_threshold": outcome_semantics["active_reply_threshold"],
            "mention_sensitivity": outcome_semantics["mention_sensitivity"],
        }
    )
    return semantics


def _schedule_outcome_metadata(
    event: SessionReviewScheduleEvent,
) -> dict[str, Any]:
    """Return one exact schedule-outcome record from an append-only journal.

    Failure diagnostics were added after the original common schedule outcome
    projection.  A failed record may therefore carry the diagnostic pair, but
    the base-only failed shape remains readable for pre-diagnostic durable
    history.  Every other outcome retains the original exact field set.
    """

    metadata = _json_object(
        event.metadata,
        field_name="review_schedule_event.metadata",
    )
    if "schedule_outcome" not in metadata:
        raise ReviewPlanTransitionValidationError(
            "new review plan journal metadata is missing schedule_outcome"
        )
    outcome = _json_object(
        metadata["schedule_outcome"],
        field_name="review_schedule_event.metadata.schedule_outcome",
    )
    kind = _exact_text(
        outcome.get("kind", _MISSING),
        field_name="review_schedule_event.metadata.schedule_outcome.kind",
    )
    outcome_fields = frozenset(outcome)
    expected_field_sets = (
        (_SCHEDULE_OUTCOME_FIELDS, _FAILED_SCHEDULE_OUTCOME_FIELDS)
        if kind == "failed"
        else (_SCHEDULE_OUTCOME_FIELDS,)
    )
    if outcome_fields not in expected_field_sets:
        expected_fields = (
            _FAILED_SCHEDULE_OUTCOME_FIELDS
            if kind == "failed"
            else _SCHEDULE_OUTCOME_FIELDS
        )
        missing = sorted(expected_fields - outcome_fields)
        unexpected = sorted(outcome_fields - expected_fields)
        details: list[str] = []
        if missing:
            details.append("missing=" + ", ".join(missing))
        if unexpected:
            details.append("unexpected=" + ", ".join(unexpected))
        raise ReviewPlanTransitionValidationError(
            "review schedule outcome metadata has an invalid field set: "
            + "; ".join(details)
        )
    return outcome


def _validate_failed_schedule_outcome_diagnostics(
    review_plan: Mapping[str, object],
    event: SessionReviewScheduleEvent,
) -> None:
    """Require new failed schedule diagnostics to agree across durable records."""

    outcome_details = _failure_diagnostics(
        _schedule_outcome_metadata(event),
        field_prefix="review_schedule_event.metadata.schedule_outcome",
    )
    plan_details = _failure_diagnostics(
        review_plan,
        field_prefix="review_plan",
    )
    if outcome_details is None and plan_details is None:
        return
    if outcome_details is None:
        raise ReviewPlanTransitionValidationError(
            "failed review plan diagnostics are absent from the schedule journal"
        )
    if plan_details is None:
        raise ReviewPlanTransitionValidationError(
            "failed schedule journal diagnostics are absent from the review plan"
        )
    if plan_details != outcome_details:
        raise ReviewPlanTransitionValidationError(
            "failed schedule diagnostics do not match the review plan"
        )


def _failure_diagnostics(
    values: Mapping[str, object],
    *,
    field_prefix: str,
) -> tuple[str, str] | None:
    """Return an optional complete failure diagnostic pair from durable JSON."""

    present = {
        field_name
        for field_name in _FAILED_SCHEDULE_OUTCOME_DIAGNOSTIC_FIELDS
        if field_name in values
    }
    if not present:
        return None
    if present != _FAILED_SCHEDULE_OUTCOME_DIAGNOSTIC_FIELDS:
        raise ReviewPlanTransitionValidationError(
            f"{field_prefix} failure diagnostics must include both "
            "failure_code and failure_message"
        )
    failure_code = _exact_text(
        values["failure_code"],
        field_name=f"{field_prefix}.failure_code",
    )
    if not failure_code.strip():
        raise ReviewPlanTransitionValidationError(
            f"{field_prefix}.failure_code must not be empty"
        )
    failure_message = _exact_text(
        values["failure_message"],
        field_name=f"{field_prefix}.failure_message",
    )
    return failure_code, failure_message


def _validate_new_schedule_identity(
    schedule: SessionReviewSchedule,
    *,
    plan_id: str,
    plan_revision: int,
) -> None:
    if _plan_id(
        schedule.plan_id,
        field_name="review_schedule.plan_id",
        allow_empty=False,
    ) != plan_id:
        raise ReviewPlanTransitionValidationError(
            "new review schedule does not match aggregate current_plan_id"
        )
    if _positive_integer(
        schedule.plan_revision,
        field_name="review_schedule.plan_revision",
    ) != plan_revision:
        raise ReviewPlanTransitionValidationError(
            "new review schedule revision does not match aggregate plan revision"
        )
    if not isinstance(schedule.status, str):
        raise ReviewPlanTransitionValidationError(
            "new review schedule status must be a string"
        )
    try:
        status = ReviewScheduleStatus(schedule.status)
    except ValueError as exc:
        raise ReviewPlanTransitionValidationError(
            "new review schedule status is invalid"
        ) from exc
    if status is not ReviewScheduleStatus.SCHEDULED:
        raise ReviewPlanTransitionValidationError(
            "a new current review plan must start scheduled"
        )
    if schedule.scheduled_from is not None or schedule.next_review_at is not None:
        raise ReviewPlanTransitionValidationError(
            "new review schedule contains caller-owned clock fields"
        )


def _validate_new_plan_identity(
    review_plan: Mapping[str, object],
    *,
    plan_id: str,
    plan_revision: int,
) -> None:
    _validate_json(review_plan, path="review_plan")
    try:
        payload_plan_id = _plan_id(
            review_plan.get("plan_id", _MISSING),
            field_name="review_plan.plan_id",
            allow_empty=False,
        )
    except ReviewPlanTransitionValidationError as exc:
        raise ReviewPlanTransitionValidationError(
            "review plan payload does not match the aggregate plan fence"
        ) from exc
    if payload_plan_id != plan_id:
        raise ReviewPlanTransitionValidationError(
            "review plan payload does not match the aggregate plan fence"
        )
    if _positive_integer(
        review_plan.get("plan_revision", _MISSING),
        field_name="review_plan.plan_revision",
    ) != plan_revision:
        raise ReviewPlanTransitionValidationError(
            "review plan payload does not match the aggregate plan fence"
        )
    clock_fields = {
        field_name
        for field_name in ("scheduled_from", "next_review_at")
        if field_name in review_plan
    }
    if clock_fields:
        raise ReviewPlanTransitionValidationError(
            "new review plan contains store-owned clock fields: "
            + ", ".join(sorted(clock_fields))
        )


def _validate_new_journal_identity(
    event: SessionReviewScheduleEvent,
    *,
    current_plan_id: str,
    plan_id: str,
    plan_revision: int,
) -> None:
    if _exact_text(
        event.event_type,
        field_name="review_schedule_event.event_type",
    ) != "scheduled":
        raise ReviewPlanTransitionValidationError(
            "new review plan journal must have event_type scheduled"
        )
    if _plan_id(
        event.plan_id,
        field_name="review_schedule_event.plan_id",
        allow_empty=False,
    ) != plan_id:
        raise ReviewPlanTransitionValidationError(
            "new review plan journal does not match aggregate current_plan_id"
        )
    if _plan_id(
        event.previous_plan_id,
        field_name="review_schedule_event.previous_plan_id",
        allow_empty=True,
    ) != current_plan_id:
        raise ReviewPlanTransitionValidationError(
            "new review plan journal changed previous_plan_id"
        )
    metadata = _json_object(
        event.metadata,
        field_name="review_schedule_event.metadata",
    )
    if "plan_revision" not in metadata:
        raise ReviewPlanTransitionValidationError(
            "new review plan journal metadata is missing plan_revision"
        )
    if _positive_integer(
        metadata["plan_revision"],
        field_name="review_schedule_event.metadata.plan_revision",
    ) != plan_revision:
        raise ReviewPlanTransitionValidationError(
            "new review plan journal revision does not match aggregate plan revision"
        )
    if event.scheduled_from is not None or event.next_review_at is not None:
        raise ReviewPlanTransitionValidationError(
            "new review plan journal contains caller-owned clock fields"
        )


def _mapping_text(values: Mapping[str, object], field_name: str) -> str:
    value = values.get(field_name, _MISSING)
    if value is _MISSING:
        return ""
    return _exact_text(value, field_name=f"review_plan.{field_name}")


def _mapping_number(
    values: Mapping[str, object],
    field_name: str,
    *,
    required: bool = False,
) -> int | float | None:
    value = values.get(field_name, _MISSING)
    if value is _MISSING:
        if required:
            raise ReviewPlanTransitionValidationError(
                f"review_plan.{field_name} is required"
            )
        return None
    if value is None and not required:
        return None
    return _finite_number(value, field_name=f"review_plan.{field_name}")


def _mapping_optional_integer(
    values: Mapping[str, object],
    field_name: str,
) -> int | None:
    value = values.get(field_name, _MISSING)
    if value is _MISSING or value is None:
        return None
    return _nonnegative_integer(value, field_name=f"review_plan.{field_name}")


def _mapping_defaulted_integer(
    values: Mapping[str, object],
    field_name: str,
    *,
    default: int,
) -> int:
    value = values.get(field_name, _MISSING)
    if value is _MISSING or value is None:
        return _nonnegative_integer(default, field_name=f"review_plan.{field_name}")
    return _nonnegative_integer(value, field_name=f"review_plan.{field_name}")


def _mapping_json_object(
    values: Mapping[str, object],
    field_name: str,
) -> dict[str, Any]:
    value = values.get(field_name, _MISSING)
    if value is _MISSING:
        return {}
    return _json_object(value, field_name=f"review_plan.{field_name}")


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ReviewPlanTransitionValidationError(f"{field_name} must be a JSON object")
    _validate_json(value, path=field_name)
    return dict(value)


def _validate_json(value: object, *, path: str) -> None:
    try:
        validate_durable_json(value, path=path)
    except DurableJSONValidationError as exc:
        raise ReviewPlanTransitionValidationError(str(exc)) from exc


def _plan_id(value: object, *, field_name: str, allow_empty: bool) -> str:
    result = _exact_text(value, field_name=field_name)
    if result != result.strip():
        raise ReviewPlanTransitionValidationError(
            f"{field_name} must not contain surrounding whitespace"
        )
    if not allow_empty and not result:
        raise ReviewPlanTransitionValidationError(f"{field_name} must not be empty")
    return result


def _exact_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ReviewPlanTransitionValidationError(f"{field_name} must be a JSON string")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise ReviewPlanTransitionValidationError(
            f"{field_name} must contain valid UTF-8 text"
        ) from exc
    return value


def _finite_number(value: object, *, field_name: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReviewPlanTransitionValidationError(
            f"{field_name} must be a finite JSON number"
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise ReviewPlanTransitionValidationError(f"{field_name} must be finite")
    if value < 0:
        raise ReviewPlanTransitionValidationError(f"{field_name} must be non-negative")
    return value


def _optional_number(value: object, *, field_name: str) -> int | float | None:
    if value is None:
        return None
    return _finite_number(value, field_name=field_name)


def _required_optional_number(value: object, *, field_name: str) -> int | float:
    if value is None:
        raise ReviewPlanTransitionValidationError(f"{field_name} is required")
    return _finite_number(value, field_name=field_name)


def _positive_integer(value: object, *, field_name: str) -> int:
    result = _nonnegative_integer(value, field_name=field_name)
    if result < 1:
        raise ReviewPlanTransitionValidationError(
            f"{field_name} must be a positive JSON integer"
        )
    return result


def _nonnegative_integer(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReviewPlanTransitionValidationError(
            f"{field_name} must be a non-negative JSON integer"
        )
    return value


def _optional_integer(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_integer(value, field_name=field_name)


def _defaulted_integer(
    value: object,
    *,
    default: int,
    field_name: str,
) -> int:
    if value is None:
        return _nonnegative_integer(default, field_name=field_name)
    return _nonnegative_integer(value, field_name=field_name)


__all__ = [
    "ReviewPlanTransitionValidationError",
    "validate_session_transition",
    "review_plan_semantics",
    "review_schedule_event_semantics",
    "review_schedule_semantics",
    "validate_review_plan_transition",
]
