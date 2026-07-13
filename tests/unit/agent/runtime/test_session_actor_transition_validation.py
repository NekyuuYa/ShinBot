from __future__ import annotations

from dataclasses import replace
from enum import IntEnum, StrEnum
from typing import Any, cast

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.events import (
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.json_validation import (
    MAX_DURABLE_JSON_DEPTH,
)
from shinbot.agent.runtime.session_actor.transition_validation import (
    ReviewPlanTransitionValidationError,
    validate_review_plan_transition,
)

_KEY = SessionKey("profile-a", "session-a")


def _valid_transition() -> tuple[AgentSessionAggregate, SessionTransition]:
    current = AgentSessionAggregate(key=_KEY)
    plan = {
        "plan_id": "plan-a",
        "plan_revision": 1,
        "applied_delay_seconds": 30.0,
        "trigger": "review_completed",
        "kind": "planned",
        "source": "test-policy",
        "reason": "topic settled",
        "active_reply_threshold": {"mention_score": 0.75},
        "expected_active_epoch": 0,
        "expected_activity_generation": 0,
        "committed_state_revision": 1,
    }
    target = current.advance(
        current_plan_id="plan-a",
        review_plan_revision=1,
        review_plan=plan,
    )
    schedule = SessionReviewSchedule(
        plan_id="plan-a",
        plan_revision=1,
        applied_delay_seconds=30.0,
        trigger="review_completed",
        outcome="planned",
        source="test-policy",
        reason="topic settled",
        active_reply_threshold={"mention_score": 0.75},
        expected_active_epoch=0,
        expected_activity_generation=0,
        committed_state_revision=1,
    )
    journal = SessionReviewScheduleEvent(
        schedule_event_id="plan-a-scheduled",
        event_type="scheduled",
        plan_id="plan-a",
        trigger="review_completed",
        outcome="planned",
        source="test-policy",
        applied_delay_seconds=30.0,
        reason="topic settled",
        expected_active_epoch=0,
        expected_activity_generation=0,
        committed_state_revision=1,
        metadata={
            "plan_revision": 1,
            "schedule_outcome": {
                "active_reply_threshold": {"mention_score": 0.75},
                "applied_delay_seconds": 30.0,
                "fallback_reason": "",
                "kind": "planned",
                "mention_sensitivity": "normal",
                "model_execution_id": "",
                "prompt_signature": "",
                "reason": "topic settled",
                "requested_delay_seconds": None,
                "source": "test-policy",
            },
        },
    )
    return current, SessionTransition(
        aggregate=target,
        disposition="review_planned",
        caused_plan_id="plan-a",
        review_schedules=(schedule,),
        review_schedule_events=(journal,),
    )


def test_valid_plan_schedule_and_journal_form_one_declaration() -> None:
    current, transition = _valid_transition()

    validate_review_plan_transition(current, transition)


def _failed_transition(
    *,
    include_diagnostics: bool = True,
) -> tuple[AgentSessionAggregate, SessionTransition]:
    """Build a coherent failed fallback schedule with optional legacy shape."""

    current, transition = _valid_transition()
    plan = dict(transition.aggregate.review_plan)
    plan.update(
        {
            "kind": "failed",
            "reason": "planner unavailable",
            "fallback_reason": "planner_deadline_reached",
        }
    )
    journal = transition.review_schedule_events[0]
    metadata = dict(journal.metadata)
    schedule_outcome = dict(metadata["schedule_outcome"])
    schedule_outcome.update(
        {
            "kind": "failed",
            "reason": "planner unavailable",
            "fallback_reason": "planner_deadline_reached",
        }
    )
    if include_diagnostics:
        plan.update(
            {
                "failure_code": "planner_deadline_reached",
                "failure_message": "",
            }
        )
        schedule_outcome.update(
            {
                "failure_code": "planner_deadline_reached",
                "failure_message": "",
            }
        )
    metadata["schedule_outcome"] = schedule_outcome
    schedule = replace(
        transition.review_schedules[0],
        outcome="failed",
        reason="planner unavailable",
        fallback_reason="planner_deadline_reached",
    )
    return current, replace(
        transition,
        aggregate=replace(transition.aggregate, review_plan=plan),
        review_schedules=(schedule,),
        review_schedule_events=(
            replace(
                journal,
                outcome="failed",
                reason="planner unavailable",
                fallback_reason="planner_deadline_reached",
                metadata=metadata,
            ),
        ),
    )


@pytest.mark.parametrize("include_diagnostics", (True, False))
def test_failed_schedule_outcome_accepts_current_and_legacy_exact_shapes(
    include_diagnostics: bool,
) -> None:
    """Failed outcomes retain diagnostics without rejecting legacy base records."""

    current, transition = _failed_transition(
        include_diagnostics=include_diagnostics
    )

    validate_review_plan_transition(current, transition)


def test_failed_schedule_diagnostics_must_match_the_aggregate_review_plan() -> None:
    """Failure evidence cannot diverge between current state and its journal."""

    current, transition = _failed_transition()
    plan = dict(transition.aggregate.review_plan)
    plan["failure_code"] = "different_failure"

    with pytest.raises(
        ReviewPlanTransitionValidationError,
        match="failed schedule diagnostics do not match",
    ):
        validate_review_plan_transition(
            current,
            replace(
                transition,
                aggregate=replace(transition.aggregate, review_plan=plan),
            ),
        )


def test_failed_schedule_diagnostics_must_be_a_complete_pair() -> None:
    """A partial failure record cannot become append-only schedule evidence."""

    current, transition = _failed_transition()
    journal = transition.review_schedule_events[0]
    metadata = dict(journal.metadata)
    schedule_outcome = dict(metadata["schedule_outcome"])
    schedule_outcome.pop("failure_message")
    metadata["schedule_outcome"] = schedule_outcome

    with pytest.raises(
        ReviewPlanTransitionValidationError,
        match="invalid field set",
    ):
        validate_review_plan_transition(
            current,
            replace(
                transition,
                review_schedule_events=(replace(journal, metadata=metadata),),
            ),
        )


@pytest.mark.parametrize(
    ("field_name", "value", "expected_message"),
    (
        ("failure_code", "", "failure_code must not be empty"),
        ("failure_code", 1, "failure_code must be a JSON string"),
        ("failure_message", 1, "failure_message must be a JSON string"),
    ),
)
def test_failed_schedule_diagnostics_must_be_well_typed(
    field_name: str,
    value: object,
    expected_message: str,
) -> None:
    """Typed diagnostics prevent opaque failure metadata from entering history."""

    current, transition = _failed_transition()
    journal = transition.review_schedule_events[0]
    metadata = dict(journal.metadata)
    schedule_outcome = dict(metadata["schedule_outcome"])
    schedule_outcome[field_name] = value
    metadata["schedule_outcome"] = schedule_outcome

    with pytest.raises(ReviewPlanTransitionValidationError, match=expected_message):
        validate_review_plan_transition(
            current,
            replace(
                transition,
                review_schedule_events=(replace(journal, metadata=metadata),),
            ),
        )


def test_nonfailed_schedule_rejects_failure_diagnostics() -> None:
    """Diagnostics remain exclusive to an explicit failed scheduling outcome."""

    current, transition = _valid_transition()
    journal = transition.review_schedule_events[0]
    metadata = dict(journal.metadata)
    schedule_outcome = dict(metadata["schedule_outcome"])
    schedule_outcome.update(
        {
            "failure_code": "should_not_be_here",
            "failure_message": "",
        }
    )
    metadata["schedule_outcome"] = schedule_outcome

    with pytest.raises(ReviewPlanTransitionValidationError, match="invalid field set"):
        validate_review_plan_transition(
            current,
            replace(
                transition,
                review_schedule_events=(replace(journal, metadata=metadata),),
            ),
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("kind", "defaulted"),
        ("applied_delay_seconds", 31.0),
        ("mention_sensitivity", "high"),
        ("active_reply_threshold", {"mention_score": 0.9}),
    ),
)
def test_schedule_outcome_metadata_cannot_diverge_from_schedule(
    field_name: str,
    value: object,
) -> None:
    current, transition = _valid_transition()
    journal = transition.review_schedule_events[0]
    metadata = dict(journal.metadata)
    schedule_outcome = dict(metadata["schedule_outcome"])
    schedule_outcome[field_name] = value
    metadata["schedule_outcome"] = schedule_outcome

    with pytest.raises(
        ReviewPlanTransitionValidationError,
        match="(outcome metadata|journal does not match)",
    ):
        validate_review_plan_transition(
            current,
            replace(
                transition,
                review_schedule_events=(replace(journal, metadata=metadata),),
            ),
        )


@pytest.mark.parametrize("mutation", ("missing", "extra", "absent"))
def test_schedule_outcome_metadata_requires_one_exact_decision_record(
    mutation: str,
) -> None:
    current, transition = _valid_transition()
    journal = transition.review_schedule_events[0]
    metadata = dict(journal.metadata)
    if mutation == "absent":
        metadata.pop("schedule_outcome")
    else:
        schedule_outcome = dict(metadata["schedule_outcome"])
        if mutation == "missing":
            schedule_outcome.pop("mention_sensitivity")
        else:
            schedule_outcome["alternate_kind"] = "defaulted"
        metadata["schedule_outcome"] = schedule_outcome

    with pytest.raises(ReviewPlanTransitionValidationError):
        validate_review_plan_transition(
            current,
            replace(
                transition,
                review_schedule_events=(replace(journal, metadata=metadata),),
            ),
        )


@pytest.mark.parametrize("event_type", ("scheduled", " scheduled "))
def test_same_plan_cannot_append_another_scheduled_journal(event_type: str) -> None:
    _current, transition = _valid_transition()
    current = transition.aggregate
    journal = replace(transition.review_schedule_events[0], event_type=event_type)
    duplicate = SessionTransition(
        aggregate=current,
        disposition="duplicate_schedule_evidence",
        review_schedule_events=(journal,),
    )

    with pytest.raises(ReviewPlanTransitionValidationError):
        validate_review_plan_transition(current, duplicate)


@pytest.mark.parametrize(
    "event_type",
    ("completed", "deferred", "due_superseded", "superseded"),
)
def test_same_plan_allows_non_creation_journal_events(event_type: str) -> None:
    _current, transition = _valid_transition()
    current = transition.aggregate
    journal = replace(transition.review_schedule_events[0], event_type=event_type)

    validate_review_plan_transition(
        current,
        SessionTransition(
            aggregate=current,
            disposition="same_plan_journal",
            review_schedule_events=(journal,),
        ),
    )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("applied_delay_seconds", "30"),
        ("applied_delay_seconds", True),
        ("applied_delay_seconds", [30]),
        ("trigger", False),
        ("kind", {"value": "planned"}),
        ("expected_active_epoch", "0"),
        ("expected_active_epoch", 0.9),
        ("expected_activity_generation", True),
        ("active_reply_threshold", [0.75]),
    ),
)
def test_review_plan_semantics_reject_type_confusion(
    field_name: str,
    value: object,
) -> None:
    current, transition = _valid_transition()
    plan = dict(transition.aggregate.review_plan)
    plan[field_name] = value
    malformed = replace(
        transition,
        aggregate=replace(transition.aggregate, review_plan=plan),
    )

    with pytest.raises(ReviewPlanTransitionValidationError):
        validate_review_plan_transition(current, malformed)


def test_schedule_and_journal_text_fields_are_not_stringified() -> None:
    current, transition = _valid_transition()
    schedule = replace(transition.review_schedules[0], trigger=False)  # type: ignore[arg-type]
    journal = replace(
        transition.review_schedule_events[0],
        source={"name": "test-policy"},  # type: ignore[arg-type]
    )

    with pytest.raises(ReviewPlanTransitionValidationError):
        validate_review_plan_transition(
            current,
            replace(transition, review_schedules=(schedule,)),
        )
    with pytest.raises(ReviewPlanTransitionValidationError):
        validate_review_plan_transition(
            current,
            replace(transition, review_schedule_events=(journal,)),
        )


def test_transition_plan_identity_is_not_stringified() -> None:
    _current, transition = _valid_transition()

    with pytest.raises(TypeError, match="caused_plan_id must be a string"):
        replace(transition, caused_plan_id=["plan-a"])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "kwargs",
    (
        {"plan_revision": "1"},
        {"plan_revision": 1.5},
        {"plan_revision": True},
        {"applied_delay_seconds": "30"},
        {"applied_delay_seconds": True},
        {"expected_active_epoch": "0"},
        {"expected_active_epoch": 0.5},
        {"expected_active_epoch": True},
    ),
)
def test_schedule_model_rejects_non_json_integer_and_number_types(
    kwargs: dict[str, object],
) -> None:
    values: dict[str, Any] = {
        "plan_id": "plan-a",
        "plan_revision": 1,
        "applied_delay_seconds": 30.0,
        **kwargs,
    }

    with pytest.raises((TypeError, ValueError)):
        SessionReviewSchedule(**values)


def test_review_plan_rejects_non_string_keys_before_freezing() -> None:
    review_plan = cast(
        dict[str, Any],
        {"plan_id": "plan-a", "plan_revision": 1, 1: "coerced-before"},
    )

    with pytest.raises((TypeError, ValueError), match="keys must be JSON strings"):
        AgentSessionAggregate(
            key=_KEY,
            current_plan_id="plan-a",
            review_plan_revision=1,
            review_plan=review_plan,
        )


@pytest.mark.parametrize("invalid_shape", ("depth", "utf8"))
def test_review_plan_validation_is_bounded_utf8_json(invalid_shape: str) -> None:
    plan: dict[str, Any] = {"plan_id": "plan-a", "plan_revision": 1}
    if invalid_shape == "depth":
        nested: object = None
        for _ in range(MAX_DURABLE_JSON_DEPTH + 1):
            nested = {"child": nested}
        plan["nested"] = nested
    else:
        plan["reason"] = "\ud800"

    with pytest.raises(ValueError):
        AgentSessionAggregate(
            key=_KEY,
            current_plan_id="plan-a",
            review_plan_revision=1,
            review_plan=plan,
        )


class _Revision(IntEnum):
    ONE = 1


class _PlanId(StrEnum):
    PLAN_A = "plan-a"


def test_valid_string_and_integer_enums_remain_supported() -> None:
    schedule = SessionReviewSchedule(
        plan_id=_PlanId.PLAN_A,
        plan_revision=_Revision.ONE,
        applied_delay_seconds=_Revision.ONE,
    )
    aggregate = AgentSessionAggregate(
        key=_KEY,
        current_plan_id=_PlanId.PLAN_A,
        review_plan_revision=_Revision.ONE,
        review_plan={"plan_id": _PlanId.PLAN_A, "plan_revision": _Revision.ONE},
    )

    assert schedule.plan_revision == 1
    assert aggregate.current_plan_id == "plan-a"
