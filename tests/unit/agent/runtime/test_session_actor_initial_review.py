"""Initial review-plan tests for the durable Agent session reducer."""

from __future__ import annotations

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


def _message_event(
    *,
    event_id: str,
    message_log_id: int,
    occurred_at: float | None = None,
    is_mentioned: bool = False,
    already_handled: bool = False,
    is_stopped: bool = False,
    sender_id: str = "user-a",
) -> SessionEventEnvelope:
    event_time = float(message_log_id if occurred_at is None else occurred_at)
    return SessionEventEnvelope(
        event_id=event_id,
        key=_KEY,
        kind=AgentSessionEventKind.MESSAGE_RECEIVED,
        ownership_generation=1,
        source="agent_route_outbox",
        occurred_at=event_time,
        created_at=event_time,
        correlation_id=f"correlation:{event_id}",
        trace_id=f"trace:{event_id}",
        payload={
            "version": 1,
            "event_id": event_id,
            "session_key": {
                "profile_id": _KEY.profile_id,
                "session_id": _KEY.session_id,
            },
            "bot_id": _KEY.profile_id,
            "bot_binding_id": "binding-a",
            "base_session_id": "instance-a:group:room-a",
            "bot_session_id": _KEY.session_id,
            "message_log_id": message_log_id,
            "sender_id": sender_id,
            "instance_id": "instance-a",
            "platform": "test",
            "self_id": "bot-a",
            "is_private": False,
            "is_mentioned": is_mentioned,
            "is_mention_to_other": False,
            "is_reply_to_bot": False,
            "is_poke_to_bot": False,
            "is_poke_to_other": False,
            "already_handled": already_handled,
            "is_stopped": is_stopped,
            "trace_id": f"trace:{event_id}",
            "observed_at": event_time,
            "event_type": "message-created",
            "response_profile": "balanced",
        },
    )


def _virgin() -> AgentSessionAggregate:
    return AgentSessionAggregate(
        key=_KEY,
        ownership_generation=1,
    )


def test_first_actionable_message_creates_one_deterministic_defaulted_plan() -> None:
    reducer = AgentSessionReducer(
        config=IdleExitReducerConfig(
            default_review_delay_seconds=75.0,
            default_review_reason=" first idle review ",
        )
    )
    event = _message_event(event_id="message:10", message_log_id=10)

    transition = reducer.reduce(_virgin(), event)

    assert transition == reducer.reduce(_virgin(), event)
    assert transition.disposition == "message_recorded"
    assert transition.aggregate.state == AgentSessionState.IDLE
    assert transition.aggregate.state_revision == 1
    assert transition.aggregate.event_sequence == 1
    assert transition.aggregate.review_plan_revision == 1
    assert transition.caused_plan_id == transition.aggregate.current_plan_id
    assert transition.aggregate.current_plan_id.startswith("initial-review-plan:")
    assert transition.aggregate.review_plan == {
        "plan_id": transition.aggregate.current_plan_id,
        "plan_revision": 1,
        "trigger": "initial_message",
        "applied_delay_seconds": 75.0,
        "reason": "first idle review",
        "requested_delay_seconds": None,
        "fallback_reason": "",
        "mention_sensitivity": "normal",
        "active_reply_threshold": {},
        "model_execution_id": "",
        "prompt_signature": "",
        "source": "initial_review_policy",
        "kind": "defaulted",
        "expected_active_epoch": 0,
        "expected_activity_generation": 0,
        "committed_state_revision": 1,
    }
    assert len(transition.review_schedules) == 1
    schedule = transition.review_schedules[0]
    assert schedule.plan_id == transition.aggregate.current_plan_id
    assert schedule.plan_revision == 1
    assert schedule.outcome == "defaulted"
    assert schedule.applied_delay_seconds == 75.0
    assert schedule.reason == "first idle review"
    assert schedule.scheduled_from is None
    assert schedule.next_review_at is None
    assert len(transition.review_schedule_events) == 1
    schedule_event = transition.review_schedule_events[0]
    assert schedule_event.event_type == "scheduled"
    assert schedule_event.plan_id == schedule.plan_id
    assert schedule_event.schedule_event_id.startswith(
        "initial-review-schedule-event:"
    )


@pytest.mark.parametrize(
    "event",
    [
        _message_event(
            event_id="message:handled",
            message_log_id=10,
            already_handled=True,
        ),
        _message_event(
            event_id="message:stopped",
            message_log_id=11,
            is_stopped=True,
        ),
        _message_event(
            event_id="message:self",
            message_log_id=12,
            sender_id="bot-a",
        ),
    ],
)
def test_suppressed_first_message_does_not_create_review_plan(
    event: SessionEventEnvelope,
) -> None:
    transition = AgentSessionReducer().reduce(_virgin(), event)

    assert transition.disposition == "message_recorded_suppressed"
    assert transition.aggregate.current_plan_id == ""
    assert transition.aggregate.review_plan_revision == 0
    assert transition.review_schedules == ()
    assert transition.review_schedule_events == ()


def test_late_first_actionable_message_advances_revision_when_delivery_data_is_unchanged(
) -> None:
    reducer = AgentSessionReducer()
    suppressed = reducer.reduce(
        _virgin(),
        _message_event(
            event_id="message:suppressed-high",
            message_log_id=100,
            occurred_at=100.0,
            is_stopped=True,
        ),
    )
    actionable = reducer.reduce(
        suppressed.aggregate,
        _message_event(
            event_id="message:actionable-low",
            message_log_id=10,
            occurred_at=110.0,
        ),
    )

    assert actionable.aggregate.data == suppressed.aggregate.data
    assert actionable.aggregate.state_revision == suppressed.aggregate.state_revision + 1
    assert actionable.aggregate.review_plan_revision == 1
    assert len(actionable.review_schedules) == 1


def test_existing_review_plan_is_not_replaced_by_later_message() -> None:
    reducer = AgentSessionReducer()
    first = reducer.reduce(
        _virgin(),
        _message_event(event_id="message:first", message_log_id=10),
    )

    second = reducer.reduce(
        first.aggregate,
        _message_event(event_id="message:second", message_log_id=11),
    )

    assert second.aggregate.current_plan_id == first.aggregate.current_plan_id
    assert second.aggregate.review_plan_revision == 1
    assert second.review_schedules == ()
    assert second.review_schedule_events == ()


def test_first_priority_message_binds_active_reply_to_initial_plan() -> None:
    transition = AgentSessionReducer().reduce(
        _virgin(),
        _message_event(
            event_id="message:mention",
            message_log_id=10,
            is_mentioned=True,
        ),
    )

    assert transition.disposition == "active_reply_started"
    assert transition.aggregate.state == AgentSessionState.ACTIVE_REPLY
    assert transition.aggregate.review_plan_revision == 1
    assert len(transition.review_schedules) == 1
    operation_id = transition.aggregate.active_reply_operation_id
    fence = transition.aggregate.data["operation_fences"][operation_id]
    assert fence["plan_id"] == transition.aggregate.current_plan_id
    assert transition.effects[0].payload["plan_id"] == transition.aggregate.current_plan_id
    assert transition.caused_plan_id == transition.aggregate.current_plan_id


def test_default_review_reason_must_be_a_nonempty_string() -> None:
    with pytest.raises(TypeError, match="must be a string"):
        IdleExitReducerConfig(default_review_reason=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must not be empty"):
        IdleExitReducerConfig(default_review_reason="  ")
