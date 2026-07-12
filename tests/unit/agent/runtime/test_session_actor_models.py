from __future__ import annotations

import math

import pytest

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
)


def _key() -> SessionKey:
    return SessionKey("profile-a", "bot:group:room")


def _event() -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id="event-1",
        key=_key(),
        kind="MessageReceived",
    )


@pytest.mark.parametrize("field_name", ["occurred_at", "available_at", "created_at"])
@pytest.mark.parametrize("invalid", [-1.0, math.inf, -math.inf, math.nan])
def test_session_event_rejects_invalid_durable_timestamps(
    field_name: str,
    invalid: float,
) -> None:
    with pytest.raises(ValueError, match=rf"{field_name}.*finite and non-negative"):
        SessionEventEnvelope(
            event_id="event-1",
            key=_key(),
            kind="MessageReceived",
            **{field_name: invalid},
        )


@pytest.mark.parametrize("invalid", [-1.0, math.inf, -math.inf, math.nan])
def test_aggregate_rejects_invalid_timestamps_and_nonfinite_json(
    invalid: float,
) -> None:
    with pytest.raises(ValueError, match="updated_at.*finite and non-negative"):
        AgentSessionAggregate(key=_key(), updated_at=invalid)
    if invalid < 0:
        return
    with pytest.raises(ValueError, match="aggregate numbers must be finite"):
        AgentSessionAggregate(key=_key(), data={"deadline_at": invalid})


@pytest.mark.parametrize("invalid", [-1.0, math.inf, -math.inf, math.nan])
def test_effect_rejects_invalid_availability(invalid: float) -> None:
    with pytest.raises(ValueError, match="available_at.*finite and non-negative"):
        SessionEffect(
            effect_id="effect-1",
            kind="run",
            contract_signature="test-run-v1",
            available_at=invalid,
        )


@pytest.mark.parametrize(
    "field_name",
    ["claimed_at", "lease_expires_at"],
)
@pytest.mark.parametrize("invalid", [-1.0, math.inf, -math.inf, math.nan])
def test_claim_rejects_invalid_timestamps(field_name: str, invalid: float) -> None:
    with pytest.raises(ValueError, match=rf"{field_name}.*finite and non-negative"):
        ClaimedSessionEvent(
            claim_id="claim-1",
            envelope=_event(),
            worker_id="worker-1",
            **{field_name: invalid},
        )


@pytest.mark.parametrize(
    "field_name",
    ["started_at", "lease_until", "superseded_at", "finished_at"],
)
@pytest.mark.parametrize("invalid", [-1.0, math.inf, -math.inf, math.nan])
def test_operation_rejects_invalid_timestamps(
    field_name: str,
    invalid: float,
) -> None:
    with pytest.raises(ValueError, match=rf"{field_name}.*finite and non-negative"):
        SessionOperation(
            operation_id="operation-1",
            kind="review",
            **{field_name: invalid},
        )


def test_operation_input_ledger_sequence_requires_a_watermark() -> None:
    with pytest.raises(ValueError, match="requires a captured input_watermark"):
        SessionOperation(
            operation_id="operation-1",
            kind="review",
            input_ledger_sequence=1,
        )

    unresolved = SessionOperation(
        operation_id="operation-2",
        kind="review",
        input_watermark=10,
    )
    assert unresolved.input_ledger_sequence is None

    with pytest.raises(TypeError, match="input_ledger_sequence must be an integer"):
        SessionOperation(
            operation_id="operation-3",
            kind="review",
            input_watermark=10,
            input_ledger_sequence=True,
        )


@pytest.mark.parametrize(
    "field_name",
    [
        "applied_delay_seconds",
        "requested_delay_seconds",
        "available_at",
        "claim_until",
        "created_at",
        "updated_at",
    ],
)
@pytest.mark.parametrize("invalid", [-1.0, math.inf, -math.inf, math.nan])
def test_review_schedule_rejects_invalid_timing(
    field_name: str,
    invalid: float,
) -> None:
    values = {
        "plan_id": "plan-1",
        "plan_revision": 1,
        "applied_delay_seconds": 30.0,
        field_name: invalid,
    }
    with pytest.raises(ValueError, match=rf"{field_name}.*finite and non-negative"):
        SessionReviewSchedule(**values)


@pytest.mark.parametrize(
    "field_name",
    [
        "requested_delay_seconds",
        "applied_delay_seconds",
        "scheduled_from",
        "next_review_at",
        "created_at",
    ],
)
@pytest.mark.parametrize("invalid", [-1.0, math.inf, -math.inf, math.nan])
def test_review_schedule_event_rejects_invalid_timing(
    field_name: str,
    invalid: float,
) -> None:
    with pytest.raises(ValueError, match=rf"{field_name}.*finite and non-negative"):
        SessionReviewScheduleEvent(
            schedule_event_id="schedule-event-1",
            event_type="scheduled",
            **{field_name: invalid},
        )
