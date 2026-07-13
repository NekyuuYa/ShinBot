"""Tests for durable Actor v2 idle-review planner context projection."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.idle_review_planning import (
    IdleReviewPlanningInput,
)
from shinbot.agent.runtime.session_actor.idle_review_planning_adapter import (
    IdleReviewPlanningEffectInput,
    IdleReviewPlanningWorkflowRequest,
)
from shinbot.agent.runtime.session_actor.idle_review_planning_context import (
    ActorIdleReviewPlanningContextProjector,
    IdleReviewPlanningContextConfig,
    IdleReviewPlanningContextError,
)
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    MessageLedgerEntry,
    MessagePriorityFlags,
)

_KEY = SessionKey("profile-a", "profile-a:group:room-a")


@dataclass(slots=True)
class _Ledger:
    """Read-only ledger fake used to prove projector fence handling."""

    entries: tuple[MessageLedgerEntry, ...]

    async def list_message_ledger(
        self,
        key: SessionKey,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Return the configured ledger after checking its actor key."""

        assert key == _KEY
        return self.entries


@dataclass(slots=True)
class _MessageStore:
    """Message-log fake that records exactly which authorized IDs were read."""

    payloads: dict[int, dict[str, object]]
    requested_ids: list[tuple[int, ...]]

    def list_by_ids(self, message_log_ids: tuple[int, ...]) -> list[dict[str, object]]:
        """Return rows in reverse order to prove ledger-order restoration."""

        self.requested_ids.append(message_log_ids)
        return [
            self.payloads[message_log_id]
            for message_log_id in reversed(message_log_ids)
            if message_log_id in self.payloads
        ]


def _entry(
    message_log_id: int,
    *,
    ledger_sequence: int,
    ownership_generation: int = 1,
    eligible_for_work: bool = True,
    base_session_id: str = "instance-a:group:room-a",
) -> MessageLedgerEntry:
    """Create one durable actor ledger row."""

    message = AppendMessageLedgerEntry(
        key=_KEY,
        message_log_id=message_log_id,
        ownership_generation=ownership_generation,
        source_event_id=f"message:{message_log_id}",
        actor_event_id=f"message:{message_log_id}",
        delivery_version=1,
        event_source="agent_route_relay",
        sender_id="user-a",
        instance_id="instance-a",
        event_type="message-created",
        bot_id="bot-a",
        bot_session_id=_KEY.session_id,
        base_session_id=base_session_id,
        platform="test",
        self_id="bot-a",
        eligible_for_work=eligible_for_work,
        suppression_reason="test_suppressed" if not eligible_for_work else "",
        priority=MessagePriorityFlags(),
        observed_at=float(ledger_sequence),
        occurred_at=float(ledger_sequence),
        event_created_at=float(ledger_sequence),
    )
    return MessageLedgerEntry(
        message=message,
        ledger_sequence=ledger_sequence,
        recorded_at=float(ledger_sequence),
        updated_at=float(ledger_sequence),
    )


def _request(
    *,
    input_watermark: int = 20,
    input_ledger_sequence: int | None = None,
) -> IdleReviewPlanningWorkflowRequest:
    """Build one trusted planner request independent of legacy scheduler state."""

    descriptor = IdleReviewPlanningInput(
        input_watermark=input_watermark,
        active_epoch=3,
        activity_generation=6,
        trigger="active_chat_decay",
        active_chat_interest=5.0,
        active_chat_tick_count=2,
    )
    return IdleReviewPlanningWorkflowRequest(
        effect=IdleReviewPlanningEffectInput(
            key=_KEY,
            operation_id="operation-a",
            plan_id="plan-a",
            effect_id="effect-a",
            idempotency_key="effect-a",
            source_event_id="exit-a",
            ownership_generation=1,
            active_epoch=3,
            activity_generation=6,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            trigger="active_chat_decay",
            source="session_actor",
            planning_input=descriptor,
        )
    )


@pytest.mark.asyncio
async def test_projector_uses_ledger_order_and_excludes_late_or_old_owner_rows() -> None:
    """Only ledger rows inside the immutable actor fence reach model context."""

    entries = (
        _entry(10, ledger_sequence=1),
        _entry(11, ledger_sequence=2),
        _entry(12, ledger_sequence=3, ownership_generation=2),
        _entry(21, ledger_sequence=4),
    )
    message_store = _MessageStore(
        payloads={
            10: {
                "id": 10,
                "session_id": "instance-a:group:room-a",
                "raw_text": "first",
            },
            11: {
                "id": 11,
                "session_id": "instance-a:group:room-a",
                "raw_text": "second",
            },
        },
        requested_ids=[],
    )
    projector = ActorIdleReviewPlanningContextProjector(
        ledger=_Ledger(entries),
        message_store=message_store,
        config=IdleReviewPlanningContextConfig(max_messages=2),
    )

    stage_input = await projector.build_idle_review_planning_stage_input(_request())

    assert message_store.requested_ids == [(10, 11)]
    assert [message["id"] for message in stage_input.source_messages] == [10, 11]
    assert stage_input.session_id == _KEY.session_id
    assert stage_input.purpose == "idle_review_planning"
    assert stage_input.metadata["actor_v2"] is True
    assert stage_input.metadata["ledger_message_log_ids"] == [10, 11]
    assert stage_input.metadata["planning_input"] == _request().effect.planning_input.to_payload()


@pytest.mark.asyncio
async def test_projector_honors_ledger_sequence_when_the_effect_captures_one() -> None:
    """A late old message cannot enter a planner snapshot with a sequence fence."""

    entries = (_entry(10, ledger_sequence=1), _entry(11, ledger_sequence=3))
    message_store = _MessageStore(
        payloads={
            10: {
                "id": 10,
                "session_id": "instance-a:group:room-a",
                "raw_text": "first",
            }
        },
        requested_ids=[],
    )
    projector = ActorIdleReviewPlanningContextProjector(
        ledger=_Ledger(entries),
        message_store=message_store,
    )

    stage_input = await projector.build_idle_review_planning_stage_input(
        _request(input_ledger_sequence=2)
    )

    assert message_store.requested_ids == [(10,)]
    assert [message["id"] for message in stage_input.source_messages] == [10]


@pytest.mark.asyncio
async def test_projector_fails_when_an_authorized_log_row_disappears() -> None:
    """A missing immutable message log fails the effect instead of widening context."""

    projector = ActorIdleReviewPlanningContextProjector(
        ledger=_Ledger((_entry(10, ledger_sequence=1),)),
        message_store=_MessageStore(payloads={}, requested_ids=[]),
    )

    with pytest.raises(
        IdleReviewPlanningContextError,
        match="message logs disappeared: 10",
    ):
        await projector.build_idle_review_planning_stage_input(_request())


@pytest.mark.asyncio
async def test_projector_excludes_ineligible_ledger_rows() -> None:
    """Suppressed messages cannot re-enter an Actor v2 planner prompt."""

    projector = ActorIdleReviewPlanningContextProjector(
        ledger=_Ledger(
            (
                _entry(10, ledger_sequence=1, eligible_for_work=False),
                _entry(11, ledger_sequence=2),
            )
        ),
        message_store=_MessageStore(
            payloads={
                11: {
                    "id": 11,
                    "session_id": "instance-a:group:room-a",
                    "raw_text": "eligible",
                }
            },
            requested_ids=[],
        ),
    )

    stage_input = await projector.build_idle_review_planning_stage_input(_request())

    assert [message["id"] for message in stage_input.source_messages] == [11]


@pytest.mark.asyncio
async def test_projector_rejects_message_log_from_another_session() -> None:
    """A corrupt ledger/message join must fail before reaching the model."""

    projector = ActorIdleReviewPlanningContextProjector(
        ledger=_Ledger((_entry(10, ledger_sequence=1),)),
        message_store=_MessageStore(
            payloads={
                10: {"id": 10, "session_id": "other-session", "raw_text": "wrong"}
            },
            requested_ids=[],
        ),
    )

    with pytest.raises(
        IdleReviewPlanningContextError,
        match="message log session mismatch: 10",
    ):
        await projector.build_idle_review_planning_stage_input(_request())


@pytest.mark.asyncio
async def test_projector_rejects_missing_ledger_session_identity() -> None:
    """A corrupt ledger cannot bypass the message-log session boundary."""

    projector = ActorIdleReviewPlanningContextProjector(
        ledger=_Ledger((_entry(10, ledger_sequence=1, base_session_id=""),)),
        message_store=_MessageStore(payloads={}, requested_ids=[]),
    )

    with pytest.raises(
        IdleReviewPlanningContextError,
        match="omitted base session identity: 10",
    ):
        await projector.build_idle_review_planning_stage_input(_request())
