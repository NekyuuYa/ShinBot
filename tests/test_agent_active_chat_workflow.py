from __future__ import annotations

import asyncio

import pytest

from shinbot.agent.active_chat import (
    ActiveChatActionKind,
    ActiveChatAttention,
    ActiveChatAttentionConfig,
    ActiveChatBatch,
    ActiveChatMessageSignal,
    ActiveChatNoReplyIntensity,
    ActiveChatReplyIntensity,
    ActiveChatRoundResult,
    ActiveChatWorkflow,
    interest_effect_for_round,
)
from shinbot.agent.scheduler import ActiveChatState


class RecordingScheduler:
    def __init__(self) -> None:
        self.consumed: list[tuple[str, list[int]]] = []
        self.adjustments: list[dict[str, object]] = []

    def mark_active_chat_consumed(
        self,
        session_id: str,
        message_log_ids: list[int],
    ) -> list[object]:
        self.consumed.append((session_id, list(message_log_ids)))
        return []

    def adjust_active_chat_interest(
        self,
        session_id: str,
        *,
        delta: float = 0.0,
        force_exit: bool = False,
        reason: str = "",
    ) -> object:
        self.adjustments.append(
            {
                "session_id": session_id,
                "delta": delta,
                "force_exit": force_exit,
                "reason": reason,
            }
        )
        return object()


def make_active_state(*, interest_value: float = 30.0) -> ActiveChatState:
    return ActiveChatState(
        session_id="bot:group:room",
        interest_value=interest_value,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
    )


def make_signal(**kwargs) -> ActiveChatMessageSignal:
    values = {
        "session_id": "bot:group:room",
        "message_log_id": 1,
        "sender_id": "user-1",
        "response_profile": "balanced",
        "self_platform_id": "bot-self",
    }
    values.update(kwargs)
    return ActiveChatMessageSignal(**values)


def test_active_chat_attention_contribution_rules() -> None:
    attention = ActiveChatAttention()

    assert attention.contribution_for(make_signal()) == 1.0
    assert attention.contribution_for(make_signal(is_mentioned=True)) == 4.0
    assert attention.contribution_for(make_signal(is_reply_to_bot=True)) == 3.0
    assert attention.contribution_for(make_signal(is_mention_to_other=True)) == 0.5
    assert attention.contribution_for(make_signal(is_poke_to_bot=True)) == 0.8
    assert attention.contribution_for(make_signal(is_poke_to_other=True)) == 0.2
    assert (
        attention.contribution_for(make_signal(sender_id="bot-self", is_mentioned=True))
        == 0.0
    )


def test_active_chat_attention_threshold_uses_interest() -> None:
    attention = ActiveChatAttention()

    assert attention.effective_threshold(60.0) == 2.5
    assert attention.effective_threshold(30.0) == 5.0
    assert attention.effective_threshold(10.0) == 15.0
    assert attention.effective_threshold(0.0) == 15.0


def test_active_chat_interest_effect_maps_round_actions() -> None:
    assert (
        interest_effect_for_round(
            ActiveChatRoundResult(action=ActiveChatActionKind.NO_REPLY)
        ).delta
        == -5.0
    )
    assert (
        interest_effect_for_round(
            ActiveChatRoundResult(
                action=ActiveChatActionKind.NO_REPLY,
                no_reply_intensity=ActiveChatNoReplyIntensity.STRONG,
            )
        ).delta
        == -10.0
    )
    assert (
        interest_effect_for_round(
            ActiveChatRoundResult(action=ActiveChatActionKind.SEND_POKE)
        ).delta
        == 3.0
    )
    assert (
        interest_effect_for_round(
            ActiveChatRoundResult(
                action=ActiveChatActionKind.SEND_REPLY,
                reply_intensity=ActiveChatReplyIntensity.ENGAGED,
            )
        ).delta
        == 10.0
    )
    assert (
        interest_effect_for_round(
            ActiveChatRoundResult(action=ActiveChatActionKind.EXIT_ACTIVE, reason="done")
        ).force_exit
        is True
    )


@pytest.mark.asyncio
async def test_active_chat_workflow_start_initializes_session_without_llm_round() -> None:
    batches: list[ActiveChatBatch] = []

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        batches.append(batch)
        return ActiveChatRoundResult(success=True)

    workflow = ActiveChatWorkflow(round_handler=handler, now=lambda: 10.0)

    result = await workflow.start_active_chat(
        session_id="bot:group:room",
        active_chat_state=make_active_state(),
        review_result_summary={"reason": "review_done"},
    )

    assert result.accepted is True
    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert state.active_epoch == 0
    assert state.review_result_summary == {"reason": "review_done"}
    assert state.pending_buffer == []
    assert batches == []


@pytest.mark.asyncio
async def test_active_chat_workflow_flushes_after_semantic_wait() -> None:
    batches: list[ActiveChatBatch] = []

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        batches.append(batch)
        return ActiveChatRoundResult(success=True, reason="ok")

    scheduler = RecordingScheduler()
    workflow = ActiveChatWorkflow(
        attention=ActiveChatAttention(
            ActiveChatAttentionConfig(
                base_threshold=2.0,
                reference_interest=30.0,
                semantic_wait_ms=1.0,
            )
        ),
        round_handler=handler,
        now=lambda: 10.0,
    )
    await workflow.start_active_chat(
        session_id="bot:group:room",
        active_chat_state=make_active_state(),
        review_result_summary={"summary": "review handoff"},
    )

    result = await workflow.notify_message(
        scheduler=scheduler,
        session_id="bot:group:room",
        message_log_id=1,
        sender_id="user-1",
        response_profile="balanced",
        is_mentioned=True,
        is_reply_to_bot=False,
        is_mention_to_other=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        self_platform_id="bot-self",
        active_chat_state=make_active_state(),
    )
    await asyncio.sleep(0.05)

    assert result.triggered is True
    assert result.timer_started is True
    assert [batch.message_log_ids for batch in batches] == [[1]]
    assert batches[0].review_result_summary == {"summary": "review handoff"}
    assert scheduler.consumed == [("bot:group:room", [1])]
    assert scheduler.adjustments == [
        {
            "session_id": "bot:group:room",
            "delta": 0.0,
            "force_exit": False,
            "reason": "ok",
        }
    ]
    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert state.pending_buffer == []
    assert state.accumulated == pytest.approx(1.0)
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_keeps_pending_without_handler() -> None:
    scheduler = RecordingScheduler()
    workflow = ActiveChatWorkflow(
        attention=ActiveChatAttention(
            ActiveChatAttentionConfig(
                base_threshold=2.0,
                reference_interest=30.0,
                semantic_wait_ms=1.0,
            )
        ),
        now=lambda: 10.0,
    )

    await workflow.notify_message(
        scheduler=scheduler,
        session_id="bot:group:room",
        message_log_id=1,
        sender_id="user-1",
        response_profile="balanced",
        is_mentioned=True,
        is_reply_to_bot=False,
        is_mention_to_other=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        self_platform_id="bot-self",
        active_chat_state=make_active_state(),
    )
    await asyncio.sleep(0.05)

    assert scheduler.consumed == []
    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert [message.message_log_id for message in state.pending_buffer] == [1]
    await workflow.shutdown()
