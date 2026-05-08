from __future__ import annotations

import asyncio

import pytest

from shinbot.agent.active_chat import (
    ActiveChatAttention,
    ActiveChatAttentionConfig,
    ActiveChatBatch,
    ActiveChatMessageSignal,
    ActiveChatRoundResult,
    ActiveChatWorkflow,
)
from shinbot.agent.scheduler import ActiveChatState


class RecordingScheduler:
    def __init__(self) -> None:
        self.consumed: list[tuple[str, list[int]]] = []

    def mark_active_chat_consumed(
        self,
        session_id: str,
        message_log_ids: list[int],
    ) -> list[object]:
        self.consumed.append((session_id, list(message_log_ids)))
        return []


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
    assert scheduler.consumed == [("bot:group:room", [1])]
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
