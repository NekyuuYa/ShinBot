from __future__ import annotations

from active_chat_workflow_support import (
    ActiveChatActionKind,
    ActiveChatAttention,
    ActiveChatAttentionConfig,
    ActiveChatBatch,
    ActiveChatCoordinator,
    ActiveChatNoReplyIntensity,
    ActiveChatReplyIntensity,
    ActiveChatRoundResult,
    RecordingScheduler,
    ReviewPlan,
    asyncio,
    interest_effect_for_round,
    make_active_state,
    make_signal,
    pytest,
    start_workflow,
)


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

    workflow = ActiveChatCoordinator(round_handler=handler, now=lambda: 10.0)

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
async def test_active_chat_workflow_skips_self_platform_messages() -> None:
    scheduler = RecordingScheduler()
    workflow = ActiveChatCoordinator(
        attention=ActiveChatAttention(
            ActiveChatAttentionConfig(
                base_threshold=2.0,
                reference_interest=30.0,
                semantic_wait_ms=1.0,
            )
        ),
        now=lambda: 10.0,
    )
    await workflow.start_active_chat(
        session_id="bot:group:room",
        active_chat_state=make_active_state(),
    )

    result = await workflow.notify_message(
        scheduler=scheduler,
        session_id="bot:group:room",
        message_log_id=1,
        sender_id="bot-self",
        response_profile="balanced",
        is_mentioned=True,
        is_reply_to_bot=False,
        is_mention_to_other=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        self_platform_id="bot-self",
        active_chat_state=make_active_state(),
    )

    assert result.accepted is False
    assert result.skipped_reason == "self_message"
    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert state.pending_buffer == []
    assert state.accumulated == 0.0
    assert scheduler.consumed == []
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_skips_stale_epoch_messages() -> None:
    scheduler = RecordingScheduler()
    workflow = ActiveChatCoordinator(
        attention=ActiveChatAttention(
            ActiveChatAttentionConfig(
                base_threshold=2.0,
                reference_interest=30.0,
                semantic_wait_ms=1.0,
            )
        ),
        now=lambda: 10.0,
    )
    await workflow.start_active_chat(
        session_id="bot:group:room",
        active_chat_state=make_active_state(active_epoch=2),
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
        active_chat_state=make_active_state(active_epoch=1),
    )

    assert result.accepted is False
    assert result.skipped_reason == "active_epoch_mismatch"
    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert state.active_epoch == 2
    assert state.pending_buffer == []
    assert state.accumulated == 0.0
    assert scheduler.consumed == []
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_rejects_message_before_session_start() -> None:
    scheduler = RecordingScheduler()
    workflow = ActiveChatCoordinator(now=lambda: 10.0)

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

    assert result.accepted is False
    assert result.skipped_reason == "inactive_session"
    assert workflow.attention_state_for("bot:group:room") is None
    assert scheduler.consumed == []
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_flushes_after_semantic_wait() -> None:
    batches: list[ActiveChatBatch] = []

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        batches.append(batch)
        return ActiveChatRoundResult(success=True, reason="ok")

    scheduler = RecordingScheduler()
    workflow = ActiveChatCoordinator(
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
            "next_review_plan": None,
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
    workflow = ActiveChatCoordinator(
        attention=ActiveChatAttention(
            ActiveChatAttentionConfig(
                base_threshold=2.0,
                reference_interest=30.0,
                semantic_wait_ms=1.0,
            )
        ),
        now=lambda: 10.0,
    )
    await start_workflow(workflow)

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
    workflow.stop_active_chat("bot:group:room")
    assert workflow.attention_state_for("bot:group:room") is None
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_retry_failed_consumes_batch_and_adjusts_interest() -> None:
    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        return ActiveChatRoundResult(
            success=True,
            action=ActiveChatActionKind.RETRY_FAILED,
            reason="tool_failed",
        )

    scheduler = RecordingScheduler()
    workflow = ActiveChatCoordinator(
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
    await start_workflow(workflow)

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

    assert scheduler.consumed == [("bot:group:room", [1])]
    assert scheduler.adjustments == [
        {
            "session_id": "bot:group:room",
            "delta": -3.0,
            "force_exit": False,
            "reason": "tool_failed",
            "next_review_plan": None,
        }
    ]
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_plans_review_before_exit_to_idle() -> None:
    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        return ActiveChatRoundResult(
            action=ActiveChatActionKind.EXIT_ACTIVE,
            reason="conversation_done",
        )

    plan = ReviewPlan(
        session_id="bot:group:room",
        next_review_at=180.0,
        reason="planned_after_exit",
        updated_at=80.0,
    )
    scheduler = RecordingScheduler()
    scheduler.planned_review = plan
    workflow = ActiveChatCoordinator(
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
    await start_workflow(workflow)

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

    assert scheduler.adjustments == [
        {
            "session_id": "bot:group:room",
            "delta": 0.0,
            "force_exit": True,
            "reason": "conversation_done",
            "next_review_plan": plan,
        }
    ]
    await workflow.shutdown()
