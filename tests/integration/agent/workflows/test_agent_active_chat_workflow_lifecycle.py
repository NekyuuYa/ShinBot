from __future__ import annotations

from active_chat_workflow_support import (
    ActiveChatAttention,
    ActiveChatAttentionConfig,
    ActiveChatBatch,
    ActiveChatCoordinator,
    ActiveChatRoundResult,
    RecordingScheduler,
    asyncio,
    make_active_state,
    make_signal,
    pytest,
    start_workflow,
)


@pytest.mark.asyncio
async def test_active_chat_workflow_runs_next_batch_for_messages_arriving_during_round() -> None:
    first_round_started = asyncio.Event()
    release_first_round = asyncio.Event()
    batches: list[ActiveChatBatch] = []

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        batches.append(batch)
        if len(batches) == 1:
            first_round_started.set()
            await release_first_round.wait()
        return ActiveChatRoundResult(success=True, reason=f"round-{len(batches)}")

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
    await asyncio.wait_for(first_round_started.wait(), timeout=1.0)

    await workflow.notify_message(
        scheduler=scheduler,
        session_id="bot:group:room",
        message_log_id=2,
        sender_id="user-2",
        response_profile="balanced",
        is_mentioned=False,
        is_reply_to_bot=False,
        is_mention_to_other=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        self_platform_id="bot-self",
        active_chat_state=make_active_state(),
    )
    release_first_round.set()
    await asyncio.sleep(0.05)

    assert [batch.message_log_ids for batch in batches] == [[1], [2]]
    assert scheduler.consumed == [
        ("bot:group:room", [1]),
        ("bot:group:room", [2]),
    ]
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_drain_pending_for_repair_clears_pending_attention() -> None:
    first_round_started = asyncio.Event()
    release_first_round = asyncio.Event()
    batches: list[ActiveChatBatch] = []

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        batches.append(batch)
        first_round_started.set()
        await release_first_round.wait()
        return ActiveChatRoundResult(
            success=True,
            reason="ok",
            consumed_message_log_ids=[1, 2],
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
    await asyncio.wait_for(first_round_started.wait(), timeout=1.0)

    await workflow.notify_message(
        scheduler=scheduler,
        session_id="bot:group:room",
        message_log_id=2,
        sender_id="user-2",
        response_profile="balanced",
        is_mentioned=True,
        is_reply_to_bot=False,
        is_mention_to_other=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        self_platform_id="bot-self",
        active_chat_state=make_active_state(),
    )
    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert state.accumulated == pytest.approx(4.0)
    assert [message.message_log_id for message in state.pending_buffer] == [2]

    drained = await workflow.drain_pending_for_repair(batches[0])

    assert [message.message_log_id for message in drained] == [2]
    assert state.accumulated == 0.0
    assert state.pending_buffer == []

    release_first_round.set()
    await asyncio.sleep(0.05)

    assert scheduler.consumed == [("bot:group:room", [1, 2])]
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_stop_cancels_running_round_without_consuming() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        started.set()
        try:
            await asyncio.sleep(60.0)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return ActiveChatRoundResult(success=True, reason="late")

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
    await asyncio.wait_for(started.wait(), timeout=1.0)

    workflow.stop_active_chat("bot:group:room")
    await asyncio.wait_for(cancelled.wait(), timeout=1.0)
    await asyncio.sleep(0)

    assert scheduler.consumed == []
    assert scheduler.adjustments == []
    assert workflow.attention_state_for("bot:group:room") is None
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_restart_cancels_old_round_and_resets_state() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        started.set()
        try:
            await asyncio.sleep(60.0)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return ActiveChatRoundResult(success=True, reason=f"late-{batch.active_chat_state.active_epoch}")

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
    workflow.last_batches["bot:group:room"] = ActiveChatBatch(
        session_id="bot:group:room",
        messages=[make_signal(message_log_id=99)],
        active_chat_state=make_active_state(),
        response_profile="balanced",
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
    await asyncio.wait_for(started.wait(), timeout=1.0)

    await start_workflow(workflow, active_state=make_active_state(active_epoch=2))
    await asyncio.wait_for(cancelled.wait(), timeout=1.0)

    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert state.active_epoch == 2
    assert state.pending_buffer == []
    assert workflow.last_batches == {}
    assert scheduler.consumed == []
    await workflow.shutdown()
