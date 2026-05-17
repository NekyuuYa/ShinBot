from __future__ import annotations

from active_chat_workflow_support import (
    ActiveChatActionKind,
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
async def test_active_chat_workflow_exit_active_forwards_reason_to_scheduler() -> None:
    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        return ActiveChatRoundResult(
            success=True,
            action=ActiveChatActionKind.EXIT_ACTIVE,
            reason="conversation has clearly ended",
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
            "delta": 0.0,
            "force_exit": True,
            "reason": "conversation has clearly ended",
        }
    ]
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_failed_round_restores_pending_attention() -> None:
    batches: list[ActiveChatBatch] = []

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        batches.append(batch)
        if len(batches) == 1:
            return ActiveChatRoundResult(success=False, reason="transient_failure")
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

    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert [message.message_log_id for message in state.pending_buffer] == [1]
    assert state.accumulated == pytest.approx(4.0)
    assert scheduler.consumed == []

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
    await asyncio.sleep(0.05)

    assert [batch.message_log_ids for batch in batches] == [[1], [1, 2]]
    assert scheduler.consumed == [("bot:group:room", [1, 2])]
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_failed_round_restores_handler_supplied_messages() -> None:
    async def handler(_batch: ActiveChatBatch) -> ActiveChatRoundResult:
        return ActiveChatRoundResult(
            success=False,
            reason="repair_failed",
            restored_messages=[
                make_signal(message_log_id=1, is_mentioned=True),
                make_signal(message_log_id=2, sender_id="user-2"),
            ],
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

    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert [message.message_log_id for message in state.pending_buffer] == [1, 2]
    assert scheduler.consumed == []
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_uses_round_result_consumed_message_ids() -> None:
    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        return ActiveChatRoundResult(success=True, consumed_message_log_ids=[1, 2])

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

    assert scheduler.consumed == [("bot:group:room", [1, 2])]
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_carries_conversation_trace_to_next_batch() -> None:
    batches: list[ActiveChatBatch] = []

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        batches.append(batch)
        return ActiveChatRoundResult(
            success=True,
            reason=f"round-{len(batches)}",
            conversation_messages_delta=[
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": f"call-{len(batches)}",
                            "type": "function",
                            "function": {
                                "name": "no_reply",
                                "arguments": "{}",
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": f"call-{len(batches)}",
                    "content": "{\"action\": \"no_reply\"}",
                },
            ],
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

    for message_log_id in (1, 2):
        await workflow.notify_message(
            scheduler=scheduler,
            session_id="bot:group:room",
            message_log_id=message_log_id,
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

    assert [batch.message_log_ids for batch in batches] == [[1], [2]]
    assert batches[0].conversation_messages == []
    assert batches[1].conversation_messages[0]["role"] == "assistant"
    assert batches[1].conversation_messages[0]["tool_calls"][0]["id"] == "call-1"
    await workflow.shutdown()


@pytest.mark.asyncio
async def test_active_chat_workflow_compacts_old_conversation_trace() -> None:
    batches: list[ActiveChatBatch] = []

    async def handler(batch: ActiveChatBatch) -> ActiveChatRoundResult:
        batches.append(batch)
        round_no = len(batches)
        return ActiveChatRoundResult(
            success=True,
            reason=f"round-{round_no}",
            conversation_messages_delta=[
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": f"call-{round_no}",
                            "type": "function",
                            "function": {
                                "name": "send_reply",
                                "arguments": "{}",
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": f"call-{round_no}",
                    "content": "{\"action\": \"send_reply\"}",
                },
            ],
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
        conversation_message_limit=3,
    )
    await start_workflow(workflow)

    for message_log_id in (1, 2, 3):
        await workflow.notify_message(
            scheduler=scheduler,
            session_id="bot:group:room",
            message_log_id=message_log_id,
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

    assert [batch.message_log_ids for batch in batches] == [[1], [2], [3]]
    state = workflow.attention_state_for("bot:group:room")
    assert state is not None
    assert len(state.conversation_messages) == 2
    assert state.conversation_messages[0]["role"] == "assistant"
    assert state.conversation_messages[0]["tool_calls"][0]["id"] == "call-3"
    assert state.conversation_messages[1]["role"] == "tool"
    assert state.conversation_messages[1]["tool_call_id"] == "call-3"
    assert "compacted_messages" in state.conversation_summary
    assert "send_reply" in state.conversation_summary
    assert batches[2].conversation_summary
    await workflow.shutdown()
