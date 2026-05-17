from __future__ import annotations

from active_chat_runner_support import (
    ActiveChatActionKind,
    ActiveChatBatch,
    ActiveChatContextBuilderAdapter,
    ActiveChatFastRunner,
    ActiveChatMessageSignal,
    ActiveChatNoReplyIntensity,
    BrokenContextBuilder,
    BrokenMessageFormatter,
    FailingModelRuntime,
    FailingRepairModelRuntime,
    FakeContextManager,
    FakeMessageStore,
    FakeModelRuntime,
    FakeToolManager,
    MessageFormatterService,
    PromptRegistry,
    ToolCallRequest,
    ToolCallResult,
    json,
    make_batch,
    make_result,
    make_tool_call,
    pytest,
    register_active_chat_prompt_components,
)


@pytest.mark.asyncio
async def test_active_chat_fast_runner_repairs_toolless_output_once() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(text="I would say nothing."),
            make_result(
                tool_calls=[
                    make_tool_call(
                        "no_reply",
                        {"internal_summary": "low value", "intensity": "strong"},
                    )
                ]
            ),
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    result = await runner.run(make_batch())

    assert result.success is True
    assert result.action == ActiveChatActionKind.NO_REPLY
    assert result.no_reply_intensity == ActiveChatNoReplyIntensity.STRONG
    assert len(model_runtime.calls) == 2
    assert model_runtime.calls[1].metadata["repair_attempt"] == 1
    assert model_runtime.calls[1].messages[-1]["role"] == "system"


@pytest.mark.asyncio
async def test_active_chat_fast_runner_repairs_failed_terminal_tool_once() -> None:
    class FailingThenSuccessfulToolManager(FakeToolManager):
        async def execute(self, call: ToolCallRequest) -> ToolCallResult:
            self.calls.append(call)
            if len(self.calls) == 1:
                return ToolCallResult(
                    tool_name=call.tool_name,
                    success=False,
                    error_code="invalid_quote",
                    error_message="quote_message_log_id was not found",
                )
            return ToolCallResult(
                tool_name=call.tool_name,
                success=True,
                output={
                    "action": "send_reply",
                    "sent": True,
                    "message_log_id": 9002,
                },
            )

    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call(
                        "send_reply",
                        {"text": "hello", "quote_message_log_id": "missing"},
                    )
                ]
            ),
            make_result(tool_calls=[make_tool_call("send_reply", {"text": "hello"})]),
        ]
    )
    tool_manager = FailingThenSuccessfulToolManager()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=tool_manager,
        message_store=FakeMessageStore(),
    )

    result = await runner.run(make_batch())

    assert result.success is True
    assert result.action == ActiveChatActionKind.SEND_REPLY
    assert len(model_runtime.calls) == 2
    assert model_runtime.calls[1].metadata["repair_attempt"] == 1
    repair_messages = model_runtime.calls[1].messages
    assert repair_messages[-3]["role"] == "assistant"
    assert repair_messages[-2]["role"] == "tool"
    assert "quote_message_log_id was not found" in repair_messages[-2]["content"]
    assert repair_messages[-1]["role"] == "system"
    assert len(tool_manager.calls) == 2
    assert tool_manager.calls[0].arguments["quote_message_log_id"] == "missing"
    assert "quote_message_log_id" not in tool_manager.calls[1].arguments
    assert len(result.conversation_messages_delta) == 4
    assert result.conversation_messages_delta[1]["role"] == "tool"
    assert result.conversation_messages_delta[3]["role"] == "tool"


@pytest.mark.asyncio
async def test_active_chat_fast_runner_reports_unconsumable_prompt_build_failure() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime([])
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        context_builder=BrokenContextBuilder(),
    )

    result = await runner.run(make_batch())

    assert result.success is False
    assert result.action == ActiveChatActionKind.RETRY_FAILED
    assert result.reason == "active_chat_prompt_build_failed"
    assert result.consumed_message_log_ids == []
    assert model_runtime.calls == []


@pytest.mark.asyncio
async def test_active_chat_fast_runner_reports_unconsumable_model_failure() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FailingModelRuntime()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    result = await runner.run(make_batch())

    assert result.success is False
    assert result.action == ActiveChatActionKind.RETRY_FAILED
    assert result.reason == "active_chat_model_call_failed"
    assert result.consumed_message_log_ids == []
    assert len(model_runtime.calls) == 1


@pytest.mark.asyncio
async def test_active_chat_fast_runner_merges_pending_messages_into_repair() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(text="I would reply without tools."),
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "merged batch"})
                ]
            ),
        ]
    )

    async def pending_provider(batch: ActiveChatBatch) -> list[ActiveChatMessageSignal]:
        return [
            ActiveChatMessageSignal(
                session_id=batch.session_id,
                message_log_id=102,
                sender_id="bob",
                response_profile="balanced",
                active_chat_state=batch.active_chat_state,
            )
        ]

    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        pending_message_provider=pending_provider,
    )

    result = await runner.run(make_batch())

    assert result.success is True
    assert result.action == ActiveChatActionKind.NO_REPLY
    assert result.consumed_message_log_ids == [101, 102]
    assert model_runtime.calls[0].metadata["message_log_ids"] == [101]
    assert model_runtime.calls[1].metadata["message_log_ids"] == [101, 102]


@pytest.mark.asyncio
async def test_active_chat_fast_runner_restores_repair_batch_on_failed_repair() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FailingRepairModelRuntime()

    async def pending_provider(batch: ActiveChatBatch) -> list[ActiveChatMessageSignal]:
        return [
            ActiveChatMessageSignal(
                session_id=batch.session_id,
                message_log_id=102,
                sender_id="bob",
                response_profile="balanced",
                active_chat_state=batch.active_chat_state,
            )
        ]

    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        pending_message_provider=pending_provider,
    )

    result = await runner.run(make_batch())

    assert result.success is False
    assert result.action == ActiveChatActionKind.RETRY_FAILED
    assert result.reason == "active_chat_toolless_repair_failed"
    assert result.consumed_message_log_ids == []
    assert [message.message_log_id for message in result.restored_messages] == [101, 102]
    assert model_runtime.calls[0].metadata["message_log_ids"] == [101]
    assert model_runtime.calls[1].metadata["message_log_ids"] == [101, 102]


@pytest.mark.asyncio
async def test_active_chat_fast_runner_injects_active_context_messages() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    context_manager = FakeContextManager()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        context_builder=ActiveChatContextBuilderAdapter(context_manager),
    )

    result = await runner.run(make_batch(self_platform_id="bot-self"))

    assert result.success is True
    assert context_manager.instruction_calls == []
    assert context_manager.context_calls[0]["self_platform_id"] == "bot-self"
    assert any(
        "Recent tail context" in str(message.get("content", ""))
        for message in model_runtime.calls[0].messages
    )


@pytest.mark.asyncio
async def test_active_chat_context_adapter_falls_back_when_formatter_fails() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    context_manager = FakeContextManager()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        context_builder=ActiveChatContextBuilderAdapter(
            context_manager,
            message_formatter=BrokenMessageFormatter(),
        ),
        message_formatter=MessageFormatterService(),
    )

    result = await runner.run(make_batch(self_platform_id="bot-self"))

    assert result.success is True
    assert context_manager.instruction_calls == []
    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "[msg_log_id:101] Alice: message 101" in rendered_prompt_text
