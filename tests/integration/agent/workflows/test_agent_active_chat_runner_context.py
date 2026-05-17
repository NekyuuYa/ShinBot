from __future__ import annotations

from active_chat_runner_support import (
    ActiveChatActionKind,
    ActiveChatContextBuilderAdapter,
    ActiveChatFastRunner,
    ActiveChatFastRunnerConfig,
    FakeContextManager,
    FakeMessageStore,
    FakeModelRuntime,
    FakeToolManager,
    MessageFormatterService,
    PromptComponent,
    PromptComponentKind,
    PromptRegistry,
    PromptStage,
    ReviewHandoffContext,
    ReviewWorkflowExplanation,
    RuntimeModelTarget,
    StageToolConfig,
    SummaryHandoffEntry,
    json,
    make_batch,
    make_result,
    make_tool_call,
    pytest,
    register_active_chat_prompt_components,
    resolve_instance_runtime_config,
)


@pytest.mark.asyncio
async def test_active_chat_fast_runner_uses_prompt_registry_and_tool_loop() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call(
                        "send_reply",
                        {"text": "收到", "intensity": "engaged"},
                    )
                ]
            )
        ]
    )
    tool_manager = FakeToolManager()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=tool_manager,
        message_store=FakeMessageStore(),
    )

    result = await runner.run(make_batch())

    assert result.success is True
    assert result.action == ActiveChatActionKind.SEND_REPLY
    assert tool_manager.calls[0].tool_name == "send_reply"
    tool_names = [
        tool["function"]["name"]
        for tool in model_runtime.calls[0].tools
    ]
    assert tool_names == ["send_reply", "no_reply", "exit_active"]
    assert model_runtime.calls[0].purpose == "active_chat_fast"
    assert model_runtime.calls[0].metadata["message_log_ids"] == [101]
    assert model_runtime.calls[0].metadata["review_result_summary"] == {
        "summary": "review found a running topic"
    }
    assert result.consumed_message_log_ids == [101]
    assert [message["role"] for message in result.conversation_messages_delta] == [
        "assistant",
        "tool",
    ]
    assert result.conversation_messages_delta[0]["tool_calls"][0]["function"]["name"] == (
        "send_reply"
    )


@pytest.mark.asyncio
async def test_active_chat_fast_runner_adds_configured_extra_tools() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [make_result(tool_calls=[make_tool_call("no_reply", {"internal_summary": "skip"})])]
    )
    tool_manager = FakeToolManager()
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=tool_manager,
        message_store=FakeMessageStore(),
        config=ActiveChatFastRunnerConfig(
            tool_config=StageToolConfig(
                extra_names=("search_memory",),
                extra_tags=("knowledge",),
            )
        ),
    )

    await runner.run(make_batch())

    tool_names = [tool["function"]["name"] for tool in model_runtime.calls[0].tools]
    assert tool_names == [
        "send_reply",
        "no_reply",
        "exit_active",
        "search_memory",
        "lookup_profile",
    ]
    assert tool_manager.build_request_tool_calls[0]["tags"] == {"chat_action"}
    assert "tags" not in tool_manager.build_request_tool_calls[1]
    assert tool_manager.export_model_tool_calls[-1]["tags"] == {"knowledge"}
    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "当前的主动聊天批次是主要目标" in rendered_prompt_text
    assert "严禁输出数值形式的兴趣或衰减" in rendered_prompt_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_applies_instance_runtime_config() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [make_result(tool_calls=[make_tool_call("no_reply", {"internal_summary": "skip"})])]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        config=ActiveChatFastRunnerConfig(
            instance_config_resolver=lambda _instance_id: resolve_instance_runtime_config(
                {
                    "main_llm": "route-main",
                    "config": {"explicit_prompt_cache_enabled": True},
                }
            ),
            model_target_resolver=lambda target: RuntimeModelTarget(route_id=target),
        ),
    )

    await runner.run(make_batch())

    assert model_runtime.calls[0].route_id == "route-main"
    assert model_runtime.calls[0].metadata["explicit_prompt_cache_enabled"] is True


@pytest.mark.asyncio
async def test_active_chat_fast_runner_injects_previous_conversation_trace() -> None:
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
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    await runner.run(
        make_batch(
            conversation_messages=[
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        make_tool_call("send_reply", {"text": "上一轮回复"})
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call-send_reply",
                    "content": "{\"ok\": true}",
                },
            ]
        )
    )

    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "send_reply" in rendered_prompt_text
    assert "call-send_reply" in rendered_prompt_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_sanitizes_conversation_trace() -> None:
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
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    await runner.run(
        make_batch(
            conversation_messages=[
                {
                    "role": "tool",
                    "tool_call_id": "call-orphan",
                    "content": "{\"action\": \"send_reply\"}",
                },
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        make_tool_call("send_reply", {"text": "missing result"}),
                    ],
                },
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        make_tool_call("send_reply", {"text": "valid result"}),
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call-send_reply",
                    "content": "{\"action\": \"send_reply\"}",
                },
            ]
        )
    )

    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "call-orphan" not in rendered_prompt_text
    assert "missing result" not in rendered_prompt_text
    assert "valid result" in rendered_prompt_text
    assert "call-send_reply" in rendered_prompt_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_orders_context_before_current_batch() -> None:
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

    await runner.run(
        make_batch(
            self_platform_id="bot-self",
            conversation_summary="{\"compacted_messages\": 2}",
            conversation_messages=[
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        make_tool_call("send_reply", {"text": "previous reply"}),
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call-send_reply",
                    "content": "{\"action\": \"send_reply\"}",
                },
            ],
        )
    )

    messages = model_runtime.calls[0].messages
    assert [message["role"] for message in messages] == [
        "system",
        "user",
        "user",
        "assistant",
        "tool",
        "user",
    ]
    assert "Recent tail context" in str(messages[1]["content"])
    assert "compacted_messages" in str(messages[2]["content"])
    assert messages[3]["tool_calls"][0]["id"] == "call-send_reply"
    assert messages[4]["tool_call_id"] == "call-send_reply"
    assert "消息日志 ID 列表: [101]" in str(messages[-1]["content"])
    assert "当前的主动聊天批次是主要目标" in str(messages[-1]["content"])


@pytest.mark.asyncio
async def test_active_chat_fast_runner_injects_compacted_conversation_summary() -> None:
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
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    await runner.run(
        make_batch(
            conversation_summary='{"compacted_messages": 4, "recent_tool_actions": ["send_reply"]}'
        )
    )

    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "Active chat compacted conversation trace summary" in rendered_prompt_text
    assert "compacted_messages" in rendered_prompt_text
    assert "send_reply" in rendered_prompt_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_uses_configured_special_prompts() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    for component_id, content in {
        "custom.active_chat.summary": "Custom summary prefix:",
        "custom.active_chat.handoff.overflow": "Custom overflow prefix:",
        "custom.active_chat.handoff.digest": "Custom digest prefix:",
        "custom.active_chat.handoff.legacy": "Custom legacy prefix:",
    }.items():
        prompt_registry.register_component(
            PromptComponent(
                id=component_id,
                stage=PromptStage.CONTEXT,
                kind=PromptComponentKind.STATIC_TEXT,
                content=content,
            )
        )
    prompt_registry.register_component(
        PromptComponent(
            id="custom.active_chat.repair",
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="Custom repair prompt.",
        )
    )
    model_runtime = FakeModelRuntime(
        [
            make_result(text="raw text"),
            make_result(tool_calls=[make_tool_call("no_reply", {"internal_summary": "fixed"})]),
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        config=ActiveChatFastRunnerConfig(
            special_prompt_ids={
                "conversation_summary": "custom.active_chat.summary",
                "repair": "custom.active_chat.repair",
                "handoff_overflow": "custom.active_chat.handoff.overflow",
                "handoff_digest": "custom.active_chat.handoff.digest",
                "handoff_legacy": "custom.active_chat.handoff.legacy",
            },
        ),
    )
    handoff = ReviewHandoffContext(
        review_run_id="test_run",
        explanation=ReviewWorkflowExplanation(
            review_run_id="test_run",
            review_started_at=123.0,
        ),
        overflow_summaries=[SummaryHandoffEntry(content="Old topic.")],
        block_digests=[SummaryHandoffEntry(content="Block topic.")],
        recent_active_chat_summary="Recent topic.",
    )

    await runner.run(
        make_batch(
            review_result_summary=handoff,
            conversation_summary='{"compacted_messages": 4}',
        )
    )

    first_call_text = json.dumps(model_runtime.calls[0].messages, ensure_ascii=False)
    assert "Custom summary prefix:" in first_call_text
    assert "Custom overflow prefix:" in first_call_text
    assert "Custom digest prefix:" in first_call_text
    assert "Custom legacy prefix:" in first_call_text
    assert model_runtime.calls[1].messages[-1]["content"][0]["text"] == "Custom repair prompt."


@pytest.mark.asyncio
async def test_active_chat_fast_runner_uses_message_formatter_without_context_builder() -> None:
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
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
        message_formatter=MessageFormatterService(),
    )

    await runner.run(make_batch())

    rendered_prompt_text = json.dumps(
        model_runtime.calls[0].messages,
        ensure_ascii=False,
    )
    assert "[msg_log_id:101] Alice: message 101" in rendered_prompt_text
    assert "Source messages JSON" not in rendered_prompt_text
