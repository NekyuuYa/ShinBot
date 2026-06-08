from __future__ import annotations

from review_workflow_support import (
    FakeModelRuntime,
    FakeReviewToolManager,
    LLMReplyDecisionStageRunner,
    PromptComponent,
    PromptComponentKind,
    PromptStage,
    ReviewLLMRunnerConfig,
    ReviewStageInput,
    StageToolConfig,
    _make_prompt_registry,
    pytest,
)


@pytest.mark.asyncio
async def test_reply_decision_runner_exports_and_executes_terminal_tools() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    call = model_runtime.calls[0]
    tool_names = [tool["function"]["name"] for tool in call.tools]
    assert tool_names == ["no_reply", "send_reply", "send_poke"]
    assert tool_manager.build_request_tool_calls[0]["tags"] == {"chat_action"}
    send_reply_tool = call.tools[1]
    send_poke_tool = call.tools[2]
    assert "quote_message_log_id" not in send_reply_tool["function"]["parameters"]["required"]
    assert "first send_reply" in send_reply_tool["function"]["description"]
    assert "only takes effect after at least one send_reply" in send_poke_tool["function"][
        "description"
    ]
    assert call.response_format is None
    assert result.replied is True
    assert result.reply_message_id == 42
    assert result.reply_message_ids == [42]
    assert result.target_message_ids == [7]
    assert result.reason == "send_reply_tool"
    assert tool_manager.execute_calls[0].tool_name == "send_reply"
    assert tool_manager.execute_calls[0].caller == "test.review"


@pytest.mark.asyncio
async def test_reply_decision_runner_adds_configured_extra_tools() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {"name": "no_reply", "arguments": "{}"},
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(
            caller="test.review",
            tool_config=StageToolConfig(
                extra_names=("attention.inspect_state",),
                extra_tags=("knowledge",),
            ),
        ),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    call = model_runtime.calls[0]
    tool_names = [tool["function"]["name"] for tool in call.tools]
    assert tool_names == [
        "no_reply",
        "send_reply",
        "send_poke",
        "attention.inspect_state",
    ]
    assert tool_manager.build_request_tool_calls[0]["tags"] == {"chat_action"}
    assert "tags" not in tool_manager.build_request_tool_calls[1]
    assert tool_manager.export_model_tool_calls[-1]["tags"] == {"knowledge"}
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_executes_multiple_replies_in_order() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "first", "quote_message_log_id": 7}',
                        },
                    },
                    {
                        "id": "tool-2",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "second"}',
                        },
                    },
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 8, "raw_text": "world"}],
            metadata={"candidate_message_ids": [7, 8]},
        )
    )

    assert result.replied is True
    assert result.reply_message_id == 42
    assert result.reply_message_ids == [42, 43]
    assert result.target_message_ids == [7, 8]
    assert result.reason == "send_reply_tool:2"
    assert [call.tool_name for call in tool_manager.execute_calls] == [
        "send_reply",
        "send_reply",
    ]
    assert [call.arguments["text"] for call in tool_manager.execute_calls] == [
        "first",
        "second",
    ]
    assert tool_manager.execute_calls[0].arguments["quote_message_log_id"] == 7
    assert "quote_message_log_id" not in tool_manager.execute_calls[1].arguments
    assert [call.arguments["idempotency_key"] for call in tool_manager.execute_calls] == [
        "exec-1:0",
        "exec-1:1",
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_allows_poke_after_reply_only() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    },
                    {
                        "id": "tool-2",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    },
                    {
                        "id": "tool-3",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    },
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is True
    assert result.reply_message_ids == [42]
    assert result.reason == "send_reply_tool:1;send_poke_tool:2"
    assert [call.tool_name for call in tool_manager.execute_calls] == [
        "send_poke",
        "send_reply",
        "send_poke",
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_ignores_standalone_poke() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.reason == "llm_reply_decision_no_terminal_tool"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_requires_quoted_reply_message() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello"}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "reply_tool_missing_quote_message_log_id"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_requires_first_quote_to_target_candidate() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 99}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 99, "raw_text": "nearby"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "reply_tool_quote_message_log_id_not_candidate"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_blocks_other_target_only_candidate_reply() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": (
                                '{"text": "是真红，不是风子哦", '
                                '"quote_message_log_id": 61698}'
                            ),
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[
                {
                    "id": 61698,
                    "raw_text": "@月梅塩絮 风子",
                    "content_json": (
                        '[{"type":"at","attrs":{"id":"2898394893","name":"月梅塩絮"}},'
                        '{"type":"text","attrs":{"content":" 风子"}}]'
                    ),
                }
            ],
            metadata={
                "candidate_message_ids": [61698],
                "other_target_only_candidate_message_ids": [61698],
                "candidate_target_facts": [
                    {
                        "message_id": 61698,
                        "mentions_other": True,
                        "targeted_to_other_only": True,
                        "other_target_ids": ["2898394893"],
                        "text_without_target_markers": "风子",
                    }
                ],
            },
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [61698]
    assert result.reason == "reply_tool_quote_message_log_id_targets_other_only"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_blocks_later_other_target_only_quoted_reply() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "first", "quote_message_log_id": 7}',
                        },
                    },
                    {
                        "id": "tool-2",
                        "function": {
                            "name": "send_reply",
                            "arguments": (
                                '{"text": "是真红，不是风子哦", '
                                '"quote_message_log_id": 61698}'
                            ),
                        },
                    },
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[
                {"id": 7, "raw_text": "hello"},
                {
                    "id": 61698,
                    "raw_text": "@月梅塩絮 风子",
                    "content_json": (
                        '[{"type":"at","attrs":{"id":"2898394893","name":"月梅塩絮"}},'
                        '{"type":"text","attrs":{"content":" 风子"}}]'
                    ),
                },
            ],
            metadata={
                "candidate_message_ids": [7, 61698],
                "other_target_only_candidate_message_ids": [61698],
            },
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7, 61698]
    assert result.reason == "reply_tool_quote_message_log_id_targets_other_only"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_repairs_toolless_text_response() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            "我应该回复一下",
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    }
                ]
            },
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is True
    assert result.reply_message_ids == [42]
    assert result.reason == "send_reply_tool"
    assert len(model_runtime.calls) == 2
    repair_call = model_runtime.calls[1]
    assert repair_call.metadata["repair_attempt"] == 1
    assert repair_call.metadata["repair_reason"] == "reply_decision_toolless_output"
    assert repair_call.messages[-2] == {
        "role": "assistant",
        "content": "我应该回复一下",
    }
    repair_text = repair_call.messages[-1]["content"][0]["text"]
    assert "必须调用工具" in repair_text
    assert "第一条 send_reply 必须带 quote_message_log_id" in repair_text
    assert tool_manager.execute_calls[0].tool_name == "send_reply"


@pytest.mark.asyncio
async def test_reply_decision_runner_uses_configured_repair_prompt() -> None:
    prompt_registry = _make_prompt_registry()
    prompt_registry.register_component(
        PromptComponent(
            id="custom.reply.repair",
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="Custom reply repair prompt.",
        )
    )
    model_runtime = FakeModelRuntime(
        [
            "raw text",
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "no_reply",
                            "arguments": '{"internal_summary": "fixed"}',
                        },
                    }
                ]
            },
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(
            caller="test.review",
            special_prompt_ids={"repair": "custom.reply.repair"},
        ),
        prompt_registry=prompt_registry,
        tool_manager=FakeReviewToolManager(),
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert model_runtime.calls[1].messages[-1]["content"][0]["text"] == (
        "Custom reply repair prompt."
    )


@pytest.mark.asyncio
async def test_reply_decision_runner_fails_after_toolless_repair() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(["raw text", "still raw"])
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "llm_reply_decision_toolless_after_repair"
    assert len(model_runtime.calls) == 2
    assert tool_manager.execute_calls == []
