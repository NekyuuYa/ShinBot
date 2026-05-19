from __future__ import annotations

from review_workflow_support import (
    ActiveChatDisposition,
    FakeModelRuntime,
    FakeReviewToolManager,
    LLMIdleReviewPlanningStageRunner,
    LLMReplyDecisionStageRunner,
    LLMReviewScanStageRunner,
    PromptComponent,
    PromptComponentKind,
    PromptRegistry,
    PromptStage,
    ReviewLLMRunnerConfig,
    ReviewRunnerFactory,
    ReviewRuntimeConfig,
    ReviewStageInput,
    ReviewStageRuntimeConfig,
    _make_prompt_registry,
    pytest,
)


@pytest.mark.asyncio
async def test_review_llm_runner_uses_configured_prompt_components() -> None:
    prompt_registry = PromptRegistry()
    prompt_registry.register_component(
        PromptComponent(
            id="review.scan.system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="registered review system",
        )
    )
    prompt_registry.register_component(
        PromptComponent(
            id="review.scan.contract",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="registered output contract",
        )
    )
    model_runtime = FakeModelRuntime(['{"candidate_message_ids": [8], "reason": "selected"}'])
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(
            component_ids_by_stage={
                PromptStage.SYSTEM_BASE: ["review.scan.system"],
                PromptStage.CONSTRAINTS: ["review.scan.contract"],
            },
        ),
        prompt_registry=prompt_registry,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="review_scan",
            source_messages=[{"id": 8, "raw_text": "hello"}],
        )
    )

    assert result.candidate_message_ids == [8]
    call = model_runtime.calls[0]
    message_text = "\n".join(
        block["text"]
        for message in call.messages
        for block in message["content"]
        if isinstance(block, dict) and "text" in block
    )
    assert "registered review system" in message_text
    assert "registered output contract" in message_text


@pytest.mark.asyncio
async def test_review_llm_runner_uses_registered_builtin_review_prompts() -> None:
    prompt_registry = _make_prompt_registry()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "no_reply",
                            "arguments": "{}",
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
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

    message_text = "\n".join(
        block["text"]
        for message in model_runtime.calls[0].messages
        for block in message["content"]
        if isinstance(block, dict) and "text" in block
    )
    assert "第一个 send_reply 必须包含 quote_message_log_id" in message_text
    assert "candidate_message_ids 是回复考虑的核心消息" in message_text
    assert "裸助手文本是无效的" in message_text
    assert "send_poke 是可选" in message_text


@pytest.mark.asyncio
async def test_review_llm_runner_uses_registered_system_prompt() -> None:
    prompt_registry = _make_prompt_registry()
    model_runtime = FakeModelRuntime(
        ['{"candidate_message_ids": [7], "reason": "selected"}']
    )
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=prompt_registry,
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="review_scan",
            source_messages=[{"id": 7, "raw_text": "hello"}],
        )
    )

    message_text = "\n".join(
        block["text"]
        for message in model_runtime.calls[0].messages
        for block in message["content"]
        if isinstance(block, dict) and "text" in block
    )
    assert "ShinBot Agent 审查流程的内部阶段" in message_text
    assert "message_log id" in message_text


@pytest.mark.asyncio
async def test_review_runner_factory_uses_llm_stages_by_default() -> None:
    model_runtime = FakeModelRuntime(
        [
            '{"candidate_message_ids": [9], "reason": "selected"}',
            '{"disposition": "watch", "reason": "observe"}',
            '{"next_review_after_seconds": 120, "reason": "topic_settled"}',
        ]
    )
    factory = ReviewRunnerFactory(
        model_runtime,
        prompt_registry=_make_prompt_registry(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 1}],
    )

    scan = await factory.create_review_scan_runner().run(stage_input)
    bootstrap = await factory.create_active_chat_bootstrap_runner().run(stage_input)
    planning = await factory.create_idle_review_planning_runner().run(stage_input)
    workflow_kwargs = factory.create_workflow_runner_kwargs()

    assert scan.candidate_message_ids == [9]
    assert bootstrap.disposition == ActiveChatDisposition.WATCH
    assert planning.next_review_after_seconds == 120.0
    assert set(workflow_kwargs) == {
        "compression_runner",
        "scan_runner",
        "block_digest_runner",
        "reply_runner",
        "bootstrap_runner",
    }
    assert len(model_runtime.calls) == 3


@pytest.mark.asyncio
async def test_review_runner_factory_keeps_explicitly_disabled_stages_noop() -> None:
    model_runtime = FakeModelRuntime(
        ['{"candidate_message_ids": [9], "reason": "should_not_run"}']
    )
    factory = ReviewRunnerFactory(
        model_runtime,
        config=ReviewRuntimeConfig(
            review_scan=ReviewStageRuntimeConfig(enabled=False),
            active_chat_bootstrap=ReviewStageRuntimeConfig(enabled=False),
            idle_review_planning=ReviewStageRuntimeConfig(enabled=False),
        ),
        prompt_registry=_make_prompt_registry(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 1}],
    )

    scan = await factory.create_review_scan_runner().run(stage_input)
    bootstrap = await factory.create_active_chat_bootstrap_runner().run(stage_input)
    planning = await factory.create_idle_review_planning_runner().run(stage_input)

    assert scan.candidate_message_ids == []
    assert bootstrap.disposition is None
    assert planning.next_review_after_seconds is None
    assert model_runtime.calls == []


@pytest.mark.asyncio
async def test_review_runner_factory_builds_enabled_llm_stage() -> None:
    model_runtime = FakeModelRuntime(['{"candidate_message_ids": [9], "reason": "selected"}'])
    prompt_registry = _make_prompt_registry()
    prompt_registry.register_component(
        PromptComponent(
            id="review.scan.contract",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="return candidate ids",
        )
    )
    factory = ReviewRunnerFactory(
        model_runtime,
        config=ReviewRuntimeConfig(
            review_scan=ReviewStageRuntimeConfig(
                enabled=True,
                route_id="route-a",
                model_id="model-a",
                caller="test.review",
                component_ids_by_stage={
                    PromptStage.CONSTRAINTS: ["review.scan.contract"],
                },
                params={"temperature": 0},
            ),
            reply_decision=ReviewStageRuntimeConfig(enabled=False),
            overflow_compression=ReviewStageRuntimeConfig(enabled=False),
            active_chat_bootstrap=ReviewStageRuntimeConfig(enabled=False),
            idle_review_planning=ReviewStageRuntimeConfig(enabled=False),
        ),
        prompt_registry=prompt_registry,
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 1}],
    )

    scan = await factory.create_review_scan_runner().run(stage_input)

    assert scan.candidate_message_ids == [9]
    assert scan.reason == "selected"
    assert model_runtime.calls[0].route_id == "route-a"
    assert model_runtime.calls[0].model_id == "model-a"
    assert model_runtime.calls[0].caller == "test.review"
    assert model_runtime.calls[0].params == {"temperature": 0}


def test_review_runtime_config_loads_plain_mapping() -> None:
    config = ReviewRuntimeConfig.from_mapping(
        {
            "review_scan": {
                "enabled": True,
                "route_id": "route-a",
                "model_id": "model-a",
                "caller": "custom.review",
                "profile_id": "review.profile",
                "component_ids_by_stage": {
                    "system_base": ["review.system"],
                    "constraints": "review.contract",
                    "unknown": ["ignored"],
                },
                "params": {"temperature": 0},
            },
            "reply_decision": {
                "special_prompt_ids": {"repair": "custom.reply.repair"},
            },
            "active_chat_bootstrap": {"enabled": False, "params": "ignored"},
            "idle_review_planning": {"enabled": False, "params": {"temperature": 0}},
        }
    )

    assert config.review_scan.enabled is True
    assert config.review_scan.route_id == "route-a"
    assert config.review_scan.model_id == "model-a"
    assert config.review_scan.caller == "custom.review"
    assert config.review_scan.profile_id == "review.profile"
    assert config.review_scan.component_ids_by_stage == {
        PromptStage.SYSTEM_BASE: ["review.system"],
        PromptStage.CONSTRAINTS: ["review.contract"],
    }
    assert config.review_scan.params == {"temperature": 0}
    assert config.reply_decision.enabled is True
    assert config.reply_decision.special_prompt_ids == {"repair": "custom.reply.repair"}
    assert config.active_chat_bootstrap.enabled is False
    assert config.active_chat_bootstrap.params == {}
    assert config.idle_review_planning.enabled is False
    assert config.idle_review_planning.params == {"temperature": 0}


@pytest.mark.asyncio
async def test_idle_review_planning_runner_parses_review_plan_parameters() -> None:
    prompt_registry = _make_prompt_registry()
    model_runtime = FakeModelRuntime(
        [
            (
                '{"next_review_after_seconds": 180, "reason": "watch_later", '
                '"mention_sensitivity": "high", "mention_wake_count": 2, '
                '"mention_wake_window_seconds": 90}'
            )
        ]
    )
    runner = LLMIdleReviewPlanningStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=prompt_registry,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="idle_review_planning",
            source_messages=[],
            metadata={"transition": "ACTIVE_CHAT->IDLE"},
        )
    )

    assert result.next_review_after_seconds == 180.0
    assert result.reason == "watch_later"
    assert result.mention_sensitivity.value == "high"
    assert result.mention_wake_count == 2
    assert result.mention_wake_window_seconds == 90.0
