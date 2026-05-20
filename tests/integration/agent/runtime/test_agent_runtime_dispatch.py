from __future__ import annotations

from agent_runtime_support import (
    ActiveChatState,
    AgentEntrySignal,
    AgentScheduler,
    AgentState,
    Any,
    FakeModelRuntime,
    InstanceConfigRecord,
    MessageLogRecord,
    Path,
    RecordingScheduler,
    RecordingWorkflowDispatcher,
    ShinBot,
    asyncio,
    install_agent_runtime,
    make_generate_result,
    make_signal,
    make_tool_call,
    pytest,
)

from shinbot.agent.signals import (
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
    AgentTimerSignal,
)


@pytest.mark.asyncio
async def test_agent_runtime_selects_profile_by_bot_id(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={
            "bot-a": {
                "agent": {
                    "id": "agent-a",
                    "active_chat": {"initial_interest": 77},
                }
            }
        },
    )
    bot_a_scheduler = RecordingScheduler()
    default_scheduler = RecordingScheduler()
    runtime.agent_profile_for_bot("bot-a").agent_scheduler = bot_a_scheduler
    runtime.agent_scheduler = default_scheduler

    await runtime.handle_agent_entry(make_signal(bot_id="bot-a"))
    await runtime.handle_agent_entry(make_signal(bot_id="bot-b"))

    assert runtime.agent_profile_for_bot("bot-a").profile_id == "agent-a"
    assert (
        runtime.agent_profile_for_bot("bot-a")
        .config.active_chat_policy_config.initial_interest_value
        == 77
    )
    assert [signal.bot_id for signal in bot_a_scheduler.calls] == ["bot-a"]
    assert [signal.bot_id for signal in default_scheduler.calls] == ["bot-b"]


def test_agent_runtime_syncs_builtin_prompt_files_to_data_dir(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)

    runtime_prompt = tmp_path / "prompts" / "zh-CN" / "review.review_scan.task.md"
    component = runtime.prompt_registry.get_component("review.review_scan.task")

    assert runtime_prompt.exists()
    assert component is not None
    assert component.metadata["prompt_file"] == str(runtime_prompt)
    assert component.metadata["runtime_prompt_file"] == str(runtime_prompt)
    assert "review_scan" in component.metadata["source_prompt_file"]


def test_agent_runtime_uses_existing_data_prompt_without_overwrite(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "prompts" / "zh-CN"
    runtime_dir.mkdir(parents=True)
    runtime_prompt = runtime_dir / "review.review_scan.task.md"
    runtime_prompt.write_text(
        """---
id: review.review_scan.task
stage: instructions
kind: static_text
priority: 100
enabled: true
---

用户自定义 review scan prompt。
""",
        encoding="utf-8",
    )

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    component = runtime.prompt_registry.get_component("review.review_scan.task")

    assert component is not None
    assert component.content == "用户自定义 review scan prompt。"
    assert runtime_prompt.read_text(encoding="utf-8").endswith(
        "用户自定义 review scan prompt。\n"
    )


def test_agent_runtime_reload_prompt_files_picks_up_data_edits(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    runtime_prompt = tmp_path / "prompts" / "zh-CN" / "review.review_scan.task.md"

    text = runtime_prompt.read_text(encoding="utf-8")
    runtime_prompt.write_text(
        text.replace("评估提供的未读消息", "用户在 WebUI 中修改后的审查提示"),
        encoding="utf-8",
    )

    runtime.reload_prompt_files()
    component = runtime.prompt_registry.get_component("review.review_scan.task")

    assert component is not None
    assert "用户在 WebUI 中修改后的审查提示" in component.content


def test_agent_runtime_applies_active_chat_threshold_delta_to_all_profiles(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={
            "bot-a": {
                "agent": {
                    "id": "agent-a",
                    "active_chat": {"attention": {"threshold": 7}},
                }
            }
        },
    )

    runtime.set_active_chat_threshold_delta(3.0, source="test")

    assert runtime.active_chat_workflow.attention_config.base_threshold == 8.0
    assert (
        runtime.agent_profile_for_bot("bot-a")
        .active_chat_workflow.attention_config.base_threshold
        == 10.0
    )

    runtime.set_active_chat_threshold_delta(0.0, source="test")

    assert runtime.active_chat_workflow.attention_config.base_threshold == 5.0
    assert (
        runtime.agent_profile_for_bot("bot-a")
        .active_chat_workflow.attention_config.base_threshold
        == 7.0
    )


@pytest.mark.asyncio
async def test_agent_runtime_without_database_shutdown_is_noop() -> None:
    bot = ShinBot()
    runtime = install_agent_runtime(bot)

    await runtime.shutdown()

    assert runtime.review_coordinator is None
    assert runtime.active_chat_workflow.active_session_ids() == []


@pytest.mark.asyncio
async def test_agent_runtime_resolves_response_profile_from_agent_boundary(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )
    bot.database.instance_configs.upsert(
        InstanceConfigRecord(
            uuid="cfg-group-profile",
            instance_id="test-bot",
            config={
                "response_profile_group": "passive",
                "response_profile_priority": "balanced",
                "response_profile_private": "disabled",
            },
        )
    )

    await runtime.handle_agent_entry(make_signal())
    await runtime.handle_agent_entry(make_signal(is_mentioned=True))
    await runtime.handle_agent_entry(make_signal(is_private=True))

    assert [call["response_profile"] for call in dispatcher.calls] == [
        "balanced",
    ]


@pytest.mark.asyncio
async def test_agent_runtime_skips_unusable_agent_entry_signals(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )

    await runtime.handle_agent_entry(make_signal(is_reply_to_bot=True))
    await runtime.handle_agent_entry(make_signal(is_mentioned=True, is_private=False))
    await runtime.handle_agent_entry(
        AgentEntrySignal(
            session_id="test-bot:group:group:1",
            message_log_id=None,
            event_type="message-created",
            sender_id="user-1",
            instance_id="test-bot",
            platform="mock",
            self_id="bot-1",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
    )

    assert [call["response_profile"] for call in dispatcher.calls] == [
        "immediate",
        "immediate",
    ]


@pytest.mark.asyncio
async def test_agent_runtime_records_ordinary_messages_without_active_reply(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )

    await runtime.handle_agent_entry(make_signal())

    assert dispatcher.calls == []
    assert [
        item.message_log_id
        for item in runtime.agent_scheduler.unread_messages("test-bot:group:group:1")
    ] == [123]


@pytest.mark.asyncio
async def test_agent_runtime_wires_active_chat_fast_runner_end_to_end(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "no_reply",
                        {"internal_summary": "watching the live chat"},
                    )
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    bot.database.instance_configs.upsert(
        InstanceConfigRecord(
            uuid="cfg-active-chat-runtime",
            instance_id="test-bot",
            main_llm="route-main",
            config={"explicit_prompt_cache_enabled": True},
        )
    )
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-1",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot hello",
            content_json="[]",
            role="user",
            created_at=10_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=3,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 1
        call = model_runtime.calls[0]
        assert call.purpose == "active_chat_fast"
        assert call.route_id == "route-main"
        assert call.metadata["message_log_ids"] == [message_log_id]
        assert call.metadata["explicit_prompt_cache_enabled"] is True
        assert {
            tool["function"]["name"]
            for tool in call.tools
        } >= {"send_reply", "no_reply", "send_poke", "exit_active"}
        assert "request_think_mode" not in {
            tool["function"]["name"]
            for tool in call.tools
        }
        assert runtime.agent_scheduler.unread_messages(session_id) == []
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert state.pending_buffer == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_keeps_active_chat_pending_unread_on_exit(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "unused"}),
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-2",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot are you there?",
            content_json="[]",
            role="user",
            created_at=20_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=4,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert [message.message_log_id for message in state.pending_buffer] == [message_log_id]

        decision = runtime.agent_scheduler.adjust_active_chat_interest(
            session_id,
            force_exit=True,
            reason="test_exit_before_batch",
        )

        assert decision.returned_to_idle is True
        unread_message_ids = [
            message.message_log_id
            for message in runtime.agent_scheduler.unread_messages(session_id)
        ]
        assert unread_message_ids == [message_log_id]
        assert runtime.active_chat_workflow.attention_state_for(session_id) is None
        assert model_runtime.calls == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_repair_merges_active_chat_pending_messages(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(text="I would answer without a tool."),
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "no_reply",
                        {"internal_summary": "merged live batch"},
                    )
                ]
            ),
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    first_message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-3",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot first",
            content_json="[]",
            role="user",
            created_at=30_000.0,
            is_mentioned=True,
        )
    )
    second_message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-4",
            sender_id="user-2",
            sender_name="User 2",
            raw_text="@bot second",
            content_json="[]",
            role="user",
            created_at=31_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=5,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    async def inject_pending_message(_call: Any) -> None:
        if len(model_runtime.calls) != 1:
            return
        await runtime.handle_agent_entry(
            make_signal(message_log_id=second_message_log_id, is_mentioned=True)
        )

    model_runtime.on_generate = inject_pending_message

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=first_message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 2
        assert model_runtime.calls[0].metadata["message_log_ids"] == [first_message_log_id]
        assert model_runtime.calls[0].metadata["repair_attempt"] == 0
        assert model_runtime.calls[1].metadata["message_log_ids"] == [
            first_message_log_id,
            second_message_log_id,
        ]
        assert model_runtime.calls[1].metadata["repair_attempt"] == 1
        assert runtime.agent_scheduler.unread_messages(session_id) == []
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert state.pending_buffer == []
        assert state.conversation_messages[0]["tool_calls"][0]["function"]["name"] == (
            "no_reply"
        )
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_exit_active_returns_idle_with_review_plan(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "exit_active",
                        {"reason": "conversation has clearly ended"},
                    )
                ]
            ),
            make_generate_result(
                text='{"next_review_after_seconds": 120, "reason": "conversation_settled"}'
            ),
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-5",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot bye",
            content_json="[]",
            role="user",
            created_at=40_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=6,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 2
        assert model_runtime.calls[1].purpose == "idle_review_planning"
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.IDLE
        assert runtime.agent_scheduler.active_chat_state_for(session_id) is None
        review_plan = runtime.agent_scheduler.review_plan_for(session_id)
        assert review_plan is not None
        assert review_plan.reason == "conversation_settled"
        assert runtime.active_chat_workflow.attention_state_for(session_id) is None
        assert runtime.agent_scheduler.unread_messages(session_id) == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_active_chat_tick_plans_review_before_idle(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                text='{"next_review_after_seconds": 120, "reason": "timer_settled"}'
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(
        bot,
        agent_config={
            "agent": {
                "active_chat": {
                    "initial_interest": 10.0,
                    "idle_interest_threshold": 5.0,
                    "decay_half_life_seconds": 5.0,
                    "tick_interval_seconds": 5.0,
                }
            }
        },
    )
    session_id = "test-bot:group:group:1"
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=10.0,
        decay_half_life_seconds=5.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=7,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_signal(
            AgentSignal(
                signal_id="tick:test-bot:group:group:1",
                kind=AgentSignalKind.ACTIVE_CHAT_TICK,
                source=AgentSignalSource.TIMER,
                session_id=session_id,
                occurred_at=15.0,
                timer=AgentTimerSignal(trigger=AgentSignalKind.ACTIVE_CHAT_TICK.value),
            )
        )

        assert runtime.agent_scheduler.state_for(session_id) == AgentState.IDLE
        assert runtime.agent_scheduler.active_chat_state_for(session_id) is None
        assert runtime.active_chat_workflow.attention_state_for(session_id) is None
        review_plan = runtime.agent_scheduler.review_plan_for(session_id)
        assert review_plan is not None
        assert review_plan.reason == "timer_settled"
        assert model_runtime.calls[0].purpose == "idle_review_planning"
    finally:
        await runtime.shutdown()
