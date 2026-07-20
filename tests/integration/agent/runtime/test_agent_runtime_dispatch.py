from __future__ import annotations

import time
from dataclasses import replace

from agent_runtime_support import (
    ActiveChatState,
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

from shinbot.agent.coordinators.review.models import (
    ConsumedUnreadRange,
    ReviewSchedulerCommitIntent,
    ReviewSchedulerCommitKind,
)
from shinbot.agent.runtime.legacy_session_local_drain import (
    LegacySessionLocalDrainRequest,
)
from shinbot.agent.runtime.legacy_signal_admission import (
    LegacyAgentSignalFrozen,
    LegacyAgentSignalQuiescenceStatus,
)
from shinbot.agent.scheduler.models import ActiveChatDisposition, ReviewPlan
from shinbot.agent.services.model_runtime import ModelCallError
from shinbot.agent.signals import (
    AgentActiveChatBootstrapSignal,
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
    AgentTimerSignal,
)
from shinbot.core.dispatch.agent_identity import DEFAULT_SESSION_ACTOR_PROFILE_ID
from shinbot.core.dispatch.message_context import WaitingInputScope


@pytest.mark.asyncio
async def test_agent_runtime_registers_background_tasks_in_manager(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("test-bot")

    profile.review_due_timer.run_once = lambda: None  # type: ignore[assignment]
    profile.review_due_timer.start()

    assert runtime.task_manager.tasks(prefix=f"agent:{profile.bot_id or profile.profile_id}") != []


@pytest.mark.asyncio
async def test_profile_builds_an_unmounted_local_legacy_task_quiescer(tmp_path: Path) -> None:
    """Constructing the local observer must not start or reroute runtime work."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("test-bot")

    quiescer = profile.build_legacy_session_local_task_quiescer()
    report = await quiescer.quiesce_session_tasks("test-bot:group:room")

    assert report.locally_confirmed_quiescent is True
    assert "review_due_timer" in [
        observation.owner_name for observation in report.observations
    ]
    assert profile.active_chat_timer.active_sessions() == []
    assert profile.review_due_timer._task is None


@pytest.mark.asyncio
async def test_runtime_base_session_quiescer_includes_all_profiles(tmp_path: Path) -> None:
    """A shared legacy base session must not drain only the selected profile."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={
            "bot-a": {"agent": {"id": "agent-a"}},
            "bot-b": {"agent": {"id": "agent-b"}},
        },
    )

    report = await runtime.build_legacy_base_session_local_task_quiescer().quiesce_session_tasks(
        "test-bot:group:room"
    )

    assert report.locally_confirmed_quiescent is True
    assert [observation.profile_id for observation in report.observations] == [
        DEFAULT_SESSION_ACTOR_PROFILE_ID,
        "bot-a",
        "bot-b",
    ]


@pytest.mark.asyncio
async def test_runtime_builds_unmounted_local_drain_participant(tmp_path: Path) -> None:
    """Composition remains dormant until a future controller explicitly uses it."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:room"
    request = LegacySessionLocalDrainRequest(
        legacy_session_id=session_id,
        waiting_input_scope=WaitingInputScope.from_routing_identity(
            legacy_session_id=session_id,
        ),
        cutover_id="cutover-a",
    )

    participant = runtime.build_legacy_session_local_drain_participant(
        bot.message_ingress
    )
    ticket = participant.freeze(request)
    receipt = await participant.drain(ticket, timeout_seconds=0.5)

    assert receipt.locally_confirmed_quiescent is True
    assert participant.thaw(receipt) is True
    assert bot.message_ingress.legacy_ingress_freeze_ticket(session_id) is None


@pytest.mark.asyncio
async def test_runtime_signal_freeze_blocks_reentry_before_session_lock(tmp_path: Path) -> None:
    """A local lifecycle freeze sees and drains the real runtime entry call."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    entered = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    class BlockingScheduler:
        async def accept_signal(self, signal: AgentSignal) -> Any | None:
            calls.append(signal.signal_id)
            entered.set()
            await release.wait()
            return None

    runtime.agent_scheduler = BlockingScheduler()  # type: ignore[assignment]
    first = replace(make_signal(), signal_id="pre-freeze")
    first_task = asyncio.create_task(runtime.handle_agent_signal(first))
    await entered.wait()
    ticket = runtime.freeze_legacy_session_signal_admission(
        first.session_id,
        cutover_id="cutover-a",
    )

    with pytest.raises(LegacyAgentSignalFrozen, match="frozen"):
        await runtime.handle_agent_signal(replace(first, signal_id="post-freeze"))
    timed_out = await runtime.await_legacy_session_signal_quiescent(
        ticket,
        timeout_seconds=0.0,
    )

    assert timed_out.status is LegacyAgentSignalQuiescenceStatus.TIMED_OUT
    assert calls == ["pre-freeze"]
    release.set()
    await first_task
    quiescent = await runtime.await_legacy_session_signal_quiescent(
        ticket,
        timeout_seconds=0.5,
    )

    assert quiescent.quiescent
    assert runtime.thaw_legacy_session_signal_admission(ticket) is True


@pytest.mark.asyncio
async def test_runtime_runs_idle_planner_outside_session_lock_and_coalesces_ticks(
    tmp_path: Path,
) -> None:
    """A slow planner cannot block a duplicate exit signal for the same session."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    scheduler = profile.agent_scheduler
    session_id = "test-bot:group:room"
    scheduler._state_store.set_state(session_id, AgentState.REVIEW)
    completion = scheduler.complete_review(
        session_id,
        enter_active_chat=True,
        active_chat_initial_interest=4.0,
        now=60.0,
    )
    assert completion.active_chat_state is not None
    planner_started = asyncio.Event()
    release_planner = asyncio.Event()
    requests: list[object] = []

    async def blocked_planner(request: object) -> ReviewPlan:
        requests.append(request)
        planner_started.set()
        await release_planner.wait()
        return ReviewPlan(
            session_id=session_id,
            next_review_at=321.0,
            reason="runtime_fenced_model_plan",
            updated_at=61.0,
        )

    profile.plan_idle_review_after_active_chat = blocked_planner  # type: ignore[method-assign]
    signal = AgentSignal(
        signal_id="bootstrap-exit:one",
        kind=AgentSignalKind.ACTIVE_CHAT_BOOTSTRAP,
        source=AgentSignalSource.MANUAL,
        session_id=session_id,
        occurred_at=61.0,
        active_chat_bootstrap=AgentActiveChatBootstrapSignal(
            disposition=ActiveChatDisposition.EXIT_SOON,
            active_epoch=completion.active_chat_state.active_epoch,
            reason="test_exit",
        ),
    )

    try:
        first = asyncio.create_task(runtime.handle_agent_signal(signal))
        await asyncio.wait_for(planner_started.wait(), timeout=0.5)

        duplicate = await asyncio.wait_for(
            runtime.handle_agent_signal(replace(signal, signal_id="bootstrap-exit:two")),
            timeout=0.2,
        )

        assert duplicate is None
        assert len(requests) == 1
        release_planner.set()
        decision = await asyncio.wait_for(first, timeout=0.5)

        assert decision is not None
        assert decision.returned_to_idle is True
        assert scheduler.state_for(session_id) == AgentState.IDLE
        assert scheduler.review_plan_for(session_id).reason == "runtime_fenced_model_plan"
        assert runtime._idle_review_planning_requests == {}
        application_rows = [
            row
            for row in bot.database.audit.list_by_session(session_id)
            if row["command_name"] == "agent.idle_review_planning.application"
        ]
        assert len(application_rows) == 1
        assert application_rows[0]["metadata"] == {
            "profile_id": DEFAULT_SESSION_ACTOR_PROFILE_ID,
            "signal_id": "bootstrap-exit:one",
            "trigger": "active_chat_bootstrap",
            "active_epoch": completion.active_chat_state.active_epoch,
            "checked_at": 61.0,
            "outcome": "applied_model_plan",
            "reason": "runtime_fenced_model_plan",
            "model_plan_supplied": True,
            "model_plan_reason": "runtime_fenced_model_plan",
            "model_plan_next_review_at": 321.0,
            "decision_skipped_reason": "",
            "applied_plan_reason": "runtime_fenced_model_plan",
            "applied_next_review_at": 321.0,
            "scheduler_state": "idle",
        }
    finally:
        release_planner.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_runs_active_reply_model_outside_session_lock(
    tmp_path: Path,
) -> None:
    """A slow active reply cannot hold up later signals for its session."""

    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "reply handled"})
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    session_id = "test-bot:group:group:1"
    first_message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="active-reply-first",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot hello",
            content_json="[]",
            role="user",
            created_at=10_000.0,
            is_mentioned=True,
        )
    )
    second_message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="active-reply-second",
            sender_id="user-2",
            sender_name="User Two",
            raw_text="follow up while the model is working",
            content_json="[]",
            role="user",
            created_at=10_001.0,
        )
    )
    model_started = asyncio.Event()
    release_model = asyncio.Event()

    async def block_active_reply_model(call: Any) -> None:
        if call.purpose != "active_chat_fast":
            return
        model_started.set()
        await release_model.wait()

    model_runtime.on_generate = block_active_reply_model

    try:
        first_signal = asyncio.create_task(
            runtime.handle_agent_signal(
                make_signal(message_log_id=first_message_log_id, is_mentioned=True)
            )
        )
        await asyncio.wait_for(model_started.wait(), timeout=0.5)
        assert first_signal.done()
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.ACTIVE_REPLY

        task_prefix = f"agent:{profile.bot_id or profile.profile_id}:active_reply"
        active_reply_tasks = runtime.task_manager.tasks(prefix=task_prefix)
        assert len(active_reply_tasks) == 1

        await asyncio.wait_for(
            runtime.handle_agent_signal(
                AgentSignal(
                    signal_id="review-due-during-active-reply",
                    kind=AgentSignalKind.REVIEW_DUE,
                    source=AgentSignalSource.TIMER,
                    session_id=session_id,
                    occurred_at=10_001.0,
                    timer=AgentTimerSignal(
                        trigger=AgentSignalKind.REVIEW_DUE.value,
                        due_at=10_001.0,
                    ),
                )
            ),
            timeout=0.2,
        )
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.ACTIVE_REPLY

        await asyncio.wait_for(
            runtime.handle_agent_signal(make_signal(message_log_id=second_message_log_id)),
            timeout=0.2,
        )
        assert second_message_log_id in {
            message.message_log_id
            for message in runtime.agent_scheduler.unread_messages(session_id)
        }

        release_model.set()
        await asyncio.wait_for(active_reply_tasks[0], timeout=0.5)
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.IDLE
        assert first_message_log_id not in {
            message.message_log_id
            for message in runtime.agent_scheduler.unread_messages(session_id)
        }
        assert second_message_log_id in {
            message.message_log_id
            for message in runtime.agent_scheduler.unread_messages(session_id)
        }
    finally:
        release_model.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_refreshes_plan_after_successful_active_reply(
    tmp_path: Path,
) -> None:
    """A drained active reply supersedes the plan created at message ingress."""

    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "reply handled"})
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    scheduler_now = 10.0
    profile.agent_scheduler._now = lambda: scheduler_now
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="active-reply-plan-refresh",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot hello",
            content_json="[]",
            role="user",
            created_at=10_000.0,
            is_mentioned=True,
        )
    )

    try:
        await runtime.handle_agent_signal(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        initial_plan = profile.agent_scheduler.review_plan_for(session_id)
        assert initial_plan is not None

        scheduler_now = 20.0
        task_prefix = f"agent:{profile.bot_id or profile.profile_id}:active_reply"
        active_reply_tasks = runtime.task_manager.tasks(prefix=task_prefix)
        assert len(active_reply_tasks) == 1
        await asyncio.wait_for(active_reply_tasks[0], timeout=0.5)

        refreshed_plan = profile.agent_scheduler.review_plan_for(session_id)
        assert refreshed_plan is not None
        assert refreshed_plan.next_review_at == 920.0
        assert refreshed_plan.next_review_at > initial_plan.next_review_at
        assert profile.agent_scheduler.state_for(session_id) == AgentState.IDLE
        assert profile.agent_scheduler.unread_messages(session_id) == []

        stale_due = profile.agent_scheduler.prepare_due_review(
            session_id,
            now=initial_plan.next_review_at,
        )
        assert stale_due.skipped_reason == "review_not_due"
        assert profile.agent_scheduler.state_for(session_id) == AgentState.IDLE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_preserves_active_reply_input_after_model_failure(
    tmp_path: Path,
) -> None:
    """A failed immediate decision leaves its exact input available to review."""

    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime([])
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="active-reply-failure",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot hello",
            content_json="[]",
            role="user",
            created_at=10_000.0,
            is_mentioned=True,
        )
    )

    async def fail_active_reply_model(call: Any) -> None:
        if call.purpose == "active_chat_fast":
            raise ModelCallError("test active reply model failure")

    model_runtime.on_generate = fail_active_reply_model

    try:
        await runtime.handle_agent_signal(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        task_prefix = f"agent:{profile.bot_id or profile.profile_id}:active_reply"
        active_reply_tasks = runtime.task_manager.tasks(prefix=task_prefix)

        assert len(active_reply_tasks) == 1
        await asyncio.wait_for(active_reply_tasks[0], timeout=0.5)
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.IDLE
        assert message_log_id in {
            message.message_log_id
            for message in runtime.agent_scheduler.unread_messages(session_id)
        }
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_fences_active_chat_round_commit_and_plans_outside_lock(
    tmp_path: Path,
) -> None:
    """A round result cannot mutate scheduling state outside the runtime fence."""

    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "exit_active",
                        {"reason": "conversation has ended"},
                    )
                ]
            ),
            make_generate_result(
                text='{"next_review_after_seconds": 120, "reason": "round_settled"}'
            ),
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="active-chat-fenced-round",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot goodbye",
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
        active_epoch=18,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )
    runtime.active_chat_workflow.update_attention_config(
        replace(
            runtime.active_chat_workflow.attention_config,
            semantic_wait_ms=60_000.0,
        )
    )
    round_model_started = asyncio.Event()
    release_round_model = asyncio.Event()
    planner_started = asyncio.Event()
    release_planner = asyncio.Event()

    async def block_round_commit_boundary(call: Any) -> None:
        if call.purpose == "active_chat_fast":
            round_model_started.set()
            await release_round_model.wait()
        elif call.purpose == "idle_review_planning":
            planner_started.set()
            await release_planner.wait()

    model_runtime.on_generate = block_round_commit_boundary
    flush_task: asyncio.Task[None] | None = None
    try:
        await runtime.handle_agent_signal(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )

        async with runtime._session_signal_lock(session_id):
            flush_task = asyncio.create_task(
                runtime.active_chat_workflow.flush_now(
                    scheduler=runtime.agent_scheduler,
                    session_id=session_id,
                )
            )
            await asyncio.wait_for(round_model_started.wait(), timeout=0.5)
            release_round_model.set()
            await asyncio.sleep(0.02)

            assert {
                message.message_log_id
                for message in runtime.agent_scheduler.unread_messages(session_id)
            } == {message_log_id}
            assert planner_started.is_set() is False
            assert runtime._idle_review_planning_requests == {}

        await asyncio.wait_for(planner_started.wait(), timeout=0.5)
        assert len(runtime._idle_review_planning_requests) == 1
        await asyncio.wait_for(
            runtime.handle_agent_signal(
                AgentSignal(
                    signal_id="review-due-during-active-chat-planner",
                    kind=AgentSignalKind.REVIEW_DUE,
                    source=AgentSignalSource.TIMER,
                    session_id=session_id,
                    occurred_at=20_001.0,
                    timer=AgentTimerSignal(
                        trigger=AgentSignalKind.REVIEW_DUE.value,
                        due_at=20_001.0,
                    ),
                )
            ),
            timeout=0.2,
        )
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.ACTIVE_CHAT

        release_planner.set()
        await asyncio.wait_for(flush_task, timeout=0.5)
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.IDLE
        assert runtime.agent_scheduler.unread_messages(session_id) == []
        assert runtime.agent_scheduler.review_plan_for(session_id).reason == "round_settled"
        assert runtime._idle_review_planning_requests == {}
    finally:
        release_round_model.set()
        release_planner.set()
        if flush_task is not None:
            await asyncio.gather(flush_task, return_exceptions=True)
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_fences_review_scheduler_commit(
    tmp_path: Path,
) -> None:
    """Review completion and range consumption wait for the runtime mutex."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    coordinator = profile.review_coordinator
    assert coordinator is not None
    commit_handler = coordinator._scheduler_commit_handler
    assert commit_handler is not None
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="review-fenced-commit",
            sender_id="user-1",
            sender_name="User",
            raw_text="one review message",
            content_json="[]",
            role="user",
            created_at=30_000.0,
        )
    )
    try:
        await runtime.handle_agent_signal(make_signal(message_log_id=message_log_id))
        review_plan = ReviewPlan(
            session_id=session_id,
            next_review_at=60.0,
            reason="review_fence_test",
            updated_at=10.0,
        )
        profile.agent_scheduler._state_store.set_state(session_id, AgentState.REVIEW)
        profile.agent_scheduler._state_store.set_review_plan(review_plan)
        intent = ReviewSchedulerCommitIntent(
            kind=ReviewSchedulerCommitKind.COMPLETE_REVIEW,
            session_id=session_id,
            review_run_id="review-fence-test",
            expected_review_plan=review_plan,
            consumed_ranges=(
                ConsumedUnreadRange(
                    range_id=None,
                    session_id=session_id,
                    start_msg_log_id=message_log_id,
                    end_msg_log_id=message_log_id,
                    message_count=1,
                ),
            ),
            enter_active_chat=True,
            active_chat_initial_interest=15.0,
            active_chat_decay_half_life_seconds=20.0,
        )

        async with runtime._session_signal_lock(session_id):
            commit_task = asyncio.create_task(commit_handler(intent))
            await asyncio.sleep(0.02)
            assert profile.agent_scheduler.state_for(session_id) == AgentState.REVIEW
            assert {
                message.message_log_id
                for message in profile.agent_scheduler.unread_messages(session_id)
            } == {message_log_id}

        decision = await asyncio.wait_for(commit_task, timeout=0.5)
        assert decision.accepted is True
        assert decision.completion is not None
        assert decision.completion.active_chat_started is True
        assert profile.agent_scheduler.state_for(session_id) == AgentState.ACTIVE_CHAT
        assert profile.agent_scheduler.unread_messages(session_id) == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_commits_deferred_review_to_idle_with_retry_plan(
    tmp_path: Path,
) -> None:
    """A failed review must retain work and schedule a near-term idle retry."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    coordinator = profile.review_coordinator
    assert coordinator is not None
    commit_handler = coordinator._scheduler_commit_handler
    assert commit_handler is not None
    session_id = "test-bot:group:deferred-review"
    current_plan = ReviewPlan(
        session_id=session_id,
        next_review_at=60.0,
        reason="review_fence_test",
        updated_at=10.0,
    )
    retry_plan = ReviewPlan(
        session_id=session_id,
        next_review_at=90.0,
        reason="review_deferred_consumption_retry",
        updated_at=60.0,
    )
    scheduler = profile.agent_scheduler
    scheduler._state_store.set_state(session_id, AgentState.REVIEW)
    scheduler._state_store.set_review_plan(current_plan)
    try:
        decision = await commit_handler(
            ReviewSchedulerCommitIntent(
                kind=ReviewSchedulerCommitKind.COMPLETE_REVIEW,
                session_id=session_id,
                review_run_id="deferred-review-test",
                expected_review_plan=current_plan,
                next_review_plan=retry_plan,
                enter_active_chat=False,
            )
        )

        assert decision.accepted is True
        assert decision.completion is not None
        assert decision.completion.returned_to_idle is True
        assert scheduler.state_for(session_id) == AgentState.IDLE
        persisted_retry_plan = scheduler.review_plan_for(session_id)
        assert persisted_retry_plan is not None
        assert persisted_retry_plan.next_review_at == 90.0
        assert persisted_retry_plan.reason == "review_deferred_consumption_retry"
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_rejects_stale_review_scheduler_commit(
    tmp_path: Path,
) -> None:
    """An old review model result cannot complete a replacement review plan."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    coordinator = profile.review_coordinator
    assert coordinator is not None
    commit_handler = coordinator._scheduler_commit_handler
    assert commit_handler is not None
    session_id = "test-bot:group:group:1"
    expected_plan = ReviewPlan(
        session_id=session_id,
        next_review_at=60.0,
        reason="old_review",
        updated_at=10.0,
    )
    replacement_plan = replace(
        expected_plan,
        next_review_at=90.0,
        reason="replacement_review",
        updated_at=20.0,
    )
    profile.agent_scheduler._state_store.set_state(session_id, AgentState.REVIEW)
    profile.agent_scheduler._state_store.set_review_plan(replacement_plan)
    try:
        decision = await commit_handler(
            ReviewSchedulerCommitIntent(
                kind=ReviewSchedulerCommitKind.COMPLETE_REVIEW,
                session_id=session_id,
                review_run_id="stale-review-fence-test",
                expected_review_plan=expected_plan,
                enter_active_chat=True,
                active_chat_initial_interest=15.0,
                active_chat_decay_half_life_seconds=20.0,
            )
        )

        assert decision.accepted is False
        assert decision.skipped_reason == "review_plan_changed"
        assert decision.completion is not None
        assert decision.completion.skipped_reason == "review_plan_changed"
        assert profile.agent_scheduler.state_for(session_id) == AgentState.REVIEW
        assert profile.agent_scheduler.review_plan_for(session_id) == replacement_plan
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_accepts_review_commit_after_state_timestamp_refresh(
    tmp_path: Path,
) -> None:
    """A REVIEW transition must not invalidate the plan it is executing."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    scheduler = profile.agent_scheduler
    session_id = "test-bot:group:review-plan-timestamp"
    expected_plan = ReviewPlan(
        session_id=session_id,
        next_review_at=100.0,
        reason="timestamp_fence_regression",
        updated_at=10.0,
    )
    scheduler._state_store.set_review_plan(expected_plan)
    scheduler._state_store.set_state(session_id, AgentState.REVIEW)
    persisted_plan = scheduler.review_plan_for(session_id)
    assert persisted_plan is not None
    assert persisted_plan.updated_at != expected_plan.updated_at

    try:
        decision = await profile._commit_review_scheduler_mutation_from_task(
            ReviewSchedulerCommitIntent(
                kind=ReviewSchedulerCommitKind.COMPLETE_REVIEW,
                session_id=session_id,
                review_run_id="timestamp-fence-regression",
                expected_review_plan=expected_plan,
                enter_active_chat=True,
                active_chat_initial_interest=15.0,
                active_chat_decay_half_life_seconds=20.0,
            )
        )

        assert decision.accepted is True
        assert decision.completion is not None
        assert decision.completion.active_chat_started is True
        assert scheduler.state_for(session_id) == AgentState.ACTIVE_CHAT
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_fences_force_idle_management_mutation(
    tmp_path: Path,
) -> None:
    """Management state changes share the same session mutex as workflow commits."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=30.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=27,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    try:
        async with runtime._session_signal_lock(session_id):
            force_idle_task = asyncio.create_task(runtime.force_idle(session_id))
            await asyncio.sleep(0.02)
            assert runtime.agent_scheduler.state_for(session_id) == AgentState.ACTIVE_CHAT

        assert await asyncio.wait_for(force_idle_task, timeout=0.5) is True
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.IDLE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_composes_actor_v2_diagnostics_without_claiming_traffic(
    tmp_path: Path,
) -> None:
    """Diagnostic composition must not publish a second ingress writer."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={"bot-a": {"agent": {"id": "agent-a"}}},
    )

    diagnostics = runtime.actor_v2_diagnostics
    graph = runtime.actor_v2_handler_graph

    assert diagnostics is not None
    assert graph is not None
    assert diagnostics.actor_wake_target_available is False
    assert diagnostics.effects_running is False
    assert diagnostics.shutdown_complete is False
    assert "actor_v2_durable_isolation_lease_unavailable" in (
        diagnostics.readiness.activation_blockers
    )
    assert "actor_v2_ownership_ingress_cutover_controller_unavailable" in (
        diagnostics.readiness.activation_blockers
    )
    assert "actor_v2_legacy_state_handoff_manifest_unavailable" in (
        diagnostics.readiness.activation_blockers
    )
    assert diagnostics.readiness.clean_session_handler_graph_complete is True
    assert diagnostics.readiness.clean_session_handler_failures == ()
    assert not hasattr(diagnostics, "recovery_scanner")
    assert not hasattr(diagnostics, "recovery_commit_coordinator")
    assert not hasattr(diagnostics, "handler_registry")
    assert not hasattr(runtime, "actor_wake_target")
    assert not hasattr(runtime, "session_actor_registry")
    assert diagnostics.recovery_materialization_states == (
        "active_chat",
        "active_chat_settling",
        "active_reply",
        "review",
    )
    assert [snapshot.status.value for snapshot in diagnostics.background_service_health] == [
        "stopped",
        "stopped",
    ]
    assert graph.profile_ids == (DEFAULT_SESSION_ACTOR_PROFILE_ID, "bot-a")
    supported_refs = {
        (contract.effect_kind, contract.version) for contract in graph.supported_contracts
    }
    assert ("cancel_model_execution", 3) in supported_refs
    assert ("run_idle_review_planning", 3) in supported_refs
    missing_kinds = {
        contract.effect_kind for contract in diagnostics.readiness.missing_handler_contracts
    }
    assert missing_kinds == {
        "active_chat_runtime_reconciliation",
        "cancel_idle_review_planning",
        "cancel_review_workflow",
        "idle_review_planning_cancellation_reconciliation",
        "run_active_chat_bootstrap",
        "run_active_chat_round",
        "stop_active_chat_runtime",
    }
    missing_refs = {
        (contract.effect_kind, contract.version)
        for contract in diagnostics.readiness.missing_handler_contracts
    }
    assert ("cancel_review_workflow", 1) in missing_refs
    assert ("cancel_review_workflow", 2) not in missing_refs
    assert len(missing_refs) == 13

    await runtime.start_background_tasks()

    assert diagnostics.effects_running is False
    assert [snapshot.status.value for snapshot in diagnostics.background_service_health] == [
        "stopped",
        "stopped",
    ]
    with bot.database.connect() as conn:
        ownership_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_runtime_ownership"
        ).fetchone()[0]
    assert ownership_count == 0

    await runtime.shutdown()

    closed_diagnostics = runtime.actor_v2_diagnostics
    assert closed_diagnostics is not None
    assert closed_diagnostics.closed is True


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

    await runtime.handle_agent_signal(make_signal(bot_id="bot-a"))
    await runtime.handle_agent_signal(make_signal(bot_id="bot-b"))

    assert runtime.agent_profile_for_bot("bot-a").profile_id == "bot-a"
    assert runtime.agent_profile_for_bot("bot-a").config.agent_id == "agent-a"
    assert runtime.agent_profile_for_bot("bot-b").profile_id == DEFAULT_SESSION_ACTOR_PROFILE_ID
    assert (
        runtime.agent_profile_for_bot(
            "bot-a"
        ).config.active_chat_policy_config.initial_interest_value
        == 77
    )
    assert [signal.bot_id for signal in bot_a_scheduler.calls] == ["bot-a"]
    assert [signal.bot_id for signal in default_scheduler.calls] == ["bot-b"]


def test_agent_runtime_shared_config_does_not_share_durable_profile(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={
            "bot-a": {"agent": {"id": "shared-agent"}},
            "bot-b": {"agent": {"id": "shared-agent"}},
        },
    )

    bot_a = runtime.agent_profile_for_bot("bot-a")
    bot_b = runtime.agent_profile_for_bot("bot-b")

    assert bot_a is not bot_b
    assert bot_a.profile_id == "bot-a"
    assert bot_b.profile_id == "bot-b"
    assert bot_a.config.agent_id == bot_b.config.agent_id == "shared-agent"


def test_agent_runtime_default_profile_uses_reserved_durable_identity(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_config={"agent": {"id": "editable-default-agent"}},
    )

    profile = runtime.agent_profile_for_bot("")

    assert profile.profile_id == DEFAULT_SESSION_ACTOR_PROFILE_ID
    assert profile.bot_id == ""
    assert profile.config.agent_id == "editable-default-agent"


@pytest.mark.asyncio
async def test_agent_runtime_starts_background_timers_only_for_bot_profiles(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={
            "bot-a": {
                "agent": {
                    "id": "agent-a",
                }
            }
        },
    )

    await runtime.start_background_tasks()

    default_tasks = runtime.task_manager.tasks(
        prefix=f"agent:{DEFAULT_SESSION_ACTOR_PROFILE_ID}:review_due_timer"
    )
    bot_tasks = runtime.task_manager.tasks(prefix="agent:bot-a:review_due_timer")
    assert default_tasks == []
    assert bot_tasks != []
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_startup_recovery_uses_session_mutation_lock(
    tmp_path: Path,
) -> None:
    """Startup recovery must not bypass the runtime mutation boundary."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    scheduler = profile.agent_scheduler
    session_id = "test-bot:group:recovery-room"
    scheduler._state_store.set_state(session_id, AgentState.REVIEW)
    scheduler._state_store.set_review_plan(
        ReviewPlan(
            session_id=session_id,
            next_review_at=200.0,
            reason="interrupted_review",
            updated_at=100.0,
        )
    )
    calls: list[str] = []
    reconcile_transient = scheduler.reconcile_transient_session
    reconcile_active_chat = scheduler.reconcile_active_chat_session

    def locked_reconcile_transient(
        recovered_session_id: str,
        *,
        now: float | None = None,
    ) -> bool:
        assert recovered_session_id == session_id
        assert runtime._session_signal_lock(recovered_session_id).locked()
        assert profile.review_due_timer._task is None
        calls.append("transient")
        return reconcile_transient(recovered_session_id, now=now)

    def locked_reconcile_active_chat(
        recovered_session_id: str,
        *,
        now: float | None = None,
    ) -> Any:
        assert recovered_session_id == session_id
        assert runtime._session_signal_lock(recovered_session_id).locked()
        assert profile.review_due_timer._task is None
        calls.append("active_chat")
        return reconcile_active_chat(recovered_session_id, now=now)

    scheduler.reconcile_transient_session = locked_reconcile_transient  # type: ignore[method-assign]
    scheduler.reconcile_active_chat_session = locked_reconcile_active_chat  # type: ignore[method-assign]

    await runtime.start_background_tasks()

    assert calls == ["transient", "active_chat"]
    assert scheduler.state_for(session_id) == AgentState.IDLE
    assert profile.review_due_timer._task is not None
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_startup_recovery_rehydrates_active_chat_workflow(
    tmp_path: Path,
) -> None:
    """A durable ACTIVE_CHAT state must not restart without its workflow state."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    scheduler = profile.agent_scheduler
    session_id = "test-bot:group:active-chat-recovery"
    now = time.time()
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=100.0,
        decay_half_life_seconds=86_400.0,
        entered_at=now,
        updated_at=now,
        active_epoch=73,
    )
    scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    scheduler._state_store.set_active_chat_state(active_state)

    await runtime.start_background_tasks()

    attention_state = profile.active_chat_workflow.attention_state_for(session_id)
    assert attention_state is not None
    assert attention_state.active_epoch == active_state.active_epoch
    assert profile.active_chat_timer.active_sessions() == [session_id]
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_startup_recovery_exits_active_chat_when_restore_fails(
    tmp_path: Path,
) -> None:
    """A failed local restore must not leave a scheduler-only active chat behind."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    scheduler = profile.agent_scheduler
    session_id = "test-bot:group:failed-active-chat-recovery"
    now = time.time()
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=100.0,
        decay_half_life_seconds=86_400.0,
        entered_at=now,
        updated_at=now,
        active_epoch=74,
    )
    scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    scheduler._state_store.set_active_chat_state(active_state)

    async def fail_restore(**_kwargs: Any) -> None:
        raise RuntimeError("restore failed")

    profile.restore_active_chat_session = fail_restore  # type: ignore[method-assign]

    await runtime.start_background_tasks()

    assert scheduler.state_for(session_id) == AgentState.IDLE
    assert scheduler.active_chat_state_for(session_id) is None
    assert profile.active_chat_workflow.attention_state_for(session_id) is None
    assert profile.active_chat_timer.active_sessions() == []
    await runtime.shutdown()


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
    assert runtime_prompt.read_text(encoding="utf-8").endswith("用户自定义 review scan prompt。\n")


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
        runtime.agent_profile_for_bot("bot-a").active_chat_workflow.attention_config.base_threshold
        == 10.0
    )

    runtime.set_active_chat_threshold_delta(0.0, source="test")

    assert runtime.active_chat_workflow.attention_config.base_threshold == 5.0
    assert (
        runtime.agent_profile_for_bot("bot-a").active_chat_workflow.attention_config.base_threshold
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

    await runtime.handle_agent_signal(make_signal())
    await runtime.handle_agent_signal(make_signal(is_mentioned=True))
    await runtime.handle_agent_signal(make_signal(is_private=True))

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

    await runtime.handle_agent_signal(make_signal(is_reply_to_bot=True))
    await runtime.handle_agent_signal(make_signal(is_mentioned=True, is_private=False))
    await runtime.handle_agent_signal(make_signal(message_log_id=None))

    assert [call["response_profile"] for call in dispatcher.calls] == [
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

    await runtime.handle_agent_signal(make_signal())

    assert dispatcher.calls == []
    assert [
        item.message_log_id
        for item in runtime.agent_scheduler.unread_messages("test-bot:group:group:1")
    ] == [123]


@pytest.mark.asyncio
async def test_agent_runtime_serializes_same_session_signals(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    entered = asyncio.Event()
    release = asyncio.Event()
    order: list[str] = []

    class SerialScheduler:
        async def accept_signal(self, signal: AgentSignal) -> Any | None:
            order.append(f"start:{signal.signal_id}")
            if signal.signal_id == "first":
                entered.set()
                await release.wait()
            order.append(f"end:{signal.signal_id}")
            return None

    runtime.agent_scheduler = SerialScheduler()  # type: ignore[assignment]
    first = replace(make_signal(), signal_id="first")
    second = replace(make_signal(message_log_id=456), signal_id="second")

    first_task = asyncio.create_task(runtime.handle_agent_signal(first))
    await entered.wait()
    second_task = asyncio.create_task(runtime.handle_agent_signal(second))
    await asyncio.sleep(0.05)
    assert order == ["start:first"]
    release.set()
    await asyncio.gather(first_task, second_task)
    assert order == ["start:first", "end:first", "start:second", "end:second"]


@pytest.mark.asyncio
async def test_agent_runtime_high_priority_message_interrupts_and_resumes_review(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("")
    dispatcher = profile._workflow_dispatcher
    session_id = "test-bot:group:group:1"
    first_review_started = asyncio.Event()
    first_review_cancelled = asyncio.Event()
    release_first_review_tail = asyncio.Event()
    resumed_review_started = asyncio.Event()
    unread_batches: list[list[int]] = []

    class BlockingReviewCoordinator:
        async def run(self, **kwargs: Any) -> Any:
            unread_batches.append([message.message_log_id for message in kwargs["unread_messages"]])
            started = first_review_started if len(unread_batches) == 1 else resumed_review_started
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if len(unread_batches) == 1:
                    first_review_cancelled.set()
                    await release_first_review_tail.wait()
                    scheduler = kwargs["scheduler"]
                    unread_range = scheduler.unread_ranges(session_id)[0]
                    scheduler.split_review_consumed(
                        range_id=unread_range.id,
                        consumed_start_msg_log_id=first_message_log_id,
                        consumed_end_msg_log_id=first_message_log_id,
                    )
                    assert first_message_log_id not in {
                        message.message_log_id
                        for message in scheduler.unread_messages(session_id)
                    }
                raise

    class ImmediateActiveReplyWorkflow:
        async def start_active_chat(self, **_kwargs: Any) -> None:
            return None

        async def notify_message(self, **_kwargs: Any) -> None:
            return None

        def attention_state_for(self, _session_id: str) -> None:
            return None

        async def flush_now(self, **_kwargs: Any) -> None:
            return None

        def stop_active_chat(self, _session_id: str) -> None:
            return None

        def active_session_ids(self) -> list[str]:
            return []

    dispatcher._review_coordinator = BlockingReviewCoordinator()
    dispatcher._active_chat_workflow = ImmediateActiveReplyWorkflow()

    try:
        first_message_log_id = bot.database.message_logs.insert(
            MessageLogRecord(
                session_id=session_id,
                platform_msg_id="platform-review-1",
                sender_id="user-1",
                sender_name="User",
                raw_text="ordinary message",
                content_json="[]",
                role="user",
                created_at=10.0,
            )
        )
        second_message_log_id = bot.database.message_logs.insert(
            MessageLogRecord(
                session_id=session_id,
                platform_msg_id="platform-review-2",
                sender_id="user-1",
                sender_name="User",
                raw_text="@bot interrupt",
                content_json="[]",
                role="user",
                created_at=20.0,
                is_mentioned=True,
            )
        )
        await runtime.handle_agent_signal(make_signal(message_log_id=first_message_log_id))
        review_plan = runtime.agent_scheduler.review_plan_for(session_id)
        assert review_plan is not None
        runtime.agent_scheduler._state_store.set_review_plan(
            replace(review_plan, next_review_at=20.0)
        )
        review_due = AgentSignal(
            signal_id="review-due",
            kind=AgentSignalKind.REVIEW_DUE,
            source=AgentSignalSource.TIMER,
            session_id=session_id,
            occurred_at=20.0,
            timer=AgentTimerSignal(
                trigger=AgentSignalKind.REVIEW_DUE.value,
                due_at=20.0,
            ),
        )

        await asyncio.wait_for(runtime.handle_agent_signal(review_due), timeout=1.0)
        await asyncio.wait_for(first_review_started.wait(), timeout=1.0)
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.REVIEW

        interrupt_task = asyncio.create_task(
            runtime.handle_agent_signal(
                make_signal(message_log_id=second_message_log_id, is_mentioned=True)
            )
        )
        await asyncio.wait_for(first_review_cancelled.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert resumed_review_started.is_set() is False
        assert interrupt_task.done() is True

        release_first_review_tail.set()
        await asyncio.wait_for(interrupt_task, timeout=1.0)
        await asyncio.wait_for(resumed_review_started.wait(), timeout=1.0)

        assert runtime.agent_scheduler.state_for(session_id) == AgentState.REVIEW
        assert runtime.agent_scheduler.high_priority_events(session_id) == []
        assert unread_batches == [
            [first_message_log_id],
            [second_message_log_id],
        ]
        assert (
            len(
                runtime.task_manager.tasks(
                    prefix=f"agent:{profile.bot_id or profile.profile_id}:review_workflow"
                )
            )
            == 1
        )
    finally:
        await runtime.shutdown()


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
        await runtime.handle_agent_signal(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0 + 0.1
        )

        assert len(model_runtime.calls) == 1
        call = model_runtime.calls[0]
        assert call.purpose == "active_chat_fast"
        assert call.route_id == "route-main"
        assert call.metadata["message_log_ids"] == [message_log_id]
        assert call.metadata["explicit_prompt_cache_enabled"] is True
        assert {tool["function"]["name"] for tool in call.tools} >= {
            "send_reply",
            "no_reply",
            "send_poke",
            "send_reaction",
            "exit_active",
        }
        assert "request_think_mode" not in {tool["function"]["name"] for tool in call.tools}
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
        await runtime.handle_agent_signal(
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
        await runtime.handle_agent_signal(
            make_signal(message_log_id=second_message_log_id, is_mentioned=True)
        )

    model_runtime.on_generate = inject_pending_message

    try:
        await runtime.handle_agent_signal(
            make_signal(message_log_id=first_message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0 + 0.1
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
        assert state.conversation_messages[0]["tool_calls"][0]["function"]["name"] == ("no_reply")
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
        await runtime.handle_agent_signal(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0 + 0.1
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
        [make_generate_result(text='{"next_review_after_seconds": 120, "reason": "timer_settled"}')]
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


@pytest.mark.asyncio
async def test_agent_runtime_idle_review_planning_uses_observed_message_count_metadata(
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
    message_log_id = runtime.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="msg-1",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot hello",
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
        active_epoch=9,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_signal(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0 + 0.1
        )

        assert len(model_runtime.calls) == 2
        metadata = model_runtime.calls[1].metadata
        assert model_runtime.calls[1].purpose == "idle_review_planning"
        assert metadata["review_stage_metadata"]["observed_message_count"] == 1
        assert metadata["review_stage_metadata"]["trace_message_count"] >= 0
        assert metadata["review_stage_metadata"]["message_log_ids"] == [message_log_id]
        assert metadata["observed_message_count"] == 1
        assert metadata["trace_message_count"] >= 0
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_review_due_signal_runs_due_review(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    calls: list[AgentSignal] = []

    class _RecordingReviewScheduler:
        async def accept_signal(self, signal: AgentSignal):
            calls.append(signal)
            return None

    runtime.agent_scheduler = _RecordingReviewScheduler()  # type: ignore[assignment]

    decision = await runtime.handle_agent_signal(
        AgentSignal(
            signal_id="review:test-bot:group:group:1",
            kind=AgentSignalKind.REVIEW_DUE,
            source=AgentSignalSource.TIMER,
            session_id=session_id,
            occurred_at=200.0,
            timer=AgentTimerSignal(trigger=AgentSignalKind.REVIEW_DUE.value, due_at=190.0),
        )
    )

    assert decision is None
    assert [signal.kind for signal in calls] == [AgentSignalKind.REVIEW_DUE]
    assert [signal.session_id for signal in calls] == [session_id]
    assert [signal.timer.due_at for signal in calls if signal.timer is not None] == [190.0]


@pytest.mark.asyncio
async def test_agent_runtime_active_chat_bootstrap_signal_applies_disposition(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=10.0,
        decay_half_life_seconds=5.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=9,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)

    decision = await runtime.handle_agent_signal(
        AgentSignal(
            signal_id="bootstrap:test-bot:group:group:1",
            kind=AgentSignalKind.ACTIVE_CHAT_BOOTSTRAP,
            source=AgentSignalSource.MANUAL,
            session_id=session_id,
            occurred_at=15.0,
            active_chat_bootstrap=AgentActiveChatBootstrapSignal(
                disposition=ActiveChatDisposition.ENGAGED,
                active_epoch=9,
                reason="test",
            ),
        )
    )

    assert decision is not None
    assert decision.bootstrap_applied is True
    assert runtime.agent_scheduler.state_for(session_id) == AgentState.ACTIVE_CHAT


@pytest.mark.asyncio
async def test_agent_runtime_only_skips_timer_signals_when_session_platform_unavailable(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    calls: list[AgentSignal] = []

    class _RecordingScheduler:
        async def accept_signal(self, signal: AgentSignal):
            calls.append(signal)
            return None

    runtime.agent_scheduler = _RecordingScheduler()  # type: ignore[assignment]
    runtime.should_pause_session = lambda _session_id: True  # type: ignore[method-assign]

    message_decision = await runtime.handle_agent_signal(make_signal())
    timer_decision = await runtime.handle_agent_signal(
        AgentSignal(
            signal_id="review-due:test-bot:group:group:1:10",
            kind=AgentSignalKind.REVIEW_DUE,
            source=AgentSignalSource.TIMER,
            session_id="test-bot:group:group:1",
            occurred_at=10.0,
            timer=AgentTimerSignal(
                trigger=AgentSignalKind.REVIEW_DUE.value,
                due_at=10.0,
            ),
        )
    )

    assert message_decision is None
    assert timer_decision is None
    assert [signal.kind for signal in calls] == [AgentSignalKind.MESSAGE]
