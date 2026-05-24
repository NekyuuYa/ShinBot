from __future__ import annotations

from agent_scheduler_support import (
    ActiveReplyDispatcher,
    AgentScheduler,
    AgentSchedulerConfig,
    AgentState,
    AlwaysWakePriorityPolicy,
    FixedReviewPolicy,
    HighPriorityEventKind,
    InMemoryAgentInbox,
    InMemoryAgentStateStore,
    RecordingActiveChatTimer,
    RecordingWorkflowDispatcher,
    ReviewDueTimerService,
    ReviewPlan,
    make_review_due_signal,
    make_signal,
    pytest,
)

from shinbot.agent.signals import AgentSignal, AgentSignalKind, AgentSignalSource


@pytest.mark.asyncio
async def test_scheduler_records_ordinary_message_without_workflow() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
    )

    decision = await scheduler.accept_signal(make_signal())

    assert decision.accepted is True
    assert decision.state == AgentState.IDLE
    assert decision.active_reply_started is False
    assert dispatcher.calls == []
    assert [item.message_log_id for item in scheduler.unread_messages("bot:group:room")] == [1]
    assert scheduler.high_priority_events("bot:group:room") == []
    assert scheduler.review_plan_for("bot:group:room").reason == "default_idle_review_interval"


@pytest.mark.asyncio
async def test_scheduler_starts_active_reply_for_mention() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "immediate",
    )

    decision = await scheduler.accept_signal(make_signal(is_mentioned=True))

    assert decision.accepted is True
    assert decision.state == AgentState.ACTIVE_REPLY
    assert decision.active_reply_started is True
    assert [event.kind for event in decision.high_priority_events] == [
        HighPriorityEventKind.MENTION
    ]
    assert dispatcher.calls[0]["response_profile"] == "immediate"
    assert dispatcher.calls[0]["is_mentioned"] is True


@pytest.mark.asyncio
async def test_concrete_active_reply_dispatcher_noops_back_to_idle() -> None:
    dispatcher = ActiveReplyDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "immediate",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )

    decision = await scheduler.accept_signal(make_signal(is_mentioned=True))

    assert decision.active_reply_started is True
    assert decision.state == AgentState.IDLE
    assert scheduler.state_for("bot:group:room") == AgentState.IDLE
    assert scheduler.high_priority_events("bot:group:room") == []


@pytest.mark.asyncio
async def test_scheduler_can_require_repeated_mentions_before_wake() -> None:
    now = 10.0
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        config=AgentSchedulerConfig(mention_wake_count=2, mention_wake_window_seconds=60),
        now=lambda: now,
    )

    first = await scheduler.accept_signal(make_signal(message_log_id=1, is_mentioned=True))
    second = await scheduler.accept_signal(make_signal(message_log_id=2, is_mentioned=True))

    assert first.active_reply_started is False
    assert second.active_reply_started is True
    assert [call["message_log_id"] for call in dispatcher.calls] == [2]
    assert [event.kind for event in scheduler.high_priority_events("bot:group:room")] == [
        HighPriorityEventKind.MENTION,
        HighPriorityEventKind.MENTION,
    ]


@pytest.mark.asyncio
async def test_scheduler_uses_injected_inbox_and_state_store() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    inbox = InMemoryAgentInbox()
    state_store = InMemoryAgentStateStore()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "immediate",
        inbox=inbox,
        state_store=state_store,
    )

    await scheduler.accept_signal(make_signal(is_reply_to_bot=True))

    assert state_store.get_state("bot:group:room") == AgentState.ACTIVE_REPLY
    assert [item.message_log_id for item in inbox.list_unread("bot:group:room")] == [1]
    assert [event.kind for event in inbox.list_high_priority_events("bot:group:room")] == [
        HighPriorityEventKind.REPLY_TO_BOT
    ]


@pytest.mark.asyncio
async def test_scheduler_uses_injected_priority_policy() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "immediate",
        priority_policy=AlwaysWakePriorityPolicy(),
    )

    decision = await scheduler.accept_signal(make_signal())

    assert decision.active_reply_started is True
    assert [event.kind for event in decision.high_priority_events] == [HighPriorityEventKind.POKE]
    assert dispatcher.calls[0]["events"][0].reason == "test_policy"


@pytest.mark.asyncio
async def test_scheduler_uses_injected_review_policy() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )

    await scheduler.accept_signal(make_signal())

    plan = scheduler.review_plan_for("bot:group:room")
    assert plan is not None
    assert plan.next_review_at == 52.0
    assert plan.reason == "fixed_test_review"


@pytest.mark.asyncio
async def test_scheduler_lists_and_starts_due_review_without_high_priority() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())

    due = scheduler.due_review_plans(now=52.0)
    decision = scheduler.prepare_due_review("bot:group:room", now=52.0)

    assert [plan.session_id for plan in due] == ["bot:group:room"]
    assert decision.review_started is True
    assert decision.active_reply_pending is False
    assert decision.state == AgentState.REVIEW
    assert scheduler.state_for("bot:group:room") == AgentState.REVIEW


@pytest.mark.asyncio
async def test_scheduler_dispatches_due_review_workflow() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())

    decision = await scheduler.run_due_review("bot:group:room", now=52.0)

    assert decision.review_started is True
    assert decision.review_workflow_started is True
    assert decision.state == AgentState.REVIEW
    assert dispatcher.review_calls[0]["session_id"] == "bot:group:room"
    assert dispatcher.review_calls[0]["review_plan"].reason == "fixed_test_review"
    assert [
        message.message_log_id
        for message in dispatcher.review_calls[0]["unread_messages"]
    ] == [1]


@pytest.mark.asyncio
async def test_scheduler_accept_signal_dispatches_due_review_workflow() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())

    decision = await scheduler.accept_signal(
        make_review_due_signal(occurred_at=80.0, due_at=52.0)
    )

    assert decision.review_started is True
    assert decision.review_workflow_started is True
    assert decision.state == AgentState.REVIEW
    assert dispatcher.review_calls[0]["session_id"] == "bot:group:room"


@pytest.mark.asyncio
async def test_scheduler_accept_signal_uses_review_due_timer_due_at() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())

    decision = await scheduler.accept_signal(
        make_review_due_signal(occurred_at=80.0, due_at=51.0)
    )

    assert decision.skipped_reason == "review_not_due"
    assert decision.state == AgentState.IDLE
    assert dispatcher.review_calls == []


@pytest.mark.asyncio
async def test_scheduler_due_review_is_interrupted_by_high_priority_queue() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal(is_mentioned=True))

    decision = scheduler.prepare_due_review("bot:group:room", now=52.0)

    assert decision.review_started is False
    assert decision.active_reply_pending is True
    assert decision.state == AgentState.ACTIVE_REPLY
    assert [event.kind for event in decision.high_priority_events] == [
        HighPriorityEventKind.MENTION
    ]
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_REPLY


@pytest.mark.asyncio
async def test_scheduler_does_not_dispatch_due_review_while_active_reply_is_running() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal(is_mentioned=True))
    dispatcher.calls.clear()

    decision = await scheduler.run_due_review("bot:group:room", now=52.0)

    assert decision.review_started is False
    assert decision.review_workflow_started is False
    assert decision.active_reply_pending is False
    assert decision.skipped_reason == "active_reply_running"
    assert dispatcher.calls == []
    assert dispatcher.review_calls == []


@pytest.mark.asyncio
async def test_concrete_active_reply_noop_allows_due_review_to_continue() -> None:
    dispatcher = ActiveReplyDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        config=AgentSchedulerConfig(mention_wake_count=2, mention_wake_window_seconds=60),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal(is_mentioned=True))

    decision = await scheduler.run_due_review("bot:group:room", now=52.0)

    assert decision.active_reply_pending is True
    assert decision.state == AgentState.REVIEW
    assert scheduler.state_for("bot:group:room") == AgentState.REVIEW
    assert scheduler.high_priority_events("bot:group:room") == []


@pytest.mark.asyncio
async def test_scheduler_due_review_skips_when_review_already_running() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler._state_store.set_state("bot:group:room", AgentState.REVIEW)

    decision = await scheduler.run_due_review("bot:group:room", now=52.0)

    assert decision.review_started is False
    assert decision.skipped_reason == "review_already_running"
    assert decision.state == AgentState.REVIEW
    assert dispatcher.review_calls == []


@pytest.mark.asyncio
async def test_scheduler_due_review_skips_while_active_chat_is_running() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler._state_store.set_state("bot:group:room", AgentState.ACTIVE_CHAT)

    decision = await scheduler.run_due_review("bot:group:room", now=52.0)

    assert decision.review_started is False
    assert decision.skipped_reason == "active_chat_running"
    assert decision.state == AgentState.ACTIVE_CHAT
    assert dispatcher.review_calls == []


@pytest.mark.asyncio
async def test_review_due_timer_dispatches_due_idle_review() -> None:
    class RecordingRuntime:
        def __init__(self) -> None:
            self.calls: list[AgentSignal] = []
            self.agent_scheduler = type(
                "Scheduler",
                (),
                {"due_review_plans": lambda self, limit=50: [ReviewPlan(session_id="bot:group:room", next_review_at=52.0, reason="fixed_test_review")]},
            )()

        async def handle_agent_signal(self, signal: AgentSignal) -> None:
            self.calls.append(signal)

        def agent_profile_for_bot(self, _bot_id: str):
            return self

    runtime = RecordingRuntime()
    timer = ReviewDueTimerService()
    timer.bind_agent_runtime(runtime, bot_id="bot-a")

    await timer.run_once()

    assert [call.kind for call in runtime.calls] == [AgentSignalKind.REVIEW_DUE]
    assert [call.source for call in runtime.calls] == [AgentSignalSource.TIMER]
    assert [call.bot_id for call in runtime.calls] == ["bot-a"]
    assert [call.session_id for call in runtime.calls] == ["bot:group:room"]
    assert [call.timer.trigger for call in runtime.calls if call.timer is not None] == [
        "review_due"
    ]


@pytest.mark.asyncio
async def test_review_due_timer_service_drives_scheduler_review_state() -> None:
    now = 10.0
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: now,
    )
    await scheduler.accept_signal(make_signal())
    now = 52.0

    class Profile:
        agent_scheduler = scheduler

    class Runtime:
        async def handle_agent_signal(self, signal: AgentSignal) -> None:
            await scheduler.accept_signal(signal)

        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

    timer = ReviewDueTimerService()
    timer.bind_agent_runtime(Runtime(), bot_id="bot-a")

    await timer.run_once()

    assert scheduler.state_for("bot:group:room") == AgentState.REVIEW
    assert [call["session_id"] for call in dispatcher.review_calls] == ["bot:group:room"]
    assert dispatcher.review_calls[0]["review_plan"].next_review_at == 52.0


@pytest.mark.asyncio
async def test_scheduler_completes_active_reply_to_idle_when_review_is_not_requested() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal(is_mentioned=True))

    decision = await scheduler.complete_active_reply("bot:group:room", now=20.0)

    assert decision.returned_to_idle is True
    assert decision.review_started is False
    assert decision.review_workflow_started is False
    assert decision.state == AgentState.IDLE
    assert [event.kind for event in decision.handled_high_priority_events] == [
        HighPriorityEventKind.MENTION
    ]
    assert scheduler.high_priority_events("bot:group:room") == []
    assert dispatcher.review_calls == []


@pytest.mark.asyncio
async def test_scheduler_completes_active_reply_and_runs_forced_review() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal(is_mentioned=True))

    decision = await scheduler.complete_active_reply(
        "bot:group:room",
        review_after=True,
        now=20.0,
    )

    assert decision.review_started is True
    assert decision.review_workflow_started is True
    assert decision.returned_to_idle is False
    assert decision.state == AgentState.REVIEW
    assert scheduler.high_priority_events("bot:group:room") == []
    assert dispatcher.review_calls[0]["review_plan"].reason == "fixed_test_review"
    assert [
        message.message_log_id
        for message in dispatcher.review_calls[0]["unread_messages"]
    ] == [1]


@pytest.mark.asyncio
async def test_scheduler_complete_active_reply_skips_when_state_is_not_active_reply() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )

    decision = await scheduler.complete_active_reply("bot:group:room")

    assert decision.skipped_reason == "not_active_reply"
    assert decision.state == AgentState.IDLE


@pytest.mark.asyncio
async def test_scheduler_completes_review_to_idle_with_next_review_plan() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)

    decision = scheduler.complete_review("bot:group:room", now=60.0)

    assert decision.returned_to_idle is True
    assert decision.active_chat_started is False
    assert decision.state == AgentState.IDLE
    assert decision.next_review_plan is not None
    assert decision.next_review_plan.next_review_at == 160.0
    assert decision.next_review_plan.reason == "fixed_after_review"
    assert scheduler.state_for("bot:group:room") == AgentState.IDLE
    assert scheduler.review_plan_for("bot:group:room") == decision.next_review_plan


@pytest.mark.asyncio
async def test_scheduler_completes_review_to_idle_with_explicit_next_plan() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    next_plan = ReviewPlan(
        session_id="bot:group:room",
        next_review_at=300.0,
        reason="workflow_requested_later_review",
        updated_at=60.0,
    )

    decision = scheduler.complete_review("bot:group:room", next_review_plan=next_plan)

    assert decision.returned_to_idle is True
    assert decision.next_review_plan == next_plan
    assert scheduler.review_plan_for("bot:group:room") == next_plan


@pytest.mark.asyncio
async def test_scheduler_completes_review_to_active_chat() -> None:
    timer = RecordingActiveChatTimer()
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_timer=timer,
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)

    decision = scheduler.complete_review(
        "bot:group:room",
        enter_active_chat=True,
        active_chat_initial_interest=20.0,
        now=60.0,
    )

    assert decision.active_chat_started is True
    assert decision.returned_to_idle is False
    assert decision.state == AgentState.ACTIVE_CHAT
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.interest_value == 20.0
    assert decision.active_chat_state.entered_at == 60.0
    assert timer.started == ["bot:group:room"]
    assert decision.next_review_plan is None
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    assert scheduler.active_chat_state_for("bot:group:room") == decision.active_chat_state
