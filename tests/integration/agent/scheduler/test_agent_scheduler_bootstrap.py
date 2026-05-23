from __future__ import annotations

from agent_scheduler_support import (
    ActiveChatDisposition,
    ActiveChatPolicyConfig,
    ActiveChatTimerService,
    AgentScheduler,
    AgentState,
    DefaultActiveChatPolicy,
    FixedReviewPolicy,
    InMemoryAgentStateStore,
    RecordingActiveChatTimer,
    RecordingWorkflowDispatcher,
    ReviewPlan,
    asyncio_sleep,
    calculate_bootstrap_correction,
    make_active_chat_bootstrap_signal,
    make_active_chat_tick_signal,
    make_signal,
    pytest,
)

from shinbot.agent.signals import AgentSignal, AgentSignalKind, AgentSignalSource


@pytest.mark.asyncio
async def test_scheduler_applies_active_chat_bootstrap_correction() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=15.0,
                decay_half_life_seconds=20.0,
                tick_interval_seconds=5.0,
            )
        ),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    completion = scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)
    assert completion.active_chat_state is not None
    epoch = completion.active_chat_state.active_epoch
    scheduler.tick_active_chat("bot:group:room", now=65.0)
    await scheduler.accept_signal(make_signal(message_log_id=2))

    decision = scheduler.apply_active_chat_bootstrap(
        "bot:group:room",
        disposition=ActiveChatDisposition.EXIT_SOON,
        active_epoch=epoch,
        now=66.0,
    )

    assert decision.bootstrap_applied is True
    assert decision.returned_to_idle is False
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.bootstrap_disposition == ActiveChatDisposition.EXIT_SOON
    assert decision.active_chat_state.bootstrap_applied is True
    assert decision.active_chat_state.decay_half_life_seconds == 10.0
    assert decision.active_chat_state.interest_value == pytest.approx(11.6066, rel=1e-4)


@pytest.mark.asyncio
async def test_scheduler_accept_signal_applies_active_chat_bootstrap() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=15.0,
                decay_half_life_seconds=20.0,
                tick_interval_seconds=5.0,
            )
        ),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    completion = scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)
    assert completion.active_chat_state is not None

    decision = await scheduler.accept_signal(
        make_active_chat_bootstrap_signal(
            disposition=ActiveChatDisposition.EXIT_SOON,
            active_epoch=completion.active_chat_state.active_epoch,
            occurred_at=66.0,
        )
    )

    assert decision is not None
    assert decision.bootstrap_applied is True
    assert decision.returned_to_idle is False
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.bootstrap_disposition == ActiveChatDisposition.EXIT_SOON


def test_scheduler_active_chat_bootstrap_can_return_idle_and_stop_runtime() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    timer = RecordingActiveChatTimer()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_timer=timer,
        now=lambda: 10.0,
    )
    scheduler._state_store.set_state("bot:group:room", AgentState.REVIEW)
    completion = scheduler.complete_review(
        "bot:group:room",
        enter_active_chat=True,
        active_chat_initial_interest=4.0,
        now=60.0,
    )
    assert completion.active_chat_state is not None

    decision = scheduler.apply_active_chat_bootstrap(
        "bot:group:room",
        disposition=ActiveChatDisposition.EXIT_SOON,
        active_epoch=completion.active_chat_state.active_epoch,
        now=61.0,
    )

    assert decision.bootstrap_applied is True
    assert decision.returned_to_idle is True
    assert decision.state == AgentState.IDLE
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.bootstrap_disposition == ActiveChatDisposition.EXIT_SOON
    assert decision.next_review_plan is not None
    assert decision.next_review_plan.next_review_at == 161.0
    assert scheduler.active_chat_state_for("bot:group:room") is None
    assert timer.cancelled == ["bot:group:room"]
    assert dispatcher.active_chat_stops == ["bot:group:room"]


def test_scheduler_bootstrap_rejected_on_epoch_mismatch() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    scheduler._state_store.set_state("bot:group:room", AgentState.REVIEW)
    completion = scheduler.complete_review(
        "bot:group:room",
        enter_active_chat=True,
        now=60.0,
    )
    assert completion.active_chat_state is not None
    correct_epoch = completion.active_chat_state.active_epoch

    # Apply with wrong epoch — should be rejected
    decision = scheduler.apply_active_chat_bootstrap(
        "bot:group:room",
        disposition=ActiveChatDisposition.ENGAGED,
        active_epoch=correct_epoch + 999,
        now=61.0,
    )
    assert decision.bootstrap_applied is False
    assert decision.skipped_reason == "active_epoch_mismatch"

    # Apply with correct epoch — should succeed
    decision = scheduler.apply_active_chat_bootstrap(
        "bot:group:room",
        disposition=ActiveChatDisposition.ENGAGED,
        active_epoch=correct_epoch,
        now=62.0,
    )
    assert decision.bootstrap_applied is True
    assert decision.skipped_reason is None


def test_bootstrap_correction_applies_tick_diff_to_current_interest() -> None:
    """Bootstrap correction uses tick_count to compute the decay curve,
    so a late bootstrap doesn't overwrite runtime message-driven deltas."""
    config = ActiveChatPolicyConfig(
        initial_interest_value=15.0,
        decay_half_life_seconds=20.0,
        tick_interval_seconds=5.0,
    )
    policy = DefaultActiveChatPolicy(config)
    state = policy.initial_state(session_id="s", now=10.0)
    # 6 ticks = 30 seconds = 1.5 half-lives
    for i in range(6):
        state = policy.decay(state, now=10.0 + (i + 1) * 5.0, count_tick=True)
    # Simulate a message bump that raises interest mid-session
    state = policy.observe_message(state, now=41.0, is_mentioned=True)
    bumped_interest = state.interest_value

    correction = calculate_bootstrap_correction(
        state,
        disposition=ActiveChatDisposition.ENGAGED,
        config=config,
    )
    corrected = policy.apply_bootstrap_disposition(
        state, disposition=ActiveChatDisposition.ENGAGED, now=42.0,
    )
    # The correction is added to the current (bumped) interest, not the initial
    assert corrected.interest_value == pytest.approx(
        bumped_interest + correction.correction, rel=1e-4,
    )
    # The correction accounts for 6 ticks of decay on both curves
    assert corrected.tick_count == 6


@pytest.mark.asyncio
async def test_active_chat_timer_service_dispatches_tick_signal() -> None:
    timer = ActiveChatTimerService(tick_interval_seconds=0.01)
    calls: list[AgentSignal] = []

    class Scheduler:
        def state_for(self, _session_id: str) -> AgentState:
            return AgentState.IDLE

    class Profile:
        agent_scheduler = Scheduler()

    class Runtime:
        async def handle_agent_signal(self, signal: AgentSignal) -> None:
            calls.append(signal)

        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

    timer.bind_agent_runtime(Runtime(), bot_id="bot-a")
    timer.start("bot:group:room")

    await asyncio_sleep(0.05)
    await timer.shutdown()

    assert [call.kind for call in calls] == [AgentSignalKind.ACTIVE_CHAT_TICK]
    assert [call.source for call in calls] == [AgentSignalSource.TIMER]
    assert [call.bot_id for call in calls] == ["bot-a"]
    assert [call.session_id for call in calls] == ["bot:group:room"]
    assert timer.active_sessions() == []


def test_scheduler_tick_active_chat_skips_when_state_is_not_active_chat() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )

    decision = scheduler.tick_active_chat("bot:group:room")

    assert decision.skipped_reason == "not_active_chat"
    assert decision.state == AgentState.IDLE


@pytest.mark.asyncio
async def test_scheduler_accept_signal_active_chat_tick_plans_before_idle() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    dispatcher.idle_review_plans.append(
        ReviewPlan(
            session_id="bot:group:room",
            next_review_at=123.0,
            reason="timer_settled",
            updated_at=65.0,
        )
    )
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                idle_interest_threshold=5.0,
                decay_half_life_seconds=5.0,
                tick_interval_seconds=5.0,
            )
        ),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    decision = await scheduler.accept_signal(
        make_active_chat_tick_signal(occurred_at=70.0, due_at=65.0)
    )

    assert decision.returned_to_idle is True
    assert decision.next_review_plan is not None
    assert decision.next_review_plan.reason == "timer_settled"
    assert scheduler.state_for("bot:group:room") == AgentState.IDLE
    assert scheduler.review_plan_for("bot:group:room") == decision.next_review_plan
    assert dispatcher.idle_review_plan_calls == ["bot:group:room"]


def test_scheduler_complete_review_skips_when_state_is_not_review() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )

    decision = scheduler.complete_review("bot:group:room")

    assert decision.skipped_reason == "not_review"
    assert decision.state == AgentState.IDLE


def test_scheduler_prepare_due_review_skips_when_not_due() -> None:
    state_store = InMemoryAgentStateStore()
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        state_store=state_store,
        now=lambda: 10.0,
    )
    state_store.set_review_plan(
        ReviewPlan(session_id="bot:group:room", next_review_at=52.0, reason="future")
    )

    decision = scheduler.prepare_due_review("bot:group:room", now=20.0)

    assert decision.skipped_reason == "review_not_due"
    assert decision.state == AgentState.IDLE


@pytest.mark.asyncio
async def test_scheduler_skips_unusable_signals() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
    )

    missing = await scheduler.accept_signal(make_signal(message_log_id=None))
    handled = await scheduler.accept_signal(make_signal(already_handled=True))
    stopped = await scheduler.accept_signal(make_signal(is_stopped=True))
    self_message = await scheduler.accept_signal(make_signal(sender_id="bot-self"))

    assert [
        missing.skipped_reason,
        handled.skipped_reason,
        stopped.skipped_reason,
        self_message.skipped_reason,
    ] == [
        "missing_message_log_id",
        "already_handled",
        "stopped",
        "self_message",
    ]
    assert dispatcher.calls == []
    assert scheduler.unread_messages("bot:group:room") == []
