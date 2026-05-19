from __future__ import annotations

from agent_scheduler_support import (
    ActiveChatPolicyConfig,
    AgentScheduler,
    AgentState,
    DefaultActiveChatPolicy,
    FixedReviewPolicy,
    RecordingActiveChatTimer,
    RecordingWorkflowDispatcher,
    make_signal,
    pytest,
)


@pytest.mark.asyncio
async def test_scheduler_ticks_active_chat_without_returning_idle() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                decay_half_life_seconds=10.0,
                idle_interest_threshold=1.0,
            )
        ),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    decision = scheduler.tick_active_chat("bot:group:room", now=70.0)

    assert decision.returned_to_idle is False
    assert decision.state == AgentState.ACTIVE_CHAT
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.interest_value == pytest.approx(
        10.0 * (0.5 ** 0.5)
    )
    assert decision.active_chat_state.tick_count == 1
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT


@pytest.mark.asyncio
async def test_scheduler_observes_message_during_active_chat() -> None:
    now = 10.0
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                decay_half_life_seconds=10.0,
                idle_interest_threshold=1.0,
                message_interest_delta=2.0,
            )
        ),
        now=lambda: now,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    now = 70.0
    decision = await scheduler.accept_signal(make_signal(message_log_id=2))

    assert decision.active_chat_observed is True
    assert decision.active_reply_started is False
    assert decision.state == AgentState.ACTIVE_CHAT
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.interest_value == pytest.approx(12.0)
    assert decision.active_chat_state.tick_count == 0
    assert scheduler.active_chat_state_for("bot:group:room") == decision.active_chat_state


@pytest.mark.asyncio
async def test_scheduler_notifies_active_chat_workflow_for_observed_message() -> None:
    now = 10.0
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "active_chat",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                decay_half_life_seconds=10.0,
                message_interest_delta=2.0,
            )
        ),
        now=lambda: now,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    now = 70.0
    decision = await scheduler.accept_signal(make_signal(message_log_id=2))

    assert decision.active_chat_observed is True
    assert decision.active_chat_workflow_notified is True
    assert dispatcher.active_chat_calls[0]["session_id"] == "bot:group:room"
    assert dispatcher.active_chat_calls[0]["message_log_id"] == 2
    assert dispatcher.active_chat_calls[0]["response_profile"] == "active_chat"
    assert dispatcher.active_chat_calls[0]["active_chat_state"] == decision.active_chat_state


@pytest.mark.asyncio
async def test_scheduler_active_chat_handles_mentions_without_active_reply_interrupt() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    timer = RecordingActiveChatTimer()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "immediate",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                decay_half_life_seconds=10.0,
                message_interest_delta=2.0,
            )
        ),
        active_chat_timer=timer,
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    active_chat_decision = scheduler.complete_review(
        "bot:group:room",
        enter_active_chat=True,
        now=60.0,
    )

    decision = await scheduler.accept_signal(make_signal(message_log_id=2, is_mentioned=True))

    assert decision.active_reply_started is False
    assert decision.active_chat_observed is True
    assert decision.active_chat_workflow_notified is True
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.interest_value == pytest.approx(18.0)
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    assert dispatcher.calls == []
    assert dispatcher.active_chat_calls[0]["message_log_id"] == 2
    assert scheduler.active_chat_state_for("bot:group:room") == decision.active_chat_state
    assert active_chat_decision.active_chat_state is not None
    assert timer.cancelled == []


@pytest.mark.asyncio
async def test_scheduler_active_chat_message_interest_ignores_low_signal_events() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "active_chat",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                message_interest_delta=2.0,
            )
        ),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    decision = await scheduler.accept_signal(
        make_signal(
            message_log_id=2,
            is_mention_to_other=True,
            is_poke_to_bot=True,
            is_poke_to_other=True,
        )
    )

    assert decision.active_chat_state is not None
    assert decision.active_chat_state.interest_value == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_scheduler_active_chat_message_interest_ignores_bot_self_messages() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "active_chat",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                message_interest_delta=2.0,
            )
        ),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    decision = await scheduler.accept_signal(
        make_signal(
            message_log_id=2,
            sender_id="bot-self",
            is_mentioned=True,
            is_reply_to_bot=True,
        )
    )

    assert decision.accepted is False
    assert decision.skipped_reason == "self_message"
    assert decision.active_chat_observed is False
    assert dispatcher.active_chat_calls == []
    assert scheduler.active_chat_state_for("bot:group:room").interest_value == pytest.approx(10.0)
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        1
    ]


@pytest.mark.asyncio
async def test_scheduler_ticks_active_chat_to_idle_with_next_review_plan() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    timer = RecordingActiveChatTimer()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                decay_half_life_seconds=10.0,
                idle_interest_threshold=5.0,
            )
        ),
        active_chat_timer=timer,
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=52.0)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    decision = scheduler.tick_active_chat("bot:group:room", now=70.0)
    if not decision.returned_to_idle:
        decision = scheduler.tick_active_chat("bot:group:room", now=75.0)

    assert decision.returned_to_idle is True
    assert decision.state == AgentState.IDLE
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.interest_value == pytest.approx(5.0)
    assert decision.next_review_plan is not None
    assert decision.next_review_plan.next_review_at == 175.0
    assert scheduler.state_for("bot:group:room") == AgentState.IDLE
    assert scheduler.active_chat_state_for("bot:group:room") is None
    assert scheduler.review_plan_for("bot:group:room") == decision.next_review_plan
    assert timer.cancelled == ["bot:group:room"]
    assert dispatcher.active_chat_stops == ["bot:group:room"]


def test_scheduler_adjusts_active_chat_interest() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    scheduler._state_store.set_state("bot:group:room", AgentState.REVIEW)
    completion = scheduler.complete_review(
        "bot:group:room",
        enter_active_chat=True,
        active_chat_initial_interest=15.0,
        now=60.0,
    )
    assert completion.active_chat_state is not None

    decision = scheduler.adjust_active_chat_interest(
        "bot:group:room",
        delta=5.0,
        reason="send_reply_light",
        now=65.0,
    )

    assert decision.returned_to_idle is False
    assert decision.state == AgentState.ACTIVE_CHAT
    assert decision.active_chat_state is not None
    assert decision.active_chat_state.interest_value == 20.0
    assert decision.reason == "send_reply_light"
    assert scheduler.active_chat_state_for("bot:group:room") == decision.active_chat_state


def test_scheduler_force_exits_active_chat_from_interest_adjustment() -> None:
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
    scheduler.complete_review(
        "bot:group:room",
        enter_active_chat=True,
        active_chat_initial_interest=15.0,
        now=60.0,
    )

    decision = scheduler.adjust_active_chat_interest(
        "bot:group:room",
        force_exit=True,
        reason="topic_done",
        now=70.0,
    )

    assert decision.returned_to_idle is True
    assert decision.state == AgentState.IDLE
    assert decision.force_exit is True
    assert decision.reason == "topic_done"
    assert decision.next_review_plan is not None
    assert decision.next_review_plan.next_review_at == 170.0
    assert scheduler.active_chat_state_for("bot:group:room") is None
    assert timer.cancelled == ["bot:group:room"]
    assert dispatcher.active_chat_stops == ["bot:group:room"]


def test_scheduler_previews_active_chat_interest_adjustment_exit() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    scheduler._state_store.set_state("bot:group:room", AgentState.REVIEW)
    scheduler.complete_review(
        "bot:group:room",
        enter_active_chat=True,
        active_chat_initial_interest=15.0,
        now=60.0,
    )

    preview = scheduler.preview_active_chat_interest_adjustment(
        "bot:group:room",
        force_exit=True,
        now=70.0,
    )

    assert preview.will_return_idle is True
    assert preview.active_chat_state is not None
    assert preview.active_chat_state.interest_value == 0.0
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    assert scheduler.active_chat_state_for("bot:group:room").interest_value == 15.0


def test_scheduler_previews_active_chat_decay_tick_exit() -> None:
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        active_chat_policy=DefaultActiveChatPolicy(
            ActiveChatPolicyConfig(
                initial_interest_value=10.0,
                decay_half_life_seconds=5.0,
                idle_interest_threshold=5.0,
                tick_interval_seconds=5.0,
            )
        ),
        now=lambda: 10.0,
    )
    scheduler._state_store.set_state("bot:group:room", AgentState.REVIEW)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    preview = scheduler.preview_active_chat_tick("bot:group:room", now=65.0)

    assert preview.will_return_idle is True
    assert preview.active_chat_state is not None
    assert preview.active_chat_state.interest_value == pytest.approx(5.0)
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    assert scheduler.active_chat_state_for("bot:group:room").interest_value == 10.0
