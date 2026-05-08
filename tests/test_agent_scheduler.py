from __future__ import annotations

from asyncio import sleep as asyncio_sleep
from typing import Any

import pytest

from shinbot.agent.scheduler import (
    ActiveChatDisposition,
    ActiveChatPolicyConfig,
    ActiveChatTimerService,
    AgentScheduler,
    AgentSchedulerConfig,
    AgentState,
    DefaultActiveChatPolicy,
    HighPriorityEventKind,
    InMemoryAgentInbox,
    InMemoryAgentStateStore,
    PriorityPolicyDecision,
)
from shinbot.agent.scheduler.models import HighPriorityEvent, ReviewPlan
from shinbot.core.dispatch.dispatchers import AgentEntrySignal


class RecordingWorkflowDispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.review_calls: list[dict[str, Any]] = []
        self.active_chat_calls: list[dict[str, Any]] = []
        self.active_chat_stops: list[str] = []

    async def run_active_reply(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        **kwargs: Any,
    ) -> None:
        self.calls.append(
            {
                "session_id": session_id,
                "message_log_id": message_log_id,
                "sender_id": sender_id,
                **kwargs,
            }
        )

    async def run_review(
        self,
        *,
        session_id: str,
        review_plan: ReviewPlan,
        unread_messages: list[Any],
    ) -> None:
        self.review_calls.append(
            {
                "session_id": session_id,
                "review_plan": review_plan,
                "unread_messages": unread_messages,
            }
        )

    async def notify_active_chat_message(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        **kwargs: Any,
    ) -> None:
        self.active_chat_calls.append(
            {
                "session_id": session_id,
                "message_log_id": message_log_id,
                "sender_id": sender_id,
                **kwargs,
            }
        )

    def stop_active_chat(self, session_id: str) -> None:
        self.active_chat_stops.append(session_id)


class RecordingActiveChatTimer:
    def __init__(self) -> None:
        self.scheduler = None
        self.started: list[str] = []
        self.cancelled: list[str] = []

    def bind_agent_scheduler(self, scheduler) -> None:
        self.scheduler = scheduler

    def start(self, session_id: str) -> None:
        self.started.append(session_id)

    def cancel(self, session_id: str) -> None:
        self.cancelled.append(session_id)

    async def shutdown(self) -> None:
        return


class AlwaysWakePriorityPolicy:
    def evaluate(self, signal, *, now, inbox):
        return PriorityPolicyDecision(
            events=[
                HighPriorityEvent(
                    session_id=signal.session_id,
                    message_log_id=signal.message_log_id or 0,
                    sender_id=signal.sender_id,
                    kind=HighPriorityEventKind.POKE,
                    created_at=now,
                    reason="test_policy",
                )
            ],
            should_start_active_reply=True,
        )


class FixedReviewPolicy:
    def initial_plan(self, *, session_id: str, now: float) -> ReviewPlan:
        return ReviewPlan(
            session_id=session_id,
            next_review_at=now + 42.0,
            reason="fixed_test_review",
            updated_at=now,
        )

    def plan_after_review(
        self,
        *,
        session_id: str,
        now: float,
        previous_plan: ReviewPlan | None = None,
    ) -> ReviewPlan:
        return ReviewPlan(
            session_id=session_id,
            next_review_at=now + 100.0,
            reason="fixed_after_review",
            updated_at=now,
        )


def make_signal(
    *,
    message_log_id: int | None = 1,
    is_mentioned: bool = False,
    is_reply_to_bot: bool = False,
    is_mention_to_other: bool = False,
    is_poke_to_bot: bool = False,
    is_poke_to_other: bool = False,
    sender_id: str = "user-1",
    already_handled: bool = False,
    is_stopped: bool = False,
) -> AgentEntrySignal:
    return AgentEntrySignal(
        session_id="bot:group:room",
        message_log_id=message_log_id,
        event_type="message-created",
        sender_id=sender_id,
        instance_id="bot",
        platform="mock",
        self_id="bot-self",
        is_private=False,
        is_mentioned=is_mentioned,
        is_reply_to_bot=is_reply_to_bot,
        is_mention_to_other=is_mention_to_other,
        is_poke_to_bot=is_poke_to_bot,
        is_poke_to_other=is_poke_to_other,
        already_handled=already_handled,
        is_stopped=is_stopped,
    )


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
async def test_scheduler_does_not_dispatch_review_when_high_priority_is_pending() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )
    await scheduler.accept_signal(make_signal(is_mentioned=True))

    decision = await scheduler.run_due_review("bot:group:room", now=52.0)

    assert decision.review_started is False
    assert decision.review_workflow_started is False
    assert decision.active_reply_pending is True
    assert dispatcher.review_calls == []


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
    assert timer.scheduler is scheduler
    assert timer.started == ["bot:group:room"]
    assert decision.next_review_plan is None
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    assert scheduler.active_chat_state_for("bot:group:room") == decision.active_chat_state


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
            sender_id="bot-self",
            is_mentioned=True,
            is_reply_to_bot=True,
        )
    )

    assert decision.active_chat_state is not None
    assert decision.active_chat_state.interest_value == pytest.approx(10.0)


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
async def test_active_chat_timer_service_ticks_session_to_idle() -> None:
    timer = ActiveChatTimerService(tick_interval_seconds=0.01)
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
        active_chat_timer=timer,
        now=lambda: 60.0,
    )
    await scheduler.accept_signal(make_signal())
    scheduler.prepare_due_review("bot:group:room", now=60.0)
    scheduler.complete_review("bot:group:room", enter_active_chat=True, now=60.0)

    await asyncio_sleep(0.05)
    await timer.shutdown()

    assert scheduler.state_for("bot:group:room") == AgentState.IDLE
    assert scheduler.active_chat_state_for("bot:group:room") is None
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

    assert [missing.skipped_reason, handled.skipped_reason, stopped.skipped_reason] == [
        "missing_message_log_id",
        "already_handled",
        "stopped",
    ]
    assert dispatcher.calls == []
    assert scheduler.unread_messages("bot:group:room") == []
