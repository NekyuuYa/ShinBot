from __future__ import annotations

from asyncio import sleep as asyncio_sleep
from typing import Any

import pytest

from shinbot.agent.coordinators.dispatcher import ActiveReplyDispatcher
from shinbot.agent.scheduler import (
    ActiveChatDisposition,
    ActiveChatPolicyConfig,
    ActiveChatTimerService,
    ActiveReplyResume,
    ActiveReplyResumeKind,
    AgentScheduler,
    AgentSchedulerConfig,
    AgentState,
    DefaultActiveChatPolicy,
    HighPriorityEventKind,
    InMemoryAgentInbox,
    InMemoryAgentStateStore,
    PriorityPolicyDecision,
    ReviewDueTimerService,
    SchedulerEventKind,
    SchedulerTransitionTrigger,
    calculate_bootstrap_correction,
)
from shinbot.agent.scheduler.models import HighPriorityEvent, ReviewPlan, UnreadMessage
from shinbot.agent.signals import (
    AgentActiveChatBootstrapSignal,
    AgentMessageSignal,
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
    AgentTimerSignal,
)


class RecordingWorkflowDispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.review_calls: list[dict[str, Any]] = []
        self.active_chat_calls: list[dict[str, Any]] = []
        self.active_chat_stops: list[str] = []
        self.idle_review_plans: list[ReviewPlan] = []
        self.idle_review_plan_calls: list[str] = []
        self.cancelled_reviews: list[str] = []

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

    async def plan_idle_review_after_active_chat(self, session_id: str) -> ReviewPlan | None:
        self.idle_review_plan_calls.append(session_id)
        if not self.idle_review_plans:
            return None
        return self.idle_review_plans.pop(0)

    def cancel_review(self, session_id: str) -> None:
        self.cancelled_reviews.append(session_id)


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
        assert signal.message is not None
        return PriorityPolicyDecision(
            events=[
                HighPriorityEvent(
                    session_id=signal.session_id,
                    message_log_id=signal.message.message_log_id or 0,
                    sender_id=signal.message.sender_id,
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
) -> AgentSignal:
    return AgentSignal(
        signal_id=f"message:bot:group:room:{message_log_id if message_log_id is not None else 'missing'}",
        kind=AgentSignalKind.MESSAGE,
        source=AgentSignalSource.MESSAGE_INGRESS,
        session_id="bot:group:room",
        occurred_at=10.0,
        message=AgentMessageSignal(
            message_log_id=message_log_id,
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
        ),
    )


def make_review_due_signal(
    *,
    session_id: str = "bot:group:room",
    occurred_at: float = 52.0,
    due_at: float | None = None,
) -> AgentSignal:
    return AgentSignal(
        signal_id=f"review-due:{session_id}:{int(occurred_at)}",
        kind=AgentSignalKind.REVIEW_DUE,
        source=AgentSignalSource.TIMER,
        session_id=session_id,
        occurred_at=occurred_at,
        timer=AgentTimerSignal(
            trigger=AgentSignalKind.REVIEW_DUE.value,
            due_at=due_at,
        ),
    )


def make_active_chat_tick_signal(
    *,
    session_id: str = "bot:group:room",
    occurred_at: float = 65.0,
    due_at: float | None = None,
) -> AgentSignal:
    return AgentSignal(
        signal_id=f"active-chat-tick:{session_id}:{int(occurred_at)}",
        kind=AgentSignalKind.ACTIVE_CHAT_TICK,
        source=AgentSignalSource.TIMER,
        session_id=session_id,
        occurred_at=occurred_at,
        timer=AgentTimerSignal(
            trigger=AgentSignalKind.ACTIVE_CHAT_TICK.value,
            due_at=due_at,
        ),
    )


def make_active_chat_bootstrap_signal(
    *,
    session_id: str = "bot:group:room",
    disposition: ActiveChatDisposition = ActiveChatDisposition.WATCH,
    active_epoch: int | None = None,
    occurred_at: float = 66.0,
) -> AgentSignal:
    return AgentSignal(
        signal_id=f"active-chat-bootstrap:{session_id}:{active_epoch or 'none'}",
        kind=AgentSignalKind.ACTIVE_CHAT_BOOTSTRAP,
        source=AgentSignalSource.MANUAL,
        session_id=session_id,
        occurred_at=occurred_at,
        active_chat_bootstrap=AgentActiveChatBootstrapSignal(
            disposition=disposition,
            active_epoch=active_epoch,
            reason="test",
        ),
    )


__all__ = [
    "ActiveChatDisposition",
    "ActiveChatPolicyConfig",
    "ActiveReplyResume",
    "ActiveReplyResumeKind",
    "ActiveChatTimerService",
    "ActiveReplyDispatcher",
    "AgentActiveChatBootstrapSignal",
    "AgentMessageSignal",
    "AgentScheduler",
    "AgentSchedulerConfig",
    "AgentSignal",
    "AgentSignalKind",
    "AgentSignalSource",
    "AgentState",
    "AlwaysWakePriorityPolicy",
    "Any",
    "DefaultActiveChatPolicy",
    "FixedReviewPolicy",
    "HighPriorityEvent",
    "HighPriorityEventKind",
    "InMemoryAgentInbox",
    "InMemoryAgentStateStore",
    "PriorityPolicyDecision",
    "RecordingActiveChatTimer",
    "RecordingWorkflowDispatcher",
    "ReviewPlan",
    "ReviewDueTimerService",
    "SchedulerEventKind",
    "SchedulerTransitionTrigger",
    "UnreadMessage",
    "annotations",
    "asyncio_sleep",
    "calculate_bootstrap_correction",
    "make_active_chat_bootstrap_signal",
    "make_active_chat_tick_signal",
    "make_review_due_signal",
    "make_signal",
    "pytest",
]
