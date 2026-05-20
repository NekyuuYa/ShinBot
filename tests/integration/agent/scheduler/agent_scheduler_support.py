from __future__ import annotations

from asyncio import sleep as asyncio_sleep
from typing import Any

import pytest

from shinbot.agent.coordinators.dispatcher import ActiveReplyDispatcher
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
    ReviewDueTimerService,
    calculate_bootstrap_correction,
)
from shinbot.agent.scheduler.models import HighPriorityEvent, ReviewPlan
from shinbot.core.dispatch.dispatchers import AgentEntrySignal


class RecordingWorkflowDispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.review_calls: list[dict[str, Any]] = []
        self.active_chat_calls: list[dict[str, Any]] = []
        self.active_chat_stops: list[str] = []
        self.idle_review_plans: list[ReviewPlan] = []
        self.idle_review_plan_calls: list[str] = []

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


__all__ = [
    "ActiveChatDisposition",
    "ActiveChatPolicyConfig",
    "ActiveChatTimerService",
    "ActiveReplyDispatcher",
    "AgentEntrySignal",
    "AgentScheduler",
    "AgentSchedulerConfig",
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
    "annotations",
    "asyncio_sleep",
    "calculate_bootstrap_correction",
    "make_signal",
    "pytest",
]
