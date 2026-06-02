"""Workflow dispatch boundary for AgentScheduler."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.scheduler.models import (
    ActiveChatState,
    HighPriorityEvent,
    ReviewPlan,
    UnreadMessage,
)


class AgentWorkflowDispatcher(Protocol):
    """Scheduler-owned boundary for invoking concrete Agent workflows."""

    async def run_active_reply(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        response_profile: str,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        is_mention_to_other: bool,
        is_poke_to_bot: bool,
        is_poke_to_other: bool,
        self_platform_id: str,
        events: list[HighPriorityEvent],
        trace_id: str = "",
    ) -> None:
        """Handle high-priority events before review or active chat workflows."""

    async def run_review(
        self,
        *,
        session_id: str,
        review_plan: ReviewPlan,
        unread_messages: list[UnreadMessage],
    ) -> None:
        """Run the review workflow for unread messages selected by Agent internals."""

    def cancel_review(self, session_id: str) -> None:
        """Cancel an in-flight review workflow for one session, if supported."""

    async def start_active_chat(
        self,
        *,
        session_id: str,
        active_chat_state: ActiveChatState,
        review_result_summary=None,
        initial_unread_messages: list[UnreadMessage] | None = None,
    ) -> None:
        """Start an active chat workflow session after review completion."""

    def stop_active_chat(self, session_id: str) -> None:
        """Stop active chat workflow runtime state after scheduler exit."""

    async def notify_active_chat_message(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        response_profile: str,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        is_mention_to_other: bool,
        is_poke_to_bot: bool,
        is_poke_to_other: bool,
        self_platform_id: str,
        active_chat_state: ActiveChatState,
        trace_id: str = "",
    ) -> None:
        """Notify active chat workflow about one observed message signal."""

    async def plan_idle_review_after_active_chat(
        self,
        session_id: str,
    ) -> ReviewPlan | None:
        """Plan the next review before active chat returns to idle."""


__all__ = ["AgentWorkflowDispatcher"]
