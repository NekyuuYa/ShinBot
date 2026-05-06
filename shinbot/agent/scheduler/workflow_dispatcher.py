"""Workflow dispatch boundary for AgentScheduler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from shinbot.agent.scheduler.models import (
    ActiveChatState,
    HighPriorityEvent,
    ReviewPlan,
    UnreadMessage,
)

if TYPE_CHECKING:
    from shinbot.agent.review import ReviewWorkflow
    from shinbot.agent.scheduler.scheduler import AgentScheduler


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
        self_platform_id: str,
        events: list[HighPriorityEvent],
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

    async def notify_active_chat_message(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        response_profile: str,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        self_platform_id: str,
        active_chat_state: ActiveChatState,
    ) -> None:
        """Notify active chat workflow about one observed message signal."""


class AttentionActiveReplyDispatcher:
    """Compatibility dispatcher that uses the existing attention scheduler."""

    def __init__(
        self,
        attention_scheduler,
        *,
        review_workflow: ReviewWorkflow | None = None,
    ) -> None:
        self._attention_scheduler = attention_scheduler
        self._review_workflow = review_workflow
        self._agent_scheduler: AgentScheduler | None = None

    def bind_agent_scheduler(self, scheduler: AgentScheduler) -> None:
        """Bind the owning scheduler so review workflow can return state decisions."""
        self._agent_scheduler = scheduler

    async def run_active_reply(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        response_profile: str,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        self_platform_id: str,
        events: list[HighPriorityEvent],
    ) -> None:
        if self._attention_scheduler is None:
            return

        await self._attention_scheduler.on_message(
            session_id,
            message_log_id,
            sender_id,
            response_profile=response_profile,
            is_mentioned=is_mentioned,
            is_reply_to_bot=is_reply_to_bot,
            self_platform_id=self_platform_id,
        )

    async def run_review(
        self,
        *,
        session_id: str,
        review_plan: ReviewPlan,
        unread_messages: list[UnreadMessage],
    ) -> None:
        if self._review_workflow is None or self._agent_scheduler is None:
            return

        await self._review_workflow.run(
            scheduler=self._agent_scheduler,
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=unread_messages,
        )

    async def notify_active_chat_message(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        response_profile: str,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        self_platform_id: str,
        active_chat_state: ActiveChatState,
    ) -> None:
        """Compatibility placeholder until the dedicated active chat workflow exists."""
        return
