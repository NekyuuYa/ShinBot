"""Workflow dispatch boundary for AgentScheduler."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.scheduler.models import HighPriorityEvent


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


class AttentionActiveReplyDispatcher:
    """Compatibility dispatcher that uses the existing attention scheduler."""

    def __init__(self, attention_scheduler) -> None:
        self._attention_scheduler = attention_scheduler

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
