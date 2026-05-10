"""Concrete coordinator dispatcher for AgentScheduler decisions."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from shinbot.agent.coordinators.review.models import (
    ReviewWorkflowConfig,
    build_review_workflow_explanation,
)
from shinbot.agent.scheduler.models import (
    ActiveChatState,
    HighPriorityEvent,
    ReviewPlan,
    UnreadMessage,
)
from shinbot.agent.services.summaries import (
    ReviewHandoffContext,
    SummaryHandoffEntry,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from shinbot.agent.coordinators.active_chat import ActiveChatCoordinator
    from shinbot.agent.coordinators.review import ReviewCoordinator
    from shinbot.agent.coordinators.review.models import (
        ReviewWorkflowExplanation,
        ReviewWorkflowResult,
    )
    from shinbot.agent.scheduler.scheduler import AgentScheduler


class ActiveReplyDispatcher:
    """Dispatcher for the 4-state machine's active reply path."""

    def __init__(
        self,
        *,
        review_coordinator: ReviewCoordinator | None = None,
        active_chat_workflow: ActiveChatCoordinator | None = None,
        summary_service: Any | None = None,
        review_config: ReviewWorkflowConfig | None = None,
    ) -> None:
        self._review_coordinator = review_coordinator
        self._active_chat_workflow = active_chat_workflow
        self._summary_service = summary_service
        self._review_config = review_config or ReviewWorkflowConfig()
        self._agent_scheduler: AgentScheduler | None = None
        self.last_review_result: ReviewWorkflowResult | None = None
        self.last_review_explanation: ReviewWorkflowExplanation | None = None

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
        """Handle a high-priority message in ACTIVE_REPLY state.

        TODO: implement direct LLM workflow dispatch for active replies.
        Currently a stub - the old AttentionEngine chain has been archived.
        """
        logger.info(
            "Active reply dispatched (stub) session=%s msg=%s sender=%s",
            session_id,
            message_log_id,
            sender_id,
        )

    async def run_review(
        self,
        *,
        session_id: str,
        review_plan: ReviewPlan,
        unread_messages: list[UnreadMessage],
    ) -> None:
        if self._review_coordinator is None or self._agent_scheduler is None:
            return

        result = await self._review_coordinator.run(
            scheduler=self._agent_scheduler,
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=unread_messages,
        )
        self.last_review_result = result
        self.last_review_explanation = build_review_workflow_explanation(result)
        if (
            self._active_chat_workflow is not None
            and result.completion is not None
            and result.completion.active_chat_started
            and result.completion.active_chat_state is not None
        ):
            handoff_context = await self._build_handoff_context(
                session_id=session_id,
                result=result,
                explanation=self.last_review_explanation,
            )
            await self.start_active_chat(
                session_id=session_id,
                active_chat_state=result.completion.active_chat_state,
                review_result_summary=handoff_context,
                initial_unread_messages=_unread_messages_added_after_review(
                    before=unread_messages,
                    after=self._agent_scheduler.unread_messages(session_id),
                ),
            )

    async def start_active_chat(
        self,
        *,
        session_id: str,
        active_chat_state: ActiveChatState,
        review_result_summary=None,
        initial_unread_messages: list[UnreadMessage] | None = None,
    ) -> None:
        if self._active_chat_workflow is None:
            return

        await self._active_chat_workflow.start_active_chat(
            session_id=session_id,
            active_chat_state=active_chat_state,
            review_result_summary=review_result_summary,
        )
        if self._agent_scheduler is None:
            return
        for message in initial_unread_messages or []:
            if message.session_id != session_id:
                continue
            await self.notify_active_chat_message(
                session_id=session_id,
                message_log_id=message.message_log_id,
                sender_id=message.sender_id,
                response_profile=message.response_profile or "balanced",
                is_mentioned=message.is_mentioned,
                is_reply_to_bot=message.is_reply_to_bot,
                is_mention_to_other=message.is_mention_to_other,
                is_poke_to_bot=message.is_poke_to_bot,
                is_poke_to_other=message.is_poke_to_other,
                self_platform_id=message.self_platform_id,
                active_chat_state=active_chat_state,
            )

    def stop_active_chat(self, session_id: str) -> None:
        if self._active_chat_workflow is None:
            return

        self._active_chat_workflow.stop_active_chat(session_id)

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
    ) -> None:
        if self._active_chat_workflow is None or self._agent_scheduler is None:
            return

        await self._active_chat_workflow.notify_message(
            scheduler=self._agent_scheduler,
            session_id=session_id,
            message_log_id=message_log_id,
            sender_id=sender_id,
            response_profile=response_profile,
            is_mentioned=is_mentioned,
            is_reply_to_bot=is_reply_to_bot,
            is_mention_to_other=is_mention_to_other,
            is_poke_to_bot=is_poke_to_bot,
            is_poke_to_other=is_poke_to_other,
            self_platform_id=self_platform_id,
            active_chat_state=active_chat_state,
        )


    async def _build_handoff_context(
        self,
        *,
        session_id: str,
        result: ReviewWorkflowResult,
        explanation: ReviewWorkflowExplanation,
    ) -> ReviewHandoffContext:
        overflow_summaries: list[SummaryHandoffEntry] = []
        block_digests: list[SummaryHandoffEntry] = []
        recent_active_chat_summary: str | None = None

        if self._summary_service is not None:
            try:
                from shinbot.agent.services.summaries import SummaryType

                overflow_records = self._summary_service.list_by_run_id(
                    result.review_run_id,
                    summary_type=SummaryType.OVERFLOW_COMPRESSION,
                )
                overflow_summaries = [
                    _summary_handoff_entry(record)
                    for record in overflow_records
                    if getattr(record, "content", None)
                ]

                digest_records = self._summary_service.list_by_run_id(
                    result.review_run_id,
                    summary_type=SummaryType.BLOCK_DIGEST,
                )
                block_digests = [
                    _summary_handoff_entry(record)
                    for record in digest_records
                    if getattr(record, "content", None)
                ]

                active_record = self._summary_service.get_latest_by_session(
                    session_id,
                    summary_type=SummaryType.ACTIVE_CHAT,
                )
                if active_record is not None:
                    max_age = self._review_config.active_chat_summary_max_age_seconds
                    created_at = float(getattr(active_record, "created_at", 0) or 0)
                    if created_at > 0 and (time.time() - created_at) <= max_age:
                        content = str(getattr(active_record, "content", "") or "").strip()
                        recent_active_chat_summary = content or None
            except Exception:
                logger.debug(
                    "Failed to build handoff context for %s", session_id, exc_info=True,
                )

        return ReviewHandoffContext(
            review_run_id=result.review_run_id,
            explanation=explanation,
            overflow_summaries=overflow_summaries,
            block_digests=block_digests,
            recent_active_chat_summary=recent_active_chat_summary,
        )


def _unread_messages_added_after_review(
    *,
    before: list[UnreadMessage],
    after: list[UnreadMessage],
) -> list[UnreadMessage]:
    before_ids = {message.message_log_id for message in before}
    return [
        message
        for message in after
        if message.message_log_id not in before_ids
    ]


def _summary_handoff_entry(record: Any) -> SummaryHandoffEntry:
    return SummaryHandoffEntry(
        content=str(getattr(record, "content", "") or ""),
        block_index=_optional_int(getattr(record, "block_index", None)),
        msg_log_start=_optional_int(getattr(record, "msg_log_start", None)),
        msg_log_end=_optional_int(getattr(record, "msg_log_end", None)),
        msg_count=_optional_int(getattr(record, "msg_count", None)) or 0,
    )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = ["ActiveReplyDispatcher"]
