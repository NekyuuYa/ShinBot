"""Concrete coordinator dispatcher for AgentScheduler decisions."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shinbot.agent.coordinators.active_chat.models import ActiveChatMessageSignal
from shinbot.agent.coordinators.active_chat.trace import sanitize_conversation_trace_messages
from shinbot.agent.coordinators.review.models import (
    ReviewWorkflowConfig,
    build_review_workflow_explanation,
)
from shinbot.agent.scheduler.models import (
    ActiveChatState,
    ActiveReplyThreshold,
    HighPriorityEvent,
    MentionSensitivity,
    ReviewPlan,
    UnreadMessage,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.summaries import (
    ReviewHandoffContext,
    SummaryHandoffEntry,
)
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:workflow", color="green")

ReviewRunRecorder = Callable[[str, Any, list[UnreadMessage]], None]

if TYPE_CHECKING:
    from shinbot.agent.coordinators.active_chat import ActiveChatCoordinator
    from shinbot.agent.coordinators.review import ReviewCoordinator
    from shinbot.agent.coordinators.review.models import (
        ReviewWorkflowExplanation,
        ReviewWorkflowResult,
    )
    from shinbot.agent.runners.review_idle_planning import IdleReviewPlanningStageRunner
    from shinbot.agent.runtime.task_manager import AgentTaskScope
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
        idle_review_planning_runner: IdleReviewPlanningStageRunner | None = None,
        review_run_recorder: ReviewRunRecorder | None = None,
    ) -> None:
        self._review_coordinator = review_coordinator
        self._active_chat_workflow = active_chat_workflow
        self._summary_service = summary_service
        self._review_config = review_config or ReviewWorkflowConfig()
        self._idle_review_planning_runner = idle_review_planning_runner
        self._review_run_recorder = review_run_recorder
        self._agent_scheduler: AgentScheduler | None = None
        self.last_review_result: ReviewWorkflowResult | None = None
        self.last_review_explanation: ReviewWorkflowExplanation | None = None
        self._review_tasks: dict[str, asyncio.Task[None]] = {}
        self._review_task_scope: AgentTaskScope | None = None

    def bind_agent_scheduler(self, scheduler: AgentScheduler) -> None:
        """Bind the owning scheduler so review workflow can return state decisions."""
        self._agent_scheduler = scheduler

    def bind_review_task_scope(self, scope: AgentTaskScope) -> None:
        """Bind the task scope used to run interruptible review workflows."""

        self._review_task_scope = scope

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
        """Handle a high-priority message in ACTIVE_REPLY state.

        Active reply reuses the active-chat fast workflow as a one-shot reply
        path. It bypasses semantic waiting because mentions/replies already
        passed the scheduler's high-priority policy.
        """
        if self._active_chat_workflow is None or self._agent_scheduler is None:
            logger.warning(
                format_log_event(
                    "agent.active_reply.skipped",
                    reason=(
                        "missing_active_chat_workflow"
                        if self._active_chat_workflow is None
                        else "missing_agent_scheduler"
                    ),
                    session_id=session_id,
                    message_log_id=message_log_id,
                    sender_id=sender_id,
                    response_profile=response_profile,
                    event_count=len(events),
                    trace_id=trace_id,
                )
            )
            if self._agent_scheduler is not None:
                await self._agent_scheduler.complete_active_reply(session_id)
            return

        now = time.time()
        active_chat_state = ActiveChatState(
            session_id=session_id,
            interest_value=max(
                self._review_config.provisional_active_chat_interest,
                30.0,
            ),
            decay_half_life_seconds=(
                self._review_config.provisional_active_chat_half_life_seconds
            ),
            entered_at=now,
            updated_at=now,
            active_epoch=int(now * 1000),
        )
        logger.info(
            format_log_event(
                "agent.active_reply.workflow.start",
                session_id=session_id,
                message_log_id=message_log_id,
                sender_id=sender_id,
                response_profile=response_profile,
                event_count=len(events),
                trace_id=trace_id,
            )
        )
        try:
            await self._active_chat_workflow.start_active_chat(
                session_id=session_id,
                active_chat_state=active_chat_state,
            )
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
                trace_id=trace_id,
            )
            state = self._active_chat_workflow.attention_state_for(session_id)
            if state is not None and not state.pending_buffer:
                state.pending_buffer.append(
                    ActiveChatMessageSignal(
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
                        created_at=now,
                        trace_id=trace_id,
                    )
                )
            await self._active_chat_workflow.flush_now(
                scheduler=self._agent_scheduler,
                session_id=session_id,
            )
            logger.info(
                format_log_event(
                    "agent.active_reply.workflow.finish",
                    session_id=session_id,
                    message_log_id=message_log_id,
                    sender_id=sender_id,
                    event_count=len(events),
                )
            )
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "agent.active_reply.workflow.failed",
                    session_id=session_id,
                    message_log_id=message_log_id,
                    sender_id=sender_id,
                    error_code=type(exc).__name__,
                )
            )
        finally:
            if self._active_chat_workflow is not None:
                self._active_chat_workflow.stop_active_chat(session_id)
            if self._agent_scheduler is not None:
                await self._agent_scheduler.complete_active_reply(session_id)

    async def run_review(
        self,
        *,
        session_id: str,
        review_plan: ReviewPlan,
        unread_messages: list[UnreadMessage],
    ) -> None:
        """Run the review workflow for a session and hand off to active chat if needed.

        Args:
            session_id: The session to review.
            review_plan: The current review plan with timing metadata.
            unread_messages: Messages accumulated since the last review.
        """
        if await self._await_previous_review_tail(session_id):
            if self._agent_scheduler is not None:
                unread_messages = self._agent_scheduler.unread_messages(session_id)
        if self._review_task_scope is not None:
            task = self._review_task_scope.create_task(
                session_id,
                self._run_review_workflow(
                    session_id=session_id,
                    review_plan=review_plan,
                    unread_messages=unread_messages,
                ),
                name=f"agent-review:{session_id}",
            )
            self._review_tasks[session_id] = task
            task.add_done_callback(
                lambda completed, target_session_id=session_id: self._finish_review_task(
                    target_session_id,
                    completed,
                )
            )
            return

        await self._run_review_workflow(
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=unread_messages,
        )

    async def _await_previous_review_tail(self, session_id: str) -> bool:
        """Fence a replacement review behind the previous run's cancellation tail."""

        previous = self._review_tasks.get(session_id)
        if previous is None or previous is asyncio.current_task():
            return False
        if not previous.done():
            previous.cancel()
            await asyncio.gather(previous, return_exceptions=True)
        self._finish_review_task(session_id, previous)
        return True

    async def _run_review_workflow(
        self,
        *,
        session_id: str,
        review_plan: ReviewPlan,
        unread_messages: list[UnreadMessage],
    ) -> None:
        if self._review_coordinator is None or self._agent_scheduler is None:
            return
        current_task = asyncio.current_task()
        if current_task is not None:
            self._review_tasks[session_id] = current_task

        logger.debug(
            format_log_event(
                "agent.review.workflow.start",
                session_id=session_id,
                next_review_at=f"{review_plan.next_review_at:.2f}",
                reason=review_plan.reason,
                unread_count=len(unread_messages),
            )
        )
        try:
            result = await self._review_coordinator.run(
                scheduler=self._agent_scheduler,
                session_id=session_id,
                review_plan=review_plan,
                unread_messages=unread_messages,
            )
            self.last_review_result = result
            self.last_review_explanation = build_review_workflow_explanation(result)
            self._record_review_run(session_id, result, unread_messages)
            logger.debug(
                format_log_event(
                    "agent.review.workflow.finish",
                    session_id=session_id,
                    review_run_id=result.review_run_id,
                    failed=result.failed,
                    reply_replied=(
                        result.reply.replied if result.reply is not None else None
                    ),
                    active_chat_started=(
                        result.completion.active_chat_started
                        if result.completion is not None
                        else None
                    ),
                )
            )
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
        finally:
            if current_task is not None:
                self._finish_review_task(session_id, current_task)

    def _finish_review_task(
        self,
        session_id: str,
        task: asyncio.Task[Any],
    ) -> None:
        if self._review_tasks.get(session_id) is task:
            self._review_tasks.pop(session_id, None)

    def cancel_review(self, session_id: str) -> None:
        """Cancel an in-flight review workflow for the given session."""

        task = self._review_tasks.get(session_id)
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()

    def _record_review_run(
        self,
        session_id: str,
        result: ReviewWorkflowResult,
        unread_messages: list[UnreadMessage],
    ) -> None:
        recorder = self._review_run_recorder
        if recorder is None:
            return
        try:
            recorder(session_id, result, unread_messages)
        except Exception as exc:
            logger.warning(
                format_log_event(
                    "agent.review.run_record.failed",
                    session_id=session_id,
                    review_run_id=getattr(result, "review_run_id", ""),
                    error_code=type(exc).__name__,
                ),
                exc_info=True,
            )

    async def start_active_chat(
        self,
        *,
        session_id: str,
        active_chat_state: ActiveChatState,
        review_result_summary: Any = None,
        initial_unread_messages: list[UnreadMessage] | None = None,
    ) -> None:
        """Start an active chat workflow session for the given session.
        Args:
            session_id: The session to start active chat for.
            active_chat_state: The initial active chat state with interest values.
            review_result_summary: Optional review handoff context for bootstrap.
            initial_unread_messages: Messages to forward to the active chat workflow.
        """
        if self._active_chat_workflow is None:
            return

        logger.debug(
            format_log_event(
                "agent.active_chat.workflow.start",
                session_id=session_id,
                active_epoch=active_chat_state.active_epoch,
                initial_interest=f"{active_chat_state.interest_value:.2f}",
                initial_unread_count=len(initial_unread_messages or []),
            )
        )
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
                trace_id=message.trace_id,
            )

    def stop_active_chat(self, session_id: str) -> None:
        """Stop the active chat workflow and persist conversation summaries.
        Args:
            session_id: The session whose active chat workflow to stop.
        """
        if self._active_chat_workflow is None:
            return

        self._save_active_chat_summary(session_id)
        self._active_chat_workflow.stop_active_chat(session_id)

    def flush_active_chat_summaries(self) -> None:
        """Save summaries for all active chat sessions before shutdown."""
        if self._active_chat_workflow is None or self._summary_service is None:
            return
        for session_id in self._active_chat_workflow.active_session_ids():
            self._save_active_chat_summary(session_id)

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
        """Notify the active chat workflow of a new incoming message.
        Args:
            session_id: The session receiving the message.
            message_log_id: The persisted message log identifier.
            sender_id: Platform ID of the message sender.
            response_profile: Response profile label (e.g. ``"balanced"``).
            is_mentioned: Whether the bot was mentioned in this message.
            is_reply_to_bot: Whether this message replies to the bot.
            is_mention_to_other: Whether the message mentions another user.
            is_poke_to_bot: Whether this is a poke directed at the bot.
            is_poke_to_other: Whether this is a poke directed at another user.
            self_platform_id: The bot's own platform identifier.
            active_chat_state: The current active chat state.
            trace_id: Optional distributed trace identifier.
        """
        if self._active_chat_workflow is None or self._agent_scheduler is None:
            return

        logger.debug(
            format_log_event(
                "agent.active_chat.workflow.message",
                session_id=session_id,
                message_log_id=message_log_id,
                sender_id=sender_id,
                response_profile=response_profile,
                is_mentioned=is_mentioned,
                is_reply_to_bot=is_reply_to_bot,
                active_epoch=active_chat_state.active_epoch,
                interest=f"{active_chat_state.interest_value:.2f}",
                trace_id=trace_id,
            )
        )
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
            trace_id=trace_id,
        )

    async def plan_idle_review_after_active_chat(
        self,
        session_id: str,
    ) -> ReviewPlan | None:
        """Plan the next review before ACTIVE_CHAT returns to IDLE."""
        if self._agent_scheduler is None or self._idle_review_planning_runner is None:
            return None
        checked_at = time.time()
        previous_plan = self._agent_scheduler.review_plan_for(session_id)
        logger.debug(
            format_log_event(
                "agent.idle_review_planning.start",
                session_id=session_id,
                previous_next_review_at=(
                    f"{previous_plan.next_review_at:.2f}"
                    if previous_plan is not None
                    else ""
                ),
            )
        )
        stage_input = self._build_idle_review_planning_input(
            session_id=session_id,
            now=checked_at,
        )
        try:
            output = await self._idle_review_planning_runner.run(stage_input)
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "agent.idle_review_planning.failed",
                    session_id=session_id,
                    error_code=type(exc).__name__,
                )
            )
            return None
        seconds = output.next_review_after_seconds
        if seconds is None:
            logger.debug(
                format_log_event(
                    "agent.idle_review_planning.skip",
                    session_id=session_id,
                    reason="missing_next_review_after_seconds",
                )
            )
            return None
        min_seconds = max(0.0, self._review_config.idle_review_planning_min_after_seconds)
        max_seconds = max(
            min_seconds,
            self._review_config.idle_review_planning_max_after_seconds,
        )
        seconds = min(max(seconds, min_seconds), max_seconds)
        previous_threshold = (
            previous_plan.active_reply_threshold if previous_plan is not None else None
        )
        scheduled_from = time.time()
        plan = ReviewPlan(
            session_id=session_id,
            next_review_at=scheduled_from + seconds,
            reason=output.reason or "idle_review_planning",
            mention_sensitivity=(
                output.mention_sensitivity
                or (
                    previous_plan.mention_sensitivity
                    if previous_plan is not None
                    else MentionSensitivity.NORMAL
                )
            ),
            active_reply_threshold=ActiveReplyThreshold(
                at_count=output.mention_wake_count
                or (previous_threshold.at_count if previous_threshold is not None else 1),
                window_seconds=output.mention_wake_window_seconds
                or (
                    previous_threshold.window_seconds
                    if previous_threshold is not None
                    else 60.0
                ),
            ),
            updated_at=scheduled_from,
        )
        logger.debug(
            format_log_event(
                "agent.idle_review_planning.finish",
                session_id=session_id,
                planning_latency_seconds=f"{max(0.0, scheduled_from - checked_at):.2f}",
                next_review_after_seconds=f"{seconds:.2f}",
                next_review_at=f"{plan.next_review_at:.2f}",
                reason=plan.reason,
                mention_sensitivity=plan.mention_sensitivity.value,
                mention_wake_count=plan.active_reply_threshold.at_count,
                mention_wake_window_seconds=plan.active_reply_threshold.window_seconds,
            )
        )
        return plan


    def _save_active_chat_summary(self, session_id: str) -> None:
        """Save active_chat summary. Write failure only logs, never blocks exit."""
        if self._summary_service is None or self._active_chat_workflow is None:
            return
        try:
            snapshot = self._active_chat_workflow.summary_snapshot_for(session_id)
            if snapshot is None:
                return
            summary_text = snapshot.conversation_summary.strip()
            if not summary_text:
                return
            self._summary_service.save_active_chat_summary(
                session_id=snapshot.session_id,
                source_run_id=f"active_chat:{snapshot.session_id}:{snapshot.active_epoch}",
                content=summary_text,
                msg_log_start=snapshot.msg_log_start,
                msg_log_end=snapshot.msg_log_end,
                msg_count=snapshot.msg_count,
                metadata={
                    "active_epoch": snapshot.active_epoch,
                    "trace_message_count": snapshot.trace_message_count,
                    "observed_message_count": snapshot.observed_message_count,
                    "range_source": snapshot.range_source,
                    "covered_message_log_ids": list(snapshot.message_log_ids),
                },
            )
        except Exception as exc:
            logger.warning(
                format_log_event(
                    "agent.active_chat.summary.persist_failed",
                    session_id=session_id,
                    error_code=type(exc).__name__,
                ),
                exc_info=True,
            )

    def _build_idle_review_planning_input(
        self,
        *,
        session_id: str,
        now: float,
    ) -> ReviewStageInput:
        snapshot = (
            self._active_chat_workflow.summary_snapshot_for(session_id)
            if self._active_chat_workflow is not None
            else None
        )
        active_chat_state = (
            self._agent_scheduler.active_chat_state_for(session_id)
            if self._agent_scheduler is not None
            else None
        )
        metadata: dict[str, object] = {
            "transition": "ACTIVE_CHAT->IDLE",
            "now": now,
        }
        if active_chat_state is not None:
            metadata.update(
                {
                    "active_epoch": active_chat_state.active_epoch,
                    "interest_value": active_chat_state.interest_value,
                    "decay_half_life_seconds": active_chat_state.decay_half_life_seconds,
                    "entered_at": active_chat_state.entered_at,
                    "updated_at": active_chat_state.updated_at,
                    "tick_count": active_chat_state.tick_count,
                    "bootstrap_applied": active_chat_state.bootstrap_applied,
                    "bootstrap_disposition": (
                        active_chat_state.bootstrap_disposition.value
                        if active_chat_state.bootstrap_disposition is not None
                        else None
                    ),
                }
            )
        if snapshot is not None:
            context_messages = sanitize_conversation_trace_messages(
                snapshot.conversation_messages
            )
            metadata.update(
                {
                    "trace_message_count": snapshot.trace_message_count,
                    "observed_message_count": snapshot.observed_message_count,
                    "conversation_summary": snapshot.conversation_summary,
                    "message_log_ids": list(snapshot.message_log_ids),
                }
            )
        else:
            context_messages = []
        return ReviewStageInput(
            session_id=session_id,
            purpose="idle_review_planning",
            source_messages=[],
            context_messages=context_messages,
            metadata=metadata,
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
            except Exception as exc:
                logger.debug(
                    format_log_event(
                        "agent.review.handoff_context.build_failed",
                        session_id=session_id,
                        review_run_id=result.review_run_id,
                        error_code=type(exc).__name__,
                    ),
                    exc_info=True,
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
