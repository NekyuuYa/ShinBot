"""Review coordinator — stage orchestration, scheduler callbacks, bootstrap."""

from __future__ import annotations

import asyncio
import math
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from shinbot.agent.coordinators.review.models import (
    ActiveChatBootstrapResult,
    ConsumedUnreadRange,
    ReplyDecisionResult,
    ReviewScanResult,
    ReviewStageTrace,
    ReviewWorkflowConfig,
    ReviewWorkflowResult,
    UnreadRangeSummaryRecord,
)
from shinbot.agent.coordinators.review.stores import (
    ReviewMessageStore,
    ReviewSummaryStore,
)
from shinbot.agent.runners.review_block_digest import (
    NoopReviewBlockDigestStageRunner,
    ReviewBlockDigestStageRunner,
)
from shinbot.agent.runners.review_bootstrap import (
    ActiveChatBootstrapStageRunner,
    NoopActiveChatBootstrapStageRunner,
)
from shinbot.agent.runners.review_compression import (
    NoopOverflowCompressionStageRunner,
    OverflowCompressionStageRunner,
)
from shinbot.agent.runners.review_models import (
    ActiveChatBootstrapStageOutput,
    OverflowCompressionStageOutput,
    ReplyDecisionStageOutput,
    ReviewBlockDigestStageOutput,
    ReviewScanStageOutput,
)
from shinbot.agent.runners.review_reply import (
    NoopReplyDecisionStageRunner,
    ReplyDecisionStageRunner,
)
from shinbot.agent.runners.review_scan import NoopReviewScanStageRunner, ReviewScanStageRunner
from shinbot.agent.scheduler.models import (
    ActiveChatBootstrapApplyDecision,
    ActiveChatDisposition,
    ReviewCompletionDecision,
    ReviewPlan,
    UnreadMessage,
    UnreadRange,
)
from shinbot.agent.services.context.review_context_builder import (
    ReviewContextBuilder,
    ReviewContextBuilderAdapter,
    ReviewContextBuildOptions,
    ReviewStageInput,
)
from shinbot.agent.signals import (
    AgentActiveChatBootstrapSignal,
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
)
from shinbot.utils.logger import format_log_event, get_logger

if False:
    from shinbot.agent.runtime.task_manager import AgentTaskScope

logger = get_logger(__name__, source="agent:review", color="green")


@dataclass(slots=True)
class _ReplyDecisionWindow:
    candidate_message_ids: list[int]
    messages: list[dict] = field(default_factory=list)

    @property
    def message_ids(self) -> list[int]:
        """Return extracted message IDs from the loaded messages in this window."""
        return _message_ids(self.messages)

    def overlaps(self, messages: list[dict]) -> bool:
        """Check whether any of the given messages overlap with this window.
        Args:
            messages: Messages to check for overlap.

        Returns:
            ``True`` if any message ID is already present in the window.
        """
        own_ids = set(self.message_ids)
        return any(_message_id(message) in own_ids for message in messages)

    def extend(self, *, candidate_message_id: int, messages: list[dict]) -> None:
        """Extend the window with a new candidate and merge additional messages.
        Messages are deduplicated by ID and sorted by creation time.

        Args:
            candidate_message_id: The message log ID of the new candidate.
            messages: Additional context messages to merge into the window.
        """
        self.candidate_message_ids.append(candidate_message_id)
        merged: dict[int, dict] = {
            message_id: message
            for message in self.messages
            if (message_id := _message_id(message)) is not None
        }
        for message in messages:
            message_id = _message_id(message)
            if message_id is not None:
                merged[message_id] = message
        self.messages = sorted(
            merged.values(),
            key=lambda message: (
                float(message.get("created_at", 0.0) or 0.0),
                int(message.get("id", 0) or 0),
            ),
        )


class ReviewSchedulerPort(Protocol):
    """Scheduler surface used by review workflow without owning scheduler internals."""

    def unread_ranges(self, session_id: str, *, limit: int = 50) -> list[UnreadRange]:
        """Return Agent unread ranges for one session."""

    def count_unread_messages(self, session_id: str) -> int:
        """Return Agent unread count for one session."""

    def complete_review(
        self,
        session_id: str,
        *,
        enter_active_chat: bool = False,
        active_chat_initial_interest: float | None = None,
        active_chat_decay_half_life_seconds: float | None = None,
        next_review_plan: ReviewPlan | None = None,
        now: float | None = None,
    ) -> ReviewCompletionDecision:
        """Complete scheduler-side review state."""

    def split_review_consumed(
        self,
        *,
        range_id: int,
        consumed_start_msg_log_id: int,
        consumed_end_msg_log_id: int,
    ) -> None:
        """Mark one interval inside an unread range consumed by review."""

    def mark_ranges_review_consumed(self, range_ids: list[int]) -> None:
        """Mark whole unread ranges consumed by review."""

    def apply_active_chat_bootstrap(
        self,
        session_id: str,
        *,
        disposition: ActiveChatDisposition,
        active_epoch: int | None = None,
        now: float | None = None,
    ) -> ActiveChatBootstrapApplyDecision:
        """Apply delayed stage-3 active chat disposition."""


ReviewBootstrapSignalHandler = Callable[[AgentSignal], Any]


class ReviewCoordinator:
    """Three-stage review workflow shell.

    The current implementation deliberately avoids LLM calls and context assembly.
    It fixes the boundary and state transition contract so later stages can plug in
    compression, scan, reply, and bootstrap models without changing scheduler logic.
    """

    def __init__(
        self,
        config: ReviewWorkflowConfig | None = None,
        *,
        message_store: ReviewMessageStore | None = None,
        summary_store: ReviewSummaryStore | None = None,
        summary_service: Any | None = None,
        context_builder: ReviewContextBuilder | None = None,
        compression_runner: OverflowCompressionStageRunner | None = None,
        scan_runner: ReviewScanStageRunner | None = None,
        block_digest_runner: ReviewBlockDigestStageRunner | None = None,
        reply_runner: ReplyDecisionStageRunner | None = None,
        bootstrap_runner: ActiveChatBootstrapStageRunner | None = None,
        bootstrap_signal_handler: ReviewBootstrapSignalHandler | None = None,
        bot_id: str = "",
        bootstrap_task_scope: AgentTaskScope | None = None,
        block_digest_task_scope: AgentTaskScope | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        self._config = config or ReviewWorkflowConfig()
        self._message_store = message_store
        self._summary_store = summary_store
        self._summary_service = summary_service
        self._context_builder = context_builder or ReviewContextBuilderAdapter()
        self._compression_runner = compression_runner or NoopOverflowCompressionStageRunner()
        self._scan_runner = scan_runner or NoopReviewScanStageRunner()
        self._block_digest_runner = block_digest_runner or NoopReviewBlockDigestStageRunner()
        self._reply_runner = reply_runner or NoopReplyDecisionStageRunner()
        self._bootstrap_runner = bootstrap_runner or NoopActiveChatBootstrapStageRunner()
        self._bootstrap_signal_handler = bootstrap_signal_handler
        self._bot_id = str(bot_id or "").strip()
        self._bootstrap_task_scope = bootstrap_task_scope
        self._block_digest_task_scope = block_digest_task_scope
        self._now = now or time.time
        self._bootstrap_tasks: set[asyncio.Task[ActiveChatBootstrapResult]] = set()
        self._last_bootstrap_results: dict[str, ActiveChatBootstrapResult] = {}

    async def run(
        self,
        *,
        scheduler: ReviewSchedulerPort,
        session_id: str,
        review_plan: ReviewPlan,
        unread_messages: list[UnreadMessage],
    ) -> ReviewWorkflowResult:
        """Run the review shell and always hand control back to scheduler."""
        review_run_id = uuid.uuid4().hex
        started_at = self._now()
        stage_traces: list[ReviewStageTrace] = []
        trace_by_message_id = _trace_by_message_id(unread_messages)
        try:
            current_unread_ranges = scheduler.unread_ranges(session_id, limit=10_000)
            unread_ranges = _freeze_unread_ranges(
                current_unread_ranges,
                unread_messages,
            )
            use_frozen_snapshot = bool(unread_messages)
            use_partial_consumption = (
                use_frozen_snapshot
                and _unread_ranges_differ(current_unread_ranges, unread_ranges)
            )
            unread_count = (
                sum(unread_range.message_count for unread_range in unread_ranges)
                if use_frozen_snapshot
                else scheduler.count_unread_messages(session_id)
            )
            scan, consumed_ranges, block_digests = await self._run_review_scan(
                session_id=session_id,
                unread_count=unread_count,
                unread_ranges=unread_ranges,
                review_run_id=review_run_id,
                trace_by_message_id=trace_by_message_id,
                use_partial_consumption=use_partial_consumption,
                stage_traces=stage_traces,
            )
            reply = await self._run_reply_decision(
                session_id=session_id,
                scan=scan,
                block_digests=block_digests,
                review_run_id=review_run_id,
                trace_by_message_id=trace_by_message_id,
                stage_traces=stage_traces,
            )
            completion = scheduler.complete_review(
                session_id,
                enter_active_chat=True,
                active_chat_initial_interest=self._config.provisional_active_chat_interest,
                active_chat_decay_half_life_seconds=(
                    self._config.provisional_active_chat_half_life_seconds
                ),
            )
            active_epoch = (
                completion.active_chat_state.active_epoch
                if completion.active_chat_state is not None
                else None
            )
            try:
                applied_consumed_ranges = self._consume_review_ranges(
                    scheduler,
                    consumed_ranges,
                )
            except Exception as exc:
                logger.exception(
                    format_log_event(
                        "agent.review.consume.failed",
                        session_id=session_id,
                        review_run_id=review_run_id,
                        error_code=type(exc).__name__,
                        trace_id=_first_trace_id(trace_by_message_id),
                    )
                )
                applied_consumed_ranges = []
            bootstrap = self._schedule_active_chat_bootstrap(
                scheduler=scheduler,
                session_id=session_id,
                started_at=started_at,
                summaries=scan.compressed_ranges,
                reply=reply,
                active_epoch=active_epoch,
                review_run_id=review_run_id,
                trace_by_message_id=trace_by_message_id,
                stage_traces=stage_traces,
            )
            return ReviewWorkflowResult(
                review_run_id=review_run_id,
                scan=scan,
                reply=reply,
                bootstrap=bootstrap,
                review_started_at=started_at,
                completion=completion,
                consumed_ranges=applied_consumed_ranges,
                consumed_range_ids=[
                    item.range_id for item in applied_consumed_ranges if item.range_id is not None
                ],
                stage_traces=stage_traces,
            )
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "agent.review.workflow.failed",
                    session_id=session_id,
                    review_run_id=review_run_id,
                    plan_reason=review_plan.reason,
                    error_code=type(exc).__name__,
                    trace_id=_first_trace_id(trace_by_message_id),
                )
            )
            completion = scheduler.complete_review(
                session_id,
                enter_active_chat=True,
                active_chat_initial_interest=self._config.provisional_active_chat_interest,
                active_chat_decay_half_life_seconds=(
                    self._config.provisional_active_chat_half_life_seconds
                ),
            )
            bootstrap = ActiveChatBootstrapResult(
                reason="review_failed_provisional_active_chat",
                active_chat_interest_value=(
                    completion.active_chat_state.interest_value
                    if completion.active_chat_state is not None
                    else None
                ),
                active_chat_decay_half_life_seconds=(
                    completion.active_chat_state.decay_half_life_seconds
                    if completion.active_chat_state is not None
                    else None
                ),
            )
            return ReviewWorkflowResult(
                review_run_id=review_run_id,
                scan=ReviewScanResult(),
                reply=ReplyDecisionResult(),
                bootstrap=bootstrap,
                review_started_at=started_at,
                completion=completion,
                failed=True,
                failure_reason=str(exc),
                stage_traces=stage_traces,
            )

    async def wait_pending_bootstraps(self) -> None:
        """Wait for currently scheduled active chat bootstrap tasks."""
        tasks = list(self._bootstrap_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        """Cancel pending background active chat bootstrap tasks."""
        tasks = list(self._bootstrap_tasks)
        self._bootstrap_tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def last_bootstrap_result(self, session_id: str) -> ActiveChatBootstrapResult | None:
        """Return the latest completed background bootstrap result for one session."""
        return self._last_bootstrap_results.get(session_id)

    async def _run_review_scan(
        self,
        *,
        session_id: str,
        unread_count: int,
        unread_ranges: list[UnreadRange],
        review_run_id: str,
        stage_traces: list[ReviewStageTrace],
        trace_by_message_id: dict[int, str],
        use_partial_consumption: bool = False,
    ) -> tuple[ReviewScanResult, list[ConsumedUnreadRange], list[ReviewBlockDigestStageOutput]]:
        scanned_count = min(unread_count, self._config.overflow_threshold_messages)
        batch_count = (
            math.ceil(scanned_count / self._config.review_scan_batch_size)
            if scanned_count
            else 0
        )
        compressed_ranges = await self._run_overflow_compression(
            session_id=session_id,
            unread_count=unread_count,
            unread_ranges=unread_ranges,
            review_run_id=review_run_id,
            trace_by_message_id=trace_by_message_id,
            stage_traces=stage_traces,
        )
        (
            loaded_message_count,
            stage_input_count,
            candidate_message_ids,
            scan_reasons,
            consumed_ranges,
            block_digest_tasks,
        ) = await self._load_scan_batches(
            session_id=session_id,
            unread_ranges=unread_ranges,
            max_messages=scanned_count,
            prefer_tail=unread_count > self._config.overflow_threshold_messages,
            summaries=compressed_ranges,
            review_run_id=review_run_id,
            trace_by_message_id=trace_by_message_id,
            use_partial_consumption=use_partial_consumption,
            stage_traces=stage_traces,
        )
        block_digests = await self._await_block_digests(block_digest_tasks)
        return (
            ReviewScanResult(
                candidate_message_ids=_dedupe_preserve_order(
                    [
                        *[
                            candidate_id
                            for item in compressed_ranges
                            for candidate_id in item.candidate_message_ids
                        ],
                        *candidate_message_ids,
                    ]
                ),
                scan_reason="; ".join(_dedupe_preserve_order(scan_reasons))
                or "noop_review_scan",
                scanned_message_count=scanned_count,
                loaded_message_count=loaded_message_count,
                stage_input_count=stage_input_count,
                batch_count=batch_count,
                compressed_ranges=compressed_ranges,
            ),
            consumed_ranges,
            block_digests,
        )

    async def _run_reply_decision(
        self,
        *,
        session_id: str,
        scan: ReviewScanResult,
        block_digests: list[ReviewBlockDigestStageOutput] | None = None,
        review_run_id: str,
        trace_by_message_id: dict[int, str],
        stage_traces: list[ReviewStageTrace],
    ) -> ReplyDecisionResult:
        if not scan.candidate_message_ids:
            return ReplyDecisionResult()
        if self._message_store is None:
            return ReplyDecisionResult(
                target_message_ids=scan.candidate_message_ids,
                reply_reason="reply_decision_skipped_no_message_store",
            )

        active_chat_summary = self._query_recent_active_chat_summary(session_id)
        loaded_message_count = 0
        stage_input_count = 0
        replied = False
        reply_message_id = None
        reply_message_ids: list[int] = []
        target_message_ids: list[int] = []
        reply_reasons: list[str] = []

        for window in self._build_reply_decision_windows(
            session_id=session_id,
            candidate_message_ids=scan.candidate_message_ids,
        ):
            if not window.messages:
                continue
            primary_candidate_message_id = window.candidate_message_ids[0]
            selected_block_digests = _select_reply_block_digests(
                block_digests or [],
                candidate_message_ids=window.candidate_message_ids,
                messages=window.messages,
            )
            loaded_message_count += len(window.messages)
            stage_input = self._build_stage_input(
                session_id=session_id,
                messages=window.messages,
                purpose="reply_decision",
                review_run_id=review_run_id,
                metadata={
                    "candidate_message_id": primary_candidate_message_id,
                    "candidate_message_ids": list(window.candidate_message_ids),
                    "before_messages": self._config.reply_context_before_messages,
                    "after_messages": self._config.reply_context_after_messages,
                    **_summary_metadata_payload(scan.compressed_ranges),
                    **_block_digest_metadata_payload(selected_block_digests),
                    **_active_chat_summary_metadata(active_chat_summary),
                    **_trace_metadata_for_messages(window.messages, trace_by_message_id),
                },
                previous_summary=_format_reply_previous_summary(
                    overflow=_format_overflow_summaries(scan.compressed_ranges),
                    block_digests=selected_block_digests,
                    active_chat_summary=active_chat_summary,
                ),
            )
            if stage_input is None:
                continue
            stage_input_count += 1
            stage_output = await self._run_reply_stage(stage_input)
            stage_traces.append(_trace_for_reply(stage_input, stage_output))
            replied = replied or stage_output.replied
            if reply_message_id is None:
                reply_message_id = stage_output.reply_message_id
            reply_message_ids.extend(stage_output.reply_message_ids)
            target_message_ids.extend(stage_output.target_message_ids)
            if stage_output.reason.strip():
                reply_reasons.append(stage_output.reason.strip())

        return ReplyDecisionResult(
            replied=replied,
            reply_message_id=reply_message_id,
            reply_message_ids=_dedupe_preserve_order(reply_message_ids),
            target_message_ids=_dedupe_preserve_order(target_message_ids),
            reply_reason="; ".join(_dedupe_preserve_order(reply_reasons))
            or "noop_reply_decision",
            loaded_message_count=loaded_message_count,
            stage_input_count=stage_input_count,
        )

    def _build_reply_decision_windows(
        self,
        *,
        session_id: str,
        candidate_message_ids: list[int],
    ) -> list[_ReplyDecisionWindow]:
        windows: list[_ReplyDecisionWindow] = []
        for candidate_message_id in _dedupe_preserve_order(candidate_message_ids):
            local_context = self._message_store.list_around_message(
                session_id=session_id,
                message_log_id=candidate_message_id,
                before=self._config.reply_context_before_messages,
                after=self._config.reply_context_after_messages,
            )
            if not local_context:
                continue
            if windows and windows[-1].overlaps(local_context):
                windows[-1].extend(
                    candidate_message_id=candidate_message_id,
                    messages=local_context,
                )
                continue
            windows.append(
                _ReplyDecisionWindow(
                    candidate_message_ids=[candidate_message_id],
                    messages=list(local_context),
                )
            )
        return windows

    async def _run_overflow_compression(
        self,
        *,
        session_id: str,
        unread_count: int,
        unread_ranges: list[UnreadRange],
        review_run_id: str,
        trace_by_message_id: dict[int, str],
        stage_traces: list[ReviewStageTrace],
    ) -> list[UnreadRangeSummaryRecord]:
        summaries: list[UnreadRangeSummaryRecord] = []
        overflow_count = unread_count - self._config.overflow_threshold_messages
        if overflow_count <= 0:
            return summaries
        if self._message_store is None:
            return self._planned_overflow_compression(
                session_id=session_id,
                unread_count=unread_count,
                unread_ranges=unread_ranges,
            )

        remaining = overflow_count
        for unread_range in unread_ranges:
            if remaining <= 0:
                break
            range_remaining = min(remaining, unread_range.message_count)
            offset = 0
            while range_remaining > 0:
                messages = self._message_store.list_for_unread_range(
                    unread_range,
                    limit=min(
                        self._config.overflow_compression_batch_size,
                        range_remaining,
                    ),
                    offset=offset,
                )
                if not messages:
                    break
                summary = await self._run_overflow_compression_batch(
                    session_id=session_id,
                    messages=messages,
                    review_run_id=review_run_id,
                    trace_by_message_id=trace_by_message_id,
                    stage_traces=stage_traces,
                )
                if summary is not None:
                    self._save_overflow_summary(summary)
                    summaries.append(summary)
                range_remaining -= len(messages)
                remaining -= len(messages)
                offset += len(messages)
        return summaries

    async def _run_overflow_compression_batch(
        self,
        *,
        session_id: str,
        messages: list[dict],
        review_run_id: str,
        trace_by_message_id: dict[int, str],
        stage_traces: list[ReviewStageTrace],
    ) -> UnreadRangeSummaryRecord | None:
        actual_start_msg_log_id = int(messages[0]["id"])
        actual_end_msg_log_id = int(messages[-1]["id"])
        actual_start_at = float(messages[0]["created_at"])
        actual_end_at = float(messages[-1]["created_at"])
        stage_input = self._build_stage_input(
            session_id=session_id,
            messages=messages,
            purpose="overflow_compression",
            review_run_id=review_run_id,
            metadata={
                "start_msg_log_id": actual_start_msg_log_id,
                "end_msg_log_id": actual_end_msg_log_id,
                "message_count": len(messages),
                "reason": "overflow_pending_compression",
                **_trace_metadata_for_messages(messages, trace_by_message_id),
            },
        )
        if stage_input is None:
            return None

        stage_output = await self._run_compression_stage(stage_input)
        stage_traces.append(_trace_for_compression(stage_input, stage_output))
        return UnreadRangeSummaryRecord(
            session_id=session_id,
            start_msg_log_id=actual_start_msg_log_id,
            end_msg_log_id=actual_end_msg_log_id,
            start_at=actual_start_at,
            end_at=actual_end_at,
            message_count=len(messages),
            summary=stage_output.summary,
            candidate_message_ids=stage_output.candidate_message_ids,
            reason=stage_output.reason,
        )

    def _schedule_active_chat_bootstrap(
        self,
        *,
        scheduler: ReviewSchedulerPort,
        session_id: str,
        started_at: float,
        summaries: list[UnreadRangeSummaryRecord],
        reply: ReplyDecisionResult,
        active_epoch: int | None,
        review_run_id: str,
        trace_by_message_id: dict[int, str],
        stage_traces: list[ReviewStageTrace],
    ) -> ActiveChatBootstrapResult:
        coro = self._run_active_chat_bootstrap_with_timeout(
            scheduler=scheduler,
            session_id=session_id,
            started_at=started_at,
            summaries=summaries,
            reply=reply,
            active_epoch=active_epoch,
            review_run_id=review_run_id,
            trace_by_message_id=trace_by_message_id,
            stage_traces=list(stage_traces),
        )
        if self._bootstrap_task_scope is not None:
            task = self._bootstrap_task_scope.create_task(
                session_id,
                coro,
                name=f"review-active-chat-bootstrap:{session_id}",
            )
        else:
            task = asyncio.create_task(
                coro,
                name=f"review-active-chat-bootstrap:{session_id}",
            )
        self._bootstrap_tasks.add(task)
        task.add_done_callback(
            lambda completed, task_session_id=session_id: self._finish_bootstrap_task(
                task_session_id,
                completed,
            )
        )
        return ActiveChatBootstrapResult(
            reason="active_chat_bootstrap_scheduled",
            tail_history_start_at=(started_at - self._config.tail_history_before_seconds)
            * 1000,
        )

    def _finish_bootstrap_task(
        self,
        session_id: str,
        task: asyncio.Task[ActiveChatBootstrapResult],
    ) -> None:
        self._bootstrap_tasks.discard(task)
        try:
            result = task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "agent.review.bootstrap.task_failed",
                    session_id=session_id,
                    error_code=type(exc).__name__,
                )
            )
            return
        self._last_bootstrap_results[session_id] = result

    async def _run_active_chat_bootstrap_with_timeout(
        self,
        *,
        scheduler: ReviewSchedulerPort,
        session_id: str,
        started_at: float,
        summaries: list[UnreadRangeSummaryRecord],
        reply: ReplyDecisionResult,
        active_epoch: int | None,
        review_run_id: str,
        trace_by_message_id: dict[int, str],
        stage_traces: list[ReviewStageTrace],
    ) -> ActiveChatBootstrapResult:
        try:
            return await asyncio.wait_for(
                self._run_active_chat_bootstrap(
                    scheduler=scheduler,
                    session_id=session_id,
                    started_at=started_at,
                    summaries=summaries,
                    reply=reply,
                    active_epoch=active_epoch,
                    review_run_id=review_run_id,
                    trace_by_message_id=trace_by_message_id,
                    stage_traces=stage_traces,
                ),
                timeout=self._config.active_chat_bootstrap_timeout_seconds,
            )
        except TimeoutError:
            return ActiveChatBootstrapResult(
                reason="active_chat_bootstrap_timeout",
            )
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "agent.review.bootstrap.failed",
                    session_id=session_id,
                    review_run_id=review_run_id,
                    error_code=type(exc).__name__,
                    trace_id=_first_trace_id(trace_by_message_id),
                )
            )
            return ActiveChatBootstrapResult(
                reason="active_chat_bootstrap_failed",
            )

    async def _run_active_chat_bootstrap(
        self,
        *,
        scheduler: ReviewSchedulerPort,
        session_id: str,
        started_at: float,
        summaries: list[UnreadRangeSummaryRecord],
        reply: ReplyDecisionResult,
        active_epoch: int | None,
        review_run_id: str,
        trace_by_message_id: dict[int, str],
        stage_traces: list[ReviewStageTrace],
    ) -> ActiveChatBootstrapResult:
        ended_at = self._now()
        (
            tail_history_message_count,
            stage_input_built,
            stage_output,
        ) = await self._load_tail_history(
            session_id=session_id,
            started_at=started_at,
            ended_at=ended_at,
            summaries=summaries,
            reply=reply,
            review_run_id=review_run_id,
            trace_by_message_id=trace_by_message_id,
            stage_traces=stage_traces,
        )
        apply_decision = None
        if stage_output.disposition is not None:
            apply_decision = await self._apply_active_chat_bootstrap(
                scheduler=scheduler,
                session_id=session_id,
                disposition=stage_output.disposition,
                active_epoch=active_epoch,
                reason=stage_output.reason,
            )
        return ActiveChatBootstrapResult(
            disposition=stage_output.disposition,
            reason=stage_output.reason,
            bootstrap_applied=bool(
                apply_decision is not None and apply_decision.bootstrap_applied
            ),
            active_chat_interest_value=(
                apply_decision.active_chat_state.interest_value
                if apply_decision is not None
                and apply_decision.active_chat_state is not None
                else None
            ),
            active_chat_decay_half_life_seconds=(
                apply_decision.active_chat_state.decay_half_life_seconds
                if apply_decision is not None
                and apply_decision.active_chat_state is not None
                else None
            ),
            tail_history_start_at=(started_at - self._config.tail_history_before_seconds) * 1000,
            tail_history_end_at=ended_at * 1000,
            tail_history_message_count=tail_history_message_count,
            stage_input_built=stage_input_built,
        )

    async def _apply_active_chat_bootstrap(
        self,
        *,
        scheduler: ReviewSchedulerPort,
        session_id: str,
        disposition: ActiveChatDisposition,
        active_epoch: int | None,
        reason: str,
    ) -> ActiveChatBootstrapApplyDecision | None:
        handler = self._bootstrap_signal_handler
        if handler is None:
            return scheduler.apply_active_chat_bootstrap(
                session_id,
                disposition=disposition,
                active_epoch=active_epoch,
            )
        signal = AgentSignal(
            signal_id=f"active-chat-bootstrap:{session_id}:{active_epoch if active_epoch is not None else 'none'}",
            kind=AgentSignalKind.ACTIVE_CHAT_BOOTSTRAP,
            source=AgentSignalSource.MANUAL,
            session_id=session_id,
            occurred_at=self._now(),
            bot_id=self._bot_id,
            active_chat_bootstrap=AgentActiveChatBootstrapSignal(
                disposition=disposition,
                active_epoch=active_epoch,
                reason=reason,
            ),
            meta={"reason": reason},
        )
        result = handler(signal)
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            result = await result
        return result if isinstance(result, ActiveChatBootstrapApplyDecision) else None

    async def _load_scan_batches(
        self,
        session_id: str,
        unread_ranges: list[UnreadRange],
        *,
        max_messages: int,
        prefer_tail: bool,
        summaries: list[UnreadRangeSummaryRecord],
        review_run_id: str,
        stage_traces: list[ReviewStageTrace],
        trace_by_message_id: dict[int, str],
        use_partial_consumption: bool = False,
    ) -> tuple[int, int, list[int], list[str], list[ConsumedUnreadRange], list[asyncio.Task[ReviewBlockDigestStageOutput]]]:
        if self._message_store is None or max_messages <= 0:
            return 0, 0, [], [], [], []

        remaining = max_messages
        loaded_count = 0
        stage_input_count = 0
        block_index = 0
        candidate_message_ids: list[int] = []
        scan_reasons: list[str] = []
        consumed_ranges: list[ConsumedUnreadRange] = []
        block_digest_tasks: list[asyncio.Task[ReviewBlockDigestStageOutput]] = []
        block_digest_semaphore = asyncio.Semaphore(
            max(1, self._config.review_block_digest_concurrency)
        )
        scan_ranges = (
            self._tail_scan_ranges_from_store(unread_ranges, max_messages=max_messages)
            if prefer_tail
            else [
                self._full_consumed_range(
                    item,
                    full_range=not use_partial_consumption,
                )
                for item in unread_ranges
            ]
        )
        if use_partial_consumption and prefer_tail:
            scan_ranges = [
                ConsumedUnreadRange(
                    range_id=item.range_id,
                    session_id=item.session_id,
                    start_msg_log_id=item.start_msg_log_id,
                    end_msg_log_id=item.end_msg_log_id,
                    message_count=item.message_count,
                    full_range=False,
                )
                for item in scan_ranges
            ]
        for consumed_range in scan_ranges:
            offset = 0
            while remaining > 0:
                unread_range = self._unread_range_from_consumed(consumed_range)
                batch = self._message_store.list_for_unread_range(
                    unread_range,
                    limit=min(self._config.review_scan_batch_size, remaining),
                    offset=offset,
                )
                if not batch:
                    break
                loaded_count += len(batch)
                stage_input = self._build_stage_input(
                    session_id=session_id,
                    messages=batch,
                    purpose="review_scan",
                    review_run_id=review_run_id,
                    metadata={
                        "range_id": unread_range.id,
                        "range_start_msg_log_id": unread_range.start_msg_log_id,
                        "range_end_msg_log_id": unread_range.end_msg_log_id,
                        "offset": offset,
                        **_summary_metadata_payload(summaries),
                        **_trace_metadata_for_messages(batch, trace_by_message_id),
                    },
                    previous_summary=_format_overflow_summaries(summaries),
                )
                if stage_input is not None:
                    stage_input_count += 1
                    block_digest_tasks.append(
                        self._schedule_block_digest(
                            session_id=session_id,
                            messages=batch,
                            block_index=block_index,
                            range_id=unread_range.id,
                            range_start=unread_range.start_msg_log_id,
                            range_end=unread_range.end_msg_log_id,
                            review_run_id=review_run_id,
                            trace_by_message_id=trace_by_message_id,
                            semaphore=block_digest_semaphore,
                        )
                    )
                    block_index += 1
                    stage_output = await self._run_scan_stage(stage_input)
                    stage_traces.append(_trace_for_scan(stage_input, stage_output))
                    candidate_message_ids.extend(stage_output.candidate_message_ids)
                    if stage_output.reason.strip():
                        scan_reasons.append(stage_output.reason.strip())
                remaining -= len(batch)
                offset += len(batch)
            if offset > 0:
                consumed_ranges.append(consumed_range)
        return (
            loaded_count,
            stage_input_count,
            candidate_message_ids,
            scan_reasons,
            consumed_ranges,
            block_digest_tasks,
        )

    async def _load_tail_history(
        self,
        *,
        session_id: str,
        started_at: float,
        ended_at: float,
        summaries: list[UnreadRangeSummaryRecord],
        reply: ReplyDecisionResult,
        review_run_id: str,
        trace_by_message_id: dict[int, str],
        stage_traces: list[ReviewStageTrace],
    ) -> tuple[int, bool, ActiveChatBootstrapStageOutput]:
        if self._message_store is None:
            return (
                0,
                False,
                ActiveChatBootstrapStageOutput(
                    reason="active_chat_bootstrap_skipped_no_message_store",
                ),
            )

        tail_history = self._message_store.list_by_time(
            session_id=session_id,
            start_at=(started_at - self._config.tail_history_before_seconds) * 1000,
            end_at=ended_at * 1000,
            limit=self._config.tail_history_limit,
        )
        stage_input = self._build_stage_input(
            session_id=session_id,
            messages=tail_history,
            purpose="active_chat_bootstrap",
            review_run_id=review_run_id,
            metadata={
                "tail_history_start_at": (started_at - self._config.tail_history_before_seconds)
                * 1000,
                "tail_history_end_at": ended_at * 1000,
                "reply_replied": reply.replied,
                "reply_message_id": reply.reply_message_id,
                "reply_message_ids": reply.reply_message_ids,
                "reply_target_message_ids": reply.target_message_ids,
                "reply_reason": reply.reply_reason,
                **_summary_metadata_payload(summaries),
                **_trace_metadata_for_messages(tail_history, trace_by_message_id),
            },
            previous_summary=_format_overflow_summaries(summaries),
        )
        if stage_input is None:
            return (
                len(tail_history),
                False,
                ActiveChatBootstrapStageOutput(
                    reason="active_chat_bootstrap_skipped_no_stage_input",
                ),
            )
        stage_output = await self._run_bootstrap_stage(stage_input)
        stage_traces.append(_trace_for_bootstrap(stage_input, stage_output))
        return len(tail_history), True, stage_output

    def _build_stage_input(
        self,
        *,
        session_id: str,
        messages: list[dict],
        purpose: str,
        review_run_id: str,
        metadata: dict,
        previous_summary: str = "",
    ) -> ReviewStageInput | None:
        if "review_run_id" not in metadata:
            metadata = {"review_run_id": review_run_id, **metadata}
        stage_input = self._context_builder.build_for_messages(
            session_id=session_id,
            messages=messages,
            purpose=purpose,
            options=ReviewContextBuildOptions(
                previous_summary=previous_summary,
                metadata=metadata,
            ),
        )
        if stage_input is not None:
            if previous_summary and "previous_summary" not in stage_input.metadata:
                return ReviewStageInput(
                    session_id=stage_input.session_id,
                    purpose=stage_input.purpose,
                    source_messages=list(stage_input.source_messages),
                    instruction_content=list(stage_input.instruction_content),
                    context_messages=list(stage_input.context_messages),
                    metadata={
                        **dict(stage_input.metadata),
                        "previous_summary": previous_summary,
                    },
                )
            return stage_input
        return ReviewStageInput(
            session_id=session_id,
            purpose=purpose,
            source_messages=list(messages),
            metadata={
                "purpose": purpose,
                **metadata,
                **({"previous_summary": previous_summary} if previous_summary else {}),
            },
        )

    async def _run_scan_stage(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        return await self._scan_runner.run(stage_input)

    async def _run_compression_stage(
        self,
        stage_input: ReviewStageInput,
    ) -> OverflowCompressionStageOutput:
        return await self._compression_runner.run(stage_input)

    async def _run_reply_stage(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        return await self._reply_runner.run(stage_input)

    async def _run_bootstrap_stage(
        self,
        stage_input: ReviewStageInput,
    ) -> ActiveChatBootstrapStageOutput:
        return await self._bootstrap_runner.run(stage_input)

    async def _run_block_digest_stage(
        self,
        stage_input: ReviewStageInput,
    ) -> ReviewBlockDigestStageOutput:
        return await self._block_digest_runner.run(stage_input)

    def _query_recent_active_chat_summary(
        self, session_id: str
    ) -> str | None:
        if self._summary_service is None:
            return None
        try:
            from shinbot.agent.services.summaries import SummaryType

            record = self._summary_service.get_latest_by_session(
                session_id,
                summary_type=SummaryType.ACTIVE_CHAT,
            )
            if record is None:
                return None
            max_age = self._config.active_chat_summary_max_age_seconds
            if max_age > 0:
                created_at = float(getattr(record, "created_at", 0) or 0)
                if created_at > 0 and (self._now() - created_at) > max_age:
                    return None
            content = str(getattr(record, "content", "") or "").strip()
            return content or None
        except Exception as exc:
            logger.debug(
                format_log_event(
                    "agent.review.active_chat_summary.query_failed",
                    session_id=session_id,
                    error_code=type(exc).__name__,
                ),
                exc_info=True,
            )
            return None

    def _schedule_block_digest(
        self,
        *,
        session_id: str,
        messages: list[dict],
        block_index: int,
        range_id: int | None,
        range_start: int,
        range_end: int,
        review_run_id: str,
        trace_by_message_id: dict[int, str],
        semaphore: asyncio.Semaphore,
    ) -> asyncio.Task[ReviewBlockDigestStageOutput]:
        start_msg_log_id = _message_id(messages[0]) if messages else None
        end_msg_log_id = _message_id(messages[-1]) if messages else None
        stage_input = self._build_stage_input(
            session_id=session_id,
            messages=messages,
            purpose="review_block_digest",
            review_run_id=review_run_id,
            metadata={
                "block_index": block_index,
                "range_id": range_id,
                "range_start_msg_log_id": range_start,
                "range_end_msg_log_id": range_end,
                "start_msg_log_id": start_msg_log_id,
                "end_msg_log_id": end_msg_log_id,
                "message_count": len(messages),
                **_trace_metadata_for_messages(messages, trace_by_message_id),
            },
        )
        if stage_input is None:
            coro = _noop_block_digest(
                block_index=block_index,
                msg_log_start=start_msg_log_id,
                msg_log_end=end_msg_log_id,
                message_count=len(messages),
            )
            task_name = f"review-block-digest:{session_id}:{block_index}"
            if self._block_digest_task_scope is not None:
                return self._block_digest_task_scope.create_task(
                    f"{session_id}:{block_index}",
                    coro,
                    name=task_name,
                )
            return asyncio.create_task(coro, name=task_name)

        async def _run() -> ReviewBlockDigestStageOutput:
            async with semaphore:
                result = await self._run_block_digest_stage(stage_input)
            return _with_block_digest_metadata(
                result,
                block_index=block_index,
                msg_log_start=start_msg_log_id,
                msg_log_end=end_msg_log_id,
                message_count=len(messages),
            )

        task_name = f"review-block-digest:{session_id}:{block_index}"
        if self._block_digest_task_scope is not None:
            return self._block_digest_task_scope.create_task(
                f"{session_id}:{block_index}",
                _run(),
                name=task_name,
            )
        return asyncio.create_task(
            _run(),
            name=task_name,
        )

    async def _await_block_digests(
        self,
        tasks: list[asyncio.Task[ReviewBlockDigestStageOutput]],
    ) -> list[ReviewBlockDigestStageOutput]:
        if not tasks:
            return []
        results = await asyncio.gather(*tasks, return_exceptions=True)
        digests: list[ReviewBlockDigestStageOutput] = []
        for result in results:
            if isinstance(result, BaseException):
                logger.debug(
                    format_log_event(
                        "agent.review.block_digest.task_failed",
                        error_code=type(result).__name__,
                    ),
                    exc_info=(
                        type(result),
                        result,
                        result.__traceback__,
                    ),
                )
                continue
            digests.append(result)
        return digests

    def _save_overflow_summary(self, record: UnreadRangeSummaryRecord) -> None:
        if self._summary_store is None or not _should_persist_summary(record):
            return
        try:
            self._summary_store.save_summary(record, created_at=self._now())
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "agent.review.summary.persist_failed",
                    session_id=record.session_id,
                    start_msg_log_id=record.start_msg_log_id,
                    end_msg_log_id=record.end_msg_log_id,
                    error_code=type(exc).__name__,
                )
            )

    def _consume_review_ranges(
        self,
        scheduler: ReviewSchedulerPort,
        consumed_ranges: list[ConsumedUnreadRange],
    ) -> list[ConsumedUnreadRange]:
        if not consumed_ranges:
            return []

        applied: list[ConsumedUnreadRange] = []
        whole_range_ids: list[int] = []
        for consumed_range in consumed_ranges:
            if consumed_range.range_id is None:
                continue
            applied.append(consumed_range)
            if consumed_range.full_range:
                whole_range_ids.append(consumed_range.range_id)
                continue
            scheduler.split_review_consumed(
                range_id=consumed_range.range_id,
                consumed_start_msg_log_id=consumed_range.start_msg_log_id,
                consumed_end_msg_log_id=consumed_range.end_msg_log_id,
            )

        scheduler.mark_ranges_review_consumed(_dedupe_preserve_order(whole_range_ids))
        return applied

    def _planned_overflow_compression(
        self,
        *,
        session_id: str,
        unread_count: int,
        unread_ranges: list[UnreadRange],
    ) -> list[UnreadRangeSummaryRecord]:
        overflow_count = unread_count - self._config.overflow_threshold_messages
        if overflow_count <= 0:
            return []

        remaining = overflow_count
        records: list[UnreadRangeSummaryRecord] = []
        for unread_range in unread_ranges:
            if remaining <= 0:
                break
            message_count = min(remaining, unread_range.message_count)
            end_msg_log_id = min(
                unread_range.end_msg_log_id,
                unread_range.start_msg_log_id + message_count - 1,
            )
            records.append(
                UnreadRangeSummaryRecord(
                    session_id=session_id,
                    start_msg_log_id=unread_range.start_msg_log_id,
                    end_msg_log_id=end_msg_log_id,
                    start_at=unread_range.start_at,
                    end_at=unread_range.end_at,
                    message_count=message_count,
                    summary="",
                )
            )
            remaining -= message_count
        return records

    def _tail_scan_ranges(
        self,
        unread_ranges: list[UnreadRange],
        *,
        max_messages: int,
    ) -> list[ConsumedUnreadRange]:
        remaining = max_messages
        selected: list[ConsumedUnreadRange] = []
        for unread_range in reversed(unread_ranges):
            if remaining <= 0:
                break
            if unread_range.message_count <= remaining:
                selected.append(self._full_consumed_range(unread_range))
                remaining -= unread_range.message_count
                continue

            selected.append(
                ConsumedUnreadRange(
                    range_id=unread_range.id,
                    session_id=unread_range.session_id,
                    start_msg_log_id=unread_range.end_msg_log_id - remaining + 1,
                    end_msg_log_id=unread_range.end_msg_log_id,
                    message_count=remaining,
                    full_range=False,
                )
            )
            remaining = 0
        selected.reverse()
        return selected

    def _tail_scan_ranges_from_store(
        self,
        unread_ranges: list[UnreadRange],
        *,
        max_messages: int,
    ) -> list[ConsumedUnreadRange]:
        if self._message_store is None:
            return self._tail_scan_ranges(unread_ranges, max_messages=max_messages)

        remaining = max_messages
        selected: list[ConsumedUnreadRange] = []
        for unread_range in reversed(unread_ranges):
            if remaining <= 0:
                break
            take = min(remaining, unread_range.message_count)
            offset = max(unread_range.message_count - take, 0)
            messages = self._message_store.list_for_unread_range(
                unread_range,
                limit=take,
                offset=offset,
            )
            if not messages:
                continue
            selected.append(
                ConsumedUnreadRange(
                    range_id=unread_range.id,
                    session_id=unread_range.session_id,
                    start_msg_log_id=int(messages[0]["id"]),
                    end_msg_log_id=int(messages[-1]["id"]),
                    message_count=len(messages),
                    full_range=len(messages) == unread_range.message_count,
                )
            )
            remaining -= len(messages)
        selected.reverse()
        return selected

    @staticmethod
    def _full_consumed_range(
        unread_range: UnreadRange,
        *,
        full_range: bool = True,
    ) -> ConsumedUnreadRange:
        return ConsumedUnreadRange(
            range_id=unread_range.id,
            session_id=unread_range.session_id,
            start_msg_log_id=unread_range.start_msg_log_id,
            end_msg_log_id=unread_range.end_msg_log_id,
            message_count=unread_range.message_count,
            full_range=full_range,
        )

    @staticmethod
    def _unread_range_from_consumed(consumed_range: ConsumedUnreadRange) -> UnreadRange:
        return UnreadRange(
            id=consumed_range.range_id,
            session_id=consumed_range.session_id,
            start_msg_log_id=consumed_range.start_msg_log_id,
            end_msg_log_id=consumed_range.end_msg_log_id,
            start_at=0.0,
            end_at=0.0,
            message_count=consumed_range.message_count,
        )


async def _noop_block_digest(
    *,
    block_index: int,
    msg_log_start: int | None,
    msg_log_end: int | None,
    message_count: int,
) -> ReviewBlockDigestStageOutput:
    return ReviewBlockDigestStageOutput(
        reason="block_digest_skipped_no_stage_input",
        block_index=block_index,
        msg_log_start=msg_log_start,
        msg_log_end=msg_log_end,
        message_count=message_count,
    )


def _with_block_digest_metadata(
    digest: ReviewBlockDigestStageOutput,
    *,
    block_index: int,
    msg_log_start: int | None,
    msg_log_end: int | None,
    message_count: int,
) -> ReviewBlockDigestStageOutput:
    return replace(
        digest,
        block_index=block_index if digest.block_index is None else digest.block_index,
        msg_log_start=msg_log_start if digest.msg_log_start is None else digest.msg_log_start,
        msg_log_end=msg_log_end if digest.msg_log_end is None else digest.msg_log_end,
        message_count=message_count if digest.message_count <= 0 else digest.message_count,
    )


def _dedupe_preserve_order[T](items: list[T]) -> list[T]:
    seen: set[T] = set()
    result: list[T] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _message_id(message: dict) -> int | None:
    value = message.get("id")
    if isinstance(value, int):
        return value
    return None


def _message_ids(messages: list[dict]) -> list[int]:
    return [message_id for message in messages if (message_id := _message_id(message)) is not None]


def _trace_by_message_id(unread_messages: list[UnreadMessage]) -> dict[int, str]:
    result: dict[int, str] = {}
    for message in unread_messages:
        trace_id = str(message.trace_id or "").strip()
        if not trace_id:
            continue
        result[message.message_log_id] = trace_id
    return result


def _trace_metadata_for_messages(
    messages: list[dict],
    trace_by_message_id: dict[int, str],
) -> dict[str, object]:
    trace_ids = _dedupe_preserve_order(
        [
            trace_id
            for message in messages
            if (message_id := _message_id(message)) is not None
            if (trace_id := trace_by_message_id.get(message_id))
        ]
    )
    if not trace_ids:
        return {}
    if len(trace_ids) == 1:
        return {"trace_id": trace_ids[0]}
    return {
        "trace_id": trace_ids[0],
        "trace_ids": trace_ids,
    }


def _first_trace_id(trace_by_message_id: dict[int, str]) -> str:
    for trace_id in trace_by_message_id.values():
        if trace_id:
            return trace_id
    return ""


def _freeze_unread_ranges(
    unread_ranges: list[UnreadRange],
    unread_messages: list[UnreadMessage],
) -> list[UnreadRange]:
    """Clamp current unread ranges to the review-entry unread message snapshot."""
    if not unread_messages:
        return list(unread_ranges)

    messages_by_id = {
        message.message_log_id: message
        for message in sorted(
            unread_messages,
            key=lambda item: (item.created_at, item.message_log_id),
        )
    }
    frozen_ranges: list[UnreadRange] = []
    for unread_range in unread_ranges:
        messages = [
            message
            for message_id, message in messages_by_id.items()
            if unread_range.start_msg_log_id <= message_id <= unread_range.end_msg_log_id
        ]
        if not messages:
            continue
        frozen_ranges.append(
            UnreadRange(
                id=unread_range.id,
                session_id=unread_range.session_id,
                start_msg_log_id=messages[0].message_log_id,
                end_msg_log_id=messages[-1].message_log_id,
                start_at=messages[0].created_at,
                end_at=messages[-1].created_at,
                message_count=len(messages),
                review_consumed=unread_range.review_consumed,
                chat_consumed=unread_range.chat_consumed,
            )
        )
    return frozen_ranges


def _unread_ranges_differ(
    left: list[UnreadRange],
    right: list[UnreadRange],
) -> bool:
    return [
        (
            item.id,
            item.session_id,
            item.start_msg_log_id,
            item.end_msg_log_id,
            item.message_count,
        )
        for item in left
    ] != [
        (
            item.id,
            item.session_id,
            item.start_msg_log_id,
            item.end_msg_log_id,
            item.message_count,
        )
        for item in right
    ]


def _should_persist_summary(record: UnreadRangeSummaryRecord) -> bool:
    return (
        bool(record.summary.strip())
        or bool(record.candidate_message_ids)
        or record.reason.strip() not in {"", "noop_overflow_compression"}
    )


def _trace_base(stage_input: ReviewStageInput) -> dict[str, object]:
    return {
        "purpose": stage_input.purpose,
        "message_ids": [
            int(message["id"])
            for message in stage_input.source_messages
            if "id" in message
        ],
        "metadata": dict(stage_input.metadata),
        "previous_summary": str(stage_input.metadata.get("previous_summary") or ""),
    }


def _trace_for_compression(
    stage_input: ReviewStageInput,
    stage_output: OverflowCompressionStageOutput,
) -> ReviewStageTrace:
    return ReviewStageTrace(
        **_trace_base(stage_input),
        reason=stage_output.reason,
        candidate_message_ids=list(stage_output.candidate_message_ids),
    )


def _trace_for_scan(
    stage_input: ReviewStageInput,
    stage_output: ReviewScanStageOutput,
) -> ReviewStageTrace:
    return ReviewStageTrace(
        **_trace_base(stage_input),
        reason=stage_output.reason,
        candidate_message_ids=list(stage_output.candidate_message_ids),
    )


def _trace_for_reply(
    stage_input: ReviewStageInput,
    stage_output: ReplyDecisionStageOutput,
) -> ReviewStageTrace:
    return ReviewStageTrace(
        **_trace_base(stage_input),
        reason=stage_output.reason,
        target_message_ids=list(stage_output.target_message_ids),
        replied=stage_output.replied,
        reply_message_id=stage_output.reply_message_id,
        reply_message_ids=list(stage_output.reply_message_ids),
    )


def _trace_for_bootstrap(
    stage_input: ReviewStageInput,
    stage_output: ActiveChatBootstrapStageOutput,
) -> ReviewStageTrace:
    return ReviewStageTrace(
        **_trace_base(stage_input),
        reason=stage_output.reason,
        active_chat_disposition=stage_output.disposition,
    )


def _summary_metadata(records: list[UnreadRangeSummaryRecord]) -> list[dict[str, object]]:
    return [
        {
            "start_msg_log_id": record.start_msg_log_id,
            "end_msg_log_id": record.end_msg_log_id,
            "message_count": record.message_count,
            "summary": record.summary,
            "candidate_message_ids": list(record.candidate_message_ids),
            "reason": record.reason,
        }
        for record in records
        if _should_persist_summary(record)
    ]


def _summary_metadata_payload(
    records: list[UnreadRangeSummaryRecord],
) -> dict[str, list[dict[str, object]]]:
    summaries = _summary_metadata(records)
    return {"overflow_summaries": summaries} if summaries else {}


def _format_overflow_summaries(records: list[UnreadRangeSummaryRecord]) -> str:
    lines: list[str] = []
    for record in records:
        if not _should_persist_summary(record):
            continue
        summary = record.summary.strip() or "(no textual summary)"
        candidate_ids = ", ".join(str(item) for item in record.candidate_message_ids)
        candidate_suffix = f"; candidate_msgids={candidate_ids}" if candidate_ids else ""
        lines.append(
            "Unread overflow summary "
            f"[msgid {record.start_msg_log_id}-{record.end_msg_log_id}; "
            f"count={record.message_count}{candidate_suffix}]: {summary}"
        )
    return "\n".join(lines)


def _block_digest_metadata_payload(
    digests: list[ReviewBlockDigestStageOutput],
) -> dict[str, list[dict[str, object]]]:
    entries = [
        {
            "block_index": digest.block_index,
            "msg_log_start": digest.msg_log_start,
            "msg_log_end": digest.msg_log_end,
            "message_count": digest.message_count,
            "summary": digest.summary,
            "reason": digest.reason,
        }
        for digest in digests
        if digest.summary.strip()
    ]
    return {"block_digests": entries} if entries else {}


def _select_reply_block_digests(
    digests: list[ReviewBlockDigestStageOutput],
    *,
    candidate_message_ids: list[int],
    messages: list[dict],
) -> list[ReviewBlockDigestStageOutput]:
    if not digests:
        return []

    target_indices: set[int] = set()
    for digest in digests:
        if digest.block_index is None:
            continue
        if _digest_contains_any(digest, candidate_message_ids):
            target_indices.add(digest.block_index)

    if not target_indices:
        message_ids = _message_ids(messages)
        for digest in digests:
            if digest.block_index is None:
                continue
            if _digest_contains_any(digest, message_ids):
                target_indices.add(digest.block_index)

    if not target_indices:
        return []

    selected_indices = {
        index + delta
        for index in target_indices
        for delta in (-1, 0, 1)
    }
    return [
        digest
        for digest in digests
        if digest.block_index is not None and digest.block_index in selected_indices
    ]


def _digest_contains_any(
    digest: ReviewBlockDigestStageOutput,
    message_ids: list[int],
) -> bool:
    if digest.msg_log_start is None or digest.msg_log_end is None:
        return False
    return any(digest.msg_log_start <= message_id <= digest.msg_log_end for message_id in message_ids)


def _active_chat_summary_metadata(
    active_chat_summary: str | None,
) -> dict[str, str]:
    if not active_chat_summary:
        return {}
    return {"active_chat_summary": active_chat_summary}


def _format_reply_previous_summary(
    *,
    overflow: str = "",
    block_digests: list[ReviewBlockDigestStageOutput] | None = None,
    active_chat_summary: str | None = None,
) -> str:
    parts: list[str] = []
    if overflow:
        parts.append(overflow)
    for digest in block_digests or []:
        summary = digest.summary.strip()
        if not summary:
            continue
        block_index = digest.block_index if digest.block_index is not None else "unknown"
        parts.append(f"Block digest [block {block_index}]: {summary}")
    if active_chat_summary:
        parts.append(f"Recent active chat summary: {active_chat_summary}")
    return "\n".join(parts)
