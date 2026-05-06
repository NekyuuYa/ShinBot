"""Skeleton implementation for the Agent review workflow."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from typing import Protocol

from shinbot.agent.review.bootstrap import (
    ActiveChatBootstrapStageRunner,
    NoopActiveChatBootstrapStageRunner,
)
from shinbot.agent.review.context_builder import (
    ReviewContextBuilder,
    ReviewContextBuilderAdapter,
    ReviewContextBuildOptions,
    ReviewStageInput,
)
from shinbot.agent.review.message_store import ReviewMessageStore
from shinbot.agent.review.models import (
    ActiveChatBootstrapResult,
    ActiveChatBootstrapStageOutput,
    ReplyDecisionResult,
    ReplyDecisionStageOutput,
    ReviewScanResult,
    ReviewScanStageOutput,
    ReviewWorkflowConfig,
    ReviewWorkflowResult,
    UnreadRangeSummaryRecord,
)
from shinbot.agent.review.reply import NoopReplyDecisionStageRunner, ReplyDecisionStageRunner
from shinbot.agent.review.scan import NoopReviewScanStageRunner, ReviewScanStageRunner
from shinbot.agent.scheduler.models import (
    ReviewCompletionDecision,
    ReviewPlan,
    UnreadMessage,
    UnreadRange,
)

logger = logging.getLogger(__name__)


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


class ReviewWorkflow:
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
        context_builder: ReviewContextBuilder | None = None,
        scan_runner: ReviewScanStageRunner | None = None,
        reply_runner: ReplyDecisionStageRunner | None = None,
        bootstrap_runner: ActiveChatBootstrapStageRunner | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        self._config = config or ReviewWorkflowConfig()
        self._message_store = message_store
        self._context_builder = context_builder or ReviewContextBuilderAdapter()
        self._scan_runner = scan_runner or NoopReviewScanStageRunner()
        self._reply_runner = reply_runner or NoopReplyDecisionStageRunner()
        self._bootstrap_runner = bootstrap_runner or NoopActiveChatBootstrapStageRunner(
            initial_interest=self._config.fallback_active_chat_interest,
        )
        self._now = now or time.time

    async def run(
        self,
        *,
        scheduler: ReviewSchedulerPort,
        session_id: str,
        review_plan: ReviewPlan,
        unread_messages: list[UnreadMessage],
    ) -> ReviewWorkflowResult:
        """Run the review shell and always hand control back to scheduler."""
        started_at = self._now()
        try:
            unread_ranges = scheduler.unread_ranges(session_id, limit=10_000)
            unread_count = scheduler.count_unread_messages(session_id)
            scan = await self._run_review_scan(
                session_id=session_id,
                unread_count=unread_count,
                unread_ranges=unread_ranges,
            )
            reply = await self._run_reply_decision(session_id=session_id, scan=scan)
            bootstrap = await self._run_active_chat_bootstrap(
                session_id=session_id,
                started_at=started_at,
                reply=reply,
            )
            completion = scheduler.complete_review(
                session_id,
                enter_active_chat=True,
                active_chat_initial_interest=bootstrap.initial_interest,
                active_chat_decay_half_life_seconds=bootstrap.decay_half_life_seconds,
            )
            return ReviewWorkflowResult(
                scan=scan,
                reply=reply,
                bootstrap=bootstrap,
                review_started_at=started_at,
                completion=completion,
            )
        except Exception as exc:
            logger.exception(
                "Review workflow failed for session %s with plan %s",
                session_id,
                review_plan.reason,
            )
            bootstrap = await self._run_active_chat_bootstrap(
                session_id=session_id,
                started_at=started_at,
                reply=ReplyDecisionResult(),
            )
            completion = scheduler.complete_review(
                session_id,
                enter_active_chat=True,
                active_chat_initial_interest=bootstrap.initial_interest,
                active_chat_decay_half_life_seconds=bootstrap.decay_half_life_seconds,
            )
            return ReviewWorkflowResult(
                scan=ReviewScanResult(),
                reply=ReplyDecisionResult(),
                bootstrap=bootstrap,
                review_started_at=started_at,
                completion=completion,
                failed=True,
                failure_reason=str(exc),
            )

    async def _run_review_scan(
        self,
        *,
        session_id: str,
        unread_count: int,
        unread_ranges: list[UnreadRange],
    ) -> ReviewScanResult:
        scanned_count = min(unread_count, self._config.overflow_threshold_messages)
        batch_count = (
            math.ceil(scanned_count / self._config.review_scan_batch_size)
            if scanned_count
            else 0
        )
        compressed_ranges = self._planned_overflow_compression(
            session_id=session_id,
            unread_count=unread_count,
            unread_ranges=unread_ranges,
        )
        (
            loaded_message_count,
            stage_input_count,
            candidate_message_ids,
            scan_reasons,
        ) = await self._load_scan_batches(
            session_id=session_id,
            unread_ranges=unread_ranges,
            max_messages=scanned_count,
            prefer_tail=unread_count > self._config.overflow_threshold_messages,
        )
        return ReviewScanResult(
            candidate_message_ids=_dedupe_preserve_order(candidate_message_ids),
            scan_reason="; ".join(_dedupe_preserve_order(scan_reasons))
            or "noop_review_scan",
            scanned_message_count=scanned_count,
            loaded_message_count=loaded_message_count,
            stage_input_count=stage_input_count,
            batch_count=batch_count,
            compressed_ranges=compressed_ranges,
        )

    async def _run_reply_decision(
        self,
        *,
        session_id: str,
        scan: ReviewScanResult,
    ) -> ReplyDecisionResult:
        if not scan.candidate_message_ids:
            return ReplyDecisionResult()
        if self._message_store is None:
            return ReplyDecisionResult(
                target_message_ids=scan.candidate_message_ids,
                reply_reason="reply_decision_skipped_no_message_store",
            )

        loaded_message_count = 0
        stage_input_count = 0
        replied = False
        reply_message_id = None
        target_message_ids: list[int] = []
        reply_reasons: list[str] = []

        for candidate_message_id in scan.candidate_message_ids:
            local_context = self._message_store.list_around_message(
                session_id=session_id,
                message_log_id=candidate_message_id,
                before=self._config.reply_context_before_messages,
                after=self._config.reply_context_after_messages,
            )
            loaded_message_count += len(local_context)
            stage_input = self._build_stage_input(
                session_id=session_id,
                messages=local_context,
                purpose="reply_decision",
                metadata={
                    "candidate_message_id": candidate_message_id,
                    "before_messages": self._config.reply_context_before_messages,
                    "after_messages": self._config.reply_context_after_messages,
                },
            )
            if stage_input is None:
                continue
            stage_input_count += 1
            stage_output = await self._run_reply_stage(stage_input)
            replied = replied or stage_output.replied
            if reply_message_id is None:
                reply_message_id = stage_output.reply_message_id
            target_message_ids.extend(stage_output.target_message_ids)
            if stage_output.reason.strip():
                reply_reasons.append(stage_output.reason.strip())

        return ReplyDecisionResult(
            replied=replied,
            reply_message_id=reply_message_id,
            target_message_ids=_dedupe_preserve_order(target_message_ids),
            reply_reason="; ".join(_dedupe_preserve_order(reply_reasons))
            or "noop_reply_decision",
            loaded_message_count=loaded_message_count,
            stage_input_count=stage_input_count,
        )

    async def _run_active_chat_bootstrap(
        self,
        *,
        session_id: str,
        started_at: float,
        reply: ReplyDecisionResult,
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
            reply=reply,
        )
        return ActiveChatBootstrapResult(
            initial_interest=stage_output.initial_interest,
            decay_half_life_seconds=stage_output.decay_half_life_seconds,
            reason=stage_output.reason,
            tail_history_start_at=(started_at - self._config.tail_history_before_seconds) * 1000,
            tail_history_end_at=ended_at * 1000,
            tail_history_message_count=tail_history_message_count,
            stage_input_built=stage_input_built,
        )

    async def _load_scan_batches(
        self,
        session_id: str,
        unread_ranges: list[UnreadRange],
        *,
        max_messages: int,
        prefer_tail: bool,
    ) -> tuple[int, int, list[int], list[str]]:
        if self._message_store is None or max_messages <= 0:
            return 0, 0, [], []

        remaining = max_messages
        loaded_count = 0
        stage_input_count = 0
        candidate_message_ids: list[int] = []
        scan_reasons: list[str] = []
        scan_ranges = (
            self._tail_scan_ranges(unread_ranges, max_messages=max_messages)
            if prefer_tail
            else unread_ranges
        )
        for unread_range in scan_ranges:
            offset = 0
            while remaining > 0:
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
                    metadata={
                        "range_id": unread_range.id,
                        "range_start_msg_log_id": unread_range.start_msg_log_id,
                        "range_end_msg_log_id": unread_range.end_msg_log_id,
                        "offset": offset,
                    },
                )
                if stage_input is not None:
                    stage_input_count += 1
                    stage_output = await self._run_scan_stage(stage_input)
                    candidate_message_ids.extend(stage_output.candidate_message_ids)
                    if stage_output.reason.strip():
                        scan_reasons.append(stage_output.reason.strip())
                remaining -= len(batch)
                offset += len(batch)
        return loaded_count, stage_input_count, candidate_message_ids, scan_reasons

    async def _load_tail_history(
        self,
        *,
        session_id: str,
        started_at: float,
        ended_at: float,
        reply: ReplyDecisionResult,
    ) -> tuple[int, bool, ActiveChatBootstrapStageOutput]:
        if self._message_store is None:
            return (
                0,
                False,
                ActiveChatBootstrapStageOutput(
                    initial_interest=self._config.fallback_active_chat_interest,
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
            metadata={
                "tail_history_start_at": (started_at - self._config.tail_history_before_seconds)
                * 1000,
                "tail_history_end_at": ended_at * 1000,
                "reply_replied": reply.replied,
                "reply_message_id": reply.reply_message_id,
                "reply_target_message_ids": reply.target_message_ids,
                "reply_reason": reply.reply_reason,
            },
        )
        if stage_input is None:
            return (
                len(tail_history),
                False,
                ActiveChatBootstrapStageOutput(
                    initial_interest=self._config.fallback_active_chat_interest,
                    reason="active_chat_bootstrap_skipped_no_stage_input",
                ),
            )
        stage_output = await self._run_bootstrap_stage(stage_input)
        return len(tail_history), True, stage_output

    def _build_stage_input(
        self,
        *,
        session_id: str,
        messages: list[dict],
        purpose: str,
        metadata: dict,
    ) -> ReviewStageInput | None:
        stage_input = self._context_builder.build_for_messages(
            session_id=session_id,
            messages=messages,
            purpose=purpose,
            options=ReviewContextBuildOptions(metadata=metadata),
        )
        if stage_input is not None:
            return stage_input
        return ReviewStageInput(
            session_id=session_id,
            purpose=purpose,
            source_messages=list(messages),
            metadata={"purpose": purpose, **metadata},
        )

    async def _run_scan_stage(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        return await self._scan_runner.run(stage_input)

    async def _run_reply_stage(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        return await self._reply_runner.run(stage_input)

    async def _run_bootstrap_stage(
        self,
        stage_input: ReviewStageInput,
    ) -> ActiveChatBootstrapStageOutput:
        return await self._bootstrap_runner.run(stage_input)

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
    ) -> list[UnreadRange]:
        remaining = max_messages
        selected: list[UnreadRange] = []
        for unread_range in reversed(unread_ranges):
            if remaining <= 0:
                break
            if unread_range.message_count <= remaining:
                selected.append(unread_range)
                remaining -= unread_range.message_count
                continue

            selected.append(
                UnreadRange(
                    id=unread_range.id,
                    session_id=unread_range.session_id,
                    start_msg_log_id=unread_range.end_msg_log_id - remaining + 1,
                    end_msg_log_id=unread_range.end_msg_log_id,
                    start_at=unread_range.start_at,
                    end_at=unread_range.end_at,
                    message_count=remaining,
                    review_consumed=unread_range.review_consumed,
                    chat_consumed=unread_range.chat_consumed,
                )
            )
            remaining = 0
        selected.reverse()
        return selected


def _dedupe_preserve_order[T](items: list[T]) -> list[T]:
    seen: set[T] = set()
    result: list[T] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
