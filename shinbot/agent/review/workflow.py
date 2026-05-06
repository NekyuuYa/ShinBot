"""Skeleton implementation for the Agent review workflow."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from typing import Protocol

from shinbot.agent.review.context_builder import (
    ReviewContextBuilder,
    ReviewContextBuildOptions,
)
from shinbot.agent.review.message_store import ReviewMessageStore
from shinbot.agent.review.models import (
    ActiveChatBootstrapResult,
    ReplyDecisionResult,
    ReviewScanResult,
    ReviewWorkflowConfig,
    ReviewWorkflowResult,
    UnreadRangeSummaryRecord,
)
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
        now: Callable[[], float] | None = None,
    ) -> None:
        self._config = config or ReviewWorkflowConfig()
        self._message_store = message_store
        self._context_builder = context_builder
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
            scan = self._run_review_scan(
                session_id=session_id,
                unread_count=unread_count,
                unread_ranges=unread_ranges,
            )
            reply = self._run_reply_decision(scan)
            bootstrap = self._run_active_chat_bootstrap(
                session_id=session_id,
                started_at=started_at,
            )
            completion = scheduler.complete_review(
                session_id,
                enter_active_chat=True,
                active_chat_initial_interest=bootstrap.initial_interest,
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
            bootstrap = self._run_active_chat_bootstrap(
                session_id=session_id,
                started_at=started_at,
            )
            completion = scheduler.complete_review(
                session_id,
                enter_active_chat=True,
                active_chat_initial_interest=bootstrap.initial_interest,
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

    def _run_review_scan(
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
        loaded_message_count, stage_input_count = self._load_scan_batches(
            session_id=session_id,
            unread_ranges=unread_ranges,
            max_messages=scanned_count,
            prefer_tail=unread_count > self._config.overflow_threshold_messages,
        )
        return ReviewScanResult(
            scanned_message_count=scanned_count,
            loaded_message_count=loaded_message_count,
            stage_input_count=stage_input_count,
            batch_count=batch_count,
            compressed_ranges=compressed_ranges,
        )

    def _run_reply_decision(self, scan: ReviewScanResult) -> ReplyDecisionResult:
        return ReplyDecisionResult(target_message_ids=scan.candidate_message_ids)

    def _run_active_chat_bootstrap(
        self,
        *,
        session_id: str,
        started_at: float,
    ) -> ActiveChatBootstrapResult:
        ended_at = self._now()
        tail_history_message_count, stage_input_built = self._load_tail_history(
            session_id=session_id,
            started_at=started_at,
            ended_at=ended_at,
        )
        return ActiveChatBootstrapResult(
            initial_interest=self._config.fallback_active_chat_interest,
            tail_history_start_at=(started_at - self._config.tail_history_before_seconds) * 1000,
            tail_history_end_at=ended_at * 1000,
            tail_history_message_count=tail_history_message_count,
            stage_input_built=stage_input_built,
        )

    def _load_scan_batches(
        self,
        session_id: str,
        unread_ranges: list[UnreadRange],
        *,
        max_messages: int,
        prefer_tail: bool,
    ) -> tuple[int, int]:
        if self._message_store is None or max_messages <= 0:
            return 0, 0

        remaining = max_messages
        loaded_count = 0
        stage_input_count = 0
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
                if self._build_stage_input(
                    session_id=session_id,
                    messages=batch,
                    purpose="review_scan",
                    metadata={
                        "range_id": unread_range.id,
                        "range_start_msg_log_id": unread_range.start_msg_log_id,
                        "range_end_msg_log_id": unread_range.end_msg_log_id,
                        "offset": offset,
                    },
                ):
                    stage_input_count += 1
                remaining -= len(batch)
                offset += len(batch)
        return loaded_count, stage_input_count

    def _load_tail_history(
        self,
        *,
        session_id: str,
        started_at: float,
        ended_at: float,
    ) -> tuple[int, bool]:
        if self._message_store is None:
            return 0, False

        tail_history = self._message_store.list_by_time(
            session_id=session_id,
            start_at=(started_at - self._config.tail_history_before_seconds) * 1000,
            end_at=ended_at * 1000,
            limit=self._config.tail_history_limit,
        )
        stage_input_built = self._build_stage_input(
            session_id=session_id,
            messages=tail_history,
            purpose="active_chat_bootstrap",
            metadata={
                "tail_history_start_at": (started_at - self._config.tail_history_before_seconds)
                * 1000,
                "tail_history_end_at": ended_at * 1000,
            },
        )
        return len(tail_history), stage_input_built

    def _build_stage_input(
        self,
        *,
        session_id: str,
        messages: list[dict],
        purpose: str,
        metadata: dict,
    ) -> bool:
        if self._context_builder is None:
            return False
        self._context_builder.build_for_messages(
            session_id=session_id,
            messages=messages,
            purpose=purpose,
            options=ReviewContextBuildOptions(metadata=metadata),
        )
        return True

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
