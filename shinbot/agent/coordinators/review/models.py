"""Result models for the Agent review workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from shinbot.agent.scheduler.models import (
    ActiveChatDisposition,
    ReviewCompletionDecision,
    ReviewPlan,
)


@dataclass(slots=True, frozen=True)
class ReviewWorkflowConfig:
    """Tunable limits for the review workflow skeleton."""

    review_scan_batch_size: int = 500
    overflow_threshold_messages: int = 3000
    overflow_compression_batch_size: int = 500
    reply_context_before_messages: int = 30
    reply_context_after_messages: int = 10
    tail_history_before_seconds: float = 180.0
    tail_history_limit: int = 500
    active_chat_summary_max_age_seconds: float = 1800.0
    review_block_digest_concurrency: int = 4
    provisional_active_chat_interest: float = 15.0
    provisional_active_chat_half_life_seconds: float = 20.0
    reply_commit_timeout_seconds: float = 20.0
    active_chat_bootstrap_timeout_seconds: float = 20.0
    deferred_consumption_retry_after_seconds: float = 30.0
    idle_review_planning_min_after_seconds: float = 30.0
    idle_review_planning_max_after_seconds: float = 3600.0


@dataclass(slots=True, frozen=True)
class UnreadRangeSummaryRecord:
    """A planned or completed compression record for old overflow unread messages."""

    session_id: str
    start_msg_log_id: int
    end_msg_log_id: int
    start_at: float
    end_at: float
    message_count: int
    summary: str
    candidate_message_ids: list[int] = field(default_factory=list)
    reason: str = "overflow_pending_compression"


@dataclass(slots=True, frozen=True)
class UnreadRangeIgnoreRecord:
    """A record explaining an unread interval intentionally skipped by review."""

    session_id: str
    start_msg_log_id: int
    end_msg_log_id: int
    start_at: float
    end_at: float
    message_count: int
    reason: str


@dataclass(slots=True, frozen=True)
class ConsumedUnreadRange:
    """Unread range interval consumed by one review run."""

    range_id: int | None
    session_id: str
    start_msg_log_id: int
    end_msg_log_id: int
    message_count: int
    full_range: bool = False


class ReviewSchedulerCommitKind(StrEnum):
    """Scheduler-owned mutation requested by a completed review stage."""

    CONSUME_RANGES = "consume_ranges"
    COMPLETE_REVIEW = "complete_review"


@dataclass(slots=True, frozen=True)
class ReviewSchedulerCommitIntent:
    """Immutable review mutation request for the runtime state owner.

    Review stages can run model and tool work without holding the runtime
    session mutex. Their durable unread consumption and terminal state
    transition are submitted through this value after that external work ends.
    """

    kind: ReviewSchedulerCommitKind
    session_id: str
    review_run_id: str
    expected_review_plan: ReviewPlan
    next_review_plan: ReviewPlan | None = None
    consumed_ranges: tuple[ConsumedUnreadRange, ...] = ()
    enter_active_chat: bool = False
    active_chat_initial_interest: float | None = None
    active_chat_decay_half_life_seconds: float | None = None


@dataclass(slots=True, frozen=True)
class ReviewSchedulerCommitDecision:
    """Terminal result of a runtime-owned review scheduler mutation."""

    session_id: str
    accepted: bool
    completion: ReviewCompletionDecision | None = None
    consumed_ranges: tuple[ConsumedUnreadRange, ...] = ()
    skipped_reason: str | None = None


@dataclass(slots=True, frozen=True)
class ReviewStageTrace:
    """Explainability record for one review stage runner invocation."""

    purpose: str
    message_ids: list[int] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)
    previous_summary: str = ""
    reason: str = ""
    candidate_message_ids: list[int] = field(default_factory=list)
    target_message_ids: list[int] = field(default_factory=list)
    replied: bool | None = None
    reply_message_id: int | None = None
    reply_message_ids: list[int] = field(default_factory=list)
    active_chat_disposition: ActiveChatDisposition | None = None
    active_chat_bootstrap_applied: bool | None = None
    active_chat_interest_value: float | None = None
    active_chat_decay_half_life_seconds: float | None = None


@dataclass(slots=True, frozen=True)
class ReviewStageExplanation:
    """Stable summary of one review stage for logs or user-facing explanations."""

    purpose: str
    input_message_count: int
    reason: str = ""
    candidate_message_ids: list[int] = field(default_factory=list)
    target_message_ids: list[int] = field(default_factory=list)
    replied: bool | None = None
    reply_message_id: int | None = None
    reply_message_ids: list[int] = field(default_factory=list)
    active_chat_disposition: ActiveChatDisposition | None = None
    active_chat_bootstrap_applied: bool | None = None
    active_chat_interest_value: float | None = None
    active_chat_decay_half_life_seconds: float | None = None


@dataclass(slots=True, frozen=True)
class ReviewWorkflowExplanation:
    """Stable review result summary decoupled from internal per-stage traces."""

    review_run_id: str
    review_started_at: float
    failed: bool = False
    failure_reason: str | None = None
    scanned_message_count: int = 0
    loaded_message_count: int = 0
    reviewed_batch_count: int = 0
    candidate_message_ids: list[int] = field(default_factory=list)
    reply_target_message_ids: list[int] = field(default_factory=list)
    replied: bool = False
    reply_message_id: int | None = None
    reply_message_ids: list[int] = field(default_factory=list)
    overflow_summary_count: int = 0
    overflow_summary_message_count: int = 0
    consumed_range_ids: list[int] = field(default_factory=list)
    consumed_message_count: int = 0
    active_chat_initial_interest: float | None = None
    active_chat_decay_half_life_seconds: float | None = None
    active_chat_disposition: ActiveChatDisposition | None = None
    active_chat_bootstrap_applied: bool = False
    active_chat_reason: str = ""
    stages: list[ReviewStageExplanation] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ReviewScanResult:
    """Stage 1 result: candidate message ids selected for closer reply review."""

    candidate_message_ids: list[int] = field(default_factory=list)
    scan_reason: str = "review_workflow_skeleton_no_llm"
    scanned_message_count: int = 0
    loaded_message_count: int = 0
    stage_input_count: int = 0
    batch_count: int = 0
    compressed_ranges: list[UnreadRangeSummaryRecord] = field(default_factory=list)
    ignored_ranges: list[UnreadRangeIgnoreRecord] = field(default_factory=list)
    consumption_deferred: bool = False


@dataclass(slots=True, frozen=True)
class ReplyDecisionResult:
    """Stage 2 result: reply decision, intentionally independent from active chat."""

    replied: bool = False
    reply_message_id: int | None = None
    reply_message_ids: list[int] = field(default_factory=list)
    target_message_ids: list[int] = field(default_factory=list)
    reply_reason: str = "review_reply_skeleton_no_llm"
    loaded_message_count: int = 0
    stage_input_count: int = 0
    consumption_deferred: bool = False

@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapResult:
    """Stage 3 result: delayed active chat disposition after review/reply finishes."""

    disposition: ActiveChatDisposition | None = None
    reason: str = "review_bootstrap_skeleton_low_interest"
    bootstrap_applied: bool = False
    active_chat_interest_value: float | None = None
    active_chat_decay_half_life_seconds: float | None = None
    tail_history_start_at: float | None = None
    tail_history_end_at: float | None = None
    tail_history_message_count: int = 0
    stage_input_built: bool = False

@dataclass(slots=True, frozen=True)
class ReviewWorkflowResult:
    """Whole review workflow result across scan, reply, and active chat bootstrap."""

    review_run_id: str
    scan: ReviewScanResult
    reply: ReplyDecisionResult
    bootstrap: ActiveChatBootstrapResult
    review_started_at: float
    completion: ReviewCompletionDecision | None = None
    consumed_ranges: list[ConsumedUnreadRange] = field(default_factory=list)
    consumed_range_ids: list[int] = field(default_factory=list)
    stage_traces: list[ReviewStageTrace] = field(default_factory=list)
    failed: bool = False
    failure_reason: str | None = None


def build_review_workflow_explanation(
    result: ReviewWorkflowResult,
) -> ReviewWorkflowExplanation:
    """Build a concise, stable explanation from a detailed review workflow result."""

    return ReviewWorkflowExplanation(
        review_run_id=result.review_run_id,
        review_started_at=result.review_started_at,
        failed=result.failed,
        failure_reason=result.failure_reason,
        scanned_message_count=result.scan.scanned_message_count,
        loaded_message_count=result.scan.loaded_message_count,
        reviewed_batch_count=result.scan.batch_count,
        candidate_message_ids=list(result.scan.candidate_message_ids),
        reply_target_message_ids=list(result.reply.target_message_ids),
        replied=result.reply.replied,
        reply_message_id=result.reply.reply_message_id,
        reply_message_ids=list(result.reply.reply_message_ids),
        overflow_summary_count=len(result.scan.compressed_ranges),
        overflow_summary_message_count=sum(
            record.message_count for record in result.scan.compressed_ranges
        ),
        consumed_range_ids=list(result.consumed_range_ids),
        consumed_message_count=sum(record.message_count for record in result.consumed_ranges),
        active_chat_initial_interest=result.bootstrap.active_chat_interest_value,
        active_chat_decay_half_life_seconds=result.bootstrap.active_chat_decay_half_life_seconds,
        active_chat_disposition=result.bootstrap.disposition,
        active_chat_bootstrap_applied=result.bootstrap.bootstrap_applied,
        active_chat_reason=result.bootstrap.reason,
        stages=[_stage_explanation(trace) for trace in result.stage_traces],
    )


def _stage_explanation(trace: ReviewStageTrace) -> ReviewStageExplanation:
    return ReviewStageExplanation(
        purpose=trace.purpose,
        input_message_count=len(trace.message_ids),
        reason=trace.reason,
        candidate_message_ids=list(trace.candidate_message_ids),
        target_message_ids=list(trace.target_message_ids),
        replied=trace.replied,
        reply_message_id=trace.reply_message_id,
        reply_message_ids=list(trace.reply_message_ids),
        active_chat_disposition=trace.active_chat_disposition,
        active_chat_bootstrap_applied=trace.active_chat_bootstrap_applied,
        active_chat_interest_value=trace.active_chat_interest_value,
        active_chat_decay_half_life_seconds=trace.active_chat_decay_half_life_seconds,
    )
