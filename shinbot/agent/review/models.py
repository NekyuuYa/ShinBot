"""Result models for the Agent review workflow."""

from __future__ import annotations

from dataclasses import dataclass, field

from shinbot.agent.scheduler.models import ReviewCompletionDecision


@dataclass(slots=True, frozen=True)
class ReviewWorkflowConfig:
    """Tunable limits for the review workflow skeleton."""

    review_scan_batch_size: int = 500
    overflow_threshold_messages: int = 3000
    tail_history_before_seconds: float = 180.0
    tail_history_limit: int = 500
    fallback_active_chat_interest: float = 0.05


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
class ReviewScanResult:
    """Stage 1 result: candidate message ids selected for closer reply review."""

    candidate_message_ids: list[int] = field(default_factory=list)
    scan_reason: str = "review_workflow_skeleton_no_llm"
    scanned_message_count: int = 0
    loaded_message_count: int = 0
    batch_count: int = 0
    compressed_ranges: list[UnreadRangeSummaryRecord] = field(default_factory=list)
    ignored_ranges: list[UnreadRangeIgnoreRecord] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ReplyDecisionResult:
    """Stage 2 result: reply decision, intentionally independent from active chat."""

    replied: bool = False
    reply_message_id: int | None = None
    target_message_ids: list[int] = field(default_factory=list)
    reply_reason: str = "review_reply_skeleton_no_llm"


@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapResult:
    """Stage 3 result: initial active chat state after review/reply finishes."""

    initial_interest: float
    decay_half_life_seconds: float | None = None
    reason: str = "review_bootstrap_skeleton_low_interest"
    tail_history_start_at: float | None = None
    tail_history_end_at: float | None = None
    tail_history_message_count: int = 0


@dataclass(slots=True, frozen=True)
class ReviewWorkflowResult:
    """Whole review workflow result across scan, reply, and active chat bootstrap."""

    scan: ReviewScanResult
    reply: ReplyDecisionResult
    bootstrap: ActiveChatBootstrapResult
    review_started_at: float
    completion: ReviewCompletionDecision | None = None
    consumed_range_ids: list[int] = field(default_factory=list)
    failed: bool = False
    failure_reason: str | None = None
