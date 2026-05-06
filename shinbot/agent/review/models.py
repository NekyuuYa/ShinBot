"""Result models for the Agent review workflow."""

from __future__ import annotations

from dataclasses import dataclass, field

from shinbot.agent.scheduler.models import ReviewCompletionDecision


@dataclass(slots=True, frozen=True)
class ReviewWorkflowConfig:
    """Tunable limits for the review workflow skeleton."""

    review_scan_batch_size: int = 500
    overflow_threshold_messages: int = 3000
    overflow_compression_batch_size: int = 500
    reply_context_before_messages: int = 20
    reply_context_after_messages: int = 20
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
class ConsumedUnreadRange:
    """Unread range interval consumed by one review run."""

    range_id: int | None
    session_id: str
    start_msg_log_id: int
    end_msg_log_id: int
    message_count: int
    full_range: bool = False


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
    initial_interest: float | None = None
    decay_half_life_seconds: float | None = None


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


@dataclass(slots=True, frozen=True)
class ReviewScanStageOutput:
    """Output from one review_scan stage runner invocation."""

    candidate_message_ids: list[int] = field(default_factory=list)
    reason: str = "noop_review_scan"


@dataclass(slots=True, frozen=True)
class OverflowCompressionStageOutput:
    """Output from one overflow compression runner invocation."""

    summary: str = ""
    candidate_message_ids: list[int] = field(default_factory=list)
    reason: str = "noop_overflow_compression"


@dataclass(slots=True, frozen=True)
class ReplyDecisionResult:
    """Stage 2 result: reply decision, intentionally independent from active chat."""

    replied: bool = False
    reply_message_id: int | None = None
    target_message_ids: list[int] = field(default_factory=list)
    reply_reason: str = "review_reply_skeleton_no_llm"
    loaded_message_count: int = 0
    stage_input_count: int = 0


@dataclass(slots=True, frozen=True)
class ReplyDecisionStageOutput:
    """Output from one reply_decision stage runner invocation."""

    replied: bool = False
    reply_message_id: int | None = None
    target_message_ids: list[int] = field(default_factory=list)
    reason: str = "noop_reply_decision"


@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapResult:
    """Stage 3 result: initial active chat state after review/reply finishes."""

    initial_interest: float
    decay_half_life_seconds: float | None = None
    reason: str = "review_bootstrap_skeleton_low_interest"
    tail_history_start_at: float | None = None
    tail_history_end_at: float | None = None
    tail_history_message_count: int = 0
    stage_input_built: bool = False


@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapStageOutput:
    """Output from the active_chat_bootstrap stage runner."""

    initial_interest: float
    decay_half_life_seconds: float | None = None
    reason: str = "noop_active_chat_bootstrap"


@dataclass(slots=True, frozen=True)
class ReviewWorkflowResult:
    """Whole review workflow result across scan, reply, and active chat bootstrap."""

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
