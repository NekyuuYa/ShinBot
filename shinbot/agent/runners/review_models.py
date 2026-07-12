"""Runner-local output contracts for review stage runners."""

from __future__ import annotations

from dataclasses import dataclass, field

from shinbot.agent.runtime.session_actor.external_actions import ExternalActionIntent
from shinbot.agent.scheduler.models import ActiveChatDisposition, MentionSensitivity


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
class ReviewBlockDigestStageOutput:
    """Output from one review block digest runner invocation."""

    summary: str = ""
    reason: str = "noop_review_block_digest"
    block_index: int | None = None
    msg_log_start: int | None = None
    msg_log_end: int | None = None
    message_count: int = 0


@dataclass(slots=True, frozen=True)
class ReplyDecisionStageOutput:
    """Output from one reply_decision stage runner invocation."""

    replied: bool = False
    reply_message_id: int | None = None
    reply_message_ids: list[int] = field(default_factory=list)
    target_message_ids: list[int] = field(default_factory=list)
    reason: str = "noop_reply_decision"
    consumption_deferred: bool = False
    external_action_intents: tuple[ExternalActionIntent, ...] = ()


@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapStageOutput:
    """Output from the active_chat_bootstrap stage runner."""

    disposition: ActiveChatDisposition | None = None
    reason: str = "noop_active_chat_bootstrap"


@dataclass(slots=True, frozen=True)
class IdleReviewPlanningStageOutput:
    """Output from the active_chat -> idle review planning stage."""

    next_review_after_seconds: float | None = None
    reason: str = "noop_idle_review_planning"
    mention_sensitivity: MentionSensitivity | None = None
    mention_wake_count: int | None = None
    mention_wake_window_seconds: float | None = None


__all__ = [
    "ActiveChatBootstrapStageOutput",
    "IdleReviewPlanningStageOutput",
    "OverflowCompressionStageOutput",
    "ReviewBlockDigestStageOutput",
    "ReplyDecisionStageOutput",
    "ReviewScanStageOutput",
]
