"""Runner-local output contracts for review stage runners."""

from __future__ import annotations

from dataclasses import dataclass, field

from shinbot.agent.scheduler.models import ActiveChatDisposition


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
class ReplyDecisionStageOutput:
    """Output from one reply_decision stage runner invocation."""

    replied: bool = False
    reply_message_id: int | None = None
    reply_message_ids: list[int] = field(default_factory=list)
    target_message_ids: list[int] = field(default_factory=list)
    reason: str = "noop_reply_decision"


@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapStageOutput:
    """Output from the active_chat_bootstrap stage runner."""

    disposition: ActiveChatDisposition | None = None
    reason: str = "noop_active_chat_bootstrap"


__all__ = [
    "ActiveChatBootstrapStageOutput",
    "OverflowCompressionStageOutput",
    "ReplyDecisionStageOutput",
    "ReviewScanStageOutput",
]
