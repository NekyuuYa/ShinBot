"""Data models for Agent-internal scheduling."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class AgentState(StrEnum):
    """Coarse Agent scheduler state for one session."""

    IDLE = "idle"
    REVIEW = "review"
    ACTIVE_REPLY = "active_reply"
    ACTIVE_CHAT = "active_chat"


class HighPriorityEventKind(StrEnum):
    """High-attention event kinds detected at message ingress time."""

    MENTION = "mention"
    REPLY_TO_BOT = "reply_to_bot"
    REPEATED_MENTION = "repeated_mention"
    POKE = "poke"


class MentionSensitivity(StrEnum):
    """How sensitive Agent should be to mentions during the current review interval."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


@dataclass(slots=True, frozen=True)
class ActiveReplyThreshold:
    """Wake threshold for mention bursts during one review interval."""

    at_count: int = 1
    window_seconds: float = 60.0


@dataclass(slots=True, frozen=True)
class ReviewPlan:
    """Scheduler-owned plan for the next review opportunity."""

    session_id: str
    next_review_at: float
    reason: str
    mention_sensitivity: MentionSensitivity = MentionSensitivity.NORMAL
    active_reply_threshold: ActiveReplyThreshold = field(default_factory=ActiveReplyThreshold)
    updated_at: float = 0.0


@dataclass(slots=True, frozen=True)
class ActiveChatState:
    """Scheduler-owned interest state for one active chat session."""

    session_id: str
    interest_value: float
    decay_half_life_seconds: float
    entered_at: float
    updated_at: float


@dataclass(slots=True, frozen=True)
class UnreadMessage:
    """A message known to Agent but not yet consumed by review/chat logic."""

    session_id: str
    message_log_id: int
    sender_id: str
    created_at: float


@dataclass(slots=True, frozen=True)
class HighPriorityEvent:
    """A high-priority notification queued for active reply handling."""

    session_id: str
    message_log_id: int
    sender_id: str
    kind: HighPriorityEventKind
    created_at: float
    reason: str


@dataclass(slots=True)
class AgentScheduleDecision:
    """Observable result of accepting one Agent entry signal."""

    accepted: bool
    state: AgentState
    unread_message: UnreadMessage | None = None
    active_chat_state: ActiveChatState | None = None
    high_priority_events: list[HighPriorityEvent] = field(default_factory=list)
    active_reply_started: bool = False
    active_chat_observed: bool = False
    skipped_reason: str | None = None


@dataclass(slots=True)
class ReviewDueDecision:
    """Result of preparing a due review transition."""

    session_id: str
    state: AgentState
    review_plan: ReviewPlan | None = None
    high_priority_events: list[HighPriorityEvent] = field(default_factory=list)
    review_started: bool = False
    review_workflow_started: bool = False
    active_reply_pending: bool = False
    skipped_reason: str | None = None


@dataclass(slots=True)
class ActiveReplyCompletionDecision:
    """Result of completing active reply and deciding the next scheduler state."""

    session_id: str
    state: AgentState
    review_plan: ReviewPlan | None = None
    handled_high_priority_events: list[HighPriorityEvent] = field(default_factory=list)
    review_started: bool = False
    review_workflow_started: bool = False
    returned_to_idle: bool = False
    skipped_reason: str | None = None


@dataclass(slots=True)
class ReviewCompletionDecision:
    """Result of completing review and deciding whether to enter active chat."""

    session_id: str
    state: AgentState
    active_chat_state: ActiveChatState | None = None
    next_review_plan: ReviewPlan | None = None
    active_chat_started: bool = False
    returned_to_idle: bool = False
    skipped_reason: str | None = None


@dataclass(slots=True)
class ActiveChatTickDecision:
    """Result of updating active chat interest and deciding whether to return idle."""

    session_id: str
    state: AgentState
    active_chat_state: ActiveChatState | None = None
    next_review_plan: ReviewPlan | None = None
    returned_to_idle: bool = False
    skipped_reason: str | None = None
