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


class ActiveReplyResumeKind(StrEnum):
    """Why ACTIVE_REPLY should hand off into a follow-up state."""

    RESUME_INTERRUPTED_REVIEW = "resume_interrupted_review"
    START_DEFERRED_REVIEW = "start_deferred_review"


class SchedulerTransitionTrigger(StrEnum):
    """Named transition triggers for the scheduler state machine."""

    MESSAGE_PRIORITY_WAKE = "message_priority_wake"
    REVIEW_DUE = "review_due"
    DEFERRED_REVIEW_AFTER_ACTIVE_REPLY = "deferred_review_after_active_reply"
    ACTIVE_REPLY_RESUME_INTERRUPTED_REVIEW = "active_reply_resume_interrupted_review"
    ACTIVE_REPLY_START_DEFERRED_REVIEW = "active_reply_start_deferred_review"
    ACTIVE_REPLY_RETURN_IDLE = "active_reply_return_idle"
    REVIEW_COMPLETE_ENTER_ACTIVE_CHAT = "review_complete_enter_active_chat"
    REVIEW_COMPLETE_RETURN_IDLE = "review_complete_return_idle"
    ACTIVE_CHAT_INTEREST_ADJUSTMENT_EXIT = "active_chat_interest_adjustment_exit"
    ACTIVE_CHAT_DECAY_EXIT = "active_chat_decay_exit"
    ACTIVE_CHAT_BOOTSTRAP_EXIT = "active_chat_bootstrap_exit"
    TRANSIENT_STATE_RECOVERED = "transient_state_recovered"


class SchedulerEventKind(StrEnum):
    """Normalized scheduler event kinds derived from Agent signals."""

    MESSAGE = "message"
    REVIEW_DUE = "review_due"
    ACTIVE_CHAT_TICK = "active_chat_tick"
    ACTIVE_CHAT_BOOTSTRAP = "active_chat_bootstrap"


class IdleReviewPlanningTrigger(StrEnum):
    """State transition that requested one external idle-review plan."""

    ACTIVE_CHAT_TICK = "active_chat_tick"
    ACTIVE_CHAT_BOOTSTRAP = "active_chat_bootstrap"
    ACTIVE_CHAT_INTEREST_ADJUSTMENT = "active_chat_interest_adjustment"


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


class ActiveChatDisposition(StrEnum):
    """Semantic active-chat bootstrap disposition chosen by review stage 3."""

    EXIT_SOON = "exit_soon"
    WATCH = "watch"
    CASUAL = "casual"
    ENGAGED = "engaged"
    FOCUSED = "focused"


@dataclass(slots=True, frozen=True)
class ActiveReplyThreshold:
    """Wake threshold for mention bursts during one review interval."""

    at_count: int = 1
    window_seconds: float = 60.0


@dataclass(slots=True, frozen=True)
class SchedulerEvent:
    """Explicit scheduler event normalized from an inbound Agent signal."""

    kind: SchedulerEventKind
    signal: object


@dataclass(slots=True, frozen=True)
class ReviewPlan:
    """Scheduler-owned plan for the next review opportunity."""

    session_id: str
    next_review_at: float
    reason: str
    mention_sensitivity: MentionSensitivity = MentionSensitivity.NORMAL
    active_reply_threshold: ActiveReplyThreshold = field(default_factory=ActiveReplyThreshold)
    updated_at: float = 0.0


def review_plan_fence_matches(
    current: ReviewPlan | None,
    expected: ReviewPlan | None,
) -> bool:
    """Return whether two plans represent the same scheduling decision.

    ``updated_at`` is persistence bookkeeping, not part of the decision. The
    legacy SQLite store currently shares its timestamp column with scheduler
    state writes, so an ``IDLE -> REVIEW`` transition can refresh that value
    without replacing the review plan. Fences must therefore compare the
    fields that affect actual scheduling behavior instead of dataclass equality.
    """

    if current is None or expected is None:
        return current is expected
    return (
        current.session_id == expected.session_id
        and current.next_review_at == expected.next_review_at
        and current.reason == expected.reason
        and current.mention_sensitivity == expected.mention_sensitivity
        and current.active_reply_threshold == expected.active_reply_threshold
    )


@dataclass(slots=True, frozen=True)
class ActiveChatState:
    """Scheduler-owned interest state for one active chat session."""

    session_id: str
    interest_value: float
    decay_half_life_seconds: float
    entered_at: float
    updated_at: float
    tick_count: int = 0
    active_epoch: int = 0
    bootstrap_applied: bool = False
    bootstrap_disposition: ActiveChatDisposition | None = None


@dataclass(slots=True, frozen=True)
class IdleReviewPlanningRequest:
    """Frozen active-chat exit intent for one external review-plan decision.

    The scheduler creates this value while its caller serializes a session,
    then the model planner runs outside that critical section.  Applying the
    result requires ``expected_active_chat_state`` to still match exactly, so
    a message, timer tick, bootstrap update, or replacement active-chat epoch
    cannot be overwritten by a stale model result.
    """

    session_id: str
    trigger: IdleReviewPlanningTrigger
    signal_id: str
    checked_at: float
    expected_active_chat_state: ActiveChatState
    planning_active_chat_state: ActiveChatState
    expected_review_plan: ReviewPlan | None = None
    bootstrap_disposition: ActiveChatDisposition | None = None
    interest_delta: float = 0.0
    force_exit: bool = False
    interest_reason: str = ""

    @property
    def active_epoch(self) -> int:
        """Return the epoch fenced by this planning request."""

        return self.expected_active_chat_state.active_epoch


@dataclass(slots=True, frozen=True)
class ActiveReplyResume:
    """Resume target remembered while ACTIVE_REPLY temporarily interrupts work."""

    session_id: str
    kind: ActiveReplyResumeKind
    resume_state: AgentState
    review_plan: ReviewPlan | None = None
    updated_at: float = 0.0


@dataclass(slots=True, frozen=True)
class UnreadMessage:
    """A message known to Agent but not yet consumed by review/chat logic."""

    session_id: str
    message_log_id: int
    sender_id: str
    created_at: float
    response_profile: str = ""
    is_mentioned: bool = False
    is_reply_to_bot: bool = False
    is_mention_to_other: bool = False
    is_poke_to_bot: bool = False
    is_poke_to_other: bool = False
    self_platform_id: str = ""
    trace_id: str = ""


@dataclass(slots=True, frozen=True)
class UnreadRange:
    """A contiguous unread timeline range known to Agent."""

    id: int | None
    session_id: str
    start_msg_log_id: int
    end_msg_log_id: int
    start_at: float
    end_at: float
    message_count: int
    review_consumed: bool = False
    chat_consumed: bool = False


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
    active_chat_workflow_notified: bool = False
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
    remaining_unread_count: int = 0
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


@dataclass(slots=True)
class ActiveChatBootstrapApplyDecision:
    """Result of applying delayed review stage-3 active-chat bootstrap output."""

    session_id: str
    state: AgentState
    active_chat_state: ActiveChatState | None = None
    next_review_plan: ReviewPlan | None = None
    bootstrap_applied: bool = False
    returned_to_idle: bool = False
    skipped_reason: str | None = None


@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapPreview:
    """Preview of a delayed active-chat bootstrap application without mutation."""

    session_id: str
    state: AgentState
    active_chat_state: ActiveChatState | None = None
    will_return_idle: bool = False
    skipped_reason: str | None = None


@dataclass(slots=True)
class ActiveChatInterestAdjustDecision:
    """Result of applying active chat workflow interest adjustment."""

    session_id: str
    state: AgentState
    active_chat_state: ActiveChatState | None = None
    next_review_plan: ReviewPlan | None = None
    delta: float = 0.0
    force_exit: bool = False
    returned_to_idle: bool = False
    reason: str = ""
    skipped_reason: str | None = None


@dataclass(slots=True, frozen=True)
class ActiveChatInterestAdjustmentPreview:
    """Preview of a workflow-driven active chat interest adjustment."""

    session_id: str
    state: AgentState
    active_chat_state: ActiveChatState | None = None
    delta: float = 0.0
    force_exit: bool = False
    will_return_idle: bool = False
    skipped_reason: str | None = None


@dataclass(slots=True, frozen=True)
class ActiveChatTickPreview:
    """Preview of an active chat decay tick without mutating scheduler state."""

    session_id: str
    state: AgentState
    active_chat_state: ActiveChatState | None = None
    will_return_idle: bool = False
    skipped_reason: str | None = None
