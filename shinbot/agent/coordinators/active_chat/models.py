"""Models for the active chat workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from shinbot.agent.scheduler.models import ActiveChatState


class ActiveChatMode(StrEnum):
    """Runtime mode for an active chat session."""

    FAST = "fast"
    THINK = "think"


class ActiveChatActionKind(StrEnum):
    """Semantic terminal action produced by one active chat round."""

    WATCH = "watch"
    NO_REPLY = "no_reply"
    SEND_POKE = "send_poke"
    SEND_REPLY = "send_reply"
    REQUEST_THINK_MODE = "request_think_mode"
    EXIT_ACTIVE = "exit_active"
    RETRY_FAILED = "retry_failed"


class ActiveChatReplyIntensity(StrEnum):
    """Reply intensity used for interest adjustment."""

    LIGHT = "light"
    ENGAGED = "engaged"


class ActiveChatNoReplyIntensity(StrEnum):
    """No-reply intensity used for interest adjustment."""

    NORMAL = "normal"
    STRONG = "strong"


@dataclass(slots=True, frozen=True)
class ActiveChatMessageSignal:
    """One active chat message notification from AgentScheduler."""

    session_id: str
    message_log_id: int
    sender_id: str
    response_profile: str
    is_mentioned: bool = False
    is_reply_to_bot: bool = False
    is_mention_to_other: bool = False
    is_poke_to_bot: bool = False
    is_poke_to_other: bool = False
    self_platform_id: str = ""
    active_chat_state: ActiveChatState | None = None
    created_at: float = 0.0


@dataclass(slots=True)
class ActiveChatAttentionState:
    """In-memory attention state for one active chat session."""

    session_id: str
    accumulated: float = 0.0
    last_update_at: float = 0.0
    pending_buffer: list[ActiveChatMessageSignal] = field(default_factory=list)
    last_sender_id: str = ""
    mode: ActiveChatMode = ActiveChatMode.FAST
    active_epoch: int = 0
    review_result_summary: Any = None
    conversation_summary: str = ""
    conversation_messages: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ActiveChatStartResult:
    """Observable result of starting one active chat session."""

    accepted: bool
    session_id: str
    active_epoch: int = 0
    skipped_reason: str | None = None


@dataclass(slots=True, frozen=True)
class ActiveChatBatch:
    """A flushed active chat batch ready for a workflow round."""

    session_id: str
    messages: list[ActiveChatMessageSignal]
    active_chat_state: ActiveChatState
    response_profile: str
    mode: ActiveChatMode = ActiveChatMode.FAST
    review_result_summary: Any = None
    conversation_summary: str = ""
    conversation_messages: list[dict[str, Any]] = field(default_factory=list)

    @property
    def message_log_ids(self) -> list[int]:
        """Return message log ids included in this batch."""
        return [message.message_log_id for message in self.messages]


@dataclass(slots=True, frozen=True)
class ActiveChatRoundResult:
    """Result returned by an active chat round handler."""

    success: bool = True
    reason: str = ""
    action: ActiveChatActionKind = ActiveChatActionKind.WATCH
    reply_intensity: ActiveChatReplyIntensity = ActiveChatReplyIntensity.LIGHT
    no_reply_intensity: ActiveChatNoReplyIntensity = ActiveChatNoReplyIntensity.NORMAL
    consumed_message_log_ids: list[int] = field(default_factory=list)
    restored_messages: list[ActiveChatMessageSignal] = field(default_factory=list)
    conversation_messages_delta: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ActiveChatNotifyResult:
    """Observable result of accepting one active chat message notification."""

    accepted: bool
    session_id: str
    message_log_id: int | None = None
    accumulated: float = 0.0
    threshold: float = 0.0
    triggered: bool = False
    timer_started: bool = False
    timer_reset: bool = False
    skipped_reason: str | None = None


@dataclass(slots=True, frozen=True)
class ActiveChatSummarySnapshot:
    """Read-only summary snapshot for active chat persistence."""

    session_id: str
    active_epoch: int
    conversation_summary: str
    conversation_message_count: int
    conversation_messages: list[dict[str, Any]] = field(default_factory=list)
    message_log_ids: list[int] = field(default_factory=list)
    range_source: str = "last_batch"

    @property
    def msg_log_start(self) -> int | None:
        """Return the covered message-log lower bound, if known."""
        return min(self.message_log_ids) if self.message_log_ids else None

    @property
    def msg_log_end(self) -> int | None:
        """Return the covered message-log upper bound, if known."""
        return max(self.message_log_ids) if self.message_log_ids else None

    @property
    def msg_count(self) -> int:
        """Return the number of message log ids covered by this snapshot."""
        return len(self.message_log_ids)


__all__ = [
    "ActiveChatActionKind",
    "ActiveChatAttentionState",
    "ActiveChatBatch",
    "ActiveChatMessageSignal",
    "ActiveChatMode",
    "ActiveChatNotifyResult",
    "ActiveChatNoReplyIntensity",
    "ActiveChatReplyIntensity",
    "ActiveChatRoundResult",
    "ActiveChatStartResult",
    "ActiveChatSummarySnapshot",
]
