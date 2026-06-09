"""Workflow-local contracts for active chat fast mode."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from shinbot.agent.scheduler.models import ActiveChatState


class ActiveChatActionKind(StrEnum):
    """Semantic terminal action produced by one active chat round."""

    WATCH = "watch"
    NO_REPLY = "no_reply"
    SEND_REACTION = "send_reaction"
    SEND_POKE = "send_poke"
    SEND_REPLY = "send_reply"
    REQUEST_THINK_MODE = "request_think_mode"
    EXIT_ACTIVE = "exit_active"
    RETRY_FAILED = "retry_failed"


class ActiveChatMode(StrEnum):
    """Runtime mode for an active chat workflow batch."""

    FAST = "fast"
    THINK = "think"


class ActiveChatReplyIntensity(StrEnum):
    """Reply intensity used by workflow results."""

    LIGHT = "light"
    ENGAGED = "engaged"


class ActiveChatNoReplyIntensity(StrEnum):
    """No-reply intensity used by workflow results."""

    NORMAL = "normal"
    STRONG = "strong"


@dataclass(slots=True, frozen=True)
class ActiveChatMessageSignal:
    """One message included in an active chat workflow batch."""

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
    trace_id: str = ""


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
    """Result returned by an active chat workflow round."""

    success: bool = True
    reason: str = ""
    action: ActiveChatActionKind = ActiveChatActionKind.WATCH
    reply_intensity: ActiveChatReplyIntensity = ActiveChatReplyIntensity.LIGHT
    no_reply_intensity: ActiveChatNoReplyIntensity = ActiveChatNoReplyIntensity.NORMAL
    consumed_message_log_ids: list[int] = field(default_factory=list)
    restored_messages: list[ActiveChatMessageSignal] = field(default_factory=list)
    conversation_messages_delta: list[dict[str, Any]] = field(default_factory=list)


__all__ = [
    "ActiveChatActionKind",
    "ActiveChatBatch",
    "ActiveChatMessageSignal",
    "ActiveChatMode",
    "ActiveChatNoReplyIntensity",
    "ActiveChatReplyIntensity",
    "ActiveChatRoundResult",
]
