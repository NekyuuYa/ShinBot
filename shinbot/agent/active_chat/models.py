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

    @property
    def message_log_ids(self) -> list[int]:
        """Return message log ids included in this batch."""
        return [message.message_log_id for message in self.messages]


@dataclass(slots=True, frozen=True)
class ActiveChatRoundResult:
    """Result returned by an active chat round handler."""

    success: bool = True
    reason: str = ""


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


__all__ = [
    "ActiveChatAttentionState",
    "ActiveChatBatch",
    "ActiveChatMessageSignal",
    "ActiveChatMode",
    "ActiveChatNotifyResult",
    "ActiveChatRoundResult",
    "ActiveChatStartResult",
]
