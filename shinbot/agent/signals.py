"""Unified Agent signal model."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from shinbot.agent.scheduler.models import AgentState


class AgentSignalKind(StrEnum):
    """Kinds of unified signals that can enter the Agent state machine."""

    MESSAGE = "message"
    REVIEW_DUE = "review_due"
    ACTIVE_CHAT_TICK = "active_chat_tick"
    ACTIVE_CHAT_BOOTSTRAP = "active_chat_bootstrap"


class AgentSignalSource(StrEnum):
    """Where a unified signal originated."""

    MESSAGE_INGRESS = "message_ingress"
    TIMER = "timer"
    MANUAL = "manual"


@dataclass(slots=True, frozen=True)
class AgentMessageSignal:
    """Message-driven payload for the unified Agent entry."""

    message_log_id: int | None
    sender_id: str
    instance_id: str
    platform: str
    self_id: str
    is_private: bool
    is_mentioned: bool
    is_reply_to_bot: bool
    is_mention_to_other: bool = False
    is_poke_to_bot: bool = False
    is_poke_to_other: bool = False
    already_handled: bool = False
    is_stopped: bool = False


@dataclass(slots=True, frozen=True)
class AgentTimerSignal:
    """Timer-driven payload for the unified Agent entry."""

    trigger: str
    due_at: float | None = None
    expected_state: AgentState | None = None
    plan_id: str = ""


@dataclass(slots=True, frozen=True)
class AgentSignal:
    """Unified signal accepted by the Agent runtime."""

    signal_id: str
    kind: AgentSignalKind
    source: AgentSignalSource
    session_id: str
    occurred_at: float
    bot_id: str = ""
    bot_binding_id: str = ""
    bot_session_id: str = ""
    message: AgentMessageSignal | None = None
    timer: AgentTimerSignal | None = None
    meta: dict[str, Any] = field(default_factory=dict)


__all__ = [
    "AgentMessageSignal",
    "AgentSignal",
    "AgentSignalKind",
    "AgentSignalSource",
    "AgentTimerSignal",
]
