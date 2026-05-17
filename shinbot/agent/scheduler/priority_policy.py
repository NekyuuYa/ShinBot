"""High-priority event detection and wake policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from shinbot.agent.scheduler.inbox import AgentInbox
from shinbot.agent.scheduler.models import HighPriorityEvent, HighPriorityEventKind

if TYPE_CHECKING:
    from shinbot.core.dispatch.dispatchers import AgentEntrySignal


@dataclass(slots=True, frozen=True)
class PriorityPolicyConfig:
    """Thresholds for high-priority active-reply wake decisions."""

    mention_wake_count: int = 1
    mention_wake_window_seconds: float = 60.0


@dataclass(slots=True)
class PriorityPolicyDecision:
    """Result of evaluating high-priority signals for one message."""

    events: list[HighPriorityEvent] = field(default_factory=list)
    should_start_active_reply: bool = False


class PriorityPolicy(Protocol):
    """Detect high-priority events and decide whether they wake active reply."""

    def evaluate(
        self,
        signal: AgentEntrySignal,
        *,
        now: float,
        inbox: AgentInbox,
    ) -> PriorityPolicyDecision:
        """Evaluate one signal against current high-priority policy."""


class DefaultPriorityPolicy:
    """Default high-priority policy for mentions and replies to the bot."""

    def __init__(self, config: PriorityPolicyConfig | None = None) -> None:
        self._config = config or PriorityPolicyConfig()

    def evaluate(
        self,
        signal: AgentEntrySignal,
        *,
        now: float,
        inbox: AgentInbox,
    ) -> PriorityPolicyDecision:
        events = self._detect_events(signal, now)
        if not events:
            return PriorityPolicyDecision(events=[], should_start_active_reply=False)

        should_wake = self._should_wake(signal, now=now, inbox=inbox)
        return PriorityPolicyDecision(events=events, should_start_active_reply=should_wake)

    def _detect_events(
        self,
        signal: AgentEntrySignal,
        now: float,
    ) -> list[HighPriorityEvent]:
        events: list[HighPriorityEvent] = []
        if signal.is_mentioned:
            events.append(
                HighPriorityEvent(
                    session_id=signal.session_id,
                    message_log_id=signal.message_log_id or 0,
                    sender_id=signal.sender_id,
                    kind=HighPriorityEventKind.MENTION,
                    created_at=now,
                    reason="message_mentions_self",
                )
            )
        if signal.is_reply_to_bot:
            events.append(
                HighPriorityEvent(
                    session_id=signal.session_id,
                    message_log_id=signal.message_log_id or 0,
                    sender_id=signal.sender_id,
                    kind=HighPriorityEventKind.REPLY_TO_BOT,
                    created_at=now,
                    reason="message_replies_to_self",
                )
            )
        if signal.is_poke_to_bot:
            events.append(
                HighPriorityEvent(
                    session_id=signal.session_id,
                    message_log_id=signal.message_log_id or 0,
                    sender_id=signal.sender_id,
                    kind=HighPriorityEventKind.POKE,
                    created_at=now,
                    reason="message_pokes_self",
                )
            )
        return events

    def _should_wake(
        self,
        signal: AgentEntrySignal,
        *,
        now: float,
        inbox: AgentInbox,
    ) -> bool:
        if signal.is_reply_to_bot:
            return True
        if signal.is_poke_to_bot:
            return True
        if not signal.is_mentioned:
            return False

        inbox.record_mention(signal.session_id, now)
        recent_count = inbox.count_recent_mentions(
            signal.session_id,
            now=now,
            window_seconds=self._config.mention_wake_window_seconds,
        )
        return recent_count >= self._config.mention_wake_count


__all__ = [
    "DefaultPriorityPolicy",
    "PriorityPolicy",
    "PriorityPolicyConfig",
    "PriorityPolicyDecision",
]
