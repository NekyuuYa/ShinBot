"""Inbox storage boundary for AgentScheduler."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Protocol

from shinbot.agent.scheduler.models import HighPriorityEvent, UnreadMessage


class AgentInbox(Protocol):
    """Storage surface for unread messages and high-priority events."""

    def add_unread(self, message: UnreadMessage) -> None:
        """Record one unread message."""

    def list_unread(self, session_id: str) -> list[UnreadMessage]:
        """List unread messages for one session."""

    def add_high_priority_events(self, events: list[HighPriorityEvent]) -> None:
        """Record high-priority events."""

    def list_high_priority_events(self, session_id: str) -> list[HighPriorityEvent]:
        """List high-priority events for one session."""

    def mark_high_priority_events_handled(self, session_id: str) -> list[HighPriorityEvent]:
        """Mark pending high-priority events for one session handled."""

    def record_mention(self, session_id: str, timestamp: float) -> None:
        """Record a mention timestamp for wake-threshold checks."""

    def count_recent_mentions(self, session_id: str, *, now: float, window_seconds: float) -> int:
        """Count mentions in the configured wake window."""


class InMemoryAgentInbox:
    """In-memory Agent inbox used before scheduler persistence exists."""

    def __init__(self) -> None:
        self._unread: dict[str, list[UnreadMessage]] = defaultdict(list)
        self._high_priority: dict[str, list[HighPriorityEvent]] = defaultdict(list)
        self._recent_mentions: dict[str, deque[float]] = defaultdict(deque)

    def add_unread(self, message: UnreadMessage) -> None:
        self._unread[message.session_id].append(message)

    def list_unread(self, session_id: str) -> list[UnreadMessage]:
        return list(self._unread.get(session_id, []))

    def add_high_priority_events(self, events: list[HighPriorityEvent]) -> None:
        for event in events:
            self._high_priority[event.session_id].append(event)

    def list_high_priority_events(self, session_id: str) -> list[HighPriorityEvent]:
        return list(self._high_priority.get(session_id, []))

    def mark_high_priority_events_handled(self, session_id: str) -> list[HighPriorityEvent]:
        events = self.list_high_priority_events(session_id)
        self._high_priority[session_id].clear()
        return events

    def record_mention(self, session_id: str, timestamp: float) -> None:
        self._recent_mentions[session_id].append(timestamp)

    def count_recent_mentions(self, session_id: str, *, now: float, window_seconds: float) -> int:
        recent_mentions = self._recent_mentions[session_id]
        while recent_mentions and now - recent_mentions[0] > window_seconds:
            recent_mentions.popleft()
        return len(recent_mentions)


__all__ = ["AgentInbox", "InMemoryAgentInbox"]
