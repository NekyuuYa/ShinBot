"""Inbox storage boundary for AgentScheduler."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Protocol

from shinbot.agent.scheduler.models import HighPriorityEvent, UnreadMessage, UnreadRange


class AgentInbox(Protocol):
    """Storage surface for unread messages and high-priority events."""

    def add_unread(self, message: UnreadMessage) -> None:
        """Record one unread message."""

    def list_unread(self, session_id: str) -> list[UnreadMessage]:
        """List unread messages for one session."""

    def list_unread_ranges(self, session_id: str, *, limit: int = 50) -> list[UnreadRange]:
        """List unread timeline ranges for one session."""

    def count_unread_messages(self, session_id: str) -> int:
        """Count unread messages for one session."""

    def split_review_consumed(
        self,
        *,
        range_id: int,
        consumed_start_msg_log_id: int,
        consumed_end_msg_log_id: int,
    ) -> None:
        """Mark the middle of an unread range consumed, preserving remaining edges."""

    def mark_ranges_review_consumed(self, range_ids: list[int]) -> None:
        """Mark whole unread ranges consumed by review."""

    def mark_active_chat_consumed(
        self,
        *,
        session_id: str,
        message_log_ids: list[int],
    ) -> list[UnreadMessage]:
        """Mark messages consumed by active chat and return the consumed messages."""

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
        self._ranges: dict[str, list[UnreadRange]] = defaultdict(list)
        self._high_priority: dict[str, list[HighPriorityEvent]] = defaultdict(list)
        self._recent_mentions: dict[str, deque[float]] = defaultdict(deque)
        self._active_chat_consumed_ids: dict[str, set[int]] = defaultdict(set)
        self._next_range_id = 1

    def add_unread(self, message: UnreadMessage) -> None:
        """Record an unread message in the inbox.

        Messages that were already consumed during active chat are silently
        ignored.  Duplicate ``message_log_id`` values for the same session are
        also skipped.  The message is appended to the unread list and
        incorporated into the contiguous-range bookkeeping.

        Args:
            message: The unread message to enqueue.
        """
        if message.message_log_id in self._active_chat_consumed_ids[message.session_id]:
            return
        if any(item.message_log_id == message.message_log_id for item in self._unread[message.session_id]):
            return
        self._unread[message.session_id].append(message)
        self._unread[message.session_id].sort(key=lambda item: (item.created_at, item.message_log_id))
        self._append_unread_range(message)

    def list_unread(self, session_id: str) -> list[UnreadMessage]:
        """Return all unread messages for *session_id*, sorted by creation time.

        Args:
            session_id: The conversation session to query.

        Returns:
            A new list of ``UnreadMessage`` instances (empty if none pending).
        """
        return list(self._unread.get(session_id, []))

    def list_unread_ranges(self, session_id: str, *, limit: int = 50) -> list[UnreadRange]:
        """Return contiguous unread timeline ranges for *session_id*.

        Only ranges that have **not** been consumed by review are included.
        Results are sorted chronologically and capped to *limit*.

        Args:
            session_id: The conversation session to query.
            limit: Maximum number of ranges to return (default 50).

        Returns:
            A list of ``UnreadRange`` instances.
        """
        ranges = [
            item
            for item in self._ranges.get(session_id, [])
            if not item.review_consumed
        ]
        ranges.sort(key=lambda item: (item.start_at, item.start_msg_log_id))
        return ranges[:limit]

    def count_unread_messages(self, session_id: str) -> int:
        """Count the total number of unread messages for *session_id*.

        Sums ``message_count`` across all active (non-consumed) ranges.

        Args:
            session_id: The conversation session to query.

        Returns:
            The total count of unread messages.
        """
        return sum(item.message_count for item in self.list_unread_ranges(session_id, limit=10_000))

    def split_review_consumed(
        self,
        *,
        range_id: int,
        consumed_start_msg_log_id: int,
        consumed_end_msg_log_id: int,
    ) -> None:
        """Mark a middle segment of an unread range as consumed by review.

        The original range is replaced with up to two remaining edge ranges
        (before and after the consumed segment).  Messages whose
        ``message_log_id`` falls within the consumed segment are removed from
        the unread list.

        Args:
            range_id: ID of the ``UnreadRange`` to split.
            consumed_start_msg_log_id: First message log ID of the consumed
                segment (inclusive).
            consumed_end_msg_log_id: Last message log ID of the consumed
                segment (inclusive).
        """
        for session_id, ranges in self._ranges.items():
            for index, unread_range in enumerate(ranges):
                if unread_range.id != range_id:
                    continue
                remaining_messages = [
                    message
                    for message in self._unread.get(session_id, [])
                    if unread_range.start_msg_log_id <= message.message_log_id <= unread_range.end_msg_log_id
                    and not consumed_start_msg_log_id <= message.message_log_id <= consumed_end_msg_log_id
                ]
                self._unread[session_id] = [
                    message
                    for message in self._unread.get(session_id, [])
                    if not (
                        consumed_start_msg_log_id
                        <= message.message_log_id
                        <= consumed_end_msg_log_id
                    )
                ]
                replacement = self._ranges_from_messages(session_id, remaining_messages)
                ranges[index:index + 1] = replacement
                return

    def mark_ranges_review_consumed(self, range_ids: list[int]) -> None:
        """Mark whole unread ranges as consumed by review.

        Each matching range is flagged ``review_consumed`` and all of its
        messages are removed from the unread list.  Ranges whose IDs are not
        present in *range_ids* are left untouched.

        Args:
            range_ids: IDs of ``UnreadRange`` instances to mark consumed.
        """
        if not range_ids:
            return
        range_id_set = set(range_ids)
        for session_id, ranges in self._ranges.items():
            consumed: list[UnreadRange] = []
            for item in ranges:
                if item.id in range_id_set:
                    consumed.append(item)
            for item in consumed:
                self._unread[session_id] = [
                    message
                    for message in self._unread.get(session_id, [])
                    if not item.start_msg_log_id <= message.message_log_id <= item.end_msg_log_id
                ]
            self._ranges[session_id] = [
                item
                if item.id not in range_id_set
                else UnreadRange(
                    id=item.id,
                    session_id=item.session_id,
                    start_msg_log_id=item.start_msg_log_id,
                    end_msg_log_id=item.end_msg_log_id,
                    start_at=item.start_at,
                    end_at=item.end_at,
                    message_count=item.message_count,
                    review_consumed=True,
                    chat_consumed=item.chat_consumed,
                )
                for item in ranges
            ]

    def mark_active_chat_consumed(
        self,
        *,
        session_id: str,
        message_log_ids: list[int],
    ) -> list[UnreadMessage]:
        """Mark messages as consumed during active chat and return them.

        Consumed message IDs are remembered so that subsequent ``add_unread``
        calls for the same messages are silently dropped.  The unread list and
        range bookkeeping are rebuilt from the remaining messages.

        Args:
            session_id: The conversation session that consumed the messages.
            message_log_ids: IDs of messages to mark as consumed.

        Returns:
            The consumed ``UnreadMessage`` instances, sorted by creation time.
            Empty if no matching unread messages were found.
        """
        if not message_log_ids:
            return []
        consumed_ids = set(message_log_ids)
        consumed = [
            message
            for message in self._unread.get(session_id, [])
            if message.message_log_id in consumed_ids
        ]
        if not consumed:
            return []

        self._unread[session_id] = [
            message
            for message in self._unread.get(session_id, [])
            if message.message_log_id not in consumed_ids
        ]
        self._active_chat_consumed_ids[session_id].update(
            message.message_log_id for message in consumed
        )
        self._ranges[session_id] = self._ranges_from_messages(
            session_id,
            list(self._unread.get(session_id, [])),
        )
        return sorted(consumed, key=lambda item: (item.created_at, item.message_log_id))

    def add_high_priority_events(self, events: list[HighPriorityEvent]) -> None:
        """Record high-priority events (mentions, pokes, replies) for later handling.

        Events are appended per session and surfaced during review or active
        reply transitions.

        Args:
            events: High-priority events to enqueue.
        """
        for event in events:
            self._high_priority[event.session_id].append(event)

    def list_high_priority_events(self, session_id: str) -> list[HighPriorityEvent]:
        """Return all pending high-priority events for *session_id*.

        Args:
            session_id: The conversation session to query.

        Returns:
            A new list of ``HighPriorityEvent`` instances (empty if none
            pending).
        """
        return list(self._high_priority.get(session_id, []))

    def mark_high_priority_events_handled(self, session_id: str) -> list[HighPriorityEvent]:
        """Mark all pending high-priority events for *session_id* as handled.

        After this call the pending list for the session is cleared.

        Args:
            session_id: The conversation session whose events to consume.

        Returns:
            The events that were previously pending (now cleared).
        """
        events = self.list_high_priority_events(session_id)
        self._high_priority[session_id].clear()
        return events

    def record_mention(self, session_id: str, timestamp: float) -> None:
        """Record a mention timestamp for wake-threshold checks.

        The timestamp is appended to a per-session deque that
        ``count_recent_mentions`` scans against a sliding time window.

        Args:
            session_id: The conversation session that received the mention.
            timestamp: Epoch timestamp of the mention event.
        """
        self._recent_mentions[session_id].append(timestamp)

    def count_recent_mentions(self, session_id: str, *, now: float, window_seconds: float) -> int:
        """Count mentions in the configured wake window.

        Stale timestamps older than *window_seconds* relative to *now* are
        evicted from the deque before counting.

        Args:
            session_id: The conversation session to query.
            now: Current epoch timestamp.
            window_seconds: Size of the sliding window in seconds.

        Returns:
            The number of mentions that fell within the window.
        """
        recent_mentions = self._recent_mentions[session_id]
        while recent_mentions and now - recent_mentions[0] > window_seconds:
            recent_mentions.popleft()
        return len(recent_mentions)

    def _append_unread_range(self, message: UnreadMessage) -> None:
        ranges = self._ranges[message.session_id]
        active_ranges = [item for item in ranges if not item.review_consumed]
        if active_ranges:
            tail = max(active_ranges, key=lambda item: (item.end_at, item.end_msg_log_id))
            if tail.end_msg_log_id < message.message_log_id and tail.end_at <= message.created_at:
                ranges[ranges.index(tail)] = UnreadRange(
                    id=tail.id,
                    session_id=tail.session_id,
                    start_msg_log_id=tail.start_msg_log_id,
                    end_msg_log_id=message.message_log_id,
                    start_at=tail.start_at,
                    end_at=message.created_at,
                    message_count=tail.message_count + 1,
                    review_consumed=tail.review_consumed,
                    chat_consumed=tail.chat_consumed,
                )
                return
        ranges.append(self._new_range_from_message(message))
        ranges.sort(key=lambda item: (item.start_at, item.start_msg_log_id))

    def _ranges_from_messages(
        self,
        session_id: str,
        messages: list[UnreadMessage],
    ) -> list[UnreadRange]:
        if not messages:
            return []
        messages.sort(key=lambda item: item.message_log_id)
        ranges: list[UnreadRange] = []
        current: list[UnreadMessage] = []
        for message in messages:
            if current and message.message_log_id != current[-1].message_log_id + 1:
                ranges.append(self._new_range_from_messages(session_id, current))
                current = []
            current.append(message)
        if current:
            ranges.append(self._new_range_from_messages(session_id, current))
        return ranges

    def _new_range_from_message(self, message: UnreadMessage) -> UnreadRange:
        return self._new_range_from_messages(message.session_id, [message])

    def _new_range_from_messages(
        self,
        session_id: str,
        messages: list[UnreadMessage],
    ) -> UnreadRange:
        range_id = self._next_range_id
        self._next_range_id += 1
        ordered = sorted(messages, key=lambda item: (item.created_at, item.message_log_id))
        return UnreadRange(
            id=range_id,
            session_id=session_id,
            start_msg_log_id=min(item.message_log_id for item in messages),
            end_msg_log_id=max(item.message_log_id for item in messages),
            start_at=ordered[0].created_at,
            end_at=ordered[-1].created_at,
            message_count=len(messages),
        )


__all__ = ["AgentInbox", "InMemoryAgentInbox"]
