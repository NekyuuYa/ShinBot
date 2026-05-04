"""Agent-internal scheduler entrypoint."""

from __future__ import annotations

import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from shinbot.agent.scheduler.models import (
    AgentScheduleDecision,
    AgentState,
    HighPriorityEvent,
    HighPriorityEventKind,
    UnreadMessage,
)
from shinbot.agent.scheduler.workflow_dispatcher import AgentWorkflowDispatcher

if TYPE_CHECKING:
    from shinbot.core.dispatch.dispatchers import AgentEntrySignal

ResponseProfileResolver = Callable[["AgentEntrySignal"], str]


@dataclass(slots=True)
class AgentSchedulerConfig:
    """Minimal scheduler thresholds for the first Agent scheduling pass."""

    mention_wake_count: int = 1
    mention_wake_window_seconds: float = 60.0


class AgentScheduler:
    """Accepts Agent entry signals and decides which Agent workflow should run."""

    def __init__(
        self,
        *,
        workflow_dispatcher: AgentWorkflowDispatcher | None = None,
        response_profile_resolver: ResponseProfileResolver,
        config: AgentSchedulerConfig | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        self._workflow_dispatcher = workflow_dispatcher
        self._response_profile_resolver = response_profile_resolver
        self._config = config or AgentSchedulerConfig()
        self._now = now or time.time
        self._states: dict[str, AgentState] = defaultdict(lambda: AgentState.IDLE)
        self._unread: dict[str, list[UnreadMessage]] = defaultdict(list)
        self._high_priority: dict[str, list[HighPriorityEvent]] = defaultdict(list)
        self._recent_mentions: dict[str, deque[float]] = defaultdict(deque)

    async def accept_signal(self, signal: AgentEntrySignal) -> AgentScheduleDecision:
        """Accept one message signal from core and decide scheduler-side action."""
        if signal.message_log_id is None:
            return AgentScheduleDecision(
                accepted=False,
                state=self._states[signal.session_id],
                skipped_reason="missing_message_log_id",
            )
        if signal.already_handled:
            return AgentScheduleDecision(
                accepted=False,
                state=self._states[signal.session_id],
                skipped_reason="already_handled",
            )
        if signal.is_stopped:
            return AgentScheduleDecision(
                accepted=False,
                state=self._states[signal.session_id],
                skipped_reason="stopped",
            )

        now = self._now()
        unread = UnreadMessage(
            session_id=signal.session_id,
            message_log_id=signal.message_log_id,
            sender_id=signal.sender_id,
            created_at=now,
        )
        self._unread[signal.session_id].append(unread)

        high_priority_events = self._detect_high_priority_events(signal, now)
        if high_priority_events:
            self._high_priority[signal.session_id].extend(high_priority_events)

        should_active_reply = self._should_wake_for_active_reply(signal, high_priority_events, now)
        if should_active_reply and self._workflow_dispatcher is not None:
            self._states[signal.session_id] = AgentState.ACTIVE_REPLY
            await self._workflow_dispatcher.run_active_reply(
                session_id=signal.session_id,
                message_log_id=signal.message_log_id,
                sender_id=signal.sender_id,
                response_profile=self._response_profile_resolver(signal),
                is_mentioned=signal.is_mentioned,
                is_reply_to_bot=signal.is_reply_to_bot,
                self_platform_id=signal.self_id,
                events=high_priority_events,
            )
            return AgentScheduleDecision(
                accepted=True,
                state=self._states[signal.session_id],
                unread_message=unread,
                high_priority_events=high_priority_events,
                active_reply_started=True,
            )

        return AgentScheduleDecision(
            accepted=True,
            state=self._states[signal.session_id],
            unread_message=unread,
            high_priority_events=high_priority_events,
            active_reply_started=False,
        )

    def unread_messages(self, session_id: str) -> list[UnreadMessage]:
        """Return unread messages known to AgentScheduler for one session."""
        return list(self._unread.get(session_id, []))

    def high_priority_events(self, session_id: str) -> list[HighPriorityEvent]:
        """Return high-priority events known to AgentScheduler for one session."""
        return list(self._high_priority.get(session_id, []))

    def state_for(self, session_id: str) -> AgentState:
        """Return current scheduler state for one session."""
        return self._states[session_id]

    def _detect_high_priority_events(
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
        return events

    def _should_wake_for_active_reply(
        self,
        signal: AgentEntrySignal,
        events: list[HighPriorityEvent],
        now: float,
    ) -> bool:
        if not events:
            return False
        if signal.is_reply_to_bot:
            return True
        if signal.is_mentioned:
            recent_mentions = self._recent_mentions[signal.session_id]
            window = self._config.mention_wake_window_seconds
            while recent_mentions and now - recent_mentions[0] > window:
                recent_mentions.popleft()
            recent_mentions.append(now)
            return len(recent_mentions) >= self._config.mention_wake_count
        return False
