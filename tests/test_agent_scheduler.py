from __future__ import annotations

from typing import Any

import pytest

from shinbot.agent.scheduler import (
    AgentScheduler,
    AgentSchedulerConfig,
    AgentState,
    HighPriorityEventKind,
    InMemoryAgentInbox,
    InMemoryAgentStateStore,
    PriorityPolicyDecision,
)
from shinbot.agent.scheduler.models import HighPriorityEvent
from shinbot.core.dispatch.dispatchers import AgentEntrySignal


class RecordingWorkflowDispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_active_reply(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        **kwargs: Any,
    ) -> None:
        self.calls.append(
            {
                "session_id": session_id,
                "message_log_id": message_log_id,
                "sender_id": sender_id,
                **kwargs,
            }
        )


class AlwaysWakePriorityPolicy:
    def evaluate(self, signal, *, now, inbox):
        return PriorityPolicyDecision(
            events=[
                HighPriorityEvent(
                    session_id=signal.session_id,
                    message_log_id=signal.message_log_id or 0,
                    sender_id=signal.sender_id,
                    kind=HighPriorityEventKind.POKE,
                    created_at=now,
                    reason="test_policy",
                )
            ],
            should_start_active_reply=True,
        )


def make_signal(
    *,
    message_log_id: int | None = 1,
    is_mentioned: bool = False,
    is_reply_to_bot: bool = False,
    already_handled: bool = False,
    is_stopped: bool = False,
) -> AgentEntrySignal:
    return AgentEntrySignal(
        session_id="bot:group:room",
        message_log_id=message_log_id,
        event_type="message-created",
        sender_id="user-1",
        instance_id="bot",
        platform="mock",
        self_id="bot-self",
        is_private=False,
        is_mentioned=is_mentioned,
        is_reply_to_bot=is_reply_to_bot,
        already_handled=already_handled,
        is_stopped=is_stopped,
    )


@pytest.mark.asyncio
async def test_scheduler_records_ordinary_message_without_workflow() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
    )

    decision = await scheduler.accept_signal(make_signal())

    assert decision.accepted is True
    assert decision.state == AgentState.IDLE
    assert decision.active_reply_started is False
    assert dispatcher.calls == []
    assert [item.message_log_id for item in scheduler.unread_messages("bot:group:room")] == [1]
    assert scheduler.high_priority_events("bot:group:room") == []


@pytest.mark.asyncio
async def test_scheduler_starts_active_reply_for_mention() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "immediate",
    )

    decision = await scheduler.accept_signal(make_signal(is_mentioned=True))

    assert decision.accepted is True
    assert decision.state == AgentState.ACTIVE_REPLY
    assert decision.active_reply_started is True
    assert [event.kind for event in decision.high_priority_events] == [
        HighPriorityEventKind.MENTION
    ]
    assert dispatcher.calls[0]["response_profile"] == "immediate"
    assert dispatcher.calls[0]["is_mentioned"] is True


@pytest.mark.asyncio
async def test_scheduler_can_require_repeated_mentions_before_wake() -> None:
    now = 10.0
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        config=AgentSchedulerConfig(mention_wake_count=2, mention_wake_window_seconds=60),
        now=lambda: now,
    )

    first = await scheduler.accept_signal(make_signal(message_log_id=1, is_mentioned=True))
    second = await scheduler.accept_signal(make_signal(message_log_id=2, is_mentioned=True))

    assert first.active_reply_started is False
    assert second.active_reply_started is True
    assert [call["message_log_id"] for call in dispatcher.calls] == [2]
    assert [event.kind for event in scheduler.high_priority_events("bot:group:room")] == [
        HighPriorityEventKind.MENTION,
        HighPriorityEventKind.MENTION,
    ]


@pytest.mark.asyncio
async def test_scheduler_uses_injected_inbox_and_state_store() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    inbox = InMemoryAgentInbox()
    state_store = InMemoryAgentStateStore()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "immediate",
        inbox=inbox,
        state_store=state_store,
    )

    await scheduler.accept_signal(make_signal(is_reply_to_bot=True))

    assert state_store.get_state("bot:group:room") == AgentState.ACTIVE_REPLY
    assert [item.message_log_id for item in inbox.list_unread("bot:group:room")] == [1]
    assert [event.kind for event in inbox.list_high_priority_events("bot:group:room")] == [
        HighPriorityEventKind.REPLY_TO_BOT
    ]


@pytest.mark.asyncio
async def test_scheduler_uses_injected_priority_policy() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "immediate",
        priority_policy=AlwaysWakePriorityPolicy(),
    )

    decision = await scheduler.accept_signal(make_signal())

    assert decision.active_reply_started is True
    assert [event.kind for event in decision.high_priority_events] == [HighPriorityEventKind.POKE]
    assert dispatcher.calls[0]["events"][0].reason == "test_policy"


@pytest.mark.asyncio
async def test_scheduler_skips_unusable_signals() -> None:
    dispatcher = RecordingWorkflowDispatcher()
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
    )

    missing = await scheduler.accept_signal(make_signal(message_log_id=None))
    handled = await scheduler.accept_signal(make_signal(already_handled=True))
    stopped = await scheduler.accept_signal(make_signal(is_stopped=True))

    assert [missing.skipped_reason, handled.skipped_reason, stopped.skipped_reason] == [
        "missing_message_log_id",
        "already_handled",
        "stopped",
    ]
    assert dispatcher.calls == []
    assert scheduler.unread_messages("bot:group:room") == []
