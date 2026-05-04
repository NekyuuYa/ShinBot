from __future__ import annotations

from shinbot.agent.scheduler import (
    AgentState,
    HighPriorityEvent,
    HighPriorityEventKind,
    InMemoryAgentInbox,
    InMemoryAgentStateStore,
    UnreadMessage,
)


def test_in_memory_agent_inbox_records_unread_and_high_priority_events() -> None:
    inbox = InMemoryAgentInbox()
    unread = UnreadMessage(
        session_id="bot:group:room",
        message_log_id=1,
        sender_id="user-1",
        created_at=10.0,
    )
    event = HighPriorityEvent(
        session_id="bot:group:room",
        message_log_id=1,
        sender_id="user-1",
        kind=HighPriorityEventKind.MENTION,
        created_at=10.0,
        reason="message_mentions_self",
    )

    inbox.add_unread(unread)
    inbox.add_high_priority_events([event])

    assert inbox.list_unread("bot:group:room") == [unread]
    assert inbox.list_high_priority_events("bot:group:room") == [event]


def test_in_memory_agent_inbox_counts_mentions_in_window() -> None:
    inbox = InMemoryAgentInbox()
    inbox.record_mention("bot:group:room", 10.0)
    inbox.record_mention("bot:group:room", 30.0)
    inbox.record_mention("bot:group:room", 80.0)

    assert inbox.count_recent_mentions(
        "bot:group:room",
        now=80.0,
        window_seconds=60.0,
    ) == 2

    assert inbox.count_recent_mentions(
        "bot:group:room",
        now=200.0,
        window_seconds=60.0,
    ) == 0


def test_in_memory_agent_state_store_defaults_to_idle_and_updates() -> None:
    store = InMemoryAgentStateStore()

    assert store.get_state("bot:group:room") == AgentState.IDLE

    store.set_state("bot:group:room", AgentState.ACTIVE_REPLY)

    assert store.get_state("bot:group:room") == AgentState.ACTIVE_REPLY
