from __future__ import annotations

from shinbot.agent.scheduler import (
    ActiveChatState,
    ActiveReplyThreshold,
    AgentState,
    HighPriorityEvent,
    HighPriorityEventKind,
    InMemoryAgentInbox,
    InMemoryAgentStateStore,
    MentionSensitivity,
    ReviewPlan,
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
    assert inbox.mark_high_priority_events_handled("bot:group:room") == [event]
    assert inbox.list_high_priority_events("bot:group:room") == []


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


def test_in_memory_agent_state_store_records_review_plan() -> None:
    store = InMemoryAgentStateStore()
    plan = ReviewPlan(
        session_id="bot:group:room",
        next_review_at=130.0,
        reason="busy_until_next_check",
        mention_sensitivity=MentionSensitivity.HIGH,
        active_reply_threshold=ActiveReplyThreshold(at_count=1, window_seconds=30.0),
        updated_at=10.0,
    )

    store.set_review_plan(plan)

    assert store.get_review_plan("bot:group:room") == plan


def test_in_memory_agent_state_store_records_active_chat_state() -> None:
    store = InMemoryAgentStateStore()
    state = ActiveChatState(
        session_id="bot:group:room",
        interest_value=1.0,
        decay_half_life_seconds=30.0,
        entered_at=10.0,
        updated_at=10.0,
    )

    store.set_active_chat_state(state)

    assert store.get_active_chat_state("bot:group:room") == state

    store.clear_active_chat_state("bot:group:room")

    assert store.get_active_chat_state("bot:group:room") is None


def test_in_memory_agent_state_store_lists_due_review_plans() -> None:
    store = InMemoryAgentStateStore()
    store.set_review_plan(
        ReviewPlan(session_id="bot:group:due", next_review_at=10.0, reason="due")
    )
    store.set_review_plan(
        ReviewPlan(session_id="bot:group:future", next_review_at=30.0, reason="future")
    )

    due = store.list_due_review_plans(now=20.0)

    assert [plan.session_id for plan in due] == ["bot:group:due"]
