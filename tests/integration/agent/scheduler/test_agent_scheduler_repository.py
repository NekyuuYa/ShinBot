from __future__ import annotations

import pytest

from shinbot.agent.scheduler import (
    ActiveChatDisposition,
    ActiveChatState,
    AgentState,
    HighPriorityEvent,
    HighPriorityEventKind,
)
from shinbot.agent.scheduler.models import (
    ActiveReplyThreshold,
    MentionSensitivity,
    ReviewPlan,
    UnreadMessage,
)
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.dispatchers import AgentEntrySignal
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


def _insert_message(
    db: DatabaseManager,
    *,
    msg_id: str = "msg-1",
    created_at: float = 10_000.0,
) -> int:
    return db.message_logs.insert(
        MessageLogRecord(
            session_id="bot:group:room",
            platform_msg_id=msg_id,
            sender_id="user-1",
            sender_name="User",
            raw_text="hello",
            content_json="[]",
            role="user",
            created_at=created_at,
        )
    )


def test_agent_scheduler_repository_persists_state_and_inbox(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_log_id = _insert_message(db)

    db.agent_scheduler.set_state("bot:group:room", AgentState.ACTIVE_REPLY)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id="bot:group:room",
            message_log_id=message_log_id,
            sender_id="user-1",
            created_at=10.0,
        )
    )
    db.agent_scheduler.add_high_priority_events(
        [
            HighPriorityEvent(
                session_id="bot:group:room",
                message_log_id=message_log_id,
                sender_id="user-1",
                kind=HighPriorityEventKind.MENTION,
                created_at=10.0,
                reason="message_mentions_self",
            )
        ]
    )

    assert db.agent_scheduler.get_state("bot:group:room") == AgentState.ACTIVE_REPLY
    assert [item.message_log_id for item in db.agent_scheduler.list_unread("bot:group:room")] == [
        message_log_id
    ]
    assert [
        (item.start_msg_log_id, item.end_msg_log_id, item.message_count)
        for item in db.agent_scheduler.list_unread_ranges("bot:group:room")
    ] == [(message_log_id, message_log_id, 1)]
    assert [
        event.kind for event in db.agent_scheduler.list_high_priority_events("bot:group:room")
    ] == [HighPriorityEventKind.MENTION]
    assert [
        event.kind
        for event in db.agent_scheduler.mark_high_priority_events_handled("bot:group:room")
    ] == [HighPriorityEventKind.MENTION]
    assert db.agent_scheduler.list_high_priority_events("bot:group:room") == []


def test_agent_scheduler_repository_merges_and_splits_unread_ranges(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, msg_id=f"msg-{index}", created_at=float(index))
        for index in [1, 2, 3]
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )

    unread_range = db.agent_scheduler.list_unread_ranges("bot:group:room")[0]

    assert (unread_range.start_msg_log_id, unread_range.end_msg_log_id) == (
        message_ids[0],
        message_ids[-1],
    )
    assert unread_range.message_count == 3

    db.agent_scheduler.split_review_consumed(
        range_id=unread_range.id or 0,
        consumed_start_msg_log_id=message_ids[1],
        consumed_end_msg_log_id=message_ids[1],
    )

    assert [
        item.message_log_id for item in db.agent_scheduler.list_unread("bot:group:room")
    ] == [message_ids[0], message_ids[2]]
    assert [
        (item.start_msg_log_id, item.end_msg_log_id, item.message_count)
        for item in db.agent_scheduler.list_unread_ranges("bot:group:room")
    ] == [
        (message_ids[0], message_ids[0], 1),
        (message_ids[2], message_ids[2], 1),
    ]


def test_agent_scheduler_repository_marks_active_chat_consumed(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, msg_id=f"msg-{index}", created_at=float(index))
        for index in [1, 2, 3, 4]
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )

    consumed = db.agent_scheduler.mark_active_chat_consumed(
        session_id="bot:group:room",
        message_log_ids=message_ids[1:3],
    )

    assert [item.message_log_id for item in consumed] == message_ids[1:3]
    assert [
        item.message_log_id for item in db.agent_scheduler.list_unread("bot:group:room")
    ] == [message_ids[0], message_ids[3]]
    assert [
        (item.start_msg_log_id, item.end_msg_log_id, item.message_count)
        for item in db.agent_scheduler.list_unread_ranges("bot:group:room")
    ] == [
        (message_ids[0], message_ids[0], 1),
        (message_ids[3], message_ids[3], 1),
    ]
    assert db.agent_scheduler.count_unread_messages("bot:group:room") == 2


def test_agent_scheduler_repository_does_not_merge_across_skipped_session_message(
    tmp_path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    first_id = _insert_message(db, msg_id="msg-1", created_at=1.0)
    skipped_id = _insert_message(db, msg_id="msg-old", created_at=0.5)
    latest_id = _insert_message(db, msg_id="msg-2", created_at=2.0)

    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id="bot:group:room",
            message_log_id=first_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id="bot:group:room",
            message_log_id=latest_id,
            sender_id="user-1",
            created_at=2.0,
        )
    )

    assert skipped_id not in [
        item.message_log_id for item in db.agent_scheduler.list_unread("bot:group:room")
    ]
    assert [
        (item.start_msg_log_id, item.end_msg_log_id)
        for item in db.agent_scheduler.list_unread_ranges("bot:group:room")
    ] == [(first_id, first_id), (latest_id, latest_id)]


def test_agent_scheduler_repository_counts_recent_mentions(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    db.agent_scheduler.record_mention("bot:group:room", 10.0)
    db.agent_scheduler.record_mention("bot:group:room", 30.0)
    db.agent_scheduler.record_mention("bot:group:room", 80.0)

    assert db.agent_scheduler.count_recent_mentions(
        "bot:group:room",
        now=80.0,
        window_seconds=60.0,
    ) == 2
    assert db.agent_scheduler.count_recent_mentions(
        "bot:group:room",
        now=200.0,
        window_seconds=60.0,
    ) == 0


def test_agent_scheduler_repository_persists_review_plan(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    plan = ReviewPlan(
        session_id="bot:group:room",
        next_review_at=130.0,
        reason="busy_until_next_check",
        mention_sensitivity=MentionSensitivity.LOW,
        active_reply_threshold=ActiveReplyThreshold(at_count=2, window_seconds=90.0),
        updated_at=10.0,
    )

    db.agent_scheduler.set_review_plan(plan)

    restored = db.agent_scheduler.get_review_plan("bot:group:room")
    assert restored == plan


def test_agent_scheduler_repository_persists_active_chat_state(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    state = ActiveChatState(
        session_id="bot:group:room",
        interest_value=1.0,
        decay_half_life_seconds=30.0,
        entered_at=10.0,
        updated_at=10.0,
        tick_count=2,
        active_epoch=10_000,
        bootstrap_applied=True,
        bootstrap_disposition=ActiveChatDisposition.CASUAL,
    )

    db.agent_scheduler.set_active_chat_state(state)

    assert db.agent_scheduler.get_active_chat_state("bot:group:room") == state

    db.agent_scheduler.clear_active_chat_state("bot:group:room")

    assert db.agent_scheduler.get_active_chat_state("bot:group:room") is None


def test_agent_scheduler_repository_lists_due_review_plans(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    db.agent_scheduler.set_review_plan(
        ReviewPlan(session_id="bot:group:due", next_review_at=10.0, reason="due")
    )
    db.agent_scheduler.set_review_plan(
        ReviewPlan(session_id="bot:group:future", next_review_at=30.0, reason="future")
    )

    due = db.agent_scheduler.list_due_review_plans(now=20.0)

    assert [plan.session_id for plan in due] == ["bot:group:due"]


@pytest.mark.asyncio
async def test_agent_runtime_uses_persistent_scheduler_store(tmp_path) -> None:
    from shinbot.agent.runtime import install_agent_runtime

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    message_log_id = _insert_message(bot.database)

    await runtime.handle_agent_entry(
        AgentEntrySignal(
            session_id="bot:group:room",
            message_log_id=message_log_id,
            event_type="message-created",
            sender_id="user-1",
            instance_id="bot",
            platform="mock",
            self_id="bot-self",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
    )

    assert [
        item.message_log_id for item in bot.database.agent_scheduler.list_unread("bot:group:room")
    ] == [message_log_id]
