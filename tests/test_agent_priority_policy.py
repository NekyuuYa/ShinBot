from __future__ import annotations

from shinbot.agent.scheduler import (
    DefaultPriorityPolicy,
    HighPriorityEventKind,
    InMemoryAgentInbox,
    PriorityPolicyConfig,
)
from shinbot.core.dispatch.dispatchers import AgentEntrySignal


def make_signal(
    *,
    message_log_id: int = 1,
    is_mentioned: bool = False,
    is_reply_to_bot: bool = False,
    is_poke_to_bot: bool = False,
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
        is_poke_to_bot=is_poke_to_bot,
    )


def test_default_priority_policy_detects_mention_without_required_wake() -> None:
    inbox = InMemoryAgentInbox()
    policy = DefaultPriorityPolicy(
        PriorityPolicyConfig(mention_wake_count=2, mention_wake_window_seconds=60.0)
    )

    decision = policy.evaluate(make_signal(is_mentioned=True), now=10.0, inbox=inbox)

    assert [event.kind for event in decision.events] == [HighPriorityEventKind.MENTION]
    assert decision.should_start_active_reply is False


def test_default_priority_policy_wakes_after_repeated_mentions() -> None:
    inbox = InMemoryAgentInbox()
    policy = DefaultPriorityPolicy(
        PriorityPolicyConfig(mention_wake_count=2, mention_wake_window_seconds=60.0)
    )

    first = policy.evaluate(
        make_signal(message_log_id=1, is_mentioned=True),
        now=10.0,
        inbox=inbox,
    )
    second = policy.evaluate(
        make_signal(message_log_id=2, is_mentioned=True),
        now=30.0,
        inbox=inbox,
    )

    assert first.should_start_active_reply is False
    assert second.should_start_active_reply is True


def test_default_priority_policy_wakes_immediately_for_reply_to_bot() -> None:
    inbox = InMemoryAgentInbox()
    policy = DefaultPriorityPolicy()

    decision = policy.evaluate(make_signal(is_reply_to_bot=True), now=10.0, inbox=inbox)

    assert [event.kind for event in decision.events] == [HighPriorityEventKind.REPLY_TO_BOT]
    assert decision.should_start_active_reply is True


def test_default_priority_policy_wakes_immediately_for_poke_to_bot() -> None:
    inbox = InMemoryAgentInbox()
    policy = DefaultPriorityPolicy()

    decision = policy.evaluate(make_signal(is_poke_to_bot=True), now=10.0, inbox=inbox)

    assert [event.kind for event in decision.events] == [HighPriorityEventKind.POKE]
    assert decision.should_start_active_reply is True


def test_default_priority_policy_ignores_ordinary_message() -> None:
    inbox = InMemoryAgentInbox()
    policy = DefaultPriorityPolicy()

    decision = policy.evaluate(make_signal(), now=10.0, inbox=inbox)

    assert decision.events == []
    assert decision.should_start_active_reply is False
