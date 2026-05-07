from __future__ import annotations

import pytest

from shinbot.agent.scheduler import ActiveChatPolicyConfig, DefaultActiveChatPolicy


def test_default_active_chat_policy_builds_initial_state() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=20.0,
            decay_half_life_seconds=30.0,
            idle_interest_threshold=5.0,
        )
    )

    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    assert state.session_id == "bot:group:room"
    assert state.interest_value == 20.0
    assert state.decay_half_life_seconds == 30.0
    assert state.entered_at == 10.0
    assert state.updated_at == 10.0
    assert policy.should_return_idle(state) is False


def test_default_active_chat_policy_applies_exponential_decay() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=10.0,
            decay_half_life_seconds=10.0,
            idle_interest_threshold=3.0,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    decayed = policy.decay(state, now=20.0)

    assert decayed.interest_value == 5.0
    assert decayed.entered_at == 10.0
    assert decayed.updated_at == 20.0
    assert policy.should_return_idle(decayed) is False


def test_default_active_chat_policy_returns_idle_below_threshold() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=10.0,
            decay_half_life_seconds=10.0,
            idle_interest_threshold=5.0,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    decayed = policy.decay(state, now=20.0)

    assert decayed.interest_value == 5.0
    assert policy.should_return_idle(decayed) is True


def test_default_active_chat_policy_observes_message_after_decay() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=10.0,
            decay_half_life_seconds=10.0,
            idle_interest_threshold=5.0,
            message_interest_delta=20.0,
            mention_interest_delta=30.0,
            reply_interest_delta=40.0,
            max_interest_value=100.0,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    observed = policy.observe_message(
        state,
        now=20.0,
        is_mentioned=True,
        is_reply_to_bot=True,
    )

    assert observed.interest_value == pytest.approx(95.0)
    assert observed.entered_at == 10.0
    assert observed.updated_at == 20.0


def test_default_active_chat_policy_caps_message_interest() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=95.0,
            decay_half_life_seconds=10.0,
            message_interest_delta=10.0,
            max_interest_value=100.0,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    observed = policy.observe_message(state, now=10.0)

    assert observed.interest_value == 100.0
