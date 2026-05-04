from __future__ import annotations

import pytest

from shinbot.agent.scheduler import ActiveChatPolicyConfig, DefaultActiveChatPolicy


def test_default_active_chat_policy_builds_initial_state() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=2.0,
            decay_half_life_seconds=30.0,
            idle_interest_threshold=0.1,
        )
    )

    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    assert state.session_id == "bot:group:room"
    assert state.interest_value == 2.0
    assert state.decay_half_life_seconds == 30.0
    assert state.entered_at == 10.0
    assert state.updated_at == 10.0
    assert policy.should_return_idle(state) is False


def test_default_active_chat_policy_applies_exponential_decay() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=1.0,
            decay_half_life_seconds=10.0,
            idle_interest_threshold=0.3,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    decayed = policy.decay(state, now=20.0)

    assert decayed.interest_value == 0.5
    assert decayed.entered_at == 10.0
    assert decayed.updated_at == 20.0
    assert policy.should_return_idle(decayed) is False


def test_default_active_chat_policy_returns_idle_below_threshold() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=1.0,
            decay_half_life_seconds=10.0,
            idle_interest_threshold=0.5,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    decayed = policy.decay(state, now=20.0)

    assert decayed.interest_value == 0.5
    assert policy.should_return_idle(decayed) is True


def test_default_active_chat_policy_observes_message_after_decay() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=1.0,
            decay_half_life_seconds=10.0,
            idle_interest_threshold=0.1,
            message_interest_delta=0.2,
            mention_interest_delta=0.3,
            reply_interest_delta=0.4,
            max_interest_value=2.0,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    observed = policy.observe_message(
        state,
        now=20.0,
        is_mentioned=True,
        is_reply_to_bot=True,
    )

    assert observed.interest_value == pytest.approx(1.4)
    assert observed.entered_at == 10.0
    assert observed.updated_at == 20.0


def test_default_active_chat_policy_caps_message_interest() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=1.9,
            decay_half_life_seconds=10.0,
            message_interest_delta=0.5,
            max_interest_value=2.0,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    observed = policy.observe_message(state, now=10.0)

    assert observed.interest_value == 2.0
