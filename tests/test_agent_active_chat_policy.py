from __future__ import annotations

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
