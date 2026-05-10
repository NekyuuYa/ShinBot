from __future__ import annotations

import pytest

from shinbot.agent.scheduler import (
    ActiveChatDisposition,
    ActiveChatPolicyConfig,
    DefaultActiveChatPolicy,
    calculate_bootstrap_correction,
)


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
    assert state.tick_count == 0
    assert state.active_epoch == 10_000
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
    assert decayed.tick_count == 0
    assert policy.should_return_idle(decayed) is False


def test_default_active_chat_policy_tick_decay_uses_fixed_tick_interval() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=15.0,
            decay_half_life_seconds=20.0,
            tick_interval_seconds=5.0,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    decayed = policy.decay(state, now=100.0, count_tick=True)

    assert decayed.interest_value == pytest.approx(15.0 * (0.5 ** (5.0 / 20.0)))
    assert decayed.tick_count == 1


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


def test_default_active_chat_policy_observes_message_without_natural_decay() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=10.0,
            decay_half_life_seconds=10.0,
            idle_interest_threshold=5.0,
            message_interest_delta=20.0,
            mention_interest_delta=120.0,
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

    assert observed.interest_value == 100.0
    assert observed.entered_at == 10.0
    assert observed.updated_at == 20.0
    assert observed.tick_count == 0


def test_default_active_chat_policy_uses_conservative_message_interest() -> None:
    policy = DefaultActiveChatPolicy(
        ActiveChatPolicyConfig(
            initial_interest_value=10.0,
            max_interest_value=100.0,
        )
    )
    state = policy.initial_state(session_id="bot:group:room", now=10.0)

    ordinary = policy.observe_message(state, now=11.0)
    mentioned = policy.observe_message(state, now=12.0, is_mentioned=True)
    replied = policy.observe_message(state, now=13.0, is_reply_to_bot=True)
    poked = policy.observe_message(
        state,
        now=14.0,
        is_poke_to_bot=True,
        is_poke_to_other=True,
        is_mention_to_other=True,
    )
    from_bot = policy.observe_message(
        state,
        now=15.0,
        is_from_bot=True,
        is_mentioned=True,
        is_reply_to_bot=True,
    )

    assert ordinary.interest_value == 11.0
    assert mentioned.interest_value == 18.0
    assert replied.interest_value == 15.0
    assert poked.interest_value == 10.0
    assert from_bot.interest_value == 10.0


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


def test_default_active_chat_policy_applies_bootstrap_curve_correction() -> None:
    config = ActiveChatPolicyConfig(
        initial_interest_value=15.0,
        decay_half_life_seconds=20.0,
        tick_interval_seconds=5.0,
    )
    policy = DefaultActiveChatPolicy(config)
    state = policy.initial_state(session_id="bot:group:room", now=10.0)
    state = policy.decay(state, now=15.0, count_tick=True)
    state = policy.observe_message(state, now=16.0)

    correction = calculate_bootstrap_correction(
        state,
        disposition=ActiveChatDisposition.EXIT_SOON,
        config=config,
    )
    corrected = policy.apply_bootstrap_disposition(
        state,
        disposition=ActiveChatDisposition.EXIT_SOON,
        now=17.0,
    )

    assert correction.default_curve_interest == pytest.approx(12.6134, rel=1e-4)
    assert correction.preset_curve_interest == pytest.approx(10.6066, rel=1e-4)
    assert corrected.interest_value == pytest.approx(state.interest_value + correction.correction)
    assert corrected.decay_half_life_seconds == 10.0
    assert corrected.tick_count == 1
    assert corrected.bootstrap_applied is True
    assert corrected.bootstrap_disposition == ActiveChatDisposition.EXIT_SOON


def test_bootstrap_correction_is_zero_at_zero_ticks() -> None:
    config = ActiveChatPolicyConfig(
        initial_interest_value=15.0,
        decay_half_life_seconds=20.0,
        tick_interval_seconds=5.0,
    )
    policy = DefaultActiveChatPolicy(config)
    state = policy.initial_state(session_id="s", now=10.0)
    # No ticks — default and preset curves both start at initial values,
    # so correction = preset_initial - default_initial.
    correction = calculate_bootstrap_correction(
        state,
        disposition=ActiveChatDisposition.WATCH,
        config=config,
    )
    # WATCH preset interest (20) - default initial (15) = 5
    assert correction.correction == pytest.approx(5.0, rel=1e-4)
    corrected = policy.apply_bootstrap_disposition(
        state, disposition=ActiveChatDisposition.WATCH, now=10.0,
    )
    assert corrected.interest_value == pytest.approx(20.0, rel=1e-4)
    assert corrected.decay_half_life_seconds == 20.0
    assert corrected.tick_count == 0


def test_bootstrap_correction_grows_with_tick_count() -> None:
    config = ActiveChatPolicyConfig(
        initial_interest_value=15.0,
        decay_half_life_seconds=20.0,
        tick_interval_seconds=5.0,
    )
    policy = DefaultActiveChatPolicy(config)
    state = policy.initial_state(session_id="s", now=10.0)
    # Simulate 4 ticks (20 seconds elapsed = one half-life)
    for i in range(4):
        state = policy.decay(state, now=10.0 + (i + 1) * 5.0, count_tick=True)

    assert state.tick_count == 4
    # After 20s (one half-life of 20s), interest should be ~7.5
    assert state.interest_value == pytest.approx(7.5, rel=1e-2)

    correction = calculate_bootstrap_correction(
        state,
        disposition=ActiveChatDisposition.ENGAGED,
        config=config,
    )
    # ENGAGED: initial=40, half_life=35, elapsed=20s
    # 40 * 0.5^(20/35) ≈ 26.92
    # Default: initial=15, half_life=20, elapsed=20s → 7.5
    # correction ≈ 26.92 - 7.5 = 19.42
    assert correction.preset_curve_interest == pytest.approx(26.918, rel=1e-2)
    assert correction.default_curve_interest == pytest.approx(7.5, rel=1e-2)

    corrected = policy.apply_bootstrap_disposition(
        state, disposition=ActiveChatDisposition.ENGAGED, now=31.0,
    )
    # current (7.5) + correction (19.42) ≈ 26.92
    assert corrected.interest_value == pytest.approx(26.918, rel=1e-2)
    assert corrected.decay_half_life_seconds == 35.0
    assert corrected.tick_count == 4


@pytest.mark.parametrize(
    "disposition,expected_half_life",
    [
        (ActiveChatDisposition.EXIT_SOON, 10.0),
        (ActiveChatDisposition.WATCH, 20.0),
        (ActiveChatDisposition.CASUAL, 25.0),
        (ActiveChatDisposition.ENGAGED, 35.0),
        (ActiveChatDisposition.FOCUSED, 45.0),
    ],
)
def test_bootstrap_correction_sets_half_life_per_disposition(
    disposition: ActiveChatDisposition,
    expected_half_life: float,
) -> None:
    config = ActiveChatPolicyConfig(
        initial_interest_value=15.0,
        decay_half_life_seconds=20.0,
        tick_interval_seconds=5.0,
    )
    policy = DefaultActiveChatPolicy(config)
    state = policy.initial_state(session_id="s", now=10.0)
    state = policy.decay(state, now=15.0, count_tick=True)

    corrected = policy.apply_bootstrap_disposition(
        state, disposition=disposition, now=16.0,
    )
    assert corrected.decay_half_life_seconds == expected_half_life
    assert corrected.bootstrap_applied is True
    assert corrected.bootstrap_disposition == disposition


def test_bootstrap_correction_preserves_tick_count_and_epoch() -> None:
    config = ActiveChatPolicyConfig(
        initial_interest_value=15.0,
        decay_half_life_seconds=20.0,
        tick_interval_seconds=5.0,
    )
    policy = DefaultActiveChatPolicy(config)
    state = policy.initial_state(session_id="s", now=10.0)
    for i in range(3):
        state = policy.decay(state, now=10.0 + (i + 1) * 5.0, count_tick=True)

    original_epoch = state.active_epoch
    original_tick_count = state.tick_count
    original_entered_at = state.entered_at

    corrected = policy.apply_bootstrap_disposition(
        state, disposition=ActiveChatDisposition.FOCUSED, now=26.0,
    )
    assert corrected.tick_count == original_tick_count
    assert corrected.active_epoch == original_epoch
    assert corrected.entered_at == original_entered_at
    assert corrected.updated_at == 26.0
