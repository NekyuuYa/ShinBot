"""Active chat interest policy boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from shinbot.agent.scheduler.models import ActiveChatDisposition, ActiveChatState


@dataclass(slots=True, frozen=True)
class ActiveChatPolicyConfig:
    """Defaults for the first active chat interest implementation.

    Interest values use a 0-100 scale. Active chat exits when the decayed
    value reaches ``idle_interest_threshold``; exponential decay is not
    expected to hit zero exactly.
    """

    initial_interest_value: float = 15.0
    decay_half_life_seconds: float = 20.0
    tick_interval_seconds: float = 5.0
    idle_interest_threshold: float = 5.0
    message_interest_delta: float = 1.0
    mention_interest_delta: float = 8.0
    reply_interest_delta: float = 5.0
    max_interest_value: float = 100.0


@dataclass(slots=True, frozen=True)
class ActiveChatPreset:
    """Internal numeric preset for one active-chat disposition."""

    interest_value: float
    decay_half_life_seconds: float


@dataclass(slots=True, frozen=True)
class ActiveChatBootstrapCorrection:
    """Correction calculated from delayed stage-3 bootstrap disposition."""

    disposition: ActiveChatDisposition
    default_curve_interest: float
    preset_curve_interest: float
    correction: float
    interest_value: float
    decay_half_life_seconds: float


ACTIVE_CHAT_DISPOSITION_PRESETS: dict[ActiveChatDisposition, ActiveChatPreset] = {
    ActiveChatDisposition.EXIT_SOON: ActiveChatPreset(
        interest_value=15.0,
        decay_half_life_seconds=10.0,
    ),
    ActiveChatDisposition.WATCH: ActiveChatPreset(
        interest_value=20.0,
        decay_half_life_seconds=20.0,
    ),
    ActiveChatDisposition.CASUAL: ActiveChatPreset(
        interest_value=30.0,
        decay_half_life_seconds=25.0,
    ),
    ActiveChatDisposition.ENGAGED: ActiveChatPreset(
        interest_value=40.0,
        decay_half_life_seconds=35.0,
    ),
    ActiveChatDisposition.FOCUSED: ActiveChatPreset(
        interest_value=50.0,
        decay_half_life_seconds=45.0,
    ),
}


class ActiveChatPolicy(Protocol):
    """Maintain active chat interest state."""

    def initial_state(
        self,
        *,
        session_id: str,
        now: float,
        initial_interest_value: float | None = None,
        decay_half_life_seconds: float | None = None,
    ) -> ActiveChatState:
        """Build the initial active chat interest state."""

    def decay(
        self,
        state: ActiveChatState,
        *,
        now: float,
        count_tick: bool = False,
    ) -> ActiveChatState:
        """Apply natural interest decay."""

    def observe_message(
        self,
        state: ActiveChatState,
        *,
        now: float,
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
        is_mention_to_other: bool = False,
        is_poke_to_bot: bool = False,
        is_poke_to_other: bool = False,
    ) -> ActiveChatState:
        """Apply message-driven interest changes."""

    def should_return_idle(self, state: ActiveChatState) -> bool:
        """Return whether decayed interest is low enough to leave active chat."""

    def apply_bootstrap_disposition(
        self,
        state: ActiveChatState,
        *,
        disposition: ActiveChatDisposition,
        now: float,
    ) -> ActiveChatState:
        """Apply delayed active-chat bootstrap disposition as a curve correction."""


class DefaultActiveChatPolicy:
    """Exponential decay policy used until message-aware interest updates exist."""

    def __init__(self, config: ActiveChatPolicyConfig | None = None) -> None:
        self._config = config or ActiveChatPolicyConfig()

    def initial_state(
        self,
        *,
        session_id: str,
        now: float,
        initial_interest_value: float | None = None,
        decay_half_life_seconds: float | None = None,
    ) -> ActiveChatState:
        raw_interest_value = (
            self._config.initial_interest_value
            if initial_interest_value is None
            else initial_interest_value
        )
        return ActiveChatState(
            session_id=session_id,
            interest_value=max(
                0.0,
                min(self._config.max_interest_value, raw_interest_value),
            ),
            decay_half_life_seconds=(
                self._config.decay_half_life_seconds
                if decay_half_life_seconds is None
                else decay_half_life_seconds
            ),
            entered_at=now,
            updated_at=now,
            active_epoch=int(now * 1000),
        )

    def decay(
        self,
        state: ActiveChatState,
        *,
        now: float,
        count_tick: bool = False,
    ) -> ActiveChatState:
        elapsed = max(0.0, now - state.updated_at)
        if elapsed == 0.0 and not count_tick:
            return state
        decay_elapsed = self._config.tick_interval_seconds if count_tick else elapsed
        if state.decay_half_life_seconds <= 0.0:
            interest_value = 0.0
        else:
            interest_value = state.interest_value * (
                0.5 ** (decay_elapsed / state.decay_half_life_seconds)
            )
        return ActiveChatState(
            session_id=state.session_id,
            interest_value=interest_value,
            decay_half_life_seconds=state.decay_half_life_seconds,
            entered_at=state.entered_at,
            updated_at=now,
            tick_count=state.tick_count + 1 if count_tick else state.tick_count,
            active_epoch=state.active_epoch,
            bootstrap_applied=state.bootstrap_applied,
            bootstrap_disposition=state.bootstrap_disposition,
        )

    def observe_message(
        self,
        state: ActiveChatState,
        *,
        now: float,
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
        is_mention_to_other: bool = False,
        is_poke_to_bot: bool = False,
        is_poke_to_other: bool = False,
    ) -> ActiveChatState:
        delta = self._config.message_interest_delta
        if is_mentioned:
            delta += self._config.mention_interest_delta
        if is_reply_to_bot:
            delta += self._config.reply_interest_delta
        return ActiveChatState(
            session_id=state.session_id,
            interest_value=min(
                self._config.max_interest_value,
                state.interest_value + delta,
            ),
            decay_half_life_seconds=state.decay_half_life_seconds,
            entered_at=state.entered_at,
            updated_at=now,
            tick_count=state.tick_count,
            active_epoch=state.active_epoch,
            bootstrap_applied=state.bootstrap_applied,
            bootstrap_disposition=state.bootstrap_disposition,
        )

    def should_return_idle(self, state: ActiveChatState) -> bool:
        return state.interest_value <= self._config.idle_interest_threshold + 1e-9

    def apply_bootstrap_disposition(
        self,
        state: ActiveChatState,
        *,
        disposition: ActiveChatDisposition,
        now: float,
    ) -> ActiveChatState:
        correction = calculate_bootstrap_correction(
            state,
            disposition=disposition,
            config=self._config,
        )
        return ActiveChatState(
            session_id=state.session_id,
            interest_value=correction.interest_value,
            decay_half_life_seconds=correction.decay_half_life_seconds,
            entered_at=state.entered_at,
            updated_at=now,
            tick_count=state.tick_count,
            active_epoch=state.active_epoch,
            bootstrap_applied=True,
            bootstrap_disposition=disposition,
        )


def interest_curve_after_ticks(
    *,
    interest_value: float,
    decay_half_life_seconds: float,
    tick_count: int,
    tick_interval_seconds: float,
) -> float:
    """Return interest after a fixed number of scheduler ticks."""

    if tick_count <= 0:
        return interest_value
    if decay_half_life_seconds <= 0.0:
        return 0.0
    elapsed = max(0.0, tick_interval_seconds) * tick_count
    return interest_value * (0.5 ** (elapsed / decay_half_life_seconds))


def calculate_bootstrap_correction(
    state: ActiveChatState,
    *,
    disposition: ActiveChatDisposition,
    config: ActiveChatPolicyConfig,
) -> ActiveChatBootstrapCorrection:
    """Calculate delayed bootstrap correction without overwriting runtime deltas."""

    preset = ACTIVE_CHAT_DISPOSITION_PRESETS[disposition]
    default_curve_interest = interest_curve_after_ticks(
        interest_value=config.initial_interest_value,
        decay_half_life_seconds=config.decay_half_life_seconds,
        tick_count=state.tick_count,
        tick_interval_seconds=config.tick_interval_seconds,
    )
    preset_curve_interest = interest_curve_after_ticks(
        interest_value=preset.interest_value,
        decay_half_life_seconds=preset.decay_half_life_seconds,
        tick_count=state.tick_count,
        tick_interval_seconds=config.tick_interval_seconds,
    )
    correction = preset_curve_interest - default_curve_interest
    interest_value = max(
        0.0,
        min(config.max_interest_value, state.interest_value + correction),
    )
    return ActiveChatBootstrapCorrection(
        disposition=disposition,
        default_curve_interest=default_curve_interest,
        preset_curve_interest=preset_curve_interest,
        correction=correction,
        interest_value=interest_value,
        decay_half_life_seconds=preset.decay_half_life_seconds,
    )


__all__ = [
    "ActiveChatPolicy",
    "ActiveChatBootstrapCorrection",
    "ActiveChatPolicyConfig",
    "ActiveChatPreset",
    "ACTIVE_CHAT_DISPOSITION_PRESETS",
    "calculate_bootstrap_correction",
    "DefaultActiveChatPolicy",
    "interest_curve_after_ticks",
]
