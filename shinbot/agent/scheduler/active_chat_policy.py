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
    mention_other_interest_delta: float = 0.0
    reply_interest_delta: float = 5.0
    poke_interest_delta: float = 0.0
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
        is_from_bot: bool = False,
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
        is_mention_to_other: bool = False,
        is_poke_to_bot: bool = False,
        is_poke_to_other: bool = False,
    ) -> ActiveChatState:
        """Apply message-driven interest changes."""

    def should_return_idle(self, state: ActiveChatState) -> bool:
        """Return whether decayed interest is low enough to leave active chat."""

    def adjust_interest(
        self,
        state: ActiveChatState,
        *,
        delta: float,
        now: float,
        force_exit: bool = False,
    ) -> ActiveChatState:
        """Apply workflow-driven interest adjustment."""

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
        """Initialize the default active chat policy.

        Args:
            config: Policy configuration. Uses ``ActiveChatPolicyConfig``
                defaults when *None*.
        """
        self._config = config or ActiveChatPolicyConfig()

    def initial_state(
        self,
        *,
        session_id: str,
        now: float,
        initial_interest_value: float | None = None,
        decay_half_life_seconds: float | None = None,
    ) -> ActiveChatState:
        """Create the initial active chat interest state.

        The returned state is clamped to ``[0, max_interest_value]`` and
        stamped with the current timestamp.

        Args:
            session_id: Unique identifier for the conversation session.
            now: Current Unix timestamp in seconds.
            initial_interest_value: Override for the starting interest
                level.  Falls back to the configured default.
            decay_half_life_seconds: Override for the decay half-life.
                Falls back to the configured default.

        Returns:
            A new ``ActiveChatState`` with the initial interest curve.
        """
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
        """Apply exponential interest decay from the last update.

        When *count_tick* is ``True`` the elapsed time is replaced by the
        configured ``tick_interval_seconds`` so that fixed-interval
        scheduler ticks produce deterministic decay regardless of wall
        clock drift.

        Args:
            state: Current active chat state.
            now: Current Unix timestamp in seconds.
            count_tick: If ``True``, use the configured tick interval
                instead of real elapsed time for the decay calculation.

        Returns:
            A new ``ActiveChatState`` with the decayed interest value.
        """
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
        is_from_bot: bool = False,
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
        is_mention_to_other: bool = False,
        is_poke_to_bot: bool = False,
        is_poke_to_other: bool = False,
    ) -> ActiveChatState:
        """Update interest based on an incoming message event.

        The interest delta is determined by message features (mentions,
        replies, pokes) and capped at ``max_interest_value``.  Messages
        from the bot itself produce no interest change.

        Args:
            state: Current active chat state.
            now: Current Unix timestamp in seconds.
            is_from_bot: ``True`` if the message was sent by the bot.
            is_mentioned: ``True`` if the bot is @-mentioned.
            is_reply_to_bot: ``True`` if the message replies to a bot
                message.
            is_mention_to_other: ``True`` if the bot is @-mentioned
                alongside another user.
            is_poke_to_bot: ``True`` if the message pokes the bot.
            is_poke_to_other: ``True`` if the message pokes another
                user.

        Returns:
            A new ``ActiveChatState`` with the adjusted interest value.
        """
        delta = self._message_interest_delta(
            is_from_bot=is_from_bot,
            is_mentioned=is_mentioned,
            is_reply_to_bot=is_reply_to_bot,
            is_mention_to_other=is_mention_to_other,
            is_poke_to_bot=is_poke_to_bot,
            is_poke_to_other=is_poke_to_other,
        )
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

    def _message_interest_delta(
        self,
        *,
        is_from_bot: bool,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        is_mention_to_other: bool,
        is_poke_to_bot: bool,
        is_poke_to_other: bool,
    ) -> float:
        if is_from_bot:
            return 0.0
        if is_mentioned:
            return self._config.mention_interest_delta
        if is_reply_to_bot:
            return self._config.reply_interest_delta
        if is_poke_to_bot or is_poke_to_other:
            return self._config.poke_interest_delta
        if is_mention_to_other:
            return self._config.mention_other_interest_delta
        return self._config.message_interest_delta

    def should_return_idle(self, state: ActiveChatState) -> bool:
        """Check whether the session should transition to idle.

        Returns ``True`` when the current interest value has fallen to
        or below the configured ``idle_interest_threshold`` (with a
        small floating-point tolerance).

        Args:
            state: Current active chat state.

        Returns:
            ``True`` if interest is low enough to leave active chat.
        """
        return state.interest_value <= self._config.idle_interest_threshold + 1e-9

    def adjust_interest(
        self,
        state: ActiveChatState,
        *,
        delta: float,
        now: float,
        force_exit: bool = False,
    ) -> ActiveChatState:
        """Manually adjust interest level (e.g. from workflow actions).

        When *force_exit* is ``True`` the interest is set to zero
        regardless of *delta*.

        Args:
            state: Current active chat state.
            delta: Amount to add to (or subtract from) the interest
                value.  Ignored when *force_exit* is ``True``.
            now: Current Unix timestamp in seconds.
            force_exit: If ``True``, force interest to zero.

        Returns:
            A new ``ActiveChatState`` with the adjusted interest value.
        """
        if force_exit:
            interest_value = 0.0
        else:
            interest_value = max(
                0.0,
                min(self._config.max_interest_value, state.interest_value + delta),
            )
        return ActiveChatState(
            session_id=state.session_id,
            interest_value=interest_value,
            decay_half_life_seconds=state.decay_half_life_seconds,
            entered_at=state.entered_at,
            updated_at=now,
            tick_count=state.tick_count,
            active_epoch=state.active_epoch,
            bootstrap_applied=state.bootstrap_applied,
            bootstrap_disposition=state.bootstrap_disposition,
        )

    def apply_bootstrap_disposition(
        self,
        state: ActiveChatState,
        *,
        disposition: ActiveChatDisposition,
        now: float,
    ) -> ActiveChatState:
        """Apply a delayed bootstrap disposition as a curve correction.

        The correction is calculated by comparing the interest that
        would have resulted from the disposition preset versus the
        default curve, preserving any runtime deltas accumulated
        since session start.

        Args:
            state: Current active chat state.
            disposition: Target disposition to bootstrap towards.
            now: Current Unix timestamp in seconds.

        Returns:
            A new ``ActiveChatState`` with corrected interest and
            decay parameters, and ``bootstrap_applied`` set to
            ``True``.
        """
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
