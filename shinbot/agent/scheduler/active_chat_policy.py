"""Active chat interest policy boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from shinbot.agent.scheduler.models import ActiveChatState


@dataclass(slots=True, frozen=True)
class ActiveChatPolicyConfig:
    """Defaults for the first active chat interest implementation."""

    initial_interest_value: float = 1.0
    decay_half_life_seconds: float = 300.0
    idle_interest_threshold: float = 0.05
    message_interest_delta: float = 0.1
    mention_interest_delta: float = 0.4
    reply_interest_delta: float = 0.3
    max_interest_value: float = 3.0


class ActiveChatPolicy(Protocol):
    """Maintain active chat interest state."""

    def initial_state(
        self,
        *,
        session_id: str,
        now: float,
        initial_interest_value: float | None = None,
    ) -> ActiveChatState:
        """Build the initial active chat interest state."""

    def decay(self, state: ActiveChatState, *, now: float) -> ActiveChatState:
        """Apply natural interest decay."""

    def observe_message(
        self,
        state: ActiveChatState,
        *,
        now: float,
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
    ) -> ActiveChatState:
        """Apply message-driven interest changes."""

    def should_return_idle(self, state: ActiveChatState) -> bool:
        """Return whether decayed interest is low enough to leave active chat."""


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
    ) -> ActiveChatState:
        return ActiveChatState(
            session_id=session_id,
            interest_value=(
                self._config.initial_interest_value
                if initial_interest_value is None
                else initial_interest_value
            ),
            decay_half_life_seconds=self._config.decay_half_life_seconds,
            entered_at=now,
            updated_at=now,
        )

    def decay(self, state: ActiveChatState, *, now: float) -> ActiveChatState:
        elapsed = max(0.0, now - state.updated_at)
        if elapsed == 0.0:
            return state
        if state.decay_half_life_seconds <= 0.0:
            interest_value = 0.0
        else:
            interest_value = state.interest_value * (
                0.5 ** (elapsed / state.decay_half_life_seconds)
            )
        return ActiveChatState(
            session_id=state.session_id,
            interest_value=interest_value,
            decay_half_life_seconds=state.decay_half_life_seconds,
            entered_at=state.entered_at,
            updated_at=now,
        )

    def observe_message(
        self,
        state: ActiveChatState,
        *,
        now: float,
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
    ) -> ActiveChatState:
        decayed = self.decay(state, now=now)
        delta = self._config.message_interest_delta
        if is_mentioned:
            delta += self._config.mention_interest_delta
        if is_reply_to_bot:
            delta += self._config.reply_interest_delta
        return ActiveChatState(
            session_id=decayed.session_id,
            interest_value=min(
                self._config.max_interest_value,
                decayed.interest_value + delta,
            ),
            decay_half_life_seconds=decayed.decay_half_life_seconds,
            entered_at=decayed.entered_at,
            updated_at=now,
        )

    def should_return_idle(self, state: ActiveChatState) -> bool:
        return state.interest_value <= self._config.idle_interest_threshold


__all__ = [
    "ActiveChatPolicy",
    "ActiveChatPolicyConfig",
    "DefaultActiveChatPolicy",
]
