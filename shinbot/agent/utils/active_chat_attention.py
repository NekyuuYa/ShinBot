"""Attention calculation for active chat."""

from __future__ import annotations

import math
from dataclasses import dataclass

from shinbot.agent.models.active_chat import ActiveChatAttentionState, ActiveChatMessageSignal


@dataclass(slots=True, frozen=True)
class ActiveChatAttentionConfig:
    """Tunable active chat attention parameters."""

    base_contribution: float = 1.0
    mention_contribution: float = 4.0
    mention_other_contribution: float = 0.5
    reply_to_bot_contribution: float = 3.0
    poke_self_contribution: float = 0.8
    poke_other_contribution: float = 0.2
    bot_self_contribution: float = 0.0
    contribution_decay_k: float = 0.003
    base_threshold: float = 5.0
    reference_interest: float = 30.0
    threshold_min: float = 2.0
    threshold_max: float = 15.0
    semantic_wait_ms: float = 800.0
    post_round_accumulated_multiplier: float = 0.25


class ActiveChatAttention:
    """Pure attention math for active chat."""

    def __init__(self, config: ActiveChatAttentionConfig | None = None) -> None:
        self.config = config or ActiveChatAttentionConfig()

    def contribution_for(self, signal: ActiveChatMessageSignal) -> float:
        """Return the short-term attention contribution for one message signal."""
        if signal.sender_id and signal.sender_id == signal.self_platform_id:
            return self.config.bot_self_contribution

        contributions: list[float] = []
        if signal.is_mentioned:
            contributions.append(self.config.mention_contribution)
        if signal.is_reply_to_bot:
            contributions.append(self.config.reply_to_bot_contribution)
        if signal.is_mention_to_other:
            contributions.append(self.config.mention_other_contribution)
        if signal.is_poke_to_bot:
            contributions.append(self.config.poke_self_contribution)
        if signal.is_poke_to_other:
            contributions.append(self.config.poke_other_contribution)
        if contributions:
            return max(contributions)
        return self.config.base_contribution

    def effective_threshold(self, interest_value: float) -> float:
        """Return the dynamic active chat attention threshold."""
        if interest_value <= 0:
            return self.config.threshold_max
        raw = self.config.base_threshold * (self.config.reference_interest / interest_value)
        return max(self.config.threshold_min, min(self.config.threshold_max, raw))

    def observe(
        self,
        state: ActiveChatAttentionState,
        signal: ActiveChatMessageSignal,
        *,
        now: float,
    ) -> ActiveChatAttentionState:
        """Apply decay and one message contribution to active chat attention."""
        elapsed = max(0.0, now - state.last_update_at) if state.last_update_at else 0.0
        decayed = state.accumulated * math.exp(-self.config.contribution_decay_k * elapsed)
        state.accumulated = decayed + self.contribution_for(signal)
        state.last_update_at = now
        state.last_sender_id = signal.sender_id
        state.pending_buffer.append(signal)
        return state

    def cool_after_round(self, state: ActiveChatAttentionState) -> ActiveChatAttentionState:
        """Reduce accumulated attention after one handled LLM round."""
        state.accumulated *= self.config.post_round_accumulated_multiplier
        return state


__all__ = ["ActiveChatAttention", "ActiveChatAttentionConfig"]
