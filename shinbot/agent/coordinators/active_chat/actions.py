"""Active chat action-to-interest mapping."""

from __future__ import annotations

from dataclasses import dataclass

from shinbot.agent.coordinators.active_chat.models import (
    ActiveChatActionKind,
    ActiveChatNoReplyIntensity,
    ActiveChatReplyIntensity,
    ActiveChatRoundResult,
)


@dataclass(slots=True, frozen=True)
class ActiveChatInterestEffect:
    """Interest effect derived from one active chat action."""

    delta: float = 0.0
    force_exit: bool = False
    reason: str = ""


@dataclass(slots=True, frozen=True)
class ActiveChatInterestEffectConfig:
    """Interest deltas applied after one active-chat workflow round."""

    send_reply_delta: float = 10.0
    send_reply_low_delta: float = 5.0
    no_reply_delta: float = -5.0
    no_reply_strong_delta: float = -10.0
    send_reaction_delta: float = 2.0
    send_poke_delta: float = 3.0
    request_think_mode_delta: float = 6.0
    retry_failed_delta: float = -3.0


def interest_effect_for_round(
    result: ActiveChatRoundResult,
    config: ActiveChatInterestEffectConfig | None = None,
) -> ActiveChatInterestEffect:
    """Map one round result to an internal interest adjustment."""
    resolved_config = config or ActiveChatInterestEffectConfig()
    action = result.action
    if action == ActiveChatActionKind.NO_REPLY:
        if result.no_reply_intensity == ActiveChatNoReplyIntensity.STRONG:
            return ActiveChatInterestEffect(
                delta=resolved_config.no_reply_strong_delta,
                reason=result.reason,
            )
        return ActiveChatInterestEffect(delta=resolved_config.no_reply_delta, reason=result.reason)
    if action == ActiveChatActionKind.SEND_POKE:
        return ActiveChatInterestEffect(delta=resolved_config.send_poke_delta, reason=result.reason)
    if action == ActiveChatActionKind.SEND_REACTION:
        return ActiveChatInterestEffect(
            delta=resolved_config.send_reaction_delta,
            reason=result.reason,
        )
    if action == ActiveChatActionKind.SEND_REPLY:
        if result.reply_intensity == ActiveChatReplyIntensity.ENGAGED:
            return ActiveChatInterestEffect(
                delta=resolved_config.send_reply_delta,
                reason=result.reason,
            )
        return ActiveChatInterestEffect(
            delta=resolved_config.send_reply_low_delta,
            reason=result.reason,
        )
    if action == ActiveChatActionKind.REQUEST_THINK_MODE:
        return ActiveChatInterestEffect(
            delta=resolved_config.request_think_mode_delta,
            reason=result.reason,
        )
    if action == ActiveChatActionKind.EXIT_ACTIVE:
        return ActiveChatInterestEffect(force_exit=True, reason=result.reason)
    if action == ActiveChatActionKind.RETRY_FAILED:
        return ActiveChatInterestEffect(
            delta=resolved_config.retry_failed_delta,
            reason=result.reason,
        )
    return ActiveChatInterestEffect(delta=0.0, reason=result.reason)


__all__ = [
    "ActiveChatInterestEffect",
    "ActiveChatInterestEffectConfig",
    "interest_effect_for_round",
]
