"""Active chat action-to-interest mapping."""

from __future__ import annotations

from dataclasses import dataclass

from shinbot.agent.models.active_chat import (
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


def interest_effect_for_round(result: ActiveChatRoundResult) -> ActiveChatInterestEffect:
    """Map one round result to an internal interest adjustment."""
    action = result.action
    if action == ActiveChatActionKind.NO_REPLY:
        if result.no_reply_intensity == ActiveChatNoReplyIntensity.STRONG:
            return ActiveChatInterestEffect(delta=-10.0, reason=result.reason)
        return ActiveChatInterestEffect(delta=-5.0, reason=result.reason)
    if action == ActiveChatActionKind.SEND_POKE:
        return ActiveChatInterestEffect(delta=3.0, reason=result.reason)
    if action == ActiveChatActionKind.SEND_REPLY:
        if result.reply_intensity == ActiveChatReplyIntensity.ENGAGED:
            return ActiveChatInterestEffect(delta=10.0, reason=result.reason)
        return ActiveChatInterestEffect(delta=5.0, reason=result.reason)
    if action == ActiveChatActionKind.REQUEST_THINK_MODE:
        return ActiveChatInterestEffect(delta=6.0, reason=result.reason)
    if action == ActiveChatActionKind.EXIT_ACTIVE:
        return ActiveChatInterestEffect(force_exit=True, reason=result.reason)
    if action == ActiveChatActionKind.RETRY_FAILED:
        return ActiveChatInterestEffect(delta=-3.0, reason=result.reason)
    return ActiveChatInterestEffect(delta=0.0, reason=result.reason)


__all__ = ["ActiveChatInterestEffect", "interest_effect_for_round"]
