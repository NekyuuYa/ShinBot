"""Trigger strategies for attention-owned message scheduling."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol

from shinbot.schema.elements import Message, MessageElement

DISABLED_RESPONSE_PROFILES: frozenset[str] = frozenset(
    {
        "",
        "disabled",
        "disable",
        "off",
        "none",
    }
)


@dataclass(frozen=True, slots=True)
class AttentionTriggerContext:
    """Runtime context available to attention trigger strategies."""

    session_id: str
    msg_log_id: int
    sender_id: str
    response_profile: str
    message: Message
    self_platform_id: str = ""
    is_reply_to_bot: bool = False


class AttentionTriggerActions(Protocol):
    """Scheduler actions exposed to trigger strategies."""

    def accumulate_attention(
        self,
        context: AttentionTriggerContext,
        *,
        is_mentioned: bool = False,
        attention_multiplier: float = 1.0,
    ) -> None:
        """Schedule attention accumulation for the message."""

    def dispatch_immediately(self, context: AttentionTriggerContext) -> None:
        """Schedule direct workflow dispatch for the message's session."""


class AttentionTriggerStrategy(Protocol):
    """Decide whether and how the attention system should handle a message."""

    def schedule(
        self,
        context: AttentionTriggerContext,
        actions: AttentionTriggerActions,
    ) -> bool:
        """Return True after scheduling ownership of the message."""


class ResponseProfileAccumulationStrategy:
    """Default strategy: enabled response profiles update attention state."""

    def schedule(
        self,
        context: AttentionTriggerContext,
        actions: AttentionTriggerActions,
    ) -> bool:
        if not is_response_profile_enabled(context.response_profile):
            return False

        actions.accumulate_attention(
            context,
            is_mentioned=is_self_mentioned(context.message, context.self_platform_id),
            attention_multiplier=resolve_attention_multiplier(
                context.message,
                context.self_platform_id,
            ),
        )
        return True


class DisabledProfileDirectDispatchStrategy:
    """Default strategy: disabled response profiles bypass attention accumulation."""

    def schedule(
        self,
        context: AttentionTriggerContext,
        actions: AttentionTriggerActions,
    ) -> bool:
        if is_response_profile_enabled(context.response_profile):
            return False

        actions.dispatch_immediately(context)
        return True


def default_attention_trigger_strategies() -> tuple[AttentionTriggerStrategy, ...]:
    """Return the built-in attention trigger strategy chain."""

    return (
        ResponseProfileAccumulationStrategy(),
        DisabledProfileDirectDispatchStrategy(),
    )


def is_response_profile_enabled(response_profile: str) -> bool:
    return str(response_profile or "").strip().lower() not in DISABLED_RESPONSE_PROFILES


def iter_message_elements(message: Message) -> Iterator[MessageElement]:
    stack = list(message.elements)
    while stack:
        element = stack.pop()
        yield element
        stack.extend(element.children)


def is_self_mentioned(message: Message, self_platform_id: str) -> bool:
    self_platform_id = str(self_platform_id or "").strip()
    if not self_platform_id:
        return False
    return any(
        element.type == "at"
        and str(element.attrs.get("id", "") or "").strip() == self_platform_id
        for element in iter_message_elements(message)
    )


def resolve_attention_multiplier(message: Message, self_platform_id: str) -> float:
    self_platform_id = str(self_platform_id or "").strip()
    has_poke_self = False
    has_poke_other = False
    has_at_self = False
    has_at_other = False

    for element in iter_message_elements(message):
        if element.type == "sb:poke":
            target = str(element.attrs.get("target", "") or "").strip()
            if target and self_platform_id and target == self_platform_id:
                has_poke_self = True
            else:
                has_poke_other = True
        elif element.type == "at":
            target = str(element.attrs.get("id", "") or "").strip()
            if target and self_platform_id and target == self_platform_id:
                has_at_self = True
            elif target:
                has_at_other = True

    if has_poke_self:
        return 2.0
    if has_poke_other:
        return 0.2
    if has_at_other and not has_at_self:
        return 0.6
    return 1.0
