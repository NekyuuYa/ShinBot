"""Built-in route target dispatchers."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from inspect import isawaitable

from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteDispatchContext
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule
from shinbot.core.message_analysis import iter_message_elements
from shinbot.schema.elements import Message
from shinbot.schema.events import UnifiedEvent

logger = logging.getLogger(__name__)

NOTICE_DISPATCHER_TARGET = "notice_dispatcher"
AGENT_ENTRY_TARGET = "agent_entry"


@dataclass(slots=True, frozen=True)
class AgentEntrySignal:
    """Minimal signal emitted when an unmatched user message reaches Agent entry."""

    session_id: str
    message_log_id: int | None
    event_type: str
    sender_id: str
    instance_id: str
    platform: str
    self_id: str
    is_private: bool
    is_mentioned: bool
    is_reply_to_bot: bool
    is_mention_to_other: bool = False
    is_poke_to_bot: bool = False
    is_poke_to_other: bool = False
    already_handled: bool = False
    is_stopped: bool = False
    bot_id: str = ""
    bot_binding_id: str = ""


AgentEntryHandler = Callable[[AgentEntrySignal], Awaitable[None] | None]


class NoticeDispatcher:
    """Route target that forwards notice events to the internal EventBus."""

    def __init__(self, event_bus: EventBus) -> None:
        self._event_bus = event_bus

    def matches(self, event: UnifiedEvent, _message: Message) -> bool:
        return event.is_notice_event and self._event_bus.has_handlers(event.type)

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        await self._event_bus.emit(context.event.type, context.event)


def make_notice_route_rule(
    dispatcher: NoticeDispatcher,
    *,
    rule_id: str = "builtin.notice_dispatcher",
    priority: int = 1000,
) -> RouteRule:
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(custom_matcher=dispatcher.matches),
        target=NOTICE_DISPATCHER_TARGET,
        match_mode=RouteMatchMode.NORMAL,
    )


class AgentEntryDispatcher:
    """Route target that hands unmatched user messages to the Agent entry layer.

    The dispatcher emits a minimal signal describing why Agent was notified.
    Agent modules are responsible for reading message_logs and constructing
    their own internal context.
    """

    def __init__(
        self,
        *,
        handler: AgentEntryHandler | None = None,
    ) -> None:
        self._handler = handler

    def set_handler(self, handler: AgentEntryHandler | None) -> None:
        """Set or clear the Agent-side signal handler."""
        self._handler = handler

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        bot = context.require_message_context()
        signal = AgentEntrySignal(
            bot_id=bot.bot_id,
            bot_binding_id=bot.bot_binding_id,
            session_id=bot.session_id,
            message_log_id=context.message_log_id,
            event_type=bot.event.type,
            sender_id=bot.event.sender_id or "",
            instance_id=bot.adapter.instance_id,
            platform=bot.event.platform,
            self_id=bot.event.self_id,
            is_private=bot.is_private,
            is_mentioned=bot.is_mentioned,
            is_mention_to_other=_contains_mention_to_other(
                bot.message,
                bot.event.self_id,
            ),
            is_reply_to_bot=bot.is_reply_to_bot(),
            is_poke_to_bot=_contains_poke_to(
                bot.message,
                bot.event.self_id,
            ),
            is_poke_to_other=_contains_poke_to_other(
                bot.message,
                bot.event.self_id,
            ),
            already_handled=bool(bot._sent_messages),
            is_stopped=bot.is_stopped,
        )

        if self._handler is not None:
            result = self._handler(signal)
            if isawaitable(result):
                await result


def _contains_mention_to_other(message: Message, self_id: str) -> bool:
    self_id = str(self_id or "").strip()
    for element in iter_message_elements(message):
        if element.type != "at":
            continue
        target = str(element.attrs.get("id") or "").strip()
        if target and target != self_id:
            return True
    return False


def _contains_poke_to(message: Message, target_id: str) -> bool:
    target_id = str(target_id or "").strip()
    if not target_id:
        return False
    for element in iter_message_elements(message):
        if element.type != "sb:poke":
            continue
        target = str(element.attrs.get("target") or "").strip()
        if target == target_id:
            return True
    return False


def _contains_poke_to_other(message: Message, self_id: str) -> bool:
    self_id = str(self_id or "").strip()
    for element in iter_message_elements(message):
        if element.type != "sb:poke":
            continue
        target = str(element.attrs.get("target") or "").strip()
        if target and target != self_id:
            return True
    return False


def make_agent_entry_fallback_route_rule(
    *,
    rule_id: str = "builtin.agent_entry_fallback",
    priority: int = -1000,
) -> RouteRule:
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target=AGENT_ENTRY_TARGET,
        match_mode=RouteMatchMode.FALLBACK,
    )
