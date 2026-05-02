"""Built-in route target dispatchers."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from inspect import isawaitable
from typing import TYPE_CHECKING

from shinbot.core.bot_config import select_response_profile
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteDispatchContext
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule
from shinbot.schema.elements import Message
from shinbot.schema.events import UnifiedEvent

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager

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
    response_profile: str
    is_private: bool
    is_mentioned: bool
    is_reply_to_bot: bool
    already_handled: bool = False
    is_stopped: bool = False


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
        database: DatabaseManager | None = None,
    ) -> None:
        self._handler = handler
        self._database = database

    def set_handler(self, handler: AgentEntryHandler | None) -> None:
        """Set or clear the Agent-side signal handler."""
        self._handler = handler

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        bot = context.require_message_context()
        signal = AgentEntrySignal(
            session_id=bot.session_id,
            message_log_id=context.message_log_id,
            event_type=bot.event.type,
            sender_id=bot.event.sender_id or "",
            instance_id=bot.adapter.instance_id,
            platform=bot.event.platform,
            self_id=bot.event.self_id,
            response_profile=self._resolve_response_profile(bot),
            is_private=bot.is_private,
            is_mentioned=bot.is_mentioned,
            is_reply_to_bot=bot.is_reply_to_bot(),
            already_handled=bool(bot._sent_messages),
            is_stopped=bot.is_stopped,
        )

        if self._handler is not None:
            result = self._handler(signal)
            if isawaitable(result):
                await result

    def _resolve_response_profile(self, bot) -> str:
        if self._database is None:
            return select_response_profile(
                None,
                is_private=bot.is_private,
                is_mentioned=bot.is_mentioned,
                is_reply_to_bot=bot.is_reply_to_bot(),
            )

        bot_config = self._database.bot_configs.get_by_instance_id(bot.adapter.instance_id)
        return select_response_profile(
            bot_config,
            is_private=bot.is_private,
            is_mentioned=bot.is_mentioned,
            is_reply_to_bot=bot.is_reply_to_bot(),
        )

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
