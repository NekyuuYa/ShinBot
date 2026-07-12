"""Built-in route target dispatchers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from inspect import isawaitable

from shinbot.core.dispatch.agent_delivery import (
    AgentRouteDelivery,
    MissingAgentMessageLogId,
)
from shinbot.core.dispatch.agent_identity import SessionKeyFactory
from shinbot.core.dispatch.agent_signals import (
    AgentSignal,
)
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteDispatchContext
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule
from shinbot.core.message_analysis import iter_message_elements
from shinbot.schema.elements import Message
from shinbot.schema.events import UnifiedEvent
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="dispatch", color="cyan")

NOTICE_DISPATCHER_TARGET = "notice_dispatcher"
AGENT_ENTRY_TARGET = "agent_entry"

AgentSignalHandler = Callable[[AgentSignal], Awaitable[None] | None]


class NoticeDispatcher:
    """Route target that forwards notice events to the internal EventBus."""

    def __init__(self, event_bus: EventBus) -> None:
        self._event_bus = event_bus

    def matches(self, event: UnifiedEvent, _message: Message) -> bool:
        """Return True if this dispatcher should handle the given event."""
        return event.is_notice_event and self._event_bus.has_handlers(event.type)

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        await self._event_bus.emit(context.event.type, context.event)


def make_notice_route_rule(
    dispatcher: NoticeDispatcher,
    *,
    rule_id: str = "builtin.notice_dispatcher",
    priority: int = 1000,
) -> RouteRule:
    """Create a built-in route rule that forwards notice events to the EventBus.

    Args:
        dispatcher: The ``NoticeDispatcher`` instance used as the custom matcher.
        rule_id: Unique identifier for the generated route rule.
        priority: Evaluation priority (lower wins when competing rules match).

    Returns:
        A ``RouteRule`` targeting the notice dispatcher.
    """
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(custom_matcher=dispatcher.matches),
        target=NOTICE_DISPATCHER_TARGET,
        match_mode=RouteMatchMode.NORMAL,
    )


class AgentEntryDispatcher:
    """Route target that hands unmatched user messages to the Agent signal layer."""

    def __init__(
        self,
        *,
        handler: AgentSignalHandler | None = None,
        key_factory: SessionKeyFactory | None = None,
    ) -> None:
        self._handler = handler
        self._key_factory = key_factory or SessionKeyFactory()

    def set_handler(self, handler: AgentSignalHandler | None) -> None:
        """Set or clear the Agent-side signal handler."""
        self._handler = handler

    def prepare_delivery(
        self,
        context: RouteDispatchContext,
        rule: RouteRule,
    ) -> AgentRouteDelivery:
        """Build one actor-eligible delivery from a matched Agent route.

        Raises:
            MissingAgentMessageLogId: If ingress did not durably persist the
                message before reaching the Agent route.
        """

        return self._build_delivery(context, rule).require_actor_delivery()

    async def __call__(self, context: RouteDispatchContext, rule: RouteRule) -> None:
        try:
            delivery = self.prepare_delivery(context, rule)
        except MissingAgentMessageLogId:
            delivery = self._build_delivery(context, rule)
            logger.warning(
                format_log_event(
                    "agent.delivery.rejected",
                    reason="message_log_id_missing",
                    session_id=delivery.base_session_id,
                    bot_id=delivery.bot_id,
                    binding_id=delivery.bot_binding_id,
                    trace_id=delivery.trace_id,
                    compatibility_signal=True,
                )
            )
        signal = delivery.to_agent_signal()

        logger.debug(
            format_log_event(
                "agent.signal.created",
                signal_id=signal.signal_id,
                session_id=signal.session_id,
                bot_id=signal.bot_id,
                binding_id=signal.bot_binding_id,
                message_log_id=context.message_log_id,
                trace_id=context.trace_id,
                is_private=signal.message.is_private if signal.message is not None else None,
                is_mentioned=(
                    signal.message.is_mentioned if signal.message is not None else None
                ),
                is_reply_to_bot=(
                    signal.message.is_reply_to_bot if signal.message is not None else None
                ),
            )
        )
        if self._handler is None:
            logger.warning(
                format_log_event(
                    "agent.signal.dropped",
                    reason="handler_missing",
                    signal_id=signal.signal_id,
                    session_id=signal.session_id,
                    bot_id=signal.bot_id,
                    message_log_id=context.message_log_id,
                    trace_id=context.trace_id,
                )
            )
            return
        result = self._handler(signal)
        if isawaitable(result):
            await result

    def _build_delivery(
        self,
        context: RouteDispatchContext,
        rule: RouteRule,
    ) -> AgentRouteDelivery:
        bot = context.require_message_context()
        session_key = self._key_factory.create(
            bot_config_id=bot.bot_id,
            bot_id=bot.bot_id,
            bot_session_id=bot.bot_session_id,
            base_session_id=bot.session_id,
        )
        return AgentRouteDelivery(
            session_key=session_key,
            bot_id=bot.bot_id,
            bot_binding_id=bot.bot_binding_id,
            bot_session_id=bot.bot_session_id,
            base_session_id=bot.session_id,
            message_log_id=context.message_log_id,
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
            trace_id=context.trace_id,
            observed_at=context.observed_at,
            event_type=bot.event.type,
            route_rule_id=rule.id,
        )


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
    """Create a fallback route rule that sends unmatched messages to the agent.

    Args:
        rule_id: Unique identifier for the generated route rule.
        priority: Evaluation priority.  The default is deliberately low so that
            this rule only fires when no normal or exclusive rule matched.

    Returns:
        A ``RouteRule`` targeting the agent entry dispatcher.
    """
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target=AGENT_ENTRY_TARGET,
        match_mode=RouteMatchMode.FALLBACK,
    )
