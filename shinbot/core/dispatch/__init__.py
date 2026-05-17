"""Event and message dispatch primitives."""

from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    NOTICE_DISPATCHER_TARGET,
    AgentEntryDispatcher,
    AgentEntryHandler,
    AgentEntrySignal,
    NoticeDispatcher,
    make_agent_entry_fallback_route_rule,
    make_notice_route_rule,
)
from shinbot.core.dispatch.event_bus import EventBus, StopPropagation
from shinbot.core.dispatch.ingress import (
    MessageIngress,
    PreRouteHook,
    RouteDispatchContext,
    RouteTargetRegistry,
)
from shinbot.core.dispatch.message_context import MessageContext
from shinbot.core.dispatch.routing import (
    RouteCondition,
    RouteMatchContext,
    RouteMatchMode,
    RouteRule,
    RouteTable,
)

__all__ = [
    "EventBus",
    "MessageIngress",
    "StopPropagation",
    "MessageContext",
    "AGENT_ENTRY_TARGET",
    "NOTICE_DISPATCHER_TARGET",
    "AgentEntryDispatcher",
    "AgentEntryHandler",
    "AgentEntrySignal",
    "NoticeDispatcher",
    "PreRouteHook",
    "RouteDispatchContext",
    "RouteCondition",
    "RouteMatchContext",
    "RouteMatchMode",
    "RouteRule",
    "RouteTable",
    "RouteTargetRegistry",
    "make_agent_entry_fallback_route_rule",
    "make_notice_route_rule",
]
