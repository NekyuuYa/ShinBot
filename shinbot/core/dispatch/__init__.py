"""Command, event, and message dispatch primitives."""

from shinbot.core.dispatch.command import (
    CommandDef,
    CommandMatch,
    CommandMode,
    CommandPriority,
    CommandRegistry,
)
from shinbot.core.dispatch.dispatchers import (
    TEXT_COMMAND_DISPATCHER_TARGET,
    TextCommandDispatcher,
    make_text_command_route_rule,
)
from shinbot.core.dispatch.event_bus import EventBus, StopPropagation
from shinbot.core.dispatch.ingress import MessageIngress, RouteDispatchContext, RouteTargetRegistry
from shinbot.core.dispatch.pipeline import MessageContext, MessagePipeline
from shinbot.core.dispatch.routing import (
    RouteCondition,
    RouteMatchContext,
    RouteMatchMode,
    RouteRule,
    RouteTable,
)

__all__ = [
    "CommandDef",
    "CommandMatch",
    "CommandMode",
    "CommandPriority",
    "CommandRegistry",
    "EventBus",
    "MessageIngress",
    "StopPropagation",
    "MessageContext",
    "MessagePipeline",
    "TEXT_COMMAND_DISPATCHER_TARGET",
    "RouteDispatchContext",
    "RouteCondition",
    "RouteMatchContext",
    "RouteMatchMode",
    "RouteRule",
    "RouteTable",
    "RouteTargetRegistry",
    "TextCommandDispatcher",
    "make_text_command_route_rule",
]
