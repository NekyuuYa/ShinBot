"""Command, event, and message dispatch primitives."""

from shinbot.core.dispatch.command import (
    CommandDef,
    CommandMatch,
    CommandMode,
    CommandPriority,
    CommandRegistry,
)
from shinbot.core.dispatch.event_bus import EventBus, StopPropagation
from shinbot.core.dispatch.pipeline import MessageContext, MessagePipeline
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule, RouteTable

__all__ = [
    "CommandDef",
    "CommandMatch",
    "CommandMode",
    "CommandPriority",
    "CommandRegistry",
    "EventBus",
    "StopPropagation",
    "MessageContext",
    "MessagePipeline",
    "RouteCondition",
    "RouteMatchMode",
    "RouteRule",
    "RouteTable",
]
