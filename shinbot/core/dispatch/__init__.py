"""Command, event, and message dispatch primitives."""

from shinbot.core.dispatch.command import (
    CommandDef,
    CommandMatch,
    CommandMode,
    CommandPriority,
    CommandRegistry,
)
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    KEYWORD_DISPATCHER_TARGET,
    NOTICE_DISPATCHER_TARGET,
    TEXT_COMMAND_DISPATCHER_TARGET,
    AgentEntryDispatcher,
    KeywordDispatcher,
    NoticeDispatcher,
    TextCommandDispatcher,
    make_agent_entry_fallback_route_rule,
    make_keyword_route_rule,
    make_notice_route_rule,
    make_text_command_route_rule,
)
from shinbot.core.dispatch.event_bus import EventBus, StopPropagation
from shinbot.core.dispatch.ingress import MessageIngress, RouteDispatchContext, RouteTargetRegistry
from shinbot.core.dispatch.keyword import KeywordDef, KeywordMatch, KeywordRegistry
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
    "KeywordDef",
    "KeywordDispatcher",
    "KeywordMatch",
    "KeywordRegistry",
    "MessageContext",
    "MessagePipeline",
    "AGENT_ENTRY_TARGET",
    "KEYWORD_DISPATCHER_TARGET",
    "NOTICE_DISPATCHER_TARGET",
    "TEXT_COMMAND_DISPATCHER_TARGET",
    "AgentEntryDispatcher",
    "NoticeDispatcher",
    "RouteDispatchContext",
    "RouteCondition",
    "RouteMatchContext",
    "RouteMatchMode",
    "RouteRule",
    "RouteTable",
    "RouteTargetRegistry",
    "TextCommandDispatcher",
    "make_agent_entry_fallback_route_rule",
    "make_keyword_route_rule",
    "make_notice_route_rule",
    "make_text_command_route_rule",
]
