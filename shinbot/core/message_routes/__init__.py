"""Built-in message route registries and dispatch targets."""

from shinbot.core.message_routes.command import (
    TEXT_COMMAND_DISPATCHER_TARGET,
    CommandDef,
    CommandHandler,
    CommandMatch,
    CommandMode,
    CommandPriority,
    CommandRegistry,
    TextCommandDispatcher,
    make_text_command_route_rule,
)
from shinbot.core.message_routes.keyword import (
    KEYWORD_DISPATCHER_TARGET,
    KeywordDef,
    KeywordDispatcher,
    KeywordHandler,
    KeywordMatch,
    KeywordRegistry,
    make_keyword_route_rule,
)

__all__ = [
    "CommandDef",
    "CommandHandler",
    "CommandMatch",
    "CommandMode",
    "CommandPriority",
    "CommandRegistry",
    "KEYWORD_DISPATCHER_TARGET",
    "KeywordDef",
    "KeywordDispatcher",
    "KeywordHandler",
    "KeywordMatch",
    "KeywordRegistry",
    "TEXT_COMMAND_DISPATCHER_TARGET",
    "TextCommandDispatcher",
    "make_keyword_route_rule",
    "make_text_command_route_rule",
]
