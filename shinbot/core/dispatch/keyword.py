"""Compatibility re-export for keyword message routes."""

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
    "KEYWORD_DISPATCHER_TARGET",
    "KeywordDef",
    "KeywordDispatcher",
    "KeywordHandler",
    "KeywordMatch",
    "KeywordRegistry",
    "make_keyword_route_rule",
]
