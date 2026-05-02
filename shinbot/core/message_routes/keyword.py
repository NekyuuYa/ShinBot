"""Keyword routes — registration, matching, and dispatch target."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule
from shinbot.core.state.session import SessionManager
from shinbot.schema.elements import Message
from shinbot.schema.events import UnifiedEvent

if TYPE_CHECKING:
    from shinbot.core.dispatch.ingress import RouteDispatchContext

logger = logging.getLogger(__name__)

KeywordHandler = Callable[..., Coroutine[Any, Any, Any]]
KEYWORD_DISPATCHER_TARGET = "keyword_dispatcher"


@dataclass(slots=True)
class KeywordDef:
    """A registered keyword trigger."""

    pattern: str
    handler: KeywordHandler
    priority: int = 100
    ignore_case: bool = True
    regex: bool = False
    owner: str | None = None
    compiled_pattern: re.Pattern | None = field(default=None, repr=False)


@dataclass(slots=True)
class KeywordMatch:
    """Result of keyword matching."""

    keyword: KeywordDef
    matched_text: str
    start: int
    end: int
    regex_match: re.Match | None = None


class KeywordRegistry:
    """Registry for simple high-frequency keyword triggers."""

    def __init__(self) -> None:
        self._keywords: list[KeywordDef] = []

    def register(self, keyword: KeywordDef) -> None:
        if not keyword.pattern:
            raise ValueError("Keyword pattern must not be empty")
        if keyword.regex and keyword.compiled_pattern is None:
            flags = re.IGNORECASE if keyword.ignore_case else 0
            keyword.compiled_pattern = re.compile(keyword.pattern, flags)
        self._keywords.append(keyword)
        self._keywords.sort(key=lambda item: item.priority)

    def unregister(self, keyword: KeywordDef) -> bool:
        before = len(self._keywords)
        self._keywords = [item for item in self._keywords if item is not keyword]
        return len(self._keywords) != before

    def unregister_by_owner(self, owner: str) -> int:
        before = len(self._keywords)
        self._keywords = [item for item in self._keywords if item.owner != owner]
        return before - len(self._keywords)

    @property
    def all_keywords(self) -> list[KeywordDef]:
        return list(self._keywords)

    def match(self, text: str) -> list[KeywordMatch]:
        if not text:
            return []

        matches: list[KeywordMatch] = []
        for keyword in self._keywords:
            match = self._match_one(keyword, text)
            if match is not None:
                matches.append(match)
        return matches

    def _match_one(self, keyword: KeywordDef, text: str) -> KeywordMatch | None:
        if keyword.regex:
            pattern = keyword.compiled_pattern
            if pattern is None:
                flags = re.IGNORECASE if keyword.ignore_case else 0
                pattern = re.compile(keyword.pattern, flags)
                keyword.compiled_pattern = pattern
            regex_match = pattern.search(text)
            if regex_match is None:
                return None
            return KeywordMatch(
                keyword=keyword,
                matched_text=regex_match.group(0),
                start=regex_match.start(),
                end=regex_match.end(),
                regex_match=regex_match,
            )

        haystack = text.lower() if keyword.ignore_case else text
        needle = keyword.pattern.lower() if keyword.ignore_case else keyword.pattern
        index = haystack.find(needle)
        if index < 0:
            return None
        end = index + len(keyword.pattern)
        return KeywordMatch(
            keyword=keyword,
            matched_text=text[index:end],
            start=index,
            end=end,
        )


class KeywordDispatcher:
    """Route target that executes registered keyword handlers."""

    def __init__(
        self,
        keyword_registry: KeywordRegistry,
        *,
        session_manager: SessionManager | None = None,
    ) -> None:
        self._keyword_registry = keyword_registry
        self._session_manager = session_manager

    def matches(self, event: UnifiedEvent, message: Message) -> bool:
        if not event.is_message_event:
            return False
        return bool(self._keyword_registry.match(message.get_text(self_id=event.self_id)))

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        bot = context.require_message_context()
        matches = self._keyword_registry.match(bot.text)
        for match in matches:
            if bot.is_stopped:
                break
            try:
                handler_result = await match.keyword.handler(bot, match)
                if handler_result is not None:
                    logger.warning(
                        "Keyword handler %s returned a value that was ignored; use bot.send()",
                        match.keyword.pattern,
                    )
            except Exception:
                logger.exception("Keyword handler error: %s", match.keyword.pattern)

        if self._session_manager is not None:
            self._session_manager.update(bot.session)


def make_keyword_route_rule(
    dispatcher: KeywordDispatcher,
    *,
    rule_id: str = "builtin.keyword_dispatcher",
    priority: int = 900,
) -> RouteRule:
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(
            event_types=frozenset({"message-created"}),
            custom_matcher=dispatcher.matches,
        ),
        target=KEYWORD_DISPATCHER_TARGET,
        match_mode=RouteMatchMode.NORMAL,
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
