"""Message route table primitives.

The route table is a pure matching layer: it decides which route targets should
receive a normalized event and parsed message, but it never dispatches them.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from inspect import Parameter, signature
from typing import Any

from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import UnifiedEvent

logger = logging.getLogger(__name__)

RouteMatcher = Callable[..., bool]


class RouteMatchMode(Enum):
    NORMAL = "normal"
    EXCLUSIVE = "exclusive"
    FALLBACK = "fallback"


@dataclass(slots=True, frozen=True)
class RouteMatchContext:
    """Optional runtime context for framework-owned route matchers."""

    adapter: Any | None = None
    session: Any | None = None
    message_context: Any | None = None


@dataclass(slots=True, frozen=True)
class RouteCondition:
    """Structured route condition with optional custom matching."""

    event_types: frozenset[str] | None = None
    element_types: frozenset[str] | None = None
    platforms: frozenset[str] | None = None
    is_private: bool | None = None
    custom_matcher: RouteMatcher | None = None


@dataclass(slots=True)
class RouteRule:
    """A route table rule pointing at a dispatcher target."""

    id: str
    priority: int
    condition: RouteCondition
    target: str
    match_mode: RouteMatchMode = RouteMatchMode.NORMAL
    enabled: bool = True


@dataclass(slots=True, frozen=True)
class _RouteEntry:
    rule: RouteRule
    registered_seq: int

    @property
    def sort_key(self) -> tuple[int, int, str]:
        return (-self.rule.priority, self.registered_seq, self.rule.id)


class RouteTable:
    """Indexed, deterministic route matcher."""

    def __init__(self) -> None:
        self._next_seq = 0
        self._entries_by_id: dict[str, _RouteEntry] = {}
        self._event_type_index: dict[str, set[str]] = defaultdict(set)
        self._event_type_wildcards: set[str] = set()
        self._element_type_index: dict[str, set[str]] = defaultdict(set)
        self._element_type_wildcards: set[str] = set()
        self._platform_index: dict[str, set[str]] = defaultdict(set)
        self._platform_wildcards: set[str] = set()
        self._private_index: dict[bool, set[str]] = defaultdict(set)
        self._private_wildcards: set[str] = set()

    def register(self, rule: RouteRule) -> None:
        """Register a route rule.

        Route ids are unique because the id is used for deterministic matching,
        audit logs, and future targeted unregister operations.
        """
        if not rule.id:
            raise ValueError("RouteRule.id must not be empty")
        if rule.id in self._entries_by_id:
            raise ValueError(f"RouteRule id already registered: {rule.id}")

        entry = _RouteEntry(rule=rule, registered_seq=self._next_seq)
        self._next_seq += 1
        self._entries_by_id[rule.id] = entry
        self._index_rule(rule)

    def unregister(self, rule_id: str) -> RouteRule | None:
        """Remove a rule by id and return it if present."""
        entry = self._entries_by_id.pop(rule_id, None)
        if entry is None:
            return None
        self._unindex_rule(entry.rule)
        return entry.rule

    def clear(self) -> None:
        """Remove all rules from the table."""
        self._next_seq = 0
        self._entries_by_id.clear()
        self._event_type_index.clear()
        self._event_type_wildcards.clear()
        self._element_type_index.clear()
        self._element_type_wildcards.clear()
        self._platform_index.clear()
        self._platform_wildcards.clear()
        self._private_index.clear()
        self._private_wildcards.clear()

    @property
    def rules(self) -> list[RouteRule]:
        """Registered rules in deterministic evaluation order."""
        return [entry.rule for entry in sorted(self._entries_by_id.values(), key=lambda e: e.sort_key)]

    def match(
        self,
        event: UnifiedEvent,
        message: Message,
        match_context: RouteMatchContext | None = None,
    ) -> list[RouteRule]:
        """Return the route rules selected for an event/message pair."""
        element_types = collect_element_types(message)
        candidate_ids = self._candidate_ids(event, element_types)

        matched_entries: list[_RouteEntry] = []
        for entry in sorted(
            (self._entries_by_id[rule_id] for rule_id in candidate_ids),
            key=lambda e: e.sort_key,
        ):
            rule = entry.rule
            if not rule.enabled:
                continue
            if self._matches_condition(rule, event, message, element_types, match_context):
                matched_entries.append(entry)

        exclusive = [
            entry.rule
            for entry in matched_entries
            if entry.rule.match_mode == RouteMatchMode.EXCLUSIVE
        ]
        if exclusive:
            return exclusive[:1]

        normal = [
            entry.rule
            for entry in matched_entries
            if entry.rule.match_mode == RouteMatchMode.NORMAL
        ]
        if normal:
            return normal

        fallback = [
            entry.rule
            for entry in matched_entries
            if entry.rule.match_mode == RouteMatchMode.FALLBACK
        ]
        return fallback[:1]

    def _candidate_ids(self, event: UnifiedEvent, element_types: frozenset[str]) -> set[str]:
        event_type_candidates = set(self._event_type_wildcards)
        event_type_candidates.update(self._event_type_index.get(event.type, set()))

        element_type_candidates = set(self._element_type_wildcards)
        for element_type in element_types:
            element_type_candidates.update(self._element_type_index.get(element_type, set()))

        platform_candidates = set(self._platform_wildcards)
        platform_candidates.update(self._platform_index.get(event.platform, set()))

        private_candidates = set(self._private_wildcards)
        private_candidates.update(self._private_index.get(event.is_private, set()))

        return (
            event_type_candidates
            & element_type_candidates
            & platform_candidates
            & private_candidates
        )

    def _matches_condition(
        self,
        rule: RouteRule,
        event: UnifiedEvent,
        message: Message,
        element_types: frozenset[str],
        match_context: RouteMatchContext | None,
    ) -> bool:
        condition = rule.condition
        if condition.event_types is not None and event.type not in condition.event_types:
            return False
        if condition.platforms is not None and event.platform not in condition.platforms:
            return False
        if condition.is_private is not None and event.is_private is not condition.is_private:
            return False
        if (
            condition.element_types is not None
            and condition.element_types.isdisjoint(element_types)
        ):
            return False
        if condition.custom_matcher is not None:
            try:
                return bool(_call_route_matcher(condition.custom_matcher, event, message, match_context))
            except Exception:
                logger.exception(
                    "route_matcher_error: rule_id=%s target=%s",
                    rule.id,
                    rule.target,
                )
                return False
        return True

    def _index_rule(self, rule: RouteRule) -> None:
        self._index_string_set(rule.id, rule.condition.event_types, self._event_type_index, self._event_type_wildcards)
        self._index_string_set(
            rule.id,
            rule.condition.element_types,
            self._element_type_index,
            self._element_type_wildcards,
        )
        self._index_string_set(rule.id, rule.condition.platforms, self._platform_index, self._platform_wildcards)
        if rule.condition.is_private is None:
            self._private_wildcards.add(rule.id)
        else:
            self._private_index[rule.condition.is_private].add(rule.id)

    def _unindex_rule(self, rule: RouteRule) -> None:
        self._unindex_string_set(
            rule.id,
            rule.condition.event_types,
            self._event_type_index,
            self._event_type_wildcards,
        )
        self._unindex_string_set(
            rule.id,
            rule.condition.element_types,
            self._element_type_index,
            self._element_type_wildcards,
        )
        self._unindex_string_set(
            rule.id,
            rule.condition.platforms,
            self._platform_index,
            self._platform_wildcards,
        )
        if rule.condition.is_private is None:
            self._private_wildcards.discard(rule.id)
        else:
            self._private_index[rule.condition.is_private].discard(rule.id)

    @staticmethod
    def _index_string_set(
        rule_id: str,
        values: frozenset[str] | None,
        index: dict[str, set[str]],
        wildcards: set[str],
    ) -> None:
        if values is None:
            wildcards.add(rule_id)
            return
        for value in values:
            index[value].add(rule_id)

    @staticmethod
    def _unindex_string_set(
        rule_id: str,
        values: frozenset[str] | None,
        index: dict[str, set[str]],
        wildcards: set[str],
    ) -> None:
        if values is None:
            wildcards.discard(rule_id)
            return
        for value in values:
            index[value].discard(rule_id)


def collect_element_types(message: Message) -> frozenset[str]:
    """Return all element types present anywhere in a message AST."""
    types: set[str] = set()
    for element in message.elements:
        _collect_element_type(element, types)
    return frozenset(types)


def _collect_element_type(element: MessageElement, types: set[str]) -> None:
    types.add(element.type)
    for child in element.children:
        _collect_element_type(child, types)


def _call_route_matcher(
    matcher: RouteMatcher,
    event: UnifiedEvent,
    message: Message,
    match_context: RouteMatchContext | None,
) -> bool:
    if _accepts_match_context(matcher):
        return matcher(event, message, match_context)
    return matcher(event, message)


def _accepts_match_context(matcher: RouteMatcher) -> bool:
    try:
        params = signature(matcher).parameters.values()
    except (TypeError, ValueError):
        return False
    positional_count = 0
    for param in params:
        if param.kind == Parameter.VAR_POSITIONAL:
            return True
        if param.kind in {
            Parameter.POSITIONAL_ONLY,
            Parameter.POSITIONAL_OR_KEYWORD,
        }:
            positional_count += 1
    return positional_count >= 3
