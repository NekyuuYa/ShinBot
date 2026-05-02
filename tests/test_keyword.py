"""Tests for keyword dispatch registry."""

import re

import pytest

from shinbot.core.dispatch.keyword import KeywordDef, KeywordRegistry


async def noop_handler(_ctx, _match) -> None:
    pass


def test_keyword_registry_matches_substring_case_insensitive() -> None:
    registry = KeywordRegistry()
    keyword = KeywordDef(pattern="Hello", handler=noop_handler)
    registry.register(keyword)

    matches = registry.match("well hello there")

    assert len(matches) == 1
    assert matches[0].keyword is keyword
    assert matches[0].matched_text == "hello"
    assert matches[0].start == 5
    assert matches[0].end == 10


def test_keyword_registry_can_match_case_sensitive() -> None:
    registry = KeywordRegistry()
    registry.register(KeywordDef(pattern="Hello", handler=noop_handler, ignore_case=False))

    assert registry.match("hello") == []
    assert len(registry.match("Hello")) == 1


def test_keyword_registry_matches_regex() -> None:
    registry = KeywordRegistry()
    keyword = KeywordDef(pattern=r"hi (?P<name>\w+)", handler=noop_handler, regex=True)
    registry.register(keyword)

    matches = registry.match("say hi Alice")

    assert len(matches) == 1
    assert matches[0].matched_text == "hi Alice"
    assert matches[0].regex_match is not None
    assert matches[0].regex_match.group("name") == "Alice"
    assert isinstance(keyword.compiled_pattern, re.Pattern)


def test_keyword_registry_orders_by_priority() -> None:
    registry = KeywordRegistry()
    low = KeywordDef(pattern="hello", handler=noop_handler, priority=100)
    high = KeywordDef(pattern="hello", handler=noop_handler, priority=10)
    registry.register(low)
    registry.register(high)

    assert [match.keyword for match in registry.match("hello")] == [high, low]


def test_keyword_registry_unregister_by_owner() -> None:
    registry = KeywordRegistry()
    registry.register(KeywordDef(pattern="a", handler=noop_handler, owner="p1"))
    registry.register(KeywordDef(pattern="b", handler=noop_handler, owner="p1"))
    registry.register(KeywordDef(pattern="c", handler=noop_handler, owner="p2"))

    assert registry.unregister_by_owner("p1") == 2
    assert [keyword.pattern for keyword in registry.all_keywords] == ["c"]


def test_keyword_registry_rejects_empty_pattern() -> None:
    registry = KeywordRegistry()

    with pytest.raises(ValueError, match="must not be empty"):
        registry.register(KeywordDef(pattern="", handler=noop_handler))
