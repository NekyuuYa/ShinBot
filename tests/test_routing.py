"""Tests for message route table matching."""

import logging

import pytest

from shinbot.core.dispatch.routing import (
    RouteCondition,
    RouteMatchContext,
    RouteMatchMode,
    RouteRule,
    RouteTable,
    collect_element_types,
)
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


def make_event(
    *,
    event_type: str = "message-created",
    platform: str = "mock",
    private: bool = True,
) -> UnifiedEvent:
    return UnifiedEvent(
        type=event_type,
        self_id="bot-1",
        platform=platform,
        user=User(id="user-1"),
        channel=Channel(id="private:user-1" if private else "group:1", type=1 if private else 0),
        message=MessagePayload(id="msg-1", content="hello")
        if event_type.startswith("message-")
        else None,
    )


def make_rule(
    rule_id: str,
    *,
    priority: int = 0,
    condition: RouteCondition | None = None,
    target: str | None = None,
    match_mode: RouteMatchMode = RouteMatchMode.NORMAL,
    enabled: bool = True,
) -> RouteRule:
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=condition or RouteCondition(),
        target=target or rule_id,
        match_mode=match_mode,
        enabled=enabled,
    )


def test_normal_routes_fan_out_in_stable_order() -> None:
    table = RouteTable()
    second = make_rule("second", priority=10)
    first = make_rule("first", priority=10)
    highest = make_rule("highest", priority=20)
    table.register(second)
    table.register(first)
    table.register(highest)

    assert table.match(make_event(), Message.from_text("hello")) == [highest, second, first]


def test_exclusive_route_wins_and_suppresses_other_matches() -> None:
    table = RouteTable()
    normal = make_rule("normal", priority=100)
    exclusive_low = make_rule(
        "exclusive-low",
        priority=10,
        match_mode=RouteMatchMode.EXCLUSIVE,
    )
    exclusive_high = make_rule(
        "exclusive-high",
        priority=20,
        match_mode=RouteMatchMode.EXCLUSIVE,
    )
    fallback = make_rule("fallback", match_mode=RouteMatchMode.FALLBACK)
    table.register(normal)
    table.register(exclusive_low)
    table.register(exclusive_high)
    table.register(fallback)

    assert table.match(make_event(), Message.from_text("hello")) == [exclusive_high]


def test_fallback_only_applies_to_its_declared_event_class() -> None:
    table = RouteTable()
    message_fallback = make_rule(
        "attention-fallback",
        priority=-1000,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        match_mode=RouteMatchMode.FALLBACK,
    )
    table.register(message_fallback)

    assert table.match(make_event(event_type="message-created"), Message.from_text("hello")) == [
        message_fallback
    ]
    assert table.match(make_event(event_type="guild-member-added"), Message()) == []


def test_fallback_is_ignored_when_normal_route_matches() -> None:
    table = RouteTable()
    normal = make_rule("normal")
    fallback = make_rule(
        "fallback",
        priority=1000,
        match_mode=RouteMatchMode.FALLBACK,
    )
    table.register(fallback)
    table.register(normal)

    assert table.match(make_event(), Message.from_text("hello")) == [normal]


def test_structured_conditions_and_wildcards_filter_candidates() -> None:
    table = RouteTable()
    platform_wildcard = make_rule(
        "platform-wildcard",
        condition=RouteCondition(
            event_types=frozenset({"message-created"}),
            platforms=None,
            is_private=True,
        ),
    )
    wrong_platform = make_rule(
        "wrong-platform",
        condition=RouteCondition(
            event_types=frozenset({"message-created"}),
            platforms=frozenset({"qq"}),
            is_private=True,
        ),
    )
    wrong_privacy = make_rule(
        "wrong-privacy",
        condition=RouteCondition(
            event_types=frozenset({"message-created"}),
            platforms=frozenset({"mock"}),
            is_private=False,
        ),
    )
    table.register(platform_wildcard)
    table.register(wrong_platform)
    table.register(wrong_privacy)

    assert table.match(make_event(platform="mock", private=True), Message.from_text("hello")) == [
        platform_wildcard
    ]


def test_element_types_are_collected_recursively() -> None:
    message = Message.from_elements(
        MessageElement.quote(
            "quoted",
            children=[
                MessageElement.img("https://example.test/image.png"),
                MessageElement(type="sb:ark"),
            ],
        )
    )
    table = RouteTable()
    img_route = make_rule(
        "img-route",
        condition=RouteCondition(element_types=frozenset({"img"})),
    )
    ark_route = make_rule(
        "ark-route",
        condition=RouteCondition(element_types=frozenset({"sb:ark"})),
    )
    video_route = make_rule(
        "video-route",
        condition=RouteCondition(element_types=frozenset({"video"})),
    )
    table.register(img_route)
    table.register(ark_route)
    table.register(video_route)

    assert collect_element_types(message) == frozenset({"quote", "img", "sb:ark"})
    assert table.match(make_event(), message) == [img_route, ark_route]


def test_custom_matcher_can_accept_or_reject() -> None:
    table = RouteTable()
    accepted = make_rule(
        "accepted",
        condition=RouteCondition(custom_matcher=lambda _event, message: message.text == "hello"),
    )
    rejected = make_rule(
        "rejected",
        condition=RouteCondition(custom_matcher=lambda _event, message: message.text == "bye"),
    )
    table.register(accepted)
    table.register(rejected)

    assert table.match(make_event(), Message.from_text("hello")) == [accepted]


def test_custom_matcher_exception_skips_only_that_rule(caplog: pytest.LogCaptureFixture) -> None:
    def broken_matcher(_event: UnifiedEvent, _message: Message) -> bool:
        raise RuntimeError("boom")

    table = RouteTable()
    broken = make_rule(
        "broken",
        priority=100,
        condition=RouteCondition(custom_matcher=broken_matcher),
    )
    healthy = make_rule("healthy")
    table.register(broken)
    table.register(healthy)

    with caplog.at_level(logging.ERROR, logger="shinbot.core.dispatch.routing"):
        assert table.match(make_event(), Message.from_text("hello")) == [healthy]

    assert "route_matcher_error: rule_id=broken target=broken" in caplog.text


def test_custom_matcher_can_receive_optional_match_context() -> None:
    table = RouteTable()
    contextual = make_rule(
        "contextual",
        condition=RouteCondition(
            custom_matcher=lambda _event, _message, context: context.session == "session-1"
        ),
    )
    table.register(contextual)

    assert table.match(
        make_event(),
        Message.from_text("hello"),
        RouteMatchContext(session="session-1"),
    ) == [contextual]
    assert table.match(
        make_event(),
        Message.from_text("hello"),
        RouteMatchContext(session="other-session"),
    ) == []


def test_disabled_rules_are_ignored() -> None:
    table = RouteTable()
    disabled = make_rule("disabled", priority=100, enabled=False)
    enabled = make_rule("enabled")
    table.register(disabled)
    table.register(enabled)

    assert table.match(make_event(), Message.from_text("hello")) == [enabled]


def test_register_rejects_duplicate_ids() -> None:
    table = RouteTable()
    table.register(make_rule("duplicate"))

    with pytest.raises(ValueError, match="already registered"):
        table.register(make_rule("duplicate"))


def test_unregister_removes_rule_from_indexes() -> None:
    table = RouteTable()
    rule = make_rule(
        "mock-only",
        condition=RouteCondition(platforms=frozenset({"mock"})),
    )
    table.register(rule)

    assert table.unregister("mock-only") is rule
    assert table.match(make_event(platform="mock"), Message.from_text("hello")) == []
