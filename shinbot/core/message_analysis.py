"""Message inspection helpers shared by core and upper layers."""

from __future__ import annotations

from collections.abc import Iterator

from shinbot.schema.elements import Message, MessageElement


def iter_message_elements(message: Message) -> Iterator[MessageElement]:
    stack = list(message.elements)
    while stack:
        element = stack.pop()
        yield element
        stack.extend(element.children)


def is_self_mentioned(message: Message, self_platform_id: str) -> bool:
    self_platform_id = str(self_platform_id or "").strip()
    if not self_platform_id:
        return False
    return any(
        element.type == "at"
        and str(element.attrs.get("id", "") or "").strip() == self_platform_id
        for element in iter_message_elements(message)
    )
