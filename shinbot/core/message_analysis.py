"""Message inspection helpers shared by core and upper layers."""

from __future__ import annotations

from collections.abc import Iterator

from shinbot.schema.elements import Message, MessageElement


def iter_message_elements(message: Message) -> Iterator[MessageElement]:
    """Iterate over all elements in a message, including nested children.

    Performs a depth-first traversal of the message element tree using an
    explicit stack, yielding each element in post-order.

    Args:
        message: The message container whose elements should be traversed.

    Yields:
        Each MessageElement in the tree, including children.
    """
    stack = list(message.elements)
    while stack:
        element = stack.pop()
        yield element
        stack.extend(element.children)


def is_self_mentioned(message: Message, self_platform_id: str) -> bool:
    """Check whether the bot is directly mentioned in the message.

    Scans for ``<at>`` elements whose ``id`` attribute matches the given
    platform-specific bot identifier.

    Args:
        message: The message to inspect.
        self_platform_id: The bot's platform-specific user ID.

    Returns:
        True if an ``<at>`` element targeting the bot is found.
    """
    self_platform_id = str(self_platform_id or "").strip()
    if not self_platform_id:
        return False
    return any(
        element.type == "at"
        and str(element.attrs.get("id", "") or "").strip() == self_platform_id
        for element in iter_message_elements(message)
    )
