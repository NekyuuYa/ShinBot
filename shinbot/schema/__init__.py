"""ShinBot protocol schema — message AST, unified events, and related types."""

from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import (
    MessagePayload,
    UnifiedEvent,
)
from shinbot.schema.resources import (
    Channel,
    Guild,
    Login,
    Member,
    User,
)
from shinbot.schema.routing import MessageRoutingSkipReason, MessageRoutingStatus

__all__ = [
    "MessageElement",
    "Message",
    "UnifiedEvent",
    "User",
    "Channel",
    "Guild",
    "Member",
    "Login",
    "MessagePayload",
    "MessageRoutingSkipReason",
    "MessageRoutingStatus",
]
