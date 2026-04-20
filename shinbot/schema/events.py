"""UnifiedEvent and related Satori protocol models.

Defines the canonical event representation used throughout ShinBot.
Modeled after the Satori protocol event schema with fields observed
in real Satori WebSocket payloads (see .agent/test/satori_events.jsonl).

After adapter ingress, every event is normalized into UnifiedEvent.
The dual-track design separates message events (with content to parse)
from notice events (with structured resource payloads).

Reference: docs/design/17_resource_schema_spec.md
"""

from __future__ import annotations

from pydantic import BaseModel

from shinbot.schema.resources import Channel, Guild, Login, Member, User  # noqa: F401


class MessagePayload(BaseModel):
    """The message sub-object within a message-created event.

    The `content` field contains the raw Satori XML string which
    gets parsed into MessageElement AST by the pipeline.
    """

    id: str
    content: str = ""
    created_at: int | None = None

    model_config = {"extra": "allow"}


class UnifiedEvent(BaseModel):
    """Canonical event representation used throughout the ShinBot pipeline.

    Implements the dual-track design for separating message events from notice events:

    **Message Events** (e.g., 'message-created'):
      - Payload: message content (XML string to parse)
      - User: sender identity
      - Channel/Guild: context where message was sent
      - Pipeline: → MessageElement AST parsing → command resolution

    **Notice Events** (e.g., 'guild-member-added', 'friend-request'):
      - Payload: structured resources (guild, user, operator, etc.)
      - NO message parsing required
      - Pipeline: → EventBus dispatch → notice handlers

    Wire format fields (from Satori WebSocket):
      - op: opcode (0 = EVENT, 4 = READY)
      - body: the event payload (this model represents the body)
    """

    id: int | None = None
    sn: int | None = None
    type: str
    """Event type string (e.g., 'message-created', 'guild-member-added')."""

    self_id: str = ""
    """The bot's own platform ID."""

    platform: str = ""
    """Platform name (QQ, Discord, Telegram, etc.)."""

    timestamp: int | None = None
    """Unix timestamp when the event occurred."""

    # Resource fields (notice/ingress payload)
    login: Login | None = None
    user: User | None = None
    """Primary user entity (sender for message, affected user for notice)."""

    operator: User | None = None
    """Operator user for notice events (e.g., who kicked a member)."""

    member: Member | None = None
    """Guild member entity (for notice events like member-added)."""

    channel: Channel | None = None
    """Channel context (where message was sent, or for channel events)."""

    guild: Guild | None = None
    """Guild/group context."""

    # Message event payload (only populated for message-created, etc.)
    message: MessagePayload | None = None

    model_config = {"extra": "allow", "populate_by_name": True}

    # ── Event type classification ────────────────────────────────────

    @property
    def is_message_event(self) -> bool:
        """Check if this is a message event requiring content parsing."""
        return self.type in ("message-created", "message-updated", "message-deleted")

    @property
    def is_notice_event(self) -> bool:
        """Check if this is a notice event (guild-member-*, friend-*, etc.)."""
        return not self.is_message_event

    def has_resource(self, resource_type: str) -> bool:
        """Check if event has a specific resource type populated.

        Args:
            resource_type: One of 'user', 'guild', 'channel', 'member', 'operator'

        Returns:
            True if the resource field is not None.
        """
        match resource_type:
            case "user":
                return self.user is not None
            case "guild":
                return self.guild is not None
            case "channel":
                return self.channel is not None
            case "member":
                return self.member is not None
            case "operator":
                return self.operator is not None
            case _:
                return False

    # ── Convenience accessors ────────────────────────────────────────

    @property
    def is_private(self) -> bool:
        """Check if this is a direct message (private channel)."""
        if self.channel is None:
            return False
        return self.channel.type == 1

    @property
    def sender_id(self) -> str | None:
        """Get the user ID of the event sender (primary user)."""
        return self.user.id if self.user else None

    @property
    def sender_name(self) -> str | None:
        """Get the sender display name for user-facing context.

        Guild member metadata is more specific than account metadata, so group
        chats should prefer member nicknames over platform-wide user nicknames.
        """
        member_user = self.member.user if self.member is not None else None
        for value in (
            self.member.nick if self.member is not None else None,
            member_user.nick if member_user is not None else None,
            member_user.name if member_user is not None else None,
            self.user.nick if self.user is not None else None,
            self.user.name if self.user is not None else None,
        ):
            text = str(value or "").strip()
            if text:
                return text
        return None

    @property
    def operator_id(self) -> str | None:
        """Get the user ID of the operator (for notice events)."""
        return self.operator.id if self.operator else None

    @property
    def channel_id(self) -> str | None:
        """Get the channel ID from event context."""
        return self.channel.id if self.channel else None

    @property
    def guild_id(self) -> str | None:
        """Get the guild ID from event context."""
        return self.guild.id if self.guild else None

    @property
    def message_content(self) -> str:
        """Raw XML content string from the message payload.

        Returns empty string if this is not a message event.
        """
        if self.message is None:
            return ""
        return self.message.content


__all__ = [
    "MessagePayload",
    "UnifiedEvent",
]
