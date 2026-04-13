"""UnifiedEvent and related Satori protocol models.

Defines the canonical event representation used throughout ShinBot.
Modeled after the Satori protocol event schema with fields observed
in real Satori WebSocket payloads (see .agent/test/satori_events.jsonl).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class User(BaseModel):
    """A user entity from the Satori protocol."""

    id: str
    name: str | None = None
    avatar: str | None = None
    is_bot: bool = False

    model_config = {"extra": "allow"}


class Channel(BaseModel):
    """A channel (conversation target) from the Satori protocol.

    Channel types (per Satori spec):
      0 = text channel
      1 = direct message
      2 = category
      3 = voice channel
    """

    id: str
    name: str | None = None
    type: int = 0

    model_config = {"extra": "allow"}


class Guild(BaseModel):
    """A guild/group entity from the Satori protocol."""

    id: str
    name: str | None = None
    avatar: str | None = None

    model_config = {"extra": "allow"}


class Login(BaseModel):
    """Login (bot identity) information from the Satori protocol."""

    sn: int | None = None
    user: User | None = None
    adapter: str | None = None
    platform: str | None = None
    status: int | None = None
    features: list[str] = Field(default_factory=list)
    proxy_urls: list[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class MessagePayload(BaseModel):
    """The message sub-object within a Satori event.

    The `content` field contains the raw Satori XML string which
    gets parsed into MessageElement AST by the framework.
    """

    id: str
    content: str = ""
    created_at: int | None = None

    model_config = {"extra": "allow"}


class Member(BaseModel):
    """A guild member entity from the Satori protocol."""

    nick: str | None = None
    avatar: str | None = None
    joined_at: int | None = None
    roles: list[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class UnifiedEvent(BaseModel):
    """Canonical event representation used throughout the ShinBot pipeline.

    After adapter ingress, every event (message-created, member-joined, etc.)
    is normalized into this structure. The `message.content` XML is further
    parsed into a Message AST and attached via the workflow context.

    Wire format fields (from Satori WebSocket):
      - op: opcode (0 = EVENT, 4 = READY)
      - body: the event payload (this model represents the body)
    """

    id: int | None = None
    sn: int | None = None
    type: str
    self_id: str = ""
    platform: str = ""
    timestamp: int | None = None

    login: Login | None = None
    user: User | None = None
    member: Member | None = None
    channel: Channel | None = None
    guild: Guild | None = None
    message: MessagePayload | None = None

    model_config = {"extra": "allow", "populate_by_name": True}

    @property
    def is_message_event(self) -> bool:
        return self.type in ("message-created", "message-updated", "message-deleted")

    @property
    def is_private(self) -> bool:
        if self.channel is None:
            return False
        return self.channel.type == 1

    @property
    def sender_id(self) -> str | None:
        return self.user.id if self.user else None

    @property
    def channel_id(self) -> str | None:
        return self.channel.id if self.channel else None

    @property
    def guild_id(self) -> str | None:
        return self.guild.id if self.guild else None

    @property
    def message_content(self) -> str:
        """Raw XML content string from the message payload."""
        if self.message is None:
            return ""
        return self.message.content
