"""Resource models for ShinBot — aligned with Satori protocol specification.

Defines the canonical representations of users, guilds, channels, members, and
other structured data entities. These models are used consistently across both
ingress (listening to events) and action (calling APIs) paths.

Reference: docs/design/17_resource_schema_spec.md
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class User(BaseModel):
    """User entity — represents a person or bot in the platform.

    Fields align with Satori protocol and are consistent across both
    event ingress and API action layers.
    """

    id: str
    """Unique user ID on the platform."""

    name: str | None = None
    """Platform username or account name."""

    nick: str | None = None
    """User's display nickname (may differ from name)."""

    avatar: str | None = None
    """URL to user's avatar image."""

    is_bot: bool = False
    """Whether this user represents a bot account."""

    model_config = {"extra": "allow"}


class Channel(BaseModel):
    """Channel entity — represents a conversation target.

    In Satori protocol terms, a channel is the destination for messages.
    Types follow Satori channel type spec:
      0 = text channel
      1 = direct message (private conversation)
      2 = category
      3 = voice channel
    """

    id: str
    """Unique channel ID on the platform."""

    name: str | None = None
    """Channel display name (e.g. '#general', 'DM with Alice')."""

    type: int = 0
    """Channel type (0=text, 1=DM, 2=category, 3=voice)."""

    model_config = {"extra": "allow"}


class Guild(BaseModel):
    """Guild/Group entity — represents a server or community space.

    In Satori terms, a guild is a container for channels and members.
    Equivalent to QQ Group, Discord Server, Telegram Supergroup, etc.
    """

    id: str
    """Unique server/group ID on the platform."""

    name: str | None = None
    """Server/group display name."""

    avatar: str | None = None
    """URL to server/group icon or avatar."""

    model_config = {"extra": "allow"}


class Member(BaseModel):
    """Guild member entity — represents a user's membership in a guild.

    Contains membership-specific metadata separate from the User object.
    """

    user: User | None = None
    """The user account this membership belongs to."""

    nick: str | None = None
    """Member's server-specific nickname (overrides User.nick)."""

    avatar: str | None = None
    """Member's server-specific avatar (overrides User.avatar)."""

    joined_at: int | None = None
    """Unix timestamp when member joined the guild."""

    roles: list[str] = Field(default_factory=list)
    """List of role IDs or role names for this member."""

    model_config = {"extra": "allow"}


class Login(BaseModel):
    """Login/Bot entity — represents the authenticated bot instance.

    Contains platform and adapter information about the bot connection.
    """

    sn: int | None = None
    """Connection serial number."""

    user: User | None = None
    """The bot's own user identity."""

    adapter: str | None = None
    """Name of the adapter (e.g. 'satori', 'qq', 'telegram')."""

    platform: str | None = None
    """Platform name (e.g. 'qq', 'discord', 'telegram')."""

    status: int | None = None
    """Connection status code."""

    features: list[str] = Field(default_factory=list)
    """List of supported features/capabilities."""

    proxy_urls: list[str] = Field(default_factory=list)
    """List of proxy endpoints for this adapter."""

    model_config = {"extra": "allow"}


# Type aliases for convenience
ResourceTypes = User | Guild | Channel | Member | Login
"""Union of all resource types."""
