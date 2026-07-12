"""Core-owned stable identity contract for Agent route delivery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

DEFAULT_SESSION_ACTOR_PROFILE_ID = "__default_agent_profile__"


@dataclass(slots=True, frozen=True, order=True)
class SessionKey:
    """Identify one Agent session within a stable runtime profile."""

    profile_id: str
    session_id: str

    def __post_init__(self) -> None:
        """Normalize identifiers and reject an empty session id."""

        profile_id = str(self.profile_id or "").strip()
        session_id = str(self.session_id or "").strip()
        if not profile_id:
            raise ValueError("profile_id must not be empty")
        if not session_id:
            raise ValueError("session_id must not be empty")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "session_id", session_id)


class SessionRoutingIdentity(Protocol):
    """Routing fields required to construct a canonical Agent session key."""

    # This is the selected BotServiceConfig.id, not an adapter/platform self id.
    bot_id: str
    bot_session_id: str
    session_id: str


@dataclass(slots=True, frozen=True)
class SessionKeyFactory:
    """Build canonical keys from stable bot and session identities."""

    default_profile_id: str = DEFAULT_SESSION_ACTOR_PROFILE_ID

    def __post_init__(self) -> None:
        """Normalize and validate the reserved default profile id."""

        normalized = str(self.default_profile_id or "").strip()
        if not normalized:
            raise ValueError("default_profile_id must not be empty")
        object.__setattr__(self, "default_profile_id", normalized)

    def create(
        self,
        *,
        bot_config_id: str = "",
        bot_id: str = "",
        bot_session_id: str = "",
        base_session_id: str = "",
    ) -> SessionKey:
        """Create one canonical profile-scoped session key.

        Args:
            bot_config_id: Stable id of the selected bot service config.
            bot_id: Bot routing id used to scope a legacy base session.
            bot_session_id: Preferred bot-scoped session id from core routing.
            base_session_id: Unscoped session id used only as a fallback.

        Returns:
            The canonical profile/session key.

        Raises:
            ValueError: If identity is empty or uses the reserved profile id.
        """

        stable_config_id = str(bot_config_id or "").strip()
        if stable_config_id == self.default_profile_id:
            raise ValueError(
                f"bot_config_id {stable_config_id!r} is reserved for the default profile"
            )
        profile_id = stable_config_id or self.default_profile_id

        scoped_session_id = str(bot_session_id or "").strip()
        if not scoped_session_id:
            base = str(base_session_id or "").strip()
            if not base:
                raise ValueError(
                    "base_session_id must not be empty when bot_session_id is absent"
                )
            scope = (
                str(bot_id or "").strip()
                or stable_config_id
                or self.default_profile_id
            )
            scoped_session_id = f"{scope}:{base}"

        return SessionKey(profile_id=profile_id, session_id=scoped_session_id)

    def from_signal(self, signal: SessionRoutingIdentity) -> SessionKey:
        """Create a key from a signal whose ``bot_id`` is BotServiceConfig.id."""

        return self.create(
            bot_config_id=signal.bot_id,
            bot_id=signal.bot_id,
            bot_session_id=signal.bot_session_id,
            base_session_id=signal.session_id,
        )


__all__ = [
    "DEFAULT_SESSION_ACTOR_PROFILE_ID",
    "SessionKey",
    "SessionKeyFactory",
    "SessionRoutingIdentity",
]
