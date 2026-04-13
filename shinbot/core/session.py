"""Session management — identity, configuration, and lifecycle.

Implements the session management specification (04_session_management.md).
Sessions are the minimal unit of context, permission binding, and state
isolation in ShinBot.

Session identity URN: {instance_id}:{type}:{target_id}
  - type: "group" | "private"
  - target_id: channel_id (or guild_id:channel_id for nested platforms)
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from shinbot.models.events import UnifiedEvent

logger = logging.getLogger(__name__)


class SessionConfig(BaseModel):
    """Per-session runtime configuration."""

    prefixes: list[str] = Field(default_factory=lambda: ["/"])
    llm_enabled: bool = True
    is_muted: bool = False
    audit_enabled: bool = False

    model_config = {"extra": "allow"}


class Session(BaseModel):
    """Core session object — the minimal unit of context in ShinBot.

    Each session represents a unique conversation scope (a specific group
    chat on a specific bot instance, or a private chat with a specific user).
    """

    # ── Identity ─────────────────────────────────────────────────────
    id: str  # Full URN: instance:type:target
    instance_id: str  # Bot instance identifier
    session_type: str  # "group" or "private"
    platform: str = ""  # Source platform name
    guild_id: str | None = None  # Top-level container (server ID)
    channel_id: str = ""  # Target container (group/channel ID)

    # ── Metadata (persisted) ─────────────────────────────────────────
    display_name: str = ""
    permission_group: str = "default"  # Associated permission group ID
    created_at: float = Field(default_factory=time.time)
    last_active: float = Field(default_factory=time.time)

    # ── Runtime config ───────────────────────────────────────────────
    config: SessionConfig = Field(default_factory=SessionConfig)

    # ── Dynamic data ─────────────────────────────────────────────────
    state: dict[str, Any] = Field(default_factory=dict)
    plugin_data: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}

    def touch(self) -> None:
        """Update last_active timestamp."""
        self.last_active = time.time()

    @property
    def is_private(self) -> bool:
        return self.session_type == "private"

    @property
    def is_group(self) -> bool:
        return self.session_type == "group"

    @property
    def is_muted(self) -> bool:
        return self.config.is_muted


def build_session_id(
    instance_id: str,
    event: UnifiedEvent,
) -> str:
    """Build a session URN from an instance ID and a UnifiedEvent.

    Rules (per 04_session_management.md):
      - Private: {instance_id}:private:{user_id}
      - Group (flat, e.g. QQ): {instance_id}:group:{channel_id}
      - Group (nested, e.g. Discord): {instance_id}:group:{guild_id}:{channel_id}
    """
    if event.is_private:
        user_id = event.sender_id or ""
        return f"{instance_id}:private:{user_id}"

    channel_id = event.channel_id or ""
    guild_id = event.guild_id

    if guild_id:
        return f"{instance_id}:group:{guild_id}:{channel_id}"
    return f"{instance_id}:group:{channel_id}"


def session_from_event(
    instance_id: str,
    event: UnifiedEvent,
) -> Session:
    """Create a new Session object from a UnifiedEvent.

    This constructs the initial session state. In production, the
    SessionManager would check the database first and only create
    a new session if one doesn't exist.
    """
    session_id = build_session_id(instance_id, event)
    session_type = "private" if event.is_private else "group"

    channel_id = event.channel_id or ""
    guild_id = event.guild_id
    display_name = ""
    if event.channel and event.channel.name:
        display_name = event.channel.name

    return Session(
        id=session_id,
        instance_id=instance_id,
        session_type=session_type,
        platform=event.platform,
        guild_id=guild_id,
        channel_id=channel_id,
        display_name=display_name,
    )


class SessionManager:
    """In-memory session store with optional JSON persistence.

    If `data_dir` is provided, sessions are loaded from and saved to
    `{data_dir}/sessions/{sanitized_id}.json` automatically.
    """

    def __init__(self, data_dir: Path | str | None = None) -> None:
        self._sessions: dict[str, Session] = {}
        self._data_dir: Path | None = Path(data_dir) / "sessions" if data_dir else None
        if self._data_dir:
            self._data_dir.mkdir(parents=True, exist_ok=True)

    def _session_path(self, session_id: str) -> Path | None:
        """Return the JSON file path for a session, or None if no persistence."""
        if self._data_dir is None:
            return None
        sanitized = session_id.replace(":", "_").replace("/", "_")
        return self._data_dir / f"{sanitized}.json"

    def _load_from_disk(self, session_id: str) -> Session | None:
        path = self._session_path(session_id)
        if path is None or not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return Session.model_validate(data)
        except Exception:
            logger.warning("Corrupted session file %s, will recreate", path)
            return None

    def _save_to_disk(self, session: Session) -> None:
        path = self._session_path(session.id)
        if path is None:
            return
        try:
            path.write_text(
                json.dumps(session.model_dump(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            logger.exception("Failed to persist session %s", session.id)

    def get(self, session_id: str) -> Session | None:
        return self._sessions.get(session_id)

    def get_or_create(self, instance_id: str, event: UnifiedEvent) -> Session:
        """Look up a session by event context, or create a new one."""
        session_id = build_session_id(instance_id, event)
        session = self._sessions.get(session_id)
        if session is not None:
            session.touch()
            return session

        # Try to restore from disk
        session = self._load_from_disk(session_id)
        if session is None:
            session = session_from_event(instance_id, event)
            logger.debug("Created new session: %s", session_id)
        else:
            logger.debug("Restored session from disk: %s", session_id)

        self._sessions[session_id] = session
        return session

    def update(self, session: Session) -> None:
        """Persist a session (update in store, write to disk if configured)."""
        self._sessions[session.id] = session
        self._save_to_disk(session)

    def remove(self, session_id: str) -> Session | None:
        return self._sessions.pop(session_id, None)

    @property
    def all_sessions(self) -> list[Session]:
        return list(self._sessions.values())

    def sessions_for_instance(self, instance_id: str) -> list[Session]:
        return [s for s in self._sessions.values() if s.instance_id == instance_id]

    def __len__(self) -> int:
        return len(self._sessions)
