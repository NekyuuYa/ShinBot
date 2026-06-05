"""Session management — identity, configuration, and lifecycle.

Implements the session management specification (session_management.md).
Sessions are the minimal unit of context, permission binding, and state
isolation in ShinBot.

Session identity URN: {instance_id}:{type}:{target_id}
  - type: "group" | "private"
  - target_id: channel_id (or guild_id:channel_id for nested platforms)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from shinbot.schema.events import UnifiedEvent

if TYPE_CHECKING:
    from shinbot.persistence.repos import SessionRepository

logger = logging.getLogger(__name__)

SESSION_STATE_AGENT_PAUSE_UNTIL_KEY = "agent_pause_until"


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
        """Return True if this session is a private (DM) conversation."""
        return self.session_type == "private"

    @property
    def is_group(self) -> bool:
        """Return True if this session is a group (multi-user) conversation."""
        return self.session_type == "group"

    @property
    def is_muted(self) -> bool:
        """Return True if the session is currently muted."""
        return self.config.is_muted


def get_agent_pause_until(payload: Session | Mapping[str, Any]) -> float | None:
    """Return the session-level Agent pause deadline, if present.

    Args:
        payload: A live :class:`Session` object or a persisted session-like
            mapping containing a ``state`` payload.

    Returns:
        The pause deadline as a Unix timestamp in seconds, or ``None`` when
        no valid pause deadline is stored.
    """
    if isinstance(payload, Session):
        state = payload.state
    else:
        raw_state = payload.get("state", {})
        state = raw_state if isinstance(raw_state, Mapping) else {}
    raw_value = state.get(SESSION_STATE_AGENT_PAUSE_UNTIL_KEY)
    if raw_value in (None, ""):
        return None
    try:
        pause_until = float(raw_value)
    except (TypeError, ValueError):
        return None
    return pause_until if pause_until > 0 else None


def set_agent_pause_until(session: Session, pause_until: float | None) -> None:
    """Store or clear the session-level Agent pause deadline.

    Args:
        session: The session to mutate.
        pause_until: Unix timestamp in seconds when Agent activity may resume.
            Pass ``None`` to clear the pause state.
    """
    if pause_until is None:
        session.state.pop(SESSION_STATE_AGENT_PAUSE_UNTIL_KEY, None)
        return
    session.state[SESSION_STATE_AGENT_PAUSE_UNTIL_KEY] = float(pause_until)


def is_agent_paused(
    payload: Session | Mapping[str, Any],
    *,
    now: float | None = None,
) -> bool:
    """Return whether the session-level Agent pause window is still active.

    Args:
        payload: A live :class:`Session` object or persisted session payload.
        now: Override the current time for tests.
    """
    pause_until = get_agent_pause_until(payload)
    if pause_until is None:
        return False
    checked_at = time.time() if now is None else now
    return pause_until > checked_at


def build_session_id(
    instance_id: str,
    event: UnifiedEvent,
) -> str:
    """Build a session URN from an instance ID and a UnifiedEvent.

    Rules (per session_management.md):
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
    """In-memory session store with optional database or JSON persistence.

    If `session_repo` is provided, it becomes the primary persistence backend.
    Otherwise, if `data_dir` is provided, sessions are loaded from and saved to
    `{data_dir}/sessions/{sanitized_id}.json`.
    """

    def __init__(
        self,
        data_dir: Path | str | None = None,
        *,
        session_repo: SessionRepository | None = None,
    ) -> None:
        """Initialise the session manager.

        Args:
            data_dir: Root directory for JSON file persistence.  Ignored when
                *session_repo* is provided.
            session_repo: Optional database repository for session persistence.
                When supplied it takes precedence over file-based storage.
        """
        self._sessions: dict[str, Session] = {}
        self._session_repo = session_repo
        self._data_dir: Path | None = Path(data_dir) / "sessions" if data_dir else None
        if self._data_dir and self._session_repo is None:
            self._data_dir.mkdir(parents=True, exist_ok=True)
        # Per-session asyncio locks — serialise concurrent event processing for
        # the same session so that interleaved coroutines cannot overwrite each
        # other's state modifications.
        self._locks: dict[str, asyncio.Lock] = {}

    def _session_path(self, session_id: str) -> Path | None:
        """Return the JSON file path for a session, or None if no persistence."""
        if self._data_dir is None:
            return None
        sanitized = session_id.replace(":", "_").replace("/", "_")
        return self._data_dir / f"{sanitized}.json"

    def _get_lock(self, session_id: str) -> asyncio.Lock:
        """Return (creating if necessary) the asyncio.Lock for a session."""
        if session_id not in self._locks:
            self._locks[session_id] = asyncio.Lock()
        return self._locks[session_id]

    @asynccontextmanager
    async def session_lock(self, session_id: str):  # type: ignore[return]
        """Async context manager that grants exclusive access to a session.

        Acquire this before reading or writing session state to prevent
        concurrent event coroutines from interleaving their modifications.

        Usage::

            async with session_manager.session_lock(session_id):
                session = session_manager.get_or_create(instance_id, event)
                # ... process event ...
                session_manager.update(session)
        """
        async with self._get_lock(session_id):
            yield

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
            content = json.dumps(session.model_dump(), ensure_ascii=False, indent=2)
            # Write to a sibling .tmp file then rename atomically.
            # os.replace() is atomic on POSIX and as-close-as-possible on Windows,
            # so a crash mid-write cannot leave a corrupt session file.
            tmp = path.with_suffix(".tmp")
            tmp.write_text(content, encoding="utf-8")
            os.replace(tmp, path)
        except Exception:
            logger.exception("Failed to persist session %s", session.id)

    def get(self, session_id: str) -> Session | None:
        """Return the in-memory session for *session_id*, or ``None``."""
        return self._sessions.get(session_id)

    def get_or_create(self, instance_id: str, event: UnifiedEvent) -> Session:
        """Look up a session by event context, or create a new one."""
        session_id = build_session_id(instance_id, event)
        session = self._sessions.get(session_id)
        if session is not None:
            session.touch()
            return session

        session = self._load_from_storage(session_id)
        if session is None:
            session = session_from_event(instance_id, event)
            logger.debug("Created new session: %s", session_id)
        else:
            logger.debug("Restored session from storage: %s", session_id)

        self._sessions[session_id] = session
        return session

    def update(self, session: Session) -> None:
        """Persist a session via the configured storage backend."""
        self._sessions[session.id] = session
        if self._session_repo is not None:
            self._session_repo.upsert(session.model_dump())
            return
        self._save_to_disk(session)

    def remove(self, session_id: str, *, delete_persisted: bool = True) -> Session | None:
        """Remove a session from memory (and persistence) and return it.

        Args:
            session_id: Full session URN to remove.
            delete_persisted: When ``True`` also delete the persisted session
                record and associated database state.

        Returns:
            The removed :class:`Session`, or ``None`` if it did not exist.
        """
        removed = self._sessions.pop(session_id, None)
        if delete_persisted and self._session_repo is not None:
            self._session_repo.delete(session_id)
        return removed

    @property
    def all_sessions(self) -> list[Session]:
        """Return a snapshot list of every session held in memory."""
        return list(self._sessions.values())

    def sessions_for_instance(self, instance_id: str) -> list[Session]:
        """Return all sessions belonging to the given bot *instance_id*."""
        return [s for s in self._sessions.values() if s.instance_id == instance_id]

    def __len__(self) -> int:
        """Return the number of sessions currently held in memory."""
        return len(self._sessions)

    def _load_from_storage(self, session_id: str) -> Session | None:
        if self._session_repo is not None:
            data = self._session_repo.get(session_id)
            if data is not None:
                return Session.model_validate(data)
        return self._load_from_disk(session_id)
