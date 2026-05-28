"""Session and audit repositories."""

from __future__ import annotations

import json
import time
from typing import Any

from .base import Repository


class SessionRepository(Repository):
    """Persistence adapter for structured session state."""

    def list(self, *, instance_id: str | None = None) -> list[dict[str, Any]]:
        """Return all sessions, optionally filtered by instance.

        Args:
            instance_id: When provided only sessions belonging to this instance
                are returned.

        Returns:
            List of session dictionaries ordered by most recently active.
        """
        with self.connect() as conn:
            if instance_id:
                rows = conn.execute(
                    """
                    SELECT
                        s.id,
                        s.instance_id,
                        s.session_type,
                        s.platform,
                        s.guild_id,
                        s.channel_id,
                        s.display_name,
                        s.permission_group,
                        s.created_at,
                        s.last_active,
                        s.state_json,
                        s.plugin_data_json,
                        c.prefixes_json,
                        c.llm_enabled,
                        c.is_muted,
                        c.audit_enabled
                    FROM sessions AS s
                    LEFT JOIN session_configs AS c ON c.session_id = s.id
                    WHERE s.instance_id = ?
                    ORDER BY s.last_active DESC, s.id ASC
                    """,
                    (instance_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        s.id,
                        s.instance_id,
                        s.session_type,
                        s.platform,
                        s.guild_id,
                        s.channel_id,
                        s.display_name,
                        s.permission_group,
                        s.created_at,
                        s.last_active,
                        s.state_json,
                        s.plugin_data_json,
                        c.prefixes_json,
                        c.llm_enabled,
                        c.is_muted,
                        c.audit_enabled
                    FROM sessions AS s
                    LEFT JOIN session_configs AS c ON c.session_id = s.id
                    ORDER BY s.last_active DESC, s.id ASC
                    """
                ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get(self, session_id: str) -> dict[str, Any] | None:
        """Return a single session by ID, or ``None`` if not found.

        Args:
            session_id: Unique session identifier.
        """
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    s.id,
                    s.instance_id,
                    s.session_type,
                    s.platform,
                    s.guild_id,
                    s.channel_id,
                    s.display_name,
                    s.permission_group,
                    s.created_at,
                    s.last_active,
                    s.state_json,
                    s.plugin_data_json,
                    c.prefixes_json,
                    c.llm_enabled,
                    c.is_muted,
                    c.audit_enabled
                FROM sessions AS s
                LEFT JOIN session_configs AS c ON c.session_id = s.id
                WHERE s.id = ?
                """,
                (session_id,),
            ).fetchone()
        if row is None:
            return None

        return {
            "id": row["id"],
            "instance_id": row["instance_id"],
            "session_type": row["session_type"],
            "platform": row["platform"],
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "display_name": row["display_name"],
            "permission_group": row["permission_group"],
            "created_at": row["created_at"],
            "last_active": row["last_active"],
            "state": self.json_loads(row["state_json"], {}),
            "plugin_data": self.json_loads(row["plugin_data_json"], {}),
            "config": {
                "prefixes": self.json_loads(row["prefixes_json"], ["/"]),
                "llm_enabled": bool(row["llm_enabled"]) if row["llm_enabled"] is not None else True,
                "is_muted": bool(row["is_muted"]) if row["is_muted"] is not None else False,
                "audit_enabled": (
                    bool(row["audit_enabled"]) if row["audit_enabled"] is not None else False
                ),
            },
        }

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        return {
            "id": row["id"],
            "instance_id": row["instance_id"],
            "session_type": row["session_type"],
            "platform": row["platform"],
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "display_name": row["display_name"],
            "permission_group": row["permission_group"],
            "created_at": row["created_at"],
            "last_active": row["last_active"],
            "state": json.loads(row["state_json"] or "{}"),
            "plugin_data": json.loads(row["plugin_data_json"] or "{}"),
            "config": {
                "prefixes": json.loads(row["prefixes_json"] or '["/"]'),
                "llm_enabled": bool(row["llm_enabled"]) if row["llm_enabled"] is not None else True,
                "is_muted": bool(row["is_muted"]) if row["is_muted"] is not None else False,
                "audit_enabled": (
                    bool(row["audit_enabled"]) if row["audit_enabled"] is not None else False
                ),
            },
        }

    def upsert(self, payload: dict[str, Any]) -> None:
        """Insert or update a session and its configuration.

        Args:
            payload: Session dictionary containing at least ``id``,
                ``instance_id``, and ``session_type``.
        """
        config = dict(payload.get("config") or {})
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (
                    id, instance_id, session_type, platform, guild_id, channel_id, display_name,
                    permission_group, created_at, last_active, state_json, plugin_data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    instance_id = excluded.instance_id,
                    session_type = excluded.session_type,
                    platform = excluded.platform,
                    guild_id = excluded.guild_id,
                    channel_id = excluded.channel_id,
                    display_name = excluded.display_name,
                    permission_group = excluded.permission_group,
                    created_at = excluded.created_at,
                    last_active = excluded.last_active,
                    state_json = excluded.state_json,
                    plugin_data_json = excluded.plugin_data_json
                """,
                (
                    payload["id"],
                    payload["instance_id"],
                    payload["session_type"],
                    payload.get("platform", ""),
                    payload.get("guild_id"),
                    payload.get("channel_id", ""),
                    payload.get("display_name", ""),
                    payload.get("permission_group", "default"),
                    payload.get("created_at", now),
                    payload.get("last_active", now),
                    self.json_dumps(payload.get("state", {})),
                    self.json_dumps(payload.get("plugin_data", {})),
                ),
            )
            conn.execute(
                """
                INSERT INTO session_configs (
                    session_id, prefixes_json, llm_enabled, is_muted, audit_enabled, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    prefixes_json = excluded.prefixes_json,
                    llm_enabled = excluded.llm_enabled,
                    is_muted = excluded.is_muted,
                    audit_enabled = excluded.audit_enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["id"],
                    self.json_dumps(config.get("prefixes", ["/"])),
                    1 if config.get("llm_enabled", True) else 0,
                    1 if config.get("is_muted", False) else 0,
                    1 if config.get("audit_enabled", False) else 0,
                    now,
                ),
            )

    def delete(self, session_id: str) -> None:
        """Delete a session and all cascaded rows.

        Args:
            session_id: Unique session identifier.
        """
        with self.connect() as conn:
            conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))


class AuditRepository(Repository):
    """Persistence adapter for structured audit logs."""

    def list_by_session(self, session_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        """Return recent audit log entries for a session.

        Args:
            session_id: Session to query.
            limit: Maximum number of entries to return.
        """
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM audit_logs
                WHERE session_id = ?
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                (session_id, limit),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_latest_by_session(self, session_id: str) -> dict[str, Any] | None:
        """Return the most recent audit entry for a session, or ``None``."""
        rows = self.list_by_session(session_id, limit=1)
        return rows[0] if rows else None

    def insert(self, payload: dict[str, Any]) -> int:
        """Insert an audit log entry.

        Args:
            payload: Audit entry dictionary with fields such as ``timestamp``,
                ``entry_type``, ``command_name``, etc.

        Returns:
            The auto-incremented row id.
        """
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO audit_logs (
                    timestamp, entry_type, command_name, plugin_id, user_id, session_id, instance_id,
                    permission_required, permission_granted, execution_time_ms, success, error, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("timestamp", ""),
                    payload.get("entry_type", "command"),
                    payload.get("command_name", ""),
                    payload.get("plugin_id", ""),
                    payload.get("user_id", ""),
                    payload.get("session_id", ""),
                    payload.get("instance_id", ""),
                    payload.get("permission_required", ""),
                    1 if payload.get("permission_granted", False) else 0,
                    payload.get("execution_time_ms", 0.0),
                    1 if payload.get("success", False) else 0,
                    payload.get("error", ""),
                    self.json_dumps(payload.get("metadata", {})),
                ),
            )
            return int(cursor.lastrowid)

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "timestamp": str(row["timestamp"] or ""),
            "entry_type": str(row["entry_type"] or ""),
            "command_name": str(row["command_name"] or ""),
            "plugin_id": str(row["plugin_id"] or ""),
            "user_id": str(row["user_id"] or ""),
            "session_id": str(row["session_id"] or ""),
            "instance_id": str(row["instance_id"] or ""),
            "permission_required": str(row["permission_required"] or ""),
            "permission_granted": bool(row["permission_granted"]),
            "execution_time_ms": float(row["execution_time_ms"] or 0.0),
            "success": bool(row["success"]),
            "error": str(row["error"] or ""),
            "metadata": json.loads(row["metadata_json"] or "{}"),
        }
