"""Session and audit repositories."""

from __future__ import annotations

import time
from typing import Any

from .base import _json_dumps, _json_loads


class SessionRepository:
    """Persistence adapter for structured session state."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def get(self, session_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
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
            "state": _json_loads(row["state_json"], {}),
            "plugin_data": _json_loads(row["plugin_data_json"], {}),
            "config": {
                "prefixes": _json_loads(row["prefixes_json"], ["/"]),
                "llm_enabled": bool(row["llm_enabled"]) if row["llm_enabled"] is not None else True,
                "is_muted": bool(row["is_muted"]) if row["is_muted"] is not None else False,
                "audit_enabled": (
                    bool(row["audit_enabled"]) if row["audit_enabled"] is not None else False
                ),
            },
        }

    def upsert(self, payload: dict[str, Any]) -> None:
        config = dict(payload.get("config") or {})
        now = time.time()
        with self._db.connect() as conn:
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
                    _json_dumps(payload.get("state", {})),
                    _json_dumps(payload.get("plugin_data", {})),
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
                    _json_dumps(config.get("prefixes", ["/"])),
                    1 if config.get("llm_enabled", True) else 0,
                    1 if config.get("is_muted", False) else 0,
                    1 if config.get("audit_enabled", False) else 0,
                    now,
                ),
            )

    def delete(self, session_id: str) -> None:
        with self._db.connect() as conn:
            conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))


class AuditRepository:
    """Persistence adapter for structured audit logs."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def insert(self, payload: dict[str, Any]) -> int:
        with self._db.connect() as conn:
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
                    _json_dumps(payload.get("metadata", {})),
                ),
            )
            return int(cursor.lastrowid)
