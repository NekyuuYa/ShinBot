"""Message log repository and context provider implementation."""

from __future__ import annotations

import time
from typing import Any

from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.routing import (
    MessageRoutingSkipReason,
    MessageRoutingStatus,
    routing_skip_reason_value,
    routing_status_value,
)

from .base import ContextProvider, Repository


class MessageLogRepository(Repository, ContextProvider):
    """Persistence adapter for the full communication log."""

    def insert(self, record: MessageLogRecord) -> int:
        """Insert a message log entry and return the auto-incremented id."""
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO message_logs (
                    session_id, platform_msg_id, sender_id, sender_name,
                    content_json, raw_text, role, is_read, is_mentioned, created_at,
                    routing_status, routed_at, routing_skip_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.session_id,
                    record.platform_msg_id,
                    record.sender_id,
                    record.sender_name,
                    record.content_json,
                    record.raw_text,
                    record.role,
                    1 if record.is_read else 0,
                    1 if record.is_mentioned else 0,
                    record.created_at,
                    routing_status_value(record.routing_status),
                    record.routed_at,
                    record.routing_skip_reason,
                ),
            )
            return cursor.lastrowid  # type: ignore[return-value]

    def mark_read(self, msg_id: int) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE message_logs SET is_read = 1 WHERE id = ?", (msg_id,))

    def mark_routing_dispatched(self, msg_id: int, routed_at: float | None = None) -> None:
        """Mark a message as routed to at least one dispatcher target.

        `routed_at` uses millisecond epoch time to match message_logs.created_at.
        """
        timestamp = routed_at if routed_at is not None else time.time() * 1000
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE message_logs
                SET routing_status = ?,
                    routed_at = ?,
                    routing_skip_reason = NULL
                WHERE id = ?
                """,
                (MessageRoutingStatus.DISPATCHED.value, timestamp, msg_id),
            )

    def mark_routing_skipped(
        self,
        msg_id: int,
        reason: MessageRoutingSkipReason | str,
        routed_at: float | None = None,
    ) -> None:
        """Mark a message as routed but intentionally not dispatched."""
        timestamp = routed_at if routed_at is not None else time.time() * 1000
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE message_logs
                SET routing_status = ?,
                    routed_at = ?,
                    routing_skip_reason = ?
                WHERE id = ?
                """,
                (
                    MessageRoutingStatus.SKIPPED.value,
                    timestamp,
                    routing_skip_reason_value(reason),
                    msg_id,
                ),
            )

    def get(self, msg_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM message_logs WHERE id = ?", (msg_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    def get_by_platform_msg_id(
        self,
        session_id: str,
        platform_msg_id: str,
    ) -> dict[str, Any] | None:
        if not session_id or not platform_msg_id:
            return None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND platform_msg_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (session_id, platform_msg_id),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    def list_by_session(
        self,
        session_id: str,
        *,
        limit: int = 50,
        before_id: int | None = None,
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if before_id is not None:
                rows = conn.execute(
                    """
                    SELECT * FROM message_logs
                    WHERE session_id = ? AND id < ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (session_id, before_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM message_logs
                    WHERE session_id = ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (session_id, limit),
                ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_recent(self, session_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent messages for a session in chronological order."""
        rows = self.list_by_session(session_id, limit=limit)
        rows.reverse()
        return rows

    def get_by_time(
        self,
        session_id: str,
        start: float,
        end: float,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return messages within a time range in chronological order."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND created_at >= ? AND created_at <= ?
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                """,
                (session_id, start, end, limit),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def search_context(
        self,
        session_id: str,
        query: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Basic keyword search placeholder for future semantic retrieval."""
        needle = query.strip()
        if not needle:
            return []
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND raw_text LIKE ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (session_id, f"%{needle}%", limit),
            ).fetchall()
        items = [self._row_to_dict(r) for r in rows]
        items.reverse()
        return items

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        return {
            "id": row["id"],
            "session_id": row["session_id"],
            "platform_msg_id": row["platform_msg_id"],
            "sender_id": row["sender_id"],
            "sender_name": row["sender_name"],
            "content_json": row["content_json"],
            "raw_text": row["raw_text"],
            "role": row["role"],
            "is_read": bool(row["is_read"]),
            "is_mentioned": bool(row["is_mentioned"]),
            "created_at": row["created_at"],
            "routing_status": row["routing_status"],
            "routed_at": row["routed_at"],
            "routing_skip_reason": row["routing_skip_reason"],
        }
