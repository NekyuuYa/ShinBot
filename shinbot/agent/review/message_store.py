"""Message-log reading boundary for Agent review workflows."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.scheduler.models import UnreadRange

MessageLogPayload = dict[str, Any]


class ReviewMessageStore(Protocol):
    """Read message logs for review stages without constructing prompts."""

    def list_for_unread_range(
        self,
        unread_range: UnreadRange,
        *,
        limit: int,
        offset: int = 0,
    ) -> list[MessageLogPayload]:
        """Read messages inside one unread range in chronological order."""

    def list_around_message(
        self,
        *,
        session_id: str,
        message_log_id: int,
        before: int,
        after: int,
    ) -> list[MessageLogPayload]:
        """Read a local context window around a candidate message."""

    def list_by_time(
        self,
        *,
        session_id: str,
        start_at: float,
        end_at: float,
        limit: int,
    ) -> list[MessageLogPayload]:
        """Read messages in a timestamp window in chronological order."""


class DatabaseReviewMessageStore:
    """SQLite-backed review message store using the existing message_logs table."""

    def __init__(self, database) -> None:
        self._database = database

    def list_for_unread_range(
        self,
        unread_range: UnreadRange,
        *,
        limit: int,
        offset: int = 0,
    ) -> list[MessageLogPayload]:
        with self._database.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ?
                  AND id >= ?
                  AND id <= ?
                ORDER BY created_at ASC, id ASC
                LIMIT ? OFFSET ?
                """,
                (
                    unread_range.session_id,
                    unread_range.start_msg_log_id,
                    unread_range.end_msg_log_id,
                    limit,
                    offset,
                ),
            ).fetchall()
        return [_row_to_payload(row) for row in rows]

    def list_around_message(
        self,
        *,
        session_id: str,
        message_log_id: int,
        before: int,
        after: int,
    ) -> list[MessageLogPayload]:
        with self._database.connect() as conn:
            before_rows = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND id < ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (session_id, message_log_id, before),
            ).fetchall()
            center_row = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND id = ?
                """,
                (session_id, message_log_id),
            ).fetchone()
            after_rows = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (session_id, message_log_id, after),
            ).fetchall()

        rows = list(reversed(before_rows))
        if center_row is not None:
            rows.append(center_row)
        rows.extend(after_rows)
        return [_row_to_payload(row) for row in rows]

    def list_by_time(
        self,
        *,
        session_id: str,
        start_at: float,
        end_at: float,
        limit: int,
    ) -> list[MessageLogPayload]:
        return self._database.message_logs.get_by_time(
            session_id,
            start=start_at,
            end=end_at,
            limit=limit,
        )


def _row_to_payload(row: Any) -> MessageLogPayload:
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


__all__ = ["DatabaseReviewMessageStore", "MessageLogPayload", "ReviewMessageStore"]
