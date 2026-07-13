"""SQLite-backed store adapters for Agent review workflows."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from typing import Any

from shinbot.agent.coordinators.review.models import UnreadRangeSummaryRecord
from shinbot.agent.coordinators.review.stores import MessageLogPayload
from shinbot.agent.scheduler.models import UnreadRange


class DatabaseReviewMessageStore:
    """SQLite-backed review message store using the existing message_logs table."""

    def __init__(self, database: Any) -> None:
        """Initialize the message store with a database connection.

        Args:
            database: Database manager providing connection context.
        """
        self._database = database

    def list_for_unread_range(
        self,
        unread_range: UnreadRange,
        *,
        limit: int,
        offset: int = 0,
    ) -> list[MessageLogPayload]:
        """List messages within an unread range, ordered by time.

        Args:
            unread_range: The unread range defining session and message ID bounds.
            limit: Maximum number of messages to return.
            offset: Number of messages to skip for pagination.

        Returns:
            List of message log payloads within the range.
        """
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
        """List messages around a specific message, including before and after context.

        Args:
            session_id: The session to query messages from.
            message_log_id: The central message to center the window on.
            before: Number of messages to fetch before the central message.
            after: Number of messages to fetch after the central message.

        Returns:
            List of message log payloads centered on the given message.
        """
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

    def list_by_ids(self, message_log_ids: Sequence[int]) -> list[MessageLogPayload]:
        """Load an exact bounded set of durable message-log rows.

        Callers are responsible for authorizing the identifiers before this
        read. The method preserves no caller ordering because actor projections
        must reapply their own durable ledger order.

        Args:
            message_log_ids: Positive, duplicate-free message-log identifiers.

        Returns:
            Existing message-log payloads ordered deterministically by id.

        Raises:
            ValueError: If an identifier is malformed or repeated.
        """

        normalized: list[int] = []
        seen: set[int] = set()
        for message_log_id in message_log_ids:
            if (
                isinstance(message_log_id, bool)
                or not isinstance(message_log_id, int)
                or message_log_id < 1
            ):
                raise ValueError("message_log_ids must contain positive integers")
            if message_log_id in seen:
                raise ValueError("message_log_ids must not contain duplicates")
            seen.add(message_log_id)
            normalized.append(message_log_id)
        if not normalized:
            return []
        placeholders = ", ".join("?" for _ in normalized)
        with self._database.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM message_logs
                WHERE id IN ({placeholders})
                ORDER BY id ASC
                """,
                tuple(normalized),
            ).fetchall()
        return [_row_to_payload(row) for row in rows]

    def list_by_time(
        self,
        *,
        session_id: str,
        start_at: float,
        end_at: float,
        limit: int,
    ) -> list[MessageLogPayload]:
        """List messages within a time range.

        Args:
            session_id: The session to query messages from.
            start_at: Start timestamp (epoch seconds) of the range.
            end_at: End timestamp (epoch seconds) of the range.
            limit: Maximum number of messages to return.

        Returns:
            List of message log payloads within the time range.
        """
        return self._database.message_logs.get_by_time(
            session_id,
            start=start_at,
            end=end_at,
            limit=limit,
        )


class DatabaseReviewSummaryStore:
    """SQLite-backed review summary store."""

    def __init__(self, database: Any) -> None:
        """Initialize the summary store with a database connection.

        Args:
            database: Database manager providing connection context.
        """
        self._database = database

    def save_summary(
        self,
        record: UnreadRangeSummaryRecord,
        *,
        created_at: float | None = None,
    ) -> int:
        """Persist a review summary record and return its database ID.

        Args:
            record: The summary record to save.
            created_at: Override timestamp; defaults to current time if not provided.

        Returns:
            The row ID of the newly inserted summary record.
        """
        with self._database.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO agent_review_summaries (
                    session_id, start_msg_log_id, end_msg_log_id, start_at, end_at,
                    message_count, summary, candidate_message_ids_json, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.session_id,
                    record.start_msg_log_id,
                    record.end_msg_log_id,
                    record.start_at,
                    record.end_at,
                    record.message_count,
                    record.summary,
                    json.dumps(record.candidate_message_ids, ensure_ascii=False),
                    record.reason,
                    created_at if created_at is not None else time.time(),
                ),
            )
            return int(cursor.lastrowid)

    def list_summaries(
        self,
        session_id: str,
        *,
        limit: int = 50,
    ) -> list[UnreadRangeSummaryRecord]:
        """List stored review summaries for a session, ordered by time.

        Args:
            session_id: The session to retrieve summaries for.
            limit: Maximum number of summaries to return (default 50).

        Returns:
            List of summary records for the session.
        """
        with self._database.connect() as conn:
            rows = conn.execute(
                """
                SELECT session_id, start_msg_log_id, end_msg_log_id, start_at, end_at,
                       message_count, summary, candidate_message_ids_json, reason
                FROM agent_review_summaries
                WHERE session_id = ?
                ORDER BY start_at ASC, start_msg_log_id ASC, id ASC
                LIMIT ?
                """,
                (session_id, limit),
            ).fetchall()
        return [self._record_from_row(row) for row in rows]

    @staticmethod
    def _record_from_row(row: Any) -> UnreadRangeSummaryRecord:
        try:
            candidate_message_ids = json.loads(row["candidate_message_ids_json"] or "[]")
        except Exception:
            candidate_message_ids = []
        return UnreadRangeSummaryRecord(
            session_id=row["session_id"],
            start_msg_log_id=int(row["start_msg_log_id"]),
            end_msg_log_id=int(row["end_msg_log_id"]),
            start_at=float(row["start_at"]),
            end_at=float(row["end_at"]),
            message_count=int(row["message_count"]),
            summary=str(row["summary"] or ""),
            candidate_message_ids=[int(item) for item in candidate_message_ids],
            reason=str(row["reason"] or ""),
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


__all__ = ["DatabaseReviewMessageStore", "DatabaseReviewSummaryStore"]
