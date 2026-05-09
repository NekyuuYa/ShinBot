"""Persistence for Agent review summary records."""

from __future__ import annotations

import json
import time
from typing import Any, Protocol

from shinbot.agent.models.review import UnreadRangeSummaryRecord


class ReviewSummaryStore(Protocol):
    """Stores Agent-owned summaries produced while reviewing unread ranges."""

    def save_summary(
        self,
        record: UnreadRangeSummaryRecord,
        *,
        created_at: float | None = None,
    ) -> int:
        """Persist a review summary and return its row id."""

    def list_summaries(
        self,
        session_id: str,
        *,
        limit: int = 50,
    ) -> list[UnreadRangeSummaryRecord]:
        """Return recent review summaries for one session in timeline order."""


class DatabaseReviewSummaryStore:
    """SQLite-backed review summary store."""

    def __init__(self, database: Any) -> None:
        self._database = database

    def save_summary(
        self,
        record: UnreadRangeSummaryRecord,
        *,
        created_at: float | None = None,
    ) -> int:
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
