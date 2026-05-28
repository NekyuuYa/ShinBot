"""SQLite-backed repository for unified agent summaries."""

from __future__ import annotations

import json
import time
from typing import Any

from shinbot.agent.services.summaries.models import (
    SummaryRecord,
    SummaryType,
    SummaryWriteRequest,
)
from shinbot.persistence.repositories.base import Repository


class AgentSummaryRepository(Repository):
    """Persistence adapter for the agent_summaries table."""

    def save(
        self,
        request: SummaryWriteRequest,
        *,
        created_at: float | None = None,
    ) -> int:
        """Insert a new summary record and return its id."""
        metadata = dict(request.metadata)
        if request.block_index is not None:
            metadata.setdefault("block_index", request.block_index)
        if request.msg_count:
            metadata.setdefault("msg_count", request.msg_count)

        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO agent_summaries (
                    session_id, summary_type, content, source_run_id,
                    msg_log_start, msg_log_end, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.session_id,
                    request.summary_type.value,
                    request.content,
                    request.source_run_id,
                    request.msg_log_start,
                    request.msg_log_end,
                    json.dumps(metadata, ensure_ascii=False),
                    created_at if created_at is not None else time.time(),
                ),
            )
            return int(cursor.lastrowid)

    def get_by_session(
        self,
        session_id: str,
        *,
        summary_type: SummaryType | None = None,
        limit: int = 50,
    ) -> list[SummaryRecord]:
        """Query summaries by session_id in chronological order."""
        if summary_type is not None:
            sql = """
                SELECT * FROM agent_summaries
                WHERE session_id = ? AND summary_type = ?
                ORDER BY created_at ASC, id ASC
                LIMIT ?
            """
            params: tuple[Any, ...] = (session_id, summary_type.value, limit)
        else:
            sql = """
                SELECT * FROM agent_summaries
                WHERE session_id = ?
                ORDER BY created_at ASC, id ASC
                LIMIT ?
            """
            params = (session_id, limit)
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_record(row) for row in rows]

    def get_latest_by_session(
        self,
        session_id: str,
        *,
        summary_type: SummaryType | None = None,
    ) -> SummaryRecord | None:
        """Return the newest summary for one session."""
        if summary_type is not None:
            sql = """
                SELECT * FROM agent_summaries
                WHERE session_id = ? AND summary_type = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """
            params: tuple[Any, ...] = (session_id, summary_type.value)
        else:
            sql = """
                SELECT * FROM agent_summaries
                WHERE session_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """
            params = (session_id,)
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return _row_to_record(row) if row is not None else None

    def get_by_run_id(
        self,
        source_run_id: str,
        *,
        summary_type: SummaryType | None = None,
    ) -> list[SummaryRecord]:
        """Query all summaries produced by a specific review run."""
        if summary_type is not None:
            sql = """
                SELECT * FROM agent_summaries
                WHERE source_run_id = ? AND summary_type = ?
                ORDER BY created_at ASC, id ASC
            """
            params = (source_run_id, summary_type.value)
        else:
            sql = """
                SELECT * FROM agent_summaries
                WHERE source_run_id = ?
                ORDER BY created_at ASC, id ASC
            """
            params = (source_run_id,)
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        records = [_row_to_record(row) for row in rows]
        return sorted(records, key=lambda record: (
            record.block_index is not None,
            record.block_index if record.block_index is not None else -1,
            record.created_at,
            record.id,
        ))

    def get_by_run_id_and_block(
        self,
        source_run_id: str,
        block_index: int,
    ) -> SummaryRecord | None:
        """Query a specific block digest by run id and block index."""
        for record in self.get_by_run_id(
            source_run_id,
            summary_type=SummaryType.BLOCK_DIGEST,
        ):
            if record.block_index == block_index:
                return record
        return None

    def get_by_message_range(
        self,
        session_id: str,
        *,
        msg_log_start: int,
        msg_log_end: int,
        summary_type: SummaryType | None = None,
    ) -> list[SummaryRecord]:
        """Query summaries whose message range overlaps the given bounds."""
        if summary_type is not None:
            sql = """
                SELECT * FROM agent_summaries
                WHERE session_id = ?
                  AND summary_type = ?
                  AND msg_log_start IS NOT NULL
                  AND msg_log_end IS NOT NULL
                  AND msg_log_start <= ?
                  AND msg_log_end >= ?
                ORDER BY created_at ASC
            """
            params: tuple[Any, ...] = (
                session_id, summary_type.value, msg_log_end, msg_log_start,
            )
        else:
            sql = """
                SELECT * FROM agent_summaries
                WHERE session_id = ?
                  AND msg_log_start IS NOT NULL
                  AND msg_log_end IS NOT NULL
                  AND msg_log_start <= ?
                  AND msg_log_end >= ?
                ORDER BY created_at ASC
            """
            params = (session_id, msg_log_end, msg_log_start)
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_record(row) for row in rows]

    def get_latest_by_session_summary(
        self,
        session_id: str,
        *,
        summary_type: SummaryType | None = None,
    ) -> SummaryRecord | None:
        """Alias for get_latest_by_session for backward compatibility."""
        return self.get_latest_by_session(session_id, summary_type=summary_type)


def _row_to_record(row: Any) -> SummaryRecord:
    metadata_json = str(row["metadata_json"] or "{}")
    metadata = _loads_metadata(metadata_json)
    return SummaryRecord(
        id=int(row["id"]),
        session_id=str(row["session_id"]),
        summary_type=SummaryType(str(row["summary_type"])),
        content=str(row["content"] or ""),
        source_run_id=str(row["source_run_id"] or ""),
        block_index=_optional_int(metadata.get("block_index")),
        msg_log_start=int(row["msg_log_start"]) if row["msg_log_start"] is not None else None,
        msg_log_end=int(row["msg_log_end"]) if row["msg_log_end"] is not None else None,
        msg_count=_optional_int(metadata.get("msg_count")) or 0,
        metadata_json=metadata_json,
        created_at=float(row["created_at"]),
    )


def _loads_metadata(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
