"""Persistence repositories for attention state and workflow runs."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from shinbot.agent.attention.models import (
    SenderWeightState,
    SessionAttentionState,
    WorkflowRunRecord,
)

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


class AttentionRepository:
    """CRUD for session_attention_states and sender_weight_states."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    # ── SessionAttentionState ───────────────────────────────────────

    def get_attention(self, session_id: str) -> SessionAttentionState | None:
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM session_attention_states WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        if row is None:
            return None
        return SessionAttentionState(
            session_id=row["session_id"],
            attention_value=row["attention_value"],
            base_threshold=row["base_threshold"],
            runtime_threshold_offset=row["runtime_threshold_offset"],
            cooldown_until=row["cooldown_until"],
            last_update_at=row["last_update_at"],
            last_consumed_msg_log_id=row["last_consumed_msg_log_id"],
            last_trigger_msg_log_id=row["last_trigger_msg_log_id"],
            metadata=json.loads(row["metadata_json"] or "{}"),
        )

    def get_or_create_attention(
        self,
        session_id: str,
        base_threshold: float = 5.0,
    ) -> SessionAttentionState:
        state = self.get_attention(session_id)
        if state is not None:
            return state
        state = SessionAttentionState(session_id=session_id, base_threshold=base_threshold)
        self.save_attention(state)
        return state

    def save_attention(self, state: SessionAttentionState) -> None:
        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT INTO session_attention_states (
                    session_id, attention_value, base_threshold,
                    runtime_threshold_offset, cooldown_until, last_update_at,
                    last_consumed_msg_log_id, last_trigger_msg_log_id, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    attention_value = excluded.attention_value,
                    base_threshold = excluded.base_threshold,
                    runtime_threshold_offset = excluded.runtime_threshold_offset,
                    cooldown_until = excluded.cooldown_until,
                    last_update_at = excluded.last_update_at,
                    last_consumed_msg_log_id = excluded.last_consumed_msg_log_id,
                    last_trigger_msg_log_id = excluded.last_trigger_msg_log_id,
                    metadata_json = excluded.metadata_json
                """,
                (
                    state.session_id,
                    state.attention_value,
                    state.base_threshold,
                    state.runtime_threshold_offset,
                    state.cooldown_until,
                    state.last_update_at,
                    state.last_consumed_msg_log_id,
                    state.last_trigger_msg_log_id,
                    _json_dumps(state.metadata),
                ),
            )

    # ── SenderWeightState ───────────────────────────────────────────

    def get_sender_weight(
        self,
        session_id: str,
        sender_id: str,
    ) -> SenderWeightState | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM sender_weight_states
                WHERE session_id = ? AND sender_id = ?
                """,
                (session_id, sender_id),
            ).fetchone()
        if row is None:
            return None
        return SenderWeightState(
            session_id=row["session_id"],
            sender_id=row["sender_id"],
            stable_weight=row["stable_weight"],
            runtime_weight=row["runtime_weight"],
            last_runtime_adjust_at=row["last_runtime_adjust_at"],
        )

    def get_or_create_sender_weight(
        self,
        session_id: str,
        sender_id: str,
    ) -> SenderWeightState:
        state = self.get_sender_weight(session_id, sender_id)
        if state is not None:
            return state
        state = SenderWeightState(session_id=session_id, sender_id=sender_id)
        self.save_sender_weight(state)
        return state

    def save_sender_weight(self, state: SenderWeightState) -> None:
        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sender_weight_states (
                    session_id, sender_id, stable_weight,
                    runtime_weight, last_runtime_adjust_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(session_id, sender_id) DO UPDATE SET
                    stable_weight = excluded.stable_weight,
                    runtime_weight = excluded.runtime_weight,
                    last_runtime_adjust_at = excluded.last_runtime_adjust_at
                """,
                (
                    state.session_id,
                    state.sender_id,
                    state.stable_weight,
                    state.runtime_weight,
                    state.last_runtime_adjust_at,
                ),
            )

    def list_sender_weights(self, session_id: str) -> list[SenderWeightState]:
        with self._db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM sender_weight_states WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        return [
            SenderWeightState(
                session_id=row["session_id"],
                sender_id=row["sender_id"],
                stable_weight=row["stable_weight"],
                runtime_weight=row["runtime_weight"],
                last_runtime_adjust_at=row["last_runtime_adjust_at"],
            )
            for row in rows
        ]

    def commit_batch_consumption(
        self,
        session_id: str,
        last_msg_id: int,
        threshold_to_deduct: float,
    ) -> None:
        """Atomically advance cursor and deduct threshold from attention_value.

        Uses a single UPDATE so it cannot race with apply_reply_fatigue, which
        only touches runtime_threshold_offset and cooldown_until.  Safe to call
        even if incremental-merge already advanced last_consumed_msg_log_id via
        update_consumed_cursor (MAX guard prevents regression).
        """
        with self._db.connect() as conn:
            conn.execute(
                """
                UPDATE session_attention_states
                SET attention_value = MAX(attention_value - ?, 0.0),
                    last_trigger_msg_log_id = ?,
                    last_consumed_msg_log_id = MAX(
                        COALESCE(last_consumed_msg_log_id, 0), ?
                    )
                WHERE session_id = ?
                """,
                (threshold_to_deduct, last_msg_id, last_msg_id, session_id),
            )
            conn.execute(
                """
                UPDATE message_logs
                SET is_read = 1
                WHERE session_id = ? AND id <= ? AND role = 'user'
                """,
                (session_id, last_msg_id),
            )

    def update_consumed_cursor(self, session_id: str, msg_log_id: int) -> None:
        """Atomically advance last_consumed_msg_log_id without touching other fields.

        Uses a MAX guard so concurrent updates never regress the cursor.
        """
        with self._db.connect() as conn:
            conn.execute(
                """
                UPDATE session_attention_states
                SET last_consumed_msg_log_id = MAX(
                    COALESCE(last_consumed_msg_log_id, 0), ?
                )
                WHERE session_id = ?
                """,
                (msg_log_id, session_id),
            )
            conn.execute(
                """
                UPDATE message_logs
                SET is_read = 1
                WHERE session_id = ? AND id <= ? AND role = 'user'
                """,
                (session_id, msg_log_id),
            )

    def update_metadata(self, session_id: str, metadata: dict[str, Any]) -> None:
        """Atomically replace metadata_json without touching other fields."""
        with self._db.connect() as conn:
            conn.execute(
                """
                UPDATE session_attention_states
                SET metadata_json = ?
                WHERE session_id = ?
                """,
                (_json_dumps(metadata), session_id),
            )

    def clear_metadata_key(self, session_id: str, key: str) -> None:
        """Remove a single key from metadata_json atomically."""
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT metadata_json FROM session_attention_states WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                return
            meta = json.loads(row["metadata_json"] or "{}")
            if key in meta:
                del meta[key]
                conn.execute(
                    "UPDATE session_attention_states SET metadata_json = ? WHERE session_id = ?",
                    (_json_dumps(meta), session_id),
                )

    def set_metadata_key(self, session_id: str, key: str, value: Any) -> None:
        """Set a single key in metadata_json atomically."""
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT metadata_json FROM session_attention_states WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                return
            meta = json.loads(row["metadata_json"] or "{}")
            meta[key] = value
            conn.execute(
                "UPDATE session_attention_states SET metadata_json = ? WHERE session_id = ?",
                (_json_dumps(meta), session_id),
            )

    def cleanup_stale_weights(self, max_age_seconds: float = 86400 * 30) -> int:
        cutoff = time.time() - max_age_seconds
        with self._db.connect() as conn:
            cursor = conn.execute(
                """
                DELETE FROM sender_weight_states
                WHERE stable_weight = 0.0 AND runtime_weight = 0.0
                  AND last_runtime_adjust_at < ?
                """,
                (cutoff,),
            )
            return cursor.rowcount


class WorkflowRunRepository:
    """Persistence for workflow run audit records."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def insert(self, record: WorkflowRunRecord) -> None:
        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT INTO workflow_runs (
                    id, session_id, instance_id,
                    response_profile,
                    batch_start_msg_id, batch_end_msg_id, batch_size,
                    trigger_attention, effective_threshold,
                    tool_calls_json, replied, response_summary,
                    started_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.session_id,
                    record.instance_id,
                    record.response_profile,
                    record.batch_start_msg_id,
                    record.batch_end_msg_id,
                    record.batch_size,
                    record.trigger_attention,
                    record.effective_threshold,
                    _json_dumps(record.tool_calls),
                    1 if record.replied else 0,
                    record.response_summary,
                    record.started_at,
                    record.finished_at,
                ),
            )

    def list_by_session(
        self,
        session_id: str,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workflow_runs
                WHERE session_id = ?
                ORDER BY started_at DESC LIMIT ?
                """,
                (session_id, limit),
            ).fetchall()
        return [
            {
                "id": row["id"],
                "session_id": row["session_id"],
                "instance_id": row["instance_id"],
                "response_profile": row["response_profile"],
                "batch_start_msg_id": row["batch_start_msg_id"],
                "batch_end_msg_id": row["batch_end_msg_id"],
                "batch_size": row["batch_size"],
                "trigger_attention": row["trigger_attention"],
                "effective_threshold": row["effective_threshold"],
                "tool_calls": json.loads(row["tool_calls_json"] or "[]"),
                "replied": bool(row["replied"]),
                "response_summary": row["response_summary"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
            }
            for row in rows
        ]
