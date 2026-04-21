"""AI interaction and prompt snapshot repositories."""

from __future__ import annotations

import time
from typing import Any

from shinbot.persistence.records import AIInteractionRecord, PromptSnapshotRecord

from .base import _json_dumps, _json_loads


class AIInteractionRepository:
    """Persistence adapter for AI decision audit records."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def insert(self, record: AIInteractionRecord) -> int:
        """Insert an AI interaction record and return the auto-incremented id."""
        with self._db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO ai_interactions (
                    execution_id, trigger_id, response_id,
                    timestamp, latency_ms,
                    input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                    model_id, provider_id,
                    think_text, injected_context_json, tool_calls_json, prompt_snapshot_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.execution_id,
                    record.trigger_id,
                    record.response_id,
                    record.timestamp,
                    record.latency_ms,
                    record.input_tokens,
                    record.output_tokens,
                    record.cache_read_tokens,
                    record.cache_write_tokens,
                    record.model_id,
                    record.provider_id,
                    record.think_text,
                    record.injected_context_json,
                    record.tool_calls_json,
                    record.prompt_snapshot_id,
                ),
            )
            return cursor.lastrowid  # type: ignore[return-value]

    def get_by_execution(self, execution_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM ai_interactions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    def attach_message_links(
        self,
        execution_id: str,
        *,
        trigger_id: int | None = None,
        response_id: int | None = None,
    ) -> bool:
        with self._db.connect() as conn:
            cursor = conn.execute(
                """
                UPDATE ai_interactions
                SET
                    trigger_id = COALESCE(?, trigger_id),
                    response_id = COALESCE(?, response_id)
                WHERE execution_id = ?
                """,
                (trigger_id, response_id, execution_id),
            )
            return cursor.rowcount > 0

    def list_by_session(
        self,
        session_id: str,
        *,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return AI interactions whose trigger message belongs to the given session."""
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT ai.*
                FROM ai_interactions AS ai
                JOIN message_logs AS ml ON ml.id = ai.trigger_id
                WHERE ml.session_id = ?
                ORDER BY ai.id DESC
                LIMIT ?
                """,
                (session_id, limit),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        return {
            "id": row["id"],
            "execution_id": row["execution_id"],
            "trigger_id": row["trigger_id"],
            "response_id": row["response_id"],
            "timestamp": row["timestamp"],
            "latency_ms": row["latency_ms"],
            "input_tokens": row["input_tokens"],
            "output_tokens": row["output_tokens"],
            "cache_read_tokens": row["cache_read_tokens"],
            "cache_write_tokens": row["cache_write_tokens"],
            "model_id": row["model_id"],
            "provider_id": row["provider_id"],
            "think_text": row["think_text"],
            "injected_context_json": row["injected_context_json"],
            "tool_calls_json": row["tool_calls_json"],
            "prompt_snapshot_id": row["prompt_snapshot_id"],
        }


class PromptSnapshotRepository:
    """Persistence adapter for TTL-based prompt snapshots."""

    SNAPSHOT_TTL_SECONDS = 10800  # 3 hours

    def __init__(self, db: Any) -> None:
        self._db = db

    def insert(self, record: PromptSnapshotRecord) -> None:
        expires_at = record.expires_at
        if expires_at is None:
            expires_at = record.created_at + self._db.config.snapshot_ttl

        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO prompt_snapshots (
                    id, profile_id, caller, session_id, instance_id, route_id,
                    model_id, prompt_signature, cache_key, messages_json, tools_json,
                    compatibility_used, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.profile_id,
                    record.caller,
                    record.session_id,
                    record.instance_id,
                    record.route_id,
                    record.model_id,
                    record.prompt_signature,
                    record.cache_key,
                    _json_dumps(record.messages),
                    _json_dumps(record.tools),
                    1 if record.compatibility_used else 0,
                    record.created_at,
                    expires_at,
                ),
            )
            # Lazy TTL cleanup: remove expired snapshots on each insert
            conn.execute(
                "DELETE FROM prompt_snapshots WHERE expires_at < ?",
                (time.time(),),
            )

    def get(self, snapshot_id: str) -> dict[str, Any] | None:
        now = time.time()
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM prompt_snapshots WHERE id = ? AND expires_at >= ?",
                (snapshot_id, now),
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "profile_id": row["profile_id"],
            "caller": row["caller"],
            "session_id": row["session_id"],
            "instance_id": row["instance_id"],
            "route_id": row["route_id"],
            "model_id": row["model_id"],
            "prompt_signature": row["prompt_signature"],
            "cache_key": row["cache_key"],
            "messages": _json_loads(row["messages_json"], []),
            "tools": _json_loads(row["tools_json"], []),
            "compatibility_used": bool(row["compatibility_used"]),
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
        }
