"""Model execution repository."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import ModelExecutionRecord

from .model_usage_hourly import ModelUsageHourlyRepositoryMixin


class ModelExecutionRepository(ModelUsageHourlyRepositoryMixin):
    """Persistence adapter for per-call model execution records."""

    def insert(self, record: ModelExecutionRecord) -> None:
        payload = asdict(record)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO model_execution_records (
                    id, route_id, provider_id, model_id, caller, session_id, instance_id, purpose,
                    started_at, first_token_at, finished_at, latency_ms, time_to_first_token_ms,
                    input_tokens, output_tokens, cache_hit, cache_read_tokens, cache_write_tokens,
                    success, error_code, error_message, fallback_from_model_id, fallback_reason,
                    estimated_cost, currency, prompt_snapshot_id, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["id"],
                    payload["route_id"],
                    payload["provider_id"],
                    payload["model_id"],
                    payload["caller"],
                    payload["session_id"],
                    payload["instance_id"],
                    payload["purpose"],
                    payload["started_at"],
                    payload["first_token_at"],
                    payload["finished_at"],
                    payload["latency_ms"],
                    payload["time_to_first_token_ms"],
                    payload["input_tokens"],
                    payload["output_tokens"],
                    1 if payload["cache_hit"] else 0,
                    payload["cache_read_tokens"],
                    payload["cache_write_tokens"],
                    1 if payload["success"] else 0,
                    payload["error_code"],
                    payload["error_message"],
                    payload["fallback_from_model_id"],
                    payload["fallback_reason"],
                    payload["estimated_cost"],
                    payload["currency"],
                    payload["prompt_snapshot_id"],
                    self.json_dumps(payload["metadata"]),
                ),
            )
            self._increment_usage_hourly(conn, payload)

    def list_recent(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM model_execution_records
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [
            {
                **{k: row[k] for k in row.keys() if k != "metadata_json"},
                "cache_hit": bool(row["cache_hit"]),
                "success": bool(row["success"]),
                "metadata": self.json_loads(row["metadata_json"], {}),
            }
            for row in rows
        ]
