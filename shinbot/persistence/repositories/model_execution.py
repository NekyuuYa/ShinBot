"""Model execution repository."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import ModelExecutionRecord

from .model_usage_hourly import ModelUsageHourlyRepositoryMixin


class ModelExecutionRepository(ModelUsageHourlyRepositoryMixin):
    """Persistence adapter for per-call model execution records."""

    def insert(self, record: ModelExecutionRecord) -> None:
        """Insert a model execution record and update hourly usage aggregates.

        Args:
            record: The execution record to persist.
        """
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

    def _row_to_dict(self, row: Any) -> dict[str, Any]:
        return {
            **{k: row[k] for k in row.keys() if k != "metadata_json"},
            "cache_hit": bool(row["cache_hit"]),
            "success": bool(row["success"]),
            "metadata": self.json_loads(row["metadata_json"], {}),
        }

    def list_recent(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """Return the most recent execution records.

        Args:
            limit: Maximum number of records to return.
        """
        return self.list_audit_records(limit=limit)["items"]

    def list_audit_records(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        provider_id: str | None = None,
        model_id: str | None = None,
        route_id: str | None = None,
        caller: str | None = None,
        session_id: str | None = None,
        instance_id: str | None = None,
        success: bool | None = None,
        query: str | None = None,
    ) -> dict[str, Any]:
        """Return paginated, filterable execution audit records.

        Args:
            limit: Page size.
            offset: Number of records to skip.
            provider_id: Filter by provider ID.
            model_id: Filter by model ID.
            route_id: Filter by route ID.
            caller: Filter by caller identifier.
            session_id: Filter by session ID.
            instance_id: Filter by instance ID.
            success: When ``True``/``False`` filter by success status.
            query: Free-text search across multiple columns.

        Returns:
            Dictionary with ``items``, ``total``, ``limit``, and ``offset``.
        """
        filters: list[str] = []
        params: list[Any] = []

        exact_filters = {
            "provider_id": provider_id,
            "model_id": model_id,
            "route_id": route_id,
            "caller": caller,
            "session_id": session_id,
            "instance_id": instance_id,
        }
        for column, value in exact_filters.items():
            if value:
                filters.append(f"{column} = ?")
                params.append(value)

        if success is not None:
            filters.append("success = ?")
            params.append(1 if success else 0)

        if query:
            like_query = f"%{query}%"
            filters.append(
                "("
                "id LIKE ? OR caller LIKE ? OR session_id LIKE ? OR instance_id LIKE ? "
                "OR purpose LIKE ? OR error_code LIKE ? OR error_message LIKE ?"
                ")"
            )
            params.extend([like_query] * 7)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        with self.connect() as conn:
            total = conn.execute(
                f"""
                SELECT COUNT(*) AS total
                FROM model_execution_records
                {where_clause}
                """,
                params,
            ).fetchone()["total"]
            rows = conn.execute(
                f"""
                SELECT *
                FROM model_execution_records
                {where_clause}
                ORDER BY started_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()

        return {
            "items": [self._row_to_dict(row) for row in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
        }
