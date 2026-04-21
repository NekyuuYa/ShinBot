"""Model registry and execution repositories."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import (
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
)

from .base import _json_dumps, _json_loads


class ModelRegistryRepository:
    """Persistence adapter for provider/model/route metadata."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def upsert_provider(self, record: ModelProviderRecord) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT provider_uuid, created_at FROM model_providers WHERE id = ?",
                (payload["id"],),
            ).fetchone()
            provider_uuid = str(row["provider_uuid"]) if row is not None else str(uuid.uuid4())
            created_at = row["created_at"] if row is not None else payload["created_at"]
            conn.execute(
                """
                INSERT INTO model_providers (
                    provider_uuid, id, type, display_name, capability_type, base_url, auth_json,
                    default_params_json, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider_uuid) DO UPDATE SET
                    type = excluded.type,
                    id = excluded.id,
                    display_name = excluded.display_name,
                    capability_type = excluded.capability_type,
                    base_url = excluded.base_url,
                    auth_json = excluded.auth_json,
                    default_params_json = excluded.default_params_json,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    provider_uuid,
                    payload["id"],
                    payload["type"],
                    payload["display_name"],
                    payload["capability_type"],
                    payload["base_url"],
                    _json_dumps(payload["auth"]),
                    _json_dumps(payload["default_params"]),
                    1 if payload["enabled"] else 0,
                    created_at,
                    payload["updated_at"],
                ),
            )

    def list_providers(self) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM model_providers
                ORDER BY id ASC
                """
            ).fetchall()
        return [
            {
                "provider_uuid": row["provider_uuid"],
                "id": row["id"],
                "type": row["type"],
                "display_name": row["display_name"],
                "capability_type": row["capability_type"],
                "base_url": row["base_url"],
                "auth": _json_loads(row["auth_json"], {}),
                "default_params": _json_loads(row["default_params_json"], {}),
                "enabled": bool(row["enabled"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def get_provider(self, provider_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM model_providers
                WHERE id = ?
                """,
                (provider_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "provider_uuid": row["provider_uuid"],
            "id": row["id"],
            "type": row["type"],
            "display_name": row["display_name"],
            "capability_type": row["capability_type"],
            "base_url": row["base_url"],
            "auth": _json_loads(row["auth_json"], {}),
            "default_params": _json_loads(row["default_params_json"], {}),
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def delete_provider(self, provider_id: str) -> int:
        with self._db.connect() as conn:
            cursor = conn.execute("DELETE FROM model_providers WHERE id = ?", (provider_id,))
            return int(cursor.rowcount)

    def rename_provider(self, provider_id: str, new_provider_id: str) -> None:
        with self._db.connect() as conn:
            conn.execute(
                "UPDATE model_providers SET id = ? WHERE id = ?",
                (new_provider_id, provider_id),
            )

    def upsert_model(self, record: ModelDefinitionRecord) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
            provider_row = conn.execute(
                "SELECT provider_uuid FROM model_providers WHERE id = ?",
                (payload["provider_id"],),
            ).fetchone()
            if provider_row is None:
                raise ValueError(f"Provider {payload['provider_id']!r} not found")
            conn.execute(
                """
                INSERT INTO model_definitions (
                    id, provider_uuid, litellm_model, display_name, capabilities_json, context_window,
                    default_params_json, cost_metadata_json, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    provider_uuid = excluded.provider_uuid,
                    litellm_model = excluded.litellm_model,
                    display_name = excluded.display_name,
                    capabilities_json = excluded.capabilities_json,
                    context_window = excluded.context_window,
                    default_params_json = excluded.default_params_json,
                    cost_metadata_json = excluded.cost_metadata_json,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["id"],
                    provider_row["provider_uuid"],
                    payload["litellm_model"],
                    payload["display_name"],
                    _json_dumps(payload["capabilities"]),
                    payload["context_window"],
                    _json_dumps(payload["default_params"]),
                    _json_dumps(payload["cost_metadata"]),
                    1 if payload["enabled"] else 0,
                    payload["created_at"],
                    payload["updated_at"],
                ),
            )

    def list_models(self, *, provider_id: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT
                m.*,
                p.id AS provider_id
            FROM model_definitions AS m
            JOIN model_providers AS p ON p.provider_uuid = m.provider_uuid
        """
        params: tuple[Any, ...] = ()
        if provider_id:
            query += " WHERE p.id = ?"
            params = (provider_id,)
        query += " ORDER BY p.id ASC, m.id ASC"

        with self._db.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "id": row["id"],
                "provider_id": row["provider_id"],
                "litellm_model": row["litellm_model"],
                "display_name": row["display_name"],
                "capabilities": _json_loads(row["capabilities_json"], []),
                "context_window": row["context_window"],
                "default_params": _json_loads(row["default_params_json"], {}),
                "cost_metadata": _json_loads(row["cost_metadata_json"], {}),
                "enabled": bool(row["enabled"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def get_model(self, model_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    m.*,
                    p.id AS provider_id
                FROM model_definitions AS m
                JOIN model_providers AS p ON p.provider_uuid = m.provider_uuid
                WHERE m.id = ?
                """,
                (model_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "provider_id": row["provider_id"],
            "litellm_model": row["litellm_model"],
            "display_name": row["display_name"],
            "capabilities": _json_loads(row["capabilities_json"], []),
            "context_window": row["context_window"],
            "default_params": _json_loads(row["default_params_json"], {}),
            "cost_metadata": _json_loads(row["cost_metadata_json"], {}),
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def delete_model(self, model_id: str) -> int:
        with self._db.connect() as conn:
            cursor = conn.execute("DELETE FROM model_definitions WHERE id = ?", (model_id,))
            return int(cursor.rowcount)

    def upsert_route(
        self,
        record: ModelRouteRecord,
        *,
        members: list[ModelRouteMemberRecord] | None = None,
    ) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT INTO model_routes (
                    id, purpose, strategy, enabled, sticky_sessions, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    purpose = excluded.purpose,
                    strategy = excluded.strategy,
                    enabled = excluded.enabled,
                    sticky_sessions = excluded.sticky_sessions,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["id"],
                    payload["purpose"],
                    payload["strategy"],
                    1 if payload["enabled"] else 0,
                    1 if payload["sticky_sessions"] else 0,
                    _json_dumps(payload["metadata"]),
                    payload["created_at"],
                    payload["updated_at"],
                ),
            )
            if members is not None:
                conn.execute("DELETE FROM model_route_members WHERE route_id = ?", (record.id,))
                for member in members:
                    conn.execute(
                        """
                        INSERT INTO model_route_members (
                            route_id, model_id, priority, weight, conditions_json, timeout_override, enabled
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            member.route_id,
                            member.model_id,
                            member.priority,
                            member.weight,
                            _json_dumps(member.conditions),
                            member.timeout_override,
                            1 if member.enabled else 0,
                        ),
                    )

    def list_routes(self) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM model_routes
                ORDER BY id ASC
                """
            ).fetchall()
        return [self._route_row_to_dict(row) for row in rows]

    def get_route(self, route_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM model_routes
                WHERE id = ?
                """,
                (route_id,),
            ).fetchone()
        if row is None:
            return None
        return self._route_row_to_dict(row)

    def delete_route(self, route_id: str) -> int:
        with self._db.connect() as conn:
            cursor = conn.execute("DELETE FROM model_routes WHERE id = ?", (route_id,))
            return int(cursor.rowcount)

    def rename_route(self, route_id: str, new_route_id: str) -> None:
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM model_routes WHERE id = ?",
                (route_id,),
            ).fetchone()
            if row is None:
                return
            conn.execute(
                """
                INSERT INTO model_routes (
                    id, purpose, strategy, enabled, sticky_sessions, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    new_route_id,
                    row["purpose"],
                    row["strategy"],
                    row["enabled"],
                    row["sticky_sessions"],
                    row["metadata_json"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
            conn.execute(
                "UPDATE model_route_members SET route_id = ? WHERE route_id = ?",
                (new_route_id, route_id),
            )
            conn.execute("DELETE FROM model_routes WHERE id = ?", (route_id,))

    def list_route_members(self, route_id: str) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM model_route_members
                WHERE route_id = ?
                ORDER BY priority ASC, id ASC
                """,
                (route_id,),
            ).fetchall()
        return [
            {
                "model_id": row["model_id"],
                "priority": row["priority"],
                "weight": row["weight"],
                "conditions": _json_loads(row["conditions_json"], {}),
                "timeout_override": row["timeout_override"],
                "enabled": bool(row["enabled"]),
            }
            for row in rows
        ]

    def _route_row_to_dict(self, row: Any) -> dict[str, Any]:
        return {
            "id": row["id"],
            "purpose": row["purpose"],
            "strategy": row["strategy"],
            "enabled": bool(row["enabled"]),
            "sticky_sessions": bool(row["sticky_sessions"]),
            "metadata": _json_loads(row["metadata_json"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


class ModelExecutionRepository:
    """Persistence adapter for per-call model execution metrics."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def insert(self, record: ModelExecutionRecord) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
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
                    _json_dumps(payload["metadata"]),
                ),
            )

    def list_recent(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
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
                "metadata": _json_loads(row["metadata_json"], {}),
            }
            for row in rows
        ]

    def summarize_tokens(
        self,
        *,
        since: str | None = None,
        top_model_limit: int = 5,
    ) -> dict[str, Any]:
        where_clause = "WHERE started_at >= ?" if since else ""
        params: tuple[Any, ...] = (since,) if since else ()

        with self._db.connect() as conn:
            summary = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS total_calls,
                    COALESCE(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), 0) AS successful_calls,
                    COALESCE(SUM(input_tokens), 0) AS input_tokens,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens,
                    COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
                    COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens,
                    COALESCE(SUM(estimated_cost), 0) AS estimated_cost
                FROM model_execution_records
                {where_clause}
                """,
                params,
            ).fetchone()
            model_rows = conn.execute(
                f"""
                SELECT
                    provider_id,
                    model_id,
                    COUNT(*) AS total_calls,
                    COALESCE(SUM(input_tokens), 0) AS input_tokens,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens,
                    COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
                    COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens
                FROM model_execution_records
                {where_clause}
                GROUP BY provider_id, model_id
                ORDER BY
                    (COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0)) DESC,
                    total_calls DESC,
                    model_id ASC
                LIMIT ?
                """,
                (*params, top_model_limit),
            ).fetchall()

        input_tokens = int(summary["input_tokens"] or 0)
        output_tokens = int(summary["output_tokens"] or 0)
        cache_read_tokens = int(summary["cache_read_tokens"] or 0)
        cache_write_tokens = int(summary["cache_write_tokens"] or 0)
        return {
            "total_calls": int(summary["total_calls"] or 0),
            "successful_calls": int(summary["successful_calls"] or 0),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "cache_read_tokens": cache_read_tokens,
            "cache_write_tokens": cache_write_tokens,
            "estimated_cost": float(summary["estimated_cost"] or 0),
            "top_models": [
                {
                    "provider_id": row["provider_id"],
                    "model_id": row["model_id"],
                    "total_calls": int(row["total_calls"] or 0),
                    "input_tokens": int(row["input_tokens"] or 0),
                    "output_tokens": int(row["output_tokens"] or 0),
                    "total_tokens": int(row["input_tokens"] or 0) + int(row["output_tokens"] or 0),
                    "cache_read_tokens": int(row["cache_read_tokens"] or 0),
                    "cache_write_tokens": int(row["cache_write_tokens"] or 0),
                }
                for row in model_rows
            ],
        }
