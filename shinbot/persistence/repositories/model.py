"""Model registry and execution repositories."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

from shinbot.persistence.records import (
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
)

from .base import _json_dumps, _json_loads


def _parse_utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _hour_bucket_start(value: str) -> str:
    return _parse_utc_datetime(value).replace(minute=0, second=0, microsecond=0).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _iter_cost_metadata_maps(cost_metadata: dict[str, Any]) -> list[dict[str, Any]]:
    maps = [cost_metadata]
    for key in ("pricing", "prices", "costs"):
        nested = cost_metadata.get(key)
        if isinstance(nested, dict):
            maps.append(nested)
    return maps


def _cost_rate_per_token(cost_metadata: dict[str, Any], candidate_groups: list[list[str]]) -> float:
    metadata_maps = _iter_cost_metadata_maps(cost_metadata)

    for keys in candidate_groups:
        for metadata in metadata_maps:
            for key in keys:
                value = _safe_float(metadata.get(key))
                if value is not None:
                    if "PerMillion" in key or "_per_million_" in key:
                        return value / 1_000_000
                    if "Per1k" in key or "_per_1k_" in key:
                        return value / 1_000
                    return value
    return 0.0


def _estimate_cost_from_metadata(
    cost_metadata: dict[str, Any],
    *,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int,
    cache_write_tokens: int,
) -> float:
    if not isinstance(cost_metadata, dict) or not cost_metadata:
        return 0.0

    input_rate = _cost_rate_per_token(
        cost_metadata,
        [
            ["inputPerToken", "promptPerToken", "input_per_token", "prompt_per_token"],
            [
                "inputPer1kTokens",
                "promptPer1kTokens",
                "input_per_1k_tokens",
                "prompt_per_1k_tokens",
            ],
            [
                "inputPerMillionTokens",
                "promptPerMillionTokens",
                "input_per_million_tokens",
                "prompt_per_million_tokens",
            ],
        ],
    )
    output_rate = _cost_rate_per_token(
        cost_metadata,
        [
            ["outputPerToken", "completionPerToken", "output_per_token", "completion_per_token"],
            [
                "outputPer1kTokens",
                "completionPer1kTokens",
                "output_per_1k_tokens",
                "completion_per_1k_tokens",
            ],
            [
                "outputPerMillionTokens",
                "completionPerMillionTokens",
                "output_per_million_tokens",
                "completion_per_million_tokens",
            ],
        ],
    )
    cache_read_rate = _cost_rate_per_token(
        cost_metadata,
        [
            ["cacheReadPerToken", "cache_read_per_token"],
            ["cacheReadPer1kTokens", "cache_read_per_1k_tokens"],
            ["cacheReadPerMillionTokens", "cache_read_per_million_tokens"],
        ],
    )
    cache_write_rate = _cost_rate_per_token(
        cost_metadata,
        [
            ["cacheWritePerToken", "cache_write_per_token"],
            ["cacheWritePer1kTokens", "cache_write_per_1k_tokens"],
            ["cacheWritePerMillionTokens", "cache_write_per_million_tokens"],
        ],
    )

    total = (
        input_tokens * input_rate
        + output_tokens * output_rate
        + cache_read_tokens * cache_read_rate
        + cache_write_tokens * cache_write_rate
    )
    return round(total, 6)


def _zero_usage_metrics() -> dict[str, Any]:
    return {
        "total_calls": 0,
        "successful_calls": 0,
        "failed_calls": 0,
        "cache_hits": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "estimated_cost": 0.0,
    }


def _add_usage_metrics(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key in (
        "total_calls",
        "successful_calls",
        "failed_calls",
        "cache_hits",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "estimated_cost",
    ):
        target[key] = target.get(key, 0) + source.get(key, 0)


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
            self._increment_usage_hourly(conn, payload)

    def _increment_usage_hourly(self, conn: Any, payload: dict[str, Any]) -> None:
        model_id = str(payload.get("model_id") or "").strip()
        provider_id = str(payload.get("provider_id") or "").strip()
        started_at = str(payload.get("started_at") or "").strip()
        if not model_id or not provider_id or not started_at:
            return

        bucket_start = _hour_bucket_start(started_at)
        latency_ms = float(payload.get("latency_ms") or 0.0)
        ttft_ms = payload.get("time_to_first_token_ms")
        ttft_value = float(ttft_ms or 0.0) if ttft_ms is not None else 0.0

        conn.execute(
            """
            INSERT INTO model_usage_hourly (
                bucket_start, provider_id, model_id,
                total_calls, successful_calls, failed_calls, cache_hits,
                input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                total_latency_ms, latency_sample_count, total_ttft_ms, ttft_sample_count,
                last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket_start, provider_id, model_id) DO UPDATE SET
                total_calls = model_usage_hourly.total_calls + excluded.total_calls,
                successful_calls = model_usage_hourly.successful_calls + excluded.successful_calls,
                failed_calls = model_usage_hourly.failed_calls + excluded.failed_calls,
                cache_hits = model_usage_hourly.cache_hits + excluded.cache_hits,
                input_tokens = model_usage_hourly.input_tokens + excluded.input_tokens,
                output_tokens = model_usage_hourly.output_tokens + excluded.output_tokens,
                cache_read_tokens = model_usage_hourly.cache_read_tokens + excluded.cache_read_tokens,
                cache_write_tokens = model_usage_hourly.cache_write_tokens + excluded.cache_write_tokens,
                total_latency_ms = model_usage_hourly.total_latency_ms + excluded.total_latency_ms,
                latency_sample_count = model_usage_hourly.latency_sample_count + excluded.latency_sample_count,
                total_ttft_ms = model_usage_hourly.total_ttft_ms + excluded.total_ttft_ms,
                ttft_sample_count = model_usage_hourly.ttft_sample_count + excluded.ttft_sample_count,
                last_seen_at = CASE
                    WHEN excluded.last_seen_at > model_usage_hourly.last_seen_at
                    THEN excluded.last_seen_at
                    ELSE model_usage_hourly.last_seen_at
                END
            """,
            (
                bucket_start,
                provider_id,
                model_id,
                1,
                1 if payload.get("success") else 0,
                0 if payload.get("success") else 1,
                1 if payload.get("cache_hit") else 0,
                int(payload.get("input_tokens") or 0),
                int(payload.get("output_tokens") or 0),
                int(payload.get("cache_read_tokens") or 0),
                int(payload.get("cache_write_tokens") or 0),
                latency_ms,
                1 if latency_ms > 0 else 0,
                ttft_value,
                1 if ttft_ms is not None and ttft_value > 0 else 0,
                started_at,
            ),
        )

    def _ensure_usage_hourly_matches_records(self, conn: Any) -> None:
        records_total = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM model_execution_records
            WHERE provider_id != '' AND model_id != '' AND started_at != ''
            """
        ).fetchone()
        hourly_total = conn.execute(
            """
            SELECT COALESCE(SUM(total_calls), 0) AS total
            FROM model_usage_hourly
            """
        ).fetchone()

        if int(records_total["total"] or 0) == int(hourly_total["total"] or 0):
            return

        self._rebuild_usage_hourly_from_records(conn)

    def _rebuild_usage_hourly_from_records(self, conn: Any) -> None:
        conn.execute("DELETE FROM model_usage_hourly")
        conn.execute(
            """
            INSERT INTO model_usage_hourly (
                bucket_start, provider_id, model_id,
                total_calls, successful_calls, failed_calls, cache_hits,
                input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                total_latency_ms, latency_sample_count, total_ttft_ms, ttft_sample_count,
                last_seen_at
            )
            SELECT
                bucket_start,
                provider_id,
                model_id,
                COUNT(*) AS total_calls,
                COALESCE(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), 0) AS successful_calls,
                COALESCE(SUM(CASE WHEN success = 1 THEN 0 ELSE 1 END), 0) AS failed_calls,
                COALESCE(SUM(CASE WHEN cache_hit = 1 THEN 1 ELSE 0 END), 0) AS cache_hits,
                COALESCE(SUM(input_tokens), 0) AS input_tokens,
                COALESCE(SUM(output_tokens), 0) AS output_tokens,
                COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
                COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens,
                COALESCE(SUM(CASE WHEN latency_ms > 0 THEN latency_ms ELSE 0 END), 0) AS total_latency_ms,
                COALESCE(SUM(CASE WHEN latency_ms > 0 THEN 1 ELSE 0 END), 0) AS latency_sample_count,
                COALESCE(SUM(
                    CASE
                        WHEN time_to_first_token_ms > 0 THEN time_to_first_token_ms
                        ELSE 0
                    END
                ), 0) AS total_ttft_ms,
                COALESCE(SUM(
                    CASE
                        WHEN time_to_first_token_ms > 0 THEN 1
                        ELSE 0
                    END
                ), 0) AS ttft_sample_count,
                MAX(started_at) AS last_seen_at
            FROM (
                SELECT
                    COALESCE(
                        strftime('%Y-%m-%dT%H:00:00+00:00', started_at),
                        substr(started_at, 1, 13) || ':00:00+00:00'
                    ) AS bucket_start,
                    *
                FROM model_execution_records
                WHERE provider_id != '' AND model_id != '' AND started_at != ''
            )
            GROUP BY bucket_start, provider_id, model_id
            """
        )

    def _list_usage_rows_since(self, since: str) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            self._ensure_usage_hourly_matches_records(conn)
            rows = conn.execute(
                """
                SELECT *
                FROM model_usage_hourly
                WHERE bucket_start >= ?
                ORDER BY bucket_start ASC, provider_id ASC, model_id ASC
                """,
                (_hour_bucket_start(since),),
            ).fetchall()

        return [{key: row[key] for key in row.keys()} for row in rows]

    def _build_cost_context(self) -> tuple[
        dict[str, dict[str, Any]],
        dict[str, str],
        dict[str, str],
    ]:
        models = self._db.model_registry.list_models()
        providers = self._db.model_registry.list_providers()
        model_map = {str(item["id"]): item for item in models}
        model_names = {
            str(item["id"]): str(item.get("display_name") or item["id"])
            for item in models
        }
        provider_names = {
            str(item["id"]): str(item.get("display_name") or item["id"])
            for item in providers
        }
        return model_map, model_names, provider_names

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

    def analyze_costs(
        self,
        *,
        since: str,
        hourly_since: str,
        model_limit: int = 8,
    ) -> dict[str, Any]:
        daily_start = _parse_utc_datetime(since)
        daily_end = datetime.now(UTC)
        hourly_start = _parse_utc_datetime(hourly_since)
        hourly_end = daily_end.replace(minute=0, second=0, microsecond=0)
        usage_rows = self._list_usage_rows_since(since)
        model_map, model_names, provider_names = self._build_cost_context()

        def _build_daily_bucket_defs() -> list[dict[str, str]]:
            buckets: list[dict[str, str]] = []
            cursor = daily_start.replace(hour=0, minute=0, second=0, microsecond=0)
            final_day = daily_end.replace(hour=0, minute=0, second=0, microsecond=0)
            while cursor <= final_day:
                buckets.append(
                    {
                        "key": cursor.strftime("%Y-%m-%d"),
                        "bucket_start": cursor.isoformat(),
                    }
                )
                cursor += timedelta(days=1)
            return buckets

        def _build_hourly_bucket_defs() -> list[dict[str, str]]:
            buckets: list[dict[str, str]] = []
            cursor = hourly_start.replace(minute=0, second=0, microsecond=0)
            while cursor <= hourly_end:
                buckets.append(
                    {
                        "key": cursor.strftime("%Y-%m-%dT%H"),
                        "bucket_start": cursor.isoformat(),
                    }
                )
                cursor += timedelta(hours=1)
            return buckets

        daily_bucket_defs = _build_daily_bucket_defs()
        hourly_bucket_defs = _build_hourly_bucket_defs()
        daily_bucket_map = {item["key"]: _zero_usage_metrics() for item in daily_bucket_defs}
        hourly_bucket_map = {item["key"]: _zero_usage_metrics() for item in hourly_bucket_defs}
        summary_metrics = _zero_usage_metrics()
        summary_latency_total = 0.0
        summary_latency_samples = 0
        summary_ttft_total = 0.0
        summary_ttft_samples = 0
        model_metrics: dict[str, dict[str, Any]] = {}
        focus_daily_map: dict[str, dict[str, dict[str, Any]]] = {}
        focus_hourly_map: dict[str, dict[str, dict[str, Any]]] = {}

        for row in usage_rows:
            bucket_start = _parse_utc_datetime(str(row["bucket_start"]))
            if bucket_start < daily_start or bucket_start > hourly_end:
                continue

            model_id = str(row["model_id"])
            provider_id = str(row["provider_id"])
            model_payload = model_map.get(model_id, {})
            cost_metadata = (
                model_payload.get("cost_metadata", {})
                if isinstance(model_payload.get("cost_metadata", {}), dict)
                else {}
            )
            bucket_metrics = {
                "total_calls": int(row["total_calls"] or 0),
                "successful_calls": int(row["successful_calls"] or 0),
                "failed_calls": int(row["failed_calls"] or 0),
                "cache_hits": int(row["cache_hits"] or 0),
                "input_tokens": int(row["input_tokens"] or 0),
                "output_tokens": int(row["output_tokens"] or 0),
                "total_tokens": int(row["input_tokens"] or 0) + int(row["output_tokens"] or 0),
                "cache_read_tokens": int(row["cache_read_tokens"] or 0),
                "cache_write_tokens": int(row["cache_write_tokens"] or 0),
                "estimated_cost": _estimate_cost_from_metadata(
                    cost_metadata,
                    input_tokens=int(row["input_tokens"] or 0),
                    output_tokens=int(row["output_tokens"] or 0),
                    cache_read_tokens=int(row["cache_read_tokens"] or 0),
                    cache_write_tokens=int(row["cache_write_tokens"] or 0),
                ),
            }

            _add_usage_metrics(summary_metrics, bucket_metrics)
            daily_key = bucket_start.strftime("%Y-%m-%d")
            _add_usage_metrics(daily_bucket_map[daily_key], bucket_metrics)

            latency_total = float(row["total_latency_ms"] or 0.0)
            latency_samples = int(row["latency_sample_count"] or 0)
            ttft_total = float(row["total_ttft_ms"] or 0.0)
            ttft_samples = int(row["ttft_sample_count"] or 0)
            summary_latency_total += latency_total
            summary_latency_samples += latency_samples
            summary_ttft_total += ttft_total
            summary_ttft_samples += ttft_samples

            model_row = model_metrics.setdefault(
                model_id,
                {
                    "provider_id": provider_id,
                    "provider_display_name": provider_names.get(provider_id, provider_id),
                    "model_id": model_id,
                    "model_display_name": model_names.get(model_id, model_id),
                    **_zero_usage_metrics(),
                    "_latency_total": 0.0,
                    "_latency_samples": 0,
                    "_ttft_total": 0.0,
                    "_ttft_samples": 0,
                    "last_seen_at": "",
                },
            )
            _add_usage_metrics(model_row, bucket_metrics)
            model_row["_latency_total"] += latency_total
            model_row["_latency_samples"] += latency_samples
            model_row["_ttft_total"] += ttft_total
            model_row["_ttft_samples"] += ttft_samples
            last_seen_at = str(row["last_seen_at"] or "")
            if last_seen_at > str(model_row["last_seen_at"] or ""):
                model_row["last_seen_at"] = last_seen_at

            if bucket_start >= hourly_start:
                hourly_key = bucket_start.strftime("%Y-%m-%dT%H")
                _add_usage_metrics(hourly_bucket_map[hourly_key], bucket_metrics)

            daily_model_buckets = focus_daily_map.setdefault(model_id, {})
            _add_usage_metrics(
                daily_model_buckets.setdefault(daily_key, _zero_usage_metrics()),
                bucket_metrics,
            )
            if bucket_start >= hourly_start:
                hourly_model_buckets = focus_hourly_map.setdefault(model_id, {})
                hourly_key = bucket_start.strftime("%Y-%m-%dT%H")
                _add_usage_metrics(
                    hourly_model_buckets.setdefault(hourly_key, _zero_usage_metrics()),
                    bucket_metrics,
                )

        def _build_bucket_series(
            bucket_defs: list[dict[str, str]],
            bucket_map: dict[str, dict[str, Any]],
        ) -> list[dict[str, Any]]:
            series: list[dict[str, Any]] = []
            for bucket_def in bucket_defs:
                metrics = bucket_map.get(bucket_def["key"], _zero_usage_metrics())
                series.append(
                    {
                        "bucket_start": bucket_def["bucket_start"],
                        **metrics,
                    }
                )
            return series

        models: list[dict[str, Any]] = []
        for row in model_metrics.values():
            total_calls = int(row["total_calls"])
            successful_calls = int(row["successful_calls"])
            cache_hits = int(row["cache_hits"])
            payload = {
                "provider_id": row["provider_id"],
                "provider_display_name": row["provider_display_name"],
                "model_id": row["model_id"],
                "model_display_name": row["model_display_name"],
                "total_calls": total_calls,
                "successful_calls": successful_calls,
                "failed_calls": int(row["failed_calls"]),
                "success_rate": (successful_calls / total_calls) if total_calls else 0.0,
                "cache_hits": cache_hits,
                "cache_hit_rate": (cache_hits / total_calls) if total_calls else 0.0,
                "input_tokens": int(row["input_tokens"]),
                "output_tokens": int(row["output_tokens"]),
                "total_tokens": int(row["total_tokens"]),
                "cache_read_tokens": int(row["cache_read_tokens"]),
                "cache_write_tokens": int(row["cache_write_tokens"]),
                "estimated_cost": round(float(row["estimated_cost"]), 6),
                "average_latency_ms": (
                    round(float(row["_latency_total"]) / int(row["_latency_samples"]), 2)
                    if int(row["_latency_samples"]) > 0
                    else None
                ),
                "average_time_to_first_token_ms": (
                    round(float(row["_ttft_total"]) / int(row["_ttft_samples"]), 2)
                    if int(row["_ttft_samples"]) > 0
                    else None
                ),
                "last_seen_at": row["last_seen_at"],
            }
            models.append(payload)

        models.sort(
            key=lambda item: (
                -float(item["estimated_cost"]),
                -int(item["total_tokens"]),
                -int(item["total_calls"]),
                str(item["model_id"]),
            )
        )
        focus_model_payloads = [
            {
                **payload,
                "daily": _build_bucket_series(
                    daily_bucket_defs,
                    focus_daily_map.get(str(payload["model_id"]), {}),
                ),
                "hourly": _build_bucket_series(
                    hourly_bucket_defs,
                    focus_hourly_map.get(str(payload["model_id"]), {}),
                ),
            }
            for payload in models[:model_limit]
        ]

        summary_total_calls = int(summary_metrics["total_calls"])
        summary_successful_calls = int(summary_metrics["successful_calls"])
        summary_cache_hits = int(summary_metrics["cache_hits"])
        summary_input_tokens = int(summary_metrics["input_tokens"])
        summary_output_tokens = int(summary_metrics["output_tokens"])

        return {
            "since": daily_start.isoformat(),
            "hourly_since": hourly_start.isoformat(),
            "currency": "USD",
            "summary": {
                "total_calls": summary_total_calls,
                "successful_calls": summary_successful_calls,
                "failed_calls": int(summary_metrics["failed_calls"]),
                "success_rate": (
                    summary_successful_calls / summary_total_calls if summary_total_calls else 0.0
                ),
                "cache_hits": summary_cache_hits,
                "cache_hit_rate": (
                    summary_cache_hits / summary_total_calls if summary_total_calls else 0.0
                ),
                "input_tokens": summary_input_tokens,
                "output_tokens": summary_output_tokens,
                "total_tokens": summary_input_tokens + summary_output_tokens,
                "cache_read_tokens": int(summary_metrics["cache_read_tokens"]),
                "cache_write_tokens": int(summary_metrics["cache_write_tokens"]),
                "estimated_cost": round(float(summary_metrics["estimated_cost"]), 6),
                "average_latency_ms": (
                    round(summary_latency_total / summary_latency_samples, 2)
                    if summary_latency_samples > 0
                    else None
                ),
                "average_time_to_first_token_ms": (
                    round(summary_ttft_total / summary_ttft_samples, 2)
                    if summary_ttft_samples > 0
                    else None
                ),
            },
            "timeline": {
                "daily": _build_bucket_series(daily_bucket_defs, daily_bucket_map),
                "hourly": _build_bucket_series(hourly_bucket_defs, hourly_bucket_map),
            },
            "models": models,
            "focus_models": focus_model_payloads,
        }
