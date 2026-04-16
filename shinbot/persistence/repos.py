"""Repository helpers for the ShinBot SQLite persistence layer."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import (
    ContextStrategyRecord,
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PersonaRecord,
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


class SessionRepository:
    """Persistence adapter for structured session state."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def get(self, session_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    s.id,
                    s.instance_id,
                    s.session_type,
                    s.platform,
                    s.guild_id,
                    s.channel_id,
                    s.display_name,
                    s.permission_group,
                    s.created_at,
                    s.last_active,
                    s.state_json,
                    s.plugin_data_json,
                    c.prefixes_json,
                    c.llm_enabled,
                    c.is_muted,
                    c.audit_enabled
                FROM sessions AS s
                LEFT JOIN session_configs AS c ON c.session_id = s.id
                WHERE s.id = ?
                """,
                (session_id,),
            ).fetchone()
        if row is None:
            return None

        return {
            "id": row["id"],
            "instance_id": row["instance_id"],
            "session_type": row["session_type"],
            "platform": row["platform"],
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "display_name": row["display_name"],
            "permission_group": row["permission_group"],
            "created_at": row["created_at"],
            "last_active": row["last_active"],
            "state": _json_loads(row["state_json"], {}),
            "plugin_data": _json_loads(row["plugin_data_json"], {}),
            "config": {
                "prefixes": _json_loads(row["prefixes_json"], ["/"]),
                "llm_enabled": bool(row["llm_enabled"]) if row["llm_enabled"] is not None else True,
                "is_muted": bool(row["is_muted"]) if row["is_muted"] is not None else False,
                "audit_enabled": (
                    bool(row["audit_enabled"]) if row["audit_enabled"] is not None else False
                ),
            },
        }

    def upsert(self, payload: dict[str, Any]) -> None:
        config = dict(payload.get("config") or {})
        now = time.time()
        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (
                    id, instance_id, session_type, platform, guild_id, channel_id, display_name,
                    permission_group, created_at, last_active, state_json, plugin_data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    instance_id = excluded.instance_id,
                    session_type = excluded.session_type,
                    platform = excluded.platform,
                    guild_id = excluded.guild_id,
                    channel_id = excluded.channel_id,
                    display_name = excluded.display_name,
                    permission_group = excluded.permission_group,
                    created_at = excluded.created_at,
                    last_active = excluded.last_active,
                    state_json = excluded.state_json,
                    plugin_data_json = excluded.plugin_data_json
                """,
                (
                    payload["id"],
                    payload["instance_id"],
                    payload["session_type"],
                    payload.get("platform", ""),
                    payload.get("guild_id"),
                    payload.get("channel_id", ""),
                    payload.get("display_name", ""),
                    payload.get("permission_group", "default"),
                    payload.get("created_at", now),
                    payload.get("last_active", now),
                    _json_dumps(payload.get("state", {})),
                    _json_dumps(payload.get("plugin_data", {})),
                ),
            )
            conn.execute(
                """
                INSERT INTO session_configs (
                    session_id, prefixes_json, llm_enabled, is_muted, audit_enabled, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    prefixes_json = excluded.prefixes_json,
                    llm_enabled = excluded.llm_enabled,
                    is_muted = excluded.is_muted,
                    audit_enabled = excluded.audit_enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["id"],
                    _json_dumps(config.get("prefixes", ["/"])),
                    1 if config.get("llm_enabled", True) else 0,
                    1 if config.get("is_muted", False) else 0,
                    1 if config.get("audit_enabled", False) else 0,
                    now,
                ),
            )

    def delete(self, session_id: str) -> None:
        with self._db.connect() as conn:
            conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))


class AuditRepository:
    """Persistence adapter for structured audit logs."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def insert(self, payload: dict[str, Any]) -> int:
        with self._db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO audit_logs (
                    timestamp, entry_type, command_name, plugin_id, user_id, session_id, instance_id,
                    permission_required, permission_granted, execution_time_ms, success, error, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("timestamp", ""),
                    payload.get("entry_type", "command"),
                    payload.get("command_name", ""),
                    payload.get("plugin_id", ""),
                    payload.get("user_id", ""),
                    payload.get("session_id", ""),
                    payload.get("instance_id", ""),
                    payload.get("permission_required", ""),
                    1 if payload.get("permission_granted", False) else 0,
                    payload.get("execution_time_ms", 0.0),
                    1 if payload.get("success", False) else 0,
                    payload.get("error", ""),
                    _json_dumps(payload.get("metadata", {})),
                ),
            )
            return int(cursor.lastrowid)


class PersonaRepository:
    """Persistence adapter for persona metadata."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def list(self) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT uuid, name, prompt_text, enabled, created_at, updated_at
                FROM personas
                ORDER BY name ASC, uuid ASC
                """
            ).fetchall()
        return [
            {
                "uuid": row["uuid"],
                "name": row["name"],
                "prompt_text": row["prompt_text"],
                "enabled": bool(row["enabled"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def get(self, persona_uuid: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT uuid, name, prompt_text, enabled, created_at, updated_at
                FROM personas
                WHERE uuid = ?
                """,
                (persona_uuid,),
            ).fetchone()
        if row is None:
            return None
        return {
            "uuid": row["uuid"],
            "name": row["name"],
            "prompt_text": row["prompt_text"],
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def get_by_name(self, name: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT uuid, name, prompt_text, enabled, created_at, updated_at
                FROM personas
                WHERE name = ?
                """,
                (name,),
            ).fetchone()
        if row is None:
            return None
        return {
            "uuid": row["uuid"],
            "name": row["name"],
            "prompt_text": row["prompt_text"],
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def upsert(self, record: PersonaRecord) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT created_at FROM personas WHERE uuid = ?",
                (payload["uuid"],),
            ).fetchone()
            created_at = row["created_at"] if row is not None else payload["created_at"]
            conn.execute(
                """
                INSERT INTO personas (
                    uuid, name, prompt_text, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    name = excluded.name,
                    prompt_text = excluded.prompt_text,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["uuid"],
                    payload["name"],
                    payload["prompt_text"],
                    1 if payload["enabled"] else 0,
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, persona_uuid: str) -> None:
        with self._db.connect() as conn:
            conn.execute("DELETE FROM personas WHERE uuid = ?", (persona_uuid,))


class ContextStrategyRepository:
    """Persistence adapter for context strategy metadata."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def list(self) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    uuid, name, resolver_ref, description, config_json,
                    max_context_tokens, max_history_turns, memory_summary_required,
                    truncate_policy, trigger_ratio, trim_ratio,
                    enabled, created_at, updated_at
                FROM context_strategies
                ORDER BY name ASC, uuid ASC
                """
            ).fetchall()
        return [self._row_to_payload(row) for row in rows]

    def get(self, strategy_uuid: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    uuid, name, resolver_ref, description, config_json,
                    max_context_tokens, max_history_turns, memory_summary_required,
                    truncate_policy, trigger_ratio, trim_ratio,
                    enabled, created_at, updated_at
                FROM context_strategies
                WHERE uuid = ?
                """,
                (strategy_uuid,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def get_by_name(self, name: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    uuid, name, resolver_ref, description, config_json,
                    max_context_tokens, max_history_turns, memory_summary_required,
                    truncate_policy, trigger_ratio, trim_ratio,
                    enabled, created_at, updated_at
                FROM context_strategies
                WHERE name = ?
                """,
                (name,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: ContextStrategyRecord) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT created_at FROM context_strategies WHERE uuid = ?",
                (payload["uuid"],),
            ).fetchone()
            created_at = row["created_at"] if row is not None else payload["created_at"]
            conn.execute(
                """
                INSERT INTO context_strategies (
                    uuid, name, resolver_ref, description, config_json,
                    max_context_tokens, max_history_turns, memory_summary_required,
                    truncate_policy, trigger_ratio, trim_ratio,
                    enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    name = excluded.name,
                    resolver_ref = excluded.resolver_ref,
                    description = excluded.description,
                    config_json = excluded.config_json,
                    max_context_tokens = excluded.max_context_tokens,
                    max_history_turns = excluded.max_history_turns,
                    memory_summary_required = excluded.memory_summary_required,
                    truncate_policy = excluded.truncate_policy,
                    trigger_ratio = excluded.trigger_ratio,
                    trim_ratio = excluded.trim_ratio,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["uuid"],
                    payload["name"],
                    payload["resolver_ref"],
                    payload["description"],
                    _json_dumps(payload["config"]),
                    payload["max_context_tokens"],
                    payload["max_history_turns"],
                    1 if payload["memory_summary_required"] else 0,
                    payload["truncate_policy"],
                    payload["trigger_ratio"],
                    payload["trim_ratio"],
                    1 if payload["enabled"] else 0,
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, strategy_uuid: str) -> None:
        with self._db.connect() as conn:
            conn.execute("DELETE FROM context_strategies WHERE uuid = ?", (strategy_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return {
            "uuid": row["uuid"],
            "name": row["name"],
            "resolver_ref": row["resolver_ref"],
            "description": row["description"],
            "config": _json_loads(row["config_json"], {}),
            "max_context_tokens": row["max_context_tokens"],
            "max_history_turns": row["max_history_turns"],
            "memory_summary_required": bool(row["memory_summary_required"]),
            "truncate_policy": row["truncate_policy"],
            "trigger_ratio": float(row["trigger_ratio"]),
            "trim_ratio": float(row["trim_ratio"]),
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


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
                    provider_uuid, id, type, display_name, base_url, auth_json,
                    default_params_json, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider_uuid) DO UPDATE SET
                    type = excluded.type,
                    id = excluded.id,
                    display_name = excluded.display_name,
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
                    estimated_cost, currency, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
