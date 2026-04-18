"""Repository helpers for the ShinBot SQLite persistence layer."""

from __future__ import annotations

import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import (
    AgentRecord,
    AIInteractionRecord,
    BotConfigRecord,
    ContextStrategyRecord,
    MessageLogRecord,
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PersonaRecord,
    PromptDefinitionRecord,
    PromptSnapshotRecord,
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


class ContextProvider(ABC):
    """Standardized session context retrieval interface."""

    @abstractmethod
    def get_recent(self, session_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent session messages in chronological order."""

    @abstractmethod
    def get_by_time(
        self,
        session_id: str,
        start: float,
        end: float,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return session messages within a time range in chronological order."""

    @abstractmethod
    def search_context(
        self,
        session_id: str,
        query: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return matching session messages for keyword/semantic retrieval."""


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
                SELECT
                    p.uuid,
                    p.name,
                    p.prompt_definition_uuid,
                    p.tags_json,
                    d.content AS prompt_text,
                    p.enabled,
                    p.created_at,
                    p.updated_at
                FROM personas AS p
                LEFT JOIN prompt_definitions AS d ON d.uuid = p.prompt_definition_uuid
                ORDER BY p.name ASC, p.uuid ASC
                """
            ).fetchall()
        return [
            {
                "uuid": row["uuid"],
                "name": row["name"],
                "prompt_definition_uuid": row["prompt_definition_uuid"],
                "tags": _json_loads(row["tags_json"], []),
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
                SELECT
                    p.uuid,
                    p.name,
                    p.prompt_definition_uuid,
                    p.tags_json,
                    d.content AS prompt_text,
                    p.enabled,
                    p.created_at,
                    p.updated_at
                FROM personas AS p
                LEFT JOIN prompt_definitions AS d ON d.uuid = p.prompt_definition_uuid
                WHERE p.uuid = ?
                """,
                (persona_uuid,),
            ).fetchone()
        if row is None:
            return None
        return {
            "uuid": row["uuid"],
            "name": row["name"],
            "prompt_definition_uuid": row["prompt_definition_uuid"],
            "tags": _json_loads(row["tags_json"], []),
            "prompt_text": row["prompt_text"],
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def get_by_name(self, name: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    p.uuid,
                    p.name,
                    p.prompt_definition_uuid,
                    p.tags_json,
                    d.content AS prompt_text,
                    p.enabled,
                    p.created_at,
                    p.updated_at
                FROM personas AS p
                LEFT JOIN prompt_definitions AS d ON d.uuid = p.prompt_definition_uuid
                WHERE p.name = ?
                """,
                (name,),
            ).fetchone()
        if row is None:
            return None
        return {
            "uuid": row["uuid"],
            "name": row["name"],
            "prompt_definition_uuid": row["prompt_definition_uuid"],
            "tags": _json_loads(row["tags_json"], []),
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
                    uuid, name, prompt_definition_uuid, tags_json, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    name = excluded.name,
                    prompt_definition_uuid = excluded.prompt_definition_uuid,
                    tags_json = excluded.tags_json,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["uuid"],
                    payload["name"],
                    payload["prompt_definition_uuid"],
                    _json_dumps(payload["tags"]),
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
                    uuid, name, type, resolver_ref, description, config_json,
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
                    uuid, name, type, resolver_ref, description, config_json,
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
                    uuid, name, type, resolver_ref, description, config_json,
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
                    uuid, name, type, resolver_ref, description, config_json,
                    enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    name = excluded.name,
                    type = excluded.type,
                    resolver_ref = excluded.resolver_ref,
                    description = excluded.description,
                    config_json = excluded.config_json,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["uuid"],
                    payload["name"],
                    payload["type"],
                    payload["resolver_ref"],
                    payload["description"],
                    _json_dumps(payload["config"]),
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
            "type": row["type"],
            "resolver_ref": row["resolver_ref"],
            "description": row["description"],
            "config": _json_loads(row["config_json"], {}),
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


class AgentRepository:
    """Persistence adapter for agent metadata."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def list(self) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT uuid, agent_id, name, persona_uuid, prompts_json, tools_json,
                       context_strategy_json,
                       config_json, tags_json,
                       created_at, updated_at
                FROM agents
                ORDER BY agent_id ASC, uuid ASC
                """
            ).fetchall()
        return [self._row_to_payload(row) for row in rows]

    def get(self, agent_uuid: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT uuid, agent_id, name, persona_uuid, prompts_json, tools_json,
                       context_strategy_json,
                       config_json, tags_json,
                       created_at, updated_at
                FROM agents
                WHERE uuid = ?
                """,
                (agent_uuid,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def get_by_agent_id(self, agent_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT uuid, agent_id, name, persona_uuid, prompts_json, tools_json,
                       context_strategy_json,
                       config_json, tags_json,
                       created_at, updated_at
                FROM agents
                WHERE agent_id = ?
                """,
                (agent_id,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: AgentRecord) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT created_at FROM agents WHERE uuid = ?",
                (payload["uuid"],),
            ).fetchone()
            created_at = row["created_at"] if row is not None else payload["created_at"]
            conn.execute(
                """
                INSERT INTO agents (
                    uuid, agent_id, name, persona_uuid, prompts_json, tools_json,
                    context_strategy_json,
                    config_json, tags_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    agent_id = excluded.agent_id,
                    name = excluded.name,
                    persona_uuid = excluded.persona_uuid,
                    prompts_json = excluded.prompts_json,
                    tools_json = excluded.tools_json,
                    context_strategy_json = excluded.context_strategy_json,
                    config_json = excluded.config_json,
                    tags_json = excluded.tags_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["uuid"],
                    payload["agent_id"],
                    payload["name"],
                    payload["persona_uuid"],
                    _json_dumps(payload["prompts"]),
                    _json_dumps(payload["tools"]),
                    _json_dumps(payload["context_strategy"]),
                    _json_dumps(payload["config"]),
                    _json_dumps(payload["tags"]),
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, agent_uuid: str) -> None:
        with self._db.connect() as conn:
            conn.execute("DELETE FROM agents WHERE uuid = ?", (agent_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return {
            "uuid": row["uuid"],
            "agent_id": row["agent_id"],
            "name": row["name"],
            "persona_uuid": row["persona_uuid"],
            "prompts": _json_loads(row["prompts_json"], []),
            "tools": _json_loads(row["tools_json"], []),
            "context_strategy": _json_loads(row["context_strategy_json"], {}),
            "config": _json_loads(row["config_json"], {}),
            "tags": _json_loads(row["tags_json"], []),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


class PromptDefinitionRepository:
    """Persistence adapter for prompt definition metadata."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def list(self) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    uuid, prompt_id, name, source_type, source_id, owner_plugin_id,
                    owner_module, module_path, stage, type, priority, version, description,
                    enabled, content, template_vars_json, resolver_ref, bundle_refs_json,
                    config_json, tags_json, metadata_json, created_at, updated_at
                FROM prompt_definitions
                ORDER BY stage ASC, priority ASC, prompt_id ASC
                """
            ).fetchall()
        return [self._row_to_payload(row) for row in rows]

    def get(self, prompt_uuid: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    uuid, prompt_id, name, source_type, source_id, owner_plugin_id,
                    owner_module, module_path, stage, type, priority, version, description,
                    enabled, content, template_vars_json, resolver_ref, bundle_refs_json,
                    config_json, tags_json, metadata_json, created_at, updated_at
                FROM prompt_definitions
                WHERE uuid = ?
                """,
                (prompt_uuid,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def get_by_prompt_id(self, prompt_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    uuid, prompt_id, name, source_type, source_id, owner_plugin_id,
                    owner_module, module_path, stage, type, priority, version, description,
                    enabled, content, template_vars_json, resolver_ref, bundle_refs_json,
                    config_json, tags_json, metadata_json, created_at, updated_at
                FROM prompt_definitions
                WHERE prompt_id = ?
                """,
                (prompt_id,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: PromptDefinitionRecord) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT created_at FROM prompt_definitions WHERE uuid = ?",
                (payload["uuid"],),
            ).fetchone()
            created_at = row["created_at"] if row is not None else payload["created_at"]
            conn.execute(
                """
                INSERT INTO prompt_definitions (
                    uuid, prompt_id, name, source_type, source_id, owner_plugin_id,
                    owner_module, module_path, stage, type, priority, version, description,
                    enabled, content, template_vars_json, resolver_ref, bundle_refs_json,
                    config_json, tags_json, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    prompt_id = excluded.prompt_id,
                    name = excluded.name,
                    source_type = excluded.source_type,
                    source_id = excluded.source_id,
                    owner_plugin_id = excluded.owner_plugin_id,
                    owner_module = excluded.owner_module,
                    module_path = excluded.module_path,
                    stage = excluded.stage,
                    type = excluded.type,
                    priority = excluded.priority,
                    version = excluded.version,
                    description = excluded.description,
                    enabled = excluded.enabled,
                    content = excluded.content,
                    template_vars_json = excluded.template_vars_json,
                    resolver_ref = excluded.resolver_ref,
                    bundle_refs_json = excluded.bundle_refs_json,
                    config_json = excluded.config_json,
                    tags_json = excluded.tags_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["uuid"],
                    payload["prompt_id"],
                    payload["name"],
                    payload["source_type"],
                    payload["source_id"],
                    payload["owner_plugin_id"],
                    payload["owner_module"],
                    payload["module_path"],
                    payload["stage"],
                    payload["type"],
                    payload["priority"],
                    payload["version"],
                    payload["description"],
                    1 if payload["enabled"] else 0,
                    payload["content"],
                    _json_dumps(payload["template_vars"]),
                    payload["resolver_ref"],
                    _json_dumps(payload["bundle_refs"]),
                    _json_dumps(payload["config"]),
                    _json_dumps(payload["tags"]),
                    _json_dumps(payload["metadata"]),
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, prompt_uuid: str) -> None:
        with self._db.connect() as conn:
            conn.execute("DELETE FROM prompt_definitions WHERE uuid = ?", (prompt_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return {
            "uuid": row["uuid"],
            "prompt_id": row["prompt_id"],
            "name": row["name"],
            "source_type": row["source_type"],
            "source_id": row["source_id"],
            "owner_plugin_id": row["owner_plugin_id"],
            "owner_module": row["owner_module"],
            "module_path": row["module_path"],
            "stage": row["stage"],
            "type": row["type"],
            "priority": row["priority"],
            "version": row["version"],
            "description": row["description"],
            "enabled": bool(row["enabled"]),
            "content": row["content"],
            "template_vars": _json_loads(row["template_vars_json"], []),
            "resolver_ref": row["resolver_ref"],
            "bundle_refs": _json_loads(row["bundle_refs_json"], []),
            "config": _json_loads(row["config_json"], {}),
            "tags": _json_loads(row["tags_json"], []),
            "metadata": _json_loads(row["metadata_json"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


class BotConfigRepository:
    """Persistence adapter for per-instance bot configuration."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def list(self) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT uuid, instance_id, default_agent_uuid, main_llm, config_json,
                       tags_json, created_at, updated_at
                FROM bot_configs
                ORDER BY instance_id ASC, uuid ASC
                """
            ).fetchall()
        return [self._row_to_payload(row) for row in rows]

    def get(self, config_uuid: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT uuid, instance_id, default_agent_uuid, main_llm, config_json,
                       tags_json, created_at, updated_at
                FROM bot_configs
                WHERE uuid = ?
                """,
                (config_uuid,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def get_by_instance_id(self, instance_id: str) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT uuid, instance_id, default_agent_uuid, main_llm, config_json,
                       tags_json, created_at, updated_at
                FROM bot_configs
                WHERE instance_id = ?
                """,
                (instance_id,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: BotConfigRecord) -> None:
        payload = asdict(record)
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT created_at FROM bot_configs WHERE uuid = ?",
                (payload["uuid"],),
            ).fetchone()
            created_at = row["created_at"] if row is not None else payload["created_at"]
            conn.execute(
                """
                INSERT INTO bot_configs (
                    uuid, instance_id, default_agent_uuid, main_llm, config_json,
                    tags_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    instance_id = excluded.instance_id,
                    default_agent_uuid = excluded.default_agent_uuid,
                    main_llm = excluded.main_llm,
                    config_json = excluded.config_json,
                    tags_json = excluded.tags_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["uuid"],
                    payload["instance_id"],
                    payload["default_agent_uuid"],
                    payload["main_llm"],
                    _json_dumps(payload["config"]),
                    _json_dumps(payload["tags"]),
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, config_uuid: str) -> None:
        with self._db.connect() as conn:
            conn.execute("DELETE FROM bot_configs WHERE uuid = ?", (config_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return {
            "uuid": row["uuid"],
            "instance_id": row["instance_id"],
            "default_agent_uuid": row["default_agent_uuid"],
            "main_llm": row["main_llm"],
            "config": _json_loads(row["config_json"], {}),
            "tags": _json_loads(row["tags_json"], []),
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


class MessageLogRepository(ContextProvider):
    """Persistence adapter for the full communication log."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def insert(self, record: MessageLogRecord) -> int:
        """Insert a message log entry and return the auto-incremented id."""
        with self._db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO message_logs (
                    session_id, platform_msg_id, sender_id, sender_name,
                    content_json, raw_text, role, is_read, is_mentioned, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.session_id,
                    record.platform_msg_id,
                    record.sender_id,
                    record.sender_name,
                    record.content_json,
                    record.raw_text,
                    record.role,
                    1 if record.is_read else 0,
                    1 if record.is_mentioned else 0,
                    record.created_at,
                ),
            )
            return cursor.lastrowid  # type: ignore[return-value]

    def mark_read(self, msg_id: int) -> None:
        with self._db.connect() as conn:
            conn.execute("UPDATE message_logs SET is_read = 1 WHERE id = ?", (msg_id,))

    def get(self, msg_id: int) -> dict[str, Any] | None:
        with self._db.connect() as conn:
            row = conn.execute("SELECT * FROM message_logs WHERE id = ?", (msg_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    def list_by_session(
        self,
        session_id: str,
        *,
        limit: int = 50,
        before_id: int | None = None,
    ) -> list[dict[str, Any]]:
        with self._db.connect() as conn:
            if before_id is not None:
                rows = conn.execute(
                    """
                    SELECT * FROM message_logs
                    WHERE session_id = ? AND id < ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (session_id, before_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM message_logs
                    WHERE session_id = ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (session_id, limit),
                ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_recent(self, session_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent messages for a session in chronological order."""
        rows = self.list_by_session(session_id, limit=limit)
        rows.reverse()
        return rows

    def get_by_time(
        self,
        session_id: str,
        start: float,
        end: float,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return messages within a time range in chronological order."""
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND created_at >= ? AND created_at <= ?
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                """,
                (session_id, start, end, limit),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def search_context(
        self,
        session_id: str,
        query: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Basic keyword search placeholder for future semantic retrieval."""
        needle = query.strip()
        if not needle:
            return []
        with self._db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND raw_text LIKE ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (session_id, f"%{needle}%", limit),
            ).fetchall()
        items = [self._row_to_dict(r) for r in rows]
        items.reverse()
        return items

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
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
        }


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
