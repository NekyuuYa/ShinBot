"""Administrative metadata repositories."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import (
    AgentRecord,
    BotConfigRecord,
    ContextStrategyRecord,
    PersonaRecord,
    PromptDefinitionRecord,
)

from .base import _json_dumps, _json_loads


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
