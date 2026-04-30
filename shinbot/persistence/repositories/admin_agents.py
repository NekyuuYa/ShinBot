"""Agent metadata repository."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import AgentRecord

from .base import Repository


class AgentRepository(Repository):
    """Persistence adapter for agent metadata."""

    _JSON_FIELDS = {
        "prompts": ("prompts_json", []),
        "tools": ("tools_json", []),
        "context_strategy": ("context_strategy_json", {}),
        "config": ("config_json", {}),
        "tags": ("tags_json", []),
    }

    def list(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
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
        with self.connect() as conn:
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
        with self.connect() as conn:
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
        with self.connect() as conn:
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
                    self.json_dumps(payload["prompts"]),
                    self.json_dumps(payload["tools"]),
                    self.json_dumps(payload["context_strategy"]),
                    self.json_dumps(payload["config"]),
                    self.json_dumps(payload["tags"]),
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, agent_uuid: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM agents WHERE uuid = ?", (agent_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(row, json_fields=self._JSON_FIELDS)
