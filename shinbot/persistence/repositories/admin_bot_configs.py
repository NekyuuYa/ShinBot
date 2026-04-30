"""Bot configuration metadata repository."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import BotConfigRecord

from .base import Repository


class BotConfigRepository(Repository):
    """Persistence adapter for per-instance bot configuration."""

    _JSON_FIELDS = {
        "config": ("config_json", {}),
        "tags": ("tags_json", []),
    }

    def list(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
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
        with self.connect() as conn:
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
        with self.connect() as conn:
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
        with self.connect() as conn:
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
                    self.json_dumps(payload["config"]),
                    self.json_dumps(payload["tags"]),
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, config_uuid: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM bot_configs WHERE uuid = ?", (config_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(row, json_fields=self._JSON_FIELDS)
