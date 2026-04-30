"""Persona metadata repository."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import PersonaRecord

from .base import Repository


class PersonaRepository(Repository):
    """Persistence adapter for persona metadata."""

    _JSON_FIELDS = {"tags": ("tags_json", [])}

    def list(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
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
        return [self._row_to_payload(row) for row in rows]

    def get(self, persona_uuid: str) -> dict[str, Any] | None:
        with self.connect() as conn:
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
        return self._row_to_payload(row)

    def get_by_name(self, name: str) -> dict[str, Any] | None:
        with self.connect() as conn:
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
        return self._row_to_payload(row)

    def upsert(self, record: PersonaRecord) -> None:
        payload = asdict(record)
        with self.connect() as conn:
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
                    self.json_dumps(payload["tags"]),
                    1 if payload["enabled"] else 0,
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, persona_uuid: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM personas WHERE uuid = ?", (persona_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(
            row,
            bool_fields=("enabled",),
            json_fields=self._JSON_FIELDS,
        )
