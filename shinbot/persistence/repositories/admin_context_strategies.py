"""Context strategy metadata repository."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import ContextStrategyRecord

from .base import Repository


class ContextStrategyRepository(Repository):
    """Persistence adapter for context strategy metadata."""

    _JSON_FIELDS = {"config": ("config_json", {})}

    def list(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
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
        with self.connect() as conn:
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
        with self.connect() as conn:
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
        with self.connect() as conn:
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
                    self.json_dumps(payload["config"]),
                    1 if payload["enabled"] else 0,
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, strategy_uuid: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM context_strategies WHERE uuid = ?", (strategy_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(
            row,
            bool_fields=("enabled",),
            json_fields=self._JSON_FIELDS,
        )
