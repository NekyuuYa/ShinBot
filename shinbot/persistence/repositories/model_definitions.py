"""Model definition registry methods."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import ModelDefinitionRecord

from .base import Repository


class ModelDefinitionRepositoryMixin(Repository):
    """Model definition CRUD methods for ModelRegistryRepository."""

    _MODEL_JSON_FIELDS = {
        "capabilities": ("capabilities_json", []),
        "default_params": ("default_params_json", {}),
        "cost_metadata": ("cost_metadata_json", {}),
    }

    def upsert_model(self, record: ModelDefinitionRecord) -> None:
        payload = asdict(record)
        with self.connect() as conn:
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
                    self.json_dumps(payload["capabilities"]),
                    payload["context_window"],
                    self.json_dumps(payload["default_params"]),
                    self.json_dumps(payload["cost_metadata"]),
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

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return self.rows_to_dicts(
            rows,
            exclude=("provider_uuid",),
            bool_fields=("enabled",),
            json_fields=self._MODEL_JSON_FIELDS,
        )

    def get_model(self, model_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
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
        return self.row_to_dict(
            row,
            exclude=("provider_uuid",),
            bool_fields=("enabled",),
            json_fields=self._MODEL_JSON_FIELDS,
        )

    def delete_model(self, model_id: str) -> int:
        with self.connect() as conn:
            cursor = conn.execute("DELETE FROM model_definitions WHERE id = ?", (model_id,))
            return int(cursor.rowcount)
