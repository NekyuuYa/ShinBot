"""Model provider registry methods."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import ModelProviderRecord

from .base import Repository


class ModelProviderRepositoryMixin(Repository):
    """Provider CRUD methods for ModelRegistryRepository."""

    _PROVIDER_JSON_FIELDS = {
        "auth": ("auth_json", {}),
        "default_params": ("default_params_json", {}),
    }

    def upsert_provider(self, record: ModelProviderRecord) -> None:
        payload = asdict(record)
        with self.connect() as conn:
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
                    self.json_dumps(payload["auth"]),
                    self.json_dumps(payload["default_params"]),
                    1 if payload["enabled"] else 0,
                    created_at,
                    payload["updated_at"],
                ),
            )

    def list_providers(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM model_providers
                ORDER BY id ASC
                """
            ).fetchall()
        return self.rows_to_dicts(
            rows,
            bool_fields=("enabled",),
            json_fields=self._PROVIDER_JSON_FIELDS,
        )

    def get_provider(self, provider_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
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
        return self.row_to_dict(
            row,
            bool_fields=("enabled",),
            json_fields=self._PROVIDER_JSON_FIELDS,
        )

    def delete_provider(self, provider_id: str) -> int:
        with self.connect() as conn:
            cursor = conn.execute("DELETE FROM model_providers WHERE id = ?", (provider_id,))
            return int(cursor.rowcount)

    def rename_provider(self, provider_id: str, new_provider_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE model_providers SET id = ? WHERE id = ?",
                (new_provider_id, provider_id),
            )
