"""Prompt definition metadata repository."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import PromptDefinitionRecord

from .base import Repository


class PromptDefinitionRepository(Repository):
    """Persistence adapter for prompt definition metadata."""

    _JSON_FIELDS = {
        "template_vars": ("template_vars_json", []),
        "bundle_refs": ("bundle_refs_json", []),
        "config": ("config_json", {}),
        "tags": ("tags_json", []),
        "metadata": ("metadata_json", {}),
    }

    def list(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
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
        with self.connect() as conn:
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
        with self.connect() as conn:
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
        with self.connect() as conn:
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
                    self.json_dumps(payload["template_vars"]),
                    payload["resolver_ref"],
                    self.json_dumps(payload["bundle_refs"]),
                    self.json_dumps(payload["config"]),
                    self.json_dumps(payload["tags"]),
                    self.json_dumps(payload["metadata"]),
                    created_at,
                    payload["updated_at"],
                ),
            )

    def delete(self, prompt_uuid: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM prompt_definitions WHERE uuid = ?", (prompt_uuid,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(
            row,
            bool_fields=("enabled",),
            json_fields=self._JSON_FIELDS,
        )
