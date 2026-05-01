"""Administrative helpers for prompt-definition management flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from shinbot.agent.prompt_manager import PromptComponent, PromptComponentKind, PromptStage
from shinbot.persistence.records import PromptDefinitionRecord, utc_now_iso


@dataclass(slots=True)
class PromptDefinitionAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def serialize_prompt_definition(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "uuid": payload["uuid"],
        "promptId": payload["prompt_id"],
        "name": payload["name"],
        "source": {
            "sourceType": payload["source_type"],
            "sourceId": payload["source_id"],
            "ownerPluginId": payload["owner_plugin_id"],
            "ownerModule": payload["owner_module"],
            "modulePath": payload["module_path"],
        },
        "stage": payload["stage"],
        "type": payload["type"],
        "priority": payload["priority"],
        "version": payload["version"],
        "description": payload["description"],
        "enabled": payload["enabled"],
        "content": payload["content"],
        "templateVars": payload["template_vars"],
        "resolverRef": payload["resolver_ref"],
        "bundleRefs": payload["bundle_refs"],
        "config": payload["config"],
        "tags": payload["tags"],
        "metadata": payload["metadata"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def normalize_prompt_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    reserved_keys = {
        "prompt_id",
        "promptId",
        "name",
        "source",
        "source_type",
        "sourceType",
        "source_id",
        "sourceId",
        "owner_plugin_id",
        "ownerPluginId",
        "owner_module",
        "ownerModule",
        "module_path",
        "modulePath",
        "stage",
        "type",
        "priority",
        "version",
        "description",
        "enabled",
        "content",
        "template_vars",
        "templateVars",
        "resolver_ref",
        "resolverRef",
        "bundle_refs",
        "bundleRefs",
        "config",
        "tags",
        "display_name",
        "displayName",
    }
    return {str(key): value for key, value in metadata.items() if str(key) not in reserved_keys}


def normalize_prompt_definition_input(
    *,
    prompt_id: str,
    name: str,
    source_type: str,
    source_id: str,
    owner_plugin_id: str,
    owner_module: str,
    module_path: str,
    stage: str,
    type: str,
    priority: int,
    version: str,
    description: str,
    enabled: bool,
    content: str,
    template_vars: list[str],
    resolver_ref: str,
    bundle_refs: list[str],
    config: dict[str, Any],
    tags: list[str],
    metadata: dict[str, Any],
) -> PromptDefinitionRecord:
    normalized_prompt_id = prompt_id.strip()
    normalized_name = name.strip()
    normalized_source_type = source_type.strip() or "unknown_source"
    normalized_source_id = source_id.strip()
    normalized_owner_plugin_id = owner_plugin_id.strip()
    normalized_owner_module = owner_module.strip()
    normalized_module_path = module_path.strip()
    normalized_version = version.strip() or "1.0.0"
    normalized_description = description.strip()
    normalized_template_vars = [item.strip() for item in template_vars if item.strip()]
    normalized_bundle_refs = [item.strip() for item in bundle_refs if item.strip()]
    normalized_tags = [item.strip() for item in tags if item.strip()]

    if not normalized_prompt_id:
        raise PromptDefinitionAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Prompt promptId must not be empty",
        )
    if not normalized_name:
        raise PromptDefinitionAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Prompt name must not be empty",
        )

    deduped_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in normalized_tags:
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        deduped_tags.append(tag)

    normalized_metadata = normalize_prompt_metadata(dict(metadata))

    try:
        component = PromptComponent(
            id=normalized_prompt_id,
            stage=PromptStage(stage),
            kind=PromptComponentKind(type),
            version=normalized_version,
            priority=priority,
            enabled=enabled,
            content=content,
            template_vars=normalized_template_vars,
            resolver_ref=resolver_ref.strip(),
            bundle_refs=normalized_bundle_refs,
            tags=deduped_tags,
            metadata=normalized_metadata,
        )
    except ValueError as exc:
        raise PromptDefinitionAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message=str(exc),
        ) from exc

    return PromptDefinitionRecord(
        uuid="",
        prompt_id=component.id,
        name=normalized_name,
        source_type=normalized_source_type,
        source_id=normalized_source_id,
        owner_plugin_id=normalized_owner_plugin_id,
        owner_module=normalized_owner_module,
        module_path=normalized_module_path,
        stage=component.stage.value,
        type=component.kind.value,
        priority=component.priority,
        version=component.version,
        description=normalized_description,
        enabled=component.enabled,
        content=component.content,
        template_vars=list(component.template_vars),
        resolver_ref=component.resolver_ref,
        bundle_refs=list(component.bundle_refs),
        config=dict(config),
        tags=list(component.tags),
        metadata=dict(component.metadata),
    )


def get_prompt_definition_or_raise(database: Any, prompt_uuid: str) -> dict[str, Any]:
    payload = database.prompt_definitions.get(prompt_uuid)
    if payload is None:
        raise PromptDefinitionAdminError(
            status_code=404,
            code="PROMPT_NOT_FOUND",
            message=f"Prompt {prompt_uuid!r} was not found",
        )
    return payload


def assert_prompt_id_available(database: Any, prompt_id: str, *, current_uuid: str | None) -> None:
    existing = database.prompt_definitions.get_by_prompt_id(prompt_id)
    if existing is not None and existing["uuid"] != current_uuid:
        raise PromptDefinitionAdminError(
            status_code=409,
            code="PROMPT_ALREADY_EXISTS",
            message=f"Prompt {prompt_id!r} already exists",
        )


def build_prompt_definition_record(
    *,
    prompt_uuid: str | None,
    normalized: PromptDefinitionRecord,
    created_at: str | None = None,
) -> PromptDefinitionRecord:
    now = utc_now_iso()
    normalized.uuid = prompt_uuid or str(uuid4())
    normalized.created_at = created_at or now
    normalized.updated_at = now
    return normalized
