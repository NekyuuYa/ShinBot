"""Prompt definition management router: /api/v1/prompt-definitions"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.agent.prompting import PromptComponent, PromptComponentKind, PromptStage
from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import EC, ok
from shinbot.persistence.records import PromptDefinitionRecord, utc_now_iso

router = APIRouter(
    prefix="/prompt-definitions",
    tags=["prompt-definitions"],
    dependencies=AuthRequired,
)


class PromptDefinitionRequest(BaseModel):
    promptId: str
    name: str
    sourceType: str = "unknown_source"
    sourceId: str = ""
    ownerPluginId: str = ""
    ownerModule: str = ""
    modulePath: str = ""
    stage: str
    type: str
    priority: int = 100
    version: str = "1.0.0"
    description: str = ""
    enabled: bool = True
    content: str = ""
    templateVars: list[str] = Field(default_factory=list)
    resolverRef: str = ""
    bundleRefs: list[str] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptDefinitionPatchRequest(BaseModel):
    promptId: str | None = None
    name: str | None = None
    sourceType: str | None = None
    sourceId: str | None = None
    ownerPluginId: str | None = None
    ownerModule: str | None = None
    modulePath: str | None = None
    stage: str | None = None
    type: str | None = None
    priority: int | None = None
    version: str | None = None
    description: str | None = None
    enabled: bool | None = None
    content: str | None = None
    templateVars: list[str] | None = None
    resolverRef: str | None = None
    bundleRefs: list[str] | None = None
    config: dict[str, Any] | None = None
    tags: list[str] | None = None
    metadata: dict[str, Any] | None = None


def _serialize_prompt(payload: dict[str, Any]) -> dict[str, Any]:
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


def _normalize_prompt_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
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
    return {
        str(key): value
        for key, value in metadata.items()
        if str(key) not in reserved_keys
    }


def _normalize_prompt_input(
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
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Prompt promptId must not be empty"},
        )
    if not normalized_name:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Prompt name must not be empty"},
        )

    deduped_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in normalized_tags:
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        deduped_tags.append(tag)

    normalized_metadata = _normalize_prompt_metadata(dict(metadata))

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
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": str(exc)},
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


@router.get("")
def list_prompt_definitions(bot=BotDep):
    return ok([_serialize_prompt(item) for item in bot.database.prompt_definitions.list()])


@router.post("", status_code=201)
def create_prompt_definition(body: PromptDefinitionRequest, bot=BotDep):
    normalized = _normalize_prompt_input(
        prompt_id=body.promptId,
        name=body.name,
        source_type=body.sourceType,
        source_id=body.sourceId,
        owner_plugin_id=body.ownerPluginId,
        owner_module=body.ownerModule,
        module_path=body.modulePath,
        stage=body.stage,
        type=body.type,
        priority=body.priority,
        version=body.version,
        description=body.description,
        enabled=body.enabled,
        content=body.content,
        template_vars=body.templateVars,
        resolver_ref=body.resolverRef,
        bundle_refs=body.bundleRefs,
        config=body.config,
        tags=body.tags,
        metadata=body.metadata,
    )
    if bot.database.prompt_definitions.get_by_prompt_id(normalized.prompt_id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.PROMPT_ALREADY_EXISTS,
                "message": f"Prompt {normalized.prompt_id!r} already exists",
            },
        )

    now = utc_now_iso()
    normalized.uuid = str(uuid4())
    normalized.created_at = now
    normalized.updated_at = now
    bot.database.prompt_definitions.upsert(normalized)
    payload = bot.database.prompt_definitions.get(normalized.uuid)
    assert payload is not None
    return ok(_serialize_prompt(payload))


@router.get("/{prompt_uuid}")
def get_prompt_definition(prompt_uuid: str, bot=BotDep):
    payload = bot.database.prompt_definitions.get(prompt_uuid)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PROMPT_NOT_FOUND, "message": f"Prompt {prompt_uuid!r} was not found"},
        )
    return ok(_serialize_prompt(payload))


@router.patch("/{prompt_uuid}")
def patch_prompt_definition(prompt_uuid: str, body: PromptDefinitionPatchRequest, bot=BotDep):
    current = bot.database.prompt_definitions.get(prompt_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PROMPT_NOT_FOUND, "message": f"Prompt {prompt_uuid!r} was not found"},
        )

    normalized = _normalize_prompt_input(
        prompt_id=body.promptId if body.promptId is not None else str(current["prompt_id"]),
        name=body.name if body.name is not None else str(current["name"]),
        source_type=body.sourceType if body.sourceType is not None else str(current["source_type"]),
        source_id=body.sourceId if body.sourceId is not None else str(current["source_id"]),
        owner_plugin_id=(
            body.ownerPluginId
            if body.ownerPluginId is not None
            else str(current["owner_plugin_id"])
        ),
        owner_module=(
            body.ownerModule if body.ownerModule is not None else str(current["owner_module"])
        ),
        module_path=body.modulePath if body.modulePath is not None else str(current["module_path"]),
        stage=body.stage if body.stage is not None else str(current["stage"]),
        type=body.type if body.type is not None else str(current["type"]),
        priority=body.priority if body.priority is not None else int(current["priority"]),
        version=body.version if body.version is not None else str(current["version"]),
        description=(
            body.description if body.description is not None else str(current["description"])
        ),
        enabled=body.enabled if body.enabled is not None else bool(current["enabled"]),
        content=body.content if body.content is not None else str(current["content"]),
        template_vars=(
            body.templateVars if body.templateVars is not None else list(current["template_vars"])
        ),
        resolver_ref=(
            body.resolverRef if body.resolverRef is not None else str(current["resolver_ref"])
        ),
        bundle_refs=body.bundleRefs if body.bundleRefs is not None else list(current["bundle_refs"]),
        config=body.config if body.config is not None else dict(current["config"]),
        tags=body.tags if body.tags is not None else list(current["tags"]),
        metadata=body.metadata if body.metadata is not None else dict(current["metadata"]),
    )

    existing = bot.database.prompt_definitions.get_by_prompt_id(normalized.prompt_id)
    if existing is not None and existing["uuid"] != prompt_uuid:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.PROMPT_ALREADY_EXISTS,
                "message": f"Prompt {normalized.prompt_id!r} already exists",
            },
        )

    normalized.uuid = prompt_uuid
    normalized.created_at = str(current["created_at"])
    normalized.updated_at = utc_now_iso()
    bot.database.prompt_definitions.upsert(normalized)
    payload = bot.database.prompt_definitions.get(prompt_uuid)
    assert payload is not None
    return ok(_serialize_prompt(payload))


@router.delete("/{prompt_uuid}")
def delete_prompt_definition(prompt_uuid: str, bot=BotDep):
    current = bot.database.prompt_definitions.get(prompt_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PROMPT_NOT_FOUND, "message": f"Prompt {prompt_uuid!r} was not found"},
        )
    bot.database.prompt_definitions.delete(prompt_uuid)
    return ok({"deleted": True, "uuid": prompt_uuid})
