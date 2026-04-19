"""Prompt definition management router: /api/v1/prompt-definitions"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import ok
from shinbot.core.prompt_definition_admin import (
    PromptDefinitionAdminError,
    assert_prompt_id_available,
    build_prompt_definition_record,
    get_prompt_definition_or_raise,
    normalize_prompt_definition_input,
    serialize_prompt_definition,
)

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


def _raise_admin_http_error(exc: PromptDefinitionAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("")
def list_prompt_definitions(bot=BotDep):
    return ok(
        [serialize_prompt_definition(item) for item in bot.database.prompt_definitions.list()]
    )


@router.post("", status_code=201)
def create_prompt_definition(body: PromptDefinitionRequest, bot=BotDep):
    try:
        normalized = normalize_prompt_definition_input(
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
        assert_prompt_id_available(bot.database, normalized.prompt_id, current_uuid=None)
        record = build_prompt_definition_record(prompt_uuid=None, normalized=normalized)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.prompt_definitions.upsert(record)
    payload = bot.database.prompt_definitions.get(record.uuid)
    assert payload is not None
    return ok(serialize_prompt_definition(payload))


@router.get("/{prompt_uuid}")
def get_prompt_definition(prompt_uuid: str, bot=BotDep):
    try:
        payload = get_prompt_definition_or_raise(bot.database, prompt_uuid)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(serialize_prompt_definition(payload))


@router.patch("/{prompt_uuid}")
def patch_prompt_definition(prompt_uuid: str, body: PromptDefinitionPatchRequest, bot=BotDep):
    try:
        current = get_prompt_definition_or_raise(bot.database, prompt_uuid)
        normalized = normalize_prompt_definition_input(
            prompt_id=body.promptId if body.promptId is not None else str(current["prompt_id"]),
            name=body.name if body.name is not None else str(current["name"]),
            source_type=(
                body.sourceType if body.sourceType is not None else str(current["source_type"])
            ),
            source_id=body.sourceId if body.sourceId is not None else str(current["source_id"]),
            owner_plugin_id=(
                body.ownerPluginId
                if body.ownerPluginId is not None
                else str(current["owner_plugin_id"])
            ),
            owner_module=(
                body.ownerModule if body.ownerModule is not None else str(current["owner_module"])
            ),
            module_path=(
                body.modulePath if body.modulePath is not None else str(current["module_path"])
            ),
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
            bundle_refs=(
                body.bundleRefs if body.bundleRefs is not None else list(current["bundle_refs"])
            ),
            config=body.config if body.config is not None else dict(current["config"]),
            tags=body.tags if body.tags is not None else list(current["tags"]),
            metadata=body.metadata if body.metadata is not None else dict(current["metadata"]),
        )
        assert_prompt_id_available(bot.database, normalized.prompt_id, current_uuid=prompt_uuid)
        record = build_prompt_definition_record(
            prompt_uuid=prompt_uuid,
            normalized=normalized,
            created_at=str(current["created_at"]),
        )
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.prompt_definitions.upsert(record)
    payload = bot.database.prompt_definitions.get(prompt_uuid)
    assert payload is not None
    return ok(serialize_prompt_definition(payload))


@router.delete("/{prompt_uuid}")
def delete_prompt_definition(prompt_uuid: str, bot=BotDep):
    try:
        get_prompt_definition_or_raise(bot.database, prompt_uuid)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)
    bot.database.prompt_definitions.delete(prompt_uuid)
    return ok({"deleted": True, "uuid": prompt_uuid})
