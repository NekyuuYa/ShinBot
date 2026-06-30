"""Prompt definition management router: /api/v1/prompt-definitions"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.admin.prompt_definition_admin import (
    PromptDefinitionAdminError,
    PromptDefinitionFileRepository,
    assert_no_runtime_prompt_conflict,
    assert_prompt_id_available,
    get_prompt_definition_or_raise,
    normalize_prompt_definition_input,
    serialize_prompt_definition,
)
from shinbot.agent.services.prompt_engine.discovery import discover_file_backed_prompts
from shinbot.agent.services.prompt_engine.files import (
    PromptFileCatalogService,
    PromptFileLoadConfig,
)
from shinbot.agent.services.prompt_engine.registry import PromptRegistry
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok

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


class PromptDefinitionSource(BaseModel):
    sourceType: str
    sourceId: str
    ownerPluginId: str
    ownerModule: str
    modulePath: str


class PromptDefinitionData(BaseModel):
    uuid: str
    promptId: str
    name: str
    source: PromptDefinitionSource
    stage: str
    type: str
    priority: int
    version: str
    description: str
    enabled: bool
    content: str
    templateVars: list[str]
    resolverRef: str
    bundleRefs: list[str]
    config: dict[str, Any]
    tags: list[str]
    metadata: dict[str, Any]
    createdAt: str
    lastModified: str


class PromptDefinitionDeleted(BaseModel):
    deleted: bool
    uuid: str


def _raise_admin_http_error(exc: PromptDefinitionAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


def _prompt_repository(boot: Any) -> PromptDefinitionFileRepository:
    return PromptDefinitionFileRepository.from_data_dir(boot.data_dir)


def _active_or_discovered_registry(bot: Any, boot: Any) -> PromptRegistry:
    agent_runtime = getattr(bot, "agent_runtime", None)
    prompt_registry = getattr(agent_runtime, "prompt_registry", None)
    catalog = getattr(prompt_registry, "prompt_file_catalog", None)
    if isinstance(catalog, PromptFileCatalogService) and catalog.list():
        return prompt_registry
    config = getattr(agent_runtime, "prompt_file_config", None)
    if not isinstance(config, PromptFileLoadConfig):
        config = PromptFileLoadConfig.from_data_dir(boot.data_dir, sync_to_data=False)
    elif config.sync_to_data:
        config = PromptFileLoadConfig(
            locale=config.locale,
            fallback_locales=config.fallback_locales,
            data_root=config.data_root,
            sync_to_data=False,
        )
    return discover_file_backed_prompts(boot.data_dir, prompt_file_config=config)


def _runtime_catalog_prompt_ids(bot: Any, boot: Any) -> set[str]:
    registry = _active_or_discovered_registry(bot, boot)
    component_ids = {str(item["id"]) for item in registry.list_component_catalog()}
    return {
        manifest.prompt_id
        for manifest in registry.prompt_file_catalog.list()
        if manifest.prompt_id in component_ids
    }


def _assert_not_runtime_prompt_conflict(prompt_id: str, bot: Any, boot: Any) -> None:
    assert_no_runtime_prompt_conflict(
        prompt_id,
        runtime_prompt_ids=_runtime_catalog_prompt_ids(bot, boot),
    )


@router.get("", response_model=Envelope[list[PromptDefinitionData]])
def list_prompt_definitions(boot: Any = BootDep) -> dict[str, Any]:
    """List all prompt definitions from the file-based repository."""
    try:
        return ok([serialize_prompt_definition(item) for item in _prompt_repository(boot).list()])
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)


@router.post("", status_code=201, response_model=Envelope[PromptDefinitionData])
def create_prompt_definition(body: PromptDefinitionRequest, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Create a new prompt definition and persist it to the file repository."""
    try:
        repository = _prompt_repository(boot)
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
        assert_prompt_id_available(repository, normalized.prompt_id, current_uuid=None)
        _assert_not_runtime_prompt_conflict(normalized.prompt_id, bot, boot)
        payload = repository.create(normalized)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)

    return ok(serialize_prompt_definition(payload))


@router.get("/{prompt_uuid}", response_model=Envelope[PromptDefinitionData])
def get_prompt_definition(prompt_uuid: str, boot: Any = BootDep) -> dict[str, Any]:
    """Retrieve a single prompt definition by its UUID."""
    try:
        payload = get_prompt_definition_or_raise(_prompt_repository(boot), prompt_uuid)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(serialize_prompt_definition(payload))


@router.patch("/{prompt_uuid}", response_model=Envelope[PromptDefinitionData])
def patch_prompt_definition(
    prompt_uuid: str,
    body: PromptDefinitionPatchRequest,
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Partially update a prompt definition by its UUID."""
    try:
        repository = _prompt_repository(boot)
        current = get_prompt_definition_or_raise(repository, prompt_uuid)
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
                body.templateVars
                if body.templateVars is not None
                else list(current["template_vars"])
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
        assert_prompt_id_available(repository, normalized.prompt_id, current_uuid=prompt_uuid)
        if normalized.prompt_id != str(current["prompt_id"]):
            _assert_not_runtime_prompt_conflict(normalized.prompt_id, bot, boot)
        payload = repository.update(prompt_uuid, normalized)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)

    return ok(serialize_prompt_definition(payload))


@router.delete("/{prompt_uuid}", response_model=Envelope[PromptDefinitionDeleted])
def delete_prompt_definition(prompt_uuid: str, boot: Any = BootDep) -> dict[str, Any]:
    """Delete a prompt definition by its UUID."""
    try:
        _prompt_repository(boot).delete(prompt_uuid)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)
    return ok({"deleted": True, "uuid": prompt_uuid})
