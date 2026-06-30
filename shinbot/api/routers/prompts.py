"""Prompt file management router: /api/v1/prompts."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote, unquote

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.admin.prompt_definition_admin import (
    PromptDefinitionAdminError,
    PromptDefinitionFileRepository,
    assert_no_runtime_prompt_conflict,
    get_prompt_definition_or_raise,
    normalize_prompt_definition_input,
    serialize_prompt_definition,
)
from shinbot.agent.services.prompt_engine.discovery import discover_file_backed_prompts
from shinbot.agent.services.prompt_engine.files import (
    PromptFileCatalogService,
    PromptFileError,
    PromptFileLoadConfig,
    PromptFileManifest,
    load_prompt_component,
    parse_prompt_markdown,
)
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok
from shinbot.persistence.records import utc_now_iso

router = APIRouter(
    prefix="/prompts",
    tags=["prompts"],
    dependencies=AuthRequired,
)

PromptLayer = Literal["runtime", "custom"]


class PromptCatalogItem(BaseModel):
    id: str
    fileId: str
    layer: str
    locale: str
    displayName: str
    description: str
    stage: str
    type: str
    version: str
    priority: int
    enabled: bool
    resolverRef: str
    templateVars: list[str]
    bundleRefs: list[str]
    tags: list[str]
    sourceType: str
    sourceId: str
    ownerPluginId: str
    ownerModule: str
    modulePath: str
    editable: bool
    deletable: bool
    resettable: bool
    sourceStatus: str
    loadedFrom: str
    sourcePath: str
    runtimePath: str
    loadedPath: str
    metadata: dict[str, Any]


class PromptFileData(PromptCatalogItem):
    promptId: str
    name: str
    content: str
    config: dict[str, Any]
    createdAt: str
    lastModified: str


class PromptFilePatchRequest(BaseModel):
    promptId: str | None = None
    name: str | None = None
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


class CustomPromptCreateRequest(BaseModel):
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


class PromptFileDeleted(BaseModel):
    deleted: bool
    fileId: str


class PromptFileReset(BaseModel):
    reset: bool
    fileId: str


def _prompt_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": payload["id"],
        "displayName": payload["display_name"],
        "description": payload["description"],
        "stage": payload["stage"],
        "type": payload["type"],
        "version": payload["version"],
        "priority": payload["priority"],
        "enabled": payload["enabled"],
        "resolverRef": payload["resolver_ref"],
        "templateVars": payload["template_vars"],
        "bundleRefs": payload["bundle_refs"],
        "tags": payload["tags"],
        "sourceType": payload["source_type"],
        "sourceId": payload["source_id"],
        "ownerPluginId": payload["owner_plugin_id"],
        "ownerModule": payload["owner_module"],
        "modulePath": payload["module_path"],
        "metadata": payload["metadata"],
    }


def _prompt_definition_dict(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(payload.get("metadata") or {})
    return {
        "id": payload["prompt_id"],
        "displayName": str(
            metadata.get("display_name") or payload.get("name") or payload["prompt_id"]
        ),
        "description": str(metadata.get("description") or payload.get("description") or ""),
        "stage": payload["stage"],
        "type": payload["type"],
        "version": payload["version"],
        "priority": payload["priority"],
        "enabled": payload["enabled"],
        "resolverRef": payload["resolver_ref"],
        "templateVars": payload["template_vars"],
        "bundleRefs": payload["bundle_refs"],
        "tags": payload["tags"],
        "sourceType": payload["source_type"],
        "sourceId": payload["source_id"],
        "ownerPluginId": payload["owner_plugin_id"],
        "ownerModule": payload["owner_module"],
        "modulePath": payload["module_path"],
        "metadata": metadata,
    }


def _raise_admin_http_error(exc: PromptDefinitionAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


def _raise_prompt_file_error(exc: PromptFileError) -> None:
    raise HTTPException(
        status_code=422,
        detail={"code": "INVALID_PROMPT_FILE", "message": str(exc)},
    ) from exc


def _encode_file_id(layer: PromptLayer, *parts: str) -> str:
    return "~".join([layer, *(quote(part, safe="") for part in parts)])


def _decode_file_id(file_id: str) -> tuple[str, list[str]]:
    layer, sep, rest = file_id.partition("~")
    if not sep:
        raise HTTPException(
            status_code=404,
            detail={"code": "PROMPT_NOT_FOUND", "message": f"Prompt file {file_id!r} was not found"},
        )
    return layer, [unquote(part) for part in rest.split("~")]


def _normalize_file_id(file_id: str) -> str:
    layer, parts = _decode_file_id(file_id)
    if layer == "runtime" and len(parts) == 2:
        return _encode_file_id("runtime", parts[0], parts[1])
    if layer == "custom" and len(parts) == 1:
        return _encode_file_id("custom", parts[0])
    raise HTTPException(
        status_code=404,
        detail={"code": "PROMPT_NOT_FOUND", "message": f"Prompt file {file_id!r} was not found"},
    )


def _prompt_repository(boot: Any) -> PromptDefinitionFileRepository:
    return PromptDefinitionFileRepository.from_data_dir(boot.data_dir)


def _active_or_discovered_registry(bot: Any, boot: Any, *, sync_runtime: bool = True) -> Any:
    agent_runtime = getattr(bot, "agent_runtime", None)
    prompt_registry = getattr(agent_runtime, "prompt_registry", None)
    catalog = getattr(prompt_registry, "prompt_file_catalog", None)
    if isinstance(catalog, PromptFileCatalogService) and catalog.list():
        return prompt_registry
    config = getattr(agent_runtime, "prompt_file_config", None)
    if not isinstance(config, PromptFileLoadConfig):
        config = PromptFileLoadConfig.from_data_dir(boot.data_dir, sync_to_data=sync_runtime)
    elif config.sync_to_data != sync_runtime:
        config = PromptFileLoadConfig(
            locale=config.locale,
            fallback_locales=config.fallback_locales,
            data_root=config.data_root,
            sync_to_data=sync_runtime,
        )
    return discover_file_backed_prompts(boot.data_dir, prompt_file_config=config)


def _component_catalog_by_id(registry: Any) -> dict[str, dict[str, Any]]:
    return {item["id"]: _prompt_dict(item) for item in registry.list_component_catalog()}


def _runtime_catalog_prompt_ids(bot: Any, boot: Any) -> set[str]:
    registry = _active_or_discovered_registry(bot, boot, sync_runtime=False)
    component_ids = {str(item["id"]) for item in registry.list_component_catalog()}
    return {
        manifest.prompt_id
        for manifest in registry.prompt_file_catalog.list()
        if manifest.prompt_id in component_ids
    }


def _normalized_prompt_file_payload(path: Path) -> tuple[dict[str, Any], str]:
    front_matter, body = parse_prompt_markdown(path.read_text(encoding="utf-8"), path=path)
    return front_matter, body.strip()


def _runtime_source_status(*, source_path: Path, runtime_path: Path) -> str:
    if not source_path.is_file():
        return "missing_source"
    if not runtime_path.is_file():
        return "source"
    try:
        source_payload = _normalized_prompt_file_payload(source_path)
        runtime_payload = _normalized_prompt_file_payload(runtime_path)
    except PromptFileError:
        return "runtime_synced" if source_path.read_bytes() == runtime_path.read_bytes() else "runtime_modified"
    return "runtime_synced" if source_payload == runtime_payload else "runtime_modified"


def _runtime_catalog_item(
    manifest: PromptFileManifest,
    component_item: dict[str, Any],
) -> dict[str, Any]:
    manifest = manifest.refresh()
    source_status = _runtime_source_status(
        source_path=manifest.source_path,
        runtime_path=manifest.runtime_path,
    )
    item = dict(component_item)
    item.update(
        {
            "fileId": _encode_file_id("runtime", manifest.locale, manifest.prompt_id),
            "layer": "runtime",
            "locale": manifest.locale,
            "editable": True,
            "deletable": False,
            "resettable": manifest.source_exists and manifest.runtime_exists,
            "sourceStatus": source_status,
            "loadedFrom": manifest.loaded_from,
            "sourcePath": str(manifest.source_path),
            "runtimePath": str(manifest.runtime_path),
            "loadedPath": str(manifest.loaded_path),
        }
    )
    return item


def _custom_catalog_item(payload: dict[str, Any]) -> dict[str, Any]:
    item = _prompt_definition_dict(payload)
    item.update(
        {
            "fileId": _encode_file_id("custom", payload["prompt_id"]),
            "layer": "custom",
            "locale": "custom",
            "editable": True,
            "deletable": True,
            "resettable": False,
            "sourceStatus": "custom",
            "loadedFrom": "custom",
            "sourcePath": "",
            "runtimePath": str(payload.get("path") or ""),
            "loadedPath": str(payload.get("path") or ""),
        }
    )
    return item


def _catalog_items(bot: Any, boot: Any, *, sync_runtime: bool = False) -> dict[str, dict[str, Any]]:
    registry = _active_or_discovered_registry(bot, boot, sync_runtime=sync_runtime)
    catalog_by_id = _component_catalog_by_id(registry)
    items_by_file_id: dict[str, dict[str, Any]] = {}

    for manifest in registry.prompt_file_catalog.list():
        component_item = catalog_by_id.get(manifest.prompt_id)
        if component_item is None:
            continue
        item = _runtime_catalog_item(manifest, component_item)
        items_by_file_id[item["fileId"]] = item

    for payload in _prompt_repository(boot).list():
        item = _custom_catalog_item(payload)
        items_by_file_id[item["fileId"]] = item

    return items_by_file_id


def _reload_prompt_runtime(bot: Any) -> None:
    agent_runtime = getattr(bot, "agent_runtime", None)
    reload_prompt_files = getattr(agent_runtime, "reload_prompt_files", None)
    if callable(reload_prompt_files):
        reload_prompt_files()


def _get_catalog_item(file_id: str, bot: Any, boot: Any, *, sync_runtime: bool = False) -> dict[str, Any]:
    normalized_file_id = _normalize_file_id(file_id)
    item = _catalog_items(bot, boot, sync_runtime=sync_runtime).get(normalized_file_id)
    if item is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "PROMPT_NOT_FOUND", "message": f"Prompt file {file_id!r} was not found"},
        )
    return item


def _assert_not_runtime_prompt_conflict(prompt_id: str, bot: Any, boot: Any) -> None:
    try:
        assert_no_runtime_prompt_conflict(
            prompt_id,
            runtime_prompt_ids=_runtime_catalog_prompt_ids(bot, boot),
        )
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)


def _runtime_file_data(item: dict[str, Any]) -> dict[str, Any]:
    runtime_path = Path(str(item["runtimePath"]))
    source_path = Path(str(item["sourcePath"]))
    path = runtime_path if runtime_path.is_file() else source_path
    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "PROMPT_NOT_FOUND",
                "message": f"Prompt source file for {item['id']!r} was not found",
            },
        )
    try:
        _front_matter, content = parse_prompt_markdown(path.read_text(encoding="utf-8"), path=path)
        component = load_prompt_component(
            path,
            locale=str(item["locale"]),
            source_path=source_path,
            runtime_path=runtime_path,
            expected_id=str(item["id"]),
        )
    except PromptFileError as exc:
        _raise_prompt_file_error(exc)
    stat = path.stat()
    metadata = dict(component.metadata)
    display_name = str(
        _front_matter.get("name")
        or metadata.get("display_name")
        or metadata.get("title")
        or item["displayName"]
    )
    description = str(
        _front_matter.get("description")
        or metadata.get("description")
        or item["description"]
    )
    return {
        **item,
        "promptId": item["id"],
        "name": display_name,
        "displayName": display_name,
        "description": description,
        "stage": component.stage.value,
        "type": component.kind.value,
        "version": component.version,
        "priority": component.priority,
        "enabled": component.enabled,
        "resolverRef": component.resolver_ref,
        "templateVars": list(component.template_vars),
        "bundleRefs": list(component.bundle_refs),
        "tags": list(component.tags),
        "content": content.strip(),
        "config": dict(_front_matter.get("config") or {}),
        "createdAt": "",
        "lastModified": str(stat.st_mtime),
        "runtimePath": str(runtime_path),
        "loadedPath": str(path),
        "sourceStatus": _runtime_source_status(
            source_path=source_path,
            runtime_path=runtime_path,
        ),
        "loadedFrom": "runtime" if runtime_path.is_file() else item["loadedFrom"],
        "resettable": source_path.is_file() and runtime_path.is_file(),
        "metadata": metadata,
    }


def _custom_file_data(item: dict[str, Any], boot: Any) -> dict[str, Any]:
    prompt_id = _decode_file_id(str(item["fileId"]))[1][0]
    try:
        payload = get_prompt_definition_or_raise(_prompt_repository(boot), prompt_id)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)
    serialized = serialize_prompt_definition(payload)
    return {
        **item,
        "promptId": serialized["promptId"],
        "name": serialized["name"],
        "content": serialized["content"],
        "config": serialized["config"],
        "createdAt": serialized["createdAt"],
        "lastModified": serialized["lastModified"],
    }


def _patch_runtime_prompt(item: dict[str, Any], body: PromptFilePatchRequest) -> dict[str, Any]:
    allowed_fields = {"content"}
    requested_fields = set(body.model_fields_set)
    blocked_fields = sorted(requested_fields - allowed_fields)
    if blocked_fields:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_ACTION",
                "message": "Runtime prompt files only allow content edits",
            },
        )
    if "content" not in requested_fields:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_ACTION",
                "message": "Runtime prompt PATCH requires content",
            },
        )
    path = Path(str(item["runtimePath"]))
    if not path.is_file():
        source_path = Path(str(item["sourcePath"]))
        if not source_path.is_file():
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "PROMPT_NOT_FOUND",
                    "message": f"Prompt source file for {item['id']!r} was not found",
                },
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, path)

    try:
        front_matter, current_body = parse_prompt_markdown(
            path.read_text(encoding="utf-8"),
            path=path,
        )
    except PromptFileError as exc:
        _raise_prompt_file_error(exc)

    content = current_body if body.content is None else body.content
    import yaml

    yaml_text = yaml.safe_dump(
        front_matter,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    ).strip()
    previous_text = path.read_text(encoding="utf-8")
    path.write_text(f"---\n{yaml_text}\n---\n\n{content.strip()}\n", encoding="utf-8")
    try:
        load_prompt_component(
            path,
            locale=str(item["locale"]),
            source_path=Path(str(item["sourcePath"])),
            runtime_path=path,
            expected_id=str(item["id"]),
        )
    except PromptFileError as exc:
        path.write_text(previous_text, encoding="utf-8")
        _raise_prompt_file_error(exc)
    payload = _runtime_file_data(item)
    return payload


def _patch_custom_prompt(
    item: dict[str, Any],
    body: PromptFilePatchRequest,
    bot: Any,
    boot: Any,
) -> dict[str, Any]:
    prompt_id = _decode_file_id(str(item["fileId"]))[1][0]
    try:
        repository = _prompt_repository(boot)
        current = get_prompt_definition_or_raise(repository, prompt_id)
        normalized = normalize_prompt_definition_input(
            prompt_id=body.promptId if body.promptId is not None else str(current["prompt_id"]),
            name=body.name if body.name is not None else str(current["name"]),
            source_type=str(current["source_type"]),
            source_id=str(current["source_id"]),
            owner_plugin_id=str(current["owner_plugin_id"]),
            owner_module=str(current["owner_module"]),
            module_path=str(current["module_path"]),
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
                body.bundleRefs
                if body.bundleRefs is not None
                else list(current["bundle_refs"])
            ),
            config=body.config if body.config is not None else dict(current["config"]),
            tags=body.tags if body.tags is not None else list(current["tags"]),
            metadata=body.metadata if body.metadata is not None else dict(current["metadata"]),
        )
        if normalized.prompt_id != str(current["prompt_id"]):
            _assert_not_runtime_prompt_conflict(normalized.prompt_id, bot, boot)
        payload = repository.update(prompt_id, normalized)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)
    next_item = _custom_catalog_item(payload)
    return _custom_file_data(next_item, boot)


@router.get("", response_model=Envelope[list[PromptCatalogItem]])
async def list_prompts(bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """List registered runtime prompt files and custom prompt definitions."""

    items = sorted(
        _catalog_items(bot, boot).values(),
        key=lambda item: (
            str(item["layer"]),
            str(item["stage"]),
            int(item["priority"]),
            str(item["id"]),
            str(item["locale"]),
        ),
    )
    return ok(items)


@router.post("/custom", status_code=201, response_model=Envelope[PromptFileData])
def create_custom_prompt(body: CustomPromptCreateRequest, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Create a new custom prompt file."""

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
            metadata={**body.metadata, "updated_at": utc_now_iso()},
        )
        _assert_not_runtime_prompt_conflict(normalized.prompt_id, bot, boot)
        payload = repository.create(normalized)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)
    item = _custom_catalog_item(payload)
    _reload_prompt_runtime(bot)
    return ok(_custom_file_data(item, boot))


@router.get("/{file_id}", response_model=Envelope[PromptFileData])
def get_prompt_file(file_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Retrieve a runtime or custom prompt file."""

    item = _get_catalog_item(file_id, bot, boot, sync_runtime=False)
    if item["layer"] == "runtime":
        return ok(_runtime_file_data(item))
    return ok(_custom_file_data(item, boot))


@router.patch("/{file_id}", response_model=Envelope[PromptFileData])
def patch_prompt_file(file_id: str, body: PromptFilePatchRequest, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Patch an editable runtime or custom prompt file."""

    item = _get_catalog_item(file_id, bot, boot, sync_runtime=False)
    if item["layer"] == "runtime":
        payload = _patch_runtime_prompt(item, body)
    else:
        payload = _patch_custom_prompt(item, body, bot, boot)
    _reload_prompt_runtime(bot)
    return ok(payload)


@router.delete("/{file_id}", response_model=Envelope[PromptFileDeleted])
def delete_prompt_file(file_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Delete a custom prompt file."""

    item = _get_catalog_item(file_id, bot, boot)
    if item["layer"] != "custom":
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_ACTION", "message": "Only custom prompts can be deleted"},
        )
    prompt_id = _decode_file_id(str(item["fileId"]))[1][0]
    try:
        _prompt_repository(boot).delete(prompt_id)
    except PromptDefinitionAdminError as exc:
        _raise_admin_http_error(exc)
    _reload_prompt_runtime(bot)
    return ok({"deleted": True, "fileId": str(item["fileId"])})


@router.post("/{file_id}/reset", response_model=Envelope[PromptFileReset])
def reset_prompt_file(file_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Reset a runtime prompt copy to the source prompt content."""

    item = _get_catalog_item(file_id, bot, boot)
    if item["layer"] != "runtime":
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_ACTION", "message": "Only runtime prompts can be reset"},
        )
    source_path = Path(str(item["sourcePath"]))
    runtime_path = Path(str(item["runtimePath"]))
    if not source_path.is_file():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "PROMPT_NOT_FOUND",
                "message": f"Prompt source file for {item['id']!r} was not found",
            },
        )
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, runtime_path)
    _reload_prompt_runtime(bot)
    return ok({"reset": True, "fileId": str(item["fileId"])})
