"""Runtime syncing helpers for file-backed prompt artifacts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

if TYPE_CHECKING:
    from shinbot.admin.prompt_definition_admin import PromptDefinitionFileRepository
    from shinbot.agent.services.prompt_engine import PromptRegistry


def build_runtime_component_ids(
    prompt_definitions: PromptDefinitionFileRepository,
    prompt_registry: PromptRegistry,
    *,
    agent: dict[str, object] | None,
) -> tuple[list[str], list[str]]:
    """Resolve agent prompt refs into registered component ids."""

    component_ids: list[str] = []
    unresolved_refs: list[str] = []

    for prompt_ref in (agent or {}).get("prompts", []):
        normalized = str(prompt_ref).strip()
        if not normalized:
            continue
        component = prompt_registry.get_component(normalized)
        if component is not None:
            component_ids.append(component.id)
            continue
        payload = prompt_definitions.get_by_prompt_id(normalized)
        if payload is not None:
            component_ids.append(sync_prompt_definition_component(prompt_registry, payload))
            continue
        unresolved_refs.append(normalized)

    seen: set[str] = set()
    deduped: list[str] = []
    for component_id in component_ids:
        if component_id and component_id not in seen:
            seen.add(component_id)
            deduped.append(component_id)
    return deduped, unresolved_refs


def sync_prompt_definition_component(
    prompt_registry: PromptRegistry,
    payload: dict[str, object],
) -> str:
    """Upsert one file-backed prompt definition into the registry."""

    metadata = dict(payload.get("metadata") or {})
    metadata.setdefault("display_name", str(payload.get("name", "")).strip())
    metadata.setdefault("description", str(payload.get("description", "")).strip())
    for key in ("source_type", "source_id", "owner_plugin_id", "owner_module", "module_path"):
        value = str(payload.get(key, "") or "").strip()
        if value:
            metadata.setdefault(key, value)

    component = PromptComponent(
        id=str(payload["prompt_id"]),
        stage=PromptStage(str(payload["stage"])),
        kind=PromptComponentKind(str(payload["type"])),
        version=str(payload.get("version", "1.0.0")),
        priority=int(payload.get("priority", 100)),
        enabled=bool(payload.get("enabled", True)),
        content=str(payload.get("content", "")),
        template_vars=list(payload.get("template_vars", [])),
        resolver_ref=str(payload.get("resolver_ref", "")),
        bundle_refs=list(payload.get("bundle_refs", [])),
        tags=list(payload.get("tags", [])),
        metadata=metadata,
    )
    prompt_registry.upsert_component(component)
    return component.id


def sync_prompt_definition_components(
    prompt_registry: PromptRegistry,
    prompt_definitions: PromptDefinitionFileRepository,
) -> list[str]:
    """Upsert all file-backed prompt definitions into the registry."""

    return [
        sync_prompt_definition_component(prompt_registry, payload)
        for payload in prompt_definitions.list()
    ]
