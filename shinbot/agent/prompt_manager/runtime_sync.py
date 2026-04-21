"""Runtime syncing helpers for DB-backed prompt artifacts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shinbot.agent.prompt_manager.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

if TYPE_CHECKING:
    from shinbot.agent.prompt_manager import PromptRegistry
    from shinbot.persistence.engine import DatabaseManager


def build_runtime_component_ids(
    database: DatabaseManager,
    prompt_registry: PromptRegistry,
    *,
    persona: dict[str, object] | None,
    agent: dict[str, object] | None,
) -> tuple[list[str], list[str]]:
    """Resolve persona/agent prompt refs into registered component ids."""

    component_ids: list[str] = []
    unresolved_refs: list[str] = []

    persona_prompt_uuid = str((persona or {}).get("prompt_definition_uuid") or "").strip()
    if persona_prompt_uuid:
        payload = database.prompt_definitions.get(persona_prompt_uuid)
        if payload is not None:
            component_ids.append(sync_prompt_definition_component(prompt_registry, payload))

    for prompt_ref in (agent or {}).get("prompts", []):
        normalized = str(prompt_ref).strip()
        if not normalized:
            continue
        payload = database.prompt_definitions.get(normalized)
        if payload is None:
            payload = database.prompt_definitions.get_by_prompt_id(normalized)
        if payload is not None:
            component_ids.append(sync_prompt_definition_component(prompt_registry, payload))
            continue
        component = prompt_registry.get_component(normalized)
        if component is not None:
            component_ids.append(component.id)
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
    """Upsert one DB-backed prompt_definition into the registry."""

    metadata = dict(payload.get("metadata") or {})
    metadata.setdefault("display_name", str(payload.get("name", "")).strip())
    metadata.setdefault("description", str(payload.get("description", "")).strip())
    for key in ("owner_plugin_id", "owner_module", "module_path"):
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
