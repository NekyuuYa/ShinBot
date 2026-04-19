"""Rendering helpers for prompt components and assembly records."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from shinbot.agent.prompt_manager.schema import (
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptComponentRecord,
    PromptSource,
    PromptSourceType,
    PromptStage,
    stable_text_hash,
)

Resolver = Callable[[PromptAssemblyRequest, PromptComponent, PromptSource], Any]


def infer_component_source(component: PromptComponent) -> PromptSource:
    """Infer prompt source metadata from a component definition."""

    metadata = component.metadata
    source_type = PromptSourceType.UNKNOWN_SOURCE
    owner_plugin_id = str(metadata.get("owner_plugin_id", ""))
    owner_module = str(metadata.get("owner_module", ""))
    module_path = str(metadata.get("module_path", ""))

    if bool(metadata.get("builtin")):
        source_type = PromptSourceType.BUILTIN_SYSTEM
    elif owner_plugin_id:
        if component.stage == PromptStage.CONTEXT:
            source_type = PromptSourceType.CONTEXT_PLUGIN
        else:
            source_type = PromptSourceType.AGENT_PLUGIN
    elif component.stage == PromptStage.ABILITIES:
        source_type = PromptSourceType.TOOLING_MODULE

    return PromptSource(
        source_type=source_type,
        source_id=owner_plugin_id or component.id,
        owner_plugin_id=owner_plugin_id,
        owner_module=owner_module,
        module_path=module_path,
        resolver_name=component.resolver_ref
        if component.kind == PromptComponentKind.RESOLVER
        else "",
        is_builtin=source_type == PromptSourceType.BUILTIN_SYSTEM,
        metadata=dict(metadata),
    )


def render_component_text(
    *,
    component: PromptComponent,
    request: PromptAssemblyRequest,
    source: PromptSource,
    resolvers: dict[str, Resolver],
) -> str:
    """Render one prompt component into plain text."""

    if component.kind == PromptComponentKind.STATIC_TEXT:
        return component.content.strip()
    if component.kind == PromptComponentKind.TEMPLATE:
        return component.content.format(**request.template_inputs).strip()
    if component.kind == PromptComponentKind.RESOLVER:
        resolver = resolvers.get(component.resolver_ref)
        if resolver is None:
            raise ValueError(f"Prompt resolver {component.resolver_ref!r} is not registered")
        result = resolver(request, component, source)
        if isinstance(result, dict):
            return str(result.get("text", "")).strip()
        return str(result).strip()
    if component.kind == PromptComponentKind.EXTERNAL_INJECTION:
        return component.content.strip()
    raise ValueError(f"Unsupported prompt component kind: {component.kind}")


def render_component_structured(
    *,
    component: PromptComponent,
    request: PromptAssemblyRequest,
    source: PromptSource,
    resolvers: dict[str, Resolver],
) -> list[dict[str, Any]]:
    """Render one prompt component as structured data, usually tool definitions."""

    if component.kind == PromptComponentKind.RESOLVER:
        resolver = resolvers.get(component.resolver_ref)
        if resolver is None:
            raise ValueError(f"Prompt resolver {component.resolver_ref!r} is not registered")
        result = resolver(request, component, source)
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            return [result]
        raise ValueError(
            f"ABILITIES resolver {component.resolver_ref!r} must return list[dict] or dict"
        )
    if component.kind == PromptComponentKind.STATIC_TEXT and component.content:
        parsed = json.loads(component.content)
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    return []


def expand_component_tree(
    *,
    component: PromptComponent,
    request: PromptAssemblyRequest,
    components: dict[str, PromptComponent],
    resolvers: dict[str, Resolver],
    records_by_stage: dict[PromptStage, list[PromptComponentRecord]],
    ordered_records: list[PromptComponentRecord],
) -> None:
    """Expand one component, recursively resolving bundles into assembly records."""

    if component.kind == PromptComponentKind.BUNDLE:
        for ref in component.bundle_refs:
            nested = components.get(ref)
            if nested is None:
                raise ValueError(
                    f"Bundle component {component.id!r} references unknown component {ref!r}"
                )
            expand_component_tree(
                component=nested,
                request=request,
                components=components,
                resolvers=resolvers,
                records_by_stage=records_by_stage,
                ordered_records=ordered_records,
            )
        return

    source = infer_component_source(component)

    if component.stage == PromptStage.ABILITIES:
        tool_defs = render_component_structured(
            component=component,
            request=request,
            source=source,
            resolvers=resolvers,
        )
        hash_input = json.dumps(tool_defs, ensure_ascii=False, sort_keys=True)
        record = PromptComponentRecord(
            component_id=component.id,
            stage=component.stage,
            kind=component.kind,
            version=component.version,
            priority=component.priority,
            source=source,
            rendered_data=tool_defs,
            text_hash=stable_text_hash(hash_input),
            cache_stable=component.cache_stable,
            metadata=dict(component.metadata),
        )
        records_by_stage[component.stage].append(record)
        ordered_records.append(record)
        return

    rendered_text = render_component_text(
        component=component,
        request=request,
        source=source,
        resolvers=resolvers,
    )

    if component.stage == PromptStage.CONTEXT and rendered_text:
        rendered_messages = [{"role": "user", "content": rendered_text}]
        hash_input = json.dumps(rendered_messages, ensure_ascii=False, sort_keys=True)
        record = PromptComponentRecord(
            component_id=component.id,
            stage=component.stage,
            kind=component.kind,
            version=component.version,
            priority=component.priority,
            source=source,
            rendered_text=rendered_text,
            rendered_messages=rendered_messages,
            text_hash=stable_text_hash(hash_input),
            cache_stable=component.cache_stable,
            metadata=dict(component.metadata),
        )
        records_by_stage[component.stage].append(record)
        ordered_records.append(record)
        return

    record = PromptComponentRecord(
        component_id=component.id,
        stage=component.stage,
        kind=component.kind,
        version=component.version,
        priority=component.priority,
        source=source,
        rendered_text=rendered_text,
        text_hash=stable_text_hash(rendered_text),
        cache_stable=component.cache_stable,
        metadata=dict(component.metadata),
    )
    records_by_stage[component.stage].append(record)
    ordered_records.append(record)
