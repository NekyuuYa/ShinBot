"""Prompt registry and basic assembly service."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

from shinbot.agent.prompting.schema import (
    PROMPT_STAGE_ORDER,
    PromptAssemblyRequest,
    PromptAssemblyResult,
    PromptComponent,
    PromptComponentKind,
    PromptComponentRecord,
    PromptLoggerRecord,
    PromptProfile,
    PromptSnapshot,
    PromptSource,
    PromptSourceType,
    PromptStage,
    PromptStageBlock,
    stable_text_hash,
)

Resolver = Callable[[PromptAssemblyRequest, PromptComponent, PromptSource], Any]


class PromptRegistry:
    """In-memory prompt registry with deterministic assembly."""

    def __init__(self) -> None:
        self._components: dict[str, PromptComponent] = {}
        self._profiles: dict[str, PromptProfile] = {}
        self._resolvers: dict[str, Resolver] = {}

    def register_component(self, component: PromptComponent) -> None:
        if component.id in self._components:
            raise ValueError(f"Prompt component {component.id!r} is already registered")
        self._components[component.id] = component

    def register_profile(self, profile: PromptProfile) -> None:
        if profile.id in self._profiles:
            raise ValueError(f"Prompt profile {profile.id!r} is already registered")
        self._profiles[profile.id] = profile

    def register_resolver(self, name: str, fn: Resolver) -> None:
        if name in self._resolvers:
            raise ValueError(f"Prompt resolver {name!r} is already registered")
        self._resolvers[name] = fn

    def get_component(self, component_id: str) -> PromptComponent | None:
        return self._components.get(component_id)

    def get_profile(self, profile_id: str) -> PromptProfile | None:
        return self._profiles.get(profile_id)

    def list_components(self, stage: PromptStage | None = None) -> list[PromptComponent]:
        components = list(self._components.values())
        if stage is not None:
            components = [component for component in components if component.stage == stage]
        return sorted(components, key=lambda item: (item.priority, item.id, item.version))

    def list_profiles(self) -> list[PromptProfile]:
        return list(self._profiles.values())

    def assemble(self, request: PromptAssemblyRequest) -> PromptAssemblyResult:
        profile = self._profiles.get(request.profile_id)
        if request.profile_id and profile is None:
            raise ValueError(f"Prompt profile {request.profile_id!r} is not registered")

        component_ids: list[str] = []
        if profile is not None:
            component_ids.extend(profile.base_components)
            component_ids.extend(profile.default_constraints)
        component_ids.extend(request.component_overrides)
        component_ids = self._dedupe(component_ids)

        disabled = set(request.disabled_components)
        records_by_stage: dict[PromptStage, list[PromptComponentRecord]] = {
            stage: [] for stage in PROMPT_STAGE_ORDER
        }
        ordered_records: list[PromptComponentRecord] = []

        for component_id in component_ids:
            if component_id in disabled:
                continue
            component = self._components.get(component_id)
            if component is None:
                raise ValueError(f"Prompt component {component_id!r} is not registered")
            if not component.enabled:
                continue
            self._expand_component(component, request, records_by_stage, ordered_records)

        self._inject_payloads(request, records_by_stage, ordered_records)

        if not records_by_stage[PromptStage.SYSTEM_BASE]:
            raise ValueError("Prompt assembly requires at least one system_base component")

        stage_blocks: list[PromptStageBlock] = []
        final_parts: list[str] = []
        has_unknown_source = False

        for stage in PROMPT_STAGE_ORDER:
            records = sorted(
                records_by_stage[stage],
                key=lambda item: (item.priority, item.component_id, item.version),
            )
            rendered_text = "\n\n".join(record.rendered_text for record in records if record.rendered_text)
            token_estimate = len(rendered_text.split()) if rendered_text else 0
            stage_block = PromptStageBlock(
                stage=stage,
                components=records,
                rendered_text=rendered_text,
                token_estimate=token_estimate,
            )
            stage_blocks.append(stage_block)
            if rendered_text:
                final_parts.append(rendered_text)
            if any(record.source.source_type == PromptSourceType.UNKNOWN_SOURCE for record in records):
                has_unknown_source = True

        final_prompt = "\n\n".join(part for part in final_parts if part)
        prompt_signature = self._build_signature(stage_blocks)
        cache_key = self._build_cache_key(prompt_signature, request)

        return PromptAssemblyResult(
            profile_id=request.profile_id,
            caller=request.caller,
            stages=stage_blocks,
            ordered_components=ordered_records,
            final_prompt=final_prompt,
            prompt_signature=prompt_signature,
            cache_key=cache_key,
            compatibility_used=bool(records_by_stage[PromptStage.COMPATIBILITY]),
            has_unknown_source=has_unknown_source,
            truncation={},
            metadata=dict(request.metadata),
        )

    def create_snapshot(
        self, result: PromptAssemblyResult, request: PromptAssemblyRequest
    ) -> PromptSnapshot:
        return PromptSnapshot(
            profile_id=result.profile_id,
            caller=result.caller,
            session_id=request.session_id,
            instance_id=request.instance_id,
            route_id=request.route_id,
            model_id=request.model_id,
            prompt_signature=result.prompt_signature,
            cache_key=result.cache_key,
            components=result.ordered_components,
            stages=result.stages,
            final_prompt=result.final_prompt,
            compatibility_used=result.compatibility_used,
            truncation=result.truncation,
            metadata=dict(result.metadata),
        )

    def build_log_record(
        self, result: PromptAssemblyResult, request: PromptAssemblyRequest
    ) -> PromptLoggerRecord:
        unknown_sources = sum(
            1
            for component in result.ordered_components
            if component.source.source_type == PromptSourceType.UNKNOWN_SOURCE
        )
        return PromptLoggerRecord(
            profile_id=result.profile_id,
            caller=result.caller,
            session_id=request.session_id,
            instance_id=request.instance_id,
            route_id=request.route_id,
            model_id=request.model_id,
            prompt_signature=result.prompt_signature,
            cache_key=result.cache_key,
            compatibility_used=result.compatibility_used,
            selected_component_count=len(result.ordered_components),
            unknown_source_count=unknown_sources,
            truncation_summary=dict(result.truncation),
            metadata=dict(result.metadata),
        )

    def _expand_component(
        self,
        component: PromptComponent,
        request: PromptAssemblyRequest,
        records_by_stage: dict[PromptStage, list[PromptComponentRecord]],
        ordered_records: list[PromptComponentRecord],
    ) -> None:
        if component.kind == PromptComponentKind.BUNDLE:
            for ref in component.bundle_refs:
                nested = self._components.get(ref)
                if nested is None:
                    raise ValueError(
                        f"Bundle component {component.id!r} references unknown component {ref!r}"
                    )
                self._expand_component(nested, request, records_by_stage, ordered_records)
            return

        source = self._infer_source(component)
        rendered_text = self._render_component(component, request, source)
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

    def _render_component(
        self, component: PromptComponent, request: PromptAssemblyRequest, source: PromptSource
    ) -> str:
        if component.kind == PromptComponentKind.STATIC_TEXT:
            return component.content.strip()
        if component.kind == PromptComponentKind.TEMPLATE:
            return component.content.format(**request.template_inputs).strip()
        if component.kind == PromptComponentKind.RESOLVER:
            resolver = self._resolvers.get(component.resolver_ref)
            if resolver is None:
                raise ValueError(f"Prompt resolver {component.resolver_ref!r} is not registered")
            result = resolver(request, component, source)
            if isinstance(result, dict):
                return str(result.get("text", "")).strip()
            return str(result).strip()
        if component.kind == PromptComponentKind.EXTERNAL_INJECTION:
            return component.content.strip()
        raise ValueError(f"Unsupported prompt component kind: {component.kind}")

    def _inject_payloads(
        self,
        request: PromptAssemblyRequest,
        records_by_stage: dict[PromptStage, list[PromptComponentRecord]],
        ordered_records: list[PromptComponentRecord],
    ) -> None:
        if request.instruction_payload:
            record = self._make_payload_record(
                component_id="__request_instructions__",
                stage=PromptStage.INSTRUCTIONS,
                payload=request.instruction_payload,
            )
            records_by_stage[PromptStage.INSTRUCTIONS].append(record)
            ordered_records.append(record)

        if request.constraint_payload:
            record = self._make_payload_record(
                component_id="__request_constraints__",
                stage=PromptStage.CONSTRAINTS,
                payload=request.constraint_payload,
            )
            records_by_stage[PromptStage.CONSTRAINTS].append(record)
            ordered_records.append(record)

        for index, payload in enumerate(request.compatibility_payloads):
            source = PromptSource(
                source_type=PromptSourceType.EXTERNAL_INJECTION,
                source_id=payload.get("source_id", f"compatibility_{index}"),
                owner_module=str(payload.get("owner_module", "")),
                module_path=str(payload.get("module_path", "")),
                metadata={k: v for k, v in payload.items() if k not in {"text"}},
            )
            record = PromptComponentRecord(
                component_id=f"__compatibility_{index}__",
                stage=PromptStage.COMPATIBILITY,
                kind=PromptComponentKind.EXTERNAL_INJECTION,
                priority=1000 + index,
                source=source,
                rendered_text=str(payload.get("text", "")).strip(),
                text_hash=stable_text_hash(str(payload.get("text", "")).strip()),
                cache_stable=False,
            )
            records_by_stage[PromptStage.COMPATIBILITY].append(record)
            ordered_records.append(record)

    def _make_payload_record(
        self,
        *,
        component_id: str,
        stage: PromptStage,
        payload: str | dict[str, Any],
    ) -> PromptComponentRecord:
        if isinstance(payload, dict):
            text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        else:
            text = payload
        return PromptComponentRecord(
            component_id=component_id,
            stage=stage,
            kind=PromptComponentKind.EXTERNAL_INJECTION,
            priority=10_000,
            source=PromptSource(source_type=PromptSourceType.UNKNOWN_SOURCE),
            rendered_text=text.strip(),
            text_hash=stable_text_hash(text.strip()),
            cache_stable=False,
        )

    def _infer_source(self, component: PromptComponent) -> PromptSource:
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
            resolver_name=component.resolver_ref if component.kind == PromptComponentKind.RESOLVER else "",
            is_builtin=source_type == PromptSourceType.BUILTIN_SYSTEM,
            metadata=dict(metadata),
        )

    def _build_signature(self, stages: list[PromptStageBlock]) -> str:
        payload = [
            {
                "stage": stage.stage.value,
                "components": [
                    {
                        "id": component.component_id,
                        "version": component.version,
                        "text_hash": component.text_hash,
                    }
                    for component in stage.components
                ],
            }
            for stage in stages
        ]
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _build_cache_key(self, prompt_signature: str, request: PromptAssemblyRequest) -> str:
        payload = {
            "prompt_signature": prompt_signature,
            "route_id": request.route_id,
            "model_id": request.model_id,
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _dedupe(self, items: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            if item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result
