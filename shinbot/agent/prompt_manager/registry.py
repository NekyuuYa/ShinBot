"""Prompt registry and structured assembly service (Chat Completions format)."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shinbot.agent.identity import (
    resolve_identity_map_prompt,
)
from shinbot.agent.prompt_manager.rendering import (
    expand_component_tree,
    infer_component_source,
    render_component_text,
)
from shinbot.agent.prompt_manager.schema import (
    PROMPT_STAGE_ORDER,
    PromptAssemblyRequest,
    PromptAssemblyResult,
    PromptComponent,
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
from shinbot.agent.prompt_manager.snapshots import (
    build_prompt_log_record,
    build_prompt_signature,
    create_prompt_snapshot,
)
from shinbot.agent.runtime import resolve_current_time_prompt, resolve_message_text_prompt

if TYPE_CHECKING:
    from shinbot.agent.context import ContextManager
    from shinbot.agent.identity import IdentityStore

Resolver = Callable[[PromptAssemblyRequest, PromptComponent, PromptSource], Any]


class PromptRegistry:
    """In-memory prompt registry with deterministic assembly.

    Produces structured ``messages`` + ``tools`` lists in Chat Completions
    API format instead of a flat prompt string.
    """

    BUILTIN_IDENTITY_MAP_PROMPT_COMPONENT_ID = "builtin.instructions.identity_map"
    BUILTIN_IDENTITY_CONSTRAINTS_COMPONENT_ID = "builtin.constraints.identity_behavior"
    BUILTIN_IDENTITY_MAP_PROMPT_RESOLVER = "builtin.identity.map"
    BUILTIN_MESSAGE_TEXT_PROMPT_COMPONENT_ID = "builtin.instructions.message_text"
    BUILTIN_MESSAGE_TEXT_PROMPT_RESOLVER = "builtin.runtime.message_text"
    BUILTIN_CURRENT_TIME_PROMPT_COMPONENT_ID = "builtin.constraints.current_time"
    BUILTIN_CURRENT_TIME_PROMPT_RESOLVER = "builtin.runtime.current_time"

    def __init__(
        self,
        *,
        context_manager: ContextManager | None = None,
        identity_store: IdentityStore | None = None,
    ) -> None:
        self._components: dict[str, PromptComponent] = {}
        self._profiles: dict[str, PromptProfile] = {}
        self._resolvers: dict[str, Resolver] = {}
        self._context_manager = context_manager
        self._identity_store = identity_store

    # ── Registration ────────────────────────────────────────────────────

    def register_component(self, component: PromptComponent) -> None:
        if component.id in self._components:
            raise ValueError(f"Prompt component {component.id!r} is already registered")
        self._components[component.id] = component

    def upsert_component(self, component: PromptComponent) -> None:
        self._components[component.id] = component

    def register_profile(self, profile: PromptProfile) -> None:
        if profile.id in self._profiles:
            raise ValueError(f"Prompt profile {profile.id!r} is already registered")
        self._profiles[profile.id] = profile

    def register_resolver(self, name: str, fn: Resolver) -> None:
        if name in self._resolvers:
            raise ValueError(f"Prompt resolver {name!r} is already registered")
        self._resolvers[name] = fn

    def attach_context_manager(self, context_manager: ContextManager | None) -> None:
        self._context_manager = context_manager

    # ── Lookup / list ───────────────────────────────────────────────────

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

    def list_component_catalog(self) -> list[dict[str, Any]]:
        catalog: list[dict[str, Any]] = []
        for component in self.list_components():
            source = infer_component_source(component)
            catalog.append(
                {
                    "id": component.id,
                    "display_name": str(
                        component.metadata.get("display_name")
                        or component.metadata.get("title")
                        or component.id
                    ),
                    "description": str(component.metadata.get("description", "")),
                    "stage": component.stage.value,
                    "type": component.kind.value,
                    "version": component.version,
                    "priority": component.priority,
                    "enabled": component.enabled,
                    "resolver_ref": component.resolver_ref,
                    "template_vars": list(component.template_vars),
                    "bundle_refs": list(component.bundle_refs),
                    "tags": list(component.tags),
                    "source_type": source.source_type.value,
                    "source_id": source.source_id,
                    "owner_plugin_id": source.owner_plugin_id,
                    "owner_module": source.owner_module,
                    "module_path": source.module_path,
                    "metadata": dict(component.metadata),
                }
            )
        return catalog

    # ── Assembly ────────────────────────────────────────────────────────

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
            expand_component_tree(
                component=component,
                request=request,
                components=self._components,
                resolvers=self._resolvers,
                records_by_stage=records_by_stage,
                ordered_records=ordered_records,
            )

        if request.identity_enabled:
            self._inject_identity_prompts(request, records_by_stage)

        # ── Sort records within each stage ──────────────────────────────
        sorted_records_by_stage: dict[PromptStage, list[PromptComponentRecord]] = {
            stage: sorted(
                records_by_stage[stage],
                key=lambda item: (item.priority, item.component_id, item.version),
            )
            for stage in PROMPT_STAGE_ORDER
        }

        # ── Build PromptStageBlock per stage ────────────────────────────
        stage_blocks: list[PromptStageBlock] = []
        has_unknown_source = False

        for stage in PROMPT_STAGE_ORDER:
            records = sorted_records_by_stage[stage]

            if any(r.source.source_type == PromptSourceType.UNKNOWN_SOURCE for r in records):
                has_unknown_source = True

            if stage == PromptStage.ABILITIES:
                tools_for_stage = [
                    tool for r in records if r.rendered_data for tool in r.rendered_data
                ]
                stage_blocks.append(
                    PromptStageBlock(
                        stage=stage,
                        components=records,
                        tools=tools_for_stage,
                    )
                )
            elif stage == PromptStage.CONTEXT:
                msgs_for_stage = [
                    msg for r in records if r.rendered_messages for msg in r.rendered_messages
                ]
                stage_blocks.append(
                    PromptStageBlock(
                        stage=stage,
                        components=records,
                        messages=msgs_for_stage,
                    )
                )
            else:
                rendered_text = "\n\n".join(r.rendered_text for r in records if r.rendered_text)
                token_estimate = len(rendered_text.split()) if rendered_text else 0
                stage_blocks.append(
                    PromptStageBlock(
                        stage=stage,
                        components=records,
                        rendered_text=rendered_text,
                        token_estimate=token_estimate,
                    )
                )

        stage_by_name = {block.stage: block for block in stage_blocks}

        # Guard: require at least one system_base component
        has_system_stage = bool(sorted_records_by_stage[PromptStage.SYSTEM_BASE]) or bool(
            sorted_records_by_stage[PromptStage.IDENTITY]
        )
        if not has_system_stage:
            raise ValueError(
                "Prompt assembly requires at least one component in SYSTEM_BASE or IDENTITY stage"
            )

        # ── Compose Chat Completions structure ──────────────────────────

        # System message: SYSTEM_BASE + IDENTITY (content array)
        system_content: list[dict[str, Any]] = []
        for stage_key in (PromptStage.SYSTEM_BASE, PromptStage.IDENTITY):
            block = stage_by_name[stage_key]
            for record in block.components:
                if record.rendered_text:
                    system_content.append({"type": "text", "text": record.rendered_text})
        system_message: dict[str, Any] = {"role": "system", "content": system_content}

        # Tools: ABILITIES
        tools = list(stage_by_name[PromptStage.ABILITIES].tools)

        # Context messages: CONTEXT
        context_stage = stage_by_name[PromptStage.CONTEXT]
        context_messages = list(context_stage.messages)

        # Final user message: COMPATIBILITY → INSTRUCTIONS → CONSTRAINTS
        final_content: list[dict[str, Any]] = []
        for stage_key in (
            PromptStage.COMPATIBILITY,
            PromptStage.INSTRUCTIONS,
            PromptStage.CONSTRAINTS,
        ):
            block = stage_by_name[stage_key]
            for record in block.components:
                if record.rendered_content_blocks:
                    final_content.extend(record.rendered_content_blocks)
                    continue
                if record.rendered_text:
                    final_content.append({"type": "text", "text": record.rendered_text})

        messages: list[dict[str, Any]] = [system_message, *context_messages]
        if final_content:
            messages.append({"role": "user", "content": final_content})

        prompt_signature = self._build_signature(stage_blocks)

        return PromptAssemblyResult(
            profile_id=request.profile_id,
            caller=request.caller,
            stages=stage_blocks,
            ordered_components=ordered_records,
            messages=messages,
            tools=tools,
            prompt_signature=prompt_signature,
            compatibility_used=bool(records_by_stage[PromptStage.COMPATIBILITY]),
            has_unknown_source=has_unknown_source,
            truncation={},
            metadata=dict(request.metadata),
        )

    # ── Snapshot / logging ──────────────────────────────────────────────

    def create_snapshot(
        self, result: PromptAssemblyResult, request: PromptAssemblyRequest
    ) -> PromptSnapshot:
        return create_prompt_snapshot(result, request)

    def build_log_record(
        self, result: PromptAssemblyResult, request: PromptAssemblyRequest
    ) -> PromptLoggerRecord:
        return build_prompt_log_record(result, request)

    def _inject_identity_prompts(
        self,
        request: PromptAssemblyRequest,
        records_by_stage: dict[PromptStage, list[PromptComponentRecord]],
        ordered_records: list[PromptComponentRecord],
    ) -> None:
        dynamic_component = self._components.get(self.BUILTIN_IDENTITY_MAP_PROMPT_COMPONENT_ID)
        static_component = self._components.get(self.BUILTIN_IDENTITY_CONSTRAINTS_COMPONENT_ID)
        if dynamic_component is None or static_component is None:
            return
        if not dynamic_component.enabled:
            return
        has_dynamic_record = any(
            record.component_id == dynamic_component.id and bool(record.rendered_text.strip())
            for record in ordered_records
        )
        has_static_record = any(
            record.component_id == static_component.id and bool(record.rendered_text.strip())
            for record in ordered_records
        )

        hydrated_request = request
        if self._context_manager is not None and request.session_id:
            context_inputs = self._context_manager.get_context_inputs(
                request.session_id,
                fallback=request.context_inputs,
            )
            hydrated_request = request.model_copy(update={"context_inputs": context_inputs})
        source = infer_component_source(dynamic_component)

        resolver = self._resolvers.get(dynamic_component.resolver_ref)
        if resolver is None:
            raise ValueError(
                f"Prompt resolver {dynamic_component.resolver_ref!r} is not registered"
            )

        resolver_output = resolver(hydrated_request, dynamic_component, source)
        if isinstance(resolver_output, dict):
            dynamic_text = str(resolver_output.get("text", "")).strip()
            dynamic_metadata = {
                key: value for key, value in resolver_output.items() if key != "text"
            }
        else:
            dynamic_text = str(resolver_output).strip()
            dynamic_metadata = {}

        if not dynamic_text:
            return

        if not has_dynamic_record:
            dynamic_record = PromptComponentRecord(
                component_id=dynamic_component.id,
                stage=dynamic_component.stage,
                kind=dynamic_component.kind,
                version=dynamic_component.version,
                priority=dynamic_component.priority,
                source=source,
                rendered_text=dynamic_text,
                text_hash=stable_text_hash(dynamic_text),
                metadata={**dict(dynamic_component.metadata), **dynamic_metadata},
            )
            records_by_stage[PromptStage.INSTRUCTIONS].append(dynamic_record)
            ordered_records.append(dynamic_record)

        if not static_component.enabled:
            return
        if has_static_record:
            return

        static_source = infer_component_source(static_component)
        static_text = render_component_text(
            component=static_component,
            request=hydrated_request,
            source=static_source,
            resolvers=self._resolvers,
        )
        if not static_text:
            return
        static_record = PromptComponentRecord(
            component_id=static_component.id,
            stage=static_component.stage,
            kind=static_component.kind,
            version=static_component.version,
            priority=static_component.priority,
            source=static_source,
            rendered_text=static_text,
            text_hash=stable_text_hash(static_text),
            metadata=dict(static_component.metadata),
        )
        records_by_stage[PromptStage.CONSTRAINTS].append(static_record)
        ordered_records.append(static_record)

    def _inject_runtime_prompts(
        self,
        request: PromptAssemblyRequest,
        records_by_stage: dict[PromptStage, list[PromptComponentRecord]],
        ordered_records: list[PromptComponentRecord],
    ) -> None:
        for component_id in (
            self.BUILTIN_MESSAGE_TEXT_PROMPT_COMPONENT_ID,
            self.BUILTIN_CURRENT_TIME_PROMPT_COMPONENT_ID,
        ):
            component = self._components.get(component_id)
            if component is None or not component.enabled:
                continue

            has_record = any(
                record.component_id == component.id and bool(record.rendered_text.strip())
                for record in ordered_records
            )
            if has_record:
                continue

            source = infer_component_source(component)
            resolver = self._resolvers.get(component.resolver_ref)
            if resolver is None:
                raise ValueError(f"Prompt resolver {component.resolver_ref!r} is not registered")

            resolver_output = resolver(request, component, source)
            if isinstance(resolver_output, dict):
                rendered_text = str(resolver_output.get("text", "")).strip()
                rendered_content_blocks = _normalize_content_blocks(
                    resolver_output.get("content_blocks")
                )
                rendered_metadata = {
                    key: value
                    for key, value in resolver_output.items()
                    if key not in ("text", "content_blocks")
                }
            else:
                rendered_text = str(resolver_output).strip()
                rendered_content_blocks = None
                rendered_metadata = {}

            if not rendered_text:
                continue

            record = PromptComponentRecord(
                component_id=component.id,
                stage=component.stage,
                kind=component.kind,
                version=component.version,
                priority=component.priority,
                source=source,
                rendered_text=rendered_text,
                rendered_content_blocks=rendered_content_blocks,
                text_hash=stable_text_hash(rendered_text),
                metadata={**dict(component.metadata), **rendered_metadata},
            )
            records_by_stage[component.stage].append(record)
            ordered_records.append(record)

    # ── Internal: signature ──────────────────────────────────────────────

    def _build_signature(self, stages: list[PromptStageBlock]) -> str:
        return build_prompt_signature(stages)

    def _dedupe(self, items: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            if item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result

    def resolve_builtin_identity_map_prompt(
        self,
        request: PromptAssemblyRequest,
        component: PromptComponent,
        source: PromptSource,
    ) -> dict[str, Any]:
        return resolve_identity_map_prompt(
            identity_store=self._identity_store,
            request=request,
            _component=component,
            _source=source,
        )

    def resolve_builtin_current_time_prompt(
        self,
        request: PromptAssemblyRequest,
        component: PromptComponent,
        source: PromptSource,
    ) -> dict[str, Any]:
        return resolve_current_time_prompt(
            request=request,
            _component=component,
            _source=source,
        )

    def resolve_builtin_message_text_prompt(
        self,
        request: PromptAssemblyRequest,
        component: PromptComponent,
        source: PromptSource,
    ) -> dict[str, Any]:
        return resolve_message_text_prompt(
            request=request,
            _component=component,
            _source=source,
        )


def _normalize_content_blocks(value: Any) -> list[dict[str, Any]] | None:
    from shinbot.agent.prompt_manager.rendering import _extract_content_blocks

    return _extract_content_blocks(value)
