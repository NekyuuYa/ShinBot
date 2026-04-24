"""Prompt registry and structured assembly service (Chat Completions format)."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.projection import PromptMemoryProjectionRequest
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
from shinbot.agent.prompt_manager.snapshots import (
    build_prompt_log_record,
    build_prompt_signature,
    create_prompt_snapshot,
)
from shinbot.agent.runtime import resolve_current_time_prompt, resolve_message_text_prompt
from shinbot.schema.context_strategies import (
    BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER as _BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER,
)
from shinbot.schema.context_strategies import (
    BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID as _BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID,
)

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
    BUILTIN_CONTEXT_PACKED_HISTORY_COMPONENT_ID = "builtin.context.packed_history"
    BUILTIN_INSTRUCTION_UNREAD_COMPONENT_ID = "builtin.instructions.unread_messages"
    BUILTIN_CONSTRAINT_ACTIVE_ALIAS_COMPONENT_ID = "builtin.constraints.active_aliases"
    BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID = _BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID
    BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER = _BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER
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

        self._inject_context_management_records(request, records_by_stage, ordered_records)
        if request.identity_enabled:
            self._inject_identity_prompts(request, records_by_stage, ordered_records)

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

    def _inject_context_management_records(
        self,
        request: PromptAssemblyRequest,
        records_by_stage: dict[PromptStage, list[PromptComponentRecord]],
        ordered_records: list[PromptComponentRecord],
    ) -> None:
        if self._context_manager is None or not request.session_id:
            return

        context_inputs = dict(request.context_inputs)
        self_platform_id = str(request.template_inputs.get("user_id", "") or "").strip()
        if not self_platform_id:
            self_platform_id = str(context_inputs.get("self_user_id", "") or "").strip()
        now_ms = _resolve_now_ms(request.metadata.get("now_ms"))
        unread_records = context_inputs.get("unread_records")
        bundle = self._context_manager.build_prompt_memory_bundle(
            PromptMemoryProjectionRequest(
                session_id=request.session_id,
                unread_records=unread_records if isinstance(unread_records, list) else [],
                previous_summary=str(context_inputs.get("previous_summary", "") or ""),
                self_platform_id=self_platform_id,
                now_ms=now_ms,
            )
        )
        context_messages = list(bundle.context_messages)
        if context_messages and _is_explicit_prompt_cache_enabled(request.metadata):
            context_messages = _apply_explicit_prompt_cache_marker(
                context_messages,
                bundle.cacheable_message_count,
            )
        if context_messages:
            hash_input = json.dumps(context_messages, ensure_ascii=False, sort_keys=True)
            context_record = PromptComponentRecord(
                component_id=self.BUILTIN_CONTEXT_PACKED_HISTORY_COMPONENT_ID,
                stage=PromptStage.CONTEXT,
                kind=PromptComponentKind.EXTERNAL_INJECTION,
                version="1.0.0",
                priority=10,
                source=PromptSource(
                    source_type=PromptSourceType.BUILTIN_SYSTEM,
                    source_id=self.BUILTIN_CONTEXT_PACKED_HISTORY_COMPONENT_ID,
                    is_builtin=True,
                ),
                rendered_messages=context_messages,
                text_hash=stable_text_hash(hash_input),
                metadata={"session_id": request.session_id},
            )
            records_by_stage[PromptStage.CONTEXT].append(context_record)
            ordered_records.append(context_record)
        if bundle.instruction_blocks:
            hash_input = json.dumps(bundle.instruction_blocks, ensure_ascii=False, sort_keys=True)
            instruction_record = PromptComponentRecord(
                component_id=self.BUILTIN_INSTRUCTION_UNREAD_COMPONENT_ID,
                stage=PromptStage.INSTRUCTIONS,
                kind=PromptComponentKind.EXTERNAL_INJECTION,
                version="1.0.0",
                priority=10,
                source=PromptSource(
                    source_type=PromptSourceType.BUILTIN_SYSTEM,
                    source_id=self.BUILTIN_INSTRUCTION_UNREAD_COMPONENT_ID,
                    is_builtin=True,
                ),
                rendered_text="[builtin unread messages]",
                rendered_content_blocks=bundle.instruction_blocks,
                text_hash=stable_text_hash(hash_input),
                metadata={
                    "session_id": request.session_id,
                    "message_count": int(bundle.metadata.get("message_count", 0) or 0),
                },
            )
            records_by_stage[PromptStage.INSTRUCTIONS].append(instruction_record)
            ordered_records.append(instruction_record)

        active_alias_text = bundle.constraint_text.strip()
        if active_alias_text:
            constraint_record = PromptComponentRecord(
                component_id=self.BUILTIN_CONSTRAINT_ACTIVE_ALIAS_COMPONENT_ID,
                stage=PromptStage.CONSTRAINTS,
                kind=PromptComponentKind.EXTERNAL_INJECTION,
                version="1.0.0",
                priority=10,
                source=PromptSource(
                    source_type=PromptSourceType.BUILTIN_SYSTEM,
                    source_id=self.BUILTIN_CONSTRAINT_ACTIVE_ALIAS_COMPONENT_ID,
                    is_builtin=True,
                ),
                rendered_text=active_alias_text,
                text_hash=stable_text_hash(active_alias_text),
                metadata={"session_id": request.session_id},
            )
            records_by_stage[PromptStage.CONSTRAINTS].append(constraint_record)
            ordered_records.append(constraint_record)

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
        from shinbot.agent.identity.prompt_runtime import resolve_identity_map_prompt

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


def _resolve_now_ms(value: Any) -> int | None:
    if value is None:
        return None
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    return int(raw if raw > 10_000_000_000 else raw * 1000)


def _is_explicit_prompt_cache_enabled(metadata: dict[str, Any]) -> bool:
    value = metadata.get("explicit_prompt_cache_enabled")
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0

    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return False


def _apply_explicit_prompt_cache_marker(
    messages: list[dict[str, Any]],
    cacheable_message_count: int,
) -> list[dict[str, Any]]:
    if not messages or cacheable_message_count <= 0:
        return messages

    target_index = min(cacheable_message_count, len(messages)) - 1
    target_message = dict(messages[target_index])
    content = target_message.get("content")

    if isinstance(content, list):
        for block_index in range(len(content) - 1, -1, -1):
            block = content[block_index]
            if not isinstance(block, dict):
                continue
            if str(block.get("type") or "") != "text":
                continue

            updated_block = dict(block)
            updated_block["cache_control"] = {"type": "ephemeral"}
            updated_content = list(content)
            updated_content[block_index] = updated_block
            target_message["content"] = updated_content
            updated_messages = list(messages)
            updated_messages[target_index] = target_message
            return updated_messages
        return messages

    if isinstance(content, str) and content.strip():
        target_message["cache_control"] = {"type": "ephemeral"}
        updated_messages = list(messages)
        updated_messages[target_index] = target_message
        return updated_messages

    return messages
