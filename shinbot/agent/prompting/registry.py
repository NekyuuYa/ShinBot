"""Prompt registry and structured assembly service (Chat Completions format)."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shinbot.agent.prompting.schema import (
    PROMPT_STAGE_ORDER,
    ContextStrategy,
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

if TYPE_CHECKING:
    from shinbot.agent.context import ContextManager
    from shinbot.agent.identity import IdentityStore

Resolver = Callable[[PromptAssemblyRequest, PromptComponent, PromptSource], Any]
ContextStrategyResolver = Callable[[PromptAssemblyRequest, ContextStrategy], Any]


class PromptRegistry:
    """In-memory prompt registry with deterministic assembly.

    Produces structured ``messages`` + ``tools`` lists in Chat Completions
    API format instead of a flat prompt string.
    """

    BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID = "builtin.context.sliding_window"
    BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER = "builtin.context.sliding_window"
    BUILTIN_IDENTITY_MAP_PROMPT_COMPONENT_ID = "builtin.instructions.identity_map"
    BUILTIN_IDENTITY_CONSTRAINTS_COMPONENT_ID = "builtin.constraints.identity_behavior"
    BUILTIN_IDENTITY_MAP_PROMPT_RESOLVER = "builtin.identity.map"

    @classmethod
    def build_builtin_sliding_window_strategy(
        cls,
        *,
        trigger_ratio: float = 0.5,
        trim_turns: int = 2,
        trim_ratio: float | None = None,
    ) -> ContextStrategy:
        return ContextStrategy(
            id=cls.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID,
            display_name="Sliding Window",
            description="Built-in context strategy based on a sliding window.",
            resolver_ref=cls.BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER,
            priority=10_000,
            metadata={"builtin": True, "default": True},
            budget={
                "truncate_policy": "sliding_window",
                "trigger_ratio": trigger_ratio,
                "trim_ratio": trim_ratio,
                "trim_turns": trim_turns,
            },
        )

    def __init__(
        self,
        *,
        fallback_context_trigger_ratio: float = 0.5,
        fallback_context_trim_turns: int = 2,
        context_manager: ContextManager | None = None,
        identity_store: IdentityStore | None = None,
    ) -> None:
        self._components: dict[str, PromptComponent] = {}
        self._context_strategies: dict[str, ContextStrategy] = {}
        self._context_strategy_resolvers: dict[str, ContextStrategyResolver] = {}
        self._profiles: dict[str, PromptProfile] = {}
        self._resolvers: dict[str, Resolver] = {}
        self._context_manager = context_manager
        self._identity_store = identity_store
        self._register_builtin_context_strategies(
            trigger_ratio=fallback_context_trigger_ratio,
            trim_turns=fallback_context_trim_turns,
        )
        self._register_builtin_identity_prompts()

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

    def register_context_strategy(self, strategy: ContextStrategy) -> None:
        if strategy.id in self._context_strategies:
            raise ValueError(f"Context strategy {strategy.id!r} is already registered")
        self._context_strategies[strategy.id] = strategy

    def upsert_context_strategy(self, strategy: ContextStrategy) -> None:
        self._context_strategies[strategy.id] = strategy

    def register_context_strategy_resolver(self, name: str, fn: ContextStrategyResolver) -> None:
        if name in self._context_strategy_resolvers:
            raise ValueError(f"Context strategy resolver {name!r} is already registered")
        self._context_strategy_resolvers[name] = fn

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

    def get_context_strategy(self, strategy_id: str) -> ContextStrategy | None:
        return self._context_strategies.get(strategy_id)

    def list_components(self, stage: PromptStage | None = None) -> list[PromptComponent]:
        components = list(self._components.values())
        if stage is not None:
            components = [component for component in components if component.stage == stage]
        return sorted(components, key=lambda item: (item.priority, item.id, item.version))

    def list_context_strategies(self) -> list[ContextStrategy]:
        return sorted(
            self._context_strategies.values(),
            key=lambda item: (item.priority, item.id),
        )

    def list_profiles(self) -> list[PromptProfile]:
        return list(self._profiles.values())

    def list_component_catalog(self) -> list[dict[str, Any]]:
        catalog: list[dict[str, Any]] = []
        for component in self.list_components():
            source = self._infer_source(component)
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
                    "cache_stable": component.cache_stable,
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
            self._expand_component(component, request, records_by_stage, ordered_records)

        self._inject_context_strategy(request, records_by_stage, ordered_records)
        if request.identity_enabled:
            self._inject_identity_prompts(request, records_by_stage, ordered_records)
        self._inject_payloads(request, records_by_stage, ordered_records)

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
        context_messages = list(stage_by_name[PromptStage.CONTEXT].messages)

        # Final user message: COMPATIBILITY → INSTRUCTIONS → CONSTRAINTS
        final_content: list[dict[str, Any]] = []
        for stage_key in (
            PromptStage.COMPATIBILITY,
            PromptStage.INSTRUCTIONS,
            PromptStage.CONSTRAINTS,
        ):
            block = stage_by_name[stage_key]
            for record in block.components:
                if record.rendered_text:
                    final_content.append({"type": "text", "text": record.rendered_text})

        messages: list[dict[str, Any]] = [system_message, *context_messages]
        if final_content:
            messages.append({"role": "user", "content": final_content})

        prompt_signature = self._build_signature(stage_blocks)
        cache_key = self._build_cache_key(prompt_signature, request)

        return PromptAssemblyResult(
            profile_id=request.profile_id,
            caller=request.caller,
            stages=stage_blocks,
            ordered_components=ordered_records,
            messages=messages,
            tools=tools,
            prompt_signature=prompt_signature,
            cache_key=cache_key,
            compatibility_used=bool(records_by_stage[PromptStage.COMPATIBILITY]),
            has_unknown_source=has_unknown_source,
            truncation={},
            metadata=dict(request.metadata),
        )

    # ── Snapshot / logging ──────────────────────────────────────────────

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
            full_messages=result.messages,
            full_tools=result.tools,
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

    # ── Internal: component expansion ───────────────────────────────────

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

        # ABILITIES stage: produce structured tool definitions
        if component.stage == PromptStage.ABILITIES:
            tool_defs = self._render_component_structured(component, request, source)
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

        # Regular text rendering for all other stages
        rendered_text = self._render_component(component, request, source)

        # CONTEXT stage: wrap text as message pairs
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

        # Default path: text-based stages
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

    def _render_component_structured(
        self, component: PromptComponent, request: PromptAssemblyRequest, source: PromptSource
    ) -> list[dict[str, Any]]:
        """Render a component as structured data (tool definitions)."""
        if component.kind == PromptComponentKind.RESOLVER:
            resolver = self._resolvers.get(component.resolver_ref)
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

    # ── Internal: payload injection ─────────────────────────────────────

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

    # ── Internal: context strategy ──────────────────────────────────────

    def _inject_context_strategy(
        self,
        request: PromptAssemblyRequest,
        records_by_stage: dict[PromptStage, list[PromptComponentRecord]],
        ordered_records: list[PromptComponentRecord],
    ) -> None:
        strategy = self._resolve_context_strategy(request)
        if strategy is None:
            return
        if not strategy.enabled:
            return

        policy_sync = self._sync_context_policy(request, strategy)
        request = self._hydrate_request_context(request)

        resolver = self._context_strategy_resolvers.get(strategy.resolver_ref)
        if resolver is None:
            raise ValueError(
                f"Context strategy resolver {strategy.resolver_ref!r} is not registered"
            )
        result = resolver(request, strategy)

        # Extract structured messages from the resolver result
        if isinstance(result, dict):
            rendered_messages: list[dict[str, Any]] = result.get("messages", [])
            # Fallback: if resolver returns {"text": "..."} without "messages"
            if not rendered_messages:
                text = str(result.get("text", "")).strip()
                if text:
                    rendered_messages = [{"role": "user", "content": text}]
            resolver_metadata = {k: v for k, v in result.items() if k not in ("text", "messages")}
            if policy_sync and int(policy_sync.get("dropped_turns", 0)) > int(
                resolver_metadata.get("dropped_turns", 0)
            ):
                resolver_metadata["dropped_turns"] = int(policy_sync["dropped_turns"])
                resolver_metadata["remaining_turns"] = int(policy_sync["remaining_turns"])
                resolver_metadata["current_tokens"] = int(policy_sync["current_tokens"])
                if "trigger_tokens" in policy_sync:
                    resolver_metadata["trigger_tokens"] = int(policy_sync["trigger_tokens"])
                if "trim_mode" in policy_sync:
                    resolver_metadata["trim_mode"] = str(policy_sync["trim_mode"])
        elif isinstance(result, str):
            text = result.strip()
            rendered_messages = [{"role": "user", "content": text}] if text else []
            resolver_metadata = {}
        else:
            rendered_messages = []
            resolver_metadata = {}

        if not rendered_messages:
            return

        if request.identity_enabled:
            rendered_messages = self._inject_identity_layers_into_messages(rendered_messages)

        hash_input = json.dumps(rendered_messages, ensure_ascii=False, sort_keys=True)
        record = PromptComponentRecord(
            component_id=f"__context_strategy__:{strategy.id}",
            stage=PromptStage.CONTEXT,
            kind=PromptComponentKind.RESOLVER,
            version="1.0.0",
            priority=strategy.priority,
            source=PromptSource(
                source_type=PromptSourceType.CONTEXT_PLUGIN,
                source_id=strategy.id,
                resolver_name=strategy.resolver_ref,
                metadata={"strategy_metadata": dict(strategy.metadata)},
            ),
            rendered_messages=rendered_messages,
            text_hash=stable_text_hash(hash_input),
            cache_stable=False,
            metadata={
                "context_strategy_id": strategy.id,
                "budget": strategy.budget.model_dump(mode="json"),
                "resolver_output": resolver_metadata,
            },
        )
        records_by_stage[PromptStage.CONTEXT].append(record)
        ordered_records.append(record)

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

        hydrated_request = self._hydrate_request_context(request)
        source = self._infer_source(dynamic_component)

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
                cache_stable=dynamic_component.cache_stable,
                metadata={**dict(dynamic_component.metadata), **dynamic_metadata},
            )
            records_by_stage[PromptStage.INSTRUCTIONS].append(dynamic_record)
            ordered_records.append(dynamic_record)

        if not static_component.enabled:
            return
        if has_static_record:
            return

        static_source = self._infer_source(static_component)
        static_text = self._render_component(static_component, hydrated_request, static_source)
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
            cache_stable=static_component.cache_stable,
            metadata=dict(static_component.metadata),
        )
        records_by_stage[PromptStage.CONSTRAINTS].append(static_record)
        ordered_records.append(static_record)

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
            resolver_name=component.resolver_ref
            if component.kind == PromptComponentKind.RESOLVER
            else "",
            is_builtin=source_type == PromptSourceType.BUILTIN_SYSTEM,
            metadata=dict(metadata),
        )

    def _resolve_context_strategy(self, request: PromptAssemblyRequest) -> ContextStrategy | None:
        if request.context_strategy_id:
            strategy = self._context_strategies.get(request.context_strategy_id)
            if strategy is None:
                raise ValueError(
                    f"Context strategy {request.context_strategy_id!r} is not registered"
                )
            return strategy
        return self._context_strategies.get(self.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID)

    # ── Builtin context strategy ────────────────────────────────────────

    def _register_builtin_context_strategies(
        self,
        *,
        trigger_ratio: float,
        trim_turns: int,
    ) -> None:
        self.register_context_strategy(
            self.build_builtin_sliding_window_strategy(
                trigger_ratio=trigger_ratio,
                trim_turns=trim_turns,
            )
        )
        self.register_context_strategy_resolver(
            self.BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER,
            self._resolve_builtin_sliding_window_context,
        )

    def _hydrate_request_context(self, request: PromptAssemblyRequest) -> PromptAssemblyRequest:
        if self._context_manager is None or not request.session_id:
            return request
        context_inputs = self._context_manager.get_context_inputs(
            request.session_id,
            fallback=request.context_inputs,
        )
        return request.model_copy(update={"context_inputs": context_inputs})

    def _sync_context_policy(
        self,
        request: PromptAssemblyRequest,
        strategy: ContextStrategy,
    ) -> dict[str, Any]:
        if self._context_manager is None or not request.session_id:
            return {}
        return self._context_manager.set_session_policy(
            request.session_id,
            strategy=strategy,
            model_context_window=request.model_context_window,
        )

    def _resolve_builtin_sliding_window_context(
        self,
        request: PromptAssemblyRequest,
        strategy: ContextStrategy,
    ) -> dict[str, Any]:
        turns = self._normalize_history_turns(request.context_inputs)
        summary = str(request.context_inputs.get("summary", "")).strip()
        model_context_window = request.model_context_window or strategy.budget.max_context_tokens
        trigger_ratio = strategy.budget.trigger_ratio
        trim_ratio = strategy.budget.trim_ratio
        trim_turns = strategy.budget.trim_turns
        dropped_turns = 0

        if (
            strategy.budget.max_history_turns is not None
            and len(turns) > strategy.budget.max_history_turns
        ):
            overflow = len(turns) - strategy.budget.max_history_turns
            turns = turns[overflow:]
            dropped_turns += overflow

        trigger_tokens = (
            max(1, math.floor(model_context_window * trigger_ratio))
            if model_context_window is not None
            else None
        )

        if self._context_manager is not None and request.session_id:
            ejection = self._context_manager.apply_batch_ejection(
                request.session_id,
                strategy=strategy,
                model_context_window=request.model_context_window,
            )
            dropped_turns = int(ejection.get("dropped_turns", 0))
            turns = self._normalize_history_turns(
                self._context_manager.get_context_inputs(
                    request.session_id,
                    fallback={"summary": summary},
                )
            )
        else:
            while trigger_tokens is not None and len(turns) > 1:
                current_tokens = self._estimate_context_tokens(turns, summary)
                if current_tokens < trigger_tokens:
                    break
                trim_count = (
                    max(1, math.floor(len(turns) * trim_ratio))
                    if trim_ratio is not None
                    else max(1, trim_turns)
                )
                trim_count = min(trim_count, len(turns) - 1)
                turns = turns[trim_count:]
                dropped_turns += trim_count

        # Build structured message pairs
        messages: list[dict[str, Any]] = []
        if summary:
            messages.append({"role": "user", "content": f"[Summary]\n{summary}"})
        for turn in turns:
            role = turn.get("role", "user") or "user"
            message: dict[str, Any] = {
                "role": role,
                "content": turn["content"],
            }
            sender_id = str(turn.get("sender_id", "") or "").strip()
            if sender_id:
                message["sender_id"] = sender_id
            messages.append(message)

        return {
            "messages": messages,
            "dropped_turns": dropped_turns,
            "trigger_tokens": trigger_tokens,
            "remaining_turns": len(turns),
        }

    def _normalize_history_turns(self, context_inputs: dict[str, Any]) -> list[dict[str, Any]]:
        raw_turns = context_inputs.get("history_turns", [])
        if not isinstance(raw_turns, list):
            return []

        turns: list[dict[str, Any]] = []
        for item in raw_turns:
            if isinstance(item, str):
                content = item.strip()
                if content:
                    turns.append({"role": "", "content": content})
                continue
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            raw_content = item.get("content", "")
            if isinstance(raw_content, list):
                content_parts: list[str] = []
                for block in raw_content:
                    if isinstance(block, dict):
                        block_text = block.get("text")
                        if isinstance(block_text, str) and block_text.strip():
                            content_parts.append(block_text.strip())
                content = "\n".join(content_parts).strip()
            else:
                content = str(raw_content).strip()
            if not content:
                continue
            turn: dict[str, Any] = {"role": role, "content": content}
            sender_id = str(
                item.get("sender_id", item.get("senderId", item.get("name", ""))) or ""
            ).strip()
            if sender_id:
                turn["sender_id"] = sender_id
            sender_name = str(item.get("sender_name", item.get("senderName", "")) or "").strip()
            if sender_name:
                turn["sender_name"] = sender_name
            platform = str(item.get("platform", "") or "").strip()
            if platform:
                turn["platform"] = platform
            turns.append(turn)
        return turns

    def _estimate_context_tokens(self, turns: list[dict[str, Any]], summary: str) -> int:
        text_parts = [summary] if summary else []
        text_parts.extend(
            f"{turn['role']}: {turn['content']}" if turn["role"] else turn["content"]
            for turn in turns
        )
        text = "\n".join(part for part in text_parts if part).strip()
        if not text:
            return 0
        word_estimate = len(text.split())
        char_estimate = math.ceil(len(text) / 4)
        return max(word_estimate, char_estimate)

    # ── Internal: signature / cache key ─────────────────────────────────

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

    def _register_builtin_identity_prompts(self) -> None:
        self.register_component(
            PromptComponent(
                id=self.BUILTIN_IDENTITY_MAP_PROMPT_COMPONENT_ID,
                stage=PromptStage.INSTRUCTIONS,
                kind=PromptComponentKind.RESOLVER,
                resolver_ref=self.BUILTIN_IDENTITY_MAP_PROMPT_RESOLVER,
                priority=9000,
                enabled=True,
                metadata={
                    "builtin": True,
                    "display_name": "Identity Map (Dynamic)",
                    "description": "Inject active participant identity mapping for current context.",
                },
            )
        )
        self.register_component(
            PromptComponent(
                id=self.BUILTIN_IDENTITY_CONSTRAINTS_COMPONENT_ID,
                stage=PromptStage.CONSTRAINTS,
                kind=PromptComponentKind.STATIC_TEXT,
                content=(
                    "### 行为约束\n"
                    "- 严禁在输出中包含任何 【ID】 格式的字符串或原始数字 ID。\n"
                    "- 称呼他人时，必须使用上述参考表中的“昵称”或“别名”。\n"
                    "- 若用户 ID 未出现在上表中，请用类似于“那个人”的称呼。"
                ),
                priority=9000,
                enabled=True,
                metadata={
                    "builtin": True,
                    "display_name": "Identity Behavior Constraints",
                    "description": "Static constraints for identity-safe assistant replies.",
                },
            )
        )
        self.register_resolver(
            self.BUILTIN_IDENTITY_MAP_PROMPT_RESOLVER,
            self._resolve_builtin_identity_map_prompt,
        )

    def _resolve_builtin_identity_map_prompt(
        self,
        request: PromptAssemblyRequest,
        _component: PromptComponent,
        _source: PromptSource,
    ) -> dict[str, Any]:
        turns = self._normalize_history_turns(request.context_inputs)
        active_participants: dict[str, dict[str, str]] = {}
        for turn in turns:
            if str(turn.get("role", "")).strip() != "user":
                continue
            sender_id = str(turn.get("sender_id", "") or "").strip()
            if not sender_id:
                continue
            participant = active_participants.setdefault(
                sender_id, {"sender_name": "", "platform": ""}
            )
            sender_name = str(turn.get("sender_name", "") or "").strip()
            if sender_name and not participant["sender_name"]:
                participant["sender_name"] = sender_name
            platform = str(turn.get("platform", "") or "").strip()
            if platform and not participant["platform"]:
                participant["platform"] = platform

        if not active_participants:
            return {"text": "", "active_user_ids": [], "mapped_user_ids": []}

        context_platform = str(request.context_inputs.get("platform", "") or "").strip()
        mapped_lines: list[str] = []
        mapped_ids: list[str] = []

        for user_id, participant in active_participants.items():
            lookup_platform = context_platform or participant["platform"]
            identity = None
            if self._identity_store is not None:
                identity = self._identity_store.get_identity(user_id, platform=lookup_platform)
                if identity is None:
                    self._identity_store.ensure_user(
                        user_id=user_id,
                        suggested_name=participant["sender_name"],
                        platform=lookup_platform,
                    )
                    identity = self._identity_store.get_identity(user_id, platform=lookup_platform)

            if identity is None:
                continue

            display_name = str(identity.get("name", "")).strip()
            if not display_name:
                continue

            aliases = identity.get("aname", [])
            if isinstance(aliases, str):
                alias_list = [aliases.strip()] if aliases.strip() else []
            elif isinstance(aliases, list):
                alias_list = [str(alias).strip() for alias in aliases if str(alias).strip()]
            else:
                alias_list = []

            note = str(identity.get("note", "")).strip()
            line = f"- ID: {user_id} -> 昵称: {display_name}"
            if alias_list:
                line += f" 别名: {'/'.join(alias_list)}"
            if note:
                line += f" (备注: {note})"
            mapped_lines.append(line)
            mapped_ids.append(user_id)

        if not mapped_lines:
            return {
                "text": "",
                "active_user_ids": list(active_participants.keys()),
                "mapped_user_ids": [],
            }

        text_lines = [
            "### 参与者身份参考 (Identity Map)",
            "以下是当前对话参与者的 ID 与你应当称呼他们的“昵称”映射：",
            *mapped_lines,
        ]
        return {
            "text": "\n".join(text_lines).strip(),
            "active_user_ids": list(active_participants.keys()),
            "mapped_user_ids": mapped_ids,
        }

    def _inject_identity_layers_into_messages(
        self,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        for message in messages:
            if not isinstance(message, dict):
                continue

            payload = dict(message)
            role = str(payload.get("role", "")).strip()
            sender_id = str(payload.get("sender_id", "")).strip()

            if role == "user" and sender_id:
                payload["name"] = self._format_sender_name(sender_id)
                content = payload.get("content")
                if isinstance(content, str):
                    payload["content"] = self._prefix_sender_id_inline(content, sender_id)

            payload.pop("sender_id", None)
            payload.pop("sender_name", None)
            payload.pop("platform", None)
            enriched.append(payload)

        return enriched

    def _prefix_sender_id_inline(self, content: str, sender_id: str) -> str:
        body = content.strip()
        if not body:
            return body
        marker = f"【{sender_id}】"
        if body.startswith(marker):
            return body
        return f"{marker}{body}"

    def _format_sender_name(self, sender_id: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_-]", "_", sender_id.strip())
        normalized = normalized.strip("_")
        if not normalized:
            normalized = stable_text_hash(sender_id)[:12]
        if normalized[0].isdigit():
            normalized = f"u_{normalized}"
        return normalized[:64]
