"""Prompt registry and structured assembly service (Chat Completions format)."""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shinbot.agent.identity import (
    inject_identity_layers_into_messages,
    resolve_identity_map_prompt,
)
from shinbot.agent.prompt_manager.context_strategies import (
    hydrate_request_context,
    resolve_builtin_sliding_window_context,
    sync_context_policy,
)
from shinbot.agent.prompt_manager.rendering import (
    expand_component_tree,
    infer_component_source,
    render_component_text,
)
from shinbot.agent.prompt_manager.schema import (
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
from shinbot.agent.prompt_manager.snapshots import (
    build_prompt_cache_key,
    build_prompt_log_record,
    build_prompt_signature,
    create_prompt_snapshot,
)
from shinbot.agent.runtime import resolve_current_time_prompt, resolve_message_text_prompt

if TYPE_CHECKING:
    from shinbot.agent.context import ContextManager
    from shinbot.agent.identity import IdentityStore

Resolver = Callable[[PromptAssemblyRequest, PromptComponent, PromptSource], Any]
ContextStrategyResolver = Callable[[PromptAssemblyRequest, ContextStrategy], Any]


# DashScope explicit cache matches backwards within up to 20 content blocks.
# Keep a safety margin to avoid running exactly on the provider boundary.
DEFAULT_CONTEXT_CACHE_BLOCK_GAP = 18
DEFAULT_EXPLICIT_CACHE_MIN_TOKENS = 1024
DEFAULT_CONTEXT_TRIGGER_TOKENS = 15_000
DEFAULT_CONTEXT_TARGET_TOKENS = 6_000


def _is_cjk_character(character: str) -> bool:
    return (
        "\u3400" <= character <= "\u4dbf"  # CJK Unified Ideographs Extension A
        or "\u4e00" <= character <= "\u9fff"  # CJK Unified Ideographs
        or "\uf900" <= character <= "\ufaff"  # CJK Compatibility Ideographs
        or "\u3040" <= character <= "\u30ff"  # Hiragana + Katakana
        or "\uac00" <= character <= "\ud7af"  # Hangul Syllables
    )


def _estimate_text_tokens(text: str) -> int:
    """Estimate token count with conservative heuristics.

    The estimate keeps the existing Latin-friendly heuristic and adds a
    CJK-aware branch so dense CJK text is not heavily undercounted.
    """

    text = text.strip()
    if not text:
        return 0

    word_estimate = len(text.split())
    char_estimate = math.ceil(len(text) / 4)
    cjk_char_count = sum(1 for character in text if _is_cjk_character(character))
    non_cjk_char_count = len(text) - cjk_char_count
    cjk_aware_char_estimate = cjk_char_count + math.ceil(non_cjk_char_count / 4)

    return max(word_estimate, char_estimate, cjk_aware_char_estimate)


def _estimate_content_blocks_tokens(content_blocks: list[dict[str, Any]], block_count: int) -> int:
    if block_count <= 0:
        return 0

    text_parts: list[str] = []
    for block in content_blocks[:block_count]:
        if not isinstance(block, dict):
            continue
        text = str(block.get("text") or "").strip()
        if text:
            text_parts.append(text)
    return _estimate_text_tokens("\n".join(text_parts))


def _mark_ephemeral_cache_boundary(
    content_blocks: list[dict[str, Any]],
    records: list[PromptComponentRecord],
    *,
    min_cache_tokens: int = DEFAULT_EXPLICIT_CACHE_MIN_TOKENS,
) -> None:
    """Mark the end of the stable SYSTEM_BASE prefix as an ephemeral cache boundary."""

    stable_prefix_len = 0
    for record in records:
        if not record.rendered_text:
            continue
        if not record.cache_stable:
            break
        stable_prefix_len += 1

    if stable_prefix_len <= 0:
        return

    marker_index = stable_prefix_len - 1
    if marker_index >= len(content_blocks):
        return

    stable_prefix_tokens = _estimate_content_blocks_tokens(content_blocks, stable_prefix_len)
    if stable_prefix_tokens < min_cache_tokens:
        return

    content_blocks[marker_index]["cache_control"] = {"type": "ephemeral"}


def _mark_ephemeral_context_cache_boundaries(
    system_message: dict[str, Any],
    context_messages: list[dict[str, Any]],
    context_records: list[PromptComponentRecord],
    *,
    max_markers: int = 4,
    max_content_block_gap: int = DEFAULT_CONTEXT_CACHE_BLOCK_GAP,
) -> None:
    """Add cache boundaries through long context without exceeding provider limits."""

    if not _context_cache_stable_enough(context_records):
        return

    remaining_markers = max_markers - _count_cache_control_markers([system_message])
    if remaining_markers <= 0:
        return

    blocks_since_marker = _blocks_after_last_cache_marker(system_message)
    for message in context_messages:
        if remaining_markers <= 0:
            break
        content = message.get("content")
        if isinstance(content, str):
            blocks_since_marker += 1
            if blocks_since_marker >= max_content_block_gap:
                message["content"] = [
                    {
                        "type": "text",
                        "text": content,
                        "cache_control": {"type": "ephemeral"},
                    }
                ]
                remaining_markers -= 1
                blocks_since_marker = 0
            continue

        if not isinstance(content, list):
            continue

        for block in content:
            if not isinstance(block, dict):
                continue
            blocks_since_marker += 1
            if block.get("cache_control"):
                blocks_since_marker = 0
                continue
            if blocks_since_marker >= max_content_block_gap:
                block["cache_control"] = {"type": "ephemeral"}
                remaining_markers -= 1
                blocks_since_marker = 0
                if remaining_markers <= 0:
                    break


def _blocks_after_last_cache_marker(message: dict[str, Any]) -> int:
    blocks_since_marker = 0
    for block in _iter_content_blocks(message):
        blocks_since_marker += 1
        if isinstance(block, dict) and block.get("cache_control"):
            blocks_since_marker = 0
    return blocks_since_marker


def _count_cache_control_markers(messages: list[dict[str, Any]]) -> int:
    count = 0
    for message in messages:
        for block in _iter_content_blocks(message):
            if isinstance(block, dict) and block.get("cache_control"):
                count += 1
    return count


def _iter_content_blocks(message: dict[str, Any]) -> list[Any]:
    content = message.get("content")
    if isinstance(content, list):
        return list(content)
    if content is None:
        return []
    return [content]


def _context_cache_stable_enough(records: list[PromptComponentRecord]) -> bool:
    """Return False when this prompt assembly just shifted the context window."""

    for record in records:
        resolver_output = record.metadata.get("resolver_output")
        if not isinstance(resolver_output, dict):
            continue
        if int(resolver_output.get("dropped_turns", 0) or 0) > 0:
            return False
    return True


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
    BUILTIN_MESSAGE_TEXT_PROMPT_COMPONENT_ID = "builtin.instructions.message_text"
    BUILTIN_MESSAGE_TEXT_PROMPT_RESOLVER = "builtin.runtime.message_text"
    BUILTIN_CURRENT_TIME_PROMPT_COMPONENT_ID = "builtin.constraints.current_time"
    BUILTIN_CURRENT_TIME_PROMPT_RESOLVER = "builtin.runtime.current_time"

    @classmethod
    def build_builtin_sliding_window_strategy(
        cls,
        *,
        trigger_ratio: float = 1.0,
        trim_turns: int = 2,
        trim_ratio: float | None = None,
        max_context_tokens: int = DEFAULT_CONTEXT_TRIGGER_TOKENS,
        target_context_tokens: int | None = DEFAULT_CONTEXT_TARGET_TOKENS,
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
                "max_context_tokens": max_context_tokens,
                "target_context_tokens": target_context_tokens,
                "trigger_ratio": trigger_ratio,
                "trim_ratio": trim_ratio,
                "trim_turns": trim_turns,
            },
        )

    def __init__(
        self,
        *,
        fallback_context_trigger_ratio: float = 1.0,
        fallback_context_trim_turns: int = 2,
        fallback_context_max_tokens: int = DEFAULT_CONTEXT_TRIGGER_TOKENS,
        fallback_context_target_tokens: int | None = DEFAULT_CONTEXT_TARGET_TOKENS,
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
            max_context_tokens=fallback_context_max_tokens,
            target_context_tokens=fallback_context_target_tokens,
        )

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
            expand_component_tree(
                component=component,
                request=request,
                components=self._components,
                resolvers=self._resolvers,
                records_by_stage=records_by_stage,
                ordered_records=ordered_records,
            )

        self._inject_context_strategy(request, records_by_stage, ordered_records)
        if request.identity_enabled:
            self._inject_identity_prompts(request, records_by_stage, ordered_records)
        self._inject_runtime_prompts(request, records_by_stage, ordered_records)

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
            if stage_key == PromptStage.SYSTEM_BASE:
                _mark_ephemeral_cache_boundary(
                    system_content,
                    block.components,
                )

        system_message: dict[str, Any] = {"role": "system", "content": system_content}

        # Tools: ABILITIES
        tools = list(stage_by_name[PromptStage.ABILITIES].tools)

        # Context messages: CONTEXT
        context_stage = stage_by_name[PromptStage.CONTEXT]
        context_messages = list(context_stage.messages)
        _mark_ephemeral_context_cache_boundaries(
            system_message,
            context_messages,
            context_stage.components,
        )

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
        return create_prompt_snapshot(result, request)

    def build_log_record(
        self, result: PromptAssemblyResult, request: PromptAssemblyRequest
    ) -> PromptLoggerRecord:
        return build_prompt_log_record(result, request)

    # ── Internal: context strategy ──────────────────────────────────────

    def _inject_context_strategy(
        self,
        request: PromptAssemblyRequest,
        records_by_stage: dict[PromptStage, list[PromptComponentRecord]],
        ordered_records: list[PromptComponentRecord],
    ) -> None:
        if not request.include_context_messages:
            return

        strategy = self._resolve_context_strategy(request)
        if strategy is None:
            return
        if not strategy.enabled:
            return

        policy_sync = sync_context_policy(self._context_manager, request, strategy)
        request = hydrate_request_context(self._context_manager, request)

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
            rendered_messages = inject_identity_layers_into_messages(rendered_messages)

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

        hydrated_request = hydrate_request_context(self._context_manager, request)
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
                cache_stable=dynamic_component.cache_stable,
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
            cache_stable=static_component.cache_stable,
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
                cache_stable=component.cache_stable,
                metadata={**dict(component.metadata), **rendered_metadata},
            )
            records_by_stage[component.stage].append(record)
            ordered_records.append(record)

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
        max_context_tokens: int,
        target_context_tokens: int | None,
    ) -> None:
        self.register_context_strategy(
            self.build_builtin_sliding_window_strategy(
                trigger_ratio=trigger_ratio,
                trim_turns=trim_turns,
                max_context_tokens=max_context_tokens,
                target_context_tokens=target_context_tokens,
            )
        )
        self.register_context_strategy_resolver(
            self.BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER,
            self._resolve_builtin_sliding_window_context,
        )

    def _resolve_builtin_sliding_window_context(
        self,
        request: PromptAssemblyRequest,
        strategy: ContextStrategy,
    ) -> dict[str, Any]:
        return resolve_builtin_sliding_window_context(
            context_manager=self._context_manager,
            request=request,
            strategy=strategy,
        )

    # ── Internal: signature / cache key ─────────────────────────────────

    def _build_signature(self, stages: list[PromptStageBlock]) -> str:
        return build_prompt_signature(stages)

    def _build_cache_key(self, prompt_signature: str, request: PromptAssemblyRequest) -> str:
        return build_prompt_cache_key(prompt_signature, request)

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
