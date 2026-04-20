"""Prompt registry schema definitions."""

from __future__ import annotations

import hashlib
import json
import time
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


class PromptStage(StrEnum):
    SYSTEM_BASE = "system_base"
    IDENTITY = "identity"
    CONTEXT = "context"
    ABILITIES = "abilities"
    COMPATIBILITY = "compatibility"
    INSTRUCTIONS = "instructions"
    CONSTRAINTS = "constraints"


PROMPT_STAGE_ORDER: tuple[PromptStage, ...] = (
    PromptStage.SYSTEM_BASE,
    PromptStage.IDENTITY,
    PromptStage.ABILITIES,
    PromptStage.CONTEXT,
    PromptStage.COMPATIBILITY,
    PromptStage.INSTRUCTIONS,
    PromptStage.CONSTRAINTS,
)


class PromptComponentKind(StrEnum):
    STATIC_TEXT = "static_text"
    TEMPLATE = "template"
    RESOLVER = "resolver"
    BUNDLE = "bundle"
    EXTERNAL_INJECTION = "external_injection"


class PromptSourceType(StrEnum):
    BUILTIN_SYSTEM = "builtin_system"
    AGENT_PLUGIN = "agent_plugin"
    CONTEXT_PLUGIN = "context_plugin"
    TOOLING_MODULE = "tooling_module"
    SKILL_MODULE = "skill_module"
    EXTERNAL_INJECTION = "external_injection"
    UNKNOWN_SOURCE = "unknown_source"


class PromptSource(BaseModel):
    source_type: PromptSourceType = PromptSourceType.UNKNOWN_SOURCE
    source_id: str = ""
    owner_plugin_id: str = ""
    owner_module: str = ""
    module_path: str = ""
    resolver_name: str = ""
    is_builtin: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptComponent(BaseModel):
    id: str
    stage: PromptStage
    kind: PromptComponentKind
    version: str = "1.0.0"
    priority: int = 100
    enabled: bool = True
    cache_stable: bool = True
    content: str = ""
    template_vars: list[str] = Field(default_factory=list)
    resolver_ref: str = ""
    bundle_refs: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_component_shape(self) -> PromptComponent:
        if not self.id.strip():
            raise ValueError("PromptComponent.id must not be empty")

        if self.kind == PromptComponentKind.EXTERNAL_INJECTION and self.stage not in (
            PromptStage.COMPATIBILITY,
            PromptStage.INSTRUCTIONS,
        ):
            raise ValueError(
                "external_injection components may only target compatibility or instructions"
            )

        if self.kind == PromptComponentKind.STATIC_TEXT and not self.content:
            raise ValueError("static_text components require content")

        if self.kind == PromptComponentKind.TEMPLATE:
            if not self.content:
                raise ValueError("template components require content")
            if not self.template_vars:
                raise ValueError("template components require template_vars")

        if self.kind == PromptComponentKind.RESOLVER and not self.resolver_ref:
            raise ValueError("resolver components require resolver_ref")

        if self.kind == PromptComponentKind.BUNDLE and not self.bundle_refs:
            raise ValueError("bundle components require bundle_refs")

        return self


class PromptProfile(BaseModel):
    id: str
    display_name: str = ""
    description: str = ""
    enabled: bool = True
    base_components: list[str] = Field(default_factory=list)
    default_constraints: list[str] = Field(default_factory=list)
    default_metadata: dict[str, Any] = Field(default_factory=dict)


class ContextStrategyBudget(BaseModel):
    max_context_tokens: int | None = None
    max_history_turns: int | None = None
    memory_summary_required: bool = False
    truncate_policy: str = "tail"
    trigger_ratio: float = 0.5
    trim_ratio: float | None = None
    trim_turns: int = 2

    @model_validator(mode="after")
    def validate_budget(self) -> ContextStrategyBudget:
        if not 0 < self.trigger_ratio <= 1:
            raise ValueError("ContextStrategyBudget.trigger_ratio must be within (0, 1]")
        if self.trim_ratio is not None and not 0 < self.trim_ratio < 1:
            raise ValueError("ContextStrategyBudget.trim_ratio must be within (0, 1)")
        if self.trim_turns < 1:
            raise ValueError("ContextStrategyBudget.trim_turns must be greater than or equal to 1")
        return self


class ContextStrategy(BaseModel):
    id: str
    display_name: str = ""
    description: str = ""
    resolver_ref: str
    enabled: bool = True
    priority: int = 100
    budget: ContextStrategyBudget = Field(default_factory=ContextStrategyBudget)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_strategy_shape(self) -> ContextStrategy:
        if not self.id.strip():
            raise ValueError("ContextStrategy.id must not be empty")
        if not self.resolver_ref.strip():
            raise ValueError("ContextStrategy.resolver_ref must not be empty")
        return self


class PromptAssemblyRequest(BaseModel):
    model_config = {"extra": "forbid"}

    profile_id: str = ""
    context_strategy_id: str = ""
    identity_enabled: bool = True
    caller: str = ""
    session_id: str = ""
    instance_id: str = ""
    route_id: str = ""
    model_id: str = ""
    model_context_window: int | None = None
    hydrate_session_context: bool = True
    include_context_messages: bool = True
    task_id: str = ""
    component_overrides: list[str] = Field(default_factory=list)
    disabled_components: list[str] = Field(default_factory=list)
    template_inputs: dict[str, Any] = Field(default_factory=dict)
    context_inputs: dict[str, Any] = Field(default_factory=dict)
    abilities_inputs: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptComponentRecord(BaseModel):
    component_id: str
    stage: PromptStage
    kind: PromptComponentKind
    version: str = ""
    priority: int = 100
    selected: bool = True
    source: PromptSource = Field(default_factory=PromptSource)
    rendered_text: str = ""
    rendered_data: list[dict[str, Any]] | None = None
    rendered_messages: list[dict[str, Any]] | None = None
    rendered_content_blocks: list[dict[str, Any]] | None = None
    text_hash: str = ""
    cache_stable: bool = True
    truncated: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptStageBlock(BaseModel):
    stage: PromptStage
    components: list[PromptComponentRecord] = Field(default_factory=list)
    rendered_text: str = ""
    tools: list[dict[str, Any]] = Field(default_factory=list)
    messages: list[dict[str, Any]] = Field(default_factory=list)
    truncated: bool = False
    token_estimate: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptAssemblyResult(BaseModel):
    profile_id: str = ""
    caller: str = ""
    stages: list[PromptStageBlock] = Field(default_factory=list)
    ordered_components: list[PromptComponentRecord] = Field(default_factory=list)
    messages: list[dict[str, Any]] = Field(default_factory=list)
    tools: list[dict[str, Any]] = Field(default_factory=list)
    prompt_signature: str = ""
    cache_key: str = ""
    compatibility_used: bool = False
    has_unknown_source: bool = False
    truncation: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptSnapshot(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: float = Field(default_factory=time.time)
    profile_id: str = ""
    caller: str = ""
    session_id: str = ""
    instance_id: str = ""
    route_id: str = ""
    model_id: str = ""
    prompt_signature: str = ""
    cache_key: str = ""
    components: list[PromptComponentRecord] = Field(default_factory=list)
    stages: list[PromptStageBlock] = Field(default_factory=list)
    full_messages: list[dict[str, Any]] = Field(default_factory=list)
    full_tools: list[dict[str, Any]] = Field(default_factory=list)
    compatibility_used: bool = False
    truncation: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptLoggerRecord(BaseModel):
    timestamp: float = Field(default_factory=time.time)
    entry_type: str = "prompt_assembly"
    profile_id: str = ""
    caller: str = ""
    session_id: str = ""
    instance_id: str = ""
    route_id: str = ""
    model_id: str = ""
    prompt_signature: str = ""
    cache_key: str = ""
    compatibility_used: bool = False
    selected_component_count: int = 0
    unknown_source_count: int = 0
    truncation_summary: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(self.model_dump(mode="json"), ensure_ascii=False)


def stable_text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
