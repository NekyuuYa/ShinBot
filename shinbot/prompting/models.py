"""Prompt registry object model."""

from __future__ import annotations

import hashlib
import json
import time
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


class PromptStage(str, Enum):
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
    PromptStage.CONTEXT,
    PromptStage.ABILITIES,
    PromptStage.COMPATIBILITY,
    PromptStage.INSTRUCTIONS,
    PromptStage.CONSTRAINTS,
)


class PromptComponentKind(str, Enum):
    STATIC_TEXT = "static_text"
    TEMPLATE = "template"
    RESOLVER = "resolver"
    BUNDLE = "bundle"
    EXTERNAL_INJECTION = "external_injection"


class PromptSourceType(str, Enum):
    BUILTIN_SYSTEM = "builtin_system"
    AGENT_PLUGIN = "agent_plugin"
    CONTEXT_PLUGIN = "context_plugin"
    TOOLING_MODULE = "tooling_module"
    SKILL_MODULE = "skill_module"
    LEGACY_BRIDGE = "legacy_bridge"
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

        if (
            self.kind == PromptComponentKind.EXTERNAL_INJECTION
            and self.stage not in (PromptStage.COMPATIBILITY, PromptStage.INSTRUCTIONS)
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


class PromptAssemblyRequest(BaseModel):
    profile_id: str = ""
    caller: str = ""
    session_id: str = ""
    instance_id: str = ""
    route_id: str = ""
    model_id: str = ""
    task_id: str = ""
    component_overrides: list[str] = Field(default_factory=list)
    disabled_components: list[str] = Field(default_factory=list)
    instruction_payload: str | dict[str, Any] | None = None
    constraint_payload: str | dict[str, Any] | None = None
    template_inputs: dict[str, Any] = Field(default_factory=dict)
    context_inputs: dict[str, Any] = Field(default_factory=dict)
    abilities_inputs: dict[str, Any] = Field(default_factory=dict)
    compatibility_payloads: list[dict[str, Any]] = Field(default_factory=list)
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
    text_hash: str = ""
    cache_stable: bool = True
    truncated: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptStageBlock(BaseModel):
    stage: PromptStage
    components: list[PromptComponentRecord] = Field(default_factory=list)
    rendered_text: str = ""
    truncated: bool = False
    token_estimate: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class PromptAssemblyResult(BaseModel):
    profile_id: str = ""
    caller: str = ""
    stages: list[PromptStageBlock] = Field(default_factory=list)
    ordered_components: list[PromptComponentRecord] = Field(default_factory=list)
    final_prompt: str = ""
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
    final_prompt: str = ""
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

