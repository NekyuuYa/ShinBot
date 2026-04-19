"""Structured persistence-layer records."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class ModelProviderRecord:
    id: str
    type: str
    display_name: str
    capability_type: str = "completion"
    base_url: str = ""
    auth: dict[str, Any] = field(default_factory=dict)
    default_params: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ModelDefinitionRecord:
    id: str
    provider_id: str
    litellm_model: str
    display_name: str
    capabilities: list[str] = field(default_factory=list)
    context_window: int | None = None
    default_params: dict[str, Any] = field(default_factory=dict)
    cost_metadata: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ModelRouteRecord:
    id: str
    purpose: str = ""
    strategy: str = "priority"
    enabled: bool = True
    sticky_sessions: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ModelRouteMemberRecord:
    route_id: str
    model_id: str
    priority: int = 0
    weight: float = 1.0
    conditions: dict[str, Any] = field(default_factory=dict)
    timeout_override: float | None = None
    enabled: bool = True


@dataclass(slots=True)
class ModelExecutionRecord:
    id: str
    started_at: str = field(default_factory=utc_now_iso)
    route_id: str = ""
    provider_id: str = ""
    model_id: str = ""
    caller: str = ""
    session_id: str = ""
    instance_id: str = ""
    purpose: str = ""
    first_token_at: str | None = None
    finished_at: str | None = None
    latency_ms: float = 0.0
    time_to_first_token_ms: float | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_hit: bool = False
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    success: bool = False
    error_code: str = ""
    error_message: str = ""
    fallback_from_model_id: str = ""
    fallback_reason: str = ""
    estimated_cost: float | None = None
    currency: str = ""
    prompt_snapshot_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PersonaRecord:
    uuid: str
    name: str
    prompt_definition_uuid: str
    tags: list[str] = field(default_factory=list)
    enabled: bool = True
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ContextStrategyRecord:
    uuid: str
    name: str
    type: str
    resolver_ref: str
    description: str = ""
    config: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class AgentRecord:
    uuid: str
    agent_id: str
    name: str
    persona_uuid: str
    prompts: list[str] = field(default_factory=list)
    tools: list[str] = field(default_factory=list)
    context_strategy: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class PromptDefinitionRecord:
    uuid: str
    prompt_id: str
    name: str
    stage: str
    type: str
    source_type: str = "unknown_source"
    source_id: str = ""
    owner_plugin_id: str = ""
    owner_module: str = ""
    module_path: str = ""
    priority: int = 100
    version: str = "1.0.0"
    description: str = ""
    enabled: bool = True
    content: str = ""
    template_vars: list[str] = field(default_factory=list)
    resolver_ref: str = ""
    bundle_refs: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class BotConfigRecord:
    uuid: str
    instance_id: str
    default_agent_uuid: str = ""
    main_llm: str = ""
    config: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class MessageLogRecord:
    """A single message in the full communication log."""

    session_id: str
    role: str  # "user" or "assistant"
    created_at: float  # millisecond-precision epoch timestamp
    platform_msg_id: str = ""
    sender_id: str = ""
    sender_name: str = ""
    content_json: str = "[]"  # serialised MessageElement AST array
    raw_text: str = ""
    is_read: bool = False
    is_mentioned: bool = False
    id: int | None = None  # set after INSERT


@dataclass(slots=True)
class AIInteractionRecord:
    """AI decision audit trail for a single trigger → response cycle."""

    execution_id: str = ""
    trigger_id: int | None = None
    response_id: int | None = None
    timestamp: float = field(default_factory=time.time)
    latency_ms: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    model_id: str = ""
    provider_id: str = ""
    think_text: str = ""
    injected_context_json: str = "[]"
    tool_calls_json: str = "[]"
    prompt_snapshot_id: str = ""
    id: int | None = None


@dataclass(slots=True)
class PromptSnapshotRecord:
    """TTL-based full prompt snapshot for audit and debugging."""

    id: str
    profile_id: str = ""
    caller: str = ""
    session_id: str = ""
    instance_id: str = ""
    route_id: str = ""
    model_id: str = ""
    prompt_signature: str = ""
    cache_key: str = ""
    messages: list[dict[str, Any]] = field(default_factory=list)
    tools: list[dict[str, Any]] = field(default_factory=list)
    compatibility_used: bool = False
    created_at: float = field(default_factory=time.time)
    expires_at: float | None = None


@dataclass(slots=True)
class MediaAssetRecord:
    raw_hash: str
    element_type: str = "img"
    storage_path: str = ""
    mime_type: str = ""
    file_size: int = 0
    strict_dhash: str = ""
    width: int | None = None
    height: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen_at: float = field(default_factory=time.time)
    last_seen_at: float = field(default_factory=time.time)
    expire_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class MessageMediaLinkRecord:
    message_log_id: int
    session_id: str
    raw_hash: str
    platform_msg_id: str = ""
    media_index: int = 0
    created_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class SessionMediaOccurrenceRecord:
    session_id: str
    raw_hash: str
    strict_dhash: str = ""
    last_sender_id: str = ""
    last_platform_msg_id: str = ""
    recent_timestamps: list[float] = field(default_factory=list)
    occurrence_count: int = 0
    first_seen_at: float = field(default_factory=time.time)
    last_seen_at: float = field(default_factory=time.time)
    expire_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class MediaSemanticRecord:
    raw_hash: str
    kind: str = ""
    digest: str = ""
    verified_by_model: bool = False
    inspection_agent_ref: str = ""
    inspection_llm_ref: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen_at: float = field(default_factory=time.time)
    last_seen_at: float = field(default_factory=time.time)
    expire_at: float = field(default_factory=time.time)
