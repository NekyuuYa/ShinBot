"""Structured persistence-layer records."""

from __future__ import annotations

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
    metadata: dict[str, Any] = field(default_factory=dict)
