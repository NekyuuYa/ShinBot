"""Schema objects for Tool registry and execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class ToolOwnerType(StrEnum):
    BUILTIN_MODULE = "builtin_module"
    PLUGIN = "plugin"
    ADAPTER_BRIDGE = "adapter_bridge"
    SKILL_MODULE = "skill_module"
    EXTERNAL_BRIDGE = "external_bridge"


class ToolVisibility(StrEnum):
    PRIVATE = "private"
    SCOPED = "scoped"
    PUBLIC = "public"


class ToolRiskLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass(slots=True)
class ToolDefinition:
    """A single registered tool definition."""

    id: str
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Any
    display_name: str = ""
    output_schema: dict[str, Any] | None = None
    owner_type: ToolOwnerType = ToolOwnerType.BUILTIN_MODULE
    owner_id: str = ""
    owner_module: str = ""
    permission: str = ""
    enabled: bool = True
    visibility: ToolVisibility = ToolVisibility.SCOPED
    timeout_seconds: float = 30.0
    risk_level: ToolRiskLevel = ToolRiskLevel.LOW
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ToolExecutionContext:
    """Standard execution context passed to tool handlers."""

    caller: str
    instance_id: str = ""
    session_id: str = ""
    user_id: str = ""
    trace_id: str = ""
    run_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ToolCallRequest:
    """A normalized tool call request."""

    tool_name: str
    arguments: dict[str, Any] = field(default_factory=dict)
    caller: str = ""
    instance_id: str = ""
    session_id: str = ""
    user_id: str = ""
    trace_id: str = ""
    run_id: str = ""
    dry_run: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ToolCallResult:
    """A normalized tool call result."""

    tool_name: str
    success: bool
    output: Any = None
    error_code: str = ""
    error_message: str = ""
    started_at: str = field(default_factory=utc_now_iso)
    finished_at: str = field(default_factory=utc_now_iso)
    latency_ms: float = 0.0
    audit_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
