"""Tool registry and runtime exports."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ToolCallRequest",
    "ToolCallResult",
    "ToolDefinition",
    "ToolExecutionContext",
    "ToolManager",
    "ToolOwnerType",
    "ToolSchemaBuilder",
    "ToolRegistry",
    "ToolRiskLevel",
    "ToolVisibility",
]

_EXPORT_MODULES = {
    "ToolManager": "shinbot.agent.services.tools.manager",
    "ToolSchemaBuilder": "shinbot.agent.services.tools.schema_builder",
    "ToolRegistry": "shinbot.agent.services.tools.registry",
    "ToolCallRequest": "shinbot.agent.services.tools.schema",
    "ToolCallResult": "shinbot.agent.services.tools.schema",
    "ToolDefinition": "shinbot.agent.services.tools.schema",
    "ToolExecutionContext": "shinbot.agent.services.tools.schema",
    "ToolOwnerType": "shinbot.agent.services.tools.schema",
    "ToolRiskLevel": "shinbot.agent.services.tools.schema",
    "ToolVisibility": "shinbot.agent.services.tools.schema",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
