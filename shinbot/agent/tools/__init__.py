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
    "ToolRegistry",
    "ToolRiskLevel",
    "ToolVisibility",
]

_EXPORT_MODULES = {
    "ToolManager": "shinbot.agent.tools.manager",
    "ToolRegistry": "shinbot.agent.tools.registry",
    "ToolCallRequest": "shinbot.agent.tools.schema",
    "ToolCallResult": "shinbot.agent.tools.schema",
    "ToolDefinition": "shinbot.agent.tools.schema",
    "ToolExecutionContext": "shinbot.agent.tools.schema",
    "ToolOwnerType": "shinbot.agent.tools.schema",
    "ToolRiskLevel": "shinbot.agent.tools.schema",
    "ToolVisibility": "shinbot.agent.tools.schema",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
