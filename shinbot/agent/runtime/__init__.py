"""Agent runtime helpers and service assembly."""

from importlib import import_module
from typing import Any

from shinbot.agent.runtime.prompt_registration import register_runtime_prompt_components
from shinbot.agent.runtime.prompt_runtime import (
    resolve_current_time_prompt,
    resolve_message_text_prompt,
)

__all__ = [
    "AgentRuntime",
    "register_runtime_prompt_components",
    "resolve_current_time_prompt",
    "resolve_message_text_prompt",
]

_EXPORT_MODULES = {
    "AgentRuntime": "shinbot.agent.runtime.services",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
