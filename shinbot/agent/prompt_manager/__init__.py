"""Prompt management primitives and assembly service exports.

This package is used by several feature modules for shared schema types, so its
package import must stay lightweight.  Public attributes are resolved lazily to
avoid importing the full prompt registry when a module only needs schema types.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "PromptAssemblyRequest",
    "PromptAssemblyResult",
    "PromptComponent",
    "PromptComponentKind",
    "PromptComponentRecord",
    "PromptLogger",
    "PromptLoggerRecord",
    "PromptProfile",
    "PromptRegistry",
    "PromptSnapshot",
    "PromptSource",
    "PromptSourceType",
    "PromptStage",
    "PromptStageBlock",
]

_EXPORT_MODULES = {
    "PromptRegistry": "shinbot.agent.prompt_manager.registry",
    "PromptLogger": "shinbot.agent.prompt_manager.logger",
    "PromptAssemblyRequest": "shinbot.agent.prompt_manager.schema",
    "PromptAssemblyResult": "shinbot.agent.prompt_manager.schema",
    "PromptComponent": "shinbot.agent.prompt_manager.schema",
    "PromptComponentKind": "shinbot.agent.prompt_manager.schema",
    "PromptComponentRecord": "shinbot.agent.prompt_manager.schema",
    "PromptLoggerRecord": "shinbot.agent.prompt_manager.schema",
    "PromptProfile": "shinbot.agent.prompt_manager.schema",
    "PromptSnapshot": "shinbot.agent.prompt_manager.schema",
    "PromptSource": "shinbot.agent.prompt_manager.schema",
    "PromptSourceType": "shinbot.agent.prompt_manager.schema",
    "PromptStage": "shinbot.agent.prompt_manager.schema",
    "PromptStageBlock": "shinbot.agent.prompt_manager.schema",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
