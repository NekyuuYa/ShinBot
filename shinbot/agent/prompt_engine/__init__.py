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
    "PromptBuildRequest",
    "PromptBuildResult",
    "PromptComponent",
    "PromptComponentKind",
    "PromptComponentRecord",
    "PromptContextPolicy",
    "PromptInjection",
    "PromptLogger",
    "PromptLoggerRecord",
    "PromptMessageBuilder",
    "PromptProfile",
    "PromptRegistry",
    "PromptSnapshot",
    "PromptSource",
    "PromptSourceType",
    "PromptStage",
    "PromptStageAssembly",
    "PromptStageBlock",
]

_EXPORT_MODULES = {
    "PromptRegistry": "shinbot.agent.prompt_engine.registry",
    "PromptLogger": "shinbot.agent.prompt_engine.logger",
    "PromptMessageBuilder": "shinbot.agent.prompt_engine.message_builder",
    "PromptAssemblyRequest": "shinbot.agent.prompt_engine.schema",
    "PromptAssemblyResult": "shinbot.agent.prompt_engine.schema",
    "PromptBuildRequest": "shinbot.agent.prompt_engine.schema",
    "PromptBuildResult": "shinbot.agent.prompt_engine.schema",
    "PromptComponent": "shinbot.agent.prompt_engine.schema",
    "PromptComponentKind": "shinbot.agent.prompt_engine.schema",
    "PromptComponentRecord": "shinbot.agent.prompt_engine.schema",
    "PromptContextPolicy": "shinbot.agent.prompt_engine.schema",
    "PromptInjection": "shinbot.agent.prompt_engine.schema",
    "PromptLoggerRecord": "shinbot.agent.prompt_engine.schema",
    "PromptProfile": "shinbot.agent.prompt_engine.schema",
    "PromptSnapshot": "shinbot.agent.prompt_engine.schema",
    "PromptSource": "shinbot.agent.prompt_engine.schema",
    "PromptSourceType": "shinbot.agent.prompt_engine.schema",
    "PromptStage": "shinbot.agent.prompt_engine.schema",
    "PromptStageAssembly": "shinbot.agent.prompt_engine.schema",
    "PromptStageBlock": "shinbot.agent.prompt_engine.schema",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
