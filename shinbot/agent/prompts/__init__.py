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
    "PromptRegistry": "shinbot.agent.prompts.registry",
    "PromptLogger": "shinbot.agent.prompts.logger",
    "PromptMessageBuilder": "shinbot.agent.prompts.message_builder",
    "PromptAssemblyRequest": "shinbot.agent.prompts.schema",
    "PromptAssemblyResult": "shinbot.agent.prompts.schema",
    "PromptBuildRequest": "shinbot.agent.prompts.schema",
    "PromptBuildResult": "shinbot.agent.prompts.schema",
    "PromptComponent": "shinbot.agent.prompts.schema",
    "PromptComponentKind": "shinbot.agent.prompts.schema",
    "PromptComponentRecord": "shinbot.agent.prompts.schema",
    "PromptContextPolicy": "shinbot.agent.prompts.schema",
    "PromptInjection": "shinbot.agent.prompts.schema",
    "PromptLoggerRecord": "shinbot.agent.prompts.schema",
    "PromptProfile": "shinbot.agent.prompts.schema",
    "PromptSnapshot": "shinbot.agent.prompts.schema",
    "PromptSource": "shinbot.agent.prompts.schema",
    "PromptSourceType": "shinbot.agent.prompts.schema",
    "PromptStage": "shinbot.agent.prompts.schema",
    "PromptStageAssembly": "shinbot.agent.prompts.schema",
    "PromptStageBlock": "shinbot.agent.prompts.schema",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
