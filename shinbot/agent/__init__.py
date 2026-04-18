"""Agent-related runtime services.

Keep package exports lazy so importing a submodule such as
``shinbot.agent.prompting`` does not eagerly import the full runtime graph.
That avoids circular imports during application bootstrap.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ActiveContextPool",
    "ContextManager",
    "EmbedResult",
    "GenerateResult",
    "ModelCallError",
    "ModelRuntime",
    "ModelRuntimeCall",
    "ContextStrategy",
    "ContextStrategyBudget",
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
    "ActiveContextPool": "shinbot.agent.context",
    "ContextManager": "shinbot.agent.context",
    "EmbedResult": "shinbot.agent.model_runtime",
    "GenerateResult": "shinbot.agent.model_runtime",
    "ModelCallError": "shinbot.agent.model_runtime",
    "ModelRuntime": "shinbot.agent.model_runtime",
    "ModelRuntimeCall": "shinbot.agent.model_runtime",
    "ContextStrategy": "shinbot.agent.prompting",
    "ContextStrategyBudget": "shinbot.agent.prompting",
    "PromptAssemblyRequest": "shinbot.agent.prompting",
    "PromptAssemblyResult": "shinbot.agent.prompting",
    "PromptComponent": "shinbot.agent.prompting",
    "PromptComponentKind": "shinbot.agent.prompting",
    "PromptComponentRecord": "shinbot.agent.prompting",
    "PromptLogger": "shinbot.agent.prompting",
    "PromptLoggerRecord": "shinbot.agent.prompting",
    "PromptProfile": "shinbot.agent.prompting",
    "PromptRegistry": "shinbot.agent.prompting",
    "PromptSnapshot": "shinbot.agent.prompting",
    "PromptSource": "shinbot.agent.prompting",
    "PromptSourceType": "shinbot.agent.prompting",
    "PromptStage": "shinbot.agent.prompting",
    "PromptStageBlock": "shinbot.agent.prompting",
    "ToolCallRequest": "shinbot.agent.tools",
    "ToolCallResult": "shinbot.agent.tools",
    "ToolDefinition": "shinbot.agent.tools",
    "ToolExecutionContext": "shinbot.agent.tools",
    "ToolManager": "shinbot.agent.tools",
    "ToolOwnerType": "shinbot.agent.tools",
    "ToolRegistry": "shinbot.agent.tools",
    "ToolRiskLevel": "shinbot.agent.tools",
    "ToolVisibility": "shinbot.agent.tools",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value

