"""Agent-related runtime services.

Keep package exports lazy so importing a submodule such as
``shinbot.agent.prompt_manager`` does not eagerly import the full runtime graph.
That avoids circular imports during application bootstrap.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ActiveContextPool",
    "BUILTIN_MEDIA_INSPECTION_AGENT_REF",
    "BUILTIN_MEDIA_INSPECTION_LLM_REF",
    "ContextManager",
    "EmbedResult",
    "GenerateResult",
    "MediaFingerprint",
    "MediaInspectionRunner",
    "MediaService",
    "ModelCallError",
    "ModelRuntime",
    "ModelRuntimeCall",
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
    "ResolvedMediaInspectionConfig",
    "register_media_tools",
    "ToolCallRequest",
    "ToolCallResult",
    "ToolDefinition",
    "ToolExecutionContext",
    "ToolManager",
    "ToolOwnerType",
    "ToolRegistry",
    "ToolRiskLevel",
    "ToolVisibility",
    "WorkflowRunner",
]

_EXPORT_MODULES = {
    "ActiveContextPool": "shinbot.agent.context",
    "BUILTIN_MEDIA_INSPECTION_AGENT_REF": "shinbot.agent.media",
    "BUILTIN_MEDIA_INSPECTION_LLM_REF": "shinbot.agent.media",
    "ContextManager": "shinbot.agent.context",
    "EmbedResult": "shinbot.agent.model_runtime",
    "GenerateResult": "shinbot.agent.model_runtime",
    "MediaFingerprint": "shinbot.agent.media",
    "MediaInspectionRunner": "shinbot.agent.media",
    "MediaService": "shinbot.agent.media",
    "ModelCallError": "shinbot.agent.model_runtime",
    "ModelRuntime": "shinbot.agent.model_runtime",
    "ModelRuntimeCall": "shinbot.agent.model_runtime",
    "PromptAssemblyRequest": "shinbot.agent.prompt_manager",
    "PromptAssemblyResult": "shinbot.agent.prompt_manager",
    "PromptComponent": "shinbot.agent.prompt_manager",
    "PromptComponentKind": "shinbot.agent.prompt_manager",
    "PromptComponentRecord": "shinbot.agent.prompt_manager",
    "PromptLogger": "shinbot.agent.prompt_manager",
    "PromptLoggerRecord": "shinbot.agent.prompt_manager",
    "PromptProfile": "shinbot.agent.prompt_manager",
    "PromptRegistry": "shinbot.agent.prompt_manager",
    "PromptSnapshot": "shinbot.agent.prompt_manager",
    "PromptSource": "shinbot.agent.prompt_manager",
    "PromptSourceType": "shinbot.agent.prompt_manager",
    "PromptStage": "shinbot.agent.prompt_manager",
    "PromptStageBlock": "shinbot.agent.prompt_manager",
    "ResolvedMediaInspectionConfig": "shinbot.agent.media",
    "register_media_tools": "shinbot.agent.media",
    "ToolCallRequest": "shinbot.agent.tools",
    "ToolCallResult": "shinbot.agent.tools",
    "ToolDefinition": "shinbot.agent.tools",
    "ToolExecutionContext": "shinbot.agent.tools",
    "ToolManager": "shinbot.agent.tools",
    "ToolOwnerType": "shinbot.agent.tools",
    "ToolRegistry": "shinbot.agent.tools",
    "ToolRiskLevel": "shinbot.agent.tools",
    "ToolVisibility": "shinbot.agent.tools",
    "WorkflowRunner": "shinbot.agent.workflow",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
