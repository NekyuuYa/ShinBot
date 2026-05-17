"""Agent-related runtime services.

Keep package exports lazy so importing a submodule such as
``shinbot.agent.services.prompt_engine`` does not eagerly import the full runtime graph.
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
]

_EXPORT_MODULES = {
    "ActiveContextPool": "shinbot.agent.services.context",
    "BUILTIN_MEDIA_INSPECTION_AGENT_REF": "shinbot.agent.services.media",
    "BUILTIN_MEDIA_INSPECTION_LLM_REF": "shinbot.agent.services.media",
    "ContextManager": "shinbot.agent.services.context",
    "EmbedResult": "shinbot.agent.services.model_runtime",
    "GenerateResult": "shinbot.agent.services.model_runtime",
    "MediaFingerprint": "shinbot.agent.services.media",
    "MediaInspectionRunner": "shinbot.agent.services.media",
    "MediaService": "shinbot.agent.services.media",
    "ModelCallError": "shinbot.agent.services.model_runtime",
    "ModelRuntime": "shinbot.agent.services.model_runtime",
    "ModelRuntimeCall": "shinbot.agent.services.model_runtime",
    "PromptAssemblyRequest": "shinbot.agent.services.prompt_engine",
    "PromptAssemblyResult": "shinbot.agent.services.prompt_engine",
    "PromptComponent": "shinbot.agent.services.prompt_engine",
    "PromptComponentKind": "shinbot.agent.services.prompt_engine",
    "PromptComponentRecord": "shinbot.agent.services.prompt_engine",
    "PromptLogger": "shinbot.agent.services.prompt_engine",
    "PromptLoggerRecord": "shinbot.agent.services.prompt_engine",
    "PromptProfile": "shinbot.agent.services.prompt_engine",
    "PromptRegistry": "shinbot.agent.services.prompt_engine",
    "PromptSnapshot": "shinbot.agent.services.prompt_engine",
    "PromptSource": "shinbot.agent.services.prompt_engine",
    "PromptSourceType": "shinbot.agent.services.prompt_engine",
    "PromptStage": "shinbot.agent.services.prompt_engine",
    "PromptStageBlock": "shinbot.agent.services.prompt_engine",
    "ResolvedMediaInspectionConfig": "shinbot.agent.services.media",
    "register_media_tools": "shinbot.agent.services.media",
    "ToolCallRequest": "shinbot.agent.services.tools",
    "ToolCallResult": "shinbot.agent.services.tools",
    "ToolDefinition": "shinbot.agent.services.tools",
    "ToolExecutionContext": "shinbot.agent.services.tools",
    "ToolManager": "shinbot.agent.services.tools",
    "ToolOwnerType": "shinbot.agent.services.tools",
    "ToolRegistry": "shinbot.agent.services.tools",
    "ToolRiskLevel": "shinbot.agent.services.tools",
    "ToolVisibility": "shinbot.agent.services.tools",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
