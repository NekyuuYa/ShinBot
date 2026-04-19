"""Prompt management primitives and assembly service."""

from shinbot.agent.prompt_manager.logger import PromptLogger
from shinbot.agent.prompt_manager.registry import PromptRegistry
from shinbot.agent.prompt_manager.schema import (
    ContextStrategy,
    ContextStrategyBudget,
    PromptAssemblyRequest,
    PromptAssemblyResult,
    PromptComponent,
    PromptComponentKind,
    PromptComponentRecord,
    PromptLoggerRecord,
    PromptProfile,
    PromptSnapshot,
    PromptSource,
    PromptSourceType,
    PromptStage,
    PromptStageBlock,
)

__all__ = [
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
]
