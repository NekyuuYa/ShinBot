"""Prompt registry primitives and assembly service."""

from shinbot.agent.prompting.logger import PromptLogger
from shinbot.agent.prompting.registry import PromptRegistry
from shinbot.agent.prompting.schema import (
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
