"""Runner templates — composable building blocks for review stage runners."""

from shinbot.agent.runners.templates.config import RunnerTemplateConfig
from shinbot.agent.runners.templates.one_shot_text import OneShotTextRunner
from shinbot.agent.runners.templates.structured_output import StructuredOutputRunner
from shinbot.agent.runners.templates.tool_call_plan import (
    ToolCallPlanResult,
    ToolCallPlanRunner,
)

__all__ = [
    "OneShotTextRunner",
    "RunnerTemplateConfig",
    "StructuredOutputRunner",
    "ToolCallPlanResult",
    "ToolCallPlanRunner",
]
