"""Runner templates — composable building blocks for review stage runners."""

from shinbot.agent.runners.templates.config import RunnerTemplateConfig
from shinbot.agent.runners.templates.one_shot_text import OneShotTextRunner
from shinbot.agent.runners.templates.review_prompt_projector import (
    ReviewPromptProjection,
    ReviewPromptProjector,
)
from shinbot.agent.runners.templates.structured_output import StructuredOutputRunner
from shinbot.agent.runners.templates.tool_call_plan import (
    ParsedToolCall,
    ToolCallPlanResult,
    ToolCallPlanRunner,
    parse_tool_call_payload,
)

__all__ = [
    "OneShotTextRunner",
    "ParsedToolCall",
    "ReviewPromptProjection",
    "ReviewPromptProjector",
    "RunnerTemplateConfig",
    "StructuredOutputRunner",
    "ToolCallPlanResult",
    "ToolCallPlanRunner",
    "parse_tool_call_payload",
]
