"""Shared configuration for review stage LLM runners."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.runtime.instance_config import (
    InstanceRuntimeConfigResolver,
    RuntimeModelTarget,
)
from shinbot.agent.runtime.tool_config import StageToolConfig
from shinbot.agent.services.message_formatter import MessageFormatConfig
from shinbot.agent.services.prompt_engine import PromptStage


@dataclass(slots=True, frozen=True)
class ReviewLLMRunnerConfig:
    """Model routing and prompt configuration shared by review LLM runners."""

    caller: str = "agent.review"
    workflow_id: str = "review"
    llm: str = ""
    default_llm: str = ""
    route_id: str | None = None
    model_id: str | None = None
    profile_id: str = ""
    component_ids_by_stage: dict[PromptStage, list[str]] = field(default_factory=dict)
    special_prompt_ids: dict[str, str] = field(default_factory=dict)
    message_format_config: MessageFormatConfig | None = None
    params: dict[str, Any] = field(default_factory=dict)
    tool_config: StageToolConfig = field(default_factory=StageToolConfig)
    max_model_retries: int = 1
    retry_backoff_seconds: float = 0.25
    model_deadline_seconds: float | None = None
    instance_config_resolver: InstanceRuntimeConfigResolver | None = None
    model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None


__all__ = ["ReviewLLMRunnerConfig"]
