"""Shared configuration for review stage LLM runners."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.services.message_formatter import MessageFormatConfig
from shinbot.agent.services.prompt_engine import PromptStage


@dataclass(slots=True, frozen=True)
class ReviewLLMRunnerConfig:
    """Model routing and prompt configuration shared by review LLM runners."""

    caller: str = "agent.review"
    route_id: str | None = None
    model_id: str | None = None
    profile_id: str = ""
    component_ids_by_stage: dict[PromptStage, list[str]] = field(default_factory=dict)
    message_format_config: MessageFormatConfig | None = None
    params: dict[str, Any] = field(default_factory=dict)
    max_model_retries: int = 1
    retry_backoff_seconds: float = 0.25


__all__ = ["ReviewLLMRunnerConfig"]
