"""Configuration dataclass for runner templates."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.runtime.instance_config import (
    InstanceRuntimeConfigResolver,
    RuntimeModelTarget,
)
from shinbot.agent.services.message_formatter import MessageFormatConfig
from shinbot.agent.services.prompt_engine import PromptStage


@dataclass(slots=True, frozen=True)
class RunnerTemplateConfig:
    """Unified configuration for all runner templates.

    Merges model-routing fields from ReviewLLMRunnerConfig with per-runner
    prompt/output declarations so that a template can be driven entirely
    by config rather than class attributes.
    """

    caller: str = "agent.review"
    llm: str = ""
    default_llm: str = ""
    route_id: str | None = None
    model_id: str | None = None
    profile_id: str = ""
    response_format: dict[str, Any] | None = None
    component_ids_by_stage: dict[PromptStage, list[str]] = field(default_factory=dict)
    builtin_component_ids: dict[PromptStage, list[str]] = field(default_factory=dict)
    message_format_config: MessageFormatConfig | None = None
    params: dict[str, Any] = field(default_factory=dict)
    max_model_retries: int = 1
    retry_backoff_seconds: float = 0.25
    instance_config_resolver: InstanceRuntimeConfigResolver | None = None
    model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None
