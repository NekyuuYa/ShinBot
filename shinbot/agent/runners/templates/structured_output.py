"""Structured-output runner template for JSON schema-constrained LLM calls."""

from __future__ import annotations

import logging
from typing import Any

from shinbot.agent.runners.templates.base import RunnerTemplateBase
from shinbot.agent.runners.templates.config import RunnerTemplateConfig
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.utils.parsing import parse_json_object

logger = logging.getLogger(__name__)


class StructuredOutputRunner(RunnerTemplateBase):
    """Template for review stages that expect a JSON object matching a schema.

    Handles prompt assembly, model call, and JSON parsing.  Returns the raw
    ``dict`` payload so callers only need to map it to their typed output.
    """

    def __init__(
        self,
        model_runtime: Any,
        *,
        prompt_registry: PromptRegistry,
        config: RunnerTemplateConfig,
        message_formatter: MessageFormatterService | None = None,
    ) -> None:
        super().__init__(
            model_runtime,
            prompt_registry=prompt_registry,
            config=config,
            message_formatter=message_formatter,
        )
        self._log_name = "StructuredOutputRunner"

    async def run(self, stage_input: ReviewStageInput) -> dict[str, Any] | None:
        """Run one stage and return the parsed JSON payload, or ``None`` on failure."""
        try:
            messages, metadata = self._build_model_call_parts(stage_input)
        except Exception:
            self._log_prompt_build_failure(stage_input)
            return None
        result = await self._generate_model(
            stage_input,
            messages=messages,
            tools=[],
            response_format=self._config.response_format,
            metadata=metadata,
        )
        if result is None:
            return None
        return parse_json_object(result.text or "")

    def _log_prompt_build_failure(self, stage_input: ReviewStageInput) -> None:
        logger.exception(
            "StructuredOutputRunner prompt build failed for stage %s session %s",
            stage_input.purpose,
            stage_input.session_id,
        )
