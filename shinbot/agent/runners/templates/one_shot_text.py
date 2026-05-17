"""One-shot text runner template for plain-text LLM calls."""

from __future__ import annotations

import logging
from typing import Any

from shinbot.agent.runners.templates.base import RunnerTemplateBase
from shinbot.agent.runners.templates.config import RunnerTemplateConfig
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry

logger = logging.getLogger(__name__)


class OneShotTextRunner(RunnerTemplateBase):
    """Template for review stages that expect plain text output.

    Calls the model without ``response_format`` and returns the raw text.
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
        self._log_name = "OneShotTextRunner"

    async def run(self, stage_input: ReviewStageInput) -> str | None:
        """Run one stage and return the text output, or ``None`` on failure."""
        try:
            messages, metadata = self._build_model_call_parts(stage_input)
        except Exception:
            logger.exception(
                "OneShotTextRunner prompt build failed for stage %s session %s",
                stage_input.purpose,
                stage_input.session_id,
            )
            return None
        result = await self._generate_model(
            stage_input,
            messages=messages,
            tools=[],
            response_format=None,
            metadata=metadata,
        )
        if result is None:
            return None
        text = (result.text or "").strip()
        return text or None
