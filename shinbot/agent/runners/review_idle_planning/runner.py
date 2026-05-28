"""Active chat idle review planning stage runner."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.runners.review_idle_planning.prompt_registration import (
    IDLE_REVIEW_PLANNING_COMPONENT_IDS,
)
from shinbot.agent.runners.review_models import IdleReviewPlanningStageOutput
from shinbot.agent.runners.templates import RunnerTemplateConfig, StructuredOutputRunner
from shinbot.agent.scheduler.models import MentionSensitivity
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.utils.parsing import json_schema_response_format

_IDLE_PLANNING_RESPONSE_FORMAT = json_schema_response_format(
    "agent_review_idle_review_planning",
    {
        "next_review_after_seconds": {"type": ["number", "null"]},
        "reason": {"type": "string"},
        "mention_sensitivity": {
            "type": ["string", "null"],
            "enum": [item.value for item in MentionSensitivity] + [None],
        },
        "mention_wake_count": {"type": ["integer", "null"]},
        "mention_wake_window_seconds": {"type": ["number", "null"]},
    },
    ["next_review_after_seconds", "reason"],
)


class IdleReviewPlanningStageRunner(Protocol):
    """Plan the next review interval before active chat returns to idle."""

    async def run(self, stage_input: ReviewStageInput) -> IdleReviewPlanningStageOutput:
        """Run one active_chat -> idle planning input."""


class NoopIdleReviewPlanningStageRunner:
    """No-op planner that lets the scheduler fallback policy decide."""

    async def run(self, stage_input: ReviewStageInput) -> IdleReviewPlanningStageOutput:
        """Return a no-op planning output with no timing override.

        Args:
            stage_input: Review stage input (ignored by the no-op runner).

        Returns:
            An output with no timing and a noop reason.
        """
        return IdleReviewPlanningStageOutput(reason="noop_idle_review_planning")


class LLMIdleReviewPlanningStageRunner:
    """Choose the next idle review parameters through the model runtime."""

    def __init__(
        self,
        model_runtime: Any,
        *,
        config: RunnerTemplateConfig | None = None,
        prompt_registry: PromptRegistry,
        message_formatter: MessageFormatterService | None = None,
    ) -> None:
        routing = config or RunnerTemplateConfig()
        self._template = StructuredOutputRunner(
            model_runtime,
            prompt_registry=prompt_registry,
            config=RunnerTemplateConfig(
                caller=routing.caller,
                llm=routing.llm,
                default_llm=routing.default_llm,
                route_id=routing.route_id,
                model_id=routing.model_id,
                profile_id=routing.profile_id,
                response_format=_IDLE_PLANNING_RESPONSE_FORMAT,
                component_ids_by_stage=routing.component_ids_by_stage,
                builtin_component_ids=IDLE_REVIEW_PLANNING_COMPONENT_IDS,
                message_format_config=routing.message_format_config,
                params=routing.params,
                tool_config=routing.tool_config,
                max_model_retries=routing.max_model_retries,
                retry_backoff_seconds=routing.retry_backoff_seconds,
                instance_config_resolver=routing.instance_config_resolver,
                model_target_resolver=routing.model_target_resolver,
            ),
            message_formatter=message_formatter,
        )

    @property
    def _config(self) -> RunnerTemplateConfig:
        return self._template._config

    async def run(self, stage_input: ReviewStageInput) -> IdleReviewPlanningStageOutput:
        """Run the LLM-based idle review planner and choose timing parameters.

        Args:
            stage_input: Review stage input with active-chat context.

        Returns:
            An output with the next review interval and mention sensitivity
            settings, or a failed output on error.
        """
        payload = await self._template.run(stage_input)
        if payload is None:
            return IdleReviewPlanningStageOutput(
                reason="llm_idle_review_planning_failed",
            )
        return IdleReviewPlanningStageOutput(
            next_review_after_seconds=_optional_positive_float(
                payload.get("next_review_after_seconds")
            ),
            reason=str(payload.get("reason") or "llm_idle_review_planning"),
            mention_sensitivity=_mention_sensitivity(payload.get("mention_sensitivity")),
            mention_wake_count=_optional_positive_int(payload.get("mention_wake_count")),
            mention_wake_window_seconds=_optional_positive_float(
                payload.get("mention_wake_window_seconds")
            ),
        )


def _optional_positive_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result <= 0.0:
        return None
    return result


def _optional_positive_int(value: Any) -> int | None:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    if result <= 0:
        return None
    return result


def _mention_sensitivity(value: Any) -> MentionSensitivity | None:
    if value is None:
        return None
    try:
        return MentionSensitivity(str(value))
    except ValueError:
        return None
