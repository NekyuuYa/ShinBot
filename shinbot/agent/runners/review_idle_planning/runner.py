"""Active chat idle review planning stage runner."""

from __future__ import annotations

import math
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
                workflow_id=routing.workflow_id,
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
                model_deadline_seconds=routing.model_deadline_seconds,
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
        run = await self._template.run_with_provenance(stage_input)
        payload = run.payload
        if payload is None:
            return IdleReviewPlanningStageOutput(
                reason="llm_idle_review_planning_failed",
                failure_code="model_output_unavailable",
                failure_message=(
                    "model runtime returned no structured idle review planning output"
                ),
                model_execution_id=run.model_execution_id,
                prompt_signature=run.prompt_signature,
            )
        next_review_after_seconds, valid_delay = _parse_optional_positive_float(
            payload.get("next_review_after_seconds")
        )
        mention_sensitivity, valid_sensitivity = _parse_mention_sensitivity(
            payload.get("mention_sensitivity")
        )
        mention_wake_count, valid_wake_count = _parse_optional_positive_int(
            payload.get("mention_wake_count")
        )
        mention_wake_window_seconds, valid_wake_window = (
            _parse_optional_positive_float(
                payload.get("mention_wake_window_seconds")
            )
        )
        invalid_fields = [
            field_name
            for field_name, valid in (
                ("next_review_after_seconds", valid_delay),
                ("mention_sensitivity", valid_sensitivity),
                ("mention_wake_count", valid_wake_count),
                ("mention_wake_window_seconds", valid_wake_window),
            )
            if not valid
        ]
        if (mention_wake_count is None) != (mention_wake_window_seconds is None):
            invalid_fields.append("mention_wake_threshold")
        if invalid_fields:
            return IdleReviewPlanningStageOutput(
                reason="llm_idle_review_planning_invalid_output",
                failure_code="invalid_model_output",
                failure_message=(
                    "invalid idle review planning fields: "
                    + ", ".join(invalid_fields)
                ),
                model_execution_id=run.model_execution_id,
                prompt_signature=run.prompt_signature,
            )
        return IdleReviewPlanningStageOutput(
            next_review_after_seconds=next_review_after_seconds,
            reason=str(payload.get("reason") or "llm_idle_review_planning"),
            mention_sensitivity=mention_sensitivity,
            mention_wake_count=mention_wake_count,
            mention_wake_window_seconds=mention_wake_window_seconds,
            model_execution_id=run.model_execution_id,
            prompt_signature=run.prompt_signature,
        )


def _parse_optional_positive_float(value: Any) -> tuple[float | None, bool]:
    """Decode a nullable positive finite float without silent coercion."""

    if value is None:
        return None, True
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None, False
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        return None, False
    return result, True


def _parse_optional_positive_int(value: Any) -> tuple[int | None, bool]:
    """Decode a nullable positive integer without truncating model output."""

    if value is None:
        return None, True
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None, False
    return value, True


def _parse_mention_sensitivity(
    value: Any,
) -> tuple[MentionSensitivity | None, bool]:
    """Decode a nullable mention sensitivity and surface malformed values."""

    if value is None:
        return None, True
    try:
        return MentionSensitivity(str(value)), True
    except (TypeError, ValueError):
        return None, False
