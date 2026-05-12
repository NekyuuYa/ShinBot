"""Review scan stage runner — selects candidate message ids."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.runners.review_models import ReviewScanStageOutput
from shinbot.agent.runners.review_scan.prompt_registration import REVIEW_SCAN_COMPONENT_IDS
from shinbot.agent.runners.templates import RunnerTemplateConfig, StructuredOutputRunner
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.utils.parsing import int_list, json_schema_response_format

_SCAN_RESPONSE_FORMAT = json_schema_response_format(
    "agent_review_scan",
    {
        "candidate_message_ids": {"type": "array", "items": {"type": "integer"}},
        "reason": {"type": "string"},
    },
    ["candidate_message_ids", "reason"],
)

class ReviewScanStageRunner(Protocol):
    """Select candidate message ids from one review_scan stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        """Run one review_scan batch and return candidate ids."""


class NoopReviewScanStageRunner:
    """No-op scan runner that returns empty candidates."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        return ReviewScanStageOutput(reason="noop_review_scan")


class LLMReviewScanStageRunner:
    """Select reply-worthy candidate message ids through the model runtime."""

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
                route_id=routing.route_id,
                model_id=routing.model_id,
                profile_id=routing.profile_id,
                response_format=_SCAN_RESPONSE_FORMAT,
                component_ids_by_stage=routing.component_ids_by_stage,
                builtin_component_ids=REVIEW_SCAN_COMPONENT_IDS,
                message_format_config=routing.message_format_config,
                params=routing.params,
                max_model_retries=routing.max_model_retries,
                retry_backoff_seconds=routing.retry_backoff_seconds,
            ),
            message_formatter=message_formatter,
        )

    @property
    def _config(self) -> RunnerTemplateConfig:
        return self._template._config

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        payload = await self._template.run(stage_input)
        if payload is None:
            return ReviewScanStageOutput(reason="llm_review_scan_failed")
        return ReviewScanStageOutput(
            candidate_message_ids=int_list(payload.get("candidate_message_ids")),
            reason=str(payload.get("reason") or "llm_review_scan"),
        )
