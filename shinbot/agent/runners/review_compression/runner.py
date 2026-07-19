"""Review overflow compression stage runner."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.runners.review_compression.prompt_registration import (
    REVIEW_COMPRESSION_COMPONENT_IDS,
)
from shinbot.agent.runners.review_models import OverflowCompressionStageOutput
from shinbot.agent.runners.templates import RunnerTemplateConfig, StructuredOutputRunner
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.services.summaries import SummaryService
from shinbot.agent.utils.parsing import int_list, json_schema_response_format

_COMPRESSION_RESPONSE_FORMAT = json_schema_response_format(
    "agent_review_overflow_compression",
    {
        "summary": {"type": "string"},
        "candidate_message_ids": {"type": "array", "items": {"type": "integer"}},
        "reason": {"type": "string"},
    },
    ["summary", "candidate_message_ids", "reason"],
)


class OverflowCompressionStageRunner(Protocol):
    """Compress old overflow unread messages before tail review_scan."""

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        """Run one overflow compression chunk."""


class NoopOverflowCompressionStageRunner:
    """No-op compression runner."""

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        """Return a no-op compression output with no summary.

        Args:
            stage_input: Review stage input (ignored by the no-op runner).

        Returns:
            An output with an empty summary and a noop reason.
        """
        return OverflowCompressionStageOutput(reason="noop_overflow_compression")


class LLMOverflowCompressionStageRunner:
    """Compress old overflow unread messages through the model runtime."""

    def __init__(
        self,
        model_runtime: Any,
        *,
        config: RunnerTemplateConfig | None = None,
        prompt_registry: PromptRegistry,
        summary_service: SummaryService | None = None,
        message_formatter: MessageFormatterService | None = None,
    ) -> None:
        routing = config or RunnerTemplateConfig()
        self._summary_service = summary_service
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
                response_format=_COMPRESSION_RESPONSE_FORMAT,
                component_ids_by_stage=routing.component_ids_by_stage,
                builtin_component_ids=REVIEW_COMPRESSION_COMPONENT_IDS,
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

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        """Run the LLM-based overflow compression and return a summary.

        Args:
            stage_input: Review stage input with overflow message context.

        Returns:
            An output containing the compressed summary and candidate message ids,
            or a failed output on error.
        """
        payload = await self._template.run(stage_input)
        if payload is None:
            return OverflowCompressionStageOutput(reason="llm_overflow_compression_failed")
        output = OverflowCompressionStageOutput(
            summary=str(payload.get("summary") or ""),
            candidate_message_ids=int_list(payload.get("candidate_message_ids")),
            reason=str(payload.get("reason") or "llm_overflow_compression"),
        )
        self._save_summary(stage_input, output)
        return output

    def _save_summary(
        self,
        stage_input: ReviewStageInput,
        output: OverflowCompressionStageOutput,
    ) -> None:
        if self._summary_service is None or not output.summary.strip():
            return
        metadata = dict(stage_input.metadata)
        self._summary_service.save_overflow_compression(
            session_id=stage_input.session_id,
            source_run_id=str(metadata.get("review_run_id") or metadata.get("source_run_id") or ""),
            content=output.summary,
            msg_log_start=_optional_int(metadata.get("start_msg_log_id")),
            msg_log_end=_optional_int(metadata.get("end_msg_log_id")),
            msg_count=_optional_int(metadata.get("message_count")) or 0,
            metadata={
                "candidate_message_ids": list(output.candidate_message_ids),
                "reason": output.reason,
                **metadata,
            },
        )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
