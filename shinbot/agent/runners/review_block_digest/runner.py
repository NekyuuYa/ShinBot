"""Review block digest runner."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.runners.review_block_digest.prompt_registration import (
    REVIEW_BLOCK_DIGEST_COMPONENT_IDS,
)
from shinbot.agent.runners.review_models import ReviewBlockDigestStageOutput
from shinbot.agent.runners.templates import RunnerTemplateConfig, StructuredOutputRunner
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.services.summaries import SummaryService
from shinbot.agent.utils.parsing import json_schema_response_format

_BLOCK_DIGEST_RESPONSE_FORMAT = json_schema_response_format(
    "agent_review_block_digest",
    {
        "summary": {"type": "string"},
        "reason": {"type": "string"},
    },
    ["summary", "reason"],
)

_BLOCK_DIGEST_TASK_PROMPT = (
    "Summarize this review scan block as a local digest. Keep useful context "
    "for later reply decisions and active chat, but do not select reply targets "
    "or make active chat decisions."
)


class ReviewBlockDigestStageRunner(Protocol):
    """Summarize one review scan block."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewBlockDigestStageOutput:
        """Return a block digest summary."""


class NoopReviewBlockDigestStageRunner:
    """No-op block digest runner."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewBlockDigestStageOutput:
        return ReviewBlockDigestStageOutput()


class LLMReviewBlockDigestStageRunner:
    """Generate a review block digest through the model runtime."""

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
                route_id=routing.route_id,
                model_id=routing.model_id,
                profile_id=routing.profile_id,
                system_prompt=routing.system_prompt,
                task_prompt=_BLOCK_DIGEST_TASK_PROMPT,
                response_format=_BLOCK_DIGEST_RESPONSE_FORMAT,
                component_ids_by_stage=routing.component_ids_by_stage,
                builtin_component_ids=REVIEW_BLOCK_DIGEST_COMPONENT_IDS,
                message_format_config=routing.message_format_config,
                params=routing.params,
                max_model_retries=routing.max_model_retries,
                retry_backoff_seconds=routing.retry_backoff_seconds,
            ),
            message_formatter=message_formatter,
        )

    async def run(self, stage_input: ReviewStageInput) -> ReviewBlockDigestStageOutput:
        payload = await self._template.run(stage_input)
        if payload is None:
            return ReviewBlockDigestStageOutput(reason="llm_review_block_digest_failed")
        output = ReviewBlockDigestStageOutput(
            summary=str(payload.get("summary") or ""),
            reason=str(payload.get("reason") or "llm_review_block_digest"),
        )
        self._save_summary(stage_input, output)
        return output

    def _save_summary(
        self,
        stage_input: ReviewStageInput,
        output: ReviewBlockDigestStageOutput,
    ) -> None:
        if self._summary_service is None or not output.summary.strip():
            return
        metadata = dict(stage_input.metadata)
        block_index = _optional_int(metadata.get("block_index"))
        if block_index is None:
            block_index = _optional_int(metadata.get("offset"))
        self._summary_service.save_block_digest(
            session_id=stage_input.session_id,
            source_run_id=str(metadata.get("review_run_id") or metadata.get("source_run_id") or ""),
            block_index=block_index or 0,
            content=output.summary,
            msg_log_start=_optional_int(metadata.get("start_msg_log_id"))
            or _optional_int(metadata.get("range_start_msg_log_id")),
            msg_log_end=_optional_int(metadata.get("end_msg_log_id"))
            or _optional_int(metadata.get("range_end_msg_log_id")),
            msg_count=_optional_int(metadata.get("message_count")) or 0,
            metadata={
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


__all__ = [
    "LLMReviewBlockDigestStageRunner",
    "NoopReviewBlockDigestStageRunner",
    "ReviewBlockDigestStageRunner",
]
