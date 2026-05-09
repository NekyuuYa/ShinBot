"""Review overflow compression stage runner."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.runners._review_base import ReviewLLMStageRunnerBase
from shinbot.agent.runners.review_compression.prompt_registration import (
    REVIEW_COMPRESSION_COMPONENT_IDS,
)
from shinbot.agent.runners.review_models import OverflowCompressionStageOutput
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.utils.parsing import int_list, json_schema_response_format


class OverflowCompressionStageRunner(Protocol):
    """Compress old overflow unread messages before tail review_scan."""

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        """Run one overflow compression chunk."""


class NoopOverflowCompressionStageRunner:
    """No-op compression runner."""

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        return OverflowCompressionStageOutput(reason="noop_overflow_compression")


class LLMOverflowCompressionStageRunner(ReviewLLMStageRunnerBase):
    """Compress old overflow unread messages through the model runtime."""

    builtin_component_ids = REVIEW_COMPRESSION_COMPONENT_IDS
    task_prompt = (
        "Compress the supplied older unread messages for later review. Keep only "
        "useful context, notable unresolved topics, and message ids worth closer reply review."
    )
    response_format = json_schema_response_format(
        "agent_review_overflow_compression",
        {
            "summary": {"type": "string"},
            "candidate_message_ids": {"type": "array", "items": {"type": "integer"}},
            "reason": {"type": "string"},
        },
        ["summary", "candidate_message_ids", "reason"],
    )

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        payload = await self._generate_payload(stage_input)
        if payload is None:
            return OverflowCompressionStageOutput(reason="llm_overflow_compression_failed")
        return OverflowCompressionStageOutput(
            summary=str(payload.get("summary") or ""),
            candidate_message_ids=int_list(payload.get("candidate_message_ids")),
            reason=str(payload.get("reason") or "llm_overflow_compression"),
        )
