"""Review scan stage runner — selects candidate message ids."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.context.review_context_builder import ReviewStageInput
from shinbot.agent.coordinators.review.models import ReviewScanStageOutput
from shinbot.agent.runners._review_base import (
    ReviewLLMStageRunnerBase,
    int_list,
    json_schema_response_format,
)
from shinbot.agent.runners.review_scan.prompt_registration import REVIEW_SCAN_COMPONENT_IDS


class ReviewScanStageRunner(Protocol):
    """Select candidate message ids from one review_scan stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        """Run one review_scan batch and return candidate ids."""


class NoopReviewScanStageRunner:
    """No-op scan runner that returns empty candidates."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        return ReviewScanStageOutput(reason="noop_review_scan")


class LLMReviewScanStageRunner(ReviewLLMStageRunnerBase):
    """Select reply-worthy candidate message ids through the model runtime."""

    builtin_component_ids = REVIEW_SCAN_COMPONENT_IDS
    task_prompt = (
        "Review the supplied unread messages and select message_log ids that may "
        "deserve a reply or closer local-context decision. Do not decide active chat state."
    )
    response_format = json_schema_response_format(
        "agent_review_scan",
        {
            "candidate_message_ids": {"type": "array", "items": {"type": "integer"}},
            "reason": {"type": "string"},
        },
        ["candidate_message_ids", "reason"],
    )

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        payload = await self._generate_payload(stage_input)
        if payload is None:
            return ReviewScanStageOutput(reason="llm_review_scan_failed")
        return ReviewScanStageOutput(
            candidate_message_ids=int_list(payload.get("candidate_message_ids")),
            reason=str(payload.get("reason") or "llm_review_scan"),
        )
