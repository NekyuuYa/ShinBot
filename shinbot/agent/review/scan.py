"""Review scan stage runner boundary."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.review.context_builder import ReviewStageInput
from shinbot.agent.review.models import ReviewScanStageOutput


class ReviewScanStageRunner(Protocol):
    """Select candidate message ids from one review_scan stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        """Run one review_scan batch and return candidate ids."""


class NoopReviewScanStageRunner:
    """Default review_scan runner used before an LLM implementation is wired."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        return ReviewScanStageOutput()


__all__ = ["NoopReviewScanStageRunner", "ReviewScanStageRunner"]
