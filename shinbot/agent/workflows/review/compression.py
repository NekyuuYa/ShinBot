"""Overflow compression stage runner boundary."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.context.review_context_builder import ReviewStageInput
from shinbot.agent.review.models import OverflowCompressionStageOutput


class OverflowCompressionStageRunner(Protocol):
    """Compress old overflow unread messages before tail review_scan."""

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        """Run one overflow compression chunk."""


class NoopOverflowCompressionStageRunner:
    """Default compression runner used before an LLM implementation is wired."""

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        return OverflowCompressionStageOutput()


__all__ = ["NoopOverflowCompressionStageRunner", "OverflowCompressionStageRunner"]
