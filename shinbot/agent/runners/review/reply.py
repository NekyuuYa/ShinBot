"""Reply decision stage runner boundary."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.context.review_context_builder import ReviewStageInput
from shinbot.agent.coordinators.review.models import ReplyDecisionStageOutput


class ReplyDecisionStageRunner(Protocol):
    """Decide whether and how to reply from one candidate-local stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Run one reply_decision input and return the decision shell."""


class NoopReplyDecisionStageRunner:
    """Default reply_decision runner used before LLM reply logic is wired."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        candidate_ids = _candidate_message_ids(stage_input)
        return ReplyDecisionStageOutput(
            target_message_ids=candidate_ids,
        )


def _candidate_message_ids(stage_input: ReviewStageInput) -> list[int]:
    values = stage_input.metadata.get("candidate_message_ids")
    if isinstance(values, list):
        return [value for value in values if isinstance(value, int)]
    value = stage_input.metadata.get("candidate_message_id")
    if isinstance(value, int):
        return [value]
    return []


__all__ = ["NoopReplyDecisionStageRunner", "ReplyDecisionStageRunner"]
