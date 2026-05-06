"""Reply decision stage runner boundary."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.review.context.builder import ReviewStageInput
from shinbot.agent.review.models import ReplyDecisionStageOutput


class ReplyDecisionStageRunner(Protocol):
    """Decide whether and how to reply from one candidate-local stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Run one reply_decision input and return the decision shell."""


class NoopReplyDecisionStageRunner:
    """Default reply_decision runner used before LLM reply logic is wired."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        candidate_id = _candidate_message_id(stage_input)
        return ReplyDecisionStageOutput(
            target_message_ids=[candidate_id] if candidate_id is not None else [],
        )


def _candidate_message_id(stage_input: ReviewStageInput) -> int | None:
    value = stage_input.metadata.get("candidate_message_id")
    if isinstance(value, int):
        return value
    return None


__all__ = ["NoopReplyDecisionStageRunner", "ReplyDecisionStageRunner"]
