"""Active chat bootstrap stage runner boundary."""

from __future__ import annotations

from typing import Protocol

from shinbot.agent.review.context_builder import ReviewStageInput
from shinbot.agent.review.models import ActiveChatBootstrapStageOutput


class ActiveChatBootstrapStageRunner(Protocol):
    """Decide initial active chat state from review tail-history input."""

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        """Run active_chat_bootstrap and return initial active chat parameters."""


class NoopActiveChatBootstrapStageRunner:
    """Default bootstrap runner used before an LLM policy is wired."""

    def __init__(self, *, initial_interest: float) -> None:
        self._initial_interest = initial_interest

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        return ActiveChatBootstrapStageOutput(
            initial_interest=self._initial_interest,
            reason="noop_active_chat_bootstrap",
        )


__all__ = ["ActiveChatBootstrapStageRunner", "NoopActiveChatBootstrapStageRunner"]
