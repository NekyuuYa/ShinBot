"""Review active chat bootstrap stage runner."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.context.review_context_builder import ReviewStageInput
from shinbot.agent.coordinators.review.models import ActiveChatBootstrapStageOutput
from shinbot.agent.runners._review_base import (
    ReviewLLMStageRunnerBase,
    json_schema_response_format,
)
from shinbot.agent.runners.review_bootstrap.prompt_registration import (
    REVIEW_BOOTSTRAP_COMPONENT_IDS,
)
from shinbot.agent.scheduler.models import ActiveChatDisposition


class ActiveChatBootstrapStageRunner(Protocol):
    """Decide initial active chat state from review tail-history input."""

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        """Run one active_chat_bootstrap input."""


class NoopActiveChatBootstrapStageRunner:
    """No-op bootstrap runner."""

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        return ActiveChatBootstrapStageOutput(reason="noop_active_chat_bootstrap")


class LLMActiveChatBootstrapStageRunner(ReviewLLMStageRunnerBase):
    """Choose active-chat bootstrap parameters through the model runtime."""

    builtin_component_ids = REVIEW_BOOTSTRAP_COMPONENT_IDS
    task_prompt = (
        "Choose the active chat disposition after review and reply-decision stages. "
        "Return only the semantic disposition; numeric interest and decay parameters "
        "are controlled by ShinBot internals."
    )
    response_format = json_schema_response_format(
        "agent_review_active_chat_bootstrap",
        {
            "disposition": {
                "type": "string",
                "enum": [item.value for item in ActiveChatDisposition],
            },
            "reason": {"type": "string"},
        },
        ["disposition", "reason"],
    )

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        payload = await self._generate_payload(stage_input)
        if payload is None:
            return ActiveChatBootstrapStageOutput(
                reason="llm_active_chat_bootstrap_failed",
            )
        disposition = _active_chat_disposition(payload.get("disposition"))
        if disposition is None:
            return ActiveChatBootstrapStageOutput(
                reason="llm_active_chat_bootstrap_invalid_disposition",
            )
        return ActiveChatBootstrapStageOutput(
            disposition=disposition,
            reason=str(payload.get("reason") or "llm_active_chat_bootstrap"),
        )


def _active_chat_disposition(value: Any) -> ActiveChatDisposition | None:
    try:
        return ActiveChatDisposition(str(value))
    except ValueError:
        return None
