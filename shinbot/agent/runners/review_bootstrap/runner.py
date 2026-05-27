"""Review active chat bootstrap stage runner."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.runners.review_bootstrap.prompt_registration import (
    REVIEW_BOOTSTRAP_COMPONENT_IDS,
)
from shinbot.agent.runners.review_models import ActiveChatBootstrapStageOutput
from shinbot.agent.runners.templates import RunnerTemplateConfig, StructuredOutputRunner
from shinbot.agent.scheduler.models import ActiveChatDisposition
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.utils.parsing import json_schema_response_format

_BOOTSTRAP_RESPONSE_FORMAT = json_schema_response_format(
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

class ActiveChatBootstrapStageRunner(Protocol):
    """Decide initial active chat state from review tail-history input."""

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        """Run one active_chat_bootstrap input."""


class NoopActiveChatBootstrapStageRunner:
    """No-op bootstrap runner."""

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        """Return a no-op bootstrap output with no disposition.

        Args:
            stage_input: Review stage input (ignored by the no-op runner).

        Returns:
            An output with no disposition and a noop reason.
        """
        return ActiveChatBootstrapStageOutput(reason="noop_active_chat_bootstrap")


class LLMActiveChatBootstrapStageRunner:
    """Choose active-chat bootstrap parameters through the model runtime."""

    def __init__(
        self,
        model_runtime: Any,
        *,
        config: RunnerTemplateConfig | None = None,
        prompt_registry: PromptRegistry,
        message_formatter: MessageFormatterService | None = None,
    ) -> None:
        routing = config or RunnerTemplateConfig()
        self._template = StructuredOutputRunner(
            model_runtime,
            prompt_registry=prompt_registry,
            config=RunnerTemplateConfig(
                caller=routing.caller,
                llm=routing.llm,
                default_llm=routing.default_llm,
                route_id=routing.route_id,
                model_id=routing.model_id,
                profile_id=routing.profile_id,
                response_format=_BOOTSTRAP_RESPONSE_FORMAT,
                component_ids_by_stage=routing.component_ids_by_stage,
                builtin_component_ids=REVIEW_BOOTSTRAP_COMPONENT_IDS,
                message_format_config=routing.message_format_config,
                params=routing.params,
                tool_config=routing.tool_config,
                max_model_retries=routing.max_model_retries,
                retry_backoff_seconds=routing.retry_backoff_seconds,
                instance_config_resolver=routing.instance_config_resolver,
                model_target_resolver=routing.model_target_resolver,
            ),
            message_formatter=message_formatter,
        )

    @property
    def _config(self) -> RunnerTemplateConfig:
        return self._template._config

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        """Run the LLM-based active chat bootstrap and choose a disposition.

        Args:
            stage_input: Review stage input with conversation tail context.

        Returns:
            An output with the chosen active chat disposition, or a failed
            output if the model response is invalid.
        """
        payload = await self._template.run(stage_input)
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
