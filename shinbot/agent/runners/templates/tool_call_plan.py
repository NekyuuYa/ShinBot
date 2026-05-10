"""Tool-call plan runner template for LLM stages that produce tool calls."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.runners.templates.base import RunnerTemplateBase
from shinbot.agent.runners.templates.config import RunnerTemplateConfig
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.utils.parsing import instance_id_from_session

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class ToolCallPlanResult:
    """Output from a ToolCallPlanRunner invocation.

    Contains the raw tool_calls list and metadata needed by the outer
    coordinator to execute tools and build the final stage output.
    """

    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    text: str = ""
    execution_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    reason: str = ""

    @property
    def has_tool_calls(self) -> bool:
        return bool(self.tool_calls)


class ToolCallPlanRunner(RunnerTemplateBase):
    """Template for review stages that produce tool calls for outer execution.

    Builds the prompt, calls the model with tool schemas, and returns the
    raw ``ToolCallPlanResult``.  The outer coordinator is responsible for
    executing the tools and assembling the final typed output.

    Supports an optional repair retry when the model returns bare text
    instead of tool calls.
    """

    def __init__(
        self,
        model_runtime: Any,
        *,
        prompt_registry: PromptRegistry,
        config: RunnerTemplateConfig,
        tool_manager: Any,
        tool_names: list[str],
        repair_prompt: str = "",
        repair_reason: str = "tool_call_plan_toolless",
        max_repair_attempts: int = 1,
        tool_transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        tool_tags: set[str] | None = None,
        message_formatter: MessageFormatterService | None = None,
    ) -> None:
        super().__init__(
            model_runtime,
            prompt_registry=prompt_registry,
            config=config,
            message_formatter=message_formatter,
        )
        self._log_name = "ToolCallPlanRunner"
        self._tool_manager = tool_manager
        self._tool_names = tool_names
        self._repair_prompt = repair_prompt
        self._repair_reason = repair_reason
        self._max_repair_attempts = max_repair_attempts
        self._tool_transform = tool_transform
        self._tool_tags = set(tool_tags) if tool_tags is not None else None

    async def run(self, stage_input: ReviewStageInput) -> ToolCallPlanResult:
        """Run one stage and return the tool-call plan result."""
        try:
            messages, metadata = self._build_model_call_parts(stage_input)
        except Exception:
            logger.exception(
                "ToolCallPlanRunner prompt build failed for stage %s session %s",
                stage_input.purpose,
                stage_input.session_id,
            )
            return ToolCallPlanResult(reason="tool_call_plan_build_failed")
        tools = self._build_tools(stage_input)
        result = await self._generate(
            stage_input, messages=messages, tools=tools, metadata=metadata,
        )
        if result is None:
            return ToolCallPlanResult(reason="tool_call_plan_llm_failed")
        if result.tool_calls:
            return ToolCallPlanResult(
                tool_calls=result.tool_calls,
                text=str(result.text or ""),
                execution_id=str(result.execution_id or ""),
                metadata=metadata,
                reason="tool_call_plan",
            )
        # Model returned bare text instead of tool calls — attempt repair.
        if tools and self._repair_prompt and self._max_repair_attempts > 0:
            repaired = await self._repair(
                stage_input,
                messages=messages,
                tools=tools,
                metadata=metadata,
                first_result=result,
            )
            if repaired is not None and repaired.tool_calls:
                return ToolCallPlanResult(
                    tool_calls=repaired.tool_calls,
                    text=str(repaired.text or ""),
                    execution_id=str(repaired.execution_id or ""),
                    metadata=metadata,
                    reason="tool_call_plan_after_repair",
                )
            return ToolCallPlanResult(
                text=str(result.text or ""),
                execution_id=str(result.execution_id or ""),
                metadata=metadata,
                reason="tool_call_plan_toolless_after_repair",
            )
        return ToolCallPlanResult(
            text=str(result.text or ""),
            execution_id=str(result.execution_id or ""),
            metadata=metadata,
            reason="tool_call_plan_toolless",
        )

    def build_tools(self, stage_input: ReviewStageInput) -> list[dict[str, Any]]:
        """Public accessor for the resolved tool schemas."""
        return self._build_tools(stage_input)

    # -- internal plumbing --

    def _build_tools(self, stage_input: ReviewStageInput) -> list[dict[str, Any]]:
        if self._tool_manager is None:
            return []
        tools = self._tool_manager.build_request_tools(
            self._tool_names,
            caller=self._config.caller,
            instance_id=instance_id_from_session(stage_input.session_id),
            session_id=stage_input.session_id,
            tags=self._tool_tags,
        )
        if self._tool_transform is not None:
            tools = [self._tool_transform(t) for t in tools]
        return tools

    async def _generate(
        self,
        stage_input: ReviewStageInput,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        metadata: dict[str, Any],
    ) -> Any | None:
        return await self._generate_model(
            stage_input,
            messages=messages,
            tools=tools,
            response_format=None if tools else self._config.response_format,
            metadata=metadata,
        )

    async def _repair(
        self,
        stage_input: ReviewStageInput,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        metadata: dict[str, Any],
        first_result: Any,
    ) -> Any | None:
        repaired_messages = list(messages)
        text = str(first_result.text or "").strip()
        if text:
            repaired_messages.append({"role": "assistant", "content": text})
        repaired_messages.append(
            {"role": "system", "content": [{"type": "text", "text": self._repair_prompt}]}
        )
        return await self._generate(
            stage_input,
            messages=repaired_messages,
            tools=tools,
            metadata={**metadata, "repair_attempt": 1, "repair_reason": self._repair_reason},
        )
