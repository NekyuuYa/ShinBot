"""Shared infrastructure for review stage LLM runners."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.context.review_context_builder import ReviewStageInput
from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.prompt_engine import (
    PromptBuildRequest,
    PromptContextPolicy,
    PromptInjection,
    PromptRegistry,
    PromptStage,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class ReviewLLMRunnerConfig:
    """Model routing and prompt configuration shared by review LLM runners."""

    caller: str = "agent.review"
    route_id: str | None = None
    model_id: str | None = None
    profile_id: str = ""
    component_ids_by_stage: dict[PromptStage, list[str]] = field(default_factory=dict)
    system_prompt: str = (
        "You are an internal ShinBot Agent review stage. Return only valid JSON "
        "matching the requested schema. Do not send user-visible replies."
    )
    params: dict[str, Any] = field(default_factory=dict)


class ReviewLLMStageRunnerBase:
    """Base for schema-constrained single-call review stage runners.

    Subclasses must set ``task_prompt``, ``response_format``, and
    ``builtin_component_ids`` as class attributes.
    """

    response_format: dict[str, Any]
    task_prompt: str
    builtin_component_ids: dict[PromptStage, list[str]] = {}

    def __init__(
        self,
        model_runtime: Any,
        *,
        config: ReviewLLMRunnerConfig | None = None,
        prompt_registry: PromptRegistry,
    ) -> None:
        if prompt_registry is None:
            raise ValueError("Review LLM stage runners require PromptRegistry")
        self._model_runtime = model_runtime
        self._config = config or ReviewLLMRunnerConfig()
        self._prompt_registry = prompt_registry

    async def _generate_payload(self, stage_input: ReviewStageInput) -> dict[str, Any] | None:
        result = await self._generate_result(stage_input)
        if result is None:
            return None
        return parse_json_object(result.text or "")

    async def _generate_result(self, stage_input: ReviewStageInput) -> Any | None:
        try:
            messages, tools, metadata = self._build_model_call_parts(stage_input)
        except Exception:
            logger.exception(
                "Review prompt build failed for stage %s session %s",
                stage_input.purpose,
                stage_input.session_id,
            )
            return None
        return await self._generate_with_parts(
            stage_input,
            messages=messages,
            tools=tools,
            metadata=metadata,
        )

    async def _generate_with_parts(
        self,
        stage_input: ReviewStageInput,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        metadata: dict[str, Any],
    ) -> Any | None:
        try:
            result = await self._model_runtime.generate(
                ModelRuntimeCall(
                    route_id=self._config.route_id,
                    model_id=self._config.model_id,
                    caller=self._config.caller,
                    session_id=stage_input.session_id,
                    instance_id=instance_id_from_session(stage_input.session_id),
                    purpose=stage_input.purpose,
                    messages=messages,
                    tools=tools,
                    response_format=self._response_format_for(stage_input, tools),
                    metadata=metadata,
                    params=dict(self._config.params),
                )
            )
        except ModelCallError:
            logger.exception(
                "Review LLM stage %s failed for session %s",
                stage_input.purpose,
                stage_input.session_id,
            )
            return None
        return result

    def _build_model_call_parts(
        self,
        stage_input: ReviewStageInput,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        fallback_metadata = {
            "review_stage": stage_input.purpose,
            **dict(stage_input.metadata),
        }
        component_ids_by_stage = self._component_ids_by_stage(stage_input)
        result = self._prompt_registry.build_messages(
            PromptBuildRequest(
                caller=self._config.caller,
                workflow_id="review",
                stage_id=stage_input.purpose,
                session_id=stage_input.session_id,
                instance_id=instance_id_from_session(stage_input.session_id),
                profile_id=self._config.profile_id,
                component_ids_by_stage=component_ids_by_stage,
                injections=self._build_prompt_injections(
                    stage_input,
                    component_ids_by_stage=component_ids_by_stage,
                ),
                context_policy=PromptContextPolicy.DISABLED,
                metadata=fallback_metadata,
            )
        )
        return result.messages, result.tools, dict(result.metadata)

    def _build_prompt_injections(
        self,
        stage_input: ReviewStageInput,
        *,
        component_ids_by_stage: dict[PromptStage, list[str]],
    ) -> list[PromptInjection]:
        injections: list[PromptInjection] = []
        if self._config.system_prompt and not component_ids_by_stage.get(
            PromptStage.SYSTEM_BASE
        ):
            injections.append(
                PromptInjection(
                    stage=PromptStage.SYSTEM_BASE,
                    component_id=f"review.{stage_input.purpose}.system",
                    text=self._config.system_prompt,
                    priority=10,
                )
            )
        injections.append(
            PromptInjection(
                stage=PromptStage.INSTRUCTIONS,
                component_id=f"review.{stage_input.purpose}.instruction",
                content_blocks=self._build_instruction_content(stage_input),
                priority=10,
                metadata={"review_stage": stage_input.purpose},
            )
        )
        return injections

    def _build_instruction_content(self, stage_input: ReviewStageInput) -> list[dict[str, Any]]:
        metadata_json = json.dumps(stage_input.metadata, ensure_ascii=False, sort_keys=True)
        instruction = (
            f"{self.task_prompt}\n\n"
            f"Stage purpose: {stage_input.purpose}\n"
            f"Metadata JSON: {metadata_json}"
        )
        content = [{"type": "text", "text": instruction}]
        if stage_input.instruction_content:
            content.extend(stage_input.instruction_content)
        else:
            content.append(
                {
                    "type": "text",
                    "text": "Source messages JSON:\n"
                    + json.dumps(stage_input.source_messages, ensure_ascii=False),
                }
            )
        return content

    def _response_format_for(
        self,
        stage_input: ReviewStageInput,
        tools: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        return self.response_format

    def _component_ids_by_stage(
        self,
        stage_input: ReviewStageInput,
    ) -> dict[PromptStage, list[str]]:
        result: dict[PromptStage, list[str]] = {
            stage: list(component_ids)
            for stage, component_ids in self._config.component_ids_by_stage.items()
        }
        for stage, component_ids in self.builtin_component_ids.items():
            registered_ids = [
                component_id
                for component_id in component_ids
                if self._prompt_registry.get_component(component_id) is not None
            ]
            if not registered_ids:
                continue
            result.setdefault(stage, [])
            result[stage].extend(
                component_id
                for component_id in registered_ids
                if component_id not in result[stage]
            )
        return result


def parse_json_object(text: str) -> dict[str, Any] | None:
    """Parse a JSON object, tolerating simple fenced-code responses."""

    candidate = text.strip()
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        candidate = "\n".join(lines).strip()
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def json_schema_response_format(
    name: str,
    properties: dict[str, Any],
    required: list[str],
) -> dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "schema": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": False,
            },
        },
    }


def instance_id_from_session(session_id: str) -> str:
    return session_id.split(":", 1)[0] if ":" in session_id else ""


def int_list(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    result: list[int] = []
    for item in value:
        item_int = optional_int(item)
        if item_int is not None:
            result.append(item_int)
    return result


def optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def optional_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def clamp_float(value: Any, *, minimum: float, maximum: float) -> float:
    parsed = optional_float(value)
    if parsed is None:
        return minimum
    return min(max(parsed, minimum), maximum)
