"""Shared plumbing for runner templates."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from shinbot.agent.runners.templates.config import RunnerTemplateConfig
from shinbot.agent.runtime.instance_config import (
    apply_instance_runtime_config_to_call,
    apply_instance_runtime_config_to_metadata,
    resolve_runtime_model_target,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.services.prompt_engine import (
    PromptBuildRequest,
    PromptContextPolicy,
    PromptInjection,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.utils.parsing import instance_id_from_session

logger = logging.getLogger(__name__)


class RunnerTemplateBase:
    """Common prompt assembly and model-call retry behavior for runner templates."""

    _log_name = "RunnerTemplate"

    def __init__(
        self,
        model_runtime: Any,
        *,
        prompt_registry: PromptRegistry,
        config: RunnerTemplateConfig,
        message_formatter: MessageFormatterService | None = None,
    ) -> None:
        if prompt_registry is None:
            raise ValueError(f"{self._log_name} requires PromptRegistry")
        self._model_runtime = model_runtime
        self._prompt_registry = prompt_registry
        self._config = config
        self._message_formatter = message_formatter

    def _build_model_call_parts(
        self,
        stage_input: ReviewStageInput,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        fallback_metadata = {
            "review_stage": stage_input.purpose,
            **dict(stage_input.metadata),
        }
        instance_id = instance_id_from_session(stage_input.session_id)
        instance_config = self._resolve_instance_config(instance_id)
        fallback_metadata = apply_instance_runtime_config_to_metadata(
            fallback_metadata,
            instance_config,
        )
        runtime_target = resolve_runtime_model_target(
            llm=self._config.llm,
            default_llm=self._config.default_llm,
            route_id=self._config.route_id,
            model_id=self._config.model_id,
            resolved=instance_config,
            model_target_resolver=self._config.model_target_resolver,
        )
        component_ids_by_stage = self._resolve_component_ids(stage_input)
        result = self._prompt_registry.build_messages(
            PromptBuildRequest(
                caller=self._config.caller,
                workflow_id="review",
                stage_id=stage_input.purpose,
                session_id=stage_input.session_id,
                instance_id=instance_id,
                route_id=(runtime_target.route_id or "") if runtime_target is not None else "",
                model_id=(runtime_target.model_id or "") if runtime_target is not None else "",
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
        return result.messages, dict(result.metadata)

    def _build_prompt_injections(
        self,
        stage_input: ReviewStageInput,
        *,
        component_ids_by_stage: dict[PromptStage, list[str]],
    ) -> list[PromptInjection]:
        injections: list[PromptInjection] = []
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

    def _build_instruction_content(
        self,
        stage_input: ReviewStageInput,
    ) -> list[dict[str, Any]]:
        metadata_json = json.dumps(
            stage_input.metadata, ensure_ascii=False, sort_keys=True
        )
        instruction = (
            f"Stage purpose: {stage_input.purpose}\n"
            f"Metadata JSON: {metadata_json}"
        )
        content: list[dict[str, Any]] = [{"type": "text", "text": instruction}]
        if stage_input.instruction_content:
            content.extend(stage_input.instruction_content)
            return content

        formatted_text = self._format_source_messages(stage_input)
        if formatted_text:
            content.append({"type": "text", "text": "Source messages:\n" + formatted_text})
            return content

        content.append(
            {
                "type": "text",
                "text": "Source messages JSON:\n"
                + json.dumps(stage_input.source_messages, ensure_ascii=False),
            }
        )
        return content

    def _format_source_messages(self, stage_input: ReviewStageInput) -> str:
        if self._message_formatter is None or not stage_input.source_messages:
            return ""
        try:
            return self._message_formatter.format_text(
                list(stage_input.source_messages),
                self._config.message_format_config,
            )
        except Exception:
            logger.exception(
                "%s message formatting failed for stage %s session %s",
                self._log_name,
                stage_input.purpose,
                stage_input.session_id,
            )
            return ""

    def _resolve_component_ids(
        self,
        stage_input: ReviewStageInput,
    ) -> dict[PromptStage, list[str]]:
        result: dict[PromptStage, list[str]] = {
            stage: list(ids)
            for stage, ids in self._config.component_ids_by_stage.items()
        }
        for stage, ids in self._config.builtin_component_ids.items():
            registered = [
                cid for cid in ids
                if self._prompt_registry.get_component(cid) is not None
            ]
            if not registered:
                continue
            result.setdefault(stage, [])
            result[stage].extend(
                cid for cid in registered if cid not in result[stage]
            )
        return result

    async def _generate_model(
        self,
        stage_input: ReviewStageInput,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        response_format: dict[str, Any] | None,
        metadata: dict[str, Any],
    ) -> Any | None:
        attempts = max(1, int(self._config.max_model_retries) + 1)
        instance_id = instance_id_from_session(stage_input.session_id)
        instance_config = self._resolve_instance_config(instance_id)
        for attempt in range(attempts):
            try:
                return await self._model_runtime.generate(
                    apply_instance_runtime_config_to_call(
                        ModelRuntimeCall(
                            route_id=self._config.route_id,
                            model_id=self._config.model_id,
                            caller=self._config.caller,
                            session_id=stage_input.session_id,
                            instance_id=instance_id,
                            purpose=stage_input.purpose,
                            messages=messages,
                            tools=tools,
                            response_format=response_format,
                            metadata=metadata,
                            params=dict(self._config.params),
                        ),
                        instance_config,
                        llm=self._config.llm,
                        default_llm=self._config.default_llm,
                        model_target_resolver=self._config.model_target_resolver,
                    )
                )
            except ModelCallError as exc:
                if attempt < attempts - 1 and _is_retryable_model_error(exc):
                    await asyncio.sleep(
                        max(0.0, self._config.retry_backoff_seconds) * (2 ** attempt)
                    )
                    continue
                logger.exception(
                    "%s LLM call failed for stage %s session %s",
                    self._log_name,
                    stage_input.purpose,
                    stage_input.session_id,
                )
                return None
        return None

    def _resolve_instance_config(self, instance_id: str) -> Any | None:
        resolver = self._config.instance_config_resolver
        if resolver is None or not instance_id:
            return None
        try:
            return resolver(instance_id)
        except Exception:
            logger.exception(
                "%s instance runtime config resolution failed for %s",
                self._log_name,
                instance_id,
            )
            return None


def _is_retryable_model_error(exc: ModelCallError) -> bool:
    text = str(exc).lower()
    return "429" in text or "rate limit" in text or "rate_limit" in text


__all__ = ["RunnerTemplateBase"]
