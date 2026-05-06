"""LLM-backed runners for Agent review workflow stages."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.review.context_builder import ReviewStageInput
from shinbot.agent.review.models import (
    ActiveChatBootstrapStageOutput,
    OverflowCompressionStageOutput,
    ReplyDecisionStageOutput,
    ReviewScanStageOutput,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class ReviewLLMRunnerConfig:
    """Model routing and prompt configuration shared by review LLM runners."""

    caller: str = "agent.review"
    route_id: str | None = None
    model_id: str | None = None
    system_prompt: str = (
        "You are an internal ShinBot Agent review stage. Return only valid JSON "
        "matching the requested schema. Do not send user-visible replies."
    )
    params: dict[str, Any] = field(default_factory=dict)


class ReviewLLMStageRunnerBase:
    """Small helper for schema-constrained review stage model calls."""

    response_format: dict[str, Any]
    task_prompt: str

    def __init__(
        self,
        model_runtime: Any,
        *,
        config: ReviewLLMRunnerConfig | None = None,
    ) -> None:
        self._model_runtime = model_runtime
        self._config = config or ReviewLLMRunnerConfig()

    async def _generate_payload(self, stage_input: ReviewStageInput) -> dict[str, Any] | None:
        try:
            result = await self._model_runtime.generate(
                ModelRuntimeCall(
                    route_id=self._config.route_id,
                    model_id=self._config.model_id,
                    caller=self._config.caller,
                    session_id=stage_input.session_id,
                    instance_id=_instance_id_from_session(stage_input.session_id),
                    purpose=stage_input.purpose,
                    messages=self._build_messages(stage_input),
                    response_format=self.response_format,
                    metadata={
                        "review_stage": stage_input.purpose,
                        **dict(stage_input.metadata),
                    },
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
        return parse_json_object(result.text or "")

    def _build_messages(self, stage_input: ReviewStageInput) -> list[dict[str, Any]]:
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
        return [
            {"role": "system", "content": self._config.system_prompt},
            {"role": "user", "content": content},
        ]


def _json_schema_response_format(
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


class LLMOverflowCompressionStageRunner(ReviewLLMStageRunnerBase):
    """Compress old overflow unread messages through the model runtime."""

    task_prompt = (
        "Compress the supplied older unread messages for later review. Keep only "
        "useful context, notable unresolved topics, and message ids worth closer reply review."
    )
    response_format = _json_schema_response_format(
        "agent_review_overflow_compression",
        {
            "summary": {"type": "string"},
            "candidate_message_ids": {"type": "array", "items": {"type": "integer"}},
            "reason": {"type": "string"},
        },
        ["summary", "candidate_message_ids", "reason"],
    )

    async def run(self, stage_input: ReviewStageInput) -> OverflowCompressionStageOutput:
        payload = await self._generate_payload(stage_input)
        if payload is None:
            return OverflowCompressionStageOutput(reason="llm_overflow_compression_failed")
        return OverflowCompressionStageOutput(
            summary=str(payload.get("summary") or ""),
            candidate_message_ids=_int_list(payload.get("candidate_message_ids")),
            reason=str(payload.get("reason") or "llm_overflow_compression"),
        )


class LLMReviewScanStageRunner(ReviewLLMStageRunnerBase):
    """Select reply-worthy candidate message ids through the model runtime."""

    task_prompt = (
        "Review the supplied unread messages and select message_log ids that may "
        "deserve a reply or closer local-context decision. Do not decide active chat state."
    )
    response_format = _json_schema_response_format(
        "agent_review_scan",
        {
            "candidate_message_ids": {"type": "array", "items": {"type": "integer"}},
            "reason": {"type": "string"},
        },
        ["candidate_message_ids", "reason"],
    )

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        payload = await self._generate_payload(stage_input)
        if payload is None:
            return ReviewScanStageOutput(reason="llm_review_scan_failed")
        return ReviewScanStageOutput(
            candidate_message_ids=_int_list(payload.get("candidate_message_ids")),
            reason=str(payload.get("reason") or "llm_review_scan"),
        )


class LLMReplyDecisionStageRunner(ReviewLLMStageRunnerBase):
    """Run the reply-decision stage through the model runtime."""

    task_prompt = (
        "Decide whether the candidate message should be replied to based on the "
        "local context. This stage may identify targets, but must not decide active chat parameters."
    )
    response_format = _json_schema_response_format(
        "agent_review_reply_decision",
        {
            "replied": {"type": "boolean"},
            "reply_message_id": {"type": ["integer", "null"]},
            "target_message_ids": {"type": "array", "items": {"type": "integer"}},
            "reason": {"type": "string"},
        },
        ["replied", "reply_message_id", "target_message_ids", "reason"],
    )

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        payload = await self._generate_payload(stage_input)
        if payload is None:
            return ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        return ReplyDecisionStageOutput(
            replied=bool(payload.get("replied")),
            reply_message_id=_optional_int(payload.get("reply_message_id")),
            target_message_ids=_int_list(payload.get("target_message_ids")),
            reason=str(payload.get("reason") or "llm_reply_decision"),
        )


class LLMActiveChatBootstrapStageRunner(ReviewLLMStageRunnerBase):
    """Choose active-chat bootstrap parameters through the model runtime."""

    task_prompt = (
        "Choose the initial active chat interest after review and reply-decision stages. "
        "Use a low value for weak observation, higher values for likely continued participation."
    )
    response_format = _json_schema_response_format(
        "agent_review_active_chat_bootstrap",
        {
            "initial_interest": {"type": "number", "minimum": 0, "maximum": 1},
            "decay_half_life_seconds": {"type": ["number", "null"], "minimum": 0},
            "reason": {"type": "string"},
        },
        ["initial_interest", "decay_half_life_seconds", "reason"],
    )

    async def run(self, stage_input: ReviewStageInput) -> ActiveChatBootstrapStageOutput:
        payload = await self._generate_payload(stage_input)
        if payload is None:
            return ActiveChatBootstrapStageOutput(
                initial_interest=0.05,
                reason="llm_active_chat_bootstrap_failed",
            )
        return ActiveChatBootstrapStageOutput(
            initial_interest=_clamp_float(payload.get("initial_interest"), minimum=0.0, maximum=1.0),
            decay_half_life_seconds=_optional_float(payload.get("decay_half_life_seconds")),
            reason=str(payload.get("reason") or "llm_active_chat_bootstrap"),
        )


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


def _instance_id_from_session(session_id: str) -> str:
    return session_id.split(":", 1)[0] if ":" in session_id else ""


def _int_list(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    result: list[int] = []
    for item in value:
        item_int = _optional_int(item)
        if item_int is not None:
            result.append(item_int)
    return result


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _optional_float(value: Any) -> float | None:
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


def _clamp_float(value: Any, *, minimum: float, maximum: float) -> float:
    parsed = _optional_float(value)
    if parsed is None:
        return minimum
    return min(max(parsed, minimum), maximum)


__all__ = [
    "LLMActiveChatBootstrapStageRunner",
    "LLMOverflowCompressionStageRunner",
    "LLMReplyDecisionStageRunner",
    "LLMReviewScanStageRunner",
    "ReviewLLMRunnerConfig",
    "ReviewLLMStageRunnerBase",
    "parse_json_object",
]
