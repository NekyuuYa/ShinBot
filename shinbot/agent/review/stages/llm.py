"""LLM-backed runners for Agent review workflow stages."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.prompt_manager import (
    PromptBuildRequest,
    PromptContextPolicy,
    PromptInjection,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.review.context.builder import ReviewStageInput
from shinbot.agent.review.models import (
    ActiveChatBootstrapStageOutput,
    OverflowCompressionStageOutput,
    ReplyDecisionStageOutput,
    ReviewScanStageOutput,
)
from shinbot.agent.review.prompt_registration import REVIEW_PROMPT_COMPONENT_IDS_BY_STAGE
from shinbot.agent.tools.schema import ToolCallRequest

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
    """Small helper for schema-constrained review stage model calls."""

    response_format: dict[str, Any]
    task_prompt: str

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
        try:
            result = await self._model_runtime.generate(
                ModelRuntimeCall(
                    route_id=self._config.route_id,
                    model_id=self._config.model_id,
                    caller=self._config.caller,
                    session_id=stage_input.session_id,
                    instance_id=_instance_id_from_session(stage_input.session_id),
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
        result = self._prompt_registry.build_messages(
            PromptBuildRequest(
                caller=self._config.caller,
                workflow_id="review",
                stage_id=stage_input.purpose,
                session_id=stage_input.session_id,
                instance_id=_instance_id_from_session(stage_input.session_id),
                profile_id=self._config.profile_id,
                component_ids_by_stage=self._component_ids_by_stage(stage_input),
                injections=self._build_prompt_injections(stage_input),
                context_policy=PromptContextPolicy.DISABLED,
                metadata=fallback_metadata,
            )
        )
        return result.messages, result.tools, dict(result.metadata)

    def _build_prompt_injections(self, stage_input: ReviewStageInput) -> list[PromptInjection]:
        injections: list[PromptInjection] = []
        if self._config.system_prompt:
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
        for stage, component_ids in REVIEW_PROMPT_COMPONENT_IDS_BY_STAGE.get(
            stage_input.purpose,
            {},
        ).items():
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

    def __init__(
        self,
        model_runtime: Any,
        *,
        config: ReviewLLMRunnerConfig | None = None,
        prompt_registry: PromptRegistry,
        tool_manager: Any | None = None,
    ) -> None:
        super().__init__(
            model_runtime,
            config=config,
            prompt_registry=prompt_registry,
        )
        self._tool_manager = tool_manager

    task_prompt = (
        "Decide whether the candidate message should be replied to based on the "
        "local context. If reply tools are available, call no_reply when no response "
        "is needed, or call one or more send_reply tools in the order they should be "
        "sent. When calling send_reply, quote the specific message being answered by "
        "passing quote_message_log_id, because review replies may refer to older "
        "timeline points. send_poke is optional and only valid together with a "
        "send_reply; do not use it as a standalone response. This stage must not "
        "decide active chat parameters."
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
        result = await self._generate_result(stage_input)
        if result is None:
            return ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        if result.tool_calls:
            return await self._run_tool_decision(stage_input, result)
        payload = parse_json_object(result.text or "")
        if payload is None:
            return ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        return ReplyDecisionStageOutput(
            replied=bool(payload.get("replied")),
            reply_message_id=_optional_int(payload.get("reply_message_id")),
            target_message_ids=_int_list(payload.get("target_message_ids")),
            reason=str(payload.get("reason") or "llm_reply_decision"),
        )

    def _build_prompt_injections(self, stage_input: ReviewStageInput) -> list[PromptInjection]:
        injections = super()._build_prompt_injections(stage_input)
        tools = self._reply_decision_tools(stage_input)
        if tools:
            injections.append(
                PromptInjection(
                    stage=PromptStage.ABILITIES,
                    component_id="review.reply_decision.terminal_tools",
                    tools=tools,
                    priority=10,
                    metadata={"review_stage": stage_input.purpose},
                )
            )
        return injections

    def _response_format_for(
        self,
        stage_input: ReviewStageInput,
        tools: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if tools:
            return None
        return self.response_format

    def _reply_decision_tools(self, stage_input: ReviewStageInput) -> list[dict[str, Any]]:
        if self._tool_manager is None:
            return []
        tools = self._tool_manager.export_model_tools(
            caller=self._config.caller,
            instance_id=_instance_id_from_session(stage_input.session_id),
            session_id=stage_input.session_id,
            tags={"attention"},
        )
        return [
            _review_reply_tool_schema(tool)
            for tool in tools
            if tool.get("function", {}).get("name")
            in {"send_reply", "no_reply", "send_poke"}
        ]

    async def _run_tool_decision(
        self,
        stage_input: ReviewStageInput,
        result: Any,
    ) -> ReplyDecisionStageOutput:
        if self._tool_manager is None:
            return ReplyDecisionStageOutput(reason="llm_reply_tool_call_skipped_no_tool_manager")
        target_message_ids = _candidate_message_ids_from_stage(stage_input)
        parsed_calls = [
            _tool_call_function(tool_call)
            for tool_call in result.tool_calls
        ]
        has_reply_call = any(tool_name == "send_reply" for tool_name, _ in parsed_calls)
        for tool_name, arguments in parsed_calls:
            if tool_name == "send_reply" and _optional_int(
                arguments.get("quote_message_log_id")
            ) is None:
                return ReplyDecisionStageOutput(
                    target_message_ids=target_message_ids,
                    reason="reply_tool_missing_quote_message_log_id",
                )

        replied = False
        reply_message_id: int | None = None
        reply_count = 0
        poke_count = 0
        saw_no_reply = False
        for tool_name, arguments in parsed_calls:
            if tool_name not in {"send_reply", "no_reply", "send_poke"}:
                continue
            if tool_name == "no_reply":
                saw_no_reply = True
                continue
            if tool_name == "send_poke" and not has_reply_call:
                continue
            tool_result = await self._tool_manager.execute(
                ToolCallRequest(
                    tool_name=tool_name,
                    arguments=arguments,
                    caller=self._config.caller,
                    instance_id=_instance_id_from_session(stage_input.session_id),
                    session_id=stage_input.session_id,
                    run_id=str(result.execution_id or ""),
                    metadata={
                        "workflow_id": "review",
                        "stage_id": stage_input.purpose,
                        "candidate_message_ids": target_message_ids,
                    },
                )
            )
            if not tool_result.success:
                return ReplyDecisionStageOutput(
                    target_message_ids=target_message_ids,
                    reason=f"reply_tool_failed:{tool_result.error_code}",
                )
            if tool_name == "send_reply":
                replied = True
                reply_count += 1
                if reply_message_id is None:
                    reply_message_id = _optional_int(
                        _tool_output_value(tool_result.output, "message_log_id")
                    )
                continue
            poke_count += 1
        if replied:
            return ReplyDecisionStageOutput(
                replied=True,
                reply_message_id=reply_message_id,
                target_message_ids=target_message_ids,
                reason=_reply_tool_reason(reply_count=reply_count, poke_count=poke_count),
            )
        if saw_no_reply:
            return ReplyDecisionStageOutput(
                replied=False,
                target_message_ids=target_message_ids,
                reason="no_reply_tool",
            )
        return ReplyDecisionStageOutput(
            target_message_ids=target_message_ids,
            reason="llm_reply_decision_no_terminal_tool",
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


def _candidate_message_ids_from_stage(stage_input: ReviewStageInput) -> list[int]:
    values = stage_input.metadata.get("candidate_message_ids")
    if isinstance(values, list):
        return _int_list(values)
    return _int_list([stage_input.metadata.get("candidate_message_id")])


def _tool_call_function(tool_call: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    function = tool_call.get("function") if isinstance(tool_call, dict) else None
    if not isinstance(function, dict):
        return "", {}
    arguments = function.get("arguments", {})
    if isinstance(arguments, str):
        try:
            parsed_arguments = json.loads(arguments)
        except json.JSONDecodeError:
            parsed_arguments = {}
    elif isinstance(arguments, dict):
        parsed_arguments = dict(arguments)
    else:
        parsed_arguments = {}
    return str(function.get("name") or ""), parsed_arguments


def _tool_output_value(output: Any, key: str) -> Any:
    if isinstance(output, dict):
        return output.get(key)
    return None


def _review_reply_tool_schema(tool: dict[str, Any]) -> dict[str, Any]:
    function = tool.get("function")
    if not isinstance(function, dict):
        return tool
    if function.get("name") == "send_poke":
        return {
            **tool,
            "function": {
                **function,
                "description": (
                    str(function.get("description") or "")
                    + "\nReview reply requirement: send_poke is optional and only "
                    "takes effect after at least one send_reply in the same reply "
                    "decision output. Never use it as the only response."
                ),
            },
        }
    if function.get("name") != "send_reply":
        return tool
    reviewed = {
        **tool,
        "function": {
            **function,
            "description": (
                str(function.get("description") or "")
                + "\nReview reply requirement: quote_message_log_id is required. "
                "Use one of the candidate message ids from the review context."
            ),
        },
    }
    parameters = reviewed["function"].get("parameters")
    if not isinstance(parameters, dict):
        return reviewed
    reviewed_parameters = dict(parameters)
    properties = dict(reviewed_parameters.get("properties") or {})
    quote_schema = dict(properties.get("quote_message_log_id") or {})
    quote_schema["description"] = (
        "Required for review replies. Message log id being answered; choose one "
        "of the candidate message ids supplied in metadata/context."
    )
    properties["quote_message_log_id"] = quote_schema
    required = list(reviewed_parameters.get("required") or [])
    if "quote_message_log_id" not in required:
        required.append("quote_message_log_id")
    reviewed_parameters["properties"] = properties
    reviewed_parameters["required"] = required
    reviewed["function"]["parameters"] = reviewed_parameters
    return reviewed


def _reply_tool_reason(*, reply_count: int, poke_count: int) -> str:
    if poke_count:
        return f"send_reply_tool:{reply_count};send_poke_tool:{poke_count}"
    return f"send_reply_tool:{reply_count}" if reply_count != 1 else "send_reply_tool"


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
