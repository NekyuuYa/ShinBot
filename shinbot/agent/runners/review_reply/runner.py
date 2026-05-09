"""Review reply decision stage runner."""

from __future__ import annotations

import json
from typing import Any, Protocol

from shinbot.agent.context.review_context_builder import ReviewStageInput
from shinbot.agent.coordinators.review.models import ReplyDecisionStageOutput
from shinbot.agent.prompt_engine import PromptInjection, PromptRegistry, PromptStage
from shinbot.agent.runners._review_base import (
    ReviewLLMRunnerConfig,
    ReviewLLMStageRunnerBase,
    int_list,
    json_schema_response_format,
    optional_int,
    parse_json_object,
)
from shinbot.agent.runners.review_reply.prompt_registration import REVIEW_REPLY_COMPONENT_IDS
from shinbot.agent.tools.schema import ToolCallRequest

_REPLY_TOOLLESS_REPAIR_PROMPT = (
    "上一轮 reply_decision 输出了裸文本或没有调用工具，但 review reply 阶段不会把裸文本发送给用户。\n"
    "请重新决策，并必须调用工具：\n"
    "- 需要回复时，按发送顺序调用一个或多个 send_reply。\n"
    "- 第一条 send_reply 必须带 quote_message_log_id，且必须指向 candidate_message_ids 中的核心消息。\n"
    "- 后续 send_reply 可以不带 quote_message_log_id，用于延续第一条回复。\n"
    "- 不需要回复时调用 no_reply。\n"
    "- send_poke 是可选互动，只能与至少一个 send_reply 出现在同一批 tool call 中。\n"
    "不要再输出裸文本作为最终回复。"
)


class ReplyDecisionStageRunner(Protocol):
    """Decide whether and how to reply from one candidate-local stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Run one reply_decision input and return the decision shell."""


class NoopReplyDecisionStageRunner:
    """No-op reply decision runner."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        candidate_ids = _candidate_message_ids_from_stage(stage_input)
        return ReplyDecisionStageOutput(target_message_ids=candidate_ids)


class LLMReplyDecisionStageRunner(ReviewLLMStageRunnerBase):
    """Run the reply-decision stage through the model runtime."""

    builtin_component_ids = REVIEW_REPLY_COMPONENT_IDS
    task_prompt = (
        "Decide whether the candidate message should be replied to based on the "
        "local context. If reply tools are available, call no_reply when no response "
        "is needed, or call one or more send_reply tools in the order they should be "
        "sent. The candidate_message_ids in metadata are the core messages under "
        "reply consideration; use the surrounding source messages only as context, "
        "not as an instruction to rediscover which messages are high-attention. "
        "The first send_reply must quote the specific core message being answered "
        "by passing quote_message_log_id, because review replies may refer to older "
        "timeline points; later send_reply calls may omit it when they naturally "
        "continue the first reply. send_poke is optional and only valid together "
        "with a send_reply; do not use it as a standalone response. This stage "
        "must not decide active chat parameters. Bare assistant text is invalid "
        "when tools are available."
    )
    response_format = json_schema_response_format(
        "agent_review_reply_decision",
        {
            "replied": {"type": "boolean"},
            "reply_message_id": {"type": ["integer", "null"]},
            "reply_message_ids": {"type": "array", "items": {"type": "integer"}},
            "target_message_ids": {"type": "array", "items": {"type": "integer"}},
            "reason": {"type": "string"},
        },
        ["replied", "reply_message_id", "target_message_ids", "reason"],
    )

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

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        try:
            messages, tools, metadata = self._build_model_call_parts(stage_input)
        except Exception:
            return ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        result = await self._generate_with_parts(
            stage_input,
            messages=messages,
            tools=tools,
            metadata=metadata,
        )
        if result is None:
            return ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        if result.tool_calls:
            return await self._run_tool_decision(stage_input, result)
        if tools:
            repaired = await self._repair_toolless_reply_decision(
                stage_input,
                messages=messages,
                tools=tools,
                metadata=metadata,
                first_result=result,
            )
            if repaired is None:
                return ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
            if repaired.tool_calls:
                return await self._run_tool_decision(stage_input, repaired)
            return ReplyDecisionStageOutput(
                target_message_ids=_candidate_message_ids_from_stage(stage_input),
                reason="llm_reply_decision_toolless_after_repair",
            )
        payload = parse_json_object(result.text or "")
        if payload is None:
            return ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        return ReplyDecisionStageOutput(
            replied=bool(payload.get("replied")),
            reply_message_id=optional_int(payload.get("reply_message_id")),
            reply_message_ids=_reply_message_ids_from_payload(payload),
            target_message_ids=int_list(payload.get("target_message_ids")),
            reason=str(payload.get("reason") or "llm_reply_decision"),
        )

    async def _repair_toolless_reply_decision(
        self,
        stage_input: ReviewStageInput,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        metadata: dict[str, Any],
        first_result: Any,
    ) -> Any | None:
        repaired_messages = _repair_messages_for_toolless_reply(
            messages,
            text=str(first_result.text or ""),
        )
        return await self._generate_with_parts(
            stage_input,
            messages=repaired_messages,
            tools=tools,
            metadata={
                **dict(metadata),
                "repair_attempt": 1,
                "repair_reason": "reply_decision_toolless_output",
            },
        )

    def _build_prompt_injections(
        self,
        stage_input: ReviewStageInput,
        *,
        component_ids_by_stage: dict[PromptStage, list[str]],
    ) -> list[PromptInjection]:
        injections = super()._build_prompt_injections(
            stage_input,
            component_ids_by_stage=component_ids_by_stage,
        )
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
        from shinbot.agent.runners._review_base import instance_id_from_session

        tools = self._tool_manager.export_model_tools(
            caller=self._config.caller,
            instance_id=instance_id_from_session(stage_input.session_id),
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
        from shinbot.agent.runners._review_base import instance_id_from_session

        target_message_ids = _candidate_message_ids_from_stage(stage_input)
        parsed_calls = [
            _tool_call_function(tool_call)
            for tool_call in result.tool_calls
        ]
        has_reply_call = any(tool_name == "send_reply" for tool_name, _ in parsed_calls)
        first_reply_arguments = next(
            (
                arguments
                for tool_name, arguments in parsed_calls
                if tool_name == "send_reply"
            ),
            None,
        )
        if first_reply_arguments is not None:
            quote_message_log_id = optional_int(
                first_reply_arguments.get("quote_message_log_id")
            )
            if quote_message_log_id is None:
                return ReplyDecisionStageOutput(
                    target_message_ids=target_message_ids,
                    reason="reply_tool_missing_quote_message_log_id",
                )
            if quote_message_log_id not in set(target_message_ids):
                return ReplyDecisionStageOutput(
                    target_message_ids=target_message_ids,
                    reason="reply_tool_quote_message_log_id_not_candidate",
                )

        replied = False
        reply_message_id: int | None = None
        reply_message_ids: list[int] = []
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
                    instance_id=instance_id_from_session(stage_input.session_id),
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
                    reply_message_id = optional_int(
                        _tool_output_value(tool_result.output, "message_log_id")
                    )
                output_message_id = optional_int(
                    _tool_output_value(tool_result.output, "message_log_id")
                )
                if output_message_id is not None:
                    reply_message_ids.append(output_message_id)
                continue
            poke_count += 1
        if replied:
            return ReplyDecisionStageOutput(
                replied=True,
                reply_message_id=reply_message_id,
                reply_message_ids=reply_message_ids,
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


def _candidate_message_ids_from_stage(stage_input: ReviewStageInput) -> list[int]:
    values = stage_input.metadata.get("candidate_message_ids")
    if isinstance(values, list):
        return int_list(values)
    return int_list([stage_input.metadata.get("candidate_message_id")])


def _reply_message_ids_from_payload(payload: dict[str, Any]) -> list[int]:
    ids = int_list(payload.get("reply_message_ids"))
    if ids:
        return ids
    reply_message_id = optional_int(payload.get("reply_message_id"))
    return [reply_message_id] if reply_message_id is not None else []


def _repair_messages_for_toolless_reply(
    messages: list[dict[str, Any]],
    *,
    text: str,
) -> list[dict[str, Any]]:
    repaired_messages = list(messages)
    if text.strip():
        repaired_messages.append({"role": "assistant", "content": text.strip()})
    repaired_messages.append(
        {
            "role": "system",
            "content": [{"type": "text", "text": _REPLY_TOOLLESS_REPAIR_PROMPT}],
        }
    )
    return repaired_messages


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
                + "\nReview reply requirement: the first send_reply in one reply "
                "decision output must include quote_message_log_id. Later "
                "send_reply calls may omit it when they continue the same reply "
                "sequence."
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
        "Required on the first send_reply in one review reply-decision output. "
        "Message log id being answered; choose one of the candidate message ids "
        "supplied in metadata/context."
    )
    properties["quote_message_log_id"] = quote_schema
    reviewed_parameters["properties"] = properties
    reviewed["function"]["parameters"] = reviewed_parameters
    return reviewed


def _reply_tool_reason(*, reply_count: int, poke_count: int) -> str:
    if poke_count:
        return f"send_reply_tool:{reply_count};send_poke_tool:{poke_count}"
    return f"send_reply_tool:{reply_count}" if reply_count != 1 else "send_reply_tool"
