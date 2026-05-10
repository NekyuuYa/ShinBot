"""Review reply decision stage runner."""

from __future__ import annotations

import json
from typing import Any, Protocol

from shinbot.agent.runners.review_models import ReplyDecisionStageOutput
from shinbot.agent.runners.review_reply.prompt_registration import REVIEW_REPLY_COMPONENT_IDS
from shinbot.agent.runners.templates import RunnerTemplateConfig, ToolCallPlanRunner
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.services.tools.schema import ToolCallRequest
from shinbot.agent.utils.parsing import (
    instance_id_from_session,
    int_list,
    json_schema_response_format,
    optional_int,
    parse_json_object,
)
from shinbot.agent.workflows.chat_actions import CHAT_ACTION_TOOL_TAG

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

_REPLY_RESPONSE_FORMAT = json_schema_response_format(
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

_REPLY_TASK_PROMPT = (
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


class ReplyDecisionStageRunner(Protocol):
    """Decide whether and how to reply from one candidate-local stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Run one reply_decision input and return the decision shell."""


class NoopReplyDecisionStageRunner:
    """No-op reply decision runner."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        candidate_ids = _candidate_message_ids_from_stage(stage_input)
        return ReplyDecisionStageOutput(target_message_ids=candidate_ids)


class LLMReplyDecisionStageRunner:
    """Run the reply-decision stage through the model runtime."""

    def __init__(
        self,
        model_runtime: Any,
        *,
        config: RunnerTemplateConfig | None = None,
        prompt_registry: PromptRegistry,
        tool_manager: Any | None = None,
        message_formatter: MessageFormatterService | None = None,
    ) -> None:
        routing = config or RunnerTemplateConfig()
        self._tool_manager = tool_manager
        self._prompt_registry = prompt_registry
        self._routing = routing
        self._template = ToolCallPlanRunner(
            model_runtime,
            prompt_registry=prompt_registry,
            config=RunnerTemplateConfig(
                caller=routing.caller,
                route_id=routing.route_id,
                model_id=routing.model_id,
                profile_id=routing.profile_id,
                system_prompt=routing.system_prompt,
                task_prompt=_REPLY_TASK_PROMPT,
                response_format=_REPLY_RESPONSE_FORMAT,
                component_ids_by_stage=routing.component_ids_by_stage,
                builtin_component_ids=REVIEW_REPLY_COMPONENT_IDS,
                message_format_config=routing.message_format_config,
                params=routing.params,
                max_model_retries=routing.max_model_retries,
                retry_backoff_seconds=routing.retry_backoff_seconds,
            ),
            tool_manager=tool_manager,
            tool_names=["no_reply", "send_reply", "send_poke"],
            repair_prompt=_REPLY_TOOLLESS_REPAIR_PROMPT,
            repair_reason="reply_decision_toolless_output",
            tool_transform=_review_reply_tool_schema,
            tool_tags={CHAT_ACTION_TOOL_TAG},
            message_formatter=message_formatter,
        )

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        plan = await self._template.run(stage_input)
        if plan.reason in ("tool_call_plan_build_failed", "tool_call_plan_llm_failed"):
            return ReplyDecisionStageOutput(reason="llm_reply_decision_failed")
        if plan.has_tool_calls:
            return await self._run_tool_decision(stage_input, plan)
        # No tool calls — try JSON fallback for the text.
        if plan.text:
            payload = parse_json_object(plan.text)
            if payload is not None:
                return ReplyDecisionStageOutput(
                    replied=bool(payload.get("replied")),
                    reply_message_id=optional_int(payload.get("reply_message_id")),
                    reply_message_ids=_reply_message_ids_from_payload(payload),
                    target_message_ids=int_list(payload.get("target_message_ids")),
                    reason=str(payload.get("reason") or "llm_reply_decision"),
                )
        candidate_ids = _candidate_message_ids_from_stage(stage_input)
        reason = "llm_reply_decision_toolless_after_repair" if plan.reason == "tool_call_plan_toolless_after_repair" else (plan.reason or "llm_reply_decision_failed")
        return ReplyDecisionStageOutput(
            target_message_ids=candidate_ids,
            reason=reason,
        )

    async def _run_tool_decision(
        self,
        stage_input: ReviewStageInput,
        plan: Any,
    ) -> ReplyDecisionStageOutput:
        if self._tool_manager is None:
            return ReplyDecisionStageOutput(
                reason="llm_reply_tool_call_skipped_no_tool_manager"
            )

        target_message_ids = _candidate_message_ids_from_stage(stage_input)
        parsed_calls = [
            _tool_call_function(tool_call) for tool_call in plan.tool_calls
        ]
        has_reply_call = any(
            tool_name == "send_reply" for tool_name, _ in parsed_calls
        )
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
                    caller=self._routing.caller,
                    instance_id=instance_id_from_session(stage_input.session_id),
                    session_id=stage_input.session_id,
                    run_id=str(plan.execution_id or ""),
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
                reason=_reply_tool_reason(
                    reply_count=reply_count, poke_count=poke_count
                ),
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
    return (
        f"send_reply_tool:{reply_count}" if reply_count != 1 else "send_reply_tool"
    )
