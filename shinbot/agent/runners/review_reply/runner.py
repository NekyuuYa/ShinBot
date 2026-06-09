"""Review reply decision stage runner."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.runners.review_models import ReplyDecisionStageOutput
from shinbot.agent.runners.review_reply.prompt_registration import REVIEW_REPLY_COMPONENT_IDS
from shinbot.agent.runners.templates import (
    RunnerTemplateConfig,
    ToolCallPlanRunner,
)
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

class ReplyDecisionStageRunner(Protocol):
    """Decide whether and how to reply from one candidate-local stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Run one reply_decision input and return the decision shell."""


class NoopReplyDecisionStageRunner:
    """No-op reply decision runner."""

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Return a no-op reply decision with candidate ids as targets.

        Args:
            stage_input: Review stage input (used to extract candidate ids).

        Returns:
            An output with no reply and a noop reason.
        """
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
        repair_component = prompt_registry.get_component(
            routing.special_prompt_ids.get("repair") or "review.reply_decision.repair"
        )
        self._template = ToolCallPlanRunner(
            model_runtime,
            prompt_registry=prompt_registry,
            config=RunnerTemplateConfig(
                caller=routing.caller,
                llm=routing.llm,
                default_llm=routing.default_llm,
                route_id=routing.route_id,
                model_id=routing.model_id,
                profile_id=routing.profile_id,
                response_format=_REPLY_RESPONSE_FORMAT,
                component_ids_by_stage=routing.component_ids_by_stage,
                builtin_component_ids=REVIEW_REPLY_COMPONENT_IDS,
                special_prompt_ids=dict(routing.special_prompt_ids),
                message_format_config=routing.message_format_config,
                params=routing.params,
                tool_config=routing.tool_config,
                max_model_retries=routing.max_model_retries,
                retry_backoff_seconds=routing.retry_backoff_seconds,
                instance_config_resolver=routing.instance_config_resolver,
                model_target_resolver=routing.model_target_resolver,
            ),
            tool_manager=tool_manager,
            tool_names=["no_reply", "send_reply", "send_poke", "send_reaction"],
            repair_prompt=repair_component.content if repair_component else "",
            repair_reason="reply_decision_toolless_output",
            tool_transform=_review_reply_tool_schema,
            tool_tags={CHAT_ACTION_TOOL_TAG},
            message_formatter=message_formatter,
        )

    async def run(self, stage_input: ReviewStageInput) -> ReplyDecisionStageOutput:
        """Run the LLM-based reply decision stage.

        Executes tool calls produced by the model (e.g. send_reply, no_reply)
        and returns a typed decision output.

        Args:
            stage_input: Review stage input with conversation context.

        Returns:
            A reply decision indicating whether a reply was sent, along with
            the target message ids and reason.
        """
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
        parsed_calls = self._template.parse_tool_calls(plan.tool_calls)
        has_reply_call = any(
            call.name == "send_reply" for call in parsed_calls
        )
        reaction_validation_error = _reaction_target_validation_error(
            parsed_calls,
            target_message_ids=target_message_ids,
        )
        if reaction_validation_error:
            return ReplyDecisionStageOutput(
                target_message_ids=target_message_ids,
                reason=reaction_validation_error,
            )
        quote_validation_error = _reply_quote_validation_error(
            stage_input,
            parsed_calls,
            target_message_ids=target_message_ids,
        )
        if quote_validation_error:
            return ReplyDecisionStageOutput(
                target_message_ids=target_message_ids,
                reason=quote_validation_error,
            )

        replied = False
        reply_message_id: int | None = None
        reply_message_ids: list[int] = []
        reply_count = 0
        poke_count = 0
        reaction_count = 0
        saw_no_reply = False
        run_id = str(plan.execution_id or "")
        for call_index, parsed_call in enumerate(parsed_calls):
            tool_name = parsed_call.name
            if tool_name not in {"send_reply", "no_reply", "send_poke", "send_reaction"}:
                continue
            if tool_name == "no_reply":
                saw_no_reply = True
                continue
            if tool_name == "send_poke" and not has_reply_call:
                continue
            call_arguments = dict(parsed_call.arguments)
            if tool_name == "send_reply" and run_id:
                call_arguments.setdefault(
                    "idempotency_key",
                    f"{run_id}:{call_index}",
                )
            tool_result = await self._tool_manager.execute(
                ToolCallRequest(
                    tool_name=tool_name,
                    arguments=call_arguments,
                    caller=self._routing.caller,
                    instance_id=instance_id_from_session(stage_input.session_id),
                    session_id=stage_input.session_id,
                    run_id=run_id,
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
            if tool_name == "send_reaction":
                reaction_count += 1
                continue
            poke_count += 1
        if replied or reaction_count:
            return ReplyDecisionStageOutput(
                replied=True,
                reply_message_id=reply_message_id,
                reply_message_ids=reply_message_ids,
                target_message_ids=target_message_ids,
                reason=_reply_tool_reason(
                    reply_count=reply_count,
                    poke_count=poke_count,
                    reaction_count=reaction_count,
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


def _reply_quote_validation_error(
    stage_input: ReviewStageInput,
    parsed_calls: list[Any],
    *,
    target_message_ids: list[int],
) -> str:
    target_message_id_set = set(target_message_ids)
    other_only_ids = _other_target_only_candidate_message_ids(stage_input)
    reply_index = 0
    for call in parsed_calls:
        if call.name != "send_reply":
            continue
        reply_index += 1
        quote_message_log_id = optional_int(call.arguments.get("quote_message_log_id"))
        if quote_message_log_id is None:
            if reply_index == 1:
                return "reply_tool_missing_quote_message_log_id"
            continue
        if quote_message_log_id not in target_message_id_set:
            return "reply_tool_quote_message_log_id_not_candidate"
        if quote_message_log_id in other_only_ids:
            return "reply_tool_quote_message_log_id_targets_other_only"
    return ""


def _reaction_target_validation_error(
    parsed_calls: list[Any],
    *,
    target_message_ids: list[int],
) -> str:
    target_message_id_set = set(target_message_ids)
    for call in parsed_calls:
        if call.name != "send_reaction":
            continue
        message_log_id = optional_int(
            call.arguments.get("message_log_id")
            or call.arguments.get("target_message_log_id")
            or call.arguments.get("quote_message_log_id")
        )
        if message_log_id is None:
            continue
        if message_log_id not in target_message_id_set:
            return "reaction_tool_message_log_id_not_candidate"
    return ""


def _other_target_only_candidate_message_ids(stage_input: ReviewStageInput) -> set[int]:
    values = stage_input.metadata.get("other_target_only_candidate_message_ids")
    if isinstance(values, list):
        return set(int_list(values))
    facts = stage_input.metadata.get("candidate_target_facts")
    if not isinstance(facts, list):
        return set()
    return {
        message_id
        for fact in facts
        if isinstance(fact, dict)
        if fact.get("targeted_to_other_only") is True
        if (message_id := optional_int(fact.get("message_id"))) is not None
    }


def _reply_message_ids_from_payload(payload: dict[str, Any]) -> list[int]:
    ids = int_list(payload.get("reply_message_ids"))
    if ids:
        return ids
    reply_message_id = optional_int(payload.get("reply_message_id"))
    return [reply_message_id] if reply_message_id is not None else []


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
    if function.get("name") == "send_reaction":
        return {
            **tool,
            "function": {
                **function,
                "description": (
                    str(function.get("description") or "")
                    + "\nReview reply requirement: send_reaction may be used as a "
                    "standalone lightweight visible response. Prefer message_log_id "
                    "and target one of the candidate message ids."
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


def _reply_tool_reason(*, reply_count: int, poke_count: int, reaction_count: int = 0) -> str:
    parts: list[str] = []
    if reply_count:
        include_reply_count = reply_count != 1 or poke_count > 0 or reaction_count > 0
        parts.append(
            f"send_reply_tool:{reply_count}" if include_reply_count else "send_reply_tool"
        )
    if poke_count:
        parts.append(f"send_poke_tool:{poke_count}")
    if reaction_count:
        parts.append(
            f"send_reaction_tool:{reaction_count}"
            if reaction_count != 1
            else "send_reaction_tool"
        )
    return ";".join(parts) or "reply_tool"
