"""Tool execution helpers for conversation workflows."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.tools.schema import ToolCallRequest

_TERMINAL_TOOL_NAMES = {"no_reply", "send_reply", "send_poke"}


@dataclass(slots=True)
class WorkflowToolLoopResult:
    """Aggregated tool execution output for one model iteration."""

    tool_messages: list[dict[str, Any]] = field(default_factory=list)
    tool_calls_log: list[dict[str, Any]] = field(default_factory=list)
    no_reply: bool = False
    reply_sent: bool = False
    terminal_action: str = ""
    finish_reason: str = ""
    invalid_reason: str = ""
    internal_summary: str = ""
    response_summary: str = ""


async def execute_workflow_tool_calls(
    tool_calls: list[dict[str, Any]],
    *,
    tool_manager: Any,
    instance_id: str,
    session_id: str,
    run_id: str,
) -> WorkflowToolLoopResult:
    """Execute one batch of tool calls and normalize outputs for the model."""

    outcome = WorkflowToolLoopResult()
    parsed_calls = [_parse_tool_call(tool_call) for tool_call in tool_calls]
    outcome.tool_calls_log.extend(
        {"name": tool_name, "arguments": tool_args}
        for _, tool_name, tool_args in parsed_calls
    )

    terminal_candidates = [
        tool_name
        for _, tool_name, tool_args in parsed_calls
        if _is_terminal_tool_call(tool_name, tool_args)
    ]
    if terminal_candidates and len(parsed_calls) > 1:
        outcome.invalid_reason = "terminal_conflict"
        outcome.finish_reason = outcome.invalid_reason
        error_str = json.dumps(
            {
                "error": (
                    "Terminal workflow tools must be called alone. "
                    "Call ordinary tools in one step, then call exactly one of "
                    "no_reply, send_reply, or send_poke to finish."
                )
            },
            ensure_ascii=False,
        )
        outcome.tool_messages.extend(
            {
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": error_str,
            }
            for tool_call_id, _, _ in parsed_calls
        )
        return outcome

    for tool_call_id, tool_name, tool_args in parsed_calls:
        tool_result = await tool_manager.execute(
            ToolCallRequest(
                tool_name=tool_name,
                arguments=tool_args,
                caller="attention.workflow_runner",
                instance_id=instance_id,
                session_id=session_id,
                run_id=run_id,
            )
        )

        if tool_result.success:
            output_str = json.dumps(tool_result.output, ensure_ascii=False)
        else:
            output_str = json.dumps(
                {"error": tool_result.error_message},
                ensure_ascii=False,
            )

        if tool_name == "no_reply" and tool_result.success:
            outcome.no_reply = True
            outcome.internal_summary = str(tool_args.get("internal_summary", "") or "")
            outcome.terminal_action = "no_reply"
            outcome.finish_reason = "no_reply"
        elif tool_name == "send_reply" and tool_result.success:
            outcome.reply_sent = True
            outcome.response_summary = str(tool_args.get("text", "") or "")[:200]
            if _tool_result_terminates(tool_result.output):
                outcome.terminal_action = "send_reply"
                outcome.finish_reason = "send_reply"
        elif tool_name == "send_poke" and tool_result.success:
            outcome.reply_sent = True
            outcome.response_summary = "戳一戳"
            if _tool_result_terminates(tool_result.output):
                outcome.terminal_action = "send_poke"
                outcome.finish_reason = "send_poke"
        elif tool_name in _TERMINAL_TOOL_NAMES and not tool_result.success:
            outcome.invalid_reason = "terminal_tool_failed"
            outcome.finish_reason = outcome.invalid_reason

        outcome.tool_messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": output_str,
            }
        )

    return outcome


def _parse_tool_call(tool_call: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    tool_call_id = str(tool_call.get("id", "") or "")
    func = tool_call.get("function", {}) if isinstance(tool_call, dict) else {}
    tool_name = str(func.get("name", "") or "")
    tool_args_raw = func.get("arguments", "{}")
    try:
        tool_args = (
            json.loads(tool_args_raw)
            if isinstance(tool_args_raw, str)
            else dict(tool_args_raw or {})
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        tool_args = {}
    return tool_call_id, tool_name, tool_args


def _is_terminal_tool_call(tool_name: str, tool_args: dict[str, Any]) -> bool:
    if tool_name == "no_reply":
        return True
    if tool_name in {"send_reply", "send_poke"}:
        return bool(tool_args.get("terminate_round", True))
    return False


def _tool_result_terminates(output: Any) -> bool:
    if isinstance(output, dict):
        return bool(output.get("terminate_round", True))
    return True
