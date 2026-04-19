"""Tool execution helpers for conversation workflows."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.tools.schema import ToolCallRequest


@dataclass(slots=True)
class WorkflowToolLoopResult:
    """Aggregated tool execution output for one model iteration."""

    tool_messages: list[dict[str, Any]] = field(default_factory=list)
    tool_calls_log: list[dict[str, Any]] = field(default_factory=list)
    no_reply: bool = False
    reply_sent: bool = False
    terminate_round: bool = False
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

    for tool_call in tool_calls:
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

        outcome.tool_calls_log.append({"name": tool_name, "arguments": tool_args})

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

        if tool_name == "no_reply":
            outcome.no_reply = True
            outcome.internal_summary = str(tool_args.get("internal_summary", "") or "")
        elif tool_name == "send_reply" and tool_result.success:
            outcome.reply_sent = True
            outcome.response_summary = str(tool_args.get("text", "") or "")[:200]
            terminate_round = True
            if isinstance(tool_result.output, dict):
                terminate_round = bool(tool_result.output.get("terminate_round", True))
            outcome.terminate_round = outcome.terminate_round or terminate_round

        outcome.tool_messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": output_str,
            }
        )

    return outcome
