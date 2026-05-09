"""Tool-loop primitives for active chat fast mode."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.active_chat.models import (
    ActiveChatActionKind,
    ActiveChatNoReplyIntensity,
    ActiveChatReplyIntensity,
    ActiveChatRoundResult,
)
from shinbot.agent.tools.parsing import parse_tool_call
from shinbot.agent.tools.schema import ToolCallRequest

_VIRTUAL_TOOL_NAMES = {"exit_active", "request_think_mode"}


@dataclass(slots=True)
class ActiveChatToolLoopResult:
    """Aggregated result of one active chat tool-call batch."""

    round_result: ActiveChatRoundResult
    tool_messages: list[dict[str, Any]] = field(default_factory=list)
    tool_calls_log: list[dict[str, Any]] = field(default_factory=list)
    invalid_reason: str = ""


class ActiveChatToolLoop:
    """Execute active chat tool calls without attention-workflow terminal rules."""

    caller = "active_chat.fast_mode"

    async def execute(
        self,
        tool_calls: list[dict[str, Any]],
        *,
        tool_manager: Any,
        instance_id: str,
        session_id: str,
        run_id: str = "",
        user_id: str = "",
        trace_id: str = "",
    ) -> ActiveChatToolLoopResult:
        """Execute tool calls sequentially and derive the strongest semantic action."""
        parsed_calls = [parse_tool_call(tool_call) for tool_call in tool_calls]
        result = ActiveChatToolLoopResult(
            round_result=ActiveChatRoundResult(
                success=False,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="no_tool_call",
            )
        )
        result.tool_calls_log.extend(
            {"name": tool_name, "arguments": tool_args}
            for _, tool_name, tool_args in parsed_calls
        )

        if not parsed_calls:
            result.invalid_reason = "no_tool_call"
            return result

        action_state = _ActionState()
        successful_action_count = 0
        failure_reasons: list[str] = []

        for tool_call_id, tool_name, tool_args in parsed_calls:
            if tool_name in _VIRTUAL_TOOL_NAMES:
                virtual_result = _execute_virtual_tool(tool_name, tool_args)
                output = virtual_result["output"]
                success = bool(virtual_result["success"])
                if success:
                    successful_action_count += 1
                    action_state.observe(tool_name, tool_args)
                else:
                    failure_reasons.append(
                        str(
                            virtual_result.get("invalid_reason")
                            or output.get("error")
                            or "virtual_tool_failed"
                        )
                    )
                result.tool_messages.append(
                    _tool_message(
                        tool_call_id,
                        tool_name=tool_name,
                        output=output,
                        success=success,
                    )
                )
                continue

            tool_result = await tool_manager.execute(
                ToolCallRequest(
                    tool_name=tool_name,
                    arguments=tool_args,
                    caller=self.caller,
                    instance_id=instance_id,
                    session_id=session_id,
                    user_id=user_id,
                    trace_id=trace_id,
                    run_id=run_id,
                )
            )
            if tool_result.success:
                successful_action_count += 1
                action_state.observe(tool_name, tool_args)
                output = tool_result.output
            else:
                failure_reasons.append(tool_result.error_message or tool_result.error_code)
                output = {"error": tool_result.error_message or tool_result.error_code}
            result.tool_messages.append(
                _tool_message(
                    tool_call_id,
                    tool_name=tool_name,
                    output=output,
                    success=tool_result.success,
                )
            )

        if successful_action_count == 0:
            reason = "; ".join(reason for reason in failure_reasons if reason)
            result.invalid_reason = reason or "all_tool_calls_failed"
            result.round_result = ActiveChatRoundResult(
                success=True,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason=result.invalid_reason,
            )
            return result

        round_result = action_state.to_round_result()
        if round_result.action == ActiveChatActionKind.EXIT_ACTIVE and not round_result.reason:
            result.invalid_reason = "exit_active_missing_reason"
            result.round_result = ActiveChatRoundResult(
                success=True,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason=result.invalid_reason,
            )
            return result

        result.round_result = round_result
        return result


@dataclass(slots=True)
class _ActionState:
    action: ActiveChatActionKind = ActiveChatActionKind.WATCH
    reason: str = ""
    reply_intensity: ActiveChatReplyIntensity = ActiveChatReplyIntensity.LIGHT
    no_reply_intensity: ActiveChatNoReplyIntensity = ActiveChatNoReplyIntensity.NORMAL

    def observe(self, tool_name: str, arguments: dict[str, Any]) -> None:
        if tool_name == "exit_active":
            self._promote(ActiveChatActionKind.EXIT_ACTIVE)
            self.reason = _reason_from(arguments)
            return
        if tool_name == "request_think_mode":
            self._promote(ActiveChatActionKind.REQUEST_THINK_MODE)
            self.reason = _reason_from(arguments)
            return
        if tool_name == "send_reply":
            self._promote(ActiveChatActionKind.SEND_REPLY)
            self.reply_intensity = max(
                self.reply_intensity,
                _reply_intensity_from(arguments),
                key=_reply_intensity_rank,
            )
            if not self.reason:
                self.reason = _reason_from(arguments)
            return
        if tool_name == "send_poke":
            self._promote(ActiveChatActionKind.SEND_POKE)
            if not self.reason:
                self.reason = _reason_from(arguments)
            return
        if tool_name == "no_reply":
            self._promote(ActiveChatActionKind.NO_REPLY)
            self.no_reply_intensity = max(
                self.no_reply_intensity,
                _no_reply_intensity_from(arguments),
                key=_no_reply_intensity_rank,
            )
            if not self.reason:
                self.reason = _reason_from(arguments)

    def to_round_result(self) -> ActiveChatRoundResult:
        return ActiveChatRoundResult(
            success=True,
            reason=self.reason,
            action=self.action,
            reply_intensity=self.reply_intensity,
            no_reply_intensity=self.no_reply_intensity,
        )

    def _promote(self, action: ActiveChatActionKind) -> None:
        if _action_rank(action) >= _action_rank(self.action):
            self.action = action




def _execute_virtual_tool(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    reason = _reason_from(arguments)
    if tool_name == "exit_active" and not reason:
        return {
            "success": False,
            "invalid_reason": "exit_active_missing_reason",
            "output": {"error": "exit_active requires a non-empty reason"},
        }
    return {
        "success": True,
        "output": {
            "action": tool_name,
            "reason": reason,
        },
    }


def _tool_message(
    tool_call_id: str,
    *,
    tool_name: str,
    output: Any,
    success: bool,
) -> dict[str, Any]:
    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": json.dumps(
            _trace_tool_output(tool_name, output, success=success),
            ensure_ascii=False,
        ),
    }


def _trace_tool_output(tool_name: str, output: Any, *, success: bool) -> dict[str, Any]:
    """Return a compact prompt-safe view of one tool result for AtC trace."""
    if not isinstance(output, dict):
        return {"success": success, "action": tool_name}

    action = str(output.get("action") or tool_name)
    if not success:
        return {
            "success": False,
            "action": action,
            "error": str(output.get("error") or "tool_failed"),
        }

    result: dict[str, Any] = {
        "success": True,
        "action": action,
    }
    if action == "send_reply":
        _copy_if_present(output, result, "sent")
        _copy_if_present(output, result, "message_log_id")
        _copy_if_present(output, result, "quote_message_id")
        _copy_if_present(output, result, "terminate_round")
        if "length" in output:
            result["text_length"] = output["length"]
        return result
    if action == "no_reply":
        _copy_if_present(output, result, "summary_stored")
        return result
    if action == "send_poke":
        _copy_if_present(output, result, "sent")
        _copy_if_present(output, result, "user_id")
        _copy_if_present(output, result, "session_type")
        _copy_if_present(output, result, "terminate_round")
        return result
    if action in _VIRTUAL_TOOL_NAMES:
        _copy_if_present(output, result, "reason")
        return result
    return result


def _copy_if_present(source: dict[str, Any], target: dict[str, Any], key: str) -> None:
    if key in source and source[key] is not None:
        target[key] = source[key]


def _reason_from(arguments: dict[str, Any]) -> str:
    return str(arguments.get("reason", "") or "").strip()


def _reply_intensity_from(arguments: dict[str, Any]) -> ActiveChatReplyIntensity:
    raw = str(arguments.get("intensity", "") or "").strip().lower()
    if raw == ActiveChatReplyIntensity.ENGAGED:
        return ActiveChatReplyIntensity.ENGAGED
    return ActiveChatReplyIntensity.LIGHT


def _no_reply_intensity_from(arguments: dict[str, Any]) -> ActiveChatNoReplyIntensity:
    raw = str(arguments.get("intensity", "") or "").strip().lower()
    if raw == ActiveChatNoReplyIntensity.STRONG:
        return ActiveChatNoReplyIntensity.STRONG
    return ActiveChatNoReplyIntensity.NORMAL


def _action_rank(action: ActiveChatActionKind) -> int:
    return {
        ActiveChatActionKind.WATCH: 0,
        ActiveChatActionKind.NO_REPLY: 1,
        ActiveChatActionKind.SEND_POKE: 2,
        ActiveChatActionKind.SEND_REPLY: 3,
        ActiveChatActionKind.REQUEST_THINK_MODE: 4,
        ActiveChatActionKind.EXIT_ACTIVE: 5,
        ActiveChatActionKind.RETRY_FAILED: 6,
    }[action]


def _reply_intensity_rank(intensity: ActiveChatReplyIntensity) -> int:
    return {
        ActiveChatReplyIntensity.LIGHT: 0,
        ActiveChatReplyIntensity.ENGAGED: 1,
    }[intensity]


def _no_reply_intensity_rank(intensity: ActiveChatNoReplyIntensity) -> int:
    return {
        ActiveChatNoReplyIntensity.NORMAL: 0,
        ActiveChatNoReplyIntensity.STRONG: 1,
    }[intensity]


__all__ = ["ActiveChatToolLoop", "ActiveChatToolLoopResult"]
