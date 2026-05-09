"""Conversation trace compaction for active chat."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from shinbot.agent.coordinators.active_chat.models import ActiveChatAttentionState


@dataclass(slots=True, frozen=True)
class ActiveChatTraceConfig:
    """Limits for the in-memory active chat conversation trace."""

    message_limit: int = 80
    summary_line_limit: int = 8


class ActiveChatTraceCompactor:
    """Keep recent assistant/tool trace and summarize evicted prefix messages."""

    def __init__(self, config: ActiveChatTraceConfig | None = None) -> None:
        self.config = config or ActiveChatTraceConfig()

    def append(
        self,
        state: ActiveChatAttentionState,
        messages: list[dict[str, Any]],
    ) -> None:
        """Append trace messages and compact old entries when over limit."""
        if not messages or self.config.message_limit <= 0:
            return

        state.conversation_messages.extend(dict(message) for message in messages)
        overflow = len(state.conversation_messages) - self.config.message_limit
        if overflow <= 0:
            return

        evicted = state.conversation_messages[:overflow]
        del state.conversation_messages[:overflow]
        orphaned_tools = self._pop_orphaned_leading_tools(state.conversation_messages)
        evicted.extend(orphaned_tools)
        state.conversation_summary = self._merge_summary(
            state.conversation_summary,
            self._summarize_evicted(evicted),
        )

    def _merge_summary(self, previous: str, new_line: str) -> str:
        lines = [line for line in previous.splitlines() if line.strip()]
        if new_line:
            lines.append(new_line)
        return "\n".join(lines[-self.config.summary_line_limit :])

    def _summarize_evicted(self, messages: list[dict[str, Any]]) -> str:
        role_counts: dict[str, int] = {}
        tool_actions: list[str] = []
        for message in messages:
            role = str(message.get("role", "unknown") or "unknown")
            role_counts[role] = role_counts.get(role, 0) + 1
            tool_actions.extend(_tool_action_names(message))

        payload: dict[str, Any] = {
            "compacted_messages": len(messages),
            "roles": role_counts,
        }
        if tool_actions:
            payload["recent_tool_actions"] = tool_actions[-8:]
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def _pop_orphaned_leading_tools(
        self,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        orphaned: list[dict[str, Any]] = []
        while messages and messages[0].get("role") == "tool":
            orphaned.append(messages.pop(0))
        return orphaned


def sanitize_conversation_trace_messages(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return prompt-safe conversation trace messages.

    Tool messages are only kept as part of a complete assistant tool-call group.
    This keeps the model request from containing orphaned tool results or an
    assistant tool-call message whose required tool results are missing.
    """
    safe_messages: list[dict[str, Any]] = []
    index = 0
    while index < len(messages):
        message = messages[index]
        role = message.get("role")
        if role == "tool":
            index += 1
            continue

        tool_calls = message.get("tool_calls", [])
        if role == "assistant" and tool_calls:
            expected_ids = _tool_call_ids(tool_calls)
            if not expected_ids:
                index += 1
                continue

            tool_messages: list[dict[str, Any]] = []
            found_ids: set[str] = set()
            next_index = index + 1
            while next_index < len(messages) and messages[next_index].get("role") == "tool":
                tool_message = messages[next_index]
                tool_call_id = str(tool_message.get("tool_call_id", "") or "")
                if tool_call_id in expected_ids:
                    tool_messages.append(dict(tool_message))
                    found_ids.add(tool_call_id)
                next_index += 1

            if found_ids == expected_ids:
                safe_messages.append(dict(message))
                safe_messages.extend(tool_messages)
            index = next_index
            continue

        safe_messages.append(dict(message))
        index += 1

    return safe_messages


def _tool_call_ids(tool_calls: Any) -> set[str]:
    if not isinstance(tool_calls, list):
        return set()
    return {
        str(tool_call.get("id", "") or "")
        for tool_call in tool_calls
        if isinstance(tool_call, dict) and str(tool_call.get("id", "") or "")
    }


def _tool_action_names(message: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for tool_call in message.get("tool_calls", []) or []:
        function = tool_call.get("function", {}) if isinstance(tool_call, dict) else {}
        name = function.get("name") if isinstance(function, dict) else None
        if name:
            names.append(str(name))
    if message.get("role") == "tool":
        content = message.get("content")
        if isinstance(content, str):
            try:
                payload = json.loads(content)
            except json.JSONDecodeError:
                payload = {}
            action = payload.get("action") if isinstance(payload, dict) else None
            if action:
                names.append(str(action))
    return names


__all__ = [
    "ActiveChatTraceCompactor",
    "ActiveChatTraceConfig",
    "sanitize_conversation_trace_messages",
]
