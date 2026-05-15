from __future__ import annotations

import json
from typing import Any

import pytest

from shinbot.agent.coordinators.review.models import ReviewWorkflowExplanation
from shinbot.agent.runtime.instance_config import RuntimeModelTarget
from shinbot.agent.runtime.tool_config import StageToolConfig
from shinbot.agent.scheduler import ActiveChatDisposition, ActiveChatState
from shinbot.agent.services.context.active_chat_context import ActiveChatContextBuilderAdapter
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.model_runtime import GenerateResult, ModelCallError
from shinbot.agent.services.prompt_engine import (
    PromptComponent,
    PromptComponentKind,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.services.summaries import ReviewHandoffContext, SummaryHandoffEntry
from shinbot.agent.services.tools.schema import ToolCallRequest, ToolCallResult
from shinbot.agent.workflows.active_chat import ActiveChatFastRunner, ActiveChatFastRunnerConfig
from shinbot.agent.workflows.active_chat.models import (
    ActiveChatActionKind,
    ActiveChatBatch,
    ActiveChatMessageSignal,
    ActiveChatNoReplyIntensity,
)
from shinbot.agent.workflows.active_chat.prompt_registration import (
    register_active_chat_prompt_components,
)
from shinbot.core.instance_config import resolve_instance_runtime_config


class FakeModelRuntime:
    def __init__(self, responses: list[GenerateResult]) -> None:
        self.responses = list(responses)
        self.calls: list[Any] = []

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        return self.responses.pop(0)


class FailingModelRuntime:
    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        raise ModelCallError("model failed")


class FailingRepairModelRuntime:
    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        if len(self.calls) == 1:
            return make_result(text="I would reply without tools.")
        raise ModelCallError("repair failed")


class FakeToolManager:
    def __init__(self) -> None:
        self.calls: list[ToolCallRequest] = []
        self.build_request_tool_calls: list[dict[str, Any]] = []
        self.export_model_tool_calls: list[dict[str, Any]] = []

    def build_request_tools(self, tool_names, **kwargs) -> list[dict[str, Any]]:
        self.build_request_tool_calls.append({"tool_names": list(tool_names), **kwargs})
        schemas = {
            str(item["function"]["name"]): item
            for item in self.export_model_tools()
        }
        return [schemas[name] for name in tool_names if name in schemas]

    def export_model_tools(self, **kwargs) -> list[dict[str, Any]]:
        self.export_model_tool_calls.append(dict(kwargs))
        tools = self._all_tools()
        tags = kwargs.get("tags")
        if tags == {"knowledge"}:
            return [
                tool
                for tool in tools
                if tool["function"]["name"] in {"lookup_profile", "send_reply"}
            ]
        return tools

    def _all_tools(self) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "send_reply",
                    "description": "Send reply",
                    "parameters": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "no_reply",
                    "description": "Do not reply",
                    "parameters": {
                        "type": "object",
                        "properties": {"internal_summary": {"type": "string"}},
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_memory",
                    "description": "Search memory",
                    "parameters": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "lookup_profile",
                    "description": "Lookup profile",
                    "parameters": {
                        "type": "object",
                        "properties": {"user_id": {"type": "string"}},
                        "required": ["user_id"],
                    },
                },
            },
        ]

    async def execute(self, call: ToolCallRequest) -> ToolCallResult:
        self.calls.append(call)
        return ToolCallResult(
            tool_name=call.tool_name,
            success=True,
            output={"action": call.tool_name, "message_log_id": 9001},
        )


class FakeMessageStore:
    def get(self, msg_id: int) -> dict[str, Any] | None:
        return {
            "id": msg_id,
            "session_id": "bot:group:room",
            "sender_id": "alice",
            "sender_name": "Alice",
            "role": "user",
            "raw_text": f"message {msg_id}",
            "created_at": 1234.0 + msg_id,
        }


class FakeContextManager:
    def __init__(self) -> None:
        self.instruction_calls: list[dict[str, Any]] = []
        self.context_calls: list[dict[str, Any]] = []

    def build_instruction_stage_content(
        self,
        session_id: str,
        unread_records: list[dict[str, Any]],
        *,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        self.instruction_calls.append(
            {
                "session_id": session_id,
                "message_ids": [record["id"] for record in unread_records],
                "previous_summary": previous_summary,
                "self_platform_id": self_platform_id,
                "now_ms": now_ms,
            }
        )
        return [{"type": "text", "text": "Active batch from context builder"}]

    def build_context_stage_messages(
        self,
        session_id: str,
        *,
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        self.context_calls.append(
            {
                "session_id": session_id,
                "self_platform_id": self_platform_id,
                "now_ms": now_ms,
            }
        )
        return [
            {
                "role": "user",
                "content": [{"type": "text", "text": "Recent tail context"}],
            }
        ]


class BrokenMessageFormatter:
    def format_instruction_content(self, *_args, **_kwargs) -> list[dict[str, Any]]:
        raise RuntimeError("format failed")


class BrokenContextBuilder:
    def build_for_messages(self, **_kwargs: Any) -> object:
        raise RuntimeError("context build failed")


def make_result(*, text: str = "", tool_calls: list[dict[str, Any]] | None = None) -> GenerateResult:
    return GenerateResult(
        text=text,
        tool_calls=list(tool_calls or []),
        raw_response={},
        execution_id="exec-1",
        route_id="",
        provider_id="",
        model_id="",
        usage={},
    )


def make_tool_call(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"call-{name}",
        "type": "function",
        "function": {
            "name": name,
            "arguments": json.dumps(arguments),
        },
    }


def make_batch(
    *,
    review_result_summary: Any | None = None,
    self_platform_id: str = "",
    conversation_summary: str = "",
    conversation_messages: list[dict[str, Any]] | None = None,
) -> ActiveChatBatch:
    active_state = ActiveChatState(
        session_id="bot:group:room",
        interest_value=42.0,
        decay_half_life_seconds=20.0,
        entered_at=1000.0,
        updated_at=1000.0,
    )
    return ActiveChatBatch(
        session_id="bot:group:room",
        messages=[
            ActiveChatMessageSignal(
                session_id="bot:group:room",
                message_log_id=101,
                sender_id="alice",
                response_profile="balanced",
                self_platform_id=self_platform_id,
            )
        ],
        active_chat_state=active_state,
        response_profile="balanced",
        review_result_summary=(
            {"summary": "review found a running topic"}
            if review_result_summary is None
            else review_result_summary
        ),
        conversation_summary=conversation_summary,
        conversation_messages=list(conversation_messages or []),
    )


__all__ = [
    "ActiveChatActionKind",
    "ActiveChatBatch",
    "ActiveChatContextBuilderAdapter",
    "ActiveChatDisposition",
    "ActiveChatFastRunner",
    "ActiveChatFastRunnerConfig",
    "ActiveChatMessageSignal",
    "ActiveChatNoReplyIntensity",
    "ActiveChatState",
    "Any",
    "BrokenContextBuilder",
    "BrokenMessageFormatter",
    "FailingModelRuntime",
    "FailingRepairModelRuntime",
    "FakeContextManager",
    "FakeMessageStore",
    "FakeModelRuntime",
    "FakeToolManager",
    "GenerateResult",
    "MessageFormatterService",
    "ModelCallError",
    "PromptComponent",
    "PromptComponentKind",
    "PromptRegistry",
    "PromptStage",
    "ReviewHandoffContext",
    "ReviewWorkflowExplanation",
    "RuntimeModelTarget",
    "StageToolConfig",
    "SummaryHandoffEntry",
    "ToolCallRequest",
    "ToolCallResult",
    "annotations",
    "json",
    "make_batch",
    "make_result",
    "make_tool_call",
    "pytest",
    "register_active_chat_prompt_components",
    "resolve_instance_runtime_config",
]
