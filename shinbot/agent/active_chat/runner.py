"""LLM-backed active chat fast-mode runner."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from shinbot.agent.active_chat.models import (
    ActiveChatActionKind,
    ActiveChatBatch,
    ActiveChatRoundResult,
)
from shinbot.agent.active_chat.prompt_registration import (
    ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE,
)
from shinbot.agent.active_chat.tool_loop import ActiveChatToolLoop
from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.prompt_manager import (
    PromptBuildRequest,
    PromptContextPolicy,
    PromptInjection,
    PromptRegistry,
    PromptStage,
)

logger = logging.getLogger(__name__)

_TOOLLESS_REPAIR_PROMPT = """
上一轮 active_chat fast-mode 没有调用工具，但该阶段不会把裸文本发送给用户。
请重新判断，并必须调用工具：
- 需要回复时，按发送顺序调用一个或多个 send_reply。
- 只想轻量互动时，可以单独调用 send_poke。
- 不需要回应时调用 no_reply，可用 intensity=normal 或 strong。
- 需要更深入思考时调用 request_think_mode，并写明 reason。
- 想结束 active chat 时调用 exit_active，并必须写明 reason。
不要输出裸文本作为最终回复。
""".strip()


class ActiveChatMessageStore(Protocol):
    """Read message logs needed by active chat context building."""

    def get(self, msg_id: int) -> dict[str, Any] | None:
        """Return one message-log payload."""


class ActiveChatContextBuilder(Protocol):
    """Build prompt-adjacent context from selected message records."""

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[dict[str, Any]],
        purpose: str,
        options: Any | None = None,
    ) -> Any:
        """Return an object with source_messages/instruction_content/metadata fields."""


@dataclass(slots=True, frozen=True)
class ActiveChatFastRunnerConfig:
    """Model routing and prompt configuration for active chat fast mode."""

    caller: str = "agent.active_chat"
    route_id: str | None = None
    model_id: str | None = None
    profile_id: str = ""
    component_ids_by_stage: dict[PromptStage, list[str]] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)


class ActiveChatFastRunner:
    """Run one active chat fast-mode LLM round and execute its tool calls."""

    stage_id = "fast_mode"

    def __init__(
        self,
        model_runtime: Any,
        *,
        prompt_registry: PromptRegistry,
        tool_manager: Any,
        message_store: ActiveChatMessageStore | None = None,
        context_builder: ActiveChatContextBuilder | None = None,
        tool_loop: ActiveChatToolLoop | None = None,
        config: ActiveChatFastRunnerConfig | None = None,
    ) -> None:
        self._model_runtime = model_runtime
        self._prompt_registry = prompt_registry
        self._tool_manager = tool_manager
        self._message_store = message_store
        self._context_builder = context_builder
        self._tool_loop = tool_loop or ActiveChatToolLoop()
        self._config = config or ActiveChatFastRunnerConfig()

    async def run(self, batch: ActiveChatBatch) -> ActiveChatRoundResult:
        """Execute one active chat fast-mode round."""
        try:
            messages, tools, metadata = self._build_model_call_parts(batch)
        except Exception:
            logger.exception("Active chat prompt build failed for session %s", batch.session_id)
            return ActiveChatRoundResult(
                success=True,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="active_chat_prompt_build_failed",
            )

        result = await self._generate(
            batch,
            messages=messages,
            tools=tools,
            metadata=metadata,
            repair_attempt=0,
        )
        if result is None:
            return ActiveChatRoundResult(
                success=True,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="active_chat_model_call_failed",
            )
        if result.tool_calls:
            return (
                await self._tool_loop.execute(
                    result.tool_calls,
                    tool_manager=self._tool_manager,
                    instance_id=_instance_id_from_session(batch.session_id),
                    session_id=batch.session_id,
                    run_id=str(result.execution_id or ""),
                )
            ).round_result

        repaired = await self._repair_toolless_round(
            batch,
            messages=messages,
            tools=tools,
            metadata=metadata,
            first_result=result,
        )
        if repaired is None:
            return ActiveChatRoundResult(
                success=True,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="active_chat_toolless_repair_failed",
            )
        if not repaired.tool_calls:
            return ActiveChatRoundResult(
                success=True,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="active_chat_toolless_after_repair",
            )
        return (
            await self._tool_loop.execute(
                repaired.tool_calls,
                tool_manager=self._tool_manager,
                instance_id=_instance_id_from_session(batch.session_id),
                session_id=batch.session_id,
                run_id=str(repaired.execution_id or ""),
            )
        ).round_result

    def _build_model_call_parts(
        self,
        batch: ActiveChatBatch,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        source_messages = self._load_source_messages(batch)
        context = self._build_context(batch, source_messages)
        metadata = {
            "active_chat_stage": self.stage_id,
            "message_log_ids": batch.message_log_ids,
            "interest_value": batch.active_chat_state.interest_value,
            "active_epoch": batch.active_chat_state.active_epoch,
            "review_result_summary": batch.review_result_summary,
            **dict(getattr(context, "metadata", {}) or {}),
        }
        component_ids_by_stage = self._component_ids_by_stage()
        build_result = self._prompt_registry.build_messages(
            PromptBuildRequest(
                caller=self._config.caller,
                workflow_id="active_chat",
                stage_id=self.stage_id,
                session_id=batch.session_id,
                instance_id=_instance_id_from_session(batch.session_id),
                profile_id=self._config.profile_id,
                component_ids_by_stage=component_ids_by_stage,
                injections=self._build_prompt_injections(
                    batch,
                    context=context,
                    source_messages=source_messages,
                    component_ids_by_stage=component_ids_by_stage,
                ),
                context_policy=PromptContextPolicy.DISABLED,
                metadata=metadata,
            )
        )
        return build_result.messages, build_result.tools, dict(build_result.metadata)

    def _load_source_messages(self, batch: ActiveChatBatch) -> list[dict[str, Any]]:
        if self._message_store is None:
            return [
                {
                    "id": signal.message_log_id,
                    "session_id": signal.session_id,
                    "sender_id": signal.sender_id,
                    "role": "user",
                    "raw_text": "",
                    "created_at": signal.created_at,
                }
                for signal in batch.messages
            ]

        messages: list[dict[str, Any]] = []
        for message_log_id in batch.message_log_ids:
            payload = self._message_store.get(message_log_id)
            if payload is not None:
                messages.append(dict(payload))
        return messages

    def _build_context(
        self,
        batch: ActiveChatBatch,
        source_messages: list[dict[str, Any]],
    ) -> Any:
        if self._context_builder is None:
            return _FallbackActiveChatContext(
                session_id=batch.session_id,
                source_messages=source_messages,
                metadata={},
            )
        return self._context_builder.build_for_messages(
            session_id=batch.session_id,
            messages=source_messages,
            purpose="active_chat_fast",
            options=None,
        )

    def _build_prompt_injections(
        self,
        batch: ActiveChatBatch,
        *,
        context: Any,
        source_messages: list[dict[str, Any]],
        component_ids_by_stage: dict[PromptStage, list[str]],
    ) -> list[PromptInjection]:
        injections: list[PromptInjection] = []
        if not component_ids_by_stage.get(PromptStage.SYSTEM_BASE):
            injections.append(
                PromptInjection(
                    stage=PromptStage.SYSTEM_BASE,
                    component_id="active_chat.fast_mode.system.fallback",
                    text=(
                        "You are ShinBot's internal active chat fast-mode stage. "
                        "Use tools to decide the immediate action."
                    ),
                    priority=10,
                )
            )
        injections.append(
            PromptInjection(
                stage=PromptStage.INSTRUCTIONS,
                component_id="active_chat.fast_mode.batch",
                content_blocks=self._instruction_content(
                    batch,
                    context=context,
                    source_messages=source_messages,
                ),
                priority=10,
                metadata={"active_chat_stage": self.stage_id},
            )
        )
        tools = self._active_chat_tools(batch)
        if tools:
            injections.append(
                PromptInjection(
                    stage=PromptStage.ABILITIES,
                    component_id="active_chat.fast_mode.tools",
                    tools=tools,
                    priority=10,
                    metadata={"active_chat_stage": self.stage_id},
                )
            )
        return injections

    def _instruction_content(
        self,
        batch: ActiveChatBatch,
        *,
        context: Any,
        source_messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        content = [
            {
                "type": "text",
                "text": (
                    "Active chat fast-mode batch. Decide one immediate action via tools.\n"
                    f"Session id: {batch.session_id}\n"
                    f"Message log ids: {json.dumps(batch.message_log_ids, ensure_ascii=False)}\n"
                    f"Current interest: {batch.active_chat_state.interest_value:.2f}"
                ),
            }
        ]
        if batch.review_result_summary is not None:
            content.append(
                {
                    "type": "text",
                    "text": "Review handoff summary JSON:\n"
                    + json.dumps(batch.review_result_summary, ensure_ascii=False),
                }
            )
        instruction_content = list(getattr(context, "instruction_content", []) or [])
        if instruction_content:
            content.extend(instruction_content)
        else:
            content.append(
                {
                    "type": "text",
                    "text": "Source messages JSON:\n"
                    + json.dumps(source_messages, ensure_ascii=False),
                }
            )
        return content

    def _active_chat_tools(self, batch: ActiveChatBatch) -> list[dict[str, Any]]:
        tools = self._tool_manager.export_model_tools(
            caller=self._config.caller,
            instance_id=_instance_id_from_session(batch.session_id),
            session_id=batch.session_id,
            tags={"attention"},
        )
        active_tools = [
            _active_chat_tool_schema(tool)
            for tool in tools
            if tool.get("function", {}).get("name")
            in {"send_reply", "no_reply", "send_poke"}
        ]
        active_tools.extend(_virtual_tool_schemas())
        return active_tools

    def _component_ids_by_stage(self) -> dict[PromptStage, list[str]]:
        result: dict[PromptStage, list[str]] = {
            stage: list(component_ids)
            for stage, component_ids in self._config.component_ids_by_stage.items()
        }
        for stage, component_ids in ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE.get(
            self.stage_id,
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

    async def _generate(
        self,
        batch: ActiveChatBatch,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        metadata: dict[str, Any],
        repair_attempt: int,
    ) -> Any | None:
        try:
            return await self._model_runtime.generate(
                ModelRuntimeCall(
                    route_id=self._config.route_id,
                    model_id=self._config.model_id,
                    caller=self._config.caller,
                    session_id=batch.session_id,
                    instance_id=_instance_id_from_session(batch.session_id),
                    purpose="active_chat_fast",
                    messages=messages,
                    tools=tools,
                    response_format=None,
                    metadata={
                        **dict(metadata),
                        "repair_attempt": repair_attempt,
                    },
                    params=dict(self._config.params),
                )
            )
        except ModelCallError:
            logger.exception("Active chat fast-mode model call failed for %s", batch.session_id)
            return None

    async def _repair_toolless_round(
        self,
        batch: ActiveChatBatch,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        metadata: dict[str, Any],
        first_result: Any,
    ) -> Any | None:
        repair_messages = list(messages)
        if str(first_result.text or "").strip():
            repair_messages.append(
                {"role": "assistant", "content": str(first_result.text or "").strip()}
            )
        repair_messages.append(
            {
                "role": "system",
                "content": [{"type": "text", "text": _TOOLLESS_REPAIR_PROMPT}],
            }
        )
        return await self._generate(
            batch,
            messages=repair_messages,
            tools=tools,
            metadata=metadata,
            repair_attempt=1,
        )


@dataclass(slots=True)
class _FallbackActiveChatContext:
    session_id: str
    source_messages: list[dict[str, Any]]
    instruction_content: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


def _active_chat_tool_schema(tool: dict[str, Any]) -> dict[str, Any]:
    function = tool.get("function")
    if not isinstance(function, dict):
        return tool
    name = function.get("name")
    if name not in {"send_reply", "no_reply", "send_poke"}:
        return tool
    reviewed = {
        **tool,
        "function": {
            **function,
            "description": _active_chat_tool_description(name, function),
        },
    }
    if name in {"send_reply", "no_reply"}:
        parameters = reviewed["function"].get("parameters")
        if isinstance(parameters, dict):
            reviewed_parameters = dict(parameters)
            properties = dict(reviewed_parameters.get("properties") or {})
            properties["intensity"] = {
                "type": "string",
                "enum": (
                    ["light", "engaged"]
                    if name == "send_reply"
                    else ["normal", "strong"]
                ),
                "description": (
                    "Active chat interest adjustment hint. Use sparingly; "
                    "omit when unsure."
                ),
            }
            reviewed_parameters["properties"] = properties
            reviewed["function"]["parameters"] = reviewed_parameters
    return reviewed


def _active_chat_tool_description(name: Any, function: dict[str, Any]) -> str:
    base = str(function.get("description") or "")
    if name == "send_reply":
        return (
            base
            + "\nActive chat rule: multiple send_reply calls are allowed and will "
            "be sent in order. quote_message_log_id is optional in active chat."
        )
    if name == "no_reply":
        return (
            base
            + "\nActive chat rule: use intensity=strong only when the session "
            "should cool down more aggressively."
        )
    if name == "send_poke":
        return base + "\nActive chat rule: send_poke may be used as a standalone action."
    return base


def _virtual_tool_schemas() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "request_think_mode",
                "description": (
                    "Ask ShinBot to switch this active chat turn to a heavier "
                    "thinking workflow. Use only when fast mode is insufficient."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reason": {
                            "type": "string",
                            "description": "Why this batch needs deeper reasoning.",
                        },
                    },
                    "required": ["reason"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "exit_active",
                "description": (
                    "End the current active chat session immediately. A clear "
                    "reason is required."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reason": {
                            "type": "string",
                            "description": "Why active chat should end now.",
                        },
                    },
                    "required": ["reason"],
                },
            },
        },
    ]


def _instance_id_from_session(session_id: str) -> str:
    return session_id.split(":", 1)[0] if ":" in session_id else ""


__all__ = [
    "ActiveChatContextBuilder",
    "ActiveChatFastRunner",
    "ActiveChatFastRunnerConfig",
    "ActiveChatMessageStore",
]
