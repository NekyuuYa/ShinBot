"""LLM-backed active chat fast-mode runner."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from enum import Enum
from inspect import isawaitable
from typing import Any, Protocol

from shinbot.agent.coordinators.active_chat.trace import sanitize_conversation_trace_messages
from shinbot.agent.runtime.instance_config import (
    InstanceRuntimeConfigResolver,
    RuntimeModelTarget,
    apply_instance_runtime_config_to_call,
    apply_instance_runtime_config_to_metadata,
    resolve_runtime_model_target,
)
from shinbot.agent.services.context.active_chat_context import (
    ActiveChatContextBuilder,
    ActiveChatContextBuildOptions,
)
from shinbot.agent.services.message_formatter import (
    MessageFormatConfig,
    MessageFormatterService,
)
from shinbot.agent.services.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.services.prompt_engine import (
    PromptBuildRequest,
    PromptContextPolicy,
    PromptInjection,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.services.summaries import ReviewHandoffContext, SummaryHandoffEntry
from shinbot.agent.utils.parsing import instance_id_from_session
from shinbot.agent.workflows.active_chat.models import (
    ActiveChatActionKind,
    ActiveChatBatch,
    ActiveChatMessageSignal,
    ActiveChatRoundResult,
)
from shinbot.agent.workflows.active_chat.prompt_registration import (
    ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE,
)
from shinbot.agent.workflows.active_chat.tool_loop import ActiveChatToolLoop
from shinbot.agent.workflows.chat_actions import CHAT_ACTION_TOOL_TAG

logger = logging.getLogger(__name__)

class ActiveChatMessageStore(Protocol):
    """Read message logs needed by active chat context building."""

    def get(self, msg_id: int) -> dict[str, Any] | None:
        """Return one message-log payload."""


@dataclass(slots=True, frozen=True)
class ActiveChatFastRunnerConfig:
    """Model routing and prompt configuration for active chat fast mode."""

    caller: str = "agent.active_chat"
    llm: str = ""
    default_llm: str = ""
    route_id: str | None = None
    model_id: str | None = None
    profile_id: str = ""
    component_ids_by_stage: dict[PromptStage, list[str]] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    message_format_config: MessageFormatConfig | None = None
    instance_config_resolver: InstanceRuntimeConfigResolver | None = None
    model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None


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
        message_formatter: MessageFormatterService | None = None,
        tool_loop: ActiveChatToolLoop | None = None,
        pending_message_provider: (
            Callable[
                [ActiveChatBatch],
                list[ActiveChatMessageSignal] | Awaitable[list[ActiveChatMessageSignal]],
            ]
            | None
        ) = None,
        config: ActiveChatFastRunnerConfig | None = None,
    ) -> None:
        self._model_runtime = model_runtime
        self._prompt_registry = prompt_registry
        self._tool_manager = tool_manager
        self._message_store = message_store
        self._context_builder = context_builder
        self._message_formatter = message_formatter
        self._tool_loop = tool_loop or ActiveChatToolLoop()
        self._pending_message_provider = pending_message_provider
        self._config = config or ActiveChatFastRunnerConfig()

    async def run(self, batch: ActiveChatBatch) -> ActiveChatRoundResult:
        """Execute one active chat fast-mode round."""
        try:
            messages, metadata = self._build_model_call_parts(batch)
        except Exception:
            logger.exception("Active chat prompt build failed for session %s", batch.session_id)
            return ActiveChatRoundResult(
                success=False,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="active_chat_prompt_build_failed",
            )
        tools = self._active_chat_tools(batch)

        result = await self._generate(
            batch,
            messages=messages,
            tools=tools,
            metadata=metadata,
            repair_attempt=0,
        )
        if result is None:
            return ActiveChatRoundResult(
                success=False,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="active_chat_model_call_failed",
            )
        if result.tool_calls:
            return _round_result_from_tool_loop(
                await self._tool_loop.execute(
                    result.tool_calls,
                    tool_manager=self._tool_manager,
                    instance_id=instance_id_from_session(batch.session_id),
                    session_id=batch.session_id,
                    run_id=str(result.execution_id or ""),
                ),
                message_log_ids=batch.message_log_ids,
                assistant_text=str(result.text or ""),
                tool_calls=result.tool_calls,
            )

        repair_batch, repaired = await self._repair_toolless_round(
            batch,
            messages=messages,
            tools=tools,
            metadata=metadata,
            first_result=result,
        )
        if repaired is None:
            return ActiveChatRoundResult(
                success=False,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="active_chat_toolless_repair_failed",
                restored_messages=list(repair_batch.messages),
            )
        if not repaired.tool_calls:
            return ActiveChatRoundResult(
                success=True,
                action=ActiveChatActionKind.RETRY_FAILED,
                reason="active_chat_toolless_after_repair",
                consumed_message_log_ids=repair_batch.message_log_ids,
            )
        return _round_result_from_tool_loop(
            await self._tool_loop.execute(
                repaired.tool_calls,
                tool_manager=self._tool_manager,
                instance_id=instance_id_from_session(batch.session_id),
                session_id=batch.session_id,
                run_id=str(repaired.execution_id or ""),
            ),
            message_log_ids=repair_batch.message_log_ids,
            assistant_text=str(repaired.text or ""),
            tool_calls=repaired.tool_calls,
        )

    def _build_model_call_parts(
        self,
        batch: ActiveChatBatch,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        source_messages = self._load_source_messages(batch)
        context = self._build_context(batch, source_messages)
        review_result_summary = _jsonable(batch.review_result_summary)
        instance_id = instance_id_from_session(batch.session_id)
        instance_config = self._resolve_instance_config(instance_id)
        metadata = {
            "active_chat_stage": self.stage_id,
            "message_log_ids": batch.message_log_ids,
            "interest_value": batch.active_chat_state.interest_value,
            "active_epoch": batch.active_chat_state.active_epoch,
            "review_result_summary": review_result_summary,
            **dict(getattr(context, "metadata", {}) or {}),
        }
        metadata = apply_instance_runtime_config_to_metadata(metadata, instance_config)
        runtime_target = resolve_runtime_model_target(
            llm=self._config.llm,
            default_llm=self._config.default_llm,
            route_id=self._config.route_id,
            model_id=self._config.model_id,
            resolved=instance_config,
            model_target_resolver=self._config.model_target_resolver,
        )
        component_ids_by_stage = self._component_ids_by_stage()
        build_result = self._prompt_registry.build_messages(
            PromptBuildRequest(
                caller=self._config.caller,
                workflow_id="active_chat",
                stage_id=self.stage_id,
                session_id=batch.session_id,
                instance_id=instance_id,
                route_id=(runtime_target.route_id or "") if runtime_target is not None else "",
                model_id=(runtime_target.model_id or "") if runtime_target is not None else "",
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
        return build_result.messages, dict(build_result.metadata)

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
            options=ActiveChatContextBuildOptions(
                self_platform_id=_self_platform_id_from_batch(batch),
                metadata={
                    "message_log_ids": batch.message_log_ids,
                    "active_epoch": batch.active_chat_state.active_epoch,
                    "interest_value": batch.active_chat_state.interest_value,
                },
            ),
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
        context_messages = list(getattr(context, "context_messages", []) or [])
        if context_messages:
            injections.append(
                PromptInjection(
                    stage=PromptStage.CONTEXT,
                    component_id="active_chat.fast_mode.context",
                    messages=context_messages,
                    priority=10,
                    metadata={"active_chat_stage": self.stage_id},
                )
            )
        if batch.conversation_summary:
            summary_component = self._prompt_registry.get_component(
                "active_chat.fast_mode.conversation_summary"
            )
            summary_prefix = summary_component.content if summary_component else "Active chat compacted conversation trace summary:"
            injections.append(
                PromptInjection(
                    stage=PromptStage.CONTEXT,
                    component_id="active_chat.fast_mode.conversation_summary",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": summary_prefix + "\n" + batch.conversation_summary,
                                }
                            ],
                        }
                    ],
                    priority=15,
                    metadata={"active_chat_stage": self.stage_id},
                )
            )
        conversation_messages = sanitize_conversation_trace_messages(
            list(batch.conversation_messages)
        )
        if conversation_messages:
            injections.append(
                PromptInjection(
                    stage=PromptStage.CONTEXT,
                    component_id="active_chat.fast_mode.conversation_trace",
                    messages=conversation_messages,
                    priority=20,
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
                    "主动聊天快速模式批次。通过工具决定一个即时动作。\n"
                    f"会话 ID: {batch.session_id}\n"
                    f"消息日志 ID 列表: {json.dumps(batch.message_log_ids, ensure_ascii=False)}\n"
                    f"当前兴趣值: {batch.active_chat_state.interest_value:.2f}"
                ),
            }
        ]
        if batch.review_result_summary is not None:
            content.extend(
                self._render_review_handoff(batch.review_result_summary)
            )
        instruction_content = list(getattr(context, "instruction_content", []) or [])
        if instruction_content:
            content.extend(instruction_content)
        else:
            formatted_text = self._format_source_messages(source_messages)
            if formatted_text:
                content.append({"type": "text", "text": "原始消息文本：\n" + formatted_text})
                return content
            content.append(
                {
                    "type": "text",
                    "text": "原始消息 JSON:\n"
                    + json.dumps(source_messages, ensure_ascii=False),
                }
            )
        return content

    def _render_review_handoff(self, review_result_summary: Any) -> list[dict[str, Any]]:
        """Render review handoff context as structured prompt sections."""
        if not isinstance(review_result_summary, ReviewHandoffContext):
            return [
                {
                    "type": "text",
                    "text": "审查移交摘要 JSON:\n"
                    + json.dumps(_jsonable(review_result_summary), ensure_ascii=False),
                }
            ]

        sections: list[dict[str, Any]] = []
        ctx = review_result_summary
        if ctx.overflow_summaries:
            prefix_comp = self._prompt_registry.get_component("active_chat.handoff.overflow")
            prefix = prefix_comp.content if prefix_comp else "之前的溢出消息摘要（较旧）："
            sections.append(
                {
                    "type": "text",
                    "text": f"{prefix}\n"
                    + "\n---\n".join(
                        _render_summary_handoff_entry(entry)
                        for entry in ctx.overflow_summaries
                    ),
                }
            )
        if ctx.block_digests:
            prefix_comp = self._prompt_registry.get_component("active_chat.handoff.digest")
            prefix = prefix_comp.content if prefix_comp else "之前的消息块局部摘要："
            digest_text = "\n".join(
                _render_summary_handoff_entry(entry) for entry in ctx.block_digests
            )
            sections.append(
                {"type": "text", "text": f"{prefix}\n" + digest_text}
            )
        if ctx.recent_active_chat_summary:
            prefix_comp = self._prompt_registry.get_component("active_chat.handoff.legacy")
            prefix = prefix_comp.content if prefix_comp else "上一次主动聊天会话的总结："
            sections.append(
                {
                    "type": "text",
                    "text": f"{prefix}\n"
                    + ctx.recent_active_chat_summary,
                }
            )
        if not sections:
            # Fallback: render explanation as JSON if no summaries available
            sections.append(
                {
                    "type": "text",
                    "text": "审查移交摘要 JSON:\n"
                    + json.dumps(_jsonable(ctx.explanation), ensure_ascii=False),
                }
            )
        return sections

    def _format_source_messages(self, source_messages: list[dict[str, Any]]) -> str:
        if self._message_formatter is None or not source_messages:
            return ""
        try:
            return self._message_formatter.format_text(
                source_messages,
                self._config.message_format_config
                or MessageFormatConfig(inject_record_id=True),
            )
        except Exception:
            logger.exception("Active chat message formatting failed")
            return ""

    def _active_chat_tools(self, batch: ActiveChatBatch) -> list[dict[str, Any]]:
        tools = self._tool_manager.build_request_tools(
            ["send_reply", "no_reply", "send_poke"],
            caller=self._config.caller,
            instance_id=instance_id_from_session(batch.session_id),
            session_id=batch.session_id,
            tags={CHAT_ACTION_TOOL_TAG},
        )
        active_tools = [_active_chat_tool_schema(tool) for tool in tools]
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
            instance_id = instance_id_from_session(batch.session_id)
            instance_config = self._resolve_instance_config(instance_id)
            return await self._model_runtime.generate(
                apply_instance_runtime_config_to_call(
                    ModelRuntimeCall(
                        route_id=self._config.route_id,
                        model_id=self._config.model_id,
                        caller=self._config.caller,
                        session_id=batch.session_id,
                        instance_id=instance_id,
                        purpose="active_chat_fast",
                        messages=messages,
                        tools=tools,
                        response_format=None,
                        metadata={
                            **dict(metadata),
                            "repair_attempt": repair_attempt,
                        },
                        params=dict(self._config.params),
                    ),
                    instance_config,
                    llm=self._config.llm,
                    default_llm=self._config.default_llm,
                    model_target_resolver=self._config.model_target_resolver,
                )
            )
        except ModelCallError:
            logger.exception("Active chat fast-mode model call failed for %s", batch.session_id)
            return None

    def _resolve_instance_config(self, instance_id: str) -> Any | None:
        resolver = self._config.instance_config_resolver
        if resolver is None or not instance_id:
            return None
        try:
            return resolver(instance_id)
        except Exception:
            logger.exception("Active chat instance runtime config resolution failed for %s", instance_id)
            return None

    async def _repair_toolless_round(
        self,
        batch: ActiveChatBatch,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        metadata: dict[str, Any],
        first_result: Any,
    ) -> tuple[ActiveChatBatch, Any | None]:
        repair_batch = await self._batch_for_repair(batch)
        if repair_batch.message_log_ids != batch.message_log_ids:
            try:
                repair_messages, metadata = self._build_model_call_parts(repair_batch)
            except Exception:
                logger.exception(
                    "Active chat repair prompt rebuild failed for session %s",
                    batch.session_id,
                )
                return repair_batch, None
        else:
            repair_messages = list(messages)
        if str(first_result.text or "").strip():
            repair_messages.append(
                {"role": "assistant", "content": str(first_result.text or "").strip()}
            )
        repair_component = self._prompt_registry.get_component("active_chat.fast_mode.repair")
        repair_text = repair_component.content if repair_component else ""
        if not repair_text.strip():
            return repair_batch, None
        repair_messages.append(
            {
                "role": "system",
                "content": [{"type": "text", "text": repair_text}],
            }
        )
        return repair_batch, await self._generate(
            repair_batch,
            messages=repair_messages,
            tools=tools,
            metadata=metadata,
            repair_attempt=1,
        )

    async def _batch_for_repair(self, batch: ActiveChatBatch) -> ActiveChatBatch:
        if self._pending_message_provider is None:
            return batch
        pending = self._pending_message_provider(batch)
        if isawaitable(pending):
            pending = await pending
        existing_ids = set(batch.message_log_ids)
        extra_messages = [
            message
            for message in pending
            if message.message_log_id not in existing_ids
        ]
        if not extra_messages:
            return batch
        latest = extra_messages[-1]
        return ActiveChatBatch(
            session_id=batch.session_id,
            messages=[*batch.messages, *extra_messages],
            active_chat_state=latest.active_chat_state or batch.active_chat_state,
            response_profile=latest.response_profile or batch.response_profile,
            mode=batch.mode,
            review_result_summary=batch.review_result_summary,
            conversation_summary=batch.conversation_summary,
            conversation_messages=batch.conversation_messages,
        )


@dataclass(slots=True)
class _FallbackActiveChatContext:
    session_id: str
    source_messages: list[dict[str, Any]]
    instruction_content: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


def _render_summary_handoff_entry(entry: SummaryHandoffEntry | str) -> str:
    if isinstance(entry, str):
        return entry
    labels: list[str] = []
    if entry.block_index is not None:
        labels.append(f"Block {entry.block_index}")
    if entry.msg_log_start is not None and entry.msg_log_end is not None:
        labels.append(f"msgid {entry.msg_log_start}-{entry.msg_log_end}")
    if entry.msg_count:
        labels.append(f"{entry.msg_count} messages")
    prefix = f"[{'; '.join(labels)}] " if labels else ""
    return prefix + entry.content


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


def _self_platform_id_from_batch(batch: ActiveChatBatch) -> str:
    for message in reversed(batch.messages):
        if message.self_platform_id:
            return message.self_platform_id
    return ""


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [_jsonable(item) for item in value]
    return value


def _round_result_from_tool_loop(
    tool_loop_result: Any,
    *,
    message_log_ids: list[int],
    assistant_text: str,
    tool_calls: list[dict[str, Any]],
) -> ActiveChatRoundResult:
    return replace(
        tool_loop_result.round_result,
        consumed_message_log_ids=list(message_log_ids),
        conversation_messages_delta=[
            _assistant_tool_call_message(
                assistant_text=assistant_text,
                tool_calls=tool_calls,
            ),
            *tool_loop_result.tool_messages,
        ],
    )


def _assistant_tool_call_message(
    *,
    assistant_text: str,
    tool_calls: list[dict[str, Any]],
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "role": "assistant",
        "content": assistant_text,
        "tool_calls": list(tool_calls),
    }
    return message


__all__ = [
    "ActiveChatContextBuilder",
    "ActiveChatFastRunner",
    "ActiveChatFastRunnerConfig",
    "ActiveChatMessageStore",
]
