"""Attention workflow — pure LLM call loop with tool execution and incremental merging."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.workflow.formatting import format_incremental_messages
from shinbot.agent.workflow.message_layout import AttentionWorkflowMessageLayout
from shinbot.agent.workflows.attention_tool_loop import execute_workflow_tool_calls

if TYPE_CHECKING:
    from shinbot.agent.media import MediaService
    from shinbot.agent.model_runtime import ModelRuntime
    from shinbot.agent.tools import ToolManager
    from shinbot.core.platform.adapter_manager import AdapterManager

logger = logging.getLogger(__name__)

# Maximum number of LLM round-trips per workflow run to prevent runaway loops.
_MAX_ITERATIONS = 30
_MAX_TOOLLESS_REPAIR_ATTEMPTS = 3

_TOOLLESS_RESPONSE_REPAIR_PROMPT = """
上一轮模型输出了裸文本，但 attention workflow 不会把裸文本发送给用户。
请重新决策，并必须调用工具：
- 要回复用户时调用 send_reply。
- 要不回复时调用 no_reply，并可写 internal_summary。
- 要发送轻量互动时调用 send_poke。
- 需要更多信息时可以先调用普通工具；终局工具必须单独调用。
不要再输出裸文本作为最终回答。
""".strip()


@dataclass(slots=True)
class WorkflowLoopResult:
    """Structured result of one attention workflow LLM loop."""

    messages: list[dict[str, Any]] = field(default_factory=list)
    no_reply: bool = False
    reply_sent: bool = False
    finish_reason: str = ""
    internal_summary: str = ""
    response_summary: str = ""
    tool_calls_log: list[dict[str, Any]] = field(default_factory=list)
    cursor_msg_id: int = 0
    incremental_count: int = 0
    iterations: int = 0


IncrementalMerger = Callable[
    [str, int, list[dict[str, Any]]],
    Awaitable[None],
]
ContextCompressor = Callable[[dict[str, Any] | None], Awaitable[str]]
IncrementalFetcher = Callable[[str, int], list[dict[str, Any]]]


class WorkflowRunner:
    """Executes the attention workflow LLM call loop.

    Receives pre-resolved config and initial messages from the coordinator.
    Runs the multi-step loop: LLM call → tool execution → incremental merge → repeat.
    Returns structured results; does NOT handle persistence, tracing, or state updates.
    """

    def __init__(
        self,
        *,
        model_runtime: ModelRuntime,
        prompt_registry: PromptRegistry,
        tool_manager: ToolManager,
        adapter_manager: AdapterManager,
        media_service: MediaService | None = None,
    ) -> None:
        self._model_runtime = model_runtime
        self._prompt_registry = prompt_registry
        self._tool_manager = tool_manager
        self._adapter_manager = adapter_manager
        self._media_service = media_service
        self._message_layout = AttentionWorkflowMessageLayout()

    async def run(
        self,
        *,
        session_id: str,
        instance_id: str,
        run_id: str,
        route_id: str,
        model_id: str,
        agent_uuid: str,
        persona_uuid: str,
        initial_messages: list[dict[str, Any]],
        all_tools: list[dict[str, Any]],
        snapshot_id: str,
        batch: list[dict[str, Any]],
        cursor_msg_id: int,
        response_profile: str,
        max_context_tokens: int,
        evict_ratio: float,
        effective_threshold: float,
        context_compression: ContextCompressor | None = None,
        fetch_incremental: IncrementalFetcher | None = None,
        on_incremental_merged: IncrementalMerger | None = None,
    ) -> WorkflowLoopResult:
        """Run the LLM call loop and return structured results."""
        conversation_messages = initial_messages
        tool_calls_log: list[dict[str, Any]] = []
        no_reply = False
        reply_sent = False
        finish_reason = ""
        internal_summary = ""
        response_summary = ""
        iteration = 0
        toolless_repair_attempts = 0
        incremental_count = 0

        for iteration in range(_MAX_ITERATIONS):
            # ── LLM call ───────────────────────────────────────────
            try:
                result = await self._model_runtime.generate(
                    ModelRuntimeCall(
                        route_id=route_id or None,
                        model_id=model_id or None,
                        caller="attention.workflow_runner",
                        session_id=session_id,
                        instance_id=instance_id,
                        purpose="attention_workflow",
                        messages=self._message_layout.build_model_call(conversation_messages),
                        tools=all_tools,
                        prompt_snapshot_id=snapshot_id,
                        metadata={
                            "agent_uuid": agent_uuid,
                            "persona_uuid": persona_uuid,
                            "workflow_run_id": run_id,
                            "iteration": iteration,
                            "response_profile": response_profile,
                        },
                    )
                )
            except ModelCallError:
                logger.exception(
                    "Workflow model call failed (iteration %d) for session %s",
                    iteration,
                    session_id,
                )
                return WorkflowLoopResult(
                    messages=conversation_messages,
                    finish_reason="model_error",
                    tool_calls_log=tool_calls_log,
                    cursor_msg_id=cursor_msg_id,
                    incremental_count=incremental_count,
                    iterations=iteration + 1,
                )

            has_tool_calls = bool(result.tool_calls)
            response_text = result.text.strip() if result.text else ""

            # ── Context compression ────────────────────────────────
            if context_compression is not None:
                await context_compression(result.usage)

            # ── No tool calls → repair and retry ───────────────────
            if not has_tool_calls:
                toolless_repair_attempts += 1
                if response_text:
                    logger.warning(
                        "Model produced raw text without send_reply tool for "
                        "session %s (iteration %d). Text will be repaired. "
                        "Model should use send_reply tool instead.",
                        session_id,
                        iteration,
                    )
                    conversation_messages.append(
                        {
                            "role": "assistant",
                            "content": response_text,
                        }
                    )
                if toolless_repair_attempts >= _MAX_TOOLLESS_REPAIR_ATTEMPTS:
                    finish_reason = "invalid_model_output"
                    break
                conversation_messages.append(
                    {
                        "role": "system",
                        "content": _TOOLLESS_RESPONSE_REPAIR_PROMPT,
                    }
                )
                continue

            # ── Process tool calls ─────────────────────────────────

            assistant_msg: dict[str, Any] = {"role": "assistant"}
            if response_text:
                assistant_msg["content"] = response_text
            assistant_msg["tool_calls"] = result.tool_calls
            conversation_messages.append(assistant_msg)

            tool_outcome = await execute_workflow_tool_calls(
                result.tool_calls,
                tool_manager=self._tool_manager,
                instance_id=instance_id,
                session_id=session_id,
                run_id=run_id,
            )
            tool_calls_log.extend(tool_outcome.tool_calls_log)
            no_reply = no_reply or tool_outcome.no_reply
            reply_sent = reply_sent or tool_outcome.reply_sent
            if tool_outcome.internal_summary:
                internal_summary = tool_outcome.internal_summary
            if tool_outcome.response_summary:
                response_summary = tool_outcome.response_summary

            conversation_messages.extend(tool_outcome.tool_messages)

            if tool_outcome.finish_reason:
                finish_reason = tool_outcome.finish_reason
                break

            # ── Incremental Merging ────────────────────────────────
            if fetch_incremental is not None:
                new_msgs = fetch_incremental(session_id, cursor_msg_id)
                if new_msgs:
                    cursor_msg_id = new_msgs[-1]["id"]
                    incremental_count += len(new_msgs)

                    if on_incremental_merged is not None:
                        await on_incremental_merged(
                            session_id,
                            cursor_msg_id,
                            new_msgs,
                        )

                    incremental_text = format_incremental_messages(
                        new_msgs,
                        media_service=self._media_service,
                    )
                    conversation_messages.append(
                        {
                            "role": "system",
                            "content": incremental_text,
                        }
                    )

                    logger.debug(
                        "Merged %d incremental messages for session %s (iteration %d)",
                        len(new_msgs),
                        session_id,
                        iteration,
                    )

        return WorkflowLoopResult(
            messages=conversation_messages,
            no_reply=no_reply,
            reply_sent=reply_sent,
            finish_reason=finish_reason,
            internal_summary=internal_summary,
            response_summary=response_summary,
            tool_calls_log=tool_calls_log,
            cursor_msg_id=cursor_msg_id,
            incremental_count=incremental_count,
            iterations=iteration + 1,
        )


__all__ = ["WorkflowLoopResult", "WorkflowRunner"]
