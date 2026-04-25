"""Conversation workflow runner for attention-triggered sessions."""

from __future__ import annotations

import time
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shinbot.agent.attention.engine import AttentionEngine
from shinbot.agent.attention.models import SessionAttentionState, WorkflowRunRecord
from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.prompt_manager import PromptAssemblyRequest, PromptRegistry
from shinbot.agent.prompt_manager.runtime_sync import (
    build_runtime_component_ids,
)
from shinbot.agent.workflow.formatting import (
    format_incremental_messages,
)
from shinbot.agent.workflow.model_resolution import resolve_model_target
from shinbot.agent.workflow.persistence import (
    persist_prompt_snapshot,
    persist_workflow_run,
)
from shinbot.agent.workflow.tool_loop import execute_workflow_tool_calls
from shinbot.core.bot_config import resolve_bot_runtime_config
from shinbot.utils.logger import get_logger

if TYPE_CHECKING:
    from shinbot.agent.context import ContextManager
    from shinbot.agent.media import MediaService
    from shinbot.agent.model_runtime import ModelRuntime
    from shinbot.agent.tools import ToolManager
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__)

# Maximum number of LLM round-trips per workflow run to prevent runaway loops.
_MAX_ITERATIONS = 30
_CONTINUATION_TTL_SECONDS = 15.0
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

CONTEXT_COMPRESSION_SYSTEM_PROMPT = """
You compress evicted ShinBot conversation context for later recall.

Keep only facts that matter for future turns: user preferences, unresolved tasks,
promises, relationship cues, notable emotions, key msgids, and media/sticker summaries.
Do not invent details. Prefer concise Chinese output.
""".strip()


@dataclass(slots=True)
class _WorkflowContinuation:
    """Short-lived in-memory continuation for one attention session."""

    session_id: str
    instance_id: str
    route_id: str
    model_id: str
    agent_uuid: str
    persona_uuid: str
    messages: list[dict[str, Any]] = field(default_factory=list)
    cursor_msg_id: int = 0
    expires_at: float = 0.0


class WorkflowRunner:
    """Executes a single attention-triggered workflow run.

    Implements a multi-step loop (Incremental Merging):
    - After each LLM call that produces tool calls, execute the tools.
    - Before the next LLM call, check for new messages that arrived during
      processing and merge them as incremental context.
    - Normal progress is either non-terminal tool use or one explicit terminal
      action: no_reply, send_reply, or send_poke.
    - Tool-less assistant text is fed back as an error message so the model can
      retry with no_reply, send_reply, send_poke, or another tool.
    - Finished runs keep their model/tool call messages for a short TTL. If the
      same session triggers again before expiry, the new batch resumes from
      that message state instead of starting from a cold prompt.
    - The max-iteration cap is a safety guard, not the primary exit path.

    All tool execution is routed through ToolManager for unified
    permission checking, auditing, and handler resolution.
    """

    def __init__(
        self,
        database: DatabaseManager,
        prompt_registry: PromptRegistry,
        model_runtime: ModelRuntime,
        tool_manager: ToolManager,
        attention_engine: AttentionEngine,
        adapter_manager: AdapterManager,
        media_service: MediaService | None = None,
        context_manager: ContextManager | None = None,
    ) -> None:
        self._database = database
        self._prompt_registry = prompt_registry
        self._model_runtime = model_runtime
        self._tool_manager = tool_manager
        self._engine = attention_engine
        self._adapter_manager = adapter_manager
        self._media_service = media_service
        self._context_manager = context_manager
        self._continuations: dict[str, _WorkflowContinuation] = {}

    async def run(
        self,
        session_id: str,
        batch: list[dict[str, Any]],
        attention_state: SessionAttentionState,
        *,
        instance_id: str = "",
        response_profile: str = "balanced",
    ) -> WorkflowRunRecord | None:
        """Execute the workflow for a claimed message batch."""
        run_id = str(uuid.uuid4())
        started_at = time.time()
        effective_threshold = self._engine.effective_threshold(attention_state)

        record = WorkflowRunRecord(
            id=run_id,
            session_id=session_id,
            instance_id=instance_id,
            response_profile=response_profile,
            batch_start_msg_id=batch[0]["id"] if batch else None,
            batch_end_msg_id=batch[-1]["id"] if batch else None,
            batch_size=len(batch),
            trigger_attention=attention_state.attention_value,
            effective_threshold=effective_threshold,
            started_at=started_at,
        )

        # ── Resolve config, agent, persona, model ──────────────────

        bot_config = self._resolve_bot_config(instance_id)
        if bot_config is None:
            logger.warning("Workflow skipped: no bot_config for instance %s", instance_id)
            return None

        resolved_config = resolve_bot_runtime_config(bot_config)
        agent_uuid = resolved_config.default_agent_uuid
        model_target = resolved_config.main_llm
        runtime_config = dict(resolved_config.config)
        max_context_tokens = int(runtime_config.get("max_context_tokens") or 32_000)
        evict_ratio = float(runtime_config.get("context_evict_ratio") or 0.6)
        if not agent_uuid or not model_target:
            logger.warning("Workflow skipped: missing agent or model config")
            return None

        agent = self._database.agents.get(agent_uuid)
        if agent is None:
            logger.warning("Workflow skipped: agent %s not found", agent_uuid)
            return None

        persona_uuid = str(agent.get("persona_uuid", "")).strip()
        persona = self._database.personas.get(persona_uuid) if persona_uuid else None
        if persona is None:
            logger.warning("Workflow skipped: persona %s not found", persona_uuid)
            return None

        route_id, model_id, model_context_window = resolve_model_target(
            self._database,
            model_target,
        )
        if not route_id and not model_id:
            logger.warning("Workflow skipped: model target %s not found", model_target)
            return None

        component_ids = self._build_component_ids(agent, persona)
        if not component_ids:
            logger.warning("Workflow skipped: no resolvable prompt components")
            return None

        # ── Initial prompt assembly ────────────────────────────────

        self_platform_id = str(attention_state.metadata.get("self_platform_id", "") or "").strip()

        request = PromptAssemblyRequest(
            caller="attention.workflow_runner",
            session_id=session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            model_context_window=model_context_window,
            component_overrides=component_ids,
            template_inputs={
                "session_id": session_id,
                "instance_id": instance_id,
                "platform": "",
                "message_text": "",
                "message_blocks": [],
                "user_id": "",
            },
            context_inputs=self._build_identity_context_inputs(
                session_id,
                batch,
                previous_summary=str(attention_state.metadata.get("internal_summary", "") or ""),
                self_platform_id=self_platform_id,
            ),
            metadata={
                "trigger": "attention_workflow",
                "agent_uuid": agent_uuid,
                "persona_uuid": persona_uuid,
                "workflow_run_id": run_id,
                "batch_size": len(batch),
                "response_profile": response_profile,
                "explicit_prompt_cache_enabled": resolved_config.explicit_prompt_cache_enabled,
                "now_ms": int(time.time() * 1000),
            },
        )

        try:
            assembly = self._prompt_registry.assemble(request)
        except Exception:
            logger.exception("Workflow prompt assembly failed for session %s", session_id)
            return None

        # Save prompt snapshot
        snapshot = self._prompt_registry.create_snapshot(assembly, request)
        try:
            persist_prompt_snapshot(self._database, snapshot)
        except Exception:
            logger.exception("Failed to persist prompt snapshot %s", snapshot.id)

        # Export attention tools (includes send_reply and no_reply)
        attention_tools = self._tool_manager.export_model_tools(
            caller="attention.workflow_runner",
            instance_id=instance_id,
            session_id=session_id,
            tags={"attention"},
        )
        all_tools = (assembly.tools or []) + attention_tools

        # ── Multi-step loop (Incremental Merging) ──────────────────
        #
        # The conversation_messages list accumulates the full multi-turn
        # exchange. It starts from the assembled prompt messages and grows
        # with assistant replies, tool results, and incremental user msgs.

        continuation = self._pop_continuation(
            session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            agent_uuid=agent_uuid,
            persona_uuid=persona_uuid,
            now=started_at,
        )
        if continuation is not None:
            conversation_messages = continuation.messages
            if batch:
                conversation_messages.append(
                    {
                        "role": "system",
                        "content": format_incremental_messages(
                            batch,
                            media_service=self._media_service,
                        ),
                    }
                )
            logger.debug(
                "Resumed workflow continuation: session=%s batch=%d",
                session_id,
                len(batch),
            )
        else:
            conversation_messages: list[dict[str, Any]] = list(assembly.messages)
        tool_calls_log: list[dict[str, Any]] = []
        no_reply = False
        reply_sent = False
        finish_reason = ""
        internal_summary = ""
        iteration = 0
        toolless_repair_attempts = 0

        # Track the high-water mark for incremental message fetching.
        # This is the id of the last message we have fed to the model.
        cursor_msg_id: int = batch[-1]["id"] if batch else (
            continuation.cursor_msg_id if continuation is not None else 0
        )

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
                        messages=conversation_messages,
                        tools=all_tools,
                        prompt_snapshot_id=snapshot.id,
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
                record.finish_reason = "model_error"
                record.finished_at = time.time()
                self._save_run(record)
                return record

            has_tool_calls = bool(result.tool_calls)
            response_text = result.text.strip() if result.text else ""
            if self._context_manager is not None:
                compressed_text = await self._maybe_build_context_compression(
                    session_id=session_id,
                    instance_id=instance_id,
                    run_id=run_id,
                    usage=result.usage,
                    runtime_config=runtime_config,
                    default_route_id=route_id,
                    default_model_id=model_id,
                    default_model_target=model_target,
                )
                self._context_manager.apply_usage_eviction(
                    session_id,
                    result.usage,
                    max_context_tokens=max_context_tokens,
                    evict_ratio=evict_ratio,
                    compressed_text=compressed_text,
                    now_ms=int(time.time() * 1000),
                )

            # ── No tool calls → repair and retry ───────────────────
            if not has_tool_calls:
                toolless_repair_attempts += 1
                # Tool-less assistant text is not a valid user-visible output in
                # the workflow runtime. Feed that back to the model and let it
                # retry with an explicit tool decision.
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

            # Append assistant message with tool calls to conversation
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
                record.replied = True
                record.response_summary = tool_outcome.response_summary

            conversation_messages.extend(tool_outcome.tool_messages)

            # Stop only when the tool batch explicitly ends or invalidates the round.
            if tool_outcome.finish_reason:
                finish_reason = tool_outcome.finish_reason
                break

            # ── Incremental Merging: fetch new messages ────────────
            #
            # Between tool execution and the next model call, check
            # whether new messages arrived in the session. If so,
            # inject them as a system-role notification so the model
            # understands this is supplementary context, not its own
            # prior output being echoed back.

            new_msgs = self._fetch_incremental_messages(session_id, cursor_msg_id)
            if new_msgs:
                cursor_msg_id = new_msgs[-1]["id"]
                # Atomically advance cursor and keep attention below the active
                # threshold for messages consumed by this in-flight workflow.
                self._engine.repo.update_consumed_cursor_and_cap_attention(
                    session_id,
                    cursor_msg_id,
                    effective_threshold,
                )
                if self._context_manager is not None:
                    self._context_manager.mark_read_until(session_id, cursor_msg_id)

                # Update record to reflect the expanded batch
                record.batch_end_msg_id = cursor_msg_id
                record.batch_size += len(new_msgs)

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

        # ── End of loop ────────────────────────────────────────────

        record.tool_calls = tool_calls_log
        record.finish_reason = finish_reason or "max_iterations"

        if reply_sent:
            # Reply already sent via send_reply tool; apply fatigue
            self._engine.apply_reply_fatigue(attention_state)
            self._engine.reset_unanswered_mention_streak(session_id)
            record.replied = True
        elif no_reply:
            # A no_reply decision closes the current loop as well. Reset streak so
            # the next round does not inherit capped mention escalation.
            self._engine.reset_unanswered_mention_streak(session_id)
        if no_reply and internal_summary:
            # Atomically store summary without overwriting attention_value
            self._engine.repo.set_metadata_key(
                session_id,
                "internal_summary",
                internal_summary,
            )
        if not reply_sent:
            record.replied = False
            if not record.response_summary:
                record.response_summary = internal_summary[:200] if internal_summary else ""

        record.finished_at = time.time()
        self._save_run(record)
        self._store_continuation(
            session_id=session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            agent_uuid=agent_uuid,
            persona_uuid=persona_uuid,
            messages=conversation_messages,
            cursor_msg_id=record.batch_end_msg_id or cursor_msg_id,
            now=record.finished_at,
        )

        self._engine.tracer.trace_workflow_result(
            session_id,
            run_id=run_id,
            replied=record.replied,
            tool_count=len(tool_calls_log),
            iterations=iteration + 1,
            duration_ms=(record.finished_at - record.started_at) * 1000,
        )

        logger.info(
            "Workflow complete: session=%s replied=%s finish_reason=%s batch=%d tools=%d "
            "iterations=%d duration=%.1fms",
            session_id,
            record.replied,
            record.finish_reason,
            record.batch_size,
            len(tool_calls_log),
            iteration + 1,
            (record.finished_at - record.started_at) * 1000,
        )
        return record

    # ── Short-lived continuation cache ──────────────────────────────

    def _pop_continuation(
        self,
        session_id: str,
        *,
        instance_id: str,
        route_id: str,
        model_id: str,
        agent_uuid: str,
        persona_uuid: str,
        now: float,
    ) -> _WorkflowContinuation | None:
        continuation = self._continuations.pop(session_id, None)
        if continuation is None:
            return None
        if continuation.expires_at < now:
            return None
        if (
            continuation.instance_id != instance_id
            or continuation.route_id != route_id
            or continuation.model_id != model_id
            or continuation.agent_uuid != agent_uuid
            or continuation.persona_uuid != persona_uuid
        ):
            return None
        continuation.messages = deepcopy(continuation.messages)
        return continuation

    def _store_continuation(
        self,
        *,
        session_id: str,
        instance_id: str,
        route_id: str,
        model_id: str,
        agent_uuid: str,
        persona_uuid: str,
        messages: list[dict[str, Any]],
        cursor_msg_id: int,
        now: float,
    ) -> None:
        self._continuations[session_id] = _WorkflowContinuation(
            session_id=session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            agent_uuid=agent_uuid,
            persona_uuid=persona_uuid,
            messages=deepcopy(messages),
            cursor_msg_id=cursor_msg_id,
            expires_at=now + _CONTINUATION_TTL_SECONDS,
        )

    # ── Incremental message fetching ────────────────────────────────

    def _fetch_incremental_messages(
        self,
        session_id: str,
        after_id: int,
    ) -> list[dict[str, Any]]:
        """Fetch messages that arrived after the given id."""
        with self._database.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM message_logs
                WHERE session_id = ? AND id > ? AND role = 'user'
                ORDER BY id ASC
                """,
                (session_id, after_id),
            ).fetchall()
        return [dict(row) for row in rows]

    # ── Config / prompt resolution helpers ──────────────────────────

    def _resolve_bot_config(self, instance_id: str) -> dict[str, Any] | None:
        return self._database.bot_configs.get_by_instance_id(instance_id)

    def _build_component_ids(
        self,
        agent: dict[str, Any],
        persona: dict[str, Any],
    ) -> list[str]:
        component_ids, unresolved_refs = build_runtime_component_ids(
            self._database,
            self._prompt_registry,
            persona=persona,
            agent=agent,
        )
        for prompt_ref in unresolved_refs:
            logger.warning("Skipped unresolvable prompt ref: %s", prompt_ref)
        return component_ids

    def _build_identity_context_inputs(
        self,
        session_id: str,
        batch: list[dict[str, Any]],
        *,
        previous_summary: str = "",
        self_platform_id: str = "",
    ) -> dict[str, Any]:
        session = self._database.sessions.get(session_id)
        platform = str((session or {}).get("platform", "") or "").strip()
        turns: list[dict[str, Any]] = []
        for msg in batch:
            turns.append(
                {
                    "role": "user",
                    "content": str(msg.get("raw_text", "") or "").strip() or "[无文本]",
                    "sender_id": str(msg.get("sender_id", "") or "").strip(),
                    "sender_name": str(msg.get("sender_name", "") or "").strip(),
                    "platform": platform,
                }
            )
        return {
            "platform": platform,
            "identity_turns": turns,
            "unread_records": [dict(msg) for msg in batch],
            "previous_summary": previous_summary.strip(),
            "self_user_id": self_platform_id,
        }

    async def _maybe_build_context_compression(
        self,
        *,
        session_id: str,
        instance_id: str,
        run_id: str,
        usage: dict[str, Any] | None,
        runtime_config: dict[str, Any],
        default_route_id: str,
        default_model_id: str,
        default_model_target: str,
    ) -> str:
        if self._context_manager is None:
            return ""

        preview = self._context_manager.preview_usage_eviction(
            session_id,
            usage,
            max_context_tokens=int(runtime_config.get("max_context_tokens") or 32_000),
            evict_ratio=float(runtime_config.get("context_evict_ratio") or 0.6),
        )
        if not preview.get("triggered"):
            return ""

        source_text = str(preview.get("source_text") or "").strip()
        if not source_text:
            return ""

        route_id = default_route_id
        model_id = default_model_id
        resolved_target = default_model_target

        compression_target = str(runtime_config.get("context_compression_llm") or "").strip()
        if compression_target:
            resolved_route_id, resolved_model_id, _resolved_window = resolve_model_target(
                self._database,
                compression_target,
            )
            if resolved_route_id or resolved_model_id:
                route_id = resolved_route_id
                model_id = resolved_model_id
                resolved_target = compression_target
            else:
                logger.warning(
                    "Context compression llm %s unavailable for instance %s; using %s",
                    compression_target,
                    instance_id,
                    default_model_target,
                )

        max_chars = int(runtime_config.get("context_compression_max_chars") or 240)
        user_prompt = (
            "请压缩下面这批即将被淘汰的历史上下文，供后续对话快速回忆。\n"
            "要求：\n"
            "1. 保留用户偏好、未完成事项、承诺、关键情绪和关系线索。\n"
            "2. 保留关键 msgid、图片/表情摘要和时间信息。\n"
            "3. 不要虚构，没有就省略。\n"
            f"4. 输出控制在 {max_chars} 个汉字以内。\n\n"
            f"{source_text}"
        )

        try:
            result = await self._model_runtime.generate(
                ModelRuntimeCall(
                    route_id=route_id or None,
                    model_id=model_id or None,
                    caller="attention.workflow_runner",
                    session_id=session_id,
                    instance_id=instance_id,
                    purpose="context_compression",
                    messages=[
                        {"role": "system", "content": CONTEXT_COMPRESSION_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    metadata={
                        "workflow_run_id": run_id,
                        "compression_target": resolved_target,
                        "source_block_ids": list(preview.get("source_block_ids") or []),
                    },
                )
            )
        except ModelCallError:
            logger.exception("Context compression model call failed for session %s", session_id)
            return ""

        return _clip_context_compression_text(result.text or "", max_chars)

    def _save_run(self, record: WorkflowRunRecord) -> None:
        try:
            persist_workflow_run(self._database, record)
        except Exception:
            logger.exception("Failed to save workflow run %s", record.id)


def _clip_context_compression_text(text: str, max_chars: int) -> str:
    normalized = text.strip()
    if not normalized or max_chars <= 0 or len(normalized) <= max_chars:
        return normalized
    if max_chars <= 3:
        return normalized[:max_chars]
    return normalized[: max_chars - 3].rstrip() + "..."
