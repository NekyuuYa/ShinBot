"""Conversation workflow runner for attention-triggered sessions."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

from shinbot.agent.attention.engine import AttentionEngine
from shinbot.agent.attention.models import SessionAttentionState, WorkflowRunRecord
from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.prompt_manager import PromptAssemblyRequest, PromptRegistry
from shinbot.agent.prompt_manager.runtime_sync import (
    build_runtime_component_ids,
    ensure_runtime_context_strategy,
)
from shinbot.agent.workflow.formatting import (
    crosstalk_detect,
    format_batch_context_blocks,
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
_MAX_ITERATIONS = 5


class WorkflowRunner:
    """Executes a single attention-triggered workflow run.

    Implements a multi-step loop (Incremental Merging):
    - After each LLM call that produces tool calls, execute the tools.
    - Before the next LLM call, check for new messages that arrived during
      processing and merge them as incremental context.
    - The loop terminates when the model produces a final text response,
      calls no_reply, calls send_reply with ``terminate_round=true``, or max
      iterations are reached.

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

        context_strategy_id = self._resolve_context_strategy(agent)

        # ── Build initial batch context ────────────────────────────

        batch_context_blocks = format_batch_context_blocks(
            batch,
            session_id=session_id,
            attention_repo=self._engine.repo,
            media_service=self._media_service,
        )
        batch_context = "\n".join(block["text"] for block in batch_context_blocks)

        topic_count = crosstalk_detect(batch)
        if topic_count > 1:
            crosstalk_hint = f"[系统提示：检测到当前批次可能包含 {topic_count} 个不相关话题线索]"
            batch_context_blocks.append({"type": "text", "text": crosstalk_hint})
            batch_context += f"\n{crosstalk_hint}"

        # ── Initial prompt assembly ────────────────────────────────

        request = PromptAssemblyRequest(
            caller="attention.workflow_runner",
            session_id=session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            model_context_window=model_context_window,
            hydrate_session_context=True,
            include_context_messages=True,
            context_strategy_id=context_strategy_id,
            component_overrides=component_ids,
            template_inputs={
                "session_id": session_id,
                "instance_id": instance_id,
                "platform": "",
                "message_text": batch_context,
                "message_blocks": batch_context_blocks,
                "user_id": "",
            },
            context_inputs=self._build_batch_context_inputs(session_id, batch),
            metadata={
                "trigger": "attention_workflow",
                "agent_uuid": agent_uuid,
                "persona_uuid": persona_uuid,
                "workflow_run_id": run_id,
                "batch_size": len(batch),
                "response_profile": response_profile,
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

        conversation_messages: list[dict[str, Any]] = list(assembly.messages)
        tool_calls_log: list[dict[str, Any]] = []
        no_reply = False
        reply_sent = False
        terminate_round = False
        internal_summary = ""
        iteration = 0

        # Track the high-water mark for incremental message fetching.
        # This is the id of the last message we have fed to the model.
        cursor_msg_id: int = batch[-1]["id"] if batch else 0

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
                record.finished_at = time.time()
                self._save_run(record)
                return record

            has_tool_calls = bool(result.tool_calls)
            response_text = result.text.strip() if result.text else ""

            # ── No tool calls → terminal stop ──────────────────────
            if not has_tool_calls:
                # Tool-less assistant text is not a valid user-visible output in
                # the workflow runtime. Log it for debugging and discard it.
                if response_text:
                    logger.warning(
                        "Model produced raw text without send_reply tool for "
                        "session %s (iteration %d). Text will be discarded. "
                        "Model should use send_reply tool instead.",
                        session_id,
                        iteration,
                    )
                break

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
            terminate_round = terminate_round or tool_outcome.terminate_round
            if tool_outcome.internal_summary:
                internal_summary = tool_outcome.internal_summary
            if tool_outcome.response_summary:
                record.replied = True
                record.response_summary = tool_outcome.response_summary

            conversation_messages.extend(tool_outcome.tool_messages)

            # Stop only when the tool batch explicitly ends the round.
            if no_reply or terminate_round:
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
                # Atomically advance cursor — safe against concurrent on_message
                self._engine.repo.update_consumed_cursor(session_id, cursor_msg_id)
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

        if reply_sent:
            # Reply already sent via send_reply tool; apply fatigue
            self._engine.reset_unanswered_mention_streak(session_id)
            self._engine.apply_reply_fatigue(attention_state)
            record.replied = True
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

        self._engine.tracer.trace_workflow_result(
            session_id,
            run_id=run_id,
            replied=record.replied,
            tool_count=len(tool_calls_log),
            iterations=iteration + 1,
            duration_ms=(record.finished_at - record.started_at) * 1000,
        )

        logger.info(
            "Workflow complete: session=%s replied=%s batch=%d tools=%d "
            "iterations=%d duration=%.1fms",
            session_id,
            record.replied,
            record.batch_size,
            len(tool_calls_log),
            iteration + 1,
            (record.finished_at - record.started_at) * 1000,
        )
        return record

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

    def _resolve_context_strategy(self, agent: dict[str, Any]) -> str:
        return ensure_runtime_context_strategy(
            self._database,
            self._prompt_registry,
            agent=agent,
        )

    def _build_batch_context_inputs(
        self,
        session_id: str,
        batch: list[dict[str, Any]],
    ) -> dict[str, Any]:
        session = self._database.sessions.get(session_id)
        platform = str((session or {}).get("platform", "") or "").strip()
        turns: list[dict[str, Any]] = []
        for msg in batch:
            text = str(msg.get("raw_text", "") or "").strip()
            if not text:
                text = "[无文本]"
            turns.append(
                {
                    "role": "user",
                    "content": text,
                    "sender_id": str(msg.get("sender_id", "") or "").strip(),
                    "sender_name": str(msg.get("sender_name", "") or "").strip(),
                    "platform": platform,
                }
            )
        return {
            "platform": platform,
            "history_turns": turns,
            # Hydrated session context replaces history_turns with read history.
            # Keep the active batch separately so identity prompts still learn
            # the current speakers without duplicating the batch as history.
            "identity_turns": turns,
        }

    def _save_run(self, record: WorkflowRunRecord) -> None:
        try:
            persist_workflow_run(self._database, record)
        except Exception:
            logger.exception("Failed to save workflow run %s", record.id)
