"""Workflow runner — batch LLM processing for attention-triggered conversations."""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import TYPE_CHECKING, Any

from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.prompting import (
    ContextStrategy,
    ContextStrategyBudget,
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.attention.engine import AttentionEngine
from shinbot.agent.attention.models import SessionAttentionState, WorkflowRunRecord
from shinbot.persistence.records import PromptSnapshotRecord
from shinbot.utils.logger import get_logger

if TYPE_CHECKING:
    from shinbot.agent.model_runtime import ModelRuntime
    from shinbot.agent.tools import ToolManager
    from shinbot.agent.tools.schema import ToolCallRequest
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__)

# Maximum number of LLM round-trips per workflow run to prevent runaway loops.
_MAX_ITERATIONS = 5

# ── CJK-aware tokenization for cross-talk detection ──────────────────

_CJK_RANGES = (
    "\u4e00-\u9fff"    # CJK Unified Ideographs
    "\u3400-\u4dbf"    # CJK Unified Ideographs Extension A
    "\uf900-\ufaff"    # CJK Compatibility Ideographs
    "\U00020000-\U0002a6df"  # Extension B
    "\U0002a700-\U0002b73f"  # Extension C
)
_CJK_PATTERN = re.compile(f"[{_CJK_RANGES}]")


def _tokenize(text: str) -> set[str]:
    """Extract keywords from text, supporting both CJK and space-delimited languages.

    For CJK text: extracts character bigrams (2-char sliding window).
    For non-CJK text: extracts whitespace-split words (length >= 2).
    Both are combined to handle mixed-language messages.
    """
    tokens: set[str] = set()

    # Space-delimited words (covers English, etc.)
    for word in text.split():
        w = word.lower().strip()
        if len(w) >= 2:
            tokens.add(w)

    # CJK character bigrams
    cjk_chars = _CJK_PATTERN.findall(text)
    for i in range(len(cjk_chars) - 1):
        tokens.add(cjk_chars[i] + cjk_chars[i + 1])

    return tokens


def _crosstalk_detect(batch: list[dict[str, Any]]) -> int:
    """Lightweight cross-talk detection via keyword co-occurrence.

    Returns estimated number of distinct topic threads (1 = single topic).
    Uses CJK-aware tokenization for proper Chinese/Japanese/Korean support.
    """
    if len(batch) <= 2:
        return 1

    sender_keywords: dict[str, set[str]] = {}
    for msg in batch:
        sender = msg.get("sender_id", "")
        text = msg.get("raw_text", "")
        words = _tokenize(text)
        if sender not in sender_keywords:
            sender_keywords[sender] = set()
        sender_keywords[sender] |= words

    if len(sender_keywords) <= 1:
        return 1

    senders = list(sender_keywords.keys())
    low_overlap_pairs = 0
    total_pairs = 0
    for i in range(len(senders)):
        for j in range(i + 1, len(senders)):
            a = sender_keywords[senders[i]]
            b = sender_keywords[senders[j]]
            if not a or not b:
                continue
            overlap = len(a & b) / min(len(a), len(b))
            total_pairs += 1
            if overlap < 0.15:
                low_overlap_pairs += 1

    if total_pairs == 0:
        return 1
    if low_overlap_pairs / total_pairs > 0.5:
        return min(len(sender_keywords), 3)
    return 1


class WorkflowRunner:
    """Executes a single attention-triggered workflow run.

    Implements a multi-step loop (Incremental Merging):
    - After each LLM call that produces tool calls, execute the tools.
    - Before the next LLM call, check for new messages that arrived during
      processing and merge them as incremental context.
    - The loop terminates when the model produces a final text response,
      calls no_reply or send_reply, or max iterations are reached.

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
    ) -> None:
        self._database = database
        self._prompt_registry = prompt_registry
        self._model_runtime = model_runtime
        self._tool_manager = tool_manager
        self._engine = attention_engine
        self._adapter_manager = adapter_manager

    async def run(
        self,
        session_id: str,
        batch: list[dict[str, Any]],
        attention_state: SessionAttentionState,
        *,
        instance_id: str = "",
    ) -> WorkflowRunRecord | None:
        """Execute the workflow for a claimed message batch."""
        run_id = str(uuid.uuid4())
        started_at = time.time()
        effective_threshold = self._engine.effective_threshold(attention_state)

        record = WorkflowRunRecord(
            id=run_id,
            session_id=session_id,
            instance_id=instance_id,
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

        agent_uuid = str(bot_config.get("default_agent_uuid", "")).strip()
        model_target = str(bot_config.get("main_llm", "")).strip()
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

        route_id, model_id, model_context_window = self._resolve_model_target(model_target)
        if not route_id and not model_id:
            logger.warning("Workflow skipped: model target %s not found", model_target)
            return None

        component_ids = self._build_component_ids(agent, persona)
        if not component_ids:
            logger.warning("Workflow skipped: no resolvable prompt components")
            return None

        context_strategy_id = self._resolve_context_strategy(agent)

        # ── Build initial batch context ────────────────────────────

        batch_context = self._format_batch_context(batch, session_id)

        topic_count = _crosstalk_detect(batch)
        if topic_count > 1:
            batch_context += (
                f"\n[系统提示：检测到当前批次可能包含 {topic_count} 个不相关话题线索]"
            )

        # ── Initial prompt assembly ────────────────────────────────

        request = PromptAssemblyRequest(
            caller="attention.workflow_runner",
            session_id=session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            model_context_window=model_context_window,
            context_strategy_id=context_strategy_id,
            component_overrides=component_ids,
            template_inputs={
                "session_id": session_id,
                "instance_id": instance_id,
                "platform": "",
                "message_text": batch_context,
                "user_id": "",
            },
            metadata={
                "trigger": "attention_workflow",
                "agent_uuid": agent_uuid,
                "persona_uuid": persona_uuid,
                "workflow_run_id": run_id,
                "batch_size": len(batch),
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
            self._database.prompt_snapshots.insert(
                PromptSnapshotRecord(
                    id=snapshot.id,
                    profile_id=snapshot.profile_id,
                    caller=snapshot.caller,
                    session_id=snapshot.session_id,
                    instance_id=snapshot.instance_id,
                    route_id=snapshot.route_id,
                    model_id=snapshot.model_id,
                    prompt_signature=snapshot.prompt_signature,
                    cache_key=snapshot.cache_key,
                    messages=snapshot.full_messages,
                    tools=snapshot.full_tools,
                    compatibility_used=snapshot.compatibility_used,
                    created_at=snapshot.timestamp,
                )
            )
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
                        },
                    )
                )
            except ModelCallError:
                logger.exception(
                    "Workflow model call failed (iteration %d) for session %s",
                    iteration, session_id,
                )
                record.finished_at = time.time()
                self._save_run(record)
                return record

            has_tool_calls = bool(result.tool_calls)
            response_text = result.text.strip() if result.text else ""

            # ── No tool calls → terminal response ──────────────────
            if not has_tool_calls:
                # Model produced text without tool calls — this is unexpected
                # in the tool-only reply model, but we log it as a fallback.
                if response_text:
                    logger.warning(
                        "Model produced raw text without send_reply tool for "
                        "session %s (iteration %d). Text will be discarded. "
                        "Model should use send_reply tool instead.",
                        session_id, iteration,
                    )
                break

            # ── Process tool calls ─────────────────────────────────

            # Append assistant message with tool calls to conversation
            assistant_msg: dict[str, Any] = {"role": "assistant"}
            if response_text:
                assistant_msg["content"] = response_text
            assistant_msg["tool_calls"] = result.tool_calls
            conversation_messages.append(assistant_msg)

            tool_results_for_model: list[dict[str, Any]] = []

            for tc in result.tool_calls:
                tc_id = tc.get("id", "")
                func = tc.get("function", {})
                tool_name = func.get("name", "")
                tool_args_raw = func.get("arguments", "{}")
                try:
                    tool_args = (
                        json.loads(tool_args_raw)
                        if isinstance(tool_args_raw, str)
                        else tool_args_raw
                    )
                except json.JSONDecodeError:
                    tool_args = {}

                tool_calls_log.append({"name": tool_name, "arguments": tool_args})

                # Execute tool through ToolManager for unified audit/permissions
                from shinbot.agent.tools.schema import ToolCallRequest

                tool_result = await self._tool_manager.execute(
                    ToolCallRequest(
                        tool_name=tool_name,
                        arguments=tool_args,
                        caller="attention.workflow_runner",
                        instance_id=instance_id,
                        session_id=session_id,
                        run_id=run_id,
                    )
                )

                # Serialize tool output for the model
                if tool_result.success:
                    output_str = json.dumps(tool_result.output, ensure_ascii=False)
                else:
                    output_str = json.dumps(
                        {"error": tool_result.error_message},
                        ensure_ascii=False,
                    )

                # Check for terminal signals
                if tool_name == "no_reply":
                    no_reply = True
                    internal_summary = str(tool_args.get("internal_summary", ""))
                elif tool_name == "send_reply" and tool_result.success:
                    reply_sent = True
                    record.replied = True
                    record.response_summary = str(
                        tool_args.get("text", "")
                    )[:200]

                # Append tool result message for the model
                tool_results_for_model.append({
                    "role": "tool",
                    "tool_call_id": tc_id,
                    "content": output_str,
                })

            conversation_messages.extend(tool_results_for_model)

            # If no_reply or send_reply was called, stop the loop
            if no_reply or reply_sent:
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

                # Update record to reflect the expanded batch
                record.batch_end_msg_id = cursor_msg_id
                record.batch_size += len(new_msgs)

                incremental_text = self._format_incremental_messages(new_msgs)
                conversation_messages.append({
                    "role": "system",
                    "content": incremental_text,
                })

                logger.debug(
                    "Merged %d incremental messages for session %s (iteration %d)",
                    len(new_msgs), session_id, iteration,
                )

        # ── End of loop ────────────────────────────────────────────

        record.tool_calls = tool_calls_log

        if reply_sent:
            # Reply already sent via send_reply tool; apply fatigue
            self._engine.apply_reply_fatigue(attention_state)
        elif no_reply or not reply_sent:
            if internal_summary:
                # Atomically store summary without overwriting attention_value
                self._engine.repo.set_metadata_key(
                    session_id, "internal_summary", internal_summary,
                )
            record.replied = False
            if not record.response_summary:
                record.response_summary = internal_summary[:200] if internal_summary else ""

        record.finished_at = time.time()
        self._save_run(record)

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

    @staticmethod
    def _format_incremental_messages(msgs: list[dict[str, Any]]) -> str:
        """Format incremental messages as a system-level notification.

        Uses system role to clearly differentiate from the model's own prior
        output, preventing confusion or repetition in multi-turn loops.
        """
        lines = [
            f"[补充上下文：在你处理上一步期间，会话中新增了 {len(msgs)} 条消息。"
            "请结合这些新消息重新评估是否需要回复以及回复内容。]"
        ]
        for msg in msgs:
            sender_name = msg.get("sender_name", "") or msg.get("sender_id", "unknown")
            text = msg.get("raw_text", "")
            mentioned = " (@bot)" if msg.get("is_mentioned") else ""
            lines.append(f"{sender_name}{mentioned}: {text}")
        return "\n".join(lines)

    # ── Config / prompt resolution helpers ──────────────────────────

    def _resolve_bot_config(self, instance_id: str) -> dict[str, Any] | None:
        return self._database.bot_configs.get_by_instance_id(instance_id)

    def _resolve_model_target(
        self, target: str,
    ) -> tuple[str, str, int | None]:
        route = self._database.model_registry.get_route(target)
        if route is not None and route["enabled"]:
            members = self._database.model_registry.list_route_members(target)
            enabled_members = [m for m in members if m["enabled"]]
            enabled_members.sort(
                key=lambda item: (item["priority"], -item["weight"], item["model_id"])
            )
            for member in enabled_members:
                model = self._database.model_registry.get_model(member["model_id"])
                if model is not None and model["enabled"]:
                    return target, "", model.get("context_window")
            return target, "", None

        model = self._database.model_registry.get_model(target)
        if model is not None and model["enabled"]:
            return "", target, model.get("context_window")

        return "", "", None

    def _build_component_ids(
        self,
        agent: dict[str, Any],
        persona: dict[str, Any],
    ) -> list[str]:
        component_ids: list[str] = []

        persona_prompt_uuid = str(persona.get("prompt_definition_uuid", "")).strip()
        if persona_prompt_uuid:
            payload = self._database.prompt_definitions.get(persona_prompt_uuid)
            if payload is not None:
                component_ids.append(self._sync_prompt_definition(payload))

        for prompt_ref in agent.get("prompts", []):
            normalized = str(prompt_ref).strip()
            payload = self._database.prompt_definitions.get(normalized)
            if payload is None:
                payload = self._database.prompt_definitions.get_by_prompt_id(normalized)
            if payload is None:
                component = self._prompt_registry.get_component(normalized)
                if component is not None:
                    component_ids.append(component.id)
                    continue
                logger.warning("Skipped unresolvable prompt ref: %s", prompt_ref)
                continue
            component_ids.append(self._sync_prompt_definition(payload))

        seen: set[str] = set()
        result: list[str] = []
        for cid in component_ids:
            if cid and cid not in seen:
                seen.add(cid)
                result.append(cid)
        return result

    def _sync_prompt_definition(self, payload: dict[str, Any]) -> str:
        metadata = dict(payload.get("metadata") or {})
        metadata.setdefault("display_name", str(payload.get("name", "")).strip())
        metadata.setdefault("description", str(payload.get("description", "")).strip())
        for key in ("owner_plugin_id", "owner_module", "module_path"):
            val = str(payload.get(key, "") or "").strip()
            if val:
                metadata.setdefault(key, val)

        component = PromptComponent(
            id=str(payload["prompt_id"]),
            stage=PromptStage(str(payload["stage"])),
            kind=PromptComponentKind(str(payload["type"])),
            version=str(payload.get("version", "1.0.0")),
            priority=int(payload.get("priority", 100)),
            enabled=bool(payload.get("enabled", True)),
            content=str(payload.get("content", "")),
            template_vars=list(payload.get("template_vars", [])),
            resolver_ref=str(payload.get("resolver_ref", "")),
            bundle_refs=list(payload.get("bundle_refs", [])),
            tags=list(payload.get("tags", [])),
            metadata=metadata,
        )
        self._prompt_registry.upsert_component(component)
        return component.id

    def _resolve_context_strategy(self, agent: dict[str, Any]) -> str:
        strategy_ref = str((agent.get("context_strategy") or {}).get("ref", "")).strip()
        if not strategy_ref:
            return ""
        strategy_payload = self._database.context_strategies.get(strategy_ref)
        if strategy_payload is None:
            return ""

        raw_config = dict(strategy_payload.get("config") or {})
        budget_payload = raw_config.get("budget", raw_config)
        if not isinstance(budget_payload, dict):
            budget_payload = {}

        budget = ContextStrategyBudget(
            max_context_tokens=int(budget_payload.get("max_context_tokens", 0) or 0),
            max_history_turns=int(budget_payload.get("max_history_turns", 0) or 0),
            truncate_policy=str(budget_payload.get("truncate_policy", "tail")),
            trigger_ratio=float(budget_payload.get("trigger_ratio", 0.5)),
            trim_turns=int(budget_payload.get("trim_turns", 2)),
        )
        strategy = ContextStrategy(
            id=str(strategy_payload.get("name", strategy_ref)),
            display_name=str(strategy_payload.get("name", "")),
            resolver_ref=str(strategy_payload.get("resolver_ref", "")),
            budget=budget,
        )
        self._prompt_registry.upsert_context_strategy(strategy)
        return strategy.id

    def _format_batch_context(
        self,
        batch: list[dict[str, Any]],
        session_id: str,
    ) -> str:
        lines: list[str] = []

        # Load previous internal_summary atomically
        state = self._engine.repo.get_attention(session_id)
        prev_summary = ""
        if state is not None:
            prev_summary = state.metadata.get("internal_summary", "")
        if prev_summary:
            lines.append(f"[上轮观察摘要：{prev_summary}]")
            lines.append("")
            # Atomically clear the summary without touching attention_value
            self._engine.repo.clear_metadata_key(session_id, "internal_summary")

        lines.append(f"[以下是会话中 {len(batch)} 条未消费消息]")
        for msg in batch:
            sender_name = msg.get("sender_name", "") or msg.get("sender_id", "unknown")
            text = msg.get("raw_text", "")
            mentioned = " (@bot)" if msg.get("is_mentioned") else ""
            lines.append(f"{sender_name}{mentioned}: {text}")

        return "\n".join(lines)

    def _save_run(self, record: WorkflowRunRecord) -> None:
        try:
            self._database.workflow_runs.insert(record)
        except Exception:
            logger.exception("Failed to save workflow run %s", record.id)
