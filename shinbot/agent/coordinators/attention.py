"""Attention workflow coordinator — config resolution, continuation cache, post-loop state."""

from __future__ import annotations

import time
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shinbot.agent.attention.engine import AttentionEngine
from shinbot.agent.attention.models import SessionAttentionState, WorkflowRunRecord
from shinbot.agent.model_runtime import ModelRuntimeCall
from shinbot.agent.prompt_manager import (
    PromptBuildRequest,
    PromptContextPolicy,
    PromptInjection,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.prompt_manager.runtime_sync import (
    build_runtime_component_ids,
)
from shinbot.agent.workflow.formatting import format_incremental_messages
from shinbot.agent.workflow.message_layout import (
    AttentionWorkflowMessageLayout,
    mark_latest_workflow_segment_boundary,
)
from shinbot.agent.workflow.model_resolution import resolve_model_target
from shinbot.agent.workflow.persistence import (
    persist_prompt_snapshot,
    persist_workflow_run,
)
from shinbot.agent.workflows.attention import WorkflowRunner
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

_CONTINUATION_TTL_SECONDS = 15.0

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
    explicit_prompt_cache_enabled: bool = False


class AttentionCoordinator:
    """Orchestrates one attention-triggered workflow run.

    Manages config resolution, continuation cache, prompt assembly,
    context compression, post-loop state updates (fatigue, streak, summary),
    persistence, and tracing. Delegates the LLM call loop to WorkflowRunner.
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
        self._message_layout = AttentionWorkflowMessageLayout()
        self._workflow_runner = WorkflowRunner(
            model_runtime=model_runtime,
            prompt_registry=prompt_registry,
            tool_manager=tool_manager,
            adapter_manager=adapter_manager,
            media_service=media_service,
        )

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

        request = PromptBuildRequest(
            caller="attention.workflow_runner",
            workflow_id="attention",
            stage_id="attention_workflow",
            session_id=session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            model_context_window=model_context_window,
            component_ids=component_ids,
            template_inputs={
                "session_id": session_id,
                "instance_id": instance_id,
                "platform": "",
                "message_text": "",
                "message_blocks": [],
                "user_id": "",
            },
            context_policy=PromptContextPolicy.MEMORY,
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
            prompt_result = self._prompt_registry.build_messages(request)
        except Exception:
            logger.exception("Workflow prompt assembly failed for session %s", session_id)
            return None

        # Export attention tools (includes send_reply and no_reply)
        attention_tools = self._tool_manager.export_model_tools(
            caller="attention.workflow_runner",
            instance_id=instance_id,
            session_id=session_id,
            tags={"attention"},
        )
        all_tools = (prompt_result.tools or []) + attention_tools

        # ── Prepare initial messages ───────────────────────────────

        initial_messages = self._message_layout.build_initial(
            prompt_result,
            explicit_prompt_cache_enabled=resolved_config.explicit_prompt_cache_enabled,
        )

        # Save prompt snapshot after workflow-specific message rearrangement so
        # audits match the actual first model-facing prompt.
        snapshot = self._prompt_registry.create_build_snapshot(prompt_result, request)
        snapshot.full_messages = self._message_layout.build_model_call(initial_messages)
        snapshot.full_tools = all_tools
        try:
            persist_prompt_snapshot(self._database, snapshot)
        except Exception:
            logger.exception("Failed to persist prompt snapshot %s", snapshot.id)

        # ── Continuation cache ─────────────────────────────────────

        continuation = self._pop_continuation(
            session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            agent_uuid=agent_uuid,
            persona_uuid=persona_uuid,
            explicit_prompt_cache_enabled=resolved_config.explicit_prompt_cache_enabled,
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
            cursor_msg_id = continuation.cursor_msg_id
            logger.debug(
                "Resumed workflow continuation: session=%s batch=%d",
                session_id,
                len(batch),
            )
        else:
            conversation_messages = initial_messages
            cursor_msg_id = batch[-1]["id"] if batch else 0

        # ── Delegate to workflow runner ────────────────────────────

        context_compression = self._build_context_compression_callback(
            session_id=session_id,
            instance_id=instance_id,
            run_id=run_id,
            runtime_config=runtime_config,
            default_route_id=route_id,
            default_model_id=model_id,
            default_model_target=model_target,
            max_context_tokens=max_context_tokens,
            evict_ratio=evict_ratio,
        )

        async def on_incremental_merged(
            sid: str, cursor: int, _msgs: list[dict[str, Any]]
        ) -> None:
            self._engine.repo.update_consumed_cursor_and_cap_attention(
                sid, cursor, effective_threshold,
            )
            if self._context_manager is not None:
                self._context_manager.mark_read_until(sid, cursor)

        loop_result = await self._workflow_runner.run(
            session_id=session_id,
            instance_id=instance_id,
            run_id=run_id,
            route_id=route_id,
            model_id=model_id,
            agent_uuid=agent_uuid,
            persona_uuid=persona_uuid,
            initial_messages=conversation_messages,
            all_tools=all_tools,
            snapshot_id=snapshot.id,
            batch=batch,
            cursor_msg_id=cursor_msg_id,
            response_profile=response_profile,
            max_context_tokens=max_context_tokens,
            evict_ratio=evict_ratio,
            effective_threshold=effective_threshold,
            context_compression=context_compression,
            fetch_incremental=self._fetch_incremental_messages,
            on_incremental_merged=on_incremental_merged,
        )

        # ── Post-loop state updates ────────────────────────────────

        record.tool_calls = loop_result.tool_calls_log
        record.finish_reason = loop_result.finish_reason or "max_iterations"
        record.batch_end_msg_id = loop_result.cursor_msg_id or record.batch_end_msg_id
        record.batch_size += loop_result.incremental_count

        if loop_result.reply_sent:
            self._engine.apply_reply_fatigue(attention_state)
            self._engine.reset_unanswered_mention_streak(session_id)
            record.replied = True
        elif loop_result.no_reply:
            self._engine.reset_unanswered_mention_streak(session_id)
        if loop_result.no_reply and loop_result.internal_summary:
            self._engine.repo.set_metadata_key(
                session_id,
                "internal_summary",
                loop_result.internal_summary,
            )
        if not loop_result.reply_sent:
            record.replied = False
            if not record.response_summary:
                record.response_summary = (
                    loop_result.internal_summary[:200] if loop_result.internal_summary else ""
                )
        elif loop_result.response_summary:
            record.replied = True
            record.response_summary = loop_result.response_summary

        record.finished_at = time.time()
        self._save_run(record)
        self._store_continuation(
            session_id=session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            agent_uuid=agent_uuid,
            persona_uuid=persona_uuid,
            messages=loop_result.messages,
            cursor_msg_id=record.batch_end_msg_id or loop_result.cursor_msg_id,
            explicit_prompt_cache_enabled=resolved_config.explicit_prompt_cache_enabled,
            finish_reason=record.finish_reason,
            now=record.finished_at,
        )

        self._engine.tracer.trace_workflow_result(
            session_id,
            run_id=run_id,
            replied=record.replied,
            tool_count=len(loop_result.tool_calls_log),
            iterations=loop_result.iterations,
            duration_ms=(record.finished_at - record.started_at) * 1000,
        )

        logger.info(
            "Workflow complete: session=%s replied=%s finish_reason=%s batch=%d tools=%d "
            "iterations=%d duration=%.1fms",
            session_id,
            record.replied,
            record.finish_reason,
            record.batch_size,
            len(loop_result.tool_calls_log),
            loop_result.iterations,
            (record.finished_at - record.started_at) * 1000,
        )
        return record

    # ── Context compression callback ──────────────────────────────

    def _build_context_compression_callback(
        self,
        *,
        session_id: str,
        instance_id: str,
        run_id: str,
        runtime_config: dict[str, Any],
        default_route_id: str,
        default_model_id: str,
        default_model_target: str,
        max_context_tokens: int,
        evict_ratio: float,
    ):
        """Return an async callback that compresses and evicts context, or None."""
        if self._context_manager is None:
            return None

        async def compress(usage: dict[str, Any] | None) -> str:
            compressed_text = await self._maybe_build_context_compression(
                session_id=session_id,
                instance_id=instance_id,
                run_id=run_id,
                usage=usage,
                runtime_config=runtime_config,
                default_route_id=default_route_id,
                default_model_id=default_model_id,
                default_model_target=default_model_target,
            )
            self._context_manager.apply_usage_eviction(
                session_id,
                usage,
                max_context_tokens=max_context_tokens,
                evict_ratio=evict_ratio,
                compressed_text=compressed_text,
                now_ms=int(time.time() * 1000),
            )
            return compressed_text

        return compress

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
        explicit_prompt_cache_enabled: bool,
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
            or continuation.explicit_prompt_cache_enabled != explicit_prompt_cache_enabled
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
        explicit_prompt_cache_enabled: bool,
        finish_reason: str,
        now: float,
    ) -> None:
        if finish_reason not in {"send_reply", "send_poke", "no_reply"}:
            self._continuations.pop(session_id, None)
            return

        continuation_messages = deepcopy(messages)
        if explicit_prompt_cache_enabled:
            continuation_messages = mark_latest_workflow_segment_boundary(continuation_messages)
        self._continuations[session_id] = _WorkflowContinuation(
            session_id=session_id,
            instance_id=instance_id,
            route_id=route_id,
            model_id=model_id,
            agent_uuid=agent_uuid,
            persona_uuid=persona_uuid,
            messages=continuation_messages,
            cursor_msg_id=cursor_msg_id,
            expires_at=now + _CONTINUATION_TTL_SECONDS,
            explicit_prompt_cache_enabled=explicit_prompt_cache_enabled,
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
        prompt_result = self._prompt_registry.build_messages(
            PromptBuildRequest(
                caller="attention.workflow_runner",
                workflow_id="attention",
                stage_id="context_compression",
                identity_enabled=False,
                session_id=session_id,
                instance_id=instance_id,
                route_id=route_id,
                model_id=model_id,
                injections=[
                    PromptInjection(
                        stage=PromptStage.SYSTEM_BASE,
                        component_id="attention.context_compression.system",
                        text=CONTEXT_COMPRESSION_SYSTEM_PROMPT,
                        priority=10,
                    ),
                    PromptInjection(
                        stage=PromptStage.INSTRUCTIONS,
                        component_id="attention.context_compression.instruction",
                        text=user_prompt,
                        priority=10,
                    ),
                ],
                context_policy=PromptContextPolicy.DISABLED,
                metadata={
                    "workflow_run_id": run_id,
                    "compression_target": resolved_target,
                    "source_block_ids": list(preview.get("source_block_ids") or []),
                },
            )
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
                    messages=prompt_result.messages,
                    metadata=dict(prompt_result.metadata),
                )
            )
        except Exception:
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
