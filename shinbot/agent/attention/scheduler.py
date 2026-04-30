"""Attention scheduler — semantic boundary wait and batch claim."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine, AttentionEngineConfig
from shinbot.agent.attention.models import SessionAttentionState
from shinbot.agent.attention.trigger_strategy import (
    AttentionTriggerActions,
    AttentionTriggerContext,
    AttentionTriggerStrategy,
    default_attention_trigger_strategies,
)
from shinbot.schema.elements import Message, MessageElement
from shinbot.utils.logger import get_logger

if TYPE_CHECKING:
    from shinbot.agent.context import ContextManager
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__)

SELF_PLATFORM_ID_METADATA_KEY = "self_platform_id"

# Type for the workflow dispatch callback
WorkflowDispatcher = Callable[
    [str, list[dict[str, Any]], SessionAttentionState, str],
    Coroutine[Any, Any, None],
]


@dataclass(slots=True)
class AttentionSchedulerConfig:
    """Tunable parameters for scheduler-owned attention timing and profiles."""

    # Count mentions in this lookback window before passing the count to the engine.
    burst_window_seconds: float = 8.0

    # Wait for semantic boundary before dispatching an accumulated batch.
    semantic_wait_ms: float = 1000.0

    # Response-profile dispatch policy.
    balanced_base_threshold: float = 5.0
    immediate_base_threshold: float = 1.0
    passive_base_threshold: float = 8.0
    passive_min_wait_ms: float = 1500.0

    @classmethod
    def from_engine_config(cls, config: AttentionEngineConfig) -> AttentionSchedulerConfig:
        """Build scheduler defaults from the engine config's public threshold."""
        return cls(balanced_base_threshold=config.base_threshold)


class _SchedulerTriggerActions(AttentionTriggerActions):
    def __init__(self, scheduler: AttentionScheduler) -> None:
        self._scheduler = scheduler

    def accumulate_attention(
        self,
        context: AttentionTriggerContext,
        *,
        is_mentioned: bool = False,
        attention_multiplier: float = 1.0,
    ) -> None:
        self._scheduler._schedule_attention_update(
            context,
            is_mentioned=is_mentioned,
            attention_multiplier=attention_multiplier,
        )

    def dispatch_immediately(self, context: AttentionTriggerContext) -> None:
        self._scheduler._schedule_direct_dispatch(context)


class AttentionScheduler:
    """Manages semantic boundary waiting and batch claim for attention triggers.

    When a message causes attention to cross the threshold, the scheduler
    doesn't fire immediately. Instead it starts a short timer (semantic_wait_ms)
    to allow the sender to finish their multi-message thought. If the same
    sender keeps sending, the timer resets. Once the timer expires, the
    message batch is claimed and dispatched to the workflow runner.
    """

    def __init__(
        self,
        engine: AttentionEngine,
        database: DatabaseManager,
        config: AttentionSchedulerConfig | AttentionConfig | None = None,
        *,
        context_manager: ContextManager | None = None,
        workflow_dispatcher: WorkflowDispatcher | None = None,
        trigger_strategies: Iterable[AttentionTriggerStrategy] | None = None,
    ) -> None:
        self._engine = engine
        self._database = database
        self._config = self._resolve_config(config, engine.config)
        self._context_manager = context_manager
        self._workflow_dispatcher = workflow_dispatcher
        self._trigger_strategies = list(
            trigger_strategies
            if trigger_strategies is not None
            else default_attention_trigger_strategies()
        )

        # Per-session pending timer tasks
        self._pending_timers: dict[str, asyncio.Task[None]] = {}
        # Track which sender last triggered the threshold per session
        self._trigger_sender: dict[str, str] = {}
        # Lock per session to avoid race conditions on attention state
        self._locks: dict[str, asyncio.Lock] = {}
        # Track running workflow tasks to prevent overlapping runs
        self._running_workflows: dict[str, asyncio.Task[None]] = {}

    @staticmethod
    def _resolve_config(
        config: AttentionSchedulerConfig | AttentionConfig | None,
        engine_config: AttentionEngineConfig,
    ) -> AttentionSchedulerConfig:
        if config is None:
            return AttentionSchedulerConfig.from_engine_config(engine_config)
        if isinstance(config, AttentionSchedulerConfig):
            return config
        return AttentionSchedulerConfig.from_engine_config(config)

    def set_workflow_dispatcher(self, dispatcher: WorkflowDispatcher) -> None:
        self._workflow_dispatcher = dispatcher

    def add_trigger_strategy(
        self,
        strategy: AttentionTriggerStrategy,
        *,
        prepend: bool = True,
    ) -> None:
        """Register a trigger strategy in the scheduler's strategy chain."""
        if prepend:
            self._trigger_strategies.insert(0, strategy)
        else:
            self._trigger_strategies.append(strategy)

    def _get_lock(self, session_id: str) -> asyncio.Lock:
        if session_id not in self._locks:
            self._locks[session_id] = asyncio.Lock()
        return self._locks[session_id]

    def _get_trigger_strategies(self) -> list[AttentionTriggerStrategy]:
        strategies = getattr(self, "_trigger_strategies", None)
        if strategies is None:
            return list(default_attention_trigger_strategies())
        return list(strategies)

    def schedule_message(
        self,
        session_id: str,
        msg_log_id: int | None,
        sender_id: str,
        *,
        response_profile: str = "balanced",
        message: Message | Iterable[MessageElement],
        self_platform_id: str = "",
        is_reply_to_bot: bool = False,
        already_handled: bool = False,
        is_stopped: bool = False,
    ) -> bool:
        """Schedule attention work for one persisted pipeline message.

        Returns True when the attention system accepted ownership of the message
        by either updating attention state or directly dispatching a pending
        batch. Returns False when the caller should finish normal read handling.
        """
        if already_handled or is_stopped or msg_log_id is None:
            return False

        normalized_message = (
            message if isinstance(message, Message) else Message(elements=list(message))
        )
        context = AttentionTriggerContext(
            session_id=session_id,
            msg_log_id=msg_log_id,
            sender_id=sender_id,
            response_profile=response_profile,
            message=normalized_message,
            self_platform_id=self_platform_id,
            is_reply_to_bot=is_reply_to_bot,
        )
        actions = _SchedulerTriggerActions(self)

        for strategy in self._get_trigger_strategies():
            if strategy.schedule(context, actions):
                return True
        return False

    def _schedule_attention_update(
        self,
        context: AttentionTriggerContext,
        *,
        is_mentioned: bool = False,
        attention_multiplier: float = 1.0,
    ) -> None:
        asyncio.create_task(
            self.on_message(
                context.session_id,
                context.msg_log_id,
                context.sender_id,
                response_profile=context.response_profile,
                is_mentioned=is_mentioned,
                is_reply_to_bot=context.is_reply_to_bot,
                attention_multiplier=attention_multiplier,
                self_platform_id=context.self_platform_id,
            ),
            name=f"attention-{context.session_id}",
        )

    def _schedule_direct_dispatch(self, context: AttentionTriggerContext) -> None:
        asyncio.create_task(
            self.dispatch_immediately(
                context.session_id,
                response_profile=context.response_profile,
            ),
            name=f"attention-direct-{context.session_id}",
        )

    async def dispatch_immediately(
        self,
        session_id: str,
        *,
        response_profile: str = "disabled",
    ) -> None:
        """Dispatch pending messages without updating attention state."""
        while True:
            async with self._get_lock(session_id):
                running = self._running_workflows.get(session_id)
                if running is None or running.done():
                    task = asyncio.create_task(
                        self._do_dispatch(session_id, response_profile),
                        name=f"attention-direct-dispatch-{session_id}",
                    )
                    self._running_workflows[session_id] = task
                    return
            await running

    async def on_message(
        self,
        session_id: str,
        msg_log_id: int,
        sender_id: str,
        *,
        response_profile: str = "balanced",
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
        attention_multiplier: float = 1.0,
        self_platform_id: str = "",
    ) -> None:
        """Called by the pipeline for each incoming group message after persistence.

        Computes attention contribution and manages the semantic wait timer.
        """
        async with self._get_lock(session_id):
            profile_name, profile_threshold, profile_wait_ms = self._resolve_response_profile(
                response_profile
            )
            # Count recent mentions in the burst window for robust interrupt.
            # The current message is already persisted in message_logs before
            # on_message is called, so the DB count already includes it.
            recent_mention_count = self._count_recent_mentions(
                session_id,
                window_seconds=self._config.burst_window_seconds,
            )

            state, triggered = self._engine.update_attention(
                session_id,
                sender_id=sender_id,
                msg_log_id=msg_log_id,
                base_threshold=profile_threshold,
                is_mentioned=is_mentioned,
                is_reply_to_bot=is_reply_to_bot,
                recent_mention_count=recent_mention_count,
                attention_multiplier=attention_multiplier,
            )

            normalized_self_platform_id = str(self_platform_id or "").strip()
            if normalized_self_platform_id and (
                str(state.metadata.get(SELF_PLATFORM_ID_METADATA_KEY) or "").strip()
                != normalized_self_platform_id
            ):
                state.metadata[SELF_PLATFORM_ID_METADATA_KEY] = normalized_self_platform_id
                self._engine.repo.save_attention(state)

            if not triggered:
                return

            # Skip if a workflow is already running for this session
            running = self._running_workflows.get(session_id)
            if running is not None and not running.done():
                logger.debug(
                    "Workflow already running for session %s, skipping trigger",
                    session_id,
                )
                self._engine.tracer.trace_semantic_wait(
                    session_id,
                    action="skipped_running",
                    sender_id=sender_id,
                )
                return

            # Triggered — manage semantic wait timer
            existing_timer = self._pending_timers.get(session_id)
            trigger_sender = self._trigger_sender.get(session_id)

            if existing_timer is not None and not existing_timer.done():
                # Same sender still talking — reset the timer
                if trigger_sender == sender_id:
                    existing_timer.cancel()
                    logger.debug(
                        "Reset semantic wait for session %s (sender %s still active)",
                        session_id,
                        sender_id,
                    )
                    self._engine.tracer.trace_semantic_wait(
                        session_id,
                        action="reset",
                        sender_id=sender_id,
                    )
                else:
                    # Different sender joined — let the existing timer run
                    self._engine.tracer.trace_semantic_wait(
                        session_id,
                        action="skipped_different_sender",
                        sender_id=sender_id,
                    )
                    return

            self._trigger_sender[session_id] = sender_id
            wait_seconds = profile_wait_ms / 1000.0
            task = asyncio.create_task(
                self._semantic_wait_then_dispatch(session_id, wait_seconds, profile_name),
                name=f"attention-wait-{session_id}",
            )
            self._pending_timers[session_id] = task
            logger.debug(
                "Attention trigger armed: session=%s profile=%s wait_ms=%.0f threshold=%.2f",
                session_id,
                profile_name,
                profile_wait_ms,
                profile_threshold,
            )
            self._engine.tracer.trace_semantic_wait(
                session_id,
                action="armed",
                sender_id=sender_id,
                wait_ms=profile_wait_ms,
                profile=profile_name,
            )

    def _resolve_response_profile(self, response_profile: str) -> tuple[str, float, float]:
        profile = str(response_profile or "").strip().lower()
        if profile == "immediate":
            return "immediate", self._config.immediate_base_threshold, 0.0
        if profile == "passive":
            return (
                "passive",
                self._config.passive_base_threshold,
                max(self._config.semantic_wait_ms, self._config.passive_min_wait_ms),
            )
        return (
            "balanced",
            self._config.balanced_base_threshold,
            self._config.semantic_wait_ms,
        )

    async def _semantic_wait_then_dispatch(
        self,
        session_id: str,
        wait_seconds: float,
        response_profile: str,
    ) -> None:
        """Wait for the semantic boundary, then claim and dispatch."""
        try:
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            return
        finally:
            self._pending_timers.pop(session_id, None)
            self._trigger_sender.pop(session_id, None)

        task = asyncio.create_task(
            self._do_dispatch(session_id, response_profile),
            name=f"attention-dispatch-{session_id}",
        )
        self._running_workflows[session_id] = task

    async def _do_dispatch(self, session_id: str, response_profile: str) -> None:
        """Claim the batch and dispatch to workflow runner."""
        # Step 1: fetch pending messages (read-only, no DB writes yet)
        async with self._get_lock(session_id):
            batch, state, last_msg_id = self._fetch_pending_batch(session_id)

        if not batch or last_msg_id is None:
            logger.debug("Empty batch for session %s, skipping workflow", session_id)
            self._engine.tracer.trace_dispatch(session_id, action="empty")
            self._running_workflows.pop(session_id, None)
            return

        logger.info(
            "Dispatching workflow: session=%s batch_size=%d attention=%.3f",
            session_id,
            len(batch),
            state.attention_value,
        )
        self._engine.tracer.trace_dispatch(
            session_id,
            action="start",
            batch_size=len(batch),
            attention_value=state.attention_value,
        )
        threshold = self._engine.effective_threshold(state)
        self._engine.repo.consume_trigger_attention(session_id, threshold)

        # Step 2: dispatch — cursor NOT advanced yet.
        # On success we commit; on failure we leave the cursor where it was so
        # the next trigger can re-claim these messages.
        dispatch_ok = False
        try:
            if self._workflow_dispatcher is not None:
                await self._workflow_dispatcher(session_id, batch, state, response_profile)
            dispatch_ok = True
        except Exception:
            logger.exception("Workflow dispatch failed for session %s", session_id)
            self._engine.tracer.trace_dispatch(session_id, action="error")
        finally:
            self._running_workflows.pop(session_id, None)

        # Step 3: commit cursor only after successful dispatch. Attention was
        # already consumed when the trigger dispatch started, so incoming
        # messages during the workflow cannot keep the trigger value pinned high.
        if dispatch_ok:
            self._engine.repo.commit_batch_cursor(session_id, last_msg_id)
            if self._context_manager is not None:
                self._context_manager.mark_read_until(session_id, last_msg_id)
            self._engine.tracer.trace_batch_claim(
                session_id,
                batch_size=len(batch),
                cursor_before=state.last_consumed_msg_log_id,
                cursor_after=last_msg_id,
                residual_attention=max(state.attention_value - threshold, 0.0),
            )

    def _fetch_pending_batch(
        self,
        session_id: str,
    ) -> tuple[list[dict[str, Any]], SessionAttentionState, int | None]:
        """Fetch unconsumed messages without modifying persistent state."""
        state = self._engine.repo.get_or_create_attention(
            session_id,
            base_threshold=self._config.balanced_base_threshold,
        )

        after_id = state.last_consumed_msg_log_id

        with self._database.connect() as conn:
            if after_id is not None:
                rows = conn.execute(
                    """
                    SELECT * FROM message_logs
                    WHERE session_id = ? AND id > ? AND role = 'user'
                    ORDER BY id ASC
                    """,
                    (session_id, after_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM message_logs
                    WHERE session_id = ? AND role = 'user'
                    ORDER BY id ASC
                    """,
                    (session_id,),
                ).fetchall()

        if not rows:
            return [], state, None

        batch = [dict(row) for row in rows]
        return batch, state, batch[-1]["id"]

    def _count_recent_mentions(
        self,
        session_id: str,
        window_seconds: float,
    ) -> int:
        """Count messages with is_mentioned=1 in the recent burst window."""
        cutoff_ms = (time.time() - window_seconds) * 1000  # message_logs uses ms
        with self._database.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) as cnt FROM message_logs
                WHERE session_id = ? AND is_mentioned = 1 AND created_at >= ?
                """,
                (session_id, cutoff_ms),
            ).fetchone()
        return int(row["cnt"]) if row else 0

    async def shutdown(self) -> None:
        """Cancel all pending timers and running workflows."""
        for task in self._pending_timers.values():
            task.cancel()
        for task in self._running_workflows.values():
            task.cancel()
        all_tasks = list(self._pending_timers.values()) + list(self._running_workflows.values())
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        self._pending_timers.clear()
        self._running_workflows.clear()
        self._trigger_sender.clear()
