"""Attention scheduler — semantic boundary wait and batch claim."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine
from shinbot.agent.attention.models import SessionAttentionState
from shinbot.utils.logger import get_logger

if TYPE_CHECKING:
    from shinbot.agent.context import ContextManager
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__)

# Type for the workflow dispatch callback
WorkflowDispatcher = Callable[
    [str, list[dict[str, Any]], SessionAttentionState, str],
    Coroutine[Any, Any, None],
]


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
        config: AttentionConfig,
        *,
        context_manager: ContextManager | None = None,
        workflow_dispatcher: WorkflowDispatcher | None = None,
    ) -> None:
        self._engine = engine
        self._database = database
        self._config = config
        self._context_manager = context_manager
        self._workflow_dispatcher = workflow_dispatcher

        # Per-session pending timer tasks
        self._pending_timers: dict[str, asyncio.Task[None]] = {}
        # Track which sender last triggered the threshold per session
        self._trigger_sender: dict[str, str] = {}
        # Lock per session to avoid race conditions on attention state
        self._locks: dict[str, asyncio.Lock] = {}
        # Track running workflow tasks to prevent overlapping runs
        self._running_workflows: dict[str, asyncio.Task[None]] = {}

    def set_workflow_dispatcher(self, dispatcher: WorkflowDispatcher) -> None:
        self._workflow_dispatcher = dispatcher

    def _get_lock(self, session_id: str) -> asyncio.Lock:
        if session_id not in self._locks:
            self._locks[session_id] = asyncio.Lock()
        return self._locks[session_id]

    async def on_message(
        self,
        session_id: str,
        msg_log_id: int,
        sender_id: str,
        *,
        response_profile: str = "balanced",
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
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
            )

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
                    session_id, action="skipped_running", sender_id=sender_id,
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
                        session_id, action="reset", sender_id=sender_id,
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
            return "immediate", 1.0, 0.0
        if profile == "passive":
            return "passive", 8.0, max(self._config.semantic_wait_ms, 1500.0)
        return "balanced", self._config.base_threshold, self._config.semantic_wait_ms

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

        # Step 3: commit cursor + consume attention only after successful dispatch.
        if dispatch_ok:
            threshold = self._engine.effective_threshold(state)
            self._engine.repo.commit_batch_consumption(session_id, last_msg_id, threshold)
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
            base_threshold=self._config.base_threshold,
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
        all_tasks = list(self._pending_timers.values()) + list(
            self._running_workflows.values()
        )
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        self._pending_timers.clear()
        self._running_workflows.clear()
        self._trigger_sender.clear()
