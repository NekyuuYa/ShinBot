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
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__)

# Type for the workflow dispatch callback
WorkflowDispatcher = Callable[
    [str, list[dict[str, Any]], SessionAttentionState],
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
        workflow_dispatcher: WorkflowDispatcher | None = None,
    ) -> None:
        self._engine = engine
        self._database = database
        self._config = config
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
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
    ) -> None:
        """Called by the pipeline for each incoming group message after persistence.

        Computes attention contribution and manages the semantic wait timer.
        """
        async with self._get_lock(session_id):
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
                else:
                    # Different sender joined — let the existing timer run
                    return

            self._trigger_sender[session_id] = sender_id
            wait_seconds = self._config.semantic_wait_ms / 1000.0
            task = asyncio.create_task(
                self._semantic_wait_then_dispatch(session_id, wait_seconds),
                name=f"attention-wait-{session_id}",
            )
            self._pending_timers[session_id] = task

    async def _semantic_wait_then_dispatch(
        self,
        session_id: str,
        wait_seconds: float,
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
            self._do_dispatch(session_id),
            name=f"attention-dispatch-{session_id}",
        )
        self._running_workflows[session_id] = task

    async def _do_dispatch(self, session_id: str) -> None:
        """Claim the batch and dispatch to workflow runner."""
        try:
            async with self._get_lock(session_id):
                batch, state = self._claim_batch(session_id)

            if not batch:
                logger.debug("Empty batch for session %s, skipping workflow", session_id)
                return

            logger.info(
                "Dispatching workflow: session=%s batch_size=%d attention=%.3f",
                session_id,
                len(batch),
                state.attention_value,
            )

            if self._workflow_dispatcher is not None:
                await self._workflow_dispatcher(session_id, batch, state)
        except Exception:
            logger.exception("Workflow dispatch failed for session %s", session_id)
        finally:
            self._running_workflows.pop(session_id, None)

    def _claim_batch(
        self,
        session_id: str,
    ) -> tuple[list[dict[str, Any]], SessionAttentionState]:
        """Claim unconsumed messages and update cursor positions."""
        state = self._engine.repo.get_or_create_attention(
            session_id,
            base_threshold=self._config.base_threshold,
        )

        after_id = state.last_consumed_msg_log_id

        # Fetch messages after the last consumed position
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
            return [], state

        batch = [dict(row) for row in rows]
        last_msg_id = batch[-1]["id"]

        # Update cursors
        state.last_trigger_msg_log_id = last_msg_id
        state.last_consumed_msg_log_id = last_msg_id

        # Consume batch: deduct threshold, preserve residual
        state = self._engine.consume_batch(state)
        self._engine.repo.save_attention(state)

        return batch, state

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
