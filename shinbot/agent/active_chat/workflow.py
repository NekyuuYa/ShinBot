"""Active chat workflow orchestration."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from inspect import isawaitable

from shinbot.agent.active_chat.attention import ActiveChatAttention, ActiveChatAttentionConfig
from shinbot.agent.active_chat.models import (
    ActiveChatAttentionState,
    ActiveChatBatch,
    ActiveChatMessageSignal,
    ActiveChatNotifyResult,
    ActiveChatRoundResult,
    ActiveChatStartResult,
)
from shinbot.agent.scheduler.models import ActiveChatState

logger = logging.getLogger(__name__)

ActiveChatRoundHandler = Callable[
    [ActiveChatBatch],
    ActiveChatRoundResult | Awaitable[ActiveChatRoundResult],
]


class ActiveChatWorkflow:
    """MVP active chat workflow with attention batching and semantic wait."""

    def __init__(
        self,
        *,
        attention: ActiveChatAttention | None = None,
        round_handler: ActiveChatRoundHandler | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        self._attention = attention or ActiveChatAttention()
        self._round_handler = round_handler
        self._now = now or time.time
        self._states: dict[str, ActiveChatAttentionState] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._semantic_wait_tasks: dict[str, asyncio.Task[None]] = {}
        self._running_rounds: dict[str, asyncio.Task[None]] = {}
        self.last_batches: dict[str, ActiveChatBatch] = {}

    async def start_active_chat(
        self,
        *,
        session_id: str,
        active_chat_state: ActiveChatState,
        review_result_summary=None,
    ) -> ActiveChatStartResult:
        """Initialize an active chat session without triggering an LLM round."""
        async with self._get_lock(session_id):
            self._cancel_semantic_wait_locked(session_id)
            state = ActiveChatAttentionState(
                session_id=session_id,
                last_update_at=self._now(),
                active_epoch=active_chat_state.active_epoch,
                review_result_summary=review_result_summary,
            )
            self._states[session_id] = state
        return ActiveChatStartResult(
            accepted=True,
            session_id=session_id,
            active_epoch=active_chat_state.active_epoch,
        )

    @property
    def attention_config(self) -> ActiveChatAttentionConfig:
        """Return the active chat attention config."""
        return self._attention.config

    async def notify_message(
        self,
        *,
        scheduler,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        response_profile: str,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        is_mention_to_other: bool,
        is_poke_to_bot: bool,
        is_poke_to_other: bool,
        self_platform_id: str,
        active_chat_state: ActiveChatState,
    ) -> ActiveChatNotifyResult:
        """Accept one active chat message signal and maybe arm semantic wait."""
        if message_log_id is None:
            return ActiveChatNotifyResult(
                accepted=False,
                session_id=session_id,
                skipped_reason="missing_message_log_id",
            )

        now = self._now()
        signal = ActiveChatMessageSignal(
            session_id=session_id,
            message_log_id=message_log_id,
            sender_id=sender_id,
            response_profile=response_profile,
            is_mentioned=is_mentioned,
            is_reply_to_bot=is_reply_to_bot,
            is_mention_to_other=is_mention_to_other,
            is_poke_to_bot=is_poke_to_bot,
            is_poke_to_other=is_poke_to_other,
            self_platform_id=self_platform_id,
            active_chat_state=active_chat_state,
            created_at=now,
        )

        async with self._get_lock(session_id):
            state = self._states.get(session_id)
            if state is None:
                state = ActiveChatAttentionState(session_id=session_id, last_update_at=now)
                self._states[session_id] = state

            previous_sender_id = state.last_sender_id
            self._attention.observe(state, signal, now=now)
            threshold = self._attention.effective_threshold(active_chat_state.interest_value)
            triggered = state.accumulated >= threshold
            if not triggered:
                return ActiveChatNotifyResult(
                    accepted=True,
                    session_id=session_id,
                    message_log_id=message_log_id,
                    accumulated=state.accumulated,
                    threshold=threshold,
                    triggered=False,
                )

            if self._is_round_running(session_id):
                return ActiveChatNotifyResult(
                    accepted=True,
                    session_id=session_id,
                    message_log_id=message_log_id,
                    accumulated=state.accumulated,
                    threshold=threshold,
                    triggered=True,
                )

            timer_started, timer_reset = self._arm_semantic_wait_locked(
                scheduler=scheduler,
                session_id=session_id,
                sender_id=sender_id,
                previous_sender_id=previous_sender_id,
            )
            return ActiveChatNotifyResult(
                accepted=True,
                session_id=session_id,
                message_log_id=message_log_id,
                accumulated=state.accumulated,
                threshold=threshold,
                triggered=True,
                timer_started=timer_started,
                timer_reset=timer_reset,
            )

    async def shutdown(self) -> None:
        """Cancel all active chat timers and running round tasks."""
        tasks = list(self._semantic_wait_tasks.values()) + list(self._running_rounds.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._semantic_wait_tasks.clear()
        self._running_rounds.clear()
        self._states.clear()

    def attention_state_for(self, session_id: str) -> ActiveChatAttentionState | None:
        """Return in-memory attention state for tests and diagnostics."""
        return self._states.get(session_id)

    def _get_lock(self, session_id: str) -> asyncio.Lock:
        if session_id not in self._locks:
            self._locks[session_id] = asyncio.Lock()
        return self._locks[session_id]

    def _arm_semantic_wait_locked(
        self,
        *,
        scheduler,
        session_id: str,
        sender_id: str,
        previous_sender_id: str,
    ) -> tuple[bool, bool]:
        existing = self._semantic_wait_tasks.get(session_id)
        timer_reset = False
        if existing is not None and not existing.done():
            if previous_sender_id == sender_id:
                self._cancel_semantic_wait_locked(session_id)
                timer_reset = True
            else:
                return False, False

        wait_seconds = self._attention.config.semantic_wait_ms / 1000.0
        task = asyncio.create_task(
            self._semantic_wait_then_flush(
                scheduler=scheduler,
                session_id=session_id,
                wait_seconds=wait_seconds,
            ),
            name=f"active-chat-wait-{session_id}",
        )
        self._semantic_wait_tasks[session_id] = task
        return True, timer_reset

    def _cancel_semantic_wait_locked(self, session_id: str) -> None:
        task = self._semantic_wait_tasks.pop(session_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _semantic_wait_then_flush(
        self,
        *,
        scheduler,
        session_id: str,
        wait_seconds: float,
    ) -> None:
        try:
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            return
        finally:
            current = asyncio.current_task()
            if self._semantic_wait_tasks.get(session_id) is current:
                self._semantic_wait_tasks.pop(session_id, None)

        async with self._get_lock(session_id):
            if self._is_round_running(session_id):
                return
            task = asyncio.create_task(
                self._flush(session_id=session_id, scheduler=scheduler),
                name=f"active-chat-round-{session_id}",
            )
            self._running_rounds[session_id] = task

    async def _flush(self, *, session_id: str, scheduler) -> None:
        try:
            async with self._get_lock(session_id):
                state = self._states.get(session_id)
                if state is None or not state.pending_buffer:
                    return
                messages = list(state.pending_buffer)
                state.pending_buffer.clear()

            latest_signal = messages[-1]
            active_chat_state = latest_signal.active_chat_state
            if active_chat_state is None:
                self._restore_pending(session_id, messages)
                return

            batch = ActiveChatBatch(
                session_id=session_id,
                messages=messages,
                active_chat_state=active_chat_state,
                response_profile=latest_signal.response_profile,
            )
            if self._round_handler is None:
                self._restore_pending(session_id, messages)
                return

            result = self._round_handler(batch)
            if isawaitable(result):
                result = await result
            if not result.success:
                self._restore_pending(session_id, messages)
                return

            scheduler.mark_active_chat_consumed(session_id, batch.message_log_ids)
            self.last_batches[session_id] = batch
            async with self._get_lock(session_id):
                state = self._states.get(session_id)
                if state is not None:
                    self._attention.cool_after_round(state)
        except Exception:
            logger.exception("Active chat round failed for session %s", session_id)
            self._restore_pending(session_id, messages if "messages" in locals() else [])
        finally:
            self._running_rounds.pop(session_id, None)

    def _restore_pending(
        self,
        session_id: str,
        messages: list[ActiveChatMessageSignal],
    ) -> None:
        if not messages:
            return
        state = self._states.get(session_id)
        if state is None:
            state = ActiveChatAttentionState(session_id=session_id, last_update_at=self._now())
            self._states[session_id] = state
        state.pending_buffer = messages + state.pending_buffer

    def _is_round_running(self, session_id: str) -> bool:
        running = self._running_rounds.get(session_id)
        return running is not None and not running.done()


__all__ = ["ActiveChatRoundHandler", "ActiveChatWorkflow"]
