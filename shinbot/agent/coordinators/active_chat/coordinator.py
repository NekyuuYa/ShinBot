"""Active chat coordinator — session lifecycle, pending buffers, round scheduling."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from typing import Any

from shinbot.agent.coordinators.active_chat.actions import (
    ActiveChatInterestEffectConfig,
    interest_effect_for_round,
)
from shinbot.agent.coordinators.active_chat.attention import (
    ActiveChatAttention,
    ActiveChatAttentionConfig,
)
from shinbot.agent.coordinators.active_chat.models import (
    ActiveChatActionKind,
    ActiveChatAttentionState,
    ActiveChatBatch,
    ActiveChatMessageSignal,
    ActiveChatNotifyResult,
    ActiveChatRoundResult,
    ActiveChatStartResult,
    ActiveChatSummarySnapshot,
)
from shinbot.agent.coordinators.active_chat.trace import (
    ActiveChatTraceCompactor,
    ActiveChatTraceConfig,
)
from shinbot.agent.scheduler.models import ActiveChatState

logger = logging.getLogger(__name__)

ActiveChatRoundHandler = Callable[
    [ActiveChatBatch],
    ActiveChatRoundResult | Awaitable[ActiveChatRoundResult],
]


class ActiveChatCoordinator:
    """Active chat coordinator with attention batching and semantic wait.

    Manages session lifecycle, pending message buffers, semantic wait timers,
    round scheduling, and failure recovery. Does not execute LLM calls directly;
    delegates to a round handler (e.g. ActiveChatFastRunner).
    """

    def __init__(
        self,
        *,
        attention: ActiveChatAttention | None = None,
        round_handler: ActiveChatRoundHandler | None = None,
        now: Callable[[], float] | None = None,
        conversation_message_limit: int = 80,
        trace_compactor: ActiveChatTraceCompactor | None = None,
        interest_effect_config: ActiveChatInterestEffectConfig | None = None,
    ) -> None:
        self._attention = attention or ActiveChatAttention()
        self._round_handler = round_handler
        self._now = now or time.time
        self._interest_effect_config = interest_effect_config or ActiveChatInterestEffectConfig()
        self._trace_compactor = trace_compactor or ActiveChatTraceCompactor(
            ActiveChatTraceConfig(message_limit=max(0, conversation_message_limit))
        )
        self._states: dict[str, ActiveChatAttentionState] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._semantic_wait_tasks: dict[str, asyncio.Task[None]] = {}
        self._running_rounds: dict[str, asyncio.Task[None]] = {}
        self.last_batches: dict[str, ActiveChatBatch] = {}

    def set_round_handler(self, round_handler: ActiveChatRoundHandler | None) -> None:
        """Replace the active chat round handler."""
        self._round_handler = round_handler

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
            self._cancel_running_round_locked(session_id)
            self.last_batches.pop(session_id, None)
            state = ActiveChatAttentionState(
                session_id=session_id,
                last_update_at=self._now(),
                active_epoch=active_chat_state.active_epoch,
                review_result_summary=review_result_summary,
            )
            self._states[session_id] = state
        logger.info(
            "Active chat session started for %s epoch=%s interest=%.2f",
            session_id,
            active_chat_state.active_epoch,
            active_chat_state.interest_value,
        )
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
        if sender_id and self_platform_id and sender_id == self_platform_id:
            logger.debug(
                "Active chat ignored self message session=%s message_log_id=%s",
                session_id,
                message_log_id,
            )
            return ActiveChatNotifyResult(
                accepted=False,
                session_id=session_id,
                message_log_id=message_log_id,
                skipped_reason="self_message",
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
                logger.debug(
                    "Active chat ignored message before session start session=%s "
                    "message_log_id=%s",
                    session_id,
                    message_log_id,
                )
                return ActiveChatNotifyResult(
                    accepted=False,
                    session_id=session_id,
                    message_log_id=message_log_id,
                    skipped_reason="inactive_session",
                )
            elif state.active_epoch != active_chat_state.active_epoch:
                logger.debug(
                    "Active chat ignored stale epoch message session=%s message_log_id=%s "
                    "state_epoch=%s signal_epoch=%s",
                    session_id,
                    message_log_id,
                    state.active_epoch,
                    active_chat_state.active_epoch,
                )
                return ActiveChatNotifyResult(
                    accepted=False,
                    session_id=session_id,
                    message_log_id=message_log_id,
                    skipped_reason="active_epoch_mismatch",
                )

            previous_sender_id = state.last_sender_id
            self._attention.observe(state, signal, now=now)
            threshold = self._attention.effective_threshold(active_chat_state.interest_value)
            triggered = state.accumulated >= threshold
            logger.debug(
                "Active chat observed message session=%s message_log_id=%s "
                "accumulated=%.3f threshold=%.3f triggered=%s",
                session_id,
                message_log_id,
                state.accumulated,
                threshold,
                triggered,
            )
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
                logger.debug(
                    "Active chat buffered message while round is running session=%s "
                    "message_log_id=%s accumulated=%.3f threshold=%.3f",
                    session_id,
                    message_log_id,
                    state.accumulated,
                    threshold,
                )
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

    def stop_active_chat(self, session_id: str) -> None:
        """Clear session-bound active chat workflow state after scheduler exit."""
        state = self._states.get(session_id)
        pending_count = len(state.pending_buffer) if state is not None else 0
        self._cancel_semantic_wait_locked(session_id)
        self._cancel_running_round_locked(session_id)
        self._states.pop(session_id, None)
        self.last_batches.pop(session_id, None)
        logger.info(
            "Active chat session stopped session=%s pending_count=%s",
            session_id,
            pending_count,
        )

    def attention_state_for(self, session_id: str) -> ActiveChatAttentionState | None:
        """Return in-memory attention state for tests and diagnostics."""
        return self._states.get(session_id)

    def active_session_ids(self) -> list[str]:
        """Return session ids that have in-memory active chat state."""
        return list(self._states.keys())

    def summary_snapshot_for(self, session_id: str) -> ActiveChatSummarySnapshot | None:
        """Return an active-chat summary snapshot without exposing internal state."""
        state = self._states.get(session_id)
        if state is None:
            return None
        batch = self.last_batches.get(session_id)
        return ActiveChatSummarySnapshot(
            session_id=session_id,
            active_epoch=state.active_epoch,
            conversation_summary=state.conversation_summary,
            conversation_message_count=len(state.conversation_messages),
            message_log_ids=batch.message_log_ids if batch is not None else [],
        )

    async def drain_pending_for_repair(
        self,
        batch: ActiveChatBatch,
    ) -> list[ActiveChatMessageSignal]:
        """Drain messages that arrived while a round was preparing repair."""
        async with self._get_lock(batch.session_id):
            state = self._states.get(batch.session_id)
            if state is None or state.active_epoch != batch.active_chat_state.active_epoch:
                return []
            messages = list(state.pending_buffer)
            state.pending_buffer.clear()
            if messages:
                state.accumulated = 0.0
            return messages

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
                logger.debug(
                    "Active chat semantic wait reset session=%s sender_id=%s",
                    session_id,
                    sender_id,
                )
            else:
                logger.debug(
                    "Active chat semantic wait already running session=%s sender_id=%s "
                    "previous_sender_id=%s",
                    session_id,
                    sender_id,
                    previous_sender_id,
                )
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
        logger.debug(
            "Active chat semantic wait started session=%s wait_seconds=%.3f",
            session_id,
            wait_seconds,
        )
        return True, timer_reset

    def _cancel_semantic_wait_locked(self, session_id: str) -> None:
        task = self._semantic_wait_tasks.pop(session_id, None)
        if task is not None and not task.done():
            task.cancel()
            logger.debug("Active chat semantic wait cancelled session=%s", session_id)

    def _cancel_running_round_locked(self, session_id: str) -> None:
        task = self._running_rounds.pop(session_id, None)
        if task is not None and not task.done() and task is not asyncio.current_task():
            task.cancel()
            logger.debug("Active chat running round cancelled session=%s", session_id)

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
                logger.debug(
                    "Active chat skipped semantic flush because round is running session=%s",
                    session_id,
                )
                return
            task = asyncio.create_task(
                self._flush(session_id=session_id, scheduler=scheduler),
                name=f"active-chat-round-{session_id}",
            )
            self._running_rounds[session_id] = task
            logger.debug("Active chat semantic wait flushed session=%s", session_id)

    async def _flush(self, *, session_id: str, scheduler) -> None:
        try:
            async with self._get_lock(session_id):
                state = self._states.get(session_id)
                if state is None or not state.pending_buffer:
                    logger.debug(
                        "Active chat flush skipped session=%s reason=%s",
                        session_id,
                        "missing_state" if state is None else "empty_pending",
                    )
                    return
                messages = list(state.pending_buffer)
                state.pending_buffer.clear()
                handled_accumulated = state.accumulated
                state.accumulated = 0.0
                review_result_summary = state.review_result_summary
                conversation_summary = state.conversation_summary
                conversation_messages = list(state.conversation_messages)

            latest_signal = messages[-1]
            active_chat_state = latest_signal.active_chat_state
            if active_chat_state is None:
                logger.warning(
                    "Active chat batch missing active state session=%s message_log_ids=%s",
                    session_id,
                    _message_ids(messages),
                )
                self._restore_pending(
                    session_id,
                    messages,
                    accumulated=handled_accumulated,
                )
                return

            batch = ActiveChatBatch(
                session_id=session_id,
                messages=messages,
                active_chat_state=active_chat_state,
                response_profile=latest_signal.response_profile,
                review_result_summary=review_result_summary,
                conversation_summary=conversation_summary,
                conversation_messages=conversation_messages,
            )
            if self._round_handler is None:
                logger.warning(
                    "Active chat batch restored because no round handler is configured "
                    "session=%s message_log_ids=%s",
                    session_id,
                    batch.message_log_ids,
                )
                self._restore_pending(
                    session_id,
                    messages,
                    accumulated=handled_accumulated,
                )
                return

            logger.info(
                "Active chat round started session=%s message_log_ids=%s accumulated=%.3f",
                session_id,
                batch.message_log_ids,
                handled_accumulated,
            )
            result = self._round_handler(batch)
            if isawaitable(result):
                result = await result
            if not result.success:
                restored_messages = result.restored_messages or messages
                logger.warning(
                    "Active chat round restored pending session=%s reason=%s "
                    "message_log_ids=%s",
                    session_id,
                    result.reason,
                    _message_ids(restored_messages),
                )
                self._restore_pending(
                    session_id,
                    restored_messages,
                    accumulated=handled_accumulated,
                )
                return
            async with self._get_lock(session_id):
                current_state = self._states.get(session_id)
                if (
                    current_state is None
                    or current_state.active_epoch != batch.active_chat_state.active_epoch
                ):
                    logger.warning(
                        "Active chat round result discarded for stale runtime session=%s "
                        "message_log_ids=%s",
                        session_id,
                        batch.message_log_ids,
                    )
                    return
                if result.action == ActiveChatActionKind.EXIT_ACTIVE and not result.reason.strip():
                    logger.warning(
                        "Active chat exit_active without reason restored session=%s "
                        "message_log_ids=%s",
                        session_id,
                        batch.message_log_ids,
                    )
                    self._restore_pending(
                        session_id,
                        messages,
                        accumulated=handled_accumulated,
                    )
                    return
                self._append_conversation_messages_locked(
                    current_state,
                    result.conversation_messages_delta,
                )

            consumed_message_log_ids = (
                result.consumed_message_log_ids or batch.message_log_ids
            )
            scheduler.mark_active_chat_consumed(session_id, consumed_message_log_ids)
            effect = interest_effect_for_round(result, self._interest_effect_config)
            self.last_batches[session_id] = batch
            logger.info(
                "Active chat round completed session=%s action=%s consumed_message_log_ids=%s "
                "interest_delta=%.2f force_exit=%s reason=%s",
                session_id,
                result.action.value,
                consumed_message_log_ids,
                effect.delta,
                effect.force_exit,
                effect.reason,
            )
            scheduler.adjust_active_chat_interest(
                session_id,
                delta=effect.delta,
                force_exit=effect.force_exit,
                reason=effect.reason,
            )
            async with self._get_lock(session_id):
                state = self._states.get(session_id)
                if state is not None:
                    state.accumulated += (
                        handled_accumulated
                        * self._attention.config.post_round_accumulated_multiplier
                    )
                    self._arm_next_round_if_pending_locked(
                        scheduler=scheduler,
                        session_id=session_id,
                        state=state,
                    )
        except Exception:
            logger.exception("Active chat round failed for session %s", session_id)
            self._restore_pending(
                session_id,
                messages if "messages" in locals() else [],
                accumulated=handled_accumulated if "handled_accumulated" in locals() else 0.0,
            )
        finally:
            self._running_rounds.pop(session_id, None)

    def _restore_pending(
        self,
        session_id: str,
        messages: list[ActiveChatMessageSignal],
        *,
        accumulated: float = 0.0,
    ) -> None:
        if not messages:
            return
        state = self._states.get(session_id)
        if state is None:
            state = ActiveChatAttentionState(session_id=session_id, last_update_at=self._now())
            self._states[session_id] = state
        state.pending_buffer = messages + state.pending_buffer
        state.accumulated += accumulated
        logger.debug(
            "Active chat pending restored session=%s message_log_ids=%s accumulated=%.3f",
            session_id,
            _message_ids(messages),
            state.accumulated,
        )

    def _append_conversation_messages_locked(
        self,
        state: ActiveChatAttentionState,
        messages: list[dict[str, Any]],
    ) -> None:
        self._trace_compactor.append(state, messages)

    def _arm_next_round_if_pending_locked(
        self,
        *,
        scheduler,
        session_id: str,
        state: ActiveChatAttentionState,
    ) -> None:
        if not state.pending_buffer:
            return
        latest_signal = state.pending_buffer[-1]
        active_chat_state = latest_signal.active_chat_state
        if active_chat_state is None:
            return
        threshold = self._attention.effective_threshold(active_chat_state.interest_value)
        if state.accumulated < threshold:
            return
        logger.debug(
            "Active chat pending still over threshold session=%s accumulated=%.3f "
            "threshold=%.3f",
            session_id,
            state.accumulated,
            threshold,
        )
        self._arm_semantic_wait_locked(
            scheduler=scheduler,
            session_id=session_id,
            sender_id=state.last_sender_id,
            previous_sender_id="",
        )

    def _is_round_running(self, session_id: str) -> bool:
        running = self._running_rounds.get(session_id)
        return running is not None and not running.done()


def _message_ids(messages: list[ActiveChatMessageSignal]) -> list[int]:
    return [message.message_log_id for message in messages]


__all__ = ["ActiveChatCoordinator", "ActiveChatRoundHandler"]
