"""Active chat coordinator — session lifecycle, pending buffers, round scheduling."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from inspect import isawaitable
from typing import TYPE_CHECKING, Any

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
    ActiveChatRoundCommitDecision,
    ActiveChatRoundCommitIntent,
    ActiveChatRoundResult,
    ActiveChatStartResult,
    ActiveChatSummarySnapshot,
)
from shinbot.agent.coordinators.active_chat.trace import (
    ActiveChatTraceCompactor,
    ActiveChatTraceConfig,
)
from shinbot.agent.runtime.task_manager import (
    AgentTaskQuiescence,
    cancel_and_wait_for_tasks,
)
from shinbot.agent.scheduler.models import ActiveChatState
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:active-chat", color="green")

if TYPE_CHECKING:
    from shinbot.agent.runtime.task_manager import AgentTaskScope

ActiveChatRoundHandler = Callable[
    [ActiveChatBatch],
    ActiveChatRoundResult | Awaitable[ActiveChatRoundResult],
]
ActiveChatRoundCommitHandler = Callable[
    [ActiveChatRoundCommitIntent],
    ActiveChatRoundCommitDecision | Awaitable[ActiveChatRoundCommitDecision],
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
        round_commit_handler: ActiveChatRoundCommitHandler | None = None,
        now: Callable[[], float] | None = None,
        conversation_message_limit: int = 80,
        trace_compactor: ActiveChatTraceCompactor | None = None,
        interest_effect_config: ActiveChatInterestEffectConfig | None = None,
    ) -> None:
        self._attention = attention or ActiveChatAttention()
        self._round_handler = round_handler
        self._round_commit_handler = round_commit_handler
        self._now = now or time.time
        self._interest_effect_config = interest_effect_config or ActiveChatInterestEffectConfig()
        self._trace_compactor = trace_compactor or ActiveChatTraceCompactor(
            ActiveChatTraceConfig(message_limit=max(0, conversation_message_limit))
        )
        self._states: dict[str, ActiveChatAttentionState] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._semantic_wait_tasks: dict[str, asyncio.Task[Any]] = {}
        self._running_rounds: dict[str, asyncio.Task[Any]] = {}
        self._retiring_tasks: dict[asyncio.Task[Any], str] = {}
        self.last_batches: dict[str, ActiveChatBatch] = {}
        self._task_scope: AgentTaskScope | None = None

    def bind_task_scope(self, scope: AgentTaskScope | None) -> None:
        """Bind the task scope used for background semantic wait work."""
        self._task_scope = scope

    def set_round_handler(self, round_handler: ActiveChatRoundHandler | None) -> None:
        """Replace the active chat round handler."""
        self._round_handler = round_handler

    def set_round_commit_handler(
        self,
        round_commit_handler: ActiveChatRoundCommitHandler | None,
    ) -> None:
        """Replace the owner that commits validated round outcomes."""

        self._round_commit_handler = round_commit_handler

    async def start_active_chat(
        self,
        *,
        session_id: str,
        active_chat_state: ActiveChatState,
        review_result_summary: Any = None,
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
            format_log_event(
                "agent.active_chat.session.started",
                session_id=session_id,
                active_epoch=active_chat_state.active_epoch,
                interest=f"{active_chat_state.interest_value:.2f}",
            )
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

    def update_attention_config(self, config: ActiveChatAttentionConfig) -> None:
        """Replace tunable active chat attention parameters at runtime."""
        self._attention.config = replace(config)

    async def notify_message(
        self,
        *,
        scheduler: Any,
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
        trace_id: str = "",
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
                format_log_event(
                    "agent.active_chat.message.skip",
                    session_id=session_id,
                    message_log_id=message_log_id,
                    reason="self_message",
                    trace_id=trace_id,
                )
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
            trace_id=trace_id,
        )

        async with self._get_lock(session_id):
            state = self._states.get(session_id)
            if state is None:
                logger.debug(
                    format_log_event(
                        "agent.active_chat.message.skip",
                        session_id=session_id,
                        message_log_id=message_log_id,
                        reason="inactive_session",
                        trace_id=trace_id,
                    )
                )
                return ActiveChatNotifyResult(
                    accepted=False,
                    session_id=session_id,
                    message_log_id=message_log_id,
                    skipped_reason="inactive_session",
                )
            elif state.active_epoch != active_chat_state.active_epoch:
                logger.debug(
                    format_log_event(
                        "agent.active_chat.message.skip",
                        session_id=session_id,
                        message_log_id=message_log_id,
                        reason="active_epoch_mismatch",
                        state_epoch=state.active_epoch,
                        signal_epoch=active_chat_state.active_epoch,
                        trace_id=trace_id,
                    )
                )
                return ActiveChatNotifyResult(
                    accepted=False,
                    session_id=session_id,
                    message_log_id=message_log_id,
                    skipped_reason="active_epoch_mismatch",
                )

            previous_sender_id = state.last_sender_id
            self._attention.observe(state, signal, now=now)
            state.observed_message_count += 1
            threshold = self._attention.effective_threshold(active_chat_state.interest_value)
            triggered = state.accumulated >= threshold
            logger.debug(
                format_log_event(
                    "agent.active_chat.message.observed",
                    session_id=session_id,
                    message_log_id=message_log_id,
                    accumulated=f"{state.accumulated:.3f}",
                    threshold=f"{threshold:.3f}",
                    triggered=triggered,
                    trace_id=trace_id,
                )
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
                    format_log_event(
                        "agent.active_chat.message.buffered",
                        session_id=session_id,
                        message_log_id=message_log_id,
                        accumulated=f"{state.accumulated:.3f}",
                        threshold=f"{threshold:.3f}",
                        reason="round_running",
                        trace_id=trace_id,
                    )
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
        tasks = list(
            dict.fromkeys(
                [
                    *self._semantic_wait_tasks.values(),
                    *self._running_rounds.values(),
                    *self._retiring_tasks,
                ]
            )
        )
        for task in tasks:
            if task is not asyncio.current_task() and not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(
                *(task for task in tasks if task is not asyncio.current_task()),
                return_exceptions=True,
            )
        self._semantic_wait_tasks.clear()
        self._running_rounds.clear()
        self._retiring_tasks.clear()
        self._states.clear()

    def pending_session_tasks(self, session_id: str) -> list[asyncio.Task[Any]]:
        """Return known semantic-wait, round, and retiring tasks for one session.

        The result is limited to task objects owned or observed by this
        coordinator in the current process. It does not freeze future message
        admission or make any statement about external model/tool effects.
        """

        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id must not be empty")
        tasks = [
            task
            for task, task_session_id in self._retiring_tasks.items()
            if task_session_id == normalized_session_id and not task.done()
        ]
        for task_map in (self._semantic_wait_tasks, self._running_rounds):
            task = task_map.get(normalized_session_id)
            if task is not None and not task.done():
                tasks.append(task)
        return list(dict.fromkeys(tasks))

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AgentTaskQuiescence:
        """Cancel and observe known active-chat work for one session locally.

        A positive result only confirms that this coordinator observed no
        surviving task in its fixed local snapshot. It is not an ingress fence,
        cross-process drain, or durable cutover receipt.
        """

        return await cancel_and_wait_for_tasks(
            self.pending_session_tasks(session_id),
            timeout_seconds=timeout_seconds,
        )

    async def flush_now(self, *, scheduler: Any, session_id: str) -> None:
        """Immediately flush pending active-chat messages for a session."""
        current_task = asyncio.current_task()
        async with self._get_lock(session_id):
            if self._is_round_running(session_id):
                logger.debug(
                    format_log_event(
                        "agent.active_chat.flush.skip",
                        session_id=session_id,
                        reason="round_running",
                    )
                )
                return
            self._cancel_semantic_wait_locked(session_id)
            if current_task is not None:
                self._running_rounds[session_id] = current_task
                self._track_session_task(current_task)

        logger.debug(
            format_log_event(
                "agent.active_chat.flush.now",
                session_id=session_id,
            )
        )
        await self._flush(session_id=session_id, scheduler=scheduler)

    def stop_active_chat(self, session_id: str) -> None:
        """Clear session-bound active chat workflow state after scheduler exit."""
        state = self._states.get(session_id)
        pending_count = len(state.pending_buffer) if state is not None else 0
        self._cancel_semantic_wait_locked(session_id)
        self._cancel_running_round_locked(session_id)
        self._states.pop(session_id, None)
        self.last_batches.pop(session_id, None)
        logger.info(
            format_log_event(
                "agent.active_chat.session.stopped",
                session_id=session_id,
                pending_count=pending_count,
            )
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
            trace_message_count=len(state.conversation_messages),
            observed_message_count=state.observed_message_count,
            conversation_messages=[dict(message) for message in state.conversation_messages],
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
        scheduler: Any,
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
                    format_log_event(
                        "agent.active_chat.semantic_wait.reset",
                        session_id=session_id,
                        sender_id=sender_id,
                    )
                )
            else:
                logger.debug(
                    format_log_event(
                        "agent.active_chat.semantic_wait.skip",
                        session_id=session_id,
                        sender_id=sender_id,
                        previous_sender_id=previous_sender_id,
                        reason="already_running_for_other_sender",
                    )
                )
                return False, False

        wait_seconds = self._attention.config.semantic_wait_ms / 1000.0
        coro = self._semantic_wait_then_flush(
            scheduler=scheduler,
            session_id=session_id,
            wait_seconds=wait_seconds,
        )
        if self._task_scope is not None:
            task = self._task_scope.create_task(
                f"{session_id}:semantic_wait",
                coro,
                name=f"active-chat-wait-{session_id}",
            )
        else:
            task = asyncio.create_task(
                coro,
                name=f"active-chat-wait-{session_id}",
            )
        self._semantic_wait_tasks[session_id] = task
        self._track_session_task(task)
        logger.debug(
            format_log_event(
                "agent.active_chat.semantic_wait.started",
                session_id=session_id,
                wait_seconds=f"{wait_seconds:.3f}",
            )
        )
        return True, timer_reset

    def _cancel_semantic_wait_locked(self, session_id: str) -> None:
        task = self._semantic_wait_tasks.pop(session_id, None)
        if task is not None and not task.done():
            self._retire_session_task(session_id, task)
            if task is not asyncio.current_task():
                task.cancel()
            logger.debug(
                format_log_event(
                    "agent.active_chat.semantic_wait.cancelled",
                    session_id=session_id,
                )
            )

    def _cancel_running_round_locked(self, session_id: str) -> None:
        task = self._running_rounds.pop(session_id, None)
        if task is not None and not task.done():
            self._retire_session_task(session_id, task)
            if task is not asyncio.current_task():
                task.cancel()
            logger.debug(
                format_log_event(
                    "agent.active_chat.round.cancelled",
                    session_id=session_id,
                )
            )

    async def _semantic_wait_then_flush(
        self,
        *,
        scheduler: Any,
        session_id: str,
        wait_seconds: float,
    ) -> None:
        current = asyncio.current_task()
        try:
            await asyncio.sleep(wait_seconds)
            async with self._get_lock(session_id):
                if self._is_round_running(session_id):
                    logger.debug(
                        format_log_event(
                            "agent.active_chat.flush.skip",
                            session_id=session_id,
                            reason="round_running",
                        )
                    )
                    return
                coro = self._flush(session_id=session_id, scheduler=scheduler)
                if self._task_scope is not None:
                    task = self._task_scope.create_task(
                        f"{session_id}:round",
                        coro,
                        name=f"active-chat-round-{session_id}",
                    )
                else:
                    task = asyncio.create_task(
                        coro,
                        name=f"active-chat-round-{session_id}",
                    )
                self._running_rounds[session_id] = task
                self._track_session_task(task)
                logger.debug(
                    format_log_event(
                        "agent.active_chat.semantic_wait.flushed",
                        session_id=session_id,
                    )
                )
        except asyncio.CancelledError:
            return
        finally:
            if self._semantic_wait_tasks.get(session_id) is current:
                self._semantic_wait_tasks.pop(session_id, None)

    async def _flush(self, *, session_id: str, scheduler: Any) -> None:
        try:
            async with self._get_lock(session_id):
                state = self._states.get(session_id)
                if state is None or not state.pending_buffer:
                    logger.debug(
                        format_log_event(
                            "agent.active_chat.flush.skip",
                            session_id=session_id,
                            reason="missing_state" if state is None else "empty_pending",
                        )
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
                    format_log_event(
                        "agent.active_chat.batch.invalid",
                        session_id=session_id,
                        reason="missing_active_state",
                        message_log_ids=_message_ids(messages),
                        trace_id=_trace_id_from_messages(messages),
                    )
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
                    format_log_event(
                        "agent.active_chat.batch.restored",
                        session_id=session_id,
                        reason="missing_round_handler",
                        message_log_ids=batch.message_log_ids,
                        trace_id=_trace_id_from_messages(messages),
                    )
                )
                self._restore_pending(
                    session_id,
                    messages,
                    accumulated=handled_accumulated,
                )
                return

            logger.info(
                format_log_event(
                    "agent.active_chat.round.started",
                    session_id=session_id,
                    message_log_ids=batch.message_log_ids,
                    accumulated=f"{handled_accumulated:.3f}",
                    trace_id=_trace_id_from_messages(messages),
                )
            )
            result = self._round_handler(batch)
            if isawaitable(result):
                result = await result
            if not result.success:
                restored_messages = result.restored_messages or messages
                logger.warning(
                    format_log_event(
                        "agent.active_chat.round.restored",
                        session_id=session_id,
                        reason=result.reason,
                        message_log_ids=_message_ids(restored_messages),
                        trace_id=_trace_id_from_messages(restored_messages),
                    )
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
                        format_log_event(
                            "agent.active_chat.round.discarded",
                            session_id=session_id,
                            reason="stale_runtime",
                            message_log_ids=batch.message_log_ids,
                            trace_id=_trace_id_from_messages(batch.messages),
                        )
                    )
                    return
                if result.action == ActiveChatActionKind.EXIT_ACTIVE and not result.reason.strip():
                    logger.warning(
                        format_log_event(
                            "agent.active_chat.round.restored",
                            session_id=session_id,
                            reason="exit_active_missing_reason",
                            message_log_ids=batch.message_log_ids,
                            trace_id=_trace_id_from_messages(batch.messages),
                        )
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

            consumed_message_log_ids = result.consumed_message_log_ids or batch.message_log_ids
            effect = interest_effect_for_round(result, self._interest_effect_config)
            self.last_batches[session_id] = batch
            logger.info(
                format_log_event(
                    "agent.active_chat.round.finished",
                    session_id=session_id,
                    action=result.action.value,
                    consumed_message_log_ids=consumed_message_log_ids,
                    interest_delta=f"{effect.delta:.2f}",
                    force_exit=effect.force_exit,
                    reason=effect.reason,
                    trace_id=_trace_id_from_messages(batch.messages),
                )
            )
            commit_intent = ActiveChatRoundCommitIntent(
                session_id=session_id,
                active_epoch=batch.active_chat_state.active_epoch,
                consumed_message_log_ids=tuple(consumed_message_log_ids),
                interest_delta=effect.delta,
                force_exit=effect.force_exit,
                reason=effect.reason,
            )
            commit_decision = await self._commit_round_result(
                scheduler=scheduler,
                intent=commit_intent,
            )
            if not commit_decision.accepted:
                logger.info(
                    format_log_event(
                        "agent.active_chat.round.commit_discarded",
                        session_id=session_id,
                        active_epoch=commit_intent.active_epoch,
                        message_log_ids=consumed_message_log_ids,
                        reason=commit_decision.skipped_reason or "unknown",
                        trace_id=_trace_id_from_messages(batch.messages),
                    )
                )
                return
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
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "agent.active_chat.round.failed",
                    session_id=session_id,
                    error_code=type(exc).__name__,
                    message_log_ids=(_message_ids(messages) if "messages" in locals() else []),
                    trace_id=(_trace_id_from_messages(messages) if "messages" in locals() else ""),
                )
            )
            self._restore_pending(
                session_id,
                messages if "messages" in locals() else [],
                accumulated=handled_accumulated if "handled_accumulated" in locals() else 0.0,
            )
        finally:
            current = asyncio.current_task()
            if self._running_rounds.get(session_id) is current:
                self._running_rounds.pop(session_id, None)

    async def _commit_round_result(
        self,
        *,
        scheduler: Any,
        intent: ActiveChatRoundCommitIntent,
    ) -> ActiveChatRoundCommitDecision:
        """Submit one validated round result to its scheduler-state owner.

        Standalone coordinators keep the legacy fallback for focused workflow
        tests. Production profiles bind a runtime handler, which serializes all
        scheduler mutation through the runtime's per-session mutex.
        """

        handler = self._round_commit_handler
        if handler is not None:
            decision = handler(intent)
            if isawaitable(decision):
                decision = await decision
            if not isinstance(decision, ActiveChatRoundCommitDecision):
                raise TypeError("round commit handler returned an invalid decision")
            return decision
        return await self._commit_round_with_scheduler_fallback(
            scheduler=scheduler,
            intent=intent,
        )

    async def _commit_round_with_scheduler_fallback(
        self,
        *,
        scheduler: Any,
        intent: ActiveChatRoundCommitIntent,
    ) -> ActiveChatRoundCommitDecision:
        """Apply a round outcome directly for standalone coordinator callers."""

        scheduler.mark_active_chat_consumed(
            intent.session_id,
            list(intent.consumed_message_log_ids),
        )
        next_review_plan = None
        planning_request = None
        preview_adjustment = getattr(
            scheduler,
            "preview_active_chat_interest_adjustment",
            None,
        )
        if preview_adjustment is not None:
            preview = preview_adjustment(
                intent.session_id,
                delta=intent.interest_delta,
                force_exit=intent.force_exit,
                active_epoch=intent.active_epoch,
            )
            if getattr(preview, "will_return_idle", False):
                prepare_planning = getattr(
                    scheduler,
                    "prepare_idle_review_planning_for_interest_adjustment",
                    None,
                )
                if prepare_planning is not None:
                    planning_request = prepare_planning(
                        intent.session_id,
                        delta=intent.interest_delta,
                        force_exit=intent.force_exit,
                        active_epoch=intent.active_epoch,
                        reason=intent.reason,
                    )
                planner = getattr(scheduler, "plan_idle_review_after_active_chat", None)
                if planner is not None:
                    if planning_request is None:
                        next_review_plan = await planner(intent.session_id)
                    else:
                        next_review_plan = await planner(
                            intent.session_id,
                            request=planning_request,
                        )
                if planning_request is not None:
                    apply_planning = getattr(
                        scheduler,
                        "apply_idle_review_planning_request",
                        None,
                    )
                    if apply_planning is not None:
                        applied = apply_planning(
                            planning_request,
                            next_review_plan=next_review_plan,
                        )
                        return ActiveChatRoundCommitDecision(
                            session_id=intent.session_id,
                            accepted=True,
                            returned_to_idle=bool(
                                getattr(applied, "returned_to_idle", False)
                            ),
                            skipped_reason=getattr(applied, "skipped_reason", None),
                        )
        decision = scheduler.adjust_active_chat_interest(
            intent.session_id,
            delta=intent.interest_delta,
            force_exit=intent.force_exit,
            active_epoch=intent.active_epoch,
            reason=intent.reason,
            next_review_plan=next_review_plan,
        )
        return ActiveChatRoundCommitDecision(
            session_id=intent.session_id,
            accepted=True,
            returned_to_idle=bool(getattr(decision, "returned_to_idle", False)),
            skipped_reason=getattr(decision, "skipped_reason", None),
        )

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
            format_log_event(
                "agent.active_chat.pending.restored",
                session_id=session_id,
                message_log_ids=_message_ids(messages),
                accumulated=f"{state.accumulated:.3f}",
                trace_id=_trace_id_from_messages(messages),
            )
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
        scheduler: Any,
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
            format_log_event(
                "agent.active_chat.pending.over_threshold",
                session_id=session_id,
                accumulated=f"{state.accumulated:.3f}",
                threshold=f"{threshold:.3f}",
                trace_id=_trace_id_from_messages(state.pending_buffer),
            )
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

    def _retire_session_task(self, session_id: str, task: asyncio.Task[Any]) -> None:
        """Keep a cancelled task observable until its cancellation tail exits."""

        self._retiring_tasks[task] = session_id

    def _track_session_task(self, task: asyncio.Task[Any]) -> None:
        """Arrange cleanup for normal completion and cancellation tails."""

        task.add_done_callback(self._finish_session_task)

    def _finish_session_task(self, task: asyncio.Task[Any]) -> None:
        """Forget a task only after its coroutine has actually terminated."""

        self._retiring_tasks.pop(task, None)
        for task_map in (self._semantic_wait_tasks, self._running_rounds):
            for session_id, tracked_task in tuple(task_map.items()):
                if tracked_task is task:
                    task_map.pop(session_id, None)


def _message_ids(messages: list[ActiveChatMessageSignal]) -> list[int]:
    return [message.message_log_id for message in messages]


def _trace_id_from_messages(messages: list[ActiveChatMessageSignal]) -> str:
    for message in messages:
        trace_id = str(message.trace_id or "").strip()
        if trace_id:
            return trace_id
    return ""


__all__ = [
    "ActiveChatCoordinator",
    "ActiveChatRoundCommitHandler",
    "ActiveChatRoundHandler",
]
