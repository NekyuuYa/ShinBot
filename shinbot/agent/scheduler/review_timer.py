"""Background timer that wakes due idle review plans."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from shinbot.agent.signals import (
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
    AgentTimerSignal,
)
from shinbot.utils.logger import format_log_event, get_logger

if TYPE_CHECKING:
    from shinbot.agent.runtime.services import AgentRuntime
    from shinbot.agent.runtime.task_manager import AgentTaskScope

logger = get_logger(__name__, source="agent:timer", color="yellow")


class ReviewDueTimerService:
    """Poll due review plans and dispatch their review workflow."""

    def __init__(
        self,
        *,
        tick_interval_seconds: float = 5.0,
        batch_limit: int = 50,
    ) -> None:
        self._tick_interval_seconds = tick_interval_seconds
        self._batch_limit = batch_limit
        self._runtime: AgentRuntime | None = None
        self._bot_id = ""
        self._task: asyncio.Task[None] | None = None
        self._in_flight: set[str] = set()
        self._task_scope: AgentTaskScope | None = None

    def bind_agent_runtime(self, runtime: AgentRuntime, *, bot_id: str = "") -> None:
        self._runtime = runtime
        self._bot_id = str(bot_id or "").strip()

    def bind_task_scope(self, scope: AgentTaskScope) -> None:
        self._task_scope = scope

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("agent.review_timer.start_skipped | reason=no_running_loop")
            return
        if self._task_scope is not None:
            self._task = self._task_scope.create_task(
                "loop",
                self._run_loop(),
                name="agent-review-due-timer",
            )
        else:
            self._task = loop.create_task(self._run_loop(), name="agent-review-due-timer")
        logger.info(
            format_log_event(
                "agent.review_timer.started",
                bot_id=self._bot_id,
                tick_interval_seconds=self._tick_interval_seconds,
                batch_limit=self._batch_limit,
            )
        )

    async def shutdown(self) -> None:
        task = self._task
        self._task = None
        if task is None or task.done():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        logger.debug(format_log_event("agent.review_timer.stopped", bot_id=self._bot_id))

    async def run_once(self) -> None:
        """Run one polling pass for tests and manual maintenance."""

        runtime = self._runtime
        if runtime is None:
            logger.debug(format_log_event("agent.review_timer.scan_skipped", reason="unbound"))
            return
        scheduler = runtime.agent_profile_for_bot(self._bot_id).agent_scheduler
        due_plans = scheduler.due_review_plans(limit=self._batch_limit)
        logger.debug(
            format_log_event(
                "agent.review_timer.scan",
                bot_id=self._bot_id,
                due_count=len(due_plans),
                batch_limit=self._batch_limit,
            )
        )
        for plan in due_plans:
            if plan.session_id in self._in_flight:
                logger.debug(
                    format_log_event(
                        "agent.review_timer.skip",
                        bot_id=self._bot_id,
                        session_id=plan.session_id,
                        reason="in_flight",
                    )
                )
                continue
            self._in_flight.add(plan.session_id)
            try:
                logger.debug(
                    format_log_event(
                        "agent.review_timer.dispatch",
                        bot_id=self._bot_id,
                        session_id=plan.session_id,
                        next_review_at=f"{plan.next_review_at:.2f}",
                        reason=plan.reason,
                    )
                )
                await runtime.handle_agent_signal(
                    AgentSignal(
                        signal_id=f"review-due:{plan.session_id}:{int(plan.next_review_at)}",
                        kind=AgentSignalKind.REVIEW_DUE,
                        source=AgentSignalSource.TIMER,
                        session_id=plan.session_id,
                        occurred_at=plan.next_review_at,
                        bot_id=self._bot_id,
                        timer=AgentTimerSignal(
                            trigger=AgentSignalKind.REVIEW_DUE.value,
                            due_at=plan.next_review_at,
                            plan_id=f"{plan.session_id}:{int(plan.next_review_at)}",
                        ),
                        meta={"review_plan": plan.reason},
                    )
                )
            except Exception:
                logger.exception(
                    "Agent review due timer failed for session %s",
                    plan.session_id,
                )
            finally:
                self._in_flight.discard(plan.session_id)

    async def _run_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._tick_interval_seconds)
                await self.run_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Agent review due timer stopped unexpectedly")


__all__ = ["ReviewDueTimerService"]
