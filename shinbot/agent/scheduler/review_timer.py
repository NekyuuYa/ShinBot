"""Background timer that wakes due idle review plans."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    supervised_backoff_seconds,
)
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


class ReviewDueDispatchError(RuntimeError):
    """Report one polling pass where one or more due sessions failed."""

    def __init__(self, failed_session_ids: tuple[str, ...]) -> None:
        self.failed_session_ids = failed_session_ids
        session_list = ", ".join(failed_session_ids)
        super().__init__(f"review-due dispatch failed for sessions: {session_list}")


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
        self._health = RuntimeServiceHealth("review_due_timer")

    def bind_agent_runtime(self, runtime: AgentRuntime, *, bot_id: str = "") -> None:
        """Bind the agent runtime used to dispatch review-due signals.
        Args:
            runtime: The agent runtime providing scheduler and signal dispatch.
            bot_id: The bot identifier for routing signals to the correct profile.
        """
        self._runtime = runtime
        self._bot_id = str(bot_id or "").strip()

    def bind_task_scope(self, scope: AgentTaskScope) -> None:
        """Bind the task scope for managed asyncio task creation.
        Args:
            scope: The agent task scope that owns background task lifecycles.
        """
        self._task_scope = scope

    def start(self) -> None:
        """Start the background polling loop that dispatches due review plans.
        The loop is a no-op if already running or if no asyncio event loop is
        available. Tasks are created through the bound task scope when set.
        """
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
        self._health.start()
        logger.info(
            format_log_event(
                "agent.review_timer.started",
                bot_id=self._bot_id,
                tick_interval_seconds=self._tick_interval_seconds,
                batch_limit=self._batch_limit,
            )
        )

    async def shutdown(self) -> None:
        """Cancel the background polling loop and wait for it to finish.
        Safe to call multiple times; subsequent calls are no-ops.
        """
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._health.stop()
        logger.debug(format_log_event("agent.review_timer.stopped", bot_id=self._bot_id))

    def health_snapshot(self) -> RuntimeServiceHealthSnapshot:
        """Return current review timer supervision health."""

        return self._health.snapshot()

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
        pause_session = getattr(runtime, "should_pause_session", None)
        dispatch_failures: list[tuple[str, Exception]] = []
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
            if callable(pause_session) and pause_session(plan.session_id):
                logger.debug(
                    format_log_event(
                        "agent.review_timer.skip",
                        bot_id=self._bot_id,
                        session_id=plan.session_id,
                        reason="platform_unavailable",
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
            except Exception as exc:
                dispatch_failures.append((plan.session_id, exc))
                logger.exception(
                    "Agent review due timer failed for session %s",
                    plan.session_id,
                )
            finally:
                self._in_flight.discard(plan.session_id)
        if dispatch_failures:
            error = ReviewDueDispatchError(
                tuple(session_id for session_id, _exc in dispatch_failures)
            )
            raise error from dispatch_failures[0][1]

    async def _run_loop(self) -> None:
        delay = max(0.01, self._tick_interval_seconds)
        try:
            while True:
                await asyncio.sleep(delay)
                self._health.scan_started()
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._health.failed(exc)
                    logger.exception(
                        format_log_event(
                            "agent.review_timer.iteration_failed",
                            bot_id=self._bot_id,
                            error_code=type(exc).__name__,
                            consecutive_failures=(
                                self._health.snapshot().consecutive_failures
                            ),
                        )
                    )
                    delay = supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=(
                            self._health.snapshot().consecutive_failures
                        ),
                    )
                    continue
                self._health.succeeded()
                delay = max(0.01, self._tick_interval_seconds)
        finally:
            self._health.stop()


__all__ = ["ReviewDueDispatchError", "ReviewDueTimerService"]
