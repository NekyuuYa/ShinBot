"""Background timer that wakes due idle review plans."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    supervised_backoff_seconds,
)
from shinbot.agent.runtime.task_manager import (
    AgentTaskQuiescence,
    cancel_and_wait_for_tasks,
)
from shinbot.agent.scheduler.models import ReviewPlan
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
    """Poll legacy due plans without crossing an Actor v2 ownership boundary."""

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
        self._in_flight_tasks: dict[str, asyncio.Task[None]] = {}
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
        tasks = list(self._in_flight_tasks.values())
        self._in_flight_tasks.clear()
        current_task = asyncio.current_task()
        for pending_task in [task, *tasks]:
            if (
                pending_task is not None
                and pending_task is not current_task
                and not pending_task.done()
            ):
                pending_task.cancel()
        awaitables = [
            pending_task
            for pending_task in [task, *tasks]
            if pending_task is not None and pending_task is not current_task
        ]
        if awaitables:
            await asyncio.gather(*awaitables, return_exceptions=True)
        self._in_flight.clear()
        self._health.stop()
        logger.debug(format_log_event("agent.review_timer.stopped", bot_id=self._bot_id))

    def pending_session_tasks(self, session_id: str) -> list[asyncio.Task[None]]:
        """Return the in-flight due-review dispatch task for one session.

        The global polling loop is intentionally not returned: it may be
        serving other sessions. This method observes only the child dispatch
        task currently owned by the requested session in this process.
        """

        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id must not be empty")
        task = self._in_flight_tasks.get(normalized_session_id)
        return [] if task is None or task.done() else [task]

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AgentTaskQuiescence:
        """Cancel and observe one local due-review dispatch child task.

        This cannot stop the shared poller, other process instances, or a
        future timer tick. It only drains the currently tracked task for this
        exact session.
        """

        return await cancel_and_wait_for_tasks(
            self.pending_session_tasks(session_id),
            timeout_seconds=timeout_seconds,
        )

    def health_snapshot(self) -> RuntimeServiceHealthSnapshot:
        """Return current review timer supervision health."""

        return self._health.snapshot()

    async def run_once(self) -> None:
        """Run one polling pass for tests and manual maintenance."""

        runtime = self._runtime
        if runtime is None:
            logger.debug(format_log_event("agent.review_timer.scan_skipped", reason="unbound"))
            return
        profile = runtime.agent_profile_for_bot(self._bot_id)
        scheduler = profile.agent_scheduler
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
        signal_admission_frozen = getattr(
            runtime,
            "is_legacy_session_signal_admission_frozen",
            None,
        )
        review_due_admission_reason = getattr(
            runtime,
            "legacy_review_due_admission_reason",
            None,
        )
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
            if (
                callable(signal_admission_frozen)
                and signal_admission_frozen(plan.session_id)
            ):
                logger.debug(
                    format_log_event(
                        "agent.review_timer.skip",
                        bot_id=self._bot_id,
                        session_id=plan.session_id,
                        reason="legacy_signal_admission_frozen",
                    )
                )
                continue
            if callable(review_due_admission_reason):
                admission_reason = review_due_admission_reason(
                    profile.profile_id,
                    plan.session_id,
                )
                if admission_reason:
                    logger.debug(
                        format_log_event(
                            "agent.review_timer.skip",
                            bot_id=self._bot_id,
                            session_id=plan.session_id,
                            reason=admission_reason,
                        )
                    )
                    continue
            self._in_flight.add(plan.session_id)
            dispatch_task = self._create_dispatch_task(runtime, plan)
            self._in_flight_tasks[plan.session_id] = dispatch_task
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
                await dispatch_task
            except asyncio.CancelledError:
                current_task = asyncio.current_task()
                if current_task is not None and current_task.cancelling():
                    raise
                logger.debug(
                    format_log_event(
                        "agent.review_timer.dispatch.cancelled",
                        bot_id=self._bot_id,
                        session_id=plan.session_id,
                        reason="local_session_quiesce",
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
                if self._in_flight_tasks.get(plan.session_id) is dispatch_task:
                    self._in_flight_tasks.pop(plan.session_id, None)
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

    def _create_dispatch_task(
        self,
        runtime: AgentRuntime,
        plan: ReviewPlan,
    ) -> asyncio.Task[None]:
        """Create one explicitly awaited child task without changing polling order.

        The polling loop owns and classifies this task's exceptions, so it must
        not enter ``AgentTaskManager`` as an independent background failure.
        """

        signal = AgentSignal(
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
        coroutine = self._dispatch_signal(runtime, signal)
        task_name = f"review-due-dispatch:{plan.session_id}"
        return asyncio.create_task(coroutine, name=task_name)

    @staticmethod
    async def _dispatch_signal(runtime: AgentRuntime, signal: AgentSignal) -> None:
        """Await one runtime signal through a coroutine accepted by task scopes."""

        await runtime.handle_agent_signal(signal)


__all__ = ["ReviewDueDispatchError", "ReviewDueTimerService"]
