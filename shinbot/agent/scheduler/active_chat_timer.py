"""Session-bound active chat timer service."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Protocol

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    supervised_backoff_seconds,
)
from shinbot.agent.runtime.task_manager import (
    AgentTaskQuiescence,
    cancel_and_wait_for_tasks,
)
from shinbot.agent.scheduler.models import AgentState
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


class ActiveChatTimer(Protocol):
    """Lifecycle boundary for per-session active chat tick tasks."""

    def bind_agent_runtime(self, runtime: AgentRuntime, *, bot_id: str = "") -> None:
        """Bind the runtime entry point used by timer ticks."""

    def bind_task_scope(self, scope: AgentTaskScope) -> None:
        """Bind the task scope used to manage timer tasks."""

    def start(self, session_id: str) -> None:
        """Start a session-bound active chat timer if one is not running."""

    def cancel(self, session_id: str) -> None:
        """Cancel one session-bound active chat timer."""

    async def shutdown(self) -> None:
        """Cancel all active chat timers."""


class ActiveChatTimerService:
    """Run one lightweight 5s tick loop for each active chat session."""

    def __init__(self, *, tick_interval_seconds: float = 5.0) -> None:
        """Initialise the active chat timer service.

        Args:
            tick_interval_seconds: Seconds between idle-detection ticks.
        """
        self._tick_interval_seconds = tick_interval_seconds
        self._runtime: AgentRuntime | None = None
        self._bot_id = ""
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._retiring_tasks: dict[asyncio.Task[None], str] = {}
        self._task_scope: AgentTaskScope | None = None
        self._health: dict[str, RuntimeServiceHealth] = {}

    def bind_agent_runtime(self, runtime: AgentRuntime, *, bot_id: str = "") -> None:
        """Bind the runtime entry point used by timer ticks.

        Args:
            runtime: The agent runtime that will handle timer signals.
            bot_id: Optional bot identifier used for log context.
        """
        self._runtime = runtime
        self._bot_id = str(bot_id or "").strip()

    def bind_task_scope(self, scope: AgentTaskScope) -> None:
        """Bind the task scope used to manage timer tasks.

        When a task scope is bound, timer tasks are created through it
        rather than directly on the event loop, allowing coordinated
        cancellation during shutdown.

        Args:
            scope: The task scope that owns timer lifecycle management.
        """
        self._task_scope = scope

    def start(self, session_id: str) -> None:
        """Start a session-bound active chat timer if one is not running.

        Creates an asyncio task that periodically fires idle-detection
        ticks for the given session. If a timer is already running for
        this session, the call is a no-op.

        Args:
            session_id: The conversation session to monitor.
        """
        task = self._tasks.get(session_id)
        if task is not None and not task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("Active chat timer start skipped outside running event loop")
            return
        if self._task_scope is not None:
            task = self._task_scope.create_task(
                session_id,
                self._run_session_timer(session_id),
                name=f"active-chat-timer:{session_id}",
            )
        else:
            task = loop.create_task(
                self._run_session_timer(session_id),
                name=f"active-chat-timer:{session_id}",
            )
        self._tasks[session_id] = task
        self._track_session_task(session_id, task)
        health = self._health.setdefault(
            session_id,
            RuntimeServiceHealth(f"active_chat_timer:{session_id}"),
        )
        health.start()
        logger.debug(
            format_log_event(
                "agent.active_chat_timer.started",
                bot_id=self._bot_id,
                session_id=session_id,
                tick_interval_seconds=self._tick_interval_seconds,
            )
        )

    def cancel(self, session_id: str) -> None:
        """Cancel one session-bound active chat timer.

        Removes the timer task from internal tracking and requests
        cancellation. No-op if no timer is running for the session
        or if the timer task is the currently executing task.

        Args:
            session_id: The conversation session whose timer to cancel.
        """
        task = self._tasks.pop(session_id, None)
        health = self._health.get(session_id)
        if health is not None:
            health.stop()
        if task is None or task.done():
            return
        self._retiring_tasks[task] = session_id
        if task is not asyncio.current_task():
            task.cancel()
        logger.debug(
            format_log_event(
                "agent.active_chat_timer.cancelled",
                bot_id=self._bot_id,
                session_id=session_id,
            )
        )

    async def shutdown(self) -> None:
        """Cancel all active chat timers and await their completion.

        Clears internal task tracking, cancels every running timer
        task, then gathers them to ensure clean shutdown. After this
        call, no timer tasks will remain active.
        """
        tasks = list(dict.fromkeys([*self._tasks.values(), *self._retiring_tasks]))
        self._tasks.clear()
        self._retiring_tasks.clear()
        for health in self._health.values():
            health.stop()
        for task in tasks:
            if task is not asyncio.current_task() and not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(
                *(task for task in tasks if task is not asyncio.current_task()),
                return_exceptions=True,
            )
        logger.debug(
            format_log_event(
                "agent.active_chat_timer.stopped_all",
                bot_id=self._bot_id,
                count=len(tasks),
            )
        )

    def active_sessions(self) -> list[str]:
        """Return sessions that currently have a live timer task."""

        return [
            session_id
            for session_id, task in self._tasks.items()
            if not task.done()
        ]

    def pending_session_tasks(self, session_id: str) -> list[asyncio.Task[None]]:
        """Return known timer tasks and cancellation tails for one session.

        This is a process-local observation only. A timer task can be observed
        and cancelled here, but another process or future scheduler signal can
        still create new work.
        """

        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id must not be empty")
        tasks = [
            task
            for task, task_session_id in self._retiring_tasks.items()
            if task_session_id == normalized_session_id and not task.done()
        ]
        task = self._tasks.get(normalized_session_id)
        if task is not None and not task.done():
            tasks.append(task)
        return list(dict.fromkeys(tasks))

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AgentTaskQuiescence:
        """Cancel and observe the known timer task for one session locally.

        The result is not an adapter pause, durable lease, or authorization to
        transfer session ownership. It describes only task objects this timer
        service tracked in the current process.
        """

        return await cancel_and_wait_for_tasks(
            self.pending_session_tasks(session_id),
            timeout_seconds=timeout_seconds,
        )

    def health_snapshot(self, session_id: str) -> RuntimeServiceHealthSnapshot | None:
        """Return supervision health for one active-chat timer."""

        health = self._health.get(session_id)
        return health.snapshot() if health is not None else None

    def health_snapshots(self) -> list[RuntimeServiceHealthSnapshot]:
        """Return all known active-chat timer health snapshots."""

        return [self._health[key].snapshot() for key in sorted(self._health)]

    async def run_once(self, session_id: str) -> None:
        """Dispatch one active-chat timer tick for tests and manual maintenance."""

        runtime = self._runtime
        if runtime is None:
            logger.debug(
                format_log_event(
                    "agent.active_chat_timer.tick_skipped",
                    bot_id=self._bot_id,
                    session_id=session_id,
                    reason="unbound",
                )
            )
            return
        pause_session = getattr(runtime, "should_pause_session", None)
        if callable(pause_session) and pause_session(session_id):
            logger.debug(
                format_log_event(
                    "agent.active_chat_timer.tick_skipped",
                    bot_id=self._bot_id,
                    session_id=session_id,
                    reason="platform_unavailable",
                )
            )
            return
        signal_admission_frozen = getattr(
            runtime,
            "is_legacy_session_signal_admission_frozen",
            None,
        )
        if callable(signal_admission_frozen) and signal_admission_frozen(session_id):
            logger.debug(
                format_log_event(
                    "agent.active_chat_timer.tick_skipped",
                    bot_id=self._bot_id,
                    session_id=session_id,
                    reason="legacy_signal_admission_frozen",
                )
            )
            return
        now = time.time()
        logger.debug(
            format_log_event(
                "agent.active_chat_timer.tick",
                bot_id=self._bot_id,
                session_id=session_id,
                due_at=f"{now:.2f}",
            )
        )
        await runtime.handle_agent_signal(
            AgentSignal(
                signal_id=f"active-chat-tick:{session_id}:{int(now)}",
                kind=AgentSignalKind.ACTIVE_CHAT_TICK,
                source=AgentSignalSource.TIMER,
                session_id=session_id,
                occurred_at=now,
                bot_id=self._bot_id,
                timer=AgentTimerSignal(
                    trigger=AgentSignalKind.ACTIVE_CHAT_TICK.value,
                    due_at=now,
                ),
            )
        )

    async def _run_session_timer(self, session_id: str) -> None:
        delay = max(0.01, self._tick_interval_seconds)
        health = self._health.setdefault(
            session_id,
            RuntimeServiceHealth(f"active_chat_timer:{session_id}"),
        )
        try:
            while True:
                await asyncio.sleep(delay)
                health.scan_started()
                try:
                    await self.run_once(session_id)
                    runtime = self._runtime
                    if runtime is None:
                        return
                    scheduler = runtime.agent_profile_for_bot(
                        self._bot_id
                    ).agent_scheduler
                    state = scheduler.state_for(session_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    health.failed(exc)
                    logger.exception(
                        format_log_event(
                            "agent.active_chat_timer.iteration_failed",
                            bot_id=self._bot_id,
                            session_id=session_id,
                            error_code=type(exc).__name__,
                            consecutive_failures=(
                                health.snapshot().consecutive_failures
                            ),
                        )
                    )
                    delay = supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=health.snapshot().consecutive_failures,
                    )
                    continue
                health.succeeded()
                delay = max(0.01, self._tick_interval_seconds)
                if state != AgentState.ACTIVE_CHAT:
                    logger.debug(
                        format_log_event(
                            "agent.active_chat_timer.exit",
                            bot_id=self._bot_id,
                            session_id=session_id,
                            reason="state_changed",
                            state=state.value,
                        )
                    )
                    return
        except asyncio.CancelledError:
            raise
        finally:
            health.stop()
            task = self._tasks.get(session_id)
            if task is asyncio.current_task():
                self._tasks.pop(session_id, None)

    def _track_session_task(self, session_id: str, task: asyncio.Task[None]) -> None:
        """Arrange cleanup after a timer exits or finishes a cancellation tail."""

        task.add_done_callback(
            lambda completed, task_session_id=session_id: self._finish_session_task(
                task_session_id,
                completed,
            )
        )

    def _finish_session_task(
        self,
        session_id: str,
        task: asyncio.Task[None],
    ) -> None:
        """Forget a timer only once the exact task has ended."""

        self._retiring_tasks.pop(task, None)
        if self._tasks.get(session_id) is task:
            self._tasks.pop(session_id, None)


__all__ = ["ActiveChatTimer", "ActiveChatTimerService"]
