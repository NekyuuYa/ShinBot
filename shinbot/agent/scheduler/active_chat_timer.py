"""Session-bound active chat timer service."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Protocol

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
        self._tick_interval_seconds = tick_interval_seconds
        self._runtime: AgentRuntime | None = None
        self._bot_id = ""
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._task_scope: AgentTaskScope | None = None

    def bind_agent_runtime(self, runtime: AgentRuntime, *, bot_id: str = "") -> None:
        self._runtime = runtime
        self._bot_id = str(bot_id or "").strip()

    def bind_task_scope(self, scope: AgentTaskScope) -> None:
        self._task_scope = scope

    def start(self, session_id: str) -> None:
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
        logger.debug(
            format_log_event(
                "agent.active_chat_timer.started",
                bot_id=self._bot_id,
                session_id=session_id,
                tick_interval_seconds=self._tick_interval_seconds,
            )
        )

    def cancel(self, session_id: str) -> None:
        task = self._tasks.pop(session_id, None)
        if task is None or task.done():
            return
        if task is asyncio.current_task():
            return
        task.cancel()
        logger.debug(
            format_log_event(
                "agent.active_chat_timer.cancelled",
                bot_id=self._bot_id,
                session_id=session_id,
            )
        )

    async def shutdown(self) -> None:
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
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
        try:
            while True:
                await asyncio.sleep(self._tick_interval_seconds)
                await self.run_once(session_id)
                if self._runtime is None:
                    return
                scheduler = self._runtime.agent_profile_for_bot(self._bot_id).agent_scheduler
                if scheduler.state_for(session_id) != AgentState.ACTIVE_CHAT:
                    logger.debug(
                        format_log_event(
                            "agent.active_chat_timer.exit",
                            bot_id=self._bot_id,
                            session_id=session_id,
                            reason="state_changed",
                            state=scheduler.state_for(session_id).value,
                        )
                    )
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Active chat timer failed for session %s", session_id)
        finally:
            task = self._tasks.get(session_id)
            if task is asyncio.current_task():
                self._tasks.pop(session_id, None)


__all__ = ["ActiveChatTimer", "ActiveChatTimerService"]
