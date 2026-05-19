"""Session-bound active chat timer service."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol

from shinbot.agent.scheduler.models import AgentState

if TYPE_CHECKING:
    from shinbot.agent.scheduler.scheduler import AgentScheduler

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from shinbot.agent.scheduler.models import ReviewPlan

ActiveChatIdleReviewPlanner = Callable[
    [str],
    Awaitable["ReviewPlan | None"],
]


class ActiveChatTimer(Protocol):
    """Lifecycle boundary for per-session active chat tick tasks."""

    def bind_agent_scheduler(self, scheduler: AgentScheduler) -> None:
        """Bind the scheduler used by timer ticks."""

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
        self._scheduler: AgentScheduler | None = None
        self._idle_review_planner: ActiveChatIdleReviewPlanner | None = None
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def bind_agent_scheduler(self, scheduler: AgentScheduler) -> None:
        self._scheduler = scheduler

    def bind_idle_review_planner(
        self,
        planner: ActiveChatIdleReviewPlanner | None,
    ) -> None:
        """Bind an async hook used before decay returns ACTIVE_CHAT to IDLE."""
        self._idle_review_planner = planner

    def start(self, session_id: str) -> None:
        task = self._tasks.get(session_id)
        if task is not None and not task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("Active chat timer start skipped outside running event loop")
            return
        self._tasks[session_id] = loop.create_task(
            self._run_session_timer(session_id),
            name=f"active-chat-timer:{session_id}",
        )

    def cancel(self, session_id: str) -> None:
        task = self._tasks.pop(session_id, None)
        if task is None or task.done():
            return
        if task is asyncio.current_task():
            return
        task.cancel()

    async def shutdown(self) -> None:
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def active_sessions(self) -> list[str]:
        """Return sessions that currently have a live timer task."""

        return [
            session_id
            for session_id, task in self._tasks.items()
            if not task.done()
        ]

    async def _run_session_timer(self, session_id: str) -> None:
        try:
            while True:
                await asyncio.sleep(self._tick_interval_seconds)
                if self._scheduler is None:
                    return
                next_review_plan = None
                preview = self._scheduler.preview_active_chat_tick(session_id)
                if preview.will_return_idle and self._idle_review_planner is not None:
                    next_review_plan = await self._idle_review_planner(session_id)
                decision = self._scheduler.tick_active_chat(
                    session_id,
                    next_review_plan=next_review_plan,
                )
                if decision.state != AgentState.ACTIVE_CHAT:
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Active chat timer failed for session %s", session_id)
        finally:
            task = self._tasks.get(session_id)
            if task is asyncio.current_task():
                self._tasks.pop(session_id, None)


__all__ = ["ActiveChatIdleReviewPlanner", "ActiveChatTimer", "ActiveChatTimerService"]
