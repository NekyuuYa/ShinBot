"""Background timer that wakes due idle review plans."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shinbot.agent.scheduler.scheduler import AgentScheduler

logger = logging.getLogger(__name__)


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
        self._scheduler: AgentScheduler | None = None
        self._task: asyncio.Task[None] | None = None
        self._in_flight: set[str] = set()

    def bind_agent_scheduler(self, scheduler: AgentScheduler) -> None:
        self._scheduler = scheduler

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("Review due timer start skipped outside running event loop")
            return
        self._task = loop.create_task(self._run_loop(), name="agent-review-due-timer")

    async def shutdown(self) -> None:
        task = self._task
        self._task = None
        if task is None or task.done():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def run_once(self) -> None:
        """Run one polling pass for tests and manual maintenance."""

        scheduler = self._scheduler
        if scheduler is None:
            return
        for plan in scheduler.due_review_plans(limit=self._batch_limit):
            if plan.session_id in self._in_flight:
                continue
            self._in_flight.add(plan.session_id)
            try:
                await scheduler.run_due_review(plan.session_id)
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
