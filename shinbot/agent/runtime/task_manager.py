"""Task management for Agent-owned background work."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)


class AgentTaskManager:
    """Register and cancel named Agent background tasks."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    def create_task(
        self,
        key: str,
        coro: Awaitable[T],
        *,
        name: str | None = None,
    ) -> asyncio.Task[T]:
        """Create or replace one named task."""

        qualified_key = self._normalize_key(key)
        self.cancel(qualified_key)
        loop = asyncio.get_running_loop()
        task = loop.create_task(coro, name=name or qualified_key)
        self._tasks[qualified_key] = task
        task.add_done_callback(
            lambda completed, task_key=qualified_key: self._finish(task_key, completed)
        )
        return task

    def task(self, key: str) -> asyncio.Task[Any] | None:
        """Return one live task by key, if present."""

        task = self._tasks.get(self._normalize_key(key))
        if task is None or task.done():
            return None
        return task

    def tasks(self, *, prefix: str | None = None) -> list[asyncio.Task[Any]]:
        """Return all live tasks, optionally filtered by key prefix."""

        prefix = self._normalize_key(prefix) if prefix else None
        return [
            task
            for key, task in self._tasks.items()
            if (prefix is None or key.startswith(prefix)) and not task.done()
        ]

    def cancel(self, key: str) -> None:
        """Cancel one named task."""

        qualified_key = self._normalize_key(key)
        task = self._tasks.pop(qualified_key, None)
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()

    async def shutdown(self, *, prefix: str | None = None) -> None:
        """Cancel all tracked tasks, or all matching a prefix."""

        qualified_prefix = self._normalize_key(prefix) if prefix else None
        tasks = [
            task
            for key, task in self._tasks.items()
            if not task.done() and (qualified_prefix is None or key.startswith(qualified_prefix))
        ]
        for task in tasks:
            task.cancel()
        for key, task in list(self._tasks.items()):
            if qualified_prefix is None or key.startswith(qualified_prefix):
                if task is not asyncio.current_task():
                    self._tasks.pop(key, None)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def scope(self, namespace: str) -> "AgentTaskScope":
        """Create a namespaced task scope."""

        return AgentTaskScope(self, namespace)

    def _finish(self, key: str, task: asyncio.Task[Any]) -> None:
        if self._tasks.get(key) is task:
            self._tasks.pop(key, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.exception("Agent background task failed: %s", key, exc_info=error)

    @staticmethod
    def _normalize_key(key: str | None) -> str:
        return str(key or "").strip()


@dataclass(slots=True)
class AgentTaskScope:
    """Namespaced task access for one runtime profile or subsystem."""

    manager: AgentTaskManager
    namespace: str

    def key(self, suffix: str) -> str:
        suffix = str(suffix or "").strip()
        base = str(self.namespace or "").strip()
        return f"{base}:{suffix}" if base else suffix

    def create_task(
        self,
        suffix: str,
        coro: Awaitable[T],
        *,
        name: str | None = None,
    ) -> asyncio.Task[T]:
        """Create or replace one namespaced task."""

        qualified_key = self.key(suffix)
        return self.manager.create_task(qualified_key, coro, name=name or qualified_key)

    def task(self, suffix: str) -> asyncio.Task[Any] | None:
        """Return one namespaced live task, if present."""

        return self.manager.task(self.key(suffix))

    def tasks(self, *, prefix: str | None = None) -> list[asyncio.Task[Any]]:
        """Return live namespaced tasks, optionally filtered by a sub-prefix."""

        return self.manager.tasks(prefix=self.key(prefix) if prefix else self.namespace)

    def cancel(self, suffix: str) -> None:
        """Cancel one namespaced task."""

        self.manager.cancel(self.key(suffix))

    async def shutdown(self) -> None:
        """Cancel all tasks in this scope."""

        await self.manager.shutdown(prefix=self.namespace)


__all__ = ["AgentTaskManager", "AgentTaskScope"]
