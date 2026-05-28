"""Task management for Agent-owned background work."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, TypeVar

from shinbot.utils.logger import format_log_event, get_logger

T = TypeVar("T")

logger = get_logger(__name__, source="agent:task", color="yellow")


@dataclass(slots=True, frozen=True)
class AgentTaskSnapshot:
    """Read-only status for one Agent-owned background task."""

    key: str
    name: str
    done: bool
    cancelled: bool
    error: str | None = None


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
        logger.debug(
            format_log_event(
                "agent.task.started",
                key=qualified_key,
                name=task.get_name(),
            )
        )
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

    def snapshots(self, *, prefix: str | None = None) -> list[AgentTaskSnapshot]:
        """Return read-only status for tracked tasks, optionally filtered by prefix."""

        normalized_prefix = self._normalize_key(prefix) if prefix else None
        result: list[AgentTaskSnapshot] = []
        for key, task in sorted(self._tasks.items()):
            if normalized_prefix is not None and not key.startswith(normalized_prefix):
                continue
            result.append(self._snapshot_task(key, task))
        return result

    def cancel(self, key: str) -> None:
        """Cancel one named task."""

        qualified_key = self._normalize_key(key)
        task = self._tasks.pop(qualified_key, None)
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        logger.debug(
            format_log_event(
                "agent.task.cancelled",
                key=qualified_key,
                name=task.get_name(),
            )
        )

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
            logger.debug(
                format_log_event(
                    "agent.task.shutdown",
                    prefix=qualified_prefix or "",
                    count=len(tasks),
                )
            )

    def scope(self, namespace: str) -> AgentTaskScope:
        """Create a namespaced task scope."""

        return AgentTaskScope(self, namespace)

    def _finish(self, key: str, task: asyncio.Task[Any]) -> None:
        if self._tasks.get(key) is task:
            self._tasks.pop(key, None)
        if task.cancelled():
            logger.debug(
                format_log_event(
                    "agent.task.finished",
                    key=key,
                    name=task.get_name(),
                    status="cancelled",
                )
            )
            return
        error = task.exception()
        if error is not None:
            logger.error(
                format_log_event(
                    "agent.task.failed",
                    key=key,
                    name=task.get_name(),
                    error_code=type(error).__name__,
                ),
                exc_info=(type(error), error, error.__traceback__),
            )
            return
        logger.debug(
            format_log_event(
                "agent.task.finished",
                key=key,
                name=task.get_name(),
                status="success",
            )
        )

    @staticmethod
    def _snapshot_task(key: str, task: asyncio.Task[Any]) -> AgentTaskSnapshot:
        error: str | None = None
        if task.done() and not task.cancelled():
            exception = task.exception()
            if exception is not None:
                error = f"{type(exception).__name__}: {exception}"
        return AgentTaskSnapshot(
            key=key,
            name=task.get_name(),
            done=task.done(),
            cancelled=task.cancelled(),
            error=error,
        )

    @staticmethod
    def _normalize_key(key: str | None) -> str:
        return str(key or "").strip()


@dataclass(slots=True)
class AgentTaskScope:
    """Namespaced task access for one runtime profile or subsystem."""

    manager: AgentTaskManager
    namespace: str

    def key(self, suffix: str) -> str:
        """Build a fully-qualified task key by appending *suffix* to the namespace."""
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

    def snapshots(self, *, prefix: str | None = None) -> list[AgentTaskSnapshot]:
        """Return task snapshots in this namespace, optionally filtered by sub-prefix."""

        return self.manager.snapshots(prefix=self.key(prefix) if prefix else self.namespace)

    def cancel(self, suffix: str) -> None:
        """Cancel one namespaced task."""

        self.manager.cancel(self.key(suffix))

    async def shutdown(self) -> None:
        """Cancel all tasks in this scope."""

        await self.manager.shutdown(prefix=self.namespace)


__all__ = ["AgentTaskManager", "AgentTaskScope", "AgentTaskSnapshot"]
