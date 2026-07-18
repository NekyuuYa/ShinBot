"""Task management for Agent-owned background work."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable
from dataclasses import dataclass
from enum import StrEnum
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


class AgentTaskQuiescenceStatus(StrEnum):
    """Outcome of one local asyncio-task drain observation."""

    NO_LOCAL_TASKS = "no_local_tasks"
    QUIESCENT = "quiescent"
    TIMED_OUT = "timed_out"
    CURRENT_TASK_ACTIVE = "current_task_active"


@dataclass(slots=True, frozen=True)
class AgentTaskQuiescence:
    """Process-local result of cancelling and waiting for known tasks.

    The result describes only the exact task objects supplied by the caller.
    It cannot prove that another process, an untracked coroutine, or an
    external model/tool invocation has stopped.
    """

    status: AgentTaskQuiescenceStatus
    matched_task_names: tuple[str, ...] = ()
    cancelled_task_names: tuple[str, ...] = ()
    remaining_task_names: tuple[str, ...] = ()

    @property
    def locally_confirmed_quiescent(self) -> bool:
        """Return whether at least one observed local task ended."""

        return self.status is AgentTaskQuiescenceStatus.QUIESCENT


async def cancel_and_wait_for_tasks(
    tasks: list[asyncio.Task[Any]] | tuple[asyncio.Task[Any], ...],
    *,
    timeout_seconds: float | None = None,
    cancel: bool = True,
) -> AgentTaskQuiescence:
    """Cancel and await a fixed set of local tasks without hiding timeouts.

    A task owned by the caller is never cancelled or awaited because doing so
    would deadlock the control path. Empty observations deliberately remain
    distinct from a positive local quiescence confirmation.
    """

    timeout = _normalize_quiescence_timeout(timeout_seconds)
    unique_tasks = tuple(
        dict.fromkeys(task for task in tasks if not task.done())
    )
    if not unique_tasks:
        return AgentTaskQuiescence(AgentTaskQuiescenceStatus.NO_LOCAL_TASKS)

    current_task = asyncio.current_task()
    current_matches = tuple(task for task in unique_tasks if task is current_task)
    observed_tasks = tuple(task for task in unique_tasks if task is not current_task)
    matched_names = tuple(sorted(task.get_name() for task in unique_tasks))
    cancelled_names: list[str] = []
    if cancel:
        for task in observed_tasks:
            task.cancel()
            cancelled_names.append(task.get_name())
    if current_matches:
        remaining_names = tuple(sorted(task.get_name() for task in unique_tasks if not task.done()))
        return AgentTaskQuiescence(
            status=AgentTaskQuiescenceStatus.CURRENT_TASK_ACTIVE,
            matched_task_names=matched_names,
            cancelled_task_names=tuple(sorted(cancelled_names)),
            remaining_task_names=remaining_names,
        )
    if observed_tasks:
        _done, pending = await asyncio.wait(observed_tasks, timeout=timeout)
    else:
        pending = set()
    if pending:
        return AgentTaskQuiescence(
            status=AgentTaskQuiescenceStatus.TIMED_OUT,
            matched_task_names=matched_names,
            cancelled_task_names=tuple(sorted(cancelled_names)),
            remaining_task_names=tuple(sorted(task.get_name() for task in pending)),
        )
    return AgentTaskQuiescence(
        status=AgentTaskQuiescenceStatus.QUIESCENT,
        matched_task_names=matched_names,
        cancelled_task_names=tuple(sorted(cancelled_names)),
    )


def _normalize_quiescence_timeout(timeout_seconds: float | None) -> float | None:
    """Validate an optional task-drain timeout."""

    if timeout_seconds is None:
        return None
    timeout = float(timeout_seconds)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout_seconds must be finite and non-negative")
    return timeout


class AgentTaskManager:
    """Register and cancel named Agent background tasks."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._retired_tasks: dict[asyncio.Task[Any], str] = {}
        self._failures: dict[str, AgentTaskSnapshot] = {}

    def create_task(
        self,
        key: str,
        coro: Awaitable[T],
        *,
        name: str | None = None,
    ) -> asyncio.Task[T]:
        """Create or replace one named task."""

        qualified_key = self._normalize_key(key)
        self._failures.pop(qualified_key, None)
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
        active = [
            task
            for key, task in self._tasks.items()
            if (prefix is None or key.startswith(prefix)) and not task.done()
        ]
        retired = [
            task
            for task, key in self._retired_tasks.items()
            if (prefix is None or key.startswith(prefix)) and not task.done()
        ]
        return [*active, *retired]

    def snapshots(self, *, prefix: str | None = None) -> list[AgentTaskSnapshot]:
        """Return read-only status for tracked tasks, optionally filtered by prefix."""

        normalized_prefix = self._normalize_key(prefix) if prefix else None
        result: list[AgentTaskSnapshot] = []
        for key, task in sorted(self._tasks.items()):
            if normalized_prefix is not None and not key.startswith(normalized_prefix):
                continue
            result.append(self._snapshot_task(key, task))
        for task, key in sorted(
            self._retired_tasks.items(),
            key=lambda item: (item[1], item[0].get_name()),
        ):
            if normalized_prefix is not None and not key.startswith(normalized_prefix):
                continue
            result.append(self._snapshot_task(key, task))
        return result

    def failures(self, *, prefix: str | None = None) -> list[AgentTaskSnapshot]:
        """Return the latest completed task failures, optionally filtered by key prefix."""

        normalized_prefix = self._normalize_key(prefix) if prefix else None
        return [
            snapshot
            for key, snapshot in sorted(self._failures.items())
            if normalized_prefix is None or key.startswith(normalized_prefix)
        ]

    def cancel(self, key: str) -> None:
        """Cancel one named task."""

        qualified_key = self._normalize_key(key)
        task = self._tasks.get(qualified_key)
        if task is None or task.done():
            return
        self._tasks.pop(qualified_key, None)
        self._retired_tasks[task] = qualified_key
        if task is asyncio.current_task():
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
        current_task = asyncio.current_task()
        active = [
            (key, task)
            for key, task in self._tasks.items()
            if task is not current_task
            and not task.done()
            and (qualified_prefix is None or key.startswith(qualified_prefix))
        ]
        retired = [
            (key, task)
            for task, key in self._retired_tasks.items()
            if task is not current_task
            and not task.done()
            and (qualified_prefix is None or key.startswith(qualified_prefix))
        ]
        tasks = [task for _key, task in [*active, *retired]]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for key, task in active:
            if self._tasks.get(key) is task:
                self._tasks.pop(key, None)
        for key, task in retired:
            if self._retired_tasks.get(task) == key:
                self._retired_tasks.pop(task, None)
        if qualified_prefix is None:
            self._failures.clear()
        else:
            for key in list(self._failures):
                if key.startswith(qualified_prefix):
                    self._failures.pop(key, None)
        if tasks:
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
        self._retired_tasks.pop(task, None)
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
            self._failures[key] = self._snapshot_task(key, task)
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


__all__ = [
    "AgentTaskManager",
    "AgentTaskQuiescence",
    "AgentTaskQuiescenceStatus",
    "AgentTaskScope",
    "AgentTaskSnapshot",
    "cancel_and_wait_for_tasks",
]
