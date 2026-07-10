from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from shinbot.agent.runtime.task_manager import AgentTaskManager
from tests.e2e.platform_sim.harness import drain_agent_runtime


async def _wait_forever() -> None:
    await asyncio.Event().wait()


def _runtime_fixture(
    task_manager: AgentTaskManager,
    *,
    review_coordinator: Any = None,
) -> tuple[SimpleNamespace, SimpleNamespace]:
    profile = SimpleNamespace(
        bot_id="",
        profile_id="test-profile",
        review_coordinator=review_coordinator,
    )
    runtime = SimpleNamespace(
        task_manager=task_manager,
        _unique_profiles=lambda: [profile],
    )
    return SimpleNamespace(agent_runtime=runtime), profile


@pytest.mark.asyncio
async def test_drain_agent_runtime_raises_with_pending_review_task_name() -> None:
    task_manager = AgentTaskManager()
    bot, _profile = _runtime_fixture(task_manager)
    task_manager.scope("agent:test-profile:review_workflow").create_task(
        "session-1",
        _wait_forever(),
        name="review:session-1",
    )

    try:
        with pytest.raises(TimeoutError, match=r"pending tasks: review:session-1"):
            await drain_agent_runtime(bot, {"enabled": True}, timeout=0.01)
    finally:
        await task_manager.shutdown()


@pytest.mark.asyncio
async def test_drain_agent_runtime_reports_late_reply_commit_task() -> None:
    task_manager = AgentTaskManager()
    reply_task = asyncio.create_task(
        _wait_forever(),
        name="review-reply-commit:session-1",
    )

    class ReplyCoordinator:
        def pending_reply_commit_tasks(self) -> list[asyncio.Task[Any]]:
            return [reply_task] if not reply_task.done() else []

    bot, _profile = _runtime_fixture(
        task_manager,
        review_coordinator=ReplyCoordinator(),
    )

    try:
        with pytest.raises(
            TimeoutError,
            match=r"pending tasks: review-reply-commit:session-1",
        ):
            await drain_agent_runtime(bot, {"enabled": True}, timeout=0.01)
    finally:
        reply_task.cancel()
        await asyncio.gather(reply_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_drain_agent_runtime_bounds_bootstrap_wait_and_reports_task_name() -> None:
    task_manager = AgentTaskManager()
    bootstrap_task = asyncio.create_task(_wait_forever(), name="bootstrap:session-1")

    class BootstrapCoordinator:
        _bootstrap_tasks = {bootstrap_task}

        async def wait_pending_bootstraps(self) -> None:
            await asyncio.gather(*self._bootstrap_tasks, return_exceptions=True)

    bot, _profile = _runtime_fixture(
        task_manager,
        review_coordinator=BootstrapCoordinator(),
    )

    try:
        with pytest.raises(TimeoutError, match=r"pending tasks: bootstrap:session-1"):
            await drain_agent_runtime(
                bot,
                {"enabled": True, "waitForBootstraps": True},
                timeout=0.01,
            )
        assert not bootstrap_task.done()
    finally:
        if not bootstrap_task.done():
            bootstrap_task.cancel()
        await asyncio.gather(bootstrap_task, return_exceptions=True)
