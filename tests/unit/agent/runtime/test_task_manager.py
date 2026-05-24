from __future__ import annotations

import asyncio
import logging

import pytest

from shinbot.agent.runtime.task_manager import AgentTaskManager


@pytest.mark.asyncio
async def test_task_manager_logs_failed_background_task(caplog) -> None:
    manager = AgentTaskManager()

    async def fail() -> None:
        raise RuntimeError("task exploded")

    with caplog.at_level(logging.ERROR, logger="shinbot.agent.runtime.task_manager"):
        manager.create_task("agent:test:failing", fail())
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert manager.task("agent:test:failing") is None
    assert "Agent background task failed: agent:test:failing" in caplog.text
    assert "RuntimeError: task exploded" in caplog.text


@pytest.mark.asyncio
async def test_task_manager_does_not_log_cancelled_background_task(caplog) -> None:
    manager = AgentTaskManager()

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    with caplog.at_level(logging.ERROR, logger="shinbot.agent.runtime.task_manager"):
        manager.create_task("agent:test:cancelled", wait_forever())
        manager.cancel("agent:test:cancelled")
        await asyncio.sleep(0)

    assert manager.task("agent:test:cancelled") is None
    assert "Agent background task failed" not in caplog.text


@pytest.mark.asyncio
async def test_task_manager_snapshots_are_filtered_by_prefix() -> None:
    manager = AgentTaskManager()

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    manager.create_task("agent:bot-a:review_due_timer:loop", wait_forever())
    manager.create_task("agent:bot-b:review_due_timer:loop", wait_forever())

    snapshots = manager.snapshots(prefix="agent:bot-a:")

    assert [snapshot.key for snapshot in snapshots] == [
        "agent:bot-a:review_due_timer:loop"
    ]
    assert snapshots[0].name == "agent:bot-a:review_due_timer:loop"
    assert snapshots[0].done is False
    assert snapshots[0].cancelled is False
    assert snapshots[0].error is None

    await manager.shutdown()
