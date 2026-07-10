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
    assert "agent.task.failed" in caplog.text
    assert "key=agent:test:failing" in caplog.text
    assert "error_code=RuntimeError" in caplog.text
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
    assert "agent.task.failed" not in caplog.text


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


@pytest.mark.asyncio
async def test_replaced_task_remains_tracked_until_cancellation_finishes() -> None:
    manager = AgentTaskManager()
    old_started = asyncio.Event()
    old_cancelled = asyncio.Event()
    release_old = asyncio.Event()

    async def old_worker() -> None:
        old_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            old_cancelled.set()
            await release_old.wait()

    async def new_worker() -> None:
        await asyncio.Event().wait()

    old_task = manager.create_task("agent:test:review", old_worker())
    await old_started.wait()
    new_task = manager.create_task("agent:test:review", new_worker())
    await old_cancelled.wait()

    assert set(manager.tasks(prefix="agent:test:review")) == {old_task, new_task}

    release_old.set()
    await old_task
    assert manager.tasks(prefix="agent:test:review") == [new_task]
    await manager.shutdown()


@pytest.mark.asyncio
async def test_task_manager_retains_failure_snapshot_until_next_attempt() -> None:
    manager = AgentTaskManager()

    async def fail() -> None:
        raise RuntimeError("review handoff failed")

    manager.create_task("agent:test:review", fail(), name="review:failed")
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    failures = manager.failures(prefix="agent:test:review")
    assert len(failures) == 1
    assert failures[0].name == "review:failed"
    assert failures[0].error == "RuntimeError: review handoff failed"

    replacement = manager.create_task(
        "agent:test:review",
        asyncio.Event().wait(),
        name="review:replacement",
    )
    assert manager.failures(prefix="agent:test:review") == []
    replacement.cancel()
    await asyncio.gather(replacement, return_exceptions=True)
