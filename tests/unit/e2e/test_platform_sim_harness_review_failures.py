from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from shinbot.agent.runtime.task_manager import AgentTaskManager
from tests.e2e.platform_sim.harness import drain_agent_runtime


@pytest.mark.asyncio
async def test_drain_agent_runtime_surfaces_completed_review_failure() -> None:
    task_manager = AgentTaskManager()
    profile = SimpleNamespace(bot_id="", profile_id="test-profile")
    runtime = SimpleNamespace(
        task_manager=task_manager,
        _unique_profiles=lambda: [profile],
    )
    bot = SimpleNamespace(agent_runtime=runtime)

    async def fail_review() -> None:
        raise RuntimeError("review reply commit failed")

    review_task = task_manager.scope("agent:test-profile:review_workflow").create_task(
        "session-1",
        fail_review(),
        name="review:session-1",
    )
    await asyncio.sleep(0)
    assert review_task.done() is True
    assert task_manager.failures(prefix="agent:test-profile:review_workflow") == []

    try:
        with pytest.raises(
            RuntimeError,
            match=(
                "agent review workflow failed: review:session-1: "
                "RuntimeError: review reply commit failed"
            ),
        ):
            await drain_agent_runtime(bot, {"enabled": True})
    finally:
        await task_manager.shutdown()
