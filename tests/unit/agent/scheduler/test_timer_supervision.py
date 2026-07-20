from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from shinbot.agent.runtime.service_health import RuntimeServiceStatus
from shinbot.agent.runtime.task_manager import (
    AgentTaskManager,
    AgentTaskQuiescenceStatus,
)
from shinbot.agent.scheduler import (
    ActiveChatTimerService,
    AgentState,
    ReviewDueDispatchError,
    ReviewDueTimerService,
    ReviewPlan,
)
from shinbot.agent.signals import AgentSignal


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 1.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_review_timer_reports_dispatch_failure_after_processing_batch() -> None:
    plans = [
        ReviewPlan(
            session_id="bot:group:bad",
            next_review_at=10.0,
            reason="test_due",
        ),
        ReviewPlan(
            session_id="bot:group:healthy",
            next_review_at=10.0,
            reason="test_due",
        ),
    ]

    class Scheduler:
        def due_review_plans(self, *, limit: int) -> list[ReviewPlan]:
            assert limit == 50
            return plans

    class Profile:
        agent_scheduler = Scheduler()

    class Runtime:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

        async def handle_agent_signal(self, signal: AgentSignal) -> None:
            self.calls.append(signal.session_id)
            if signal.session_id == "bot:group:bad":
                raise ValueError("dispatch failed")

    runtime = Runtime()
    timer = ReviewDueTimerService()
    timer.bind_agent_runtime(runtime)  # type: ignore[arg-type]

    with pytest.raises(ReviewDueDispatchError) as error:
        await timer.run_once()

    assert error.value.failed_session_ids == ("bot:group:bad",)
    assert isinstance(error.value.__cause__, ValueError)
    assert runtime.calls == ["bot:group:bad", "bot:group:healthy"]


@pytest.mark.asyncio
async def test_review_timer_loop_recovers_after_dispatch_failure() -> None:
    plan = ReviewPlan(
        session_id="bot:group:room",
        next_review_at=10.0,
        reason="test_due",
    )
    first_failure = asyncio.Event()
    allow_recovery = asyncio.Event()

    class Scheduler:
        def due_review_plans(self, *, limit: int) -> list[ReviewPlan]:
            assert limit == 50
            return [plan]

    class Profile:
        agent_scheduler = Scheduler()

    class Runtime:
        def __init__(self) -> None:
            self.call_count = 0

        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

        async def handle_agent_signal(self, _signal: AgentSignal) -> None:
            self.call_count += 1
            if self.call_count == 1:
                first_failure.set()
                raise ValueError("transient dispatch failure")
            await allow_recovery.wait()

    runtime = Runtime()
    timer = ReviewDueTimerService(tick_interval_seconds=0.01)
    timer.bind_agent_runtime(runtime)  # type: ignore[arg-type]
    timer.start()
    try:
        await asyncio.wait_for(first_failure.wait(), timeout=1.0)
        await _wait_until(
            lambda: timer.health_snapshot().status
            == RuntimeServiceStatus.DEGRADED
        )
        degraded = timer.health_snapshot()
        assert degraded.consecutive_failures == 1
        assert degraded.last_error_code == "ReviewDueDispatchError"
        assert timer._task is not None and not timer._task.done()

        allow_recovery.set()
        await _wait_until(
            lambda: timer.health_snapshot().status == RuntimeServiceStatus.RUNNING
        )
        recovered = timer.health_snapshot()
        assert recovered.consecutive_failures == 0
        assert recovered.scan_count >= 2
        assert recovered.success_count >= 1
        assert recovered.last_error_code == "ReviewDueDispatchError"
    finally:
        await timer.shutdown()

    assert timer.health_snapshot().status == RuntimeServiceStatus.STOPPED


@pytest.mark.asyncio
async def test_active_chat_timer_skips_frozen_legacy_signal_admission() -> None:
    """A deliberate local freeze does not dispatch a tick into the scheduler."""

    session_id = "bot:group:room"

    class Runtime:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def is_legacy_session_signal_admission_frozen(self, value: str) -> bool:
            return value == session_id

        async def handle_agent_signal(self, signal: AgentSignal) -> None:
            self.calls.append(signal.session_id)

    runtime = Runtime()
    timer = ActiveChatTimerService()
    timer.bind_agent_runtime(runtime)  # type: ignore[arg-type]

    await timer.run_once(session_id)

    assert runtime.calls == []


@pytest.mark.asyncio
async def test_review_due_timer_quiescence_drains_only_the_session_dispatch() -> None:
    """A session drain does not need to cancel the shared polling loop."""

    session_id = "bot:group:room"
    started = asyncio.Event()
    cancelled = asyncio.Event()
    plan = ReviewPlan(
        session_id=session_id,
        next_review_at=10.0,
        reason="test_due",
    )

    class Scheduler:
        def due_review_plans(self, *, limit: int) -> list[ReviewPlan]:
            assert limit == 50
            return [plan]

    class Profile:
        agent_scheduler = Scheduler()

    class Runtime:
        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

        async def handle_agent_signal(self, _signal: AgentSignal) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    timer = ReviewDueTimerService()
    timer.bind_agent_runtime(Runtime())  # type: ignore[arg-type]
    run_once_task = asyncio.create_task(timer.run_once())
    try:
        await asyncio.wait_for(started.wait(), timeout=0.5)

        report = await timer.quiesce_session_tasks(
            session_id,
            timeout_seconds=0.5,
        )

        assert cancelled.is_set()
        assert report.status is AgentTaskQuiescenceStatus.QUIESCENT
        assert report.locally_confirmed_quiescent is True
        await asyncio.wait_for(run_once_task, timeout=0.5)
        assert timer.pending_session_tasks(session_id) == []
    finally:
        if not run_once_task.done():
            run_once_task.cancel()
            await asyncio.gather(run_once_task, return_exceptions=True)
        await timer.shutdown()


@pytest.mark.asyncio
async def test_review_timer_skips_a_session_with_frozen_legacy_signal_admission() -> None:
    """A deliberate local drain must not degrade global timer supervision."""

    session_id = "bot:group:room"
    plan = ReviewPlan(
        session_id=session_id,
        next_review_at=10.0,
        reason="test_due",
    )

    class Scheduler:
        def due_review_plans(self, *, limit: int) -> list[ReviewPlan]:
            assert limit == 50
            return [plan]

    class Profile:
        agent_scheduler = Scheduler()

    class Runtime:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

        def is_legacy_session_signal_admission_frozen(self, value: str) -> bool:
            return value == session_id

        async def handle_agent_signal(self, signal: AgentSignal) -> None:
            self.calls.append(signal.session_id)

    runtime = Runtime()
    timer = ReviewDueTimerService()
    timer.bind_agent_runtime(runtime)  # type: ignore[arg-type]

    await timer.run_once()

    assert runtime.calls == []
    assert timer.pending_session_tasks(session_id) == []


@pytest.mark.asyncio
async def test_review_timer_skips_nonlegacy_durable_ownership() -> None:
    """A due legacy plan cannot dispatch after Actor v2 takes ownership."""

    session_id = "bot:group:actor-owned"
    plan = ReviewPlan(
        session_id=session_id,
        next_review_at=10.0,
        reason="test_due",
    )

    class Scheduler:
        def due_review_plans(self, *, limit: int) -> list[ReviewPlan]:
            assert limit == 50
            return [plan]

    class Profile:
        profile_id = "bot-a"
        agent_scheduler = Scheduler()

    class Runtime:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.admission_checks: list[tuple[str, str]] = []

        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

        def legacy_review_due_admission_reason(
            self,
            profile_id: str,
            value: str,
        ) -> str:
            self.admission_checks.append((profile_id, value))
            return "actor_v2_owned"

        async def handle_agent_signal(self, signal: AgentSignal) -> None:
            self.calls.append(signal.session_id)

    runtime = Runtime()
    timer = ReviewDueTimerService()
    timer.bind_agent_runtime(runtime)  # type: ignore[arg-type]

    await timer.run_once()

    assert runtime.admission_checks == [("bot-a", session_id)]
    assert runtime.calls == []
    assert timer.pending_session_tasks(session_id) == []


@pytest.mark.asyncio
async def test_review_due_dispatch_failure_is_not_a_task_manager_failure() -> None:
    """The timer, not the background-task API, owns a due dispatch error."""

    plan = ReviewPlan(
        session_id="bot:group:room",
        next_review_at=10.0,
        reason="test_due",
    )

    class Scheduler:
        def due_review_plans(self, *, limit: int) -> list[ReviewPlan]:
            assert limit == 50
            return [plan]

    class Profile:
        agent_scheduler = Scheduler()

    class Runtime:
        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

        async def handle_agent_signal(self, _signal: AgentSignal) -> None:
            raise ValueError("dispatch failed")

    manager = AgentTaskManager()
    timer = ReviewDueTimerService()
    timer.bind_agent_runtime(Runtime())  # type: ignore[arg-type]
    timer.bind_task_scope(manager.scope("agent:test:review_due_timer"))

    with pytest.raises(ReviewDueDispatchError):
        await timer.run_once()

    assert manager.failures(prefix="agent:test:review_due_timer") == []


@pytest.mark.asyncio
async def test_active_chat_timer_loop_survives_dispatch_and_state_failures() -> None:
    first_failure = asyncio.Event()
    second_failure = asyncio.Event()
    allow_recovery = asyncio.Event()

    class Scheduler:
        def __init__(self) -> None:
            self.call_count = 0

        def state_for(self, _session_id: str) -> AgentState:
            self.call_count += 1
            if self.call_count == 1:
                second_failure.set()
                raise LookupError("transient state lookup failure")
            return AgentState.ACTIVE_CHAT

    scheduler = Scheduler()

    class Profile:
        agent_scheduler = scheduler

    class Runtime:
        def __init__(self) -> None:
            self.call_count = 0

        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

        async def handle_agent_signal(self, _signal: AgentSignal) -> None:
            self.call_count += 1
            if self.call_count == 1:
                first_failure.set()
                raise ValueError("transient tick dispatch failure")
            if self.call_count >= 3:
                await allow_recovery.wait()

    runtime = Runtime()
    timer = ActiveChatTimerService(tick_interval_seconds=0.01)
    timer.bind_agent_runtime(runtime)  # type: ignore[arg-type]
    session_id = "bot:group:room"
    timer.start(session_id)
    try:
        await asyncio.wait_for(first_failure.wait(), timeout=1.0)
        await asyncio.wait_for(second_failure.wait(), timeout=1.0)
        await _wait_until(
            lambda: (
                (snapshot := timer.health_snapshot(session_id)) is not None
                and snapshot.status == RuntimeServiceStatus.DEGRADED
                and snapshot.consecutive_failures == 2
            )
        )
        assert session_id in timer.active_sessions()

        allow_recovery.set()
        await _wait_until(
            lambda: (
                (snapshot := timer.health_snapshot(session_id)) is not None
                and snapshot.status == RuntimeServiceStatus.RUNNING
            )
        )
        recovered = timer.health_snapshot(session_id)
        assert recovered is not None
        assert recovered.consecutive_failures == 0
        assert recovered.scan_count >= 3
        assert recovered.success_count >= 1
        assert recovered.last_error_code == "LookupError"
    finally:
        await timer.shutdown()

    stopped = timer.health_snapshot(session_id)
    assert stopped is not None
    assert stopped.status == RuntimeServiceStatus.STOPPED


@pytest.mark.asyncio
async def test_active_chat_timer_quiescence_waits_for_the_session_task() -> None:
    """The local timer drain reports completion after its tick task exits."""

    session_id = "bot:group:room"
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class Scheduler:
        def state_for(self, _session_id: str) -> AgentState:
            return AgentState.ACTIVE_CHAT

    class Profile:
        agent_scheduler = Scheduler()

    class Runtime:
        def agent_profile_for_bot(self, _bot_id: str) -> Profile:
            return Profile()

        async def handle_agent_signal(self, _signal: AgentSignal) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    timer = ActiveChatTimerService(tick_interval_seconds=0.01)
    timer.bind_agent_runtime(Runtime())  # type: ignore[arg-type]
    timer.start(session_id)
    try:
        await asyncio.wait_for(started.wait(), timeout=1.0)

        report = await timer.quiesce_session_tasks(
            session_id,
            timeout_seconds=0.5,
        )

        assert cancelled.is_set()
        assert report.status is AgentTaskQuiescenceStatus.QUIESCENT
        assert report.locally_confirmed_quiescent is True
        assert timer.pending_session_tasks(session_id) == []
    finally:
        await timer.shutdown()
