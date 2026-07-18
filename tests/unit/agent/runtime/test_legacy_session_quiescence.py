from __future__ import annotations

import pytest

from shinbot.agent.runtime.legacy_session_quiescence import (
    LegacySessionAllProfilesTaskQuiescer,
    LegacySessionLocalTaskObservation,
    LegacySessionLocalTaskQuiescence,
    LegacySessionLocalTaskQuiescer,
)
from shinbot.agent.runtime.task_manager import (
    AgentTaskQuiescence,
    AgentTaskQuiescenceStatus,
)


class _Owner:
    """Controlled local-task owner used to exercise the aggregate boundary."""

    def __init__(
        self,
        report: AgentTaskQuiescence | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.report = report or AgentTaskQuiescence(
            AgentTaskQuiescenceStatus.NO_LOCAL_TASKS
        )
        self.error = error
        self.calls: list[tuple[str, float | None]] = []

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AgentTaskQuiescence:
        self.calls.append((session_id, timeout_seconds))
        if self.error is not None:
            raise self.error
        return self.report


class _ProfileQuiescer:
    """Controlled per-profile quiescer used for base-session aggregation."""

    def __init__(
        self,
        report: LegacySessionLocalTaskQuiescence,
        *,
        error: Exception | None = None,
    ) -> None:
        self.report = report
        self.error = error
        self.calls: list[tuple[str, float | None]] = []

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalTaskQuiescence:
        self.calls.append((session_id, timeout_seconds))
        if self.error is not None:
            raise self.error
        return self.report


@pytest.mark.asyncio
async def test_local_quiescer_composes_only_its_configured_process_owners() -> None:
    """A clean report means every known local owner completed its snapshot."""

    timer = _Owner()
    dispatcher = _Owner()
    coordinator = _Owner(
        AgentTaskQuiescence(AgentTaskQuiescenceStatus.QUIESCENT)
    )
    active_chat = _Owner()
    quiescer = LegacySessionLocalTaskQuiescer(
        active_chat_timer=timer,
        review_dispatcher=dispatcher,
        review_coordinator=coordinator,
        active_chat_workflow=active_chat,
    )

    report = await quiescer.quiesce_session_tasks(
        "bot:group:room",
        timeout_seconds=0.5,
    )

    assert report.locally_confirmed_quiescent is True
    assert report.remaining_task_names == ()
    assert report.failed_owner_names == ()
    assert [observation.owner_name for observation in report.observations] == [
        "active_chat_timer",
        "review_dispatcher",
        "review_coordinator",
        "active_chat_workflow",
    ]
    assert all(
        owner.calls and owner.calls[0][0] == "bot:group:room"
        for owner in (timer, dispatcher, coordinator, active_chat)
    )


@pytest.mark.asyncio
async def test_local_quiescer_keeps_timeout_and_owner_failure_visible() -> None:
    """A local aggregate cannot turn incomplete work into a clean observation."""

    timer = _Owner()
    dispatcher = _Owner(
        AgentTaskQuiescence(
            AgentTaskQuiescenceStatus.TIMED_OUT,
            remaining_task_names=("agent-review:bot:group:room",),
        )
    )
    coordinator = _Owner(error=RuntimeError("unexpected coordinator failure"))
    active_chat = _Owner()
    quiescer = LegacySessionLocalTaskQuiescer(
        active_chat_timer=timer,
        review_dispatcher=dispatcher,
        review_coordinator=coordinator,
        active_chat_workflow=active_chat,
    )

    report = await quiescer.quiesce_session_tasks("bot:group:room")

    assert report.locally_confirmed_quiescent is False
    assert report.remaining_task_names == (
        "review_dispatcher:agent-review:bot:group:room",
    )
    assert report.failed_owner_names == ("review_coordinator",)
    assert active_chat.calls != []


@pytest.mark.asyncio
async def test_all_profiles_quiescer_keeps_shared_session_failures_visible() -> None:
    """A clean profile cannot hide a timeout or error from another profile."""

    session_id = "instance-a:group:room"
    clean = _ProfileQuiescer(
        LegacySessionLocalTaskQuiescence(
            session_id=session_id,
            observations=(
                LegacySessionLocalTaskObservation(
                    owner_name="active_chat",
                    task_quiescence=AgentTaskQuiescence(
                        AgentTaskQuiescenceStatus.NO_LOCAL_TASKS
                    ),
                ),
            ),
        )
    )
    timed_out = _ProfileQuiescer(
        LegacySessionLocalTaskQuiescence(
            session_id=session_id,
            observations=(
                LegacySessionLocalTaskObservation(
                    owner_name="review_dispatcher",
                    task_quiescence=AgentTaskQuiescence(
                        AgentTaskQuiescenceStatus.TIMED_OUT,
                        remaining_task_names=("agent-review:instance-a:group:room",),
                    ),
                ),
            ),
        )
    )
    failing = _ProfileQuiescer(
        LegacySessionLocalTaskQuiescence(session_id=session_id, observations=()),
        error=RuntimeError("profile failed"),
    )
    quiescer = LegacySessionAllProfilesTaskQuiescer(
        (
            ("bot-b", timed_out),
            ("__default_agent_profile__", clean),
            ("bot-a", failing),
        )
    )

    report = await quiescer.quiesce_session_tasks(session_id, timeout_seconds=0.5)

    assert report.locally_confirmed_quiescent is False
    assert report.remaining_task_names == (
        "bot-b:review_dispatcher:agent-review:instance-a:group:room",
    )
    assert report.failed_profile_ids == ("bot-a",)
    assert [observation.profile_id for observation in report.observations] == [
        "__default_agent_profile__",
        "bot-a",
        "bot-b",
    ]
    assert all(
        quiescer.calls and quiescer.calls[0][0] == session_id
        for quiescer in (clean, timed_out, failing)
    )
