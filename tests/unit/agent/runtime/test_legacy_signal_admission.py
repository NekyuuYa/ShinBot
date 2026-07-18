"""Unit coverage for local legacy Agent signal admission freeze semantics."""

from __future__ import annotations

import asyncio

import pytest

from shinbot.agent.runtime.legacy_signal_admission import (
    LegacyAgentSignalAdmissionRegistry,
    LegacyAgentSignalFreezeError,
    LegacyAgentSignalFrozen,
    LegacyAgentSignalQuiescenceStatus,
)


@pytest.mark.asyncio
async def test_freeze_rejects_new_signals_and_drains_pre_freeze_call() -> None:
    """Only a task admitted before the local freeze remains eligible to drain."""

    registry = LegacyAgentSignalAdmissionRegistry()
    session_id = "instance-a:private:user-a"
    started = asyncio.Event()
    release = asyncio.Event()

    async def running_signal() -> None:
        async with registry.admit_signal(session_id):
            started.set()
            await release.wait()

    signal_task = asyncio.create_task(
        running_signal(),
        name="legacy-agent-signal:instance-a:private:user-a",
    )
    await started.wait()
    ticket = registry.freeze(session_id, cutover_id="cutover-a")

    with pytest.raises(LegacyAgentSignalFrozen, match="frozen"):
        async with registry.admit_signal(session_id):
            pass

    timed_out = await registry.await_quiescent(ticket, timeout_seconds=0.0)
    assert timed_out.status is LegacyAgentSignalQuiescenceStatus.TIMED_OUT
    assert timed_out.remaining_task_names == (
        "legacy-agent-signal:instance-a:private:user-a",
    )

    release.set()
    await signal_task
    quiescent = await registry.await_quiescent(ticket, timeout_seconds=0.5)

    assert quiescent.status is LegacyAgentSignalQuiescenceStatus.QUIESCENT
    assert registry.thaw(ticket) is True


@pytest.mark.asyncio
async def test_nested_admission_does_not_drop_the_outer_signal_task() -> None:
    """Nested legacy signals remain tracked until the outer call exits."""

    registry = LegacyAgentSignalAdmissionRegistry()
    session_id = "instance-a:group:room"
    nested_finished = asyncio.Event()
    release_outer = asyncio.Event()

    async def nested_signal() -> None:
        async with registry.admit_signal(session_id):
            async with registry.admit_signal(session_id):
                pass
            nested_finished.set()
            await release_outer.wait()

    signal_task = asyncio.create_task(
        nested_signal(),
        name="legacy-agent-signal:instance-a:group:room",
    )
    await nested_finished.wait()
    ticket = registry.freeze(session_id, cutover_id="cutover-a")

    receipt = await registry.await_quiescent(ticket, timeout_seconds=0.0)

    assert receipt.status is LegacyAgentSignalQuiescenceStatus.TIMED_OUT
    release_outer.set()
    await signal_task
    assert (await registry.await_quiescent(ticket, timeout_seconds=0.5)).quiescent
    assert registry.thaw(ticket) is True


@pytest.mark.asyncio
async def test_current_signal_cannot_certify_its_own_freeze() -> None:
    """A self-drain returns a negative receipt instead of deadlocking."""

    registry = LegacyAgentSignalAdmissionRegistry()
    session_id = "instance-a:group:room"

    async with registry.admit_signal(session_id):
        ticket = registry.freeze(session_id, cutover_id="cutover-a")
        receipt = await registry.await_quiescent(ticket, timeout_seconds=0.5)

        assert receipt.status is LegacyAgentSignalQuiescenceStatus.CURRENT_TASK_ACTIVE
        current_task = asyncio.current_task()
        assert current_task is not None
        assert receipt.remaining_task_names == (current_task.get_name(),)

    assert (await registry.await_quiescent(ticket, timeout_seconds=0.5)).quiescent
    assert registry.thaw(ticket) is True


def test_freeze_ticket_is_bound_to_one_cutover_identity() -> None:
    """Concurrent future controllers cannot share a local signal freeze."""

    registry = LegacyAgentSignalAdmissionRegistry()
    ticket = registry.freeze("instance-a:private:user-a", cutover_id="cutover-a")

    assert registry.freeze("instance-a:private:user-a", cutover_id="cutover-a") == ticket
    with pytest.raises(LegacyAgentSignalFreezeError, match="another cutover"):
        registry.freeze("instance-a:private:user-a", cutover_id="cutover-b")
