from __future__ import annotations

import asyncio

import pytest

from shinbot.core.dispatch.legacy_ingress_quiescence import (
    LegacyIngressFreezeError,
    LegacyIngressQuiescenceStatus,
    LegacyIngressSessionRegistry,
)


@pytest.mark.asyncio
async def test_freeze_waits_for_a_direct_route_task_spawned_by_pre_freeze_ingress() -> None:
    """A child scheduled after freeze inherits its parent's legacy epoch."""

    registry = LegacyIngressSessionRegistry()
    session_id = "bot:group:room"
    parent_started = asyncio.Event()
    allow_route_schedule = asyncio.Event()
    route_started = asyncio.Event()
    release_route = asyncio.Event()

    async def route_task() -> None:
        route_started.set()
        await release_route.wait()

    async def ingress_task() -> None:
        async with registry.admit_message(session_id) as admission:
            parent_started.set()
            await allow_route_schedule.wait()
            task = asyncio.create_task(
                route_task(),
                name="legacy-route-target:bot:group:room",
            )
            assert registry.track_route_task(admission.task_token, task) is True

    task = asyncio.create_task(ingress_task(), name="legacy-ingress:bot:group:room")
    await parent_started.wait()

    ticket = registry.freeze(session_id, cutover_id="cutover-a")
    allow_route_schedule.set()
    await asyncio.wait_for(route_started.wait(), timeout=0.5)
    await asyncio.wait_for(task, timeout=0.5)

    timed_out = await registry.await_quiescent(ticket, timeout_seconds=0.0)

    assert timed_out.status is LegacyIngressQuiescenceStatus.TIMED_OUT
    assert timed_out.remaining_task_names == ("legacy-route-target:bot:group:room",)

    release_route.set()
    quiescent = await registry.await_quiescent(ticket, timeout_seconds=0.5)

    assert quiescent.status is LegacyIngressQuiescenceStatus.QUIESCENT
    assert registry.thaw(ticket) is True


@pytest.mark.asyncio
async def test_freeze_requires_new_messages_to_use_durable_admission() -> None:
    """Post-freeze ingress cannot be counted as newly admitted legacy work."""

    registry = LegacyIngressSessionRegistry()
    ticket = registry.freeze("bot:group:room", cutover_id="cutover-a")

    async with registry.admit_message("bot:group:room") as admission:
        assert admission.requires_durable_admission is True
        assert admission.task_token is None

    receipt = await registry.await_quiescent(ticket, timeout_seconds=0.0)

    assert receipt.status is LegacyIngressQuiescenceStatus.QUIESCENT
    assert registry.thaw(ticket) is True


def test_freeze_ticket_is_bound_to_one_cutover_identity() -> None:
    """Another local controller cannot reuse an existing base-session freeze."""

    registry = LegacyIngressSessionRegistry()
    ticket = registry.freeze("bot:group:room", cutover_id="cutover-a")

    assert registry.freeze("bot:group:room", cutover_id="cutover-a") == ticket
    with pytest.raises(LegacyIngressFreezeError, match="another cutover"):
        registry.freeze("bot:group:room", cutover_id="cutover-b")


@pytest.mark.asyncio
async def test_freeze_rejects_a_self_drain_attempt() -> None:
    """A running ingress task cannot certify that it already stopped."""

    registry = LegacyIngressSessionRegistry()
    session_id = "bot:group:room"

    async with registry.admit_message(session_id):
        ticket = registry.freeze(session_id, cutover_id="cutover-a")
        receipt = await registry.await_quiescent(ticket, timeout_seconds=0.5)
        current_task = asyncio.current_task()

        assert receipt.status is LegacyIngressQuiescenceStatus.CURRENT_TASK_ACTIVE
        assert current_task is not None
        assert receipt.remaining_task_names == (current_task.get_name(),)
