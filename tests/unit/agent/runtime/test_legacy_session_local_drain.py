"""Unit coverage for the unmounted ordered legacy-session local drain."""

from __future__ import annotations

import asyncio

import pytest

from shinbot.agent.runtime.legacy_session_local_drain import (
    LegacySessionLocalDrainConflict,
    LegacySessionLocalDrainError,
    LegacySessionLocalDrainParticipant,
    LegacySessionLocalDrainRequest,
    LegacySessionLocalDrainStage,
)
from shinbot.agent.runtime.legacy_session_quiescence import (
    LegacySessionAllProfilesTaskQuiescence,
    LegacySessionLocalTaskObservation,
    LegacySessionLocalTaskQuiescence,
    LegacySessionProfileTaskObservation,
)
from shinbot.agent.runtime.legacy_signal_admission import (
    LegacyAgentSignalAdmissionRegistry,
)
from shinbot.agent.runtime.task_manager import (
    AgentTaskQuiescence,
    AgentTaskQuiescenceStatus,
)
from shinbot.core.dispatch.legacy_ingress_quiescence import (
    LegacyIngressFreezeTicket,
    LegacyIngressQuiescenceReceipt,
    LegacyIngressSessionRegistry,
)
from shinbot.core.dispatch.message_context import (
    WaitingInputFreezeTicket,
    WaitingInputLeaseInspection,
    WaitingInputQuiescenceReceipt,
    WaitingInputRegistry,
    WaitingInputScope,
)


class _IngressPort:
    """Adapt the real local ingress and waiter registries to the participant port."""

    def __init__(self) -> None:
        self.ingress = LegacyIngressSessionRegistry()
        self.waiting_input = WaitingInputRegistry()

    def freeze_legacy_ingress_session(
        self,
        session_id: str,
        *,
        cutover_id: str,
    ) -> LegacyIngressFreezeTicket:
        return self.ingress.freeze(session_id, cutover_id=cutover_id)

    def legacy_ingress_freeze_ticket(
        self,
        session_id: str,
    ) -> LegacyIngressFreezeTicket | None:
        return self.ingress.active_freeze_ticket(session_id)

    async def await_legacy_ingress_quiescent(
        self,
        ticket: LegacyIngressFreezeTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacyIngressQuiescenceReceipt:
        return await self.ingress.await_quiescent(
            ticket,
            timeout_seconds=timeout_seconds,
        )

    def thaw_legacy_ingress_session(self, ticket: LegacyIngressFreezeTicket) -> bool:
        return self.ingress.thaw(ticket)

    def freeze_legacy_waiting_input(
        self,
        scope: WaitingInputScope,
        *,
        cutover_id: str,
    ) -> WaitingInputFreezeTicket:
        return self.waiting_input.freeze(scope, cutover_id=cutover_id)

    def legacy_waiting_input_freeze_ticket(
        self,
        session_id: str,
    ) -> WaitingInputFreezeTicket | None:
        return self.waiting_input.active_freeze_ticket(session_id)

    def legacy_waiting_input_lease_inspection(
        self,
        session_id: str,
    ) -> WaitingInputLeaseInspection | None:
        return self.waiting_input.active_lease_inspection(session_id)

    async def await_legacy_waiting_input_quiescent(
        self,
        ticket: WaitingInputFreezeTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> WaitingInputQuiescenceReceipt:
        return await self.waiting_input.await_quiescent(
            ticket,
            timeout=timeout_seconds,
        )

    def thaw_legacy_waiting_input(self, ticket: WaitingInputFreezeTicket) -> bool:
        return self.waiting_input.thaw(ticket)


class _TaskQuiescer:
    """Record task drain calls and return a fixed clean profile report."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, float | None]] = []

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionAllProfilesTaskQuiescence:
        self.calls.append((session_id, timeout_seconds))
        return LegacySessionAllProfilesTaskQuiescence(
            session_id=session_id,
            observations=(
                LegacySessionProfileTaskObservation(
                    profile_id="__default_agent_profile__",
                    task_quiescence=LegacySessionLocalTaskQuiescence(
                        session_id=session_id,
                        observations=(
                            LegacySessionLocalTaskObservation(
                                owner_name="review_dispatcher",
                                task_quiescence=AgentTaskQuiescence(
                                    AgentTaskQuiescenceStatus.NO_LOCAL_TASKS
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )


def _request(session_id: str, *, cutover_id: str = "cutover-a") -> LegacySessionLocalDrainRequest:
    """Build a canonical scoped local drain request."""

    return LegacySessionLocalDrainRequest(
        legacy_session_id=session_id,
        waiting_input_scope=WaitingInputScope.from_routing_identity(
            legacy_session_id=session_id,
            bot_id="bot-a",
            bot_session_id=f"bot-a:{session_id}",
        ),
        cutover_id=cutover_id,
    )


@pytest.mark.asyncio
async def test_drain_waits_for_ingress_before_freezing_agent_signals() -> None:
    """A pre-freeze route tail may still enter Agent runtime before its drain."""

    port = _IngressPort()
    signals = LegacyAgentSignalAdmissionRegistry()
    task_quiescer = _TaskQuiescer()
    participant = LegacySessionLocalDrainParticipant(
        ingress=port,
        signal_admission=signals,
        task_quiescer=task_quiescer,
    )
    session_id = "instance-a:group:room"
    entered_ingress = asyncio.Event()
    release_ingress = asyncio.Event()

    async def pre_freeze_ingress() -> None:
        async with port.ingress.admit_message(session_id):
            entered_ingress.set()
            await release_ingress.wait()

    ingress_task = asyncio.create_task(
        pre_freeze_ingress(),
        name="legacy-ingress:instance-a:group:room",
    )
    await entered_ingress.wait()
    ticket = participant.freeze(_request(session_id))
    drain_task = asyncio.create_task(participant.drain(ticket, timeout_seconds=0.5))
    await asyncio.sleep(0)

    assert signals.active_freeze_ticket(session_id) is None
    assert task_quiescer.calls == []

    release_ingress.set()
    await ingress_task
    receipt = await drain_task

    assert receipt.locally_confirmed_quiescent is True
    assert receipt.ticket.signal_ticket is not None
    assert task_quiescer.calls[0][0] == session_id
    assert participant.thaw(receipt) is True


@pytest.mark.asyncio
async def test_failed_ingress_drain_does_not_snapshot_agent_tasks() -> None:
    """A timeout retains a negative receipt instead of observing an unstable task set."""

    port = _IngressPort()
    signals = LegacyAgentSignalAdmissionRegistry()
    task_quiescer = _TaskQuiescer()
    participant = LegacySessionLocalDrainParticipant(
        ingress=port,
        signal_admission=signals,
        task_quiescer=task_quiescer,
    )
    session_id = "instance-a:group:room"
    entered_ingress = asyncio.Event()
    release_ingress = asyncio.Event()

    async def pre_freeze_ingress() -> None:
        async with port.ingress.admit_message(session_id):
            entered_ingress.set()
            await release_ingress.wait()

    ingress_task = asyncio.create_task(pre_freeze_ingress())
    await entered_ingress.wait()
    ticket = participant.freeze(_request(session_id))
    failed = await participant.drain(ticket, timeout_seconds=0.0)

    assert failed.locally_confirmed_quiescent is False
    assert failed.skipped_stages == (
        LegacySessionLocalDrainStage.AGENT_SIGNALS,
        LegacySessionLocalDrainStage.AGENT_TASKS,
    )
    assert signals.active_freeze_ticket(session_id) is None
    assert task_quiescer.calls == []
    with pytest.raises(LegacySessionLocalDrainError, match="cannot thaw"):
        participant.thaw(failed)

    release_ingress.set()
    await ingress_task
    completed = await participant.drain(ticket, timeout_seconds=0.5)

    assert completed.locally_confirmed_quiescent is True
    assert participant.thaw(completed) is True


def test_preflight_conflict_does_not_leave_other_components_frozen() -> None:
    """A conflicting signal ticket is found before ingress or waiter mutation."""

    port = _IngressPort()
    signals = LegacyAgentSignalAdmissionRegistry()
    task_quiescer = _TaskQuiescer()
    participant = LegacySessionLocalDrainParticipant(
        ingress=port,
        signal_admission=signals,
        task_quiescer=task_quiescer,
    )
    session_id = "instance-a:private:user-a"
    foreign_ticket = signals.freeze(session_id, cutover_id="cutover-b")

    with pytest.raises(LegacySessionLocalDrainConflict, match="another cutover"):
        participant.freeze(_request(session_id, cutover_id="cutover-a"))

    assert port.ingress.active_freeze_ticket(session_id) is None
    assert port.waiting_input.active_freeze_ticket(session_id) is None
    assert signals.thaw(foreign_ticket) is True


def test_preflight_rejects_signal_gate_that_closed_too_early() -> None:
    """Signal admission cannot be externally frozen before ingress is fenced."""

    port = _IngressPort()
    signals = LegacyAgentSignalAdmissionRegistry()
    participant = LegacySessionLocalDrainParticipant(
        ingress=port,
        signal_admission=signals,
        task_quiescer=_TaskQuiescer(),
    )
    session_id = "instance-a:private:user-a"
    early_ticket = signals.freeze(session_id, cutover_id="cutover-a")

    with pytest.raises(LegacySessionLocalDrainConflict, match="without matching"):
        participant.freeze(_request(session_id))

    assert port.ingress.active_freeze_ticket(session_id) is None
    assert port.waiting_input.active_freeze_ticket(session_id) is None
    assert signals.thaw(early_ticket) is True


@pytest.mark.asyncio
async def test_preflight_rejects_another_scope_before_freezing_ingress() -> None:
    """A live cross-binding waiter cannot create a partial local freeze."""

    port = _IngressPort()
    signals = LegacyAgentSignalAdmissionRegistry()
    participant = LegacySessionLocalDrainParticipant(
        ingress=port,
        signal_admission=signals,
        task_quiescer=_TaskQuiescer(),
    )
    session_id = "instance-a:private:user-a"
    other_scope = WaitingInputScope.from_routing_identity(
        legacy_session_id=session_id,
        bot_id="bot-b",
        bot_session_id=f"bot-b:{session_id}",
    )
    current_task = asyncio.current_task()
    assert current_task is not None
    lease = port.waiting_input.acquire(other_scope, owner_task=current_task)

    with pytest.raises(LegacySessionLocalDrainConflict, match="another scope"):
        participant.freeze(_request(session_id))

    assert port.ingress.active_freeze_ticket(session_id) is None
    assert port.waiting_input.active_freeze_ticket(session_id) is None
    assert port.waiting_input.release(lease) is True


@pytest.mark.asyncio
async def test_preflight_rejects_an_unmanaged_waiter_before_freezing_ingress() -> None:
    """A compatibility Future cannot become a positive local drain receipt."""

    port = _IngressPort()
    signals = LegacyAgentSignalAdmissionRegistry()
    participant = LegacySessionLocalDrainParticipant(
        ingress=port,
        signal_admission=signals,
        task_quiescer=_TaskQuiescer(),
    )
    session_id = "instance-a:private:user-a"
    request = _request(session_id)
    lease = port.waiting_input.acquire(
        request.waiting_input_scope,
        track_owner=False,
    )

    with pytest.raises(LegacySessionLocalDrainConflict, match="cannot prove"):
        participant.freeze(request)

    assert port.ingress.active_freeze_ticket(session_id) is None
    assert port.waiting_input.active_freeze_ticket(session_id) is None
    assert port.waiting_input.release(lease) is True


@pytest.mark.asyncio
async def test_thaw_revalidates_every_component_ticket_before_mutating() -> None:
    """A stale signal ticket cannot partially reopen waiter or ingress admission."""

    port = _IngressPort()
    signals = LegacyAgentSignalAdmissionRegistry()
    participant = LegacySessionLocalDrainParticipant(
        ingress=port,
        signal_admission=signals,
        task_quiescer=_TaskQuiescer(),
    )
    session_id = "instance-a:private:user-a"
    ticket = participant.freeze(_request(session_id))
    receipt = await participant.drain(ticket, timeout_seconds=0.5)
    assert receipt.locally_confirmed_quiescent
    assert receipt.ticket.signal_ticket is not None

    assert signals.thaw(receipt.ticket.signal_ticket) is True
    replacement = signals.freeze(session_id, cutover_id="cutover-b")

    with pytest.raises(LegacySessionLocalDrainError, match="ticket changed"):
        participant.thaw(receipt)

    assert port.ingress.active_freeze_ticket(session_id) == ticket.ingress_ticket
    assert (
        port.waiting_input.active_freeze_ticket(session_id)
        == ticket.waiting_input_ticket
    )
    assert signals.thaw(replacement) is True
    assert port.waiting_input.thaw(ticket.waiting_input_ticket) is True
    assert port.ingress.thaw(ticket.ingress_ticket) is True
