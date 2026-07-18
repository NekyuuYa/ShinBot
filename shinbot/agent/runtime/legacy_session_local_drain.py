"""Unmounted process-local drain participant for one legacy Agent session.

This module composes existing local ingress, waiting-input, signal-admission,
and task-observation primitives. It is deliberately not a durable Actor v2
cutover controller and must not be mounted on production ingress or timers.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.legacy_session_quiescence import (
    LegacySessionAllProfilesTaskQuiescence,
)
from shinbot.agent.runtime.legacy_signal_admission import (
    LegacyAgentSignalAdmissionRegistry,
    LegacyAgentSignalFreezeTicket,
    LegacyAgentSignalQuiescenceReceipt,
)
from shinbot.core.dispatch.legacy_ingress_quiescence import (
    LegacyIngressFreezeTicket,
    LegacyIngressQuiescenceReceipt,
)
from shinbot.core.dispatch.message_context import (
    WaitingInputFreezeTicket,
    WaitingInputLeaseInspection,
    WaitingInputQuiescenceReceipt,
    WaitingInputScope,
)


class LegacySessionLocalDrainStage(StrEnum):
    """Local drain stages whose separate receipts must all be positive."""

    INGRESS = "ingress"
    AGENT_SIGNALS = "agent_signals"
    AGENT_TASKS = "agent_tasks"
    WAITING_INPUT = "waiting_input"


class LegacySessionLocalDrainError(RuntimeError):
    """Base error for an invalid local legacy-session drain operation."""


class LegacySessionLocalDrainConflict(LegacySessionLocalDrainError):
    """Raised when a component belongs to another local cutover identity."""


class LegacyIngressDrainPort(Protocol):
    """Core-owned local ingress and waiter lifecycle surface."""

    def freeze_legacy_ingress_session(
        self,
        session_id: str,
        *,
        cutover_id: str,
    ) -> LegacyIngressFreezeTicket:
        """Freeze legacy ingress admission for one base session."""

    def legacy_ingress_freeze_ticket(
        self,
        session_id: str,
    ) -> LegacyIngressFreezeTicket | None:
        """Return the active local ingress freeze ticket, if any."""

    async def await_legacy_ingress_quiescent(
        self,
        ticket: LegacyIngressFreezeTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacyIngressQuiescenceReceipt:
        """Observe local pre-freeze ingress work."""

    def thaw_legacy_ingress_session(self, ticket: LegacyIngressFreezeTicket) -> bool:
        """Release a quiescent local ingress freeze."""

    def freeze_legacy_waiting_input(
        self,
        scope: WaitingInputScope,
        *,
        cutover_id: str,
    ) -> WaitingInputFreezeTicket:
        """Freeze one local waiting-input scope."""

    def legacy_waiting_input_freeze_ticket(
        self,
        session_id: str,
    ) -> WaitingInputFreezeTicket | None:
        """Return the active local waiting-input freeze ticket, if any."""

    def legacy_waiting_input_lease_inspection(
        self,
        session_id: str,
    ) -> WaitingInputLeaseInspection | None:
        """Return local active waiter-handler ownership facts, if any."""

    async def await_legacy_waiting_input_quiescent(
        self,
        ticket: WaitingInputFreezeTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> WaitingInputQuiescenceReceipt:
        """Observe local frozen waiting-input work."""

    def thaw_legacy_waiting_input(self, ticket: WaitingInputFreezeTicket) -> bool:
        """Release a quiescent local waiting-input freeze."""


class LegacySessionTaskDrainPort(Protocol):
    """Agent-owned all-profile local task observer."""

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionAllProfilesTaskQuiescence:
        """Cancel and observe every configured profile's known task snapshot."""


@dataclass(slots=True, frozen=True)
class LegacySessionLocalDrainRequest:
    """Stable local identities required to freeze one legacy base session."""

    legacy_session_id: str
    waiting_input_scope: WaitingInputScope
    cutover_id: str

    def __post_init__(self) -> None:
        """Reject ambiguous scope and cutover identities before a freeze starts."""

        legacy_session_id = _required_identifier(
            self.legacy_session_id,
            "legacy_session_id",
        )
        if not isinstance(self.waiting_input_scope, WaitingInputScope):
            raise TypeError("waiting_input_scope must be a WaitingInputScope")
        if self.waiting_input_scope.legacy_session_id != legacy_session_id:
            raise ValueError("waiting_input_scope must use the legacy_session_id")
        if self.waiting_input_scope.session_key is None:
            raise ValueError("waiting_input_scope must carry a canonical session_key")
        cutover_id = _required_identifier(self.cutover_id, "cutover_id")
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        object.__setattr__(self, "cutover_id", cutover_id)


@dataclass(slots=True, frozen=True)
class LegacySessionLocalDrainTicket:
    """Opaque local freeze state for one unmounted drain attempt."""

    request: LegacySessionLocalDrainRequest
    ingress_ticket: LegacyIngressFreezeTicket
    waiting_input_ticket: WaitingInputFreezeTicket
    signal_ticket: LegacyAgentSignalFreezeTicket | None = None

    def __post_init__(self) -> None:
        """Require every component ticket to belong to exactly one request."""

        if not isinstance(self.request, LegacySessionLocalDrainRequest):
            raise TypeError("request must be a LegacySessionLocalDrainRequest")
        if not isinstance(self.ingress_ticket, LegacyIngressFreezeTicket):
            raise TypeError("ingress_ticket must be a LegacyIngressFreezeTicket")
        if not isinstance(self.waiting_input_ticket, WaitingInputFreezeTicket):
            raise TypeError("waiting_input_ticket must be a WaitingInputFreezeTicket")
        request = self.request
        if (
            self.ingress_ticket.session_id != request.legacy_session_id
            or self.ingress_ticket.cutover_id != request.cutover_id
        ):
            raise ValueError("ingress_ticket does not match the local drain request")
        if (
            self.waiting_input_ticket.scope != request.waiting_input_scope
            or self.waiting_input_ticket.cutover_id != request.cutover_id
        ):
            raise ValueError("waiting_input_ticket does not match the local drain request")
        if self.signal_ticket is not None and (
            self.signal_ticket.session_id != request.legacy_session_id
            or self.signal_ticket.cutover_id != request.cutover_id
        ):
            raise ValueError("signal_ticket does not match the local drain request")

    def with_signal_ticket(
        self,
        signal_ticket: LegacyAgentSignalFreezeTicket,
    ) -> LegacySessionLocalDrainTicket:
        """Attach the signal-admission freeze reached after ingress drained."""

        return replace(self, signal_ticket=signal_ticket)


@dataclass(slots=True, frozen=True)
class LegacySessionLocalDrainStageFailure:
    """Stable failure metadata without retaining an unbounded exception payload."""

    stage: LegacySessionLocalDrainStage
    error_code: str


@dataclass(slots=True, frozen=True)
class LegacySessionLocalDrainReceipt:
    """Aggregate result from the ordered local drain protocol.

    A positive result means only that all configured components reached a clean
    current-process observation. It is not durable cutover evidence and says
    nothing about adapter queues, other processes, replay workers, plugins, or
    external model and tool effects.
    """

    ticket: LegacySessionLocalDrainTicket
    ingress: LegacyIngressQuiescenceReceipt | None
    waiting_input: WaitingInputQuiescenceReceipt | None
    agent_signals: LegacyAgentSignalQuiescenceReceipt | None
    agent_tasks: LegacySessionAllProfilesTaskQuiescence | None
    failures: tuple[LegacySessionLocalDrainStageFailure, ...] = ()
    skipped_stages: tuple[LegacySessionLocalDrainStage, ...] = ()

    @property
    def locally_confirmed_quiescent(self) -> bool:
        """Return whether every required local component reported cleanly."""

        return (
            not self.failures
            and not self.skipped_stages
            and self.ingress is not None
            and self.ingress.quiescent
            and self.waiting_input is not None
            and self.waiting_input.quiescent
            and self.agent_signals is not None
            and self.agent_signals.quiescent
            and self.agent_tasks is not None
            and self.agent_tasks.locally_confirmed_quiescent
        )

    @property
    def remaining_task_names(self) -> tuple[str, ...]:
        """Return all named surviving local tasks from component receipts."""

        names: list[str] = []
        if self.ingress is not None:
            names.extend(f"ingress:{name}" for name in self.ingress.remaining_task_names)
        if self.agent_signals is not None:
            names.extend(
                f"agent_signals:{name}"
                for name in self.agent_signals.remaining_task_names
            )
        if self.agent_tasks is not None:
            names.extend(
                f"agent_tasks:{name}"
                for name in self.agent_tasks.remaining_task_names
            )
        return tuple(sorted(names))


class LegacySessionLocalDrainParticipant:
    """Compose one ordered, process-local legacy base-session drain.

    The participant starts by freezing ingress and waiting input. It permits
    pre-freeze ingress to finish normally, then freezes Agent signal admission,
    and only then snapshots/cancels all profile task owners. This ordering
    prevents a route target admitted before ingress freeze from being rejected
    merely because it had not entered ``AgentRuntime`` yet.
    """

    def __init__(
        self,
        *,
        ingress: LegacyIngressDrainPort,
        signal_admission: LegacyAgentSignalAdmissionRegistry,
        task_quiescer: LegacySessionTaskDrainPort,
    ) -> None:
        self._ingress = ingress
        self._signal_admission = signal_admission
        self._task_quiescer = task_quiescer

    def freeze(
        self,
        request: LegacySessionLocalDrainRequest,
    ) -> LegacySessionLocalDrainTicket:
        """Freeze ingress and waiting input after a no-yield local preflight.

        The method intentionally does not freeze Agent signal admission yet.
        That happens only after pre-freeze ingress has exited, preserving old
        route-target calls that are already part of the ingress drain.
        """

        if not isinstance(request, LegacySessionLocalDrainRequest):
            raise TypeError("request must be a LegacySessionLocalDrainRequest")
        ingress_ticket = self._compatible_ingress_ticket(request)
        waiting_ticket = self._compatible_waiting_input_ticket(request)
        signal_ticket = self._compatible_signal_ticket(request)
        if signal_ticket is not None and (
            ingress_ticket is None or waiting_ticket is None
        ):
            raise LegacySessionLocalDrainConflict(
                "legacy Agent signals are frozen without matching ingress and waiter freezes"
            )
        if ingress_ticket is None:
            ingress_ticket = self._ingress.freeze_legacy_ingress_session(
                request.legacy_session_id,
                cutover_id=request.cutover_id,
            )
        if waiting_ticket is None:
            waiting_ticket = self._ingress.freeze_legacy_waiting_input(
                request.waiting_input_scope,
                cutover_id=request.cutover_id,
            )
        return LegacySessionLocalDrainTicket(
            request=request,
            ingress_ticket=ingress_ticket,
            waiting_input_ticket=waiting_ticket,
            signal_ticket=signal_ticket,
        )

    async def drain(
        self,
        ticket: LegacySessionLocalDrainTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalDrainReceipt:
        """Observe the ordered local drain against one total timeout budget."""

        if not isinstance(ticket, LegacySessionLocalDrainTicket):
            raise TypeError("ticket must be a LegacySessionLocalDrainTicket")
        timeout = _normalize_timeout(timeout_seconds)
        deadline = (
            None
            if timeout is None
            else asyncio.get_running_loop().time() + timeout
        )
        failures: list[LegacySessionLocalDrainStageFailure] = []
        skipped_stages: list[LegacySessionLocalDrainStage] = []
        ingress_receipt: LegacyIngressQuiescenceReceipt | None = None
        waiting_input_receipt: WaitingInputQuiescenceReceipt | None = None
        signal_receipt: LegacyAgentSignalQuiescenceReceipt | None = None
        task_receipt: LegacySessionAllProfilesTaskQuiescence | None = None
        active_ticket = ticket

        try:
            ingress_receipt = await self._ingress.await_legacy_ingress_quiescent(
                ticket.ingress_ticket,
                timeout_seconds=_remaining_timeout(deadline),
            )
        except Exception as exc:
            failures.append(_stage_failure(LegacySessionLocalDrainStage.INGRESS, exc))

        if ingress_receipt is not None and ingress_receipt.quiescent:
            try:
                signal_ticket = (
                    ticket.signal_ticket
                    or self._signal_admission.freeze(
                        ticket.request.legacy_session_id,
                        cutover_id=ticket.request.cutover_id,
                    )
                )
                active_ticket = ticket.with_signal_ticket(signal_ticket)
                signal_receipt = await self._signal_admission.await_quiescent(
                    signal_ticket,
                    timeout_seconds=_remaining_timeout(deadline),
                )
            except Exception as exc:
                failures.append(
                    _stage_failure(LegacySessionLocalDrainStage.AGENT_SIGNALS, exc)
                )
        else:
            skipped_stages.append(LegacySessionLocalDrainStage.AGENT_SIGNALS)

        if signal_receipt is not None and signal_receipt.quiescent:
            try:
                task_receipt = await self._task_quiescer.quiesce_session_tasks(
                    ticket.request.legacy_session_id,
                    timeout_seconds=_remaining_timeout(deadline),
                )
            except Exception as exc:
                failures.append(
                    _stage_failure(LegacySessionLocalDrainStage.AGENT_TASKS, exc)
                )
        else:
            skipped_stages.append(LegacySessionLocalDrainStage.AGENT_TASKS)

        try:
            waiting_input_receipt = (
                await self._ingress.await_legacy_waiting_input_quiescent(
                    ticket.waiting_input_ticket,
                    timeout_seconds=_remaining_timeout(deadline),
                )
            )
        except Exception as exc:
            failures.append(
                _stage_failure(LegacySessionLocalDrainStage.WAITING_INPUT, exc)
            )

        return LegacySessionLocalDrainReceipt(
            ticket=active_ticket,
            ingress=ingress_receipt,
            waiting_input=waiting_input_receipt,
            agent_signals=signal_receipt,
            agent_tasks=task_receipt,
            failures=tuple(failures),
            skipped_stages=tuple(skipped_stages),
        )

    def thaw(self, receipt: LegacySessionLocalDrainReceipt) -> bool:
        """Release a fully drained local session back to legacy handling.

        This resumes local legacy handling only. A future successful Actor v2
        cutover must retain its freezes until ownership and target publication
        are durably committed instead of calling this method.
        """

        if not isinstance(receipt, LegacySessionLocalDrainReceipt):
            raise TypeError("receipt must be a LegacySessionLocalDrainReceipt")
        if not receipt.locally_confirmed_quiescent:
            raise LegacySessionLocalDrainError(
                "cannot thaw a local drain before every component is quiescent"
            )
        signal_ticket = receipt.ticket.signal_ticket
        if signal_ticket is None:
            raise LegacySessionLocalDrainError(
                "cannot thaw a local drain without a signal-admission ticket"
            )
        if (
            self._ingress.legacy_ingress_freeze_ticket(
                receipt.ticket.request.legacy_session_id
            )
            != receipt.ticket.ingress_ticket
            or self._ingress.legacy_waiting_input_freeze_ticket(
                receipt.ticket.request.legacy_session_id
            )
            != receipt.ticket.waiting_input_ticket
            or self._signal_admission.active_freeze_ticket(
                receipt.ticket.request.legacy_session_id
            )
            != signal_ticket
        ):
            raise LegacySessionLocalDrainError(
                "cannot thaw a local drain after a component ticket changed"
            )
        self._ingress.thaw_legacy_waiting_input(receipt.ticket.waiting_input_ticket)
        self._signal_admission.thaw(signal_ticket)
        self._ingress.thaw_legacy_ingress_session(receipt.ticket.ingress_ticket)
        return True

    def _compatible_ingress_ticket(
        self,
        request: LegacySessionLocalDrainRequest,
    ) -> LegacyIngressFreezeTicket | None:
        ticket = self._ingress.legacy_ingress_freeze_ticket(request.legacy_session_id)
        if ticket is not None and ticket.cutover_id != request.cutover_id:
            raise LegacySessionLocalDrainConflict(
                "legacy ingress is already frozen for another cutover"
            )
        return ticket

    def _compatible_waiting_input_ticket(
        self,
        request: LegacySessionLocalDrainRequest,
    ) -> WaitingInputFreezeTicket | None:
        lease_inspection = self._ingress.legacy_waiting_input_lease_inspection(
            request.legacy_session_id
        )
        if lease_inspection is not None:
            if not lease_inspection.managed or lease_inspection.owner_task_done:
                raise LegacySessionLocalDrainConflict(
                    "legacy waiting input cannot prove handler quiescence"
                )
            if not lease_inspection.scope.matches(request.waiting_input_scope):
                raise LegacySessionLocalDrainConflict(
                    "legacy waiting input belongs to another scope"
                )
        ticket = self._ingress.legacy_waiting_input_freeze_ticket(
            request.legacy_session_id
        )
        if ticket is None:
            return None
        if (
            ticket.cutover_id != request.cutover_id
            or ticket.scope != request.waiting_input_scope
        ):
            raise LegacySessionLocalDrainConflict(
                "legacy waiting input is already frozen for another scope or cutover"
            )
        return ticket

    def _compatible_signal_ticket(
        self,
        request: LegacySessionLocalDrainRequest,
    ) -> LegacyAgentSignalFreezeTicket | None:
        ticket = self._signal_admission.active_freeze_ticket(request.legacy_session_id)
        if ticket is not None and ticket.cutover_id != request.cutover_id:
            raise LegacySessionLocalDrainConflict(
                "legacy Agent signals are already frozen for another cutover"
            )
        return ticket


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize one local non-empty identity value."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _normalize_timeout(timeout_seconds: float | None) -> float | None:
    """Validate a total local-drain timeout."""

    if timeout_seconds is None:
        return None
    timeout = float(timeout_seconds)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout_seconds must be finite and non-negative")
    return timeout


def _remaining_timeout(deadline: float | None) -> float | None:
    """Return the remaining total timeout without granting a new budget."""

    if deadline is None:
        return None
    return max(0.0, deadline - asyncio.get_running_loop().time())


def _stage_failure(
    stage: LegacySessionLocalDrainStage,
    exc: Exception,
) -> LegacySessionLocalDrainStageFailure:
    """Record stable local failure metadata without retaining raw exceptions."""

    return LegacySessionLocalDrainStageFailure(
        stage=stage,
        error_code=type(exc).__name__,
    )


__all__ = [
    "LegacyIngressDrainPort",
    "LegacySessionLocalDrainConflict",
    "LegacySessionLocalDrainError",
    "LegacySessionLocalDrainParticipant",
    "LegacySessionLocalDrainReceipt",
    "LegacySessionLocalDrainRequest",
    "LegacySessionLocalDrainStage",
    "LegacySessionLocalDrainStageFailure",
    "LegacySessionLocalDrainTicket",
    "LegacySessionTaskDrainPort",
]
