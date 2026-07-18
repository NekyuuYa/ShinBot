"""Process-local admission freeze and drain for legacy Agent signals.

The registry covers only calls entering ``AgentRuntime.handle_agent_signal`` in
the current asyncio process. It deliberately has no durable-ownership,
ingress, adapter, or external-effect authority.
"""

from __future__ import annotations

import asyncio
import math
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class LegacyAgentSignalQuiescenceStatus(StrEnum):
    """Outcome of observing one frozen local Agent-signal epoch."""

    QUIESCENT = "quiescent"
    TIMED_OUT = "timed_out"
    CURRENT_TASK_ACTIVE = "current_task_active"
    FREEZE_LOST = "freeze_lost"


@dataclass(slots=True, frozen=True)
class LegacyAgentSignalTaskToken:
    """Local admission identity inherited by one legacy signal call."""

    session_id: str
    admission_epoch: int
    token: str


@dataclass(slots=True, frozen=True)
class LegacyAgentSignalFreezeTicket:
    """Opaque local authority that freezes legacy signal admission."""

    session_id: str
    cutover_id: str
    freeze_epoch: int
    token: str


@dataclass(slots=True, frozen=True)
class LegacyAgentSignalQuiescenceReceipt:
    """Result of waiting for pre-freeze signal calls in this process."""

    ticket: LegacyAgentSignalFreezeTicket
    status: LegacyAgentSignalQuiescenceStatus
    remaining_task_names: tuple[str, ...] = ()

    @property
    def quiescent(self) -> bool:
        """Return whether every tracked pre-freeze signal call exited."""

        return self.status is LegacyAgentSignalQuiescenceStatus.QUIESCENT


class LegacyAgentSignalFreezeError(RuntimeError):
    """Raised when a local signal freeze ticket is stale or incomplete."""


class LegacyAgentSignalFrozen(RuntimeError):
    """Raised when a frozen session tries to enter the legacy Agent runtime."""


@dataclass(slots=True)
class _LegacyAgentSignalTaskState:
    """Track one task across nested legacy signal admissions."""

    token: LegacyAgentSignalTaskToken
    depth: int = 1


@dataclass(slots=True)
class _LegacyAgentSignalSessionState:
    """Mutable current-process admission state for one legacy base session."""

    admission_epoch: int = 0
    freeze_ticket: LegacyAgentSignalFreezeTicket | None = None
    tasks: dict[asyncio.Task[Any], _LegacyAgentSignalTaskState] = field(
        default_factory=dict
    )


class LegacyAgentSignalAdmissionRegistry:
    """Freeze and drain calls entering the legacy Agent signal runtime.

    A future controller must establish durable ingress admission and adapter
    pause/drain independently before freezing this registry. The registry
    prevents new local legacy signal handling only; it neither preserves a
    rejected signal nor makes it durable for Actor v2 replay.
    """

    def __init__(self) -> None:
        self._states: dict[str, _LegacyAgentSignalSessionState] = {}

    def is_frozen(self, session_id: str) -> bool:
        """Return whether new local legacy signal handling is frozen."""

        state = self._states.get(_normalize_session_id(session_id))
        return state is not None and state.freeze_ticket is not None

    def active_freeze_ticket(
        self,
        session_id: str,
    ) -> LegacyAgentSignalFreezeTicket | None:
        """Return the current local signal freeze without changing admission."""

        state = self._states.get(_normalize_session_id(session_id))
        return None if state is None else state.freeze_ticket

    @asynccontextmanager
    async def admit_signal(
        self,
        session_id: str,
    ) -> AsyncIterator[LegacyAgentSignalTaskToken]:
        """Track one local legacy signal call or fail closed after a freeze.

        The context must wrap the complete call, including any per-session
        lock wait. Otherwise a task waiting for that lock could be omitted from
        the frozen snapshot and incorrectly permit a positive receipt.
        """

        normalized_session_id = _normalize_session_id(session_id)
        state = self._states.setdefault(
            normalized_session_id,
            _LegacyAgentSignalSessionState(),
        )
        if state.freeze_ticket is not None:
            raise LegacyAgentSignalFrozen(
                f"session {normalized_session_id!r} is frozen for legacy signal admission"
            )

        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("legacy Agent signal admission requires an asyncio task")
        existing = state.tasks.get(task)
        if existing is not None:
            existing.depth += 1
            try:
                yield existing.token
            finally:
                self._finish_task_admission(normalized_session_id, state, task, existing)
            return

        token = LegacyAgentSignalTaskToken(
            session_id=normalized_session_id,
            admission_epoch=state.admission_epoch,
            token=uuid.uuid4().hex,
        )
        task_state = _LegacyAgentSignalTaskState(token=token)
        state.tasks[task] = task_state
        try:
            yield token
        finally:
            self._finish_task_admission(normalized_session_id, state, task, task_state)

    def freeze(
        self,
        session_id: str,
        *,
        cutover_id: str,
    ) -> LegacyAgentSignalFreezeTicket:
        """Reject new local legacy signals for one base session."""

        normalized_session_id = _normalize_session_id(session_id)
        normalized_cutover_id = _normalize_cutover_id(cutover_id)
        state = self._states.setdefault(
            normalized_session_id,
            _LegacyAgentSignalSessionState(),
        )
        existing_ticket = state.freeze_ticket
        if existing_ticket is not None:
            if existing_ticket.cutover_id == normalized_cutover_id:
                return existing_ticket
            raise LegacyAgentSignalFreezeError(
                f"session {normalized_session_id!r} is already frozen for another cutover"
            )
        state.admission_epoch += 1
        ticket = LegacyAgentSignalFreezeTicket(
            session_id=normalized_session_id,
            cutover_id=normalized_cutover_id,
            freeze_epoch=state.admission_epoch,
            token=uuid.uuid4().hex,
        )
        state.freeze_ticket = ticket
        return ticket

    async def await_quiescent(
        self,
        ticket: LegacyAgentSignalFreezeTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacyAgentSignalQuiescenceReceipt:
        """Wait until all direct pre-freeze legacy signal calls have exited."""

        state = self._require_ticket(ticket)
        timeout = _normalize_timeout(timeout_seconds)
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            if not self._ticket_is_current(ticket):
                return _freeze_lost_receipt(ticket)
            tasks = self._pre_freeze_tasks(state, ticket)
            current_task = asyncio.current_task()
            if current_task is not None and current_task in tasks:
                return LegacyAgentSignalQuiescenceReceipt(
                    ticket=ticket,
                    status=LegacyAgentSignalQuiescenceStatus.CURRENT_TASK_ACTIVE,
                    remaining_task_names=_task_names(tasks),
                )
            if not tasks:
                return LegacyAgentSignalQuiescenceReceipt(
                    ticket=ticket,
                    status=LegacyAgentSignalQuiescenceStatus.QUIESCENT,
                )
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                return LegacyAgentSignalQuiescenceReceipt(
                    ticket=ticket,
                    status=LegacyAgentSignalQuiescenceStatus.TIMED_OUT,
                    remaining_task_names=_task_names(tasks),
                )
            _done, pending = await asyncio.wait(tasks, timeout=remaining)
            if pending:
                if not self._ticket_is_current(ticket):
                    return _freeze_lost_receipt(ticket)
                return LegacyAgentSignalQuiescenceReceipt(
                    ticket=ticket,
                    status=LegacyAgentSignalQuiescenceStatus.TIMED_OUT,
                    remaining_task_names=_task_names(pending),
                )

    def thaw(self, ticket: LegacyAgentSignalFreezeTicket) -> bool:
        """Release a freeze after every tracked pre-freeze call exits."""

        state = self._require_ticket(ticket)
        if self._pre_freeze_tasks(state, ticket):
            raise LegacyAgentSignalFreezeError(
                "cannot thaw legacy signal admission before tracked calls exit"
            )
        state.freeze_ticket = None
        self._discard_idle_state(ticket.session_id, state)
        return True

    def _finish_task_admission(
        self,
        session_id: str,
        state: _LegacyAgentSignalSessionState,
        task: asyncio.Task[Any],
        task_state: _LegacyAgentSignalTaskState,
    ) -> None:
        current = state.tasks.get(task)
        if current is not task_state:
            return
        task_state.depth -= 1
        if task_state.depth <= 0:
            state.tasks.pop(task, None)
        self._discard_idle_state(session_id, state)

    def _require_ticket(
        self,
        ticket: LegacyAgentSignalFreezeTicket,
    ) -> _LegacyAgentSignalSessionState:
        if not isinstance(ticket, LegacyAgentSignalFreezeTicket):
            raise TypeError("ticket must be a LegacyAgentSignalFreezeTicket")
        state = self._states.get(ticket.session_id)
        if state is None or state.freeze_ticket != ticket:
            raise LegacyAgentSignalFreezeError("freeze ticket is no longer active")
        return state

    def _ticket_is_current(self, ticket: LegacyAgentSignalFreezeTicket) -> bool:
        state = self._states.get(ticket.session_id)
        return state is not None and state.freeze_ticket == ticket

    @staticmethod
    def _pre_freeze_tasks(
        state: _LegacyAgentSignalSessionState,
        ticket: LegacyAgentSignalFreezeTicket,
    ) -> tuple[asyncio.Task[Any], ...]:
        return tuple(
            task
            for task, task_state in state.tasks.items()
            if not task.done() and task_state.token.admission_epoch < ticket.freeze_epoch
        )

    def _discard_idle_state(
        self,
        session_id: str,
        state: _LegacyAgentSignalSessionState,
    ) -> None:
        if state.freeze_ticket is None and not state.tasks:
            self._states.pop(session_id, None)


def _normalize_session_id(session_id: str) -> str:
    """Validate one legacy base session identifier."""

    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        raise ValueError("session_id must not be empty")
    return normalized_session_id


def _normalize_cutover_id(cutover_id: str) -> str:
    """Validate one future-controller operation identity."""

    normalized_cutover_id = str(cutover_id or "").strip()
    if not normalized_cutover_id:
        raise ValueError("cutover_id must not be empty")
    return normalized_cutover_id


def _normalize_timeout(timeout_seconds: float | None) -> float | None:
    """Validate an optional local-drain timeout."""

    if timeout_seconds is None:
        return None
    timeout = float(timeout_seconds)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout_seconds must be finite and non-negative")
    return timeout


def _task_names(tasks: tuple[asyncio.Task[Any], ...] | set[asyncio.Task[Any]]) -> tuple[str, ...]:
    """Return stable task labels for a local quiescence receipt."""

    return tuple(sorted(task.get_name() for task in tasks))


def _freeze_lost_receipt(
    ticket: LegacyAgentSignalFreezeTicket,
) -> LegacyAgentSignalQuiescenceReceipt:
    """Build a negative receipt after an unexpected ticket replacement."""

    return LegacyAgentSignalQuiescenceReceipt(
        ticket=ticket,
        status=LegacyAgentSignalQuiescenceStatus.FREEZE_LOST,
    )


__all__ = [
    "LegacyAgentSignalAdmissionRegistry",
    "LegacyAgentSignalFreezeError",
    "LegacyAgentSignalFreezeTicket",
    "LegacyAgentSignalFrozen",
    "LegacyAgentSignalQuiescenceReceipt",
    "LegacyAgentSignalQuiescenceStatus",
    "LegacyAgentSignalTaskToken",
]
