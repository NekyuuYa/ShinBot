"""Process-local legacy message-ingress freeze and drain primitives.

The registry tracks only the ingress coroutine and direct route-target tasks it
was allowed to create in this process. It deliberately does not pause an
adapter, coordinate other processes, or represent a durable ownership receipt.
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


class LegacyIngressQuiescenceStatus(StrEnum):
    """Outcome of waiting for one frozen local ingress epoch."""

    QUIESCENT = "quiescent"
    TIMED_OUT = "timed_out"
    CURRENT_TASK_ACTIVE = "current_task_active"
    FREEZE_LOST = "freeze_lost"


@dataclass(slots=True, frozen=True)
class LegacyIngressTaskToken:
    """Local inheritance token for work admitted before one ingress freeze."""

    session_id: str
    admission_epoch: int
    token: str


@dataclass(slots=True, frozen=True)
class LegacyIngressSessionAdmission:
    """One message's local ingress admission classification."""

    session_id: str
    requires_durable_admission: bool
    task_token: LegacyIngressTaskToken | None = None


@dataclass(slots=True, frozen=True)
class LegacyIngressFreezeTicket:
    """Opaque process-local freeze authority for one base session."""

    session_id: str
    cutover_id: str
    freeze_epoch: int
    token: str


@dataclass(slots=True, frozen=True)
class LegacyIngressQuiescenceReceipt:
    """Result of waiting for one frozen local ingress epoch to drain."""

    ticket: LegacyIngressFreezeTicket
    status: LegacyIngressQuiescenceStatus
    remaining_task_names: tuple[str, ...] = ()

    @property
    def quiescent(self) -> bool:
        """Return whether the tracked pre-freeze local work has exited."""

        return self.status is LegacyIngressQuiescenceStatus.QUIESCENT


class LegacyIngressFreezeError(RuntimeError):
    """Raised when a caller uses a stale or incomplete local freeze ticket."""


class LegacyIngressDurableAdmissionRequired(RuntimeError):
    """Reject post-freeze ingress that would otherwise enter legacy routing."""


@dataclass(slots=True)
class _LegacyIngressSessionState:
    """Mutable local task ownership and freeze state for one base session."""

    admission_epoch: int = 0
    freeze_ticket: LegacyIngressFreezeTicket | None = None
    tasks: dict[asyncio.Future[Any], LegacyIngressTaskToken] = field(
        default_factory=dict
    )


class LegacyIngressSessionRegistry:
    """Freeze and drain direct legacy ingress work in one asyncio process.

    A freeze rejects new *legacy* admission for a base session. Callers must
    therefore have established a durable admission fence first: new messages
    may continue only when the enclosing ingress path can persist them behind
    that fence. The registry intentionally makes no claim about adapter-level
    pause/drain, plugin-spawned child work, external effects, or other runtime
    processes.
    """

    def __init__(self) -> None:
        self._states: dict[str, _LegacyIngressSessionState] = {}

    def is_frozen(self, session_id: str) -> bool:
        """Return whether local legacy admission is frozen for a base session."""

        state = self._states.get(_normalize_session_id(session_id))
        return state is not None and state.freeze_ticket is not None

    def active_freeze_ticket(
        self,
        session_id: str,
    ) -> LegacyIngressFreezeTicket | None:
        """Return the current local freeze ticket without changing admission."""

        state = self._states.get(_normalize_session_id(session_id))
        return None if state is None else state.freeze_ticket

    @asynccontextmanager
    async def admit_message(
        self,
        session_id: str,
    ) -> AsyncIterator[LegacyIngressSessionAdmission]:
        """Classify and track one message ingress coroutine locally.

        Work admitted before a freeze receives a token that direct route-target
        tasks must inherit. Work admitted after a freeze is not tracked as
        legacy work and the caller must route it through durable admission.
        """

        normalized_session_id = _normalize_session_id(session_id)
        state = self._states.setdefault(
            normalized_session_id,
            _LegacyIngressSessionState(),
        )
        if state.freeze_ticket is not None:
            yield LegacyIngressSessionAdmission(
                session_id=normalized_session_id,
                requires_durable_admission=True,
            )
            return

        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("legacy ingress admission requires an asyncio task")
        task_token = LegacyIngressTaskToken(
            session_id=normalized_session_id,
            admission_epoch=state.admission_epoch,
            token=uuid.uuid4().hex,
        )
        state.tasks[task] = task_token
        try:
            yield LegacyIngressSessionAdmission(
                session_id=normalized_session_id,
                requires_durable_admission=False,
                task_token=task_token,
            )
        finally:
            if state.tasks.get(task) == task_token:
                state.tasks.pop(task, None)
            self._discard_idle_state(normalized_session_id, state)

    def track_route_task(
        self,
        task_token: LegacyIngressTaskToken | None,
        task: asyncio.Future[Any],
    ) -> bool:
        """Associate one direct route-target task with an admitted ingress task.

        Returns:
            ``True`` when the task inherited a live local legacy token.
        """

        if task_token is None or task.done():
            return False
        if not isinstance(task_token, LegacyIngressTaskToken):
            raise TypeError("task_token must be a LegacyIngressTaskToken or None")
        state = self._states.get(task_token.session_id)
        if state is None or task_token not in state.tasks.values():
            return False
        existing = state.tasks.get(task)
        if existing is not None:
            return existing == task_token
        state.tasks[task] = task_token
        task.add_done_callback(
            lambda completed, session_id=task_token.session_id, token=task_token: (
                self._finish_route_task(session_id, token, completed)
            )
        )
        return True

    def freeze(
        self,
        session_id: str,
        *,
        cutover_id: str,
    ) -> LegacyIngressFreezeTicket:
        """Freeze new local legacy ingress admission for one base session."""

        normalized_session_id = _normalize_session_id(session_id)
        normalized_cutover_id = _normalize_cutover_id(cutover_id)
        state = self._states.setdefault(
            normalized_session_id,
            _LegacyIngressSessionState(),
        )
        existing_ticket = state.freeze_ticket
        if existing_ticket is not None:
            if existing_ticket.cutover_id == normalized_cutover_id:
                return existing_ticket
            raise LegacyIngressFreezeError(
                f"session {normalized_session_id!r} is already frozen for another cutover"
            )
        state.admission_epoch += 1
        ticket = LegacyIngressFreezeTicket(
            session_id=normalized_session_id,
            cutover_id=normalized_cutover_id,
            freeze_epoch=state.admission_epoch,
            token=uuid.uuid4().hex,
        )
        state.freeze_ticket = ticket
        return ticket

    async def await_quiescent(
        self,
        ticket: LegacyIngressFreezeTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacyIngressQuiescenceReceipt:
        """Wait until all direct pre-freeze local ingress work has exited.

        The task set is re-read after each wait because an already admitted
        ingress coroutine can schedule a tagged direct route target before it
        returns. A ticket replacement or thaw during the wait produces a
        negative receipt instead of silently accepting a new epoch.
        """

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
                return LegacyIngressQuiescenceReceipt(
                    ticket=ticket,
                    status=LegacyIngressQuiescenceStatus.CURRENT_TASK_ACTIVE,
                    remaining_task_names=_task_names(tasks),
                )
            if not tasks:
                return LegacyIngressQuiescenceReceipt(
                    ticket=ticket,
                    status=LegacyIngressQuiescenceStatus.QUIESCENT,
                )
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                return LegacyIngressQuiescenceReceipt(
                    ticket=ticket,
                    status=LegacyIngressQuiescenceStatus.TIMED_OUT,
                    remaining_task_names=_task_names(tasks),
                )
            _done, pending = await asyncio.wait(tasks, timeout=remaining)
            if pending:
                if not self._ticket_is_current(ticket):
                    return _freeze_lost_receipt(ticket)
                return LegacyIngressQuiescenceReceipt(
                    ticket=ticket,
                    status=LegacyIngressQuiescenceStatus.TIMED_OUT,
                    remaining_task_names=_task_names(pending),
                )

    def thaw(self, ticket: LegacyIngressFreezeTicket) -> bool:
        """Release a freeze only after its exact pre-freeze task set drained."""

        state = self._require_ticket(ticket)
        if self._pre_freeze_tasks(state, ticket):
            raise LegacyIngressFreezeError(
                "cannot thaw legacy ingress before tracked pre-freeze tasks exit"
            )
        state.freeze_ticket = None
        self._discard_idle_state(ticket.session_id, state)
        return True

    def _finish_route_task(
        self,
        session_id: str,
        task_token: LegacyIngressTaskToken,
        task: asyncio.Future[Any],
    ) -> None:
        state = self._states.get(session_id)
        if state is None:
            return
        if state.tasks.get(task) == task_token:
            state.tasks.pop(task, None)
        self._discard_idle_state(session_id, state)

    def _require_ticket(
        self,
        ticket: LegacyIngressFreezeTicket,
    ) -> _LegacyIngressSessionState:
        if not isinstance(ticket, LegacyIngressFreezeTicket):
            raise TypeError("ticket must be a LegacyIngressFreezeTicket")
        state = self._states.get(ticket.session_id)
        if state is None or state.freeze_ticket != ticket:
            raise LegacyIngressFreezeError("freeze ticket is no longer active")
        return state

    def _ticket_is_current(self, ticket: LegacyIngressFreezeTicket) -> bool:
        state = self._states.get(ticket.session_id)
        return state is not None and state.freeze_ticket == ticket

    @staticmethod
    def _pre_freeze_tasks(
        state: _LegacyIngressSessionState,
        ticket: LegacyIngressFreezeTicket,
    ) -> tuple[asyncio.Future[Any], ...]:
        return tuple(
            task
            for task, task_token in state.tasks.items()
            if not task.done() and task_token.admission_epoch < ticket.freeze_epoch
        )

    def _discard_idle_state(
        self,
        session_id: str,
        state: _LegacyIngressSessionState,
    ) -> None:
        if state.freeze_ticket is None and not state.tasks:
            self._states.pop(session_id, None)


def _normalize_session_id(session_id: str) -> str:
    """Validate one base session identifier used for local ingress tracking."""

    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        raise ValueError("session_id must not be empty")
    return normalized_session_id


def _normalize_timeout(timeout_seconds: float | None) -> float | None:
    """Validate an optional local ingress drain timeout."""

    if timeout_seconds is None:
        return None
    timeout = float(timeout_seconds)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout_seconds must be finite and non-negative")
    return timeout


def _normalize_cutover_id(cutover_id: str) -> str:
    """Validate one future-controller operation identity without persisting it."""

    normalized_cutover_id = str(cutover_id or "").strip()
    if not normalized_cutover_id:
        raise ValueError("cutover_id must not be empty")
    return normalized_cutover_id


def _task_names(tasks: tuple[asyncio.Future[Any], ...] | set[asyncio.Future[Any]]) -> tuple[str, ...]:
    """Return stable task labels without assuming every future has a name."""

    names = []
    for task in tasks:
        get_name = getattr(task, "get_name", None)
        name = get_name() if callable(get_name) else type(task).__name__
        names.append(str(name))
    return tuple(sorted(names))


def _freeze_lost_receipt(
    ticket: LegacyIngressFreezeTicket,
) -> LegacyIngressQuiescenceReceipt:
    """Return a negative receipt when the local freeze epoch changed mid-drain."""

    return LegacyIngressQuiescenceReceipt(
        ticket=ticket,
        status=LegacyIngressQuiescenceStatus.FREEZE_LOST,
    )


__all__ = [
    "LegacyIngressDurableAdmissionRequired",
    "LegacyIngressFreezeError",
    "LegacyIngressFreezeTicket",
    "LegacyIngressQuiescenceReceipt",
    "LegacyIngressQuiescenceStatus",
    "LegacyIngressSessionAdmission",
    "LegacyIngressSessionRegistry",
    "LegacyIngressTaskToken",
]
