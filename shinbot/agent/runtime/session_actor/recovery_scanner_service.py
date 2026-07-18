"""Supervised typed-recovery discovery without granting production activation.

The recovery graph scanner creates only durable recovery cases and mailbox
deliveries. This service owns bounded polling, legacy unfenced wake retry, and
advisory fenced mailbox-handoff notification. Runtime composition may construct
it while inactive, but must not start it until the full Actor v2 cutover gate is
satisfied.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    supervised_backoff_seconds,
)
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.recovery_scanner import (
    MAX_RECOVERY_SCAN_CANDIDATES,
    RecoveryScanDisposition,
    RecoveryScanSummary,
    RecoveryWakeCursor,
    RecoveryWakeDebt,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffNotifier
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:recovery", color="yellow")

type _LegacyWakeInflightKey = tuple[
    int,
    int,
    FencedMailboxWakeRequest,
]


class RecoveryScannerPort(Protocol):
    """Read-only scanner surface supervised after activation."""

    def scan(
        self,
        *,
        limit: int = MAX_RECOVERY_SCAN_CANDIDATES,
        profile_id: str | None = None,
    ) -> RecoveryScanSummary:
        """Persist one bounded recovery discovery pass."""

    def pending_recovery_wake_requests(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        profile_id: str | None = None,
    ) -> tuple[FencedMailboxWakeRequest, ...]:
        """Return one bounded page of exact durable recovery wake debt."""

    def pending_recovery_wake_debts(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        after: RecoveryWakeCursor | None = None,
        profile_id: str | None = None,
    ) -> tuple[RecoveryWakeDebt, ...]:
        """Return event-versioned recovery debt using a stable keyset cursor."""

    def is_pending_recovery_wake_request(
        self,
        request: FencedMailboxWakeRequest,
    ) -> bool:
        """Return whether one exact durable recovery wake remains current."""

    def is_pending_recovery_wake_debt(self, debt: RecoveryWakeDebt) -> bool:
        """Return whether one exact mailbox event still requires a wake."""


class RecoveryScannerWakeTarget(Protocol):
    """Actor wake surface used only after scanner commits complete."""

    def wake(self, key: SessionKey) -> Awaitable[None] | None:
        """Wake the actor that owns an already-persisted mailbox delivery."""


class RecoveryScannerWakeError(RuntimeError):
    """Report an unfenced legacy recovery wake that could not be delivered."""

    def __init__(self, requests: tuple[FencedMailboxWakeRequest, ...]) -> None:
        """Preserve full failed identities while retaining legacy key access."""

        self.requests = requests
        self.keys = _unique_keys(request.key for request in requests)
        rendered = ", ".join(
            f"{request.key.profile_id}:{request.key.session_id}"
            for request in requests
        )
        super().__init__("typed recovery wake failed for: " + rendered)


@dataclass(slots=True, frozen=True)
class _RecoveryWakeDebt:
    """One local wake handoff with an exact mailbox identity when available."""

    request: FencedMailboxWakeRequest
    event_id: str = ""
    mailbox_id: int | None = None
    cursor: RecoveryWakeCursor | None = field(default=None, compare=False, repr=False)
    durable_debt: RecoveryWakeDebt | None = field(
        default=None,
        compare=False,
        repr=False,
    )
    new_delivery: bool = field(default=False, compare=False)

    def __post_init__(self) -> None:
        """Normalize optional event evidence before it enters local retry state."""

        if not isinstance(self.request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        event_id = str(self.event_id or "").strip()
        if self.new_delivery and not event_id:
            raise ValueError("new_delivery requires a durable event_id")
        mailbox_id = self.mailbox_id
        if mailbox_id is not None and (
            isinstance(mailbox_id, bool)
            or not isinstance(mailbox_id, int)
            or mailbox_id < 1
        ):
            raise ValueError("mailbox_id must be a positive integer when provided")
        if self.new_delivery and mailbox_id is None:
            raise ValueError("new_delivery requires an exact mailbox_id")
        object.__setattr__(self, "event_id", event_id)


class _WakeAttemptDisposition(StrEnum):
    """Process-local handoff result used to retain durable redrive evidence."""

    HANDLED = "handled"
    DEFERRED = "deferred"
    IN_FLIGHT = "in_flight"
    RETRY = "retry"


class DurableRecoveryScannerService:
    """Supervise typed recovery discovery and durable wake redrive.

    An already-delivered recovery case is included in later wake passes. This
    preserves recovery after a post-commit wake failure without issuing a
    second mailbox event or replaying a model workflow.
    """

    def __init__(
        self,
        scanner: RecoveryScannerPort,
        *,
        wake_target: RecoveryScannerWakeTarget | None = None,
        mailbox_handoff_notifier: MailboxHandoffNotifier | None = None,
        tick_interval_seconds: float = 5.0,
        batch_limit: int = MAX_RECOVERY_SCAN_CANDIDATES,
        wake_limit: int = MAX_RECOVERY_SCAN_CANDIDATES,
        wake_timeout_seconds: float = 20.0,
        profile_id: str | None = None,
        runtime_id: str | None = None,
    ) -> None:
        """Initialize an unstarted bounded recovery supervisor."""

        if scanner is None:
            raise TypeError("scanner must not be None")
        if mailbox_handoff_notifier is not None and not callable(
            getattr(mailbox_handoff_notifier, "notify", None)
        ):
            raise TypeError("mailbox_handoff_notifier must implement notify(mailbox_id)")
        self._scanner = scanner
        self._wake_target = wake_target
        self._mailbox_handoff_notifier = mailbox_handoff_notifier
        self._tick_interval_seconds = _positive_finite(
            tick_interval_seconds,
            field_name="tick_interval_seconds",
        )
        if (
            isinstance(batch_limit, bool)
            or not isinstance(batch_limit, int)
            or not 1 <= batch_limit <= MAX_RECOVERY_SCAN_CANDIDATES
        ):
            raise ValueError(
                "batch_limit must be between 1 and "
                f"{MAX_RECOVERY_SCAN_CANDIDATES}"
            )
        self._batch_limit = batch_limit
        self._wake_limit = _positive_int(wake_limit, field_name="wake_limit")
        self._wake_timeout_seconds = _positive_finite(
            wake_timeout_seconds,
            field_name="wake_timeout_seconds",
        )
        normalized_profile_id = str(profile_id or "").strip()
        self._profile_id = normalized_profile_id or None
        self._runtime_id = str(
            runtime_id or f"recovery-scanner:{uuid.uuid4().hex}"
        ).strip()
        if not self._runtime_id:
            raise ValueError("runtime_id must not be empty")
        self._task: asyncio.Task[None] | None = None
        self._health = RuntimeServiceHealth("durable_recovery_scanner")
        self._last_summary = RecoveryScanSummary(results=())
        self._state_capacity = max(1, self._wake_limit * 2)
        self._accepted_wake_events: OrderedDict[_RecoveryWakeDebt, None] = (
            OrderedDict()
        )
        self._legacy_wake_inflight: set[_LegacyWakeInflightKey] = set()
        self._wake_deferred: OrderedDict[_RecoveryWakeDebt, None] = OrderedDict()
        self._wake_followups: OrderedDict[_RecoveryWakeDebt, None] = OrderedDict()
        self._wake_discovery_cursor: RecoveryWakeCursor | None = None
        self._wake_head_probe_cursor: RecoveryWakeCursor | None = None
        self._wake_discovery_offset = 0
        self._use_keyset_wake_query = callable(
            getattr(scanner, "pending_recovery_wake_debts", None)
        )
        self._wake_target_epoch = 0
        self._lifecycle_epoch = 0
        self._wake_followup_turn = False
        self._run_lock = asyncio.Lock()
        self._wake_pages_since_head_probe = 0
        self._wake_head_probe_interval = max(4, self._state_capacity)
        self._wake_head_probe_required = False

    @property
    def last_summary(self) -> RecoveryScanSummary:
        """Return the most recent persisted recovery discovery result."""

        return self._last_summary

    @property
    def runtime_id(self) -> str:
        """Return the process-local identity used in task diagnostics."""

        return self._runtime_id

    def health_snapshot(self) -> RuntimeServiceHealthSnapshot:
        """Return process-local lifecycle and retry health."""

        return self._health.snapshot()

    def bind_wake_target(
        self,
        wake_target: RecoveryScannerWakeTarget | None,
    ) -> None:
        """Replace the post-commit wake target without starting a scan."""

        self._wake_target = wake_target
        self._wake_target_epoch += 1
        self._legacy_wake_inflight.clear()
        self._accepted_wake_events.clear()
        self._wake_deferred.clear()
        self._reset_pending_wake_cursors()

    def bind_mailbox_handoff_notifier(
        self,
        notifier: MailboxHandoffNotifier | None,
    ) -> None:
        """Replace the advisory notifier without binding an Actor target."""

        if notifier is not None and not callable(getattr(notifier, "notify", None)):
            raise TypeError("notifier must implement notify(mailbox_id)")
        self._mailbox_handoff_notifier = notifier
        self._reset_pending_wake_cursors()

    def start(self) -> None:
        """Start bounded polling when an activation supervisor authorizes it."""

        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("agent.recovery_scanner.start_skipped | no_running_loop")
            return
        self._health.start()
        self._task = loop.create_task(
            self._run_loop(),
            name=f"agent-durable-recovery-scanner:{self._runtime_id}",
        )

    async def shutdown(self) -> None:
        """Stop polling without mutating recovery cases or mailbox rows."""

        self._lifecycle_epoch += 1
        self._wake_target_epoch += 1
        self._clear_local_wake_state()
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._health.stop()

    async def run_once(self) -> RecoveryScanSummary:
        """Persist one scan pass, then redrive all durable recovery wake debt."""

        request_epoch = self._lifecycle_epoch
        async with self._run_lock:
            if request_epoch != self._lifecycle_epoch:
                return self._last_summary
            return await self._run_once_locked(lifecycle_epoch=request_epoch)

    async def _run_once_locked(
        self,
        *,
        lifecycle_epoch: int,
    ) -> RecoveryScanSummary:
        """Run one serialized scanner pass under its current lifecycle epoch."""

        if lifecycle_epoch != self._lifecycle_epoch:
            return self._last_summary
        self._health.scan_started()
        wake_target_epoch = self._wake_target_epoch
        summary = RecoveryScanSummary(results=())
        try:
            summary = self._scanner.scan(
                limit=self._batch_limit,
                profile_id=self._profile_id,
            )
            self._last_summary = summary
            summary_debts = _summary_wake_debts(summary)
            pending_debts, is_head_probe = self._select_pending_wake_debts()
            debts = _merge_wake_debt_sources(
                followups=self._wake_followups,
                summary=summary_debts,
                pending=pending_debts,
            )
            failures: list[FencedMailboxWakeRequest] = []
            progressed_debts: set[_RecoveryWakeDebt] = set()
            for debt in self._attemptable_wake_debts(debts):
                if lifecycle_epoch != self._lifecycle_epoch:
                    break
                if (
                    debt in self._wake_followups
                    and not self._is_pending_wake_debt(debt)
                ):
                    self._wake_followups.pop(debt, None)
                    continue
                try:
                    disposition = await self._wake_request(
                        debt,
                        lifecycle_epoch=lifecycle_epoch,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if lifecycle_epoch != self._lifecycle_epoch:
                        break
                    self._enqueue_wake_followup(debt)
                    progressed_debts.add(debt)
                    failures.append(debt.request)
                    logger.exception(
                        format_log_event(
                            "agent.recovery_scanner.wake_failed",
                            profile_id=debt.request.key.profile_id,
                            session_id=debt.request.key.session_id,
                            ownership_generation=debt.request.ownership_generation,
                            admission_fence_id=debt.request.admission_fence_id,
                            admission_fence_generation=(
                                debt.request.admission_fence_generation
                            ),
                        )
                    )
                    continue
                if lifecycle_epoch != self._lifecycle_epoch:
                    break
                if disposition is _WakeAttemptDisposition.HANDLED:
                    self._wake_followups.pop(debt, None)
                    progressed_debts.add(debt)
                elif disposition is _WakeAttemptDisposition.DEFERRED:
                    # No local retry is retained without a target. Binding a
                    # target resets discovery, so this page can keep moving.
                    progressed_debts.add(debt)
                elif disposition in {
                    _WakeAttemptDisposition.IN_FLIGHT,
                    _WakeAttemptDisposition.RETRY,
                }:
                    self._enqueue_wake_followup(debt)
                    progressed_debts.add(debt)
            if lifecycle_epoch == self._lifecycle_epoch:
                if wake_target_epoch == self._wake_target_epoch:
                    self._advance_pending_wake_cursor(
                        pending_debts,
                        is_head_probe=is_head_probe,
                        progressed_debts=progressed_debts,
                    )
                else:
                    self._reset_pending_wake_cursors()
            if failures and lifecycle_epoch == self._lifecycle_epoch:
                raise RecoveryScannerWakeError(_unique_wake_requests(failures))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._health.failed(exc)
            raise
        if lifecycle_epoch != self._lifecycle_epoch:
            return summary
        self._health.succeeded()
        return summary

    async def _run_loop(self) -> None:
        """Run bounded passes with backoff while the supervisor stays active."""

        delay = 0.0
        try:
            while True:
                if delay > 0:
                    await asyncio.sleep(delay)
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        format_log_event(
                            "agent.recovery_scanner.iteration_failed",
                            error_code=type(exc).__name__,
                            consecutive_failures=(
                                self._health.snapshot().consecutive_failures
                            ),
                        )
                    )
                    delay = supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=(
                            self._health.snapshot().consecutive_failures
                        ),
                    )
                    continue
                delay = self._tick_interval_seconds
        finally:
            self._health.stop()

    def _select_pending_wake_debts(
        self,
    ) -> tuple[tuple[_RecoveryWakeDebt, ...], bool]:
        """Select a main or fair head-probe page without dropping keyset progress."""

        if not self._use_keyset_wake_query:
            debts = self._pending_wake_debts(after=None)
            if not debts and self._wake_discovery_offset:
                self._wake_discovery_offset = 0
                debts = self._pending_wake_debts(after=None)
            return debts, False
        is_head_probe = self._next_head_probe()
        cursor = (
            self._wake_head_probe_cursor
            if is_head_probe
            else self._wake_discovery_cursor
        )
        debts = self._pending_wake_debts(after=cursor)
        if not debts:
            if is_head_probe:
                self._wake_head_probe_cursor = None
                self._wake_head_probe_required = False
            elif cursor is not None:
                self._wake_discovery_cursor = None
                debts = self._pending_wake_debts(after=None)
        return debts, is_head_probe

    def _pending_wake_debts(
        self,
        *,
        after: RecoveryWakeCursor | None,
    ) -> tuple[_RecoveryWakeDebt, ...]:
        """Read one bounded page through the newest scanner API or legacy query."""

        if self._use_keyset_wake_query:
            raw_debts = self._scanner.pending_recovery_wake_debts(
                limit=self._wake_limit,
                after=after,
                profile_id=self._profile_id,
            )
            return _unique_wake_debts(
                _project_pending_wake_debt(raw_debt) for raw_debt in raw_debts
            )
        fetch_requests = getattr(
            self._scanner,
            "pending_recovery_wake_requests",
            None,
        )
        if not callable(fetch_requests):
            raise RuntimeError(
                "recovery scanner must provide an exact pending wake query"
            )
        requests = fetch_requests(
            limit=self._wake_limit,
            offset=self._wake_discovery_offset,
            profile_id=self._profile_id,
        )
        return _unique_wake_debts(
            _RecoveryWakeDebt(request=request) for request in requests
        )

    def _next_head_probe(self) -> bool:
        """Return whether this keyset pass must advance the independent head scan."""

        if not self._wake_head_probe_required:
            return False
        self._wake_pages_since_head_probe += 1
        if self._wake_pages_since_head_probe < self._wake_head_probe_interval:
            return False
        self._wake_pages_since_head_probe = 0
        return True

    def _reset_pending_wake_cursors(self) -> None:
        """Forget all process-local cursor state for a fresh durable discovery lap."""

        self._wake_discovery_cursor = None
        self._wake_head_probe_cursor = None
        self._wake_discovery_offset = 0
        self._wake_pages_since_head_probe = 0
        self._wake_head_probe_required = False

    def _advance_pending_wake_cursor(
        self,
        debts: tuple[_RecoveryWakeDebt, ...],
        *,
        is_head_probe: bool,
        progressed_debts: set[_RecoveryWakeDebt],
    ) -> None:
        """Advance through only the durable page prefix handled this pass.

        A fair follow-up retry may consume the wake budget before the current
        durable page is attempted. Advancing past that unattempted row would
        make keyset pagination lose it forever under continuous append.
        """

        if not debts:
            if self._use_keyset_wake_query:
                if is_head_probe:
                    self._wake_head_probe_cursor = None
                else:
                    self._wake_discovery_cursor = None
            else:
                self._wake_discovery_offset = 0
            return
        progressed_requests = {debt.request for debt in progressed_debts}
        safe_count = 0
        for debt in debts:
            request_progressed = (
                not self._use_keyset_wake_query
                and debt.request in progressed_requests
            )
            if (
                debt not in progressed_debts
                and not request_progressed
                and not self._is_accepted_wake(debt)
            ):
                break
            safe_count += 1
        if not self._use_keyset_wake_query:
            self._wake_discovery_offset += safe_count
            return
        if safe_count == 0:
            return
        cursor = debts[safe_count - 1].cursor
        if cursor is None:
            raise RuntimeError("event-versioned recovery debt must carry a cursor")
        if is_head_probe:
            self._wake_head_probe_cursor = cursor
        else:
            self._wake_discovery_cursor = cursor

    def _is_pending_wake_debt(self, debt: _RecoveryWakeDebt) -> bool:
        """Revalidate only a selected local follow-up before it can wake again."""

        if debt.durable_debt is not None:
            result = self._scanner.is_pending_recovery_wake_debt(debt.durable_debt)
        else:
            result = self._scanner.is_pending_recovery_wake_request(debt.request)
        if not isinstance(result, bool):
            raise TypeError("recovery wake revalidation must return a bool")
        return result

    def _attemptable_wake_debts(
        self,
        debts: Iterable[_RecoveryWakeDebt],
    ) -> tuple[_RecoveryWakeDebt, ...]:
        """Apply a fair hard wake budget after cached terminal filtering."""

        followups: list[_RecoveryWakeDebt] = []
        discovered: list[_RecoveryWakeDebt] = []
        for debt in debts:
            if self._is_accepted_wake(debt):
                self._wake_followups.pop(debt, None)
                continue
            if debt in self._wake_followups:
                followups.append(debt)
            else:
                discovered.append(debt)
        result: list[_RecoveryWakeDebt] = []
        had_both_sources = bool(followups and discovered)
        choose_followup = self._wake_followup_turn
        while len(result) < self._wake_limit and (followups or discovered):
            if followups and discovered:
                source = followups if choose_followup else discovered
                choose_followup = not choose_followup
            else:
                source = followups or discovered
            result.append(source.pop(0))
        if had_both_sources:
            self._wake_followup_turn = choose_followup
        elif result:
            self._wake_followup_turn = not self._wake_followup_turn
        return tuple(result)

    def _enqueue_wake_followup(self, debt: _RecoveryWakeDebt) -> None:
        """Retain only bounded event-specific work that arrived during a wake."""

        if not debt.event_id:
            return
        self._wake_followups.pop(debt, None)
        was_evicted = len(self._wake_followups) >= self._state_capacity
        self._wake_followups[debt] = None
        _trim_ordered_state(self._wake_followups, limit=self._state_capacity)
        if was_evicted:
            self._wake_head_probe_required = True
            # Restart the independent head lap when local retry state loses an
            # entry; the previous head cursor may already be beyond that debt.
            self._wake_head_probe_cursor = None
            self._wake_pages_since_head_probe = 0

    async def _wake_request(
        self,
        debt: _RecoveryWakeDebt,
        *,
        lifecycle_epoch: int,
    ) -> _WakeAttemptDisposition:
        """Hand off one recovery delivery through its permitted boundary."""

        request = debt.request
        if self._is_accepted_wake(debt):
            return _WakeAttemptDisposition.HANDLED
        if request.has_admission_fence:
            return await self._notify_fenced_mailbox_handoff(debt)
        return await self._wake_legacy_request(
            debt,
            lifecycle_epoch=lifecycle_epoch,
        )

    async def _notify_fenced_mailbox_handoff(
        self,
        debt: _RecoveryWakeDebt,
    ) -> _WakeAttemptDisposition:
        """Publish an advisory hint for one exact fenced mailbox handoff.

        The recovery scanner owns neither a handoff claim nor an Actor target.
        It accepts an exact mailbox id only when the scanner has published it
        on a validated summary result or durable pending-debt projection.
        """

        request = debt.request
        mailbox_id = debt.mailbox_id
        if mailbox_id is None:
            logger.debug(
                format_log_event(
                    "agent.recovery_scanner.fenced_mailbox_handoff_waiting_for_identity",
                    profile_id=request.key.profile_id,
                    session_id=request.key.session_id,
                    ownership_generation=request.ownership_generation,
                    admission_fence_id=request.admission_fence_id,
                    admission_fence_generation=request.admission_fence_generation,
                )
            )
            return _WakeAttemptDisposition.HANDLED
        notifier = self._mailbox_handoff_notifier
        if notifier is None:
            logger.debug(
                format_log_event(
                    "agent.recovery_scanner.fenced_mailbox_handoff_deferred",
                    mailbox_id=mailbox_id,
                    profile_id=request.key.profile_id,
                    session_id=request.key.session_id,
                    ownership_generation=request.ownership_generation,
                    admission_fence_id=request.admission_fence_id,
                    admission_fence_generation=request.admission_fence_generation,
                )
            )
            return _WakeAttemptDisposition.HANDLED
        try:
            notification = notifier.notify(mailbox_id)
            if inspect.isawaitable(notification):
                await notification
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                format_log_event(
                    "agent.recovery_scanner.fenced_mailbox_handoff_notify_failed",
                    mailbox_id=mailbox_id,
                    profile_id=request.key.profile_id,
                    session_id=request.key.session_id,
                    ownership_generation=request.ownership_generation,
                    admission_fence_id=request.admission_fence_id,
                    admission_fence_generation=request.admission_fence_generation,
                )
            )
        return _WakeAttemptDisposition.HANDLED

    async def _wake_legacy_request(
        self,
        debt: _RecoveryWakeDebt,
        *,
        lifecycle_epoch: int,
    ) -> _WakeAttemptDisposition:
        """Keep key-based wake compatibility only for explicitly unfenced debt."""

        target = self._wake_target
        if target is None:
            self._record_deferred_wake(debt)
            return _WakeAttemptDisposition.DEFERRED
        request = debt.request
        target_epoch = self._wake_target_epoch
        inflight: _LegacyWakeInflightKey = (
            lifecycle_epoch,
            target_epoch,
            request,
        )
        if inflight in self._legacy_wake_inflight:
            return _WakeAttemptDisposition.IN_FLIGHT
        if (
            target is not self._wake_target
            or target_epoch != self._wake_target_epoch
            or lifecycle_epoch != self._lifecycle_epoch
        ):
            return _WakeAttemptDisposition.RETRY
        self._legacy_wake_inflight.add(inflight)
        try:
            async with asyncio.timeout(self._wake_timeout_seconds):
                result = target.wake(request.key)
                if inspect.isawaitable(result):
                    await result
        except asyncio.CancelledError:
            raise
        except Exception:
            if (
                target is not self._wake_target
                or target_epoch != self._wake_target_epoch
                or lifecycle_epoch != self._lifecycle_epoch
            ):
                return _WakeAttemptDisposition.RETRY
            raise
        finally:
            self._legacy_wake_inflight.discard(inflight)
        if (
            target is not self._wake_target
            or target_epoch != self._wake_target_epoch
            or lifecycle_epoch != self._lifecycle_epoch
        ):
            return _WakeAttemptDisposition.RETRY
        self._mark_accepted_wake(debt)
        return _WakeAttemptDisposition.HANDLED

    def _clear_local_wake_state(self) -> None:
        """Discard process-local suppression state across a shutdown epoch."""

        self._accepted_wake_events.clear()
        self._wake_deferred.clear()
        self._wake_followups.clear()
        self._legacy_wake_inflight.clear()
        self._wake_followup_turn = False
        self._reset_pending_wake_cursors()

    def _is_accepted_wake(self, debt: _RecoveryWakeDebt) -> bool:
        """Respect a prior handoff while allowing a newer mailbox to wake again."""

        if debt.request.has_admission_fence:
            return False
        # A request-only pending projection cannot distinguish a newly committed
        # mailbox from an accepted earlier one. Treat it as unacknowledged until
        # the scanner supplies event-versioned debt rather than losing a wake.
        if not debt.event_id:
            return False
        return debt in self._accepted_wake_events

    def _mark_accepted_wake(self, debt: _RecoveryWakeDebt) -> None:
        """Record one successful handoff by durable event identity when available."""

        if debt.event_id:
            self._accepted_wake_events.pop(debt, None)
            self._accepted_wake_events[debt] = None
            _trim_ordered_state(self._accepted_wake_events, limit=self._state_capacity)
        self._wake_deferred.pop(debt, None)

    def _record_deferred_wake(self, debt: _RecoveryWakeDebt) -> None:
        """Log an unavailable legacy wake target while leaving work durable."""

        if debt in self._wake_deferred:
            return
        self._wake_deferred[debt] = None
        _trim_ordered_state(self._wake_deferred, limit=self._state_capacity)
        logger.warning(
            format_log_event(
                "agent.recovery_scanner.legacy_wake_deferred",
                profile_id=debt.request.key.profile_id,
                session_id=debt.request.key.session_id,
                ownership_generation=debt.request.ownership_generation,
                admission_fence_id=debt.request.admission_fence_id,
                admission_fence_generation=(
                    debt.request.admission_fence_generation
                ),
            )
        )


def _summary_wake_debts(
    summary: RecoveryScanSummary,
) -> tuple[_RecoveryWakeDebt, ...]:
    """Project scanner-validated result identities without collapsing to keys."""

    debts: list[_RecoveryWakeDebt] = []
    for result in summary.results:
        if result.disposition not in {
            RecoveryScanDisposition.DELIVERED,
            RecoveryScanDisposition.ALREADY_DELIVERED,
        }:
            continue
        request = getattr(result, "wake_request", None)
        if request is None:
            continue
        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("recovery summary wake_request must be fenced")
        event_id = str(getattr(result, "event_id", "") or "").strip()
        mailbox_id = getattr(result, "mailbox_id", None)
        debts.append(
            _RecoveryWakeDebt(
                request=request,
                event_id=event_id,
                mailbox_id=mailbox_id,
                new_delivery=(
                    result.disposition is RecoveryScanDisposition.DELIVERED
                ),
            )
        )
    if debts:
        return _unique_wake_debts(debts)
    # ``wake_requests`` is the scanner's compact public projection. Use it
    # only as a compatibility fallback because it cannot carry event identity.
    requests = getattr(summary, "wake_requests", ())
    return _unique_wake_debts(_RecoveryWakeDebt(request=request) for request in requests)


def _project_pending_wake_debt(raw_debt: RecoveryWakeDebt) -> _RecoveryWakeDebt:
    """Carry scanner-owned mailbox identity into service-local wake state."""

    if not isinstance(raw_debt, RecoveryWakeDebt):
        raise TypeError("pending recovery wake debt must be a RecoveryWakeDebt")
    return _RecoveryWakeDebt(
        request=raw_debt.request,
        event_id=raw_debt.event_id,
        mailbox_id=raw_debt.mailbox_id,
        cursor=raw_debt.cursor,
        durable_debt=raw_debt,
    )


def _unique_wake_debts(
    debts: Iterable[_RecoveryWakeDebt],
) -> tuple[_RecoveryWakeDebt, ...]:
    """Preserve order while deduplicating only an exact request/event pair."""

    selected: dict[tuple[FencedMailboxWakeRequest, str], _RecoveryWakeDebt] = {}
    for debt in debts:
        if not isinstance(debt, _RecoveryWakeDebt):
            raise TypeError("wake debt must contain _RecoveryWakeDebt values")
        identity = (debt.request, debt.event_id)
        previous = selected.get(identity)
        if previous is None or (debt.new_delivery and not previous.new_delivery):
            selected[identity] = debt
    return tuple(selected.values())


def _merge_wake_debt_sources(
    *,
    followups: Iterable[_RecoveryWakeDebt],
    summary: Iterable[_RecoveryWakeDebt],
    pending: Iterable[_RecoveryWakeDebt],
) -> tuple[_RecoveryWakeDebt, ...]:
    """Merge only exact mailbox-event identities from all bounded sources."""

    versioned = _unique_wake_debts((*followups, *summary))
    versioned_requests = {
        debt.request for debt in versioned if debt.event_id
    }
    return _unique_wake_debts(
        (
            *versioned,
            *(
                debt
                for debt in pending
                if debt.event_id or debt.request not in versioned_requests
            ),
        )
    )


def _trim_ordered_state(
    state: OrderedDict[object, None],
    *,
    limit: int,
) -> None:
    """Bound an acknowledgement-only cache without dropping durable work."""

    while len(state) > limit:
        state.popitem(last=False)


def _unique_wake_requests(
    requests: Iterable[FencedMailboxWakeRequest],
) -> tuple[FencedMailboxWakeRequest, ...]:
    """Preserve all distinct ownership incarnations in deterministic order."""

    result: list[FencedMailboxWakeRequest] = []
    seen: set[FencedMailboxWakeRequest] = set()
    for request in requests:
        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("wake requests must be FencedMailboxWakeRequest values")
        if request in seen:
            continue
        seen.add(request)
        result.append(request)
    return tuple(result)


def _unique_keys(keys: Iterable[SessionKey]) -> tuple[SessionKey, ...]:
    """Retain the legacy error projection without using it for wake dispatch."""

    result: list[SessionKey] = []
    seen: set[SessionKey] = set()
    for key in keys:
        if not isinstance(key, SessionKey):
            raise TypeError("wake keys must be SessionKey values")
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
    return tuple(result)


def _positive_int(value: object, *, field_name: str) -> int:
    """Normalize one positive bounded-work limit."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _positive_finite(value: object, *, field_name: str) -> float:
    """Normalize one finite positive service interval."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite positive number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{field_name} must be a finite positive number")
    return normalized


__all__ = [
    "DurableRecoveryScannerService",
    "RecoveryScannerPort",
    "RecoveryScannerWakeError",
    "RecoveryScannerWakeTarget",
]
