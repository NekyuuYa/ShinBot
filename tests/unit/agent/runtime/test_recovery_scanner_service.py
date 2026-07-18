"""Tests for supervised recovery discovery and mailbox-handoff notification."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from shinbot.agent.runtime.service_health import RuntimeServiceStatus
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.recovery_scanner import (
    RecoveryScanDisposition,
    RecoveryScanResult,
    RecoveryScanSummary,
    RecoveryWakeCursor,
    RecoveryWakeDebt,
)
from shinbot.agent.runtime.session_actor.recovery_scanner_service import (
    DurableRecoveryScannerService,
    RecoveryScannerWakeError,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest


@dataclass(slots=True)
class _Scanner:
    """Deterministic scanner with an event-versioned durable debt projection."""

    summary: RecoveryScanSummary
    pending_debts: tuple[RecoveryWakeDebt, ...] = ()
    calls: list[tuple[int, str | None]] = field(default_factory=list)
    pending_calls: list[tuple[int, int | None, str | None]] = field(
        default_factory=list
    )
    debt_validation_calls: list[RecoveryWakeDebt] = field(default_factory=list)

    def scan(self, *, limit: int, profile_id: str | None) -> RecoveryScanSummary:
        """Return the current bounded discovery summary."""

        self.calls.append((limit, profile_id))
        return self.summary

    def pending_recovery_wake_debts(
        self,
        *,
        limit: int,
        offset: int = 0,
        after: RecoveryWakeCursor | None = None,
        profile_id: str | None,
    ) -> tuple[RecoveryWakeDebt, ...]:
        """Return one profile-scoped stable keyset page."""

        assert offset == 0
        self.pending_calls.append(
            (limit, after.mailbox_id if after is not None else None, profile_id)
        )
        debts = self.pending_debts
        if profile_id is not None:
            debts = tuple(
                debt for debt in debts if debt.request.key.profile_id == profile_id
            )
        if after is not None:
            debts = tuple(debt for debt in debts if debt.mailbox_id > after.mailbox_id)
        return debts[:limit]

    def pending_recovery_wake_requests(
        self,
        *,
        limit: int,
        offset: int,
        profile_id: str | None,
    ) -> tuple[FencedMailboxWakeRequest, ...]:
        """Expose the legacy projection for structural protocol compatibility."""

        debts = self.pending_debts
        if profile_id is not None:
            debts = tuple(
                debt for debt in debts if debt.request.key.profile_id == profile_id
            )
        return tuple(debt.request for debt in debts[offset : offset + limit])

    def is_pending_recovery_wake_request(
        self,
        request: FencedMailboxWakeRequest,
    ) -> bool:
        """Report whether an older request-only debt remains pending."""

        return any(debt.request == request for debt in self.pending_debts)

    def is_pending_recovery_wake_debt(self, debt: RecoveryWakeDebt) -> bool:
        """Revalidate one exact event-versioned durable debt."""

        self.debt_validation_calls.append(debt)
        return debt in self.pending_debts


@dataclass(slots=True)
class _LegacyScanner:
    """Older scanner double that cannot provide a durable mailbox identity."""

    summary: RecoveryScanSummary
    pending_requests: tuple[FencedMailboxWakeRequest, ...]
    calls: list[tuple[int, str | None]] = field(default_factory=list)
    pending_calls: list[tuple[int, int, str | None]] = field(default_factory=list)

    def scan(self, *, limit: int, profile_id: str | None) -> RecoveryScanSummary:
        """Return the current discovery summary."""

        self.calls.append((limit, profile_id))
        return self.summary

    def pending_recovery_wake_requests(
        self,
        *,
        limit: int,
        offset: int,
        profile_id: str | None,
    ) -> tuple[FencedMailboxWakeRequest, ...]:
        """Return a request-only page without a mailbox id."""

        self.pending_calls.append((limit, offset, profile_id))
        requests = self.pending_requests
        if profile_id is not None:
            requests = tuple(
                request
                for request in requests
                if request.key.profile_id == profile_id
            )
        return requests[offset : offset + limit]

    def is_pending_recovery_wake_request(
        self,
        request: FencedMailboxWakeRequest,
    ) -> bool:
        """Keep request-only debt available for legacy redrive."""

        return request in self.pending_requests


@dataclass(slots=True)
class _WakeTarget:
    """Legacy key-only wake double for explicitly unfenced recovery debt."""

    failing_keys: set[SessionKey] = field(default_factory=set)
    calls: list[SessionKey] = field(default_factory=list)

    async def wake(self, key: SessionKey) -> None:
        """Record one key wake or simulate a transient failure."""

        self.calls.append(key)
        if key in self.failing_keys:
            raise RuntimeError(f"wake failed for {key.session_id}")


@dataclass(slots=True)
class _MailboxHandoffNotifier:
    """Capture advisory exact-mailbox hints without acting as an Actor target."""

    failures_remaining: int = 0
    mailbox_ids: list[int] = field(default_factory=list)

    async def notify(self, mailbox_id: int) -> None:
        """Record one hint and optionally simulate an advisory failure."""

        self.mailbox_ids.append(mailbox_id)
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("synthetic handoff notifier failure")


@dataclass(slots=True)
class _SynchronousMailboxHandoffNotifier:
    """Verify that a notifier may complete synchronously."""

    mailbox_ids: list[int] = field(default_factory=list)

    def notify(self, mailbox_id: int) -> None:
        """Record one synchronous advisory hint."""

        self.mailbox_ids.append(mailbox_id)


class _BlockingLegacyWakeTarget(_WakeTarget):
    """Hold one legacy wake so shutdown serialization remains covered."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def wake(self, key: SessionKey) -> None:
        """Expose the running legacy wake to an overlapping caller."""

        self.calls.append(key)
        self.entered.set()
        await self.release.wait()


def _request(
    key: SessionKey,
    *,
    generation: int = 1,
    fence_id: str = "",
    fence_generation: int = 0,
) -> FencedMailboxWakeRequest:
    """Build one exact recovery wake identity."""

    return FencedMailboxWakeRequest(
        key=key,
        ownership_generation=generation,
        admission_fence_id=fence_id,
        admission_fence_generation=fence_generation,
    )


def _debt(
    request: FencedMailboxWakeRequest,
    *,
    mailbox_id: int,
    event_id: str,
) -> RecoveryWakeDebt:
    """Build an exact durable pending recovery handoff projection."""

    return RecoveryWakeDebt(
        request=request,
        event_id=event_id,
        cursor=RecoveryWakeCursor(
            mailbox_id=mailbox_id,
            profile_id=request.key.profile_id,
            session_id=request.key.session_id,
            ownership_generation=request.ownership_generation,
            admission_fence_id=request.admission_fence_id,
            admission_fence_generation=request.admission_fence_generation,
        ),
    )


def _result(
    request: FencedMailboxWakeRequest,
    disposition: RecoveryScanDisposition,
    *,
    event_id: str,
    mailbox_id: int,
) -> RecoveryScanResult:
    """Build one persisted recovery delivery result."""

    return RecoveryScanResult(
        key=request.key,
        disposition=disposition,
        event_id=event_id,
        mailbox_id=mailbox_id,
        wake_request=request,
    )


@pytest.mark.asyncio
async def test_fenced_summary_notifies_its_exact_mailbox_id() -> None:
    """A scanner-validated summary hints its mailbox without a legacy wake."""

    request = _request(
        SessionKey("profile-a", "profile-a:group:fenced"),
        fence_id="grant-a",
        fence_generation=3,
    )
    scanner = _Scanner(
        RecoveryScanSummary(
            results=(
                _result(
                    request,
                    RecoveryScanDisposition.DELIVERED,
                    event_id="recovery:fenced",
                    mailbox_id=41,
                ),
            )
        )
    )
    notifier = _MailboxHandoffNotifier()
    legacy_target = _WakeTarget()
    service = DurableRecoveryScannerService(
        scanner,
        wake_target=legacy_target,
        mailbox_handoff_notifier=notifier,
    )

    summary = await service.run_once()

    assert summary is scanner.summary
    assert notifier.mailbox_ids == [41]
    assert legacy_target.calls == []
    assert scanner.pending_calls == [(64, None, None)]
    assert not service._accepted_wake_events
    assert not service._wake_followups
    assert service.health_snapshot().status is RuntimeServiceStatus.RUNNING
    await service.shutdown()


@pytest.mark.asyncio
async def test_request_only_legacy_projection_cannot_notify_fenced_debt() -> None:
    """An older scanner without `RecoveryWakeDebt` leaves fenced work durable."""

    request = _request(
        SessionKey("profile-a", "profile-a:group:request-only"),
        fence_id="grant-a",
        fence_generation=2,
    )
    scanner = _LegacyScanner(
        RecoveryScanSummary(results=()),
        pending_requests=(request,),
    )
    notifier = _MailboxHandoffNotifier()
    legacy_target = _WakeTarget()
    service = DurableRecoveryScannerService(
        scanner,
        wake_target=legacy_target,
        mailbox_handoff_notifier=notifier,
    )

    await service.run_once()

    assert scanner.pending_calls == [(64, 0, None)]
    assert notifier.mailbox_ids == []
    assert legacy_target.calls == []
    assert service.health_snapshot().status is RuntimeServiceStatus.RUNNING
    await service.shutdown()


@pytest.mark.asyncio
async def test_missing_fenced_notifier_keeps_scanning_healthy_and_debt_queryable() -> None:
    """No notifier is a pull-delivery condition, not a recovery failure."""

    request = _request(
        SessionKey("profile-a", "profile-a:group:no-notifier"),
        fence_id="grant-a",
        fence_generation=1,
    )
    debt = _debt(request, mailbox_id=52, event_id="recovery:no-notifier")
    scanner = _Scanner(RecoveryScanSummary(results=()), pending_debts=(debt,))
    service = DurableRecoveryScannerService(scanner)

    summary = await service.run_once()

    assert summary is scanner.summary
    assert scanner.pending_calls == [(64, None, None)]
    assert scanner.is_pending_recovery_wake_debt(debt)
    assert not service._wake_followups
    assert not service._wake_deferred
    assert service.health_snapshot().status is RuntimeServiceStatus.RUNNING
    await service.shutdown()


@pytest.mark.asyncio
async def test_failing_fenced_notifier_is_best_effort_and_keeps_debt_queryable() -> None:
    """Notifier failure neither raises a wake error nor consumes durable debt."""

    request = _request(
        SessionKey("profile-a", "profile-a:group:notifier-failure"),
        fence_id="grant-a",
        fence_generation=1,
    )
    debt = _debt(request, mailbox_id=53, event_id="recovery:notifier-failure")
    scanner = _Scanner(RecoveryScanSummary(results=()), pending_debts=(debt,))
    notifier = _MailboxHandoffNotifier(failures_remaining=1)
    service = DurableRecoveryScannerService(
        scanner,
        mailbox_handoff_notifier=notifier,
    )

    await service.run_once()
    await service.run_once()

    assert notifier.mailbox_ids == [53, 53]
    assert scanner.is_pending_recovery_wake_debt(debt)
    assert not service._accepted_wake_events
    assert not service._wake_followups
    assert service.health_snapshot().status is RuntimeServiceStatus.RUNNING
    assert service.health_snapshot().success_count == 2
    await service.shutdown()


@pytest.mark.asyncio
async def test_notifier_can_be_bound_after_the_durable_debt_already_exists() -> None:
    """Notifier rebinding never publishes an Actor target or changes the debt."""

    request = _request(
        SessionKey("profile-a", "profile-a:group:bind-notifier"),
        fence_id="grant-a",
        fence_generation=1,
    )
    debt = _debt(request, mailbox_id=54, event_id="recovery:bind-notifier")
    scanner = _Scanner(RecoveryScanSummary(results=()), pending_debts=(debt,))
    service = DurableRecoveryScannerService(scanner)

    await service.run_once()
    notifier = _SynchronousMailboxHandoffNotifier()
    service.bind_mailbox_handoff_notifier(notifier)
    await service.run_once()

    assert notifier.mailbox_ids == [54]
    assert debt in scanner.pending_debts
    await service.shutdown()


@pytest.mark.asyncio
async def test_fenced_debts_keep_distinct_mailbox_ids_for_one_session() -> None:
    """Fenced handoffs are not collapsed to a session key or ownership request."""

    key = SessionKey("profile-a", "profile-a:group:reincarnated")
    first = _request(key, generation=1, fence_id="grant-a", fence_generation=1)
    second = _request(key, generation=2, fence_id="grant-b", fence_generation=1)
    scanner = _Scanner(
        RecoveryScanSummary(results=()),
        pending_debts=(
            _debt(first, mailbox_id=61, event_id="recovery:first"),
            _debt(second, mailbox_id=62, event_id="recovery:second"),
        ),
    )
    notifier = _MailboxHandoffNotifier()
    service = DurableRecoveryScannerService(
        scanner,
        mailbox_handoff_notifier=notifier,
    )

    await service.run_once()

    assert notifier.mailbox_ids == [61, 62]
    await service.shutdown()


@pytest.mark.asyncio
async def test_unfenced_recovery_keeps_legacy_key_wake_compatibility() -> None:
    """Only explicitly unfenced recovery debt may call the legacy target."""

    new_key = SessionKey("profile-a", "profile-a:group:new")
    debt_key = SessionKey("profile-a", "profile-a:group:debt")
    new_request = _request(new_key)
    debt_request = _request(debt_key)
    new_debt = _debt(new_request, mailbox_id=71, event_id="recovery:new")
    old_debt = _debt(debt_request, mailbox_id=72, event_id="recovery:debt")
    scanner = _Scanner(
        RecoveryScanSummary(
            results=(
                _result(
                    new_request,
                    RecoveryScanDisposition.DELIVERED,
                    event_id=new_debt.event_id,
                    mailbox_id=71,
                ),
                _result(
                    debt_request,
                    RecoveryScanDisposition.ALREADY_DELIVERED,
                    event_id=old_debt.event_id,
                    mailbox_id=72,
                ),
            )
        ),
        pending_debts=(new_debt, old_debt),
    )
    target = _WakeTarget()
    service = DurableRecoveryScannerService(
        scanner,
        wake_target=target,
        batch_limit=7,
        profile_id="profile-a",
    )

    summary = await service.run_once()

    assert summary is scanner.summary
    assert scanner.calls == [(7, "profile-a")]
    assert scanner.pending_calls == [(64, None, "profile-a")]
    assert target.calls == [new_key, debt_key]
    assert service.health_snapshot().status is RuntimeServiceStatus.RUNNING
    await service.shutdown()


@pytest.mark.asyncio
async def test_unfenced_legacy_failure_remains_retryable_after_target_rebind() -> None:
    """Legacy key wakes retain their existing failure and redrive behavior."""

    key = SessionKey("profile-a", "profile-a:group:legacy-retry")
    request = _request(key)
    debt = _debt(request, mailbox_id=81, event_id="recovery:legacy-retry")
    scanner = _Scanner(
        RecoveryScanSummary(
            results=(
                _result(
                    request,
                    RecoveryScanDisposition.DELIVERED,
                    event_id=debt.event_id,
                    mailbox_id=81,
                ),
            )
        ),
        pending_debts=(debt,),
    )
    failed_target = _WakeTarget(failing_keys={key})
    service = DurableRecoveryScannerService(scanner, wake_target=failed_target)

    with pytest.raises(RecoveryScannerWakeError) as raised:
        await service.run_once()

    assert raised.value.keys == (key,)
    assert failed_target.calls == [key]
    assert service.health_snapshot().status is RuntimeServiceStatus.DEGRADED

    recovered_target = _WakeTarget()
    service.bind_wake_target(recovered_target)
    await service.run_once()

    assert recovered_target.calls == [key]
    assert service.health_snapshot().status is RuntimeServiceStatus.RUNNING
    await service.shutdown()


@pytest.mark.asyncio
async def test_keyset_notification_does_not_skip_debt_after_prior_row_is_removed() -> None:
    """Recovery notification follows immutable mailbox-id pagination."""

    first = _request(
        SessionKey("profile-a", "profile-a:group:keyset-first"),
        fence_id="grant-first",
        fence_generation=1,
    )
    second = _request(
        SessionKey("profile-a", "profile-a:group:keyset-second"),
        fence_id="grant-second",
        fence_generation=1,
    )
    third = _request(
        SessionKey("profile-a", "profile-a:group:keyset-third"),
        fence_id="grant-third",
        fence_generation=1,
    )
    first_debt = _debt(first, mailbox_id=10, event_id="recovery:first")
    second_debt = _debt(second, mailbox_id=20, event_id="recovery:second")
    third_debt = _debt(third, mailbox_id=30, event_id="recovery:third")
    scanner = _Scanner(
        RecoveryScanSummary(results=()),
        pending_debts=(first_debt, second_debt),
    )
    notifier = _MailboxHandoffNotifier()
    service = DurableRecoveryScannerService(
        scanner,
        mailbox_handoff_notifier=notifier,
        wake_limit=1,
    )

    await service.run_once()
    scanner.pending_debts = (second_debt, third_debt)
    await service.run_once()
    await service.run_once()

    assert notifier.mailbox_ids == [10, 20, 30]
    assert scanner.pending_calls == [
        (1, None, None),
        (1, 10, None),
        (1, 20, None),
    ]
    await service.shutdown()


@pytest.mark.asyncio
async def test_fenced_notification_honors_the_service_profile_scope() -> None:
    """One profile supervisor only hints its own exact mailbox handoffs."""

    selected = _request(
        SessionKey("profile-a", "profile-a:group:selected"),
        fence_id="grant-a",
        fence_generation=1,
    )
    other = _request(
        SessionKey("profile-b", "profile-b:group:other"),
        fence_id="grant-b",
        fence_generation=1,
    )
    scanner = _Scanner(
        RecoveryScanSummary(results=()),
        pending_debts=(
            _debt(selected, mailbox_id=91, event_id="recovery:selected"),
            _debt(other, mailbox_id=92, event_id="recovery:other"),
        ),
    )
    notifier = _MailboxHandoffNotifier()
    service = DurableRecoveryScannerService(
        scanner,
        mailbox_handoff_notifier=notifier,
        profile_id="profile-a",
    )

    await service.run_once()

    assert notifier.mailbox_ids == [91]
    assert scanner.pending_calls == [(64, None, "profile-a")]
    await service.shutdown()


@pytest.mark.asyncio
async def test_shutdown_skips_a_queued_legacy_recovery_pass() -> None:
    """A queued caller cannot start another legacy scan after shutdown wins."""

    request = _request(SessionKey("profile-a", "profile-a:group:shutdown"))
    debt = _debt(request, mailbox_id=101, event_id="recovery:shutdown")
    scanner = _Scanner(RecoveryScanSummary(results=()), pending_debts=(debt,))
    target = _BlockingLegacyWakeTarget()
    service = DurableRecoveryScannerService(scanner, wake_target=target)

    running = asyncio.create_task(service.run_once())
    await target.entered.wait()
    queued = asyncio.create_task(service.run_once())
    await asyncio.sleep(0)

    await service.shutdown()
    target.release.set()
    await running
    await queued

    assert scanner.calls == [(64, None)]
    assert target.calls == [request.key]


def test_notifier_must_expose_notify() -> None:
    """The service rejects a target-shaped object at the notifier boundary."""

    scanner = _Scanner(RecoveryScanSummary(results=()))
    with pytest.raises(TypeError, match="mailbox_handoff_notifier"):
        DurableRecoveryScannerService(scanner, mailbox_handoff_notifier=object())

    service = DurableRecoveryScannerService(scanner)
    with pytest.raises(TypeError, match="notifier"):
        service.bind_mailbox_handoff_notifier(object())
