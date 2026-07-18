"""Integration coverage for the dormant exact mailbox handoff dispatcher."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.mailbox_handoff_dispatcher import (
    DurableMailboxHandoffDispatcher,
    MailboxHandoffDispatchDisposition,
)
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffClaim,
    FencedMailboxHandoffReceipt,
    MailboxHandoffState,
    MailboxHandoffTarget,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_mailbox_handoff import (
    ActorV2MailboxHandoffRepository,
)


class _Target:
    """Return one typed receipt for every exact handoff claim."""

    def __init__(self, disposition: FencedMailboxWakeDisposition) -> None:
        self.disposition = disposition
        self.claims: list[FencedMailboxHandoffClaim] = []

    async def wake_handoff(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        self.claims.append(claim)
        return FencedMailboxHandoffReceipt(
            claim=claim,
            wake_receipt=FencedMailboxWakeReceipt(
                request=claim.request,
                disposition=self.disposition,
            ),
        )


class _MismatchedReceiptTarget:
    """Return another claim with the same request to prove exact checking."""

    async def wake_handoff(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        mismatched = replace(
            claim,
            target=MailboxHandoffTarget("other-target", "other-incarnation"),
        )
        return FencedMailboxHandoffReceipt(
            claim=mismatched,
            wake_receipt=FencedMailboxWakeReceipt(
                request=claim.request,
                disposition=FencedMailboxWakeDisposition.ACCEPTED,
            ),
        )


class _BlockingTarget:
    """Wait until a test changes its target binding."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def wake_handoff(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        self.entered.set()
        await self.release.wait()
        return FencedMailboxHandoffReceipt(
            claim=claim,
            wake_receipt=FencedMailboxWakeReceipt(
                request=claim.request,
                disposition=FencedMailboxWakeDisposition.ACCEPTED,
            ),
        )


class _LeaseExpiringDeferredTarget:
    """Advance a deterministic claim clock before returning a deferred receipt."""

    def __init__(self, now: list[float], *, expires_at: float) -> None:
        self._now = now
        self._expires_at = expires_at

    async def wake_handoff(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        self._now[0] = self._expires_at
        return FencedMailboxHandoffReceipt(
            claim=claim,
            wake_receipt=FencedMailboxWakeReceipt(
                request=claim.request,
                disposition=FencedMailboxWakeDisposition.DEFERRED,
            ),
        )


class _KeyOnlyWakeTarget:
    """Represent the legacy registry shape that lacks handoff identity."""

    async def wake(self, _key: SessionKey) -> None:
        """Accept only a session key, which is insufficient for fenced work."""


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable database."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _fenced_mailbox(
    database: DatabaseManager,
    *,
    event_id: str = "handoff-dispatch-event",
    record_handoff: bool = True,
) -> tuple[int, FencedMailboxWakeRequest]:
    """Create a current fenced owner and optionally its pending sidecar event."""

    key = SessionKey("profile-a", "profile-a:group:room")
    grant: ActorV2AdmissionGrant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="handoff-dispatch-test",
        ttl_seconds=300.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="handoff dispatcher test owner",
        admission_grant=grant,
    ).ownership
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 1.0, 1.0)
            """,
            (key.profile_id, key.session_id, ownership.generation),
        )
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, occurred_at, available_at, created_at
            ) VALUES (?, ?, ?, ?, 'MessageReceived', 1.0, 1.0, 1.0)
            """,
            (event_id, key.profile_id, key.session_id, ownership.generation),
        )
    mailbox_id = int(inserted.lastrowid)
    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership.generation,
        admission_fence_id=ownership.admission_fence_id,
        admission_fence_generation=ownership.admission_fence_generation,
    )
    if record_handoff:
        database.actor_v2_mailbox_handoffs.record_fenced_handoff(mailbox_id, request)
    return mailbox_id, request


@pytest.mark.asyncio
async def test_unbound_dispatcher_leaves_fenced_handoff_pending(tmp_path: Path) -> None:
    """No implicit target or legacy wake occurs before an explicit bind."""

    database = _database(tmp_path)
    mailbox_id, _request = _fenced_mailbox(database)
    dispatcher = DurableMailboxHandoffDispatcher(database.actor_v2_mailbox_handoffs)

    result = await dispatcher.dispatch(mailbox_id)

    assert result.disposition is MailboxHandoffDispatchDisposition.DEFERRED
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.state is MailboxHandoffState.PENDING


def test_dispatcher_rejects_a_key_only_wake_target(tmp_path: Path) -> None:
    """A SessionKey-only registry cannot receive a fenced mailbox handoff."""

    dispatcher = DurableMailboxHandoffDispatcher(_database(tmp_path).actor_v2_mailbox_handoffs)

    with pytest.raises(TypeError, match="FencedMailboxHandoffPort"):
        dispatcher.bind_target(
            _KeyOnlyWakeTarget(),
            target_identity=MailboxHandoffTarget("target-a", "incarnation-a"),
        )


@pytest.mark.asyncio
async def test_dispatcher_settles_accepted_claim_with_full_target_identity(
    tmp_path: Path,
) -> None:
    """An accepted receipt settles only the exact lease-bound sidecar."""

    database = _database(tmp_path)
    mailbox_id, _request = _fenced_mailbox(database)
    target = _Target(FencedMailboxWakeDisposition.ACCEPTED)
    identity = MailboxHandoffTarget("target-a", "incarnation-a")
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        worker_id="dispatcher-a",
    )
    dispatcher.bind_target(target, target_identity=identity)

    result = await dispatcher.dispatch(mailbox_id)

    assert result.disposition is MailboxHandoffDispatchDisposition.ACCEPTED
    assert len(target.claims) == 1
    assert target.claims[0].identity.mailbox_id == mailbox_id
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.state is MailboxHandoffState.SETTLED
    assert record.target == identity
    assert record.target_disposition == FencedMailboxWakeDisposition.ACCEPTED.value


@pytest.mark.asyncio
async def test_dispatcher_settles_stale_claim_without_key_fallback(tmp_path: Path) -> None:
    """A target-declared stale receipt is terminal for that exact handoff."""

    database = _database(tmp_path)
    mailbox_id, _request = _fenced_mailbox(database)
    dispatcher = DurableMailboxHandoffDispatcher(database.actor_v2_mailbox_handoffs)
    dispatcher.bind_target(
        _Target(FencedMailboxWakeDisposition.STALE),
        target_identity=MailboxHandoffTarget("target-a", "incarnation-a"),
    )

    result = await dispatcher.dispatch(mailbox_id)

    assert result.disposition is MailboxHandoffDispatchDisposition.STALE
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.state is MailboxHandoffState.SETTLED
    assert record.target_disposition == FencedMailboxWakeDisposition.STALE.value


@pytest.mark.asyncio
async def test_dispatcher_releases_deferred_claim_without_terminal_settlement(
    tmp_path: Path,
) -> None:
    """A temporarily unavailable target leaves durable work for a new incarnation."""

    database = _database(tmp_path)
    mailbox_id, _request = _fenced_mailbox(database)
    dispatcher = DurableMailboxHandoffDispatcher(database.actor_v2_mailbox_handoffs)
    dispatcher.bind_target(
        _Target(FencedMailboxWakeDisposition.DEFERRED),
        target_identity=MailboxHandoffTarget("target-a", "incarnation-a"),
    )

    result = await dispatcher.dispatch(mailbox_id)

    assert result.disposition is MailboxHandoffDispatchDisposition.DEFERRED
    assert result.error == "target deferred; claim released"
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.state is MailboxHandoffState.PENDING
    assert record.target is None
    assert record.target_disposition == ""
    assert record.last_error == "target deferred fenced mailbox handoff"


@pytest.mark.asyncio
async def test_expired_deferred_claim_remains_redeliverable(tmp_path: Path) -> None:
    """A receipt that arrives after expiry cannot settle or erase durable debt."""

    now = [100.0]
    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database, record_handoff=False)
    repository = ActorV2MailboxHandoffRepository(
        database,
        clock=lambda: now[0],
        lease_seconds=5.0,
    )
    repository.record_fenced_handoff(mailbox_id, request)
    dispatcher = DurableMailboxHandoffDispatcher(repository)
    dispatcher.bind_target(
        _LeaseExpiringDeferredTarget(now, expires_at=105.0),
        target_identity=MailboxHandoffTarget("target-a", "incarnation-a"),
    )

    result = await dispatcher.dispatch(mailbox_id)

    assert result.disposition is MailboxHandoffDispatchDisposition.FAILED
    assert "lease is expired" in result.error
    expired = repository.read(mailbox_id)
    assert expired is not None
    assert expired.state is MailboxHandoffState.CLAIMED
    assert expired.target_disposition == ""

    now[0] = 106.0
    redelivered = repository.claim_fenced_handoff(
        mailbox_id,
        worker_id="dispatcher-b",
        target=MailboxHandoffTarget("target-b", "incarnation-b"),
    )
    assert redelivered is not None
    assert redelivered.attempt_count == 2
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_rejects_receipt_for_another_claim(tmp_path: Path) -> None:
    """Matching ownership requests cannot substitute another handoff claim."""

    database = _database(tmp_path)
    mailbox_id, _request = _fenced_mailbox(database)
    dispatcher = DurableMailboxHandoffDispatcher(database.actor_v2_mailbox_handoffs)
    dispatcher.bind_target(
        _MismatchedReceiptTarget(),
        target_identity=MailboxHandoffTarget("target-a", "incarnation-a"),
    )

    result = await dispatcher.dispatch(mailbox_id)

    assert result.disposition is MailboxHandoffDispatchDisposition.FAILED
    assert "does not match" in result.error
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.state is MailboxHandoffState.CLAIMED


@pytest.mark.asyncio
async def test_rebinding_target_discards_late_receipt_and_retains_lease(tmp_path: Path) -> None:
    """A prior target cannot settle work after its incarnation is replaced."""

    database = _database(tmp_path)
    mailbox_id, _request = _fenced_mailbox(database)
    blocking = _BlockingTarget()
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        target_timeout_seconds=1.0,
    )
    dispatcher.bind_target(
        blocking,
        target_identity=MailboxHandoffTarget("target-a", "incarnation-a"),
    )
    pending = asyncio.create_task(dispatcher.dispatch(mailbox_id))
    await asyncio.wait_for(blocking.entered.wait(), timeout=0.5)
    dispatcher.bind_target(
        _Target(FencedMailboxWakeDisposition.ACCEPTED),
        target_identity=MailboxHandoffTarget("target-b", "incarnation-b"),
    )
    blocking.release.set()

    result = await pending

    assert result.disposition is MailboxHandoffDispatchDisposition.DEFERRED
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.state is MailboxHandoffState.CLAIMED


@pytest.mark.asyncio
async def test_pull_dispatch_uses_mailbox_keyset_without_merging_sessions(
    tmp_path: Path,
) -> None:
    """A page preserves separately durable events for one SessionKey."""

    database = _database(tmp_path)
    first_id, request = _fenced_mailbox(database, event_id="dispatch-page-a")
    second_id: int
    with database.connect() as conn:
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, occurred_at, available_at, created_at
            ) VALUES ('dispatch-page-b', ?, ?, ?, 'MessageReceived', 1.0, 1.0, 1.0)
            """,
            (
                request.key.profile_id,
                request.key.session_id,
                request.ownership_generation,
            ),
        )
        second_id = int(inserted.lastrowid)
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(second_id, request)
    dispatcher = DurableMailboxHandoffDispatcher(database.actor_v2_mailbox_handoffs)
    dispatcher.bind_target(
        _Target(FencedMailboxWakeDisposition.ACCEPTED),
        target_identity=MailboxHandoffTarget("target-a", "incarnation-a"),
    )

    first_page = await dispatcher.dispatch_pending(limit=1)
    assert [result.mailbox_id for result in first_page.results] == [first_id]
    assert [result.disposition for result in first_page.results] == [
        MailboxHandoffDispatchDisposition.ACCEPTED
    ]
    assert first_page.has_more is True
    assert first_page.next_cursor is not None

    second_page = await dispatcher.dispatch_pending(
        limit=1,
        after=first_page.next_cursor,
    )
    assert [result.mailbox_id for result in second_page.results] == [second_id]
    assert [result.disposition for result in second_page.results] == [
        MailboxHandoffDispatchDisposition.ACCEPTED
    ]
    assert second_page.has_more is False


def test_dispatcher_hints_are_bounded_and_validate_mailbox_ids(tmp_path: Path) -> None:
    """Hints are advisory LRU state and never replace durable discovery."""

    dispatcher = DurableMailboxHandoffDispatcher(
        _database(tmp_path).actor_v2_mailbox_handoffs,
        hint_capacity=2,
    )
    dispatcher.notify(1)
    dispatcher.notify(2)
    dispatcher.notify(3)

    assert dispatcher.drain_hints(limit=1) == (2,)
    assert dispatcher.drain_hints() == (3,)
    with pytest.raises(ValueError, match="positive"):
        dispatcher.notify(0)
    with pytest.raises(ValueError, match="at least one"):
        dispatcher.drain_hints(limit=0)
