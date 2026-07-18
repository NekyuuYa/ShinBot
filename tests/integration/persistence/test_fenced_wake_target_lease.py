"""Integration coverage for durable fenced Actor wake-target publication."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from shinbot.core.dispatch.actor_v2_admission import (
    ActorV2AdmissionFenceError,
    ActorV2AdmissionFenceExpired,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipNotFound,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedWakeTargetLeaseConflict,
    FencedWakeTargetLeaseExpired,
    FencedWakeTargetLeaseLost,
    FencedWakeTargetLeaseStatus,
)
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_admission_fence import (
    ActorV2AdmissionFenceRepository,
)
from shinbot.persistence.repositories.actor_v2_fenced_wake_target_lease import (
    ActorV2FencedWakeTargetLeaseRepository,
)
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for wake-target lease tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _clocked_repositories(
    database: DatabaseManager,
    now: list[float],
) -> tuple[
    ActorV2AdmissionFenceRepository,
    AgentRuntimeOwnershipRepository,
    ActorV2FencedWakeTargetLeaseRepository,
]:
    """Install repositories that share one deterministic clock and fence view."""

    admission = ActorV2AdmissionFenceRepository(
        database,
        clock=lambda: now[0],
        fence_id_factory=lambda: "admission-fence-a",
        holder_token_factory=lambda: "admission-holder-token-a",
    )
    ownership = AgentRuntimeOwnershipRepository(database, clock=lambda: now[0])
    leases = ActorV2FencedWakeTargetLeaseRepository(
        database,
        clock=lambda: now[0],
        holder_token_factory=lambda: "wake-target-token-a",
    )
    database.actor_v2_admission_fences = admission
    database.agent_runtime_ownership = ownership
    database.actor_v2_fenced_wake_target_leases = leases
    return admission, ownership, leases


def _committed_owner(
    tmp_path: Path,
    now: list[float],
    *,
    admission_ttl: float = 60.0,
) -> tuple[
    DatabaseManager,
    ActorV2AdmissionFenceRepository,
    ActorV2FencedWakeTargetLeaseRepository,
    object,
    FencedMailboxWakeRequest,
]:
    """Create one committed Actor v2 owner and its exact fenced wake request."""

    database = _database(tmp_path)
    admission, ownership_repository, leases = _clocked_repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    admission_grant = admission.reserve(
        key,
        holder_id="wake-target-test",
        ttl_seconds=admission_ttl,
    )
    ownership = ownership_repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="wake target lease test owner",
        admission_grant=admission_grant,
    ).ownership
    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership.generation,
        admission_fence_id=ownership.admission_fence_id,
        admission_fence_generation=ownership.admission_fence_generation,
    )
    return database, admission, leases, admission_grant, request


def test_fresh_schema_installs_the_wake_target_lease_contract(tmp_path: Path) -> None:
    """A new database exposes the durable table, index, and repository accessor."""

    database = _database(tmp_path)

    assert isinstance(
        database.actor_v2_fenced_wake_target_leases,
        ActorV2FencedWakeTargetLeaseRepository,
    )
    with database.connect() as conn:
        table = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'agent_session_actor_v2_fenced_wake_target_leases'
            """
        ).fetchone()
        index = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'index'
              AND name = 'idx_actor_v2_fenced_wake_target_leases_expiry'
            """
        ).fetchone()

    assert table is not None
    assert index is not None


def test_acquire_requires_a_committed_fenced_actor_owner(tmp_path: Path) -> None:
    """A reserved fence cannot publish a target before ownership commits."""

    now = [10.0]
    database = _database(tmp_path)
    admission, ownership_repository, leases = _clocked_repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    admission_grant = admission.reserve(
        key,
        holder_id="wake-target-test",
        ttl_seconds=60.0,
    )
    reserved_request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=1,
        admission_fence_id=admission_grant.fence.fence_id,
        admission_fence_generation=admission_grant.fence.generation,
    )

    with pytest.raises(AgentRuntimeOwnershipNotFound):
        leases.acquire(
            reserved_request,
            target=MailboxHandoffTarget("target-a", "incarnation-a"),
            ttl_seconds=20.0,
        )

    ownership = ownership_repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="commit owner before publication",
        admission_grant=admission_grant,
    ).ownership
    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership.generation,
        admission_fence_id=ownership.admission_fence_id,
        admission_fence_generation=ownership.admission_fence_generation,
    )
    grant = leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
        ttl_seconds=20.0,
    )

    assert grant.lease.request == request
    assert grant.lease.lease_epoch == 1
    assert leases.get(request) == grant.lease
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT holder_token_digest
            FROM agent_session_actor_v2_fenced_wake_target_leases
            """
        ).fetchone()
    assert row is not None
    assert row["holder_token_digest"] != grant.holder_token


def test_active_target_is_exclusive_and_reads_require_the_exact_request(
    tmp_path: Path,
) -> None:
    """A key-only lookup cannot observe or replace another target incarnation."""

    now = [10.0]
    _database, _admission, leases, _admission_grant, request = _committed_owner(
        tmp_path,
        now,
    )
    initial = leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
        ttl_seconds=20.0,
    )

    with pytest.raises(FencedWakeTargetLeaseConflict, match="active wake target"):
        leases.acquire(
            request,
            target=MailboxHandoffTarget("target-b", "incarnation-b"),
            ttl_seconds=20.0,
        )
    different_request = FencedMailboxWakeRequest(
        key=request.key,
        ownership_generation=request.ownership_generation,
        admission_fence_id="other-admission-fence",
        admission_fence_generation=1,
    )
    with pytest.raises(FencedWakeTargetLeaseConflict, match="another ownership"):
        leases.get(different_request)

    assert leases.validate(initial) == initial.lease


def test_renew_preserves_the_target_incarnation_and_lease_epoch(tmp_path: Path) -> None:
    """Renewal can extend only the original holder's durable publication."""

    now = [10.0]
    database, _admission, leases, _admission_grant, request = _committed_owner(
        tmp_path,
        now,
    )
    initial = leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
        ttl_seconds=5.0,
    )

    now[0] = 12.0
    renewed = leases.renew(initial, ttl_seconds=20.0)

    assert renewed.holder_token == initial.holder_token
    assert renewed.lease.target == initial.lease.target
    assert renewed.lease.lease_epoch == initial.lease.lease_epoch == 1
    assert renewed.lease.created_at == initial.lease.created_at == 10.0
    assert renewed.lease.updated_at == 12.0
    assert renewed.lease.expires_at == 32.0
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        assert leases.validate_in_transaction(conn, initial) == renewed.lease


def test_expired_or_released_target_requires_a_new_incarnation(tmp_path: Path) -> None:
    """A prior process cannot resume with its former durable target identity."""

    now = [10.0]
    _database, _admission, leases, _admission_grant, request = _committed_owner(
        tmp_path,
        now,
    )
    first = leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
        ttl_seconds=5.0,
    )

    now[0] = 15.0
    with pytest.raises(FencedWakeTargetLeaseExpired):
        leases.renew(first, ttl_seconds=10.0)
    second = leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-b"),
        ttl_seconds=10.0,
    )

    assert second.lease.lease_epoch == 2
    with pytest.raises(FencedWakeTargetLeaseLost):
        leases.renew(first, ttl_seconds=10.0)
    with pytest.raises(FencedWakeTargetLeaseLost):
        leases.release(first)

    released = leases.release(second)
    assert released.status is FencedWakeTargetLeaseStatus.RELEASED
    with pytest.raises(FencedWakeTargetLeaseConflict, match="new incarnation"):
        leases.acquire(
            request,
            target=MailboxHandoffTarget("target-a", "incarnation-b"),
            ttl_seconds=10.0,
        )
    third = leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-c"),
        ttl_seconds=10.0,
    )

    assert third.lease.lease_epoch == 3
    with pytest.raises(FencedWakeTargetLeaseLost):
        leases.release(second)


def test_schema_rejects_owner_rewrites_epoch_retargeting_and_history_deletion(
    tmp_path: Path,
) -> None:
    """Raw SQL cannot bypass the durable target-publication lifecycle contract."""

    now = [10.0]
    database, _admission, leases, _admission_grant, request = _committed_owner(
        tmp_path,
        now,
    )
    leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
        ttl_seconds=20.0,
    )

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="owner identity"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_fenced_wake_target_leases
                SET admission_fence_id = 'other-fence'
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="lifecycle transition"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_fenced_wake_target_leases
                SET target_incarnation_id = 'other-incarnation'
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
            conn.execute("DELETE FROM agent_session_actor_v2_fenced_wake_target_leases")


def test_admission_revocation_blocks_use_but_allows_target_cleanup(tmp_path: Path) -> None:
    """A revoked owner cannot wake or renew, while its process can unpublish."""

    now = [10.0]
    _database, admission, leases, admission_grant, request = _committed_owner(tmp_path, now)
    grant = leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
        ttl_seconds=30.0,
    )
    admission.revoke(admission_grant, reason="target process lost ownership")

    with pytest.raises(ActorV2AdmissionFenceError):
        leases.validate(grant)
    with pytest.raises(ActorV2AdmissionFenceError):
        leases.renew(grant, ttl_seconds=20.0)

    released = leases.release(grant)
    assert released.status is FencedWakeTargetLeaseStatus.RELEASED


def test_admission_expiry_blocks_validation_and_renewal(tmp_path: Path) -> None:
    """Target liveness never extends a committed admission fence."""

    now = [10.0]
    _database, _admission, leases, _admission_grant, request = _committed_owner(
        tmp_path,
        now,
        admission_ttl=20.0,
    )
    grant = leases.acquire(
        request,
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
        ttl_seconds=100.0,
    )

    now[0] = 31.0
    with pytest.raises(ActorV2AdmissionFenceExpired):
        leases.validate(grant)
    with pytest.raises(ActorV2AdmissionFenceExpired):
        leases.renew(grant, ttl_seconds=20.0)


def test_unrepresentable_future_expiry_fails_before_publishing(tmp_path: Path) -> None:
    """A huge wall-clock value cannot create a lease that is already expired."""

    now = [10.0]
    database, _admission, _leases, _admission_grant, request = _committed_owner(
        tmp_path,
        now,
    )
    overflowing_clock = ActorV2FencedWakeTargetLeaseRepository(
        database,
        clock=lambda: 1e308,
        holder_token_factory=lambda: "overflow-token",
    )

    with pytest.raises(ValueError, match="future lease expiry"):
        overflowing_clock.acquire(
            request,
            target=MailboxHandoffTarget("target-a", "incarnation-a"),
            ttl_seconds=10.0,
        )
