"""Integration coverage for the fail-closed legacy recovery interlock."""

from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.durable_routing_service import DurableRoutingService
from shinbot.core.dispatch.legacy_recovery_gate import (
    LegacyRecoveryGateBlocked,
    LegacyRecoveryGateError,
    LegacyRecoveryGateMode,
)
from shinbot.persistence import DatabaseManager


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for recovery-gate tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def test_fresh_database_starts_legacy_open_then_reservation_is_irreversible(
    tmp_path: Path,
) -> None:
    """The first successful admission reservation closes broad recovery forever."""

    database = _database(tmp_path)
    gate = database.actor_v2_legacy_recovery_gate
    assert gate.snapshot().mode is LegacyRecoveryGateMode.LEGACY_OPEN

    database.actor_v2_admission_fences.reserve(
        SessionKey("profile-a", "session-a"),
        holder_id="canary-a",
        ttl_seconds=30.0,
    )

    assert gate.snapshot().mode is LegacyRecoveryGateMode.FENCED_ONLY
    with pytest.raises(LegacyRecoveryGateBlocked, match="fenced_only"):
        gate.acquire_legacy_recovery(holder_id="legacy-worker")


def test_active_legacy_permit_blocks_reservation_without_opening_a_window(
    tmp_path: Path,
) -> None:
    """Admission and broad recovery have one SQLite-serialized winner."""

    database = _database(tmp_path)
    gate = database.actor_v2_legacy_recovery_gate
    permit = gate.acquire_legacy_recovery(holder_id="legacy-worker")

    with pytest.raises(LegacyRecoveryGateBlocked, match="active legacy recovery"):
        database.actor_v2_admission_fences.reserve(
            SessionKey("profile-a", "session-a"),
            holder_id="canary-a",
            ttl_seconds=30.0,
        )

    assert gate.snapshot().mode is LegacyRecoveryGateMode.LEGACY_RECOVERY_ACTIVE
    gate.release_legacy_recovery(permit)
    database.actor_v2_admission_fences.reserve(
        SessionKey("profile-a", "session-a"),
        holder_id="canary-a",
        ttl_seconds=30.0,
    )
    assert gate.snapshot().mode is LegacyRecoveryGateMode.FENCED_ONLY


def test_restart_with_pre_gate_admission_history_fences_only(tmp_path: Path) -> None:
    """Schema migration never reopens a domain with historical fence evidence."""

    database = _database(tmp_path)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_actor_v2_admission_fences (
                profile_id, session_id, fence_id, generation,
                holder_token_digest, holder_id, status, expires_at,
                created_at, updated_at, committed_at, revoked_at,
                revocation_reason
            ) VALUES (?, ?, ?, 1, ?, ?, 'revoked', ?, ?, ?, NULL, ?, ?)
            """,
            (
                "profile-a",
                "session-a",
                "historical-fence",
                "historical-digest",
                "historical-holder",
                10.0,
                1.0,
                1.0,
                2.0,
                "historical",
            ),
        )

    database.initialize()

    assert (
        database.actor_v2_legacy_recovery_gate.snapshot().mode
        is LegacyRecoveryGateMode.FENCED_ONLY
    )


@pytest.mark.asyncio
async def test_service_never_invokes_broad_legacy_recovery_even_for_a_permit_target(
    tmp_path: Path,
) -> None:
    """A routing pass cannot lend a permit to an actor-owning target."""

    database = _database(tmp_path)
    gate = database.actor_v2_legacy_recovery_gate

    class BroadRecoveryTarget:
        accepting = True

        def __init__(self) -> None:
            self.recover_calls = 0
            self.guarded_recover_calls = 0

        async def wake(self, _key: SessionKey) -> None:
            return None

        async def recover(self) -> int:
            self.recover_calls += 1
            return 0

        async def recover_with_legacy_recovery_permit(
            self,
            _permit: object,
        ) -> int:
            self.guarded_recover_calls += 1
            return 0

    target = BroadRecoveryTarget()

    async def replay(_claim: object, _adapter: object) -> None:
        return None

    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=replay,  # type: ignore[arg-type]
        adapter_resolver=lambda _instance_id: None,
        actor_wake_target=target,
    )
    try:
        await service._recover_wake_debt(force=True)
        assert target.recover_calls == 0
        assert target.guarded_recover_calls == 0
        assert gate.snapshot().mode is LegacyRecoveryGateMode.LEGACY_OPEN
        assert service.health_snapshot().consecutive_failures == 0
    finally:
        await service.shutdown()


def test_missing_singleton_is_fail_closed(tmp_path: Path) -> None:
    """A missing durable singleton is never interpreted as a fresh gate."""

    database = _database(tmp_path)
    with database.connect() as conn:
        conn.execute(
            "DROP TRIGGER trg_agent_runtime_legacy_recovery_gate_delete_forbidden"
        )
        conn.execute("DELETE FROM agent_runtime_legacy_recovery_gate")

    with pytest.raises(LegacyRecoveryGateError, match="singleton is missing"):
        database.actor_v2_legacy_recovery_gate.snapshot()
