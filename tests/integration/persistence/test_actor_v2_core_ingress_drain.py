"""Integration coverage for the barrier-bound Actor v2 core-ingress drain."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainCoverageError,
    ActorV2CoreIngressDrainNotReady,
    ActorV2CoreIngressDrainReceipt,
    ActorV2CoreIngressDrainStatus,
)
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressDrainConflict,
    ActorV2IngressStopProof,
)
from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierConflict,
    ActorV2MigrationBarrierGrant,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainRepository,
)
from shinbot.persistence.repositories.actor_v2_ingress_drain import (
    ActorV2IngressDrainRepository,
)
from shinbot.persistence.repositories.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierRepository,
)
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for core-ingress drain tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _digest(value: str) -> str:
    """Build deterministic token-free test evidence."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _repositories(
    database: DatabaseManager,
    now: list[float],
) -> tuple[
    AgentRuntimeOwnershipRepository,
    ActorV2MigrationBarrierRepository,
    ActorV2IngressDrainRepository,
    ActorV2CoreIngressDrainRepository,
]:
    """Install deterministic repositories over the shared SQLite domain."""

    member_ids = iter(("member-a", "member-b", "member-late"))
    participant_tokens = iter(
        (
            "participant-token-a-secret",
            "participant-token-b-secret",
            "participant-token-late-secret",
        )
    )
    ownership = AgentRuntimeOwnershipRepository(database, clock=lambda: now[0])
    barrier = ActorV2MigrationBarrierRepository(
        database,
        clock=lambda: now[0],
        barrier_id_factory=lambda: "migration-barrier-a",
        holder_token_factory=lambda: "migration-holder-token-secret",
    )
    ingress = ActorV2IngressDrainRepository(
        database,
        clock=lambda: now[0],
        member_id_factory=lambda: next(member_ids),
        request_id_factory=lambda: "adapter-drain-request-unused",
        holder_token_factory=lambda: next(participant_tokens),
    )
    core = ActorV2CoreIngressDrainRepository(
        database,
        clock=lambda: now[0],
        request_id_factory=lambda: "core-drain-request-a",
    )
    database.agent_runtime_ownership = ownership
    database.actor_v2_migration_barriers = barrier
    database.actor_v2_ingress_drains = ingress
    database.actor_v2_core_ingress_drains = core
    return ownership, barrier, ingress, core


def _receipt(member_id: str) -> ActorV2CoreIngressDrainReceipt:
    """Build one exact local core and legacy proof pair."""

    return ActorV2CoreIngressDrainReceipt(
        core_ingress_digest=_digest(f"core-ingress:{member_id}"),
        legacy_quiescence_digest=_digest(f"legacy-quiescence:{member_id}"),
        proof_epoch=1,
        summary_code="process.local_quiescent",
    )


def _start_barrier(
    ownership: AgentRuntimeOwnershipRepository,
    barrier: ActorV2MigrationBarrierRepository,
) -> tuple[SessionKey, ActorV2MigrationBarrierGrant]:
    """Create one active legacy source and its holder-fenced migration barrier."""

    key = SessionKey("profile-a", "profile-a:group:room")
    source = ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy source for core ingress drain",
        legacy_session_id="legacy-session-a",
        requested_by="test",
    ).ownership
    return (
        key,
        barrier.start_legacy_to_actor_v2(
            key,
            expected_generation=source.generation,
            adapter_instance_ids=("adapter-a", "adapter-b"),
            holder_id="cutover-controller-a",
            reason="begin barrier-bound core ingress drain",
        ),
    )


def test_core_drain_requires_full_coverage_before_sealing(tmp_path: Path) -> None:
    """Every barrier adapter needs a live registered process member."""

    now = [100.0]
    database = _database(tmp_path)
    ownership, barrier, ingress, core = _repositories(database, now)
    ingress.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )
    _key, barrier_grant = _start_barrier(ownership, barrier)

    with pytest.raises(ActorV2CoreIngressDrainCoverageError) as missing_coverage:
        core.begin_drain(barrier_grant)

    assert missing_coverage.value.adapter_instance_ids == ("adapter-b",)


def test_core_drain_seals_membership_and_rejects_unacknowledged_escape(
    tmp_path: Path,
) -> None:
    """A core snapshot cannot lose a member by replacement, retire, or revocation."""

    now = [100.0]
    database = _database(tmp_path)
    ownership, barrier, ingress, core = _repositories(database, now)
    grant_a = ingress.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )
    grant_b = ingress.register_participant(
        adapter_instance_id="adapter-b",
        participant_id="process-b:incarnation-b",
        participant_epoch=1,
    )
    _key, barrier_grant = _start_barrier(ownership, barrier)
    request = core.begin_drain(barrier_grant)

    assert request.status is ActorV2CoreIngressDrainStatus.OPEN
    assert tuple(member.member_id for member in request.members) == ("member-a", "member-b")
    with pytest.raises(ActorV2IngressDrainConflict, match="core request"):
        ingress.register_participant(
            adapter_instance_id="adapter-a",
            participant_id="process-late:incarnation-late",
            participant_epoch=1,
        )
    with pytest.raises(ActorV2IngressDrainConflict, match="core drain request"):
        ingress.retire(grant_a)
    with pytest.raises(ActorV2IngressDrainConflict, match="core drain request"):
        ingress.revoke_with_stop_proof(
            grant_b.participant,
            stop_proof=ActorV2IngressStopProof(
                issuer_id="orchestrator-a",
                proof_epoch=1,
                digest=_digest("external-stop-proof"),
                summary_code="external.process_stopped",
            ),
        )
    with pytest.raises(ActorV2CoreIngressDrainNotReady, match="unacknowledged"):
        core.confirm_drained(
            request_id=request.request_id,
            barrier_grant=barrier_grant,
        )
    with pytest.raises(ActorV2MigrationBarrierConflict, match="core ingress drain request"):
        barrier.abort(
            barrier_grant,
            reason="ownership-only abort cannot release a core snapshot",
        )


def test_core_receipts_are_immutable_and_produce_journal_safe_digests(
    tmp_path: Path,
) -> None:
    """Every member acknowledgement is token-free and required for terminal proof."""

    now = [100.0]
    database = _database(tmp_path)
    ownership, barrier, ingress, core = _repositories(database, now)
    grant_a = ingress.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )
    grant_b = ingress.register_participant(
        adapter_instance_id="adapter-b",
        participant_id="process-b:incarnation-b",
        participant_epoch=1,
    )
    _key, barrier_grant = _start_barrier(ownership, barrier)
    request = core.begin_drain(barrier_grant)

    acknowledgement_a = core.acknowledge_quiescent(
        request_id=request.request_id,
        participant_grant=grant_a,
        receipt=_receipt("member-a"),
    )
    assert acknowledgement_a == core.acknowledge_quiescent(
        request_id=request.request_id,
        participant_grant=grant_a,
        receipt=_receipt("member-a"),
    )
    core.acknowledge_quiescent(
        request_id=request.request_id,
        participant_grant=grant_b,
        receipt=_receipt("member-b"),
    )
    now[0] = 101.0
    drained = core.confirm_drained(
        request_id=request.request_id,
        barrier_grant=barrier_grant,
    )

    assert drained.status is ActorV2CoreIngressDrainStatus.DRAINED
    assert drained.durably_drained
    assert drained.core_ingress_proof_digest() != drained.legacy_quiescence_proof_digest()
    with database.connect() as conn:
        persisted = conn.execute(
            """
            SELECT core_ingress_digest, legacy_quiescence_digest, summary_code
            FROM agent_session_actor_v2_core_ingress_drain_acknowledgements
            """
        ).fetchall()
        serialized = str([dict(row) for row in persisted])
        assert "participant-token-a-secret" not in serialized
        assert "participant-token-b-secret" not in serialized
        with pytest.raises(sqlite3.IntegrityError, match="acknowledgement is immutable"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_core_ingress_drain_acknowledgements
                SET summary_code = 'tampered'
                WHERE request_id = ? AND member_id = ?
                """,
                (request.request_id, "member-a"),
            )
    database.initialize()
    assert core.get(request.request_id) == drained
