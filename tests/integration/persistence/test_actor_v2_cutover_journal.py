"""Integration coverage for the dormant Actor v2 production cutover journal.

The journal binds existing durable primitives into an auditable forward path,
but it must never start an Actor runtime or become an ingress controller.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.clean_session_activation import (
    SQLiteCleanSessionActivationPreflight,
)
from shinbot.core.dispatch.actor_v2_admission import (
    ActorV2AdmissionFenceStatus,
    ActorV2AdmissionGrant,
)
from shinbot.core.dispatch.actor_v2_cutover import (
    ACTOR_V2_CUTOVER_FORWARD_PHASES,
    ActorV2CutoverEvidence,
    ActorV2CutoverEvidenceBundle,
    ActorV2CutoverJournalConflict,
    ActorV2CutoverPhase,
    ActorV2CutoverProofKind,
    ActorV2CutoverRecord,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnership,
    AgentRuntimeOwnershipMode,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedWakeTargetLeaseGrant,
    FencedWakeTargetLeaseLost,
)
from shinbot.core.dispatch.legacy_recovery_gate import (
    LegacyRecoveryGateBlocked,
    LegacyRecoveryGateMode,
)
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_admission_fence import (
    ActorV2AdmissionFenceRepository,
)
from shinbot.persistence.repositories.actor_v2_cutover_journal import (
    ActorV2CutoverJournalRepository,
)
from shinbot.persistence.repositories.actor_v2_fenced_wake_target_lease import (
    ActorV2FencedWakeTargetLeaseRepository,
)
from shinbot.persistence.repositories.actor_v2_legacy_recovery_gate import (
    ActorV2LegacyRecoveryGateRepository,
)
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)

_PROOF_KINDS_BY_PHASE: dict[
    ActorV2CutoverPhase, tuple[ActorV2CutoverProofKind, ...]
] = {
    ActorV2CutoverPhase.PREFLIGHTED: (
        ActorV2CutoverProofKind.CLEAN_PREFLIGHT,
    ),
    ActorV2CutoverPhase.ADMISSION_RESERVED: (
        ActorV2CutoverProofKind.ADMISSION_RESERVATION,
    ),
    ActorV2CutoverPhase.LEGACY_QUIESCED: (
        ActorV2CutoverProofKind.LEGACY_QUIESCENCE,
        ActorV2CutoverProofKind.ADAPTER_PAUSE_DRAIN,
    ),
    ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED: (
        ActorV2CutoverProofKind.ACTOR_OWNER_COMMIT,
    ),
    ActorV2CutoverPhase.TARGET_PUBLISHED: (
        ActorV2CutoverProofKind.TARGET_PUBLICATION,
    ),
    ActorV2CutoverPhase.INGRESS_RESUMED: (
        ActorV2CutoverProofKind.INGRESS_RESUME,
    ),
    ActorV2CutoverPhase.BLOCKED: (ActorV2CutoverProofKind.BLOCKED,),
}


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized persistence domain for cutover-journal tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _bundle(
    phase: ActorV2CutoverPhase,
    *,
    proof_secret: str,
) -> ActorV2CutoverEvidenceBundle:
    """Build token-free evidence from an external proof value."""

    evidence = tuple(
        ActorV2CutoverEvidence(
            kind=kind,
            issuer_id=f"cutover-proof-{kind.value}",
            proof_epoch=1,
            digest=hashlib.sha256(
                f"{proof_secret}:{kind.value}".encode()
            ).hexdigest(),
            summary_code=f"proof.{kind.value}",
        )
        for kind in _PROOF_KINDS_BY_PHASE[phase]
    )
    return ActorV2CutoverEvidenceBundle(phase=phase, evidence=evidence)


def _core_ingress_bundle() -> ActorV2CutoverEvidenceBundle:
    """Build the normalized-core-ingress alternative for legacy quiescence."""

    return ActorV2CutoverEvidenceBundle(
        phase=ActorV2CutoverPhase.LEGACY_QUIESCED,
        evidence=(
            ActorV2CutoverEvidence(
                kind=ActorV2CutoverProofKind.CORE_INGRESS_DRAIN,
                issuer_id="core-ingress-drain-worker",
                proof_epoch=1,
                digest=hashlib.sha256(b"core-ingress-drain-proof").hexdigest(),
                summary_code="proof.core_ingress_drain",
            ),
            ActorV2CutoverEvidence(
                kind=ActorV2CutoverProofKind.LEGACY_QUIESCENCE,
                issuer_id="core-ingress-drain-worker",
                proof_epoch=1,
                digest=hashlib.sha256(b"core-legacy-quiescence-proof").hexdigest(),
                summary_code="proof.legacy_quiescence",
            ),
        ),
    )


def _repositories(
    database: DatabaseManager,
    now: list[float],
) -> tuple[
    ActorV2CutoverJournalRepository,
    ActorV2AdmissionFenceRepository,
    AgentRuntimeOwnershipRepository,
    ActorV2FencedWakeTargetLeaseRepository,
]:
    """Install deterministic durable primitives sharing one clock."""

    admission = ActorV2AdmissionFenceRepository(
        database,
        clock=lambda: now[0],
        fence_id_factory=lambda: "admission-fence-a",
        holder_token_factory=lambda: "admission-holder-token-secret",
    )
    ownership = AgentRuntimeOwnershipRepository(database, clock=lambda: now[0])
    target_tokens = iter(("target-holder-token-a", "target-holder-token-b"))
    leases = ActorV2FencedWakeTargetLeaseRepository(
        database,
        clock=lambda: now[0],
        holder_token_factory=lambda: next(target_tokens),
    )
    journal = ActorV2CutoverJournalRepository(
        database,
        clock=lambda: now[0],
        cutover_id_factory=lambda: "cutover-a",
    )
    database.actor_v2_legacy_recovery_gate = ActorV2LegacyRecoveryGateRepository(
        database,
        clock=lambda: now[0],
        holder_token_factory=lambda: "legacy-recovery-token",
    )
    database.actor_v2_admission_fences = admission
    database.agent_runtime_ownership = ownership
    database.actor_v2_fenced_wake_target_leases = leases
    database.actor_v2_cutover_journal = journal
    return journal, admission, ownership, leases


def _begin_preflight(
    journal: ActorV2CutoverJournalRepository,
    key: SessionKey,
) -> ActorV2CutoverRecord:
    """Record one clean-session journal preflight."""

    return journal.begin_preflight(
        key,
        legacy_session_id="legacy-session-a",
        adapter_instance_ids=("adapter-b", "adapter-a"),
        initiated_by="cutover-controller-a",
        evidence=_bundle(
            ActorV2CutoverPhase.PREFLIGHTED,
            proof_secret="preflight-proof-secret",
        ),
    )


def _progress_to_target_published(
    journal: ActorV2CutoverJournalRepository,
    admission: ActorV2AdmissionFenceRepository,
    ownerships: AgentRuntimeOwnershipRepository,
    leases: ActorV2FencedWakeTargetLeaseRepository,
    key: SessionKey,
) -> tuple[
    ActorV2CutoverRecord,
    ActorV2AdmissionGrant,
    AgentRuntimeOwnership,
    FencedWakeTargetLeaseGrant,
]:
    """Drive existing durable primitives through the journal's target phase."""

    preflighted = _begin_preflight(journal, key)
    admission_grant = admission.reserve(
        key,
        holder_id="cutover-controller-a",
        ttl_seconds=60.0,
    )
    journal.record_admission_reserved(
        preflighted.identity.cutover_id,
        grant=admission_grant,
        evidence=_bundle(
            ActorV2CutoverPhase.ADMISSION_RESERVED,
            proof_secret="admission-proof-secret",
        ),
    )
    journal.record_legacy_quiesced(
        preflighted.identity.cutover_id,
        admission_grant=admission_grant,
        evidence=_bundle(
            ActorV2CutoverPhase.LEGACY_QUIESCED,
            proof_secret="adapter-pause-token-secret",
        ),
    )
    owner = ownerships.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="cutover journal integration test",
        legacy_session_id=preflighted.identity.legacy_session_id,
        admission_grant=admission_grant,
    ).ownership
    journal.record_actor_owner_committed(
        preflighted.identity.cutover_id,
        ownership=owner,
        evidence=_bundle(
            ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED,
            proof_secret="owner-commit-proof-secret",
        ),
    )
    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=owner.generation,
        admission_fence_id=owner.admission_fence_id,
        admission_fence_generation=owner.admission_fence_generation,
    )
    target_grant = leases.acquire(
        request,
        target=MailboxHandoffTarget("actor-target-a", "incarnation-a"),
        ttl_seconds=60.0,
    )
    published = journal.record_target_published(
        preflighted.identity.cutover_id,
        target_grant=target_grant,
        evidence=_bundle(
            ActorV2CutoverPhase.TARGET_PUBLISHED,
            proof_secret="target-publication-proof-secret",
        ),
    )
    return published, admission_grant, owner, target_grant


def _progress_to_legacy_quiesced(
    journal: ActorV2CutoverJournalRepository,
    admission: ActorV2AdmissionFenceRepository,
    key: SessionKey,
) -> tuple[ActorV2CutoverRecord, ActorV2AdmissionGrant]:
    """Prepare one clean journal through its source-quiescence phase."""

    preflighted = _begin_preflight(journal, key)
    reserved, admission_grant = journal.reserve_clean_admission_and_record(
        preflighted.identity.cutover_id,
        holder_id="cutover-controller-a",
        ttl_seconds=60.0,
        evidence=_bundle(
            ActorV2CutoverPhase.ADMISSION_RESERVED,
            proof_secret="admission-proof-secret",
        ),
    )
    quiesced = journal.record_legacy_quiesced(
        preflighted.identity.cutover_id,
        admission_grant=admission_grant,
        evidence=_bundle(
            ActorV2CutoverPhase.LEGACY_QUIESCED,
            proof_secret="adapter-pause-token-secret",
        ),
    )
    return quiesced, admission_grant


def test_preflight_is_journal_only_and_fences_broad_legacy_recovery(
    tmp_path: Path,
) -> None:
    """A journal record neither reserves traffic nor starts Actor v2 work."""

    now = [10.0]
    database = _database(tmp_path)
    journal, _admission, _ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")

    record = _begin_preflight(journal, key)

    assert record.phase is ActorV2CutoverPhase.PREFLIGHTED
    assert record.identity.adapter_instance_ids == ("adapter-a", "adapter-b")
    with database.connect() as conn:
        counts = tuple(
            conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM agent_session_actor_v2_admission_fences),
                    (SELECT COUNT(*) FROM agent_session_runtime_ownership),
                    (SELECT COUNT(*) FROM agent_session_actor_v2_fenced_wake_target_leases),
                    (SELECT COUNT(*) FROM agent_session_aggregates),
                    (SELECT COUNT(*) FROM agent_session_mailbox),
                    (SELECT COUNT(*) FROM agent_route_outbox),
                    (SELECT COUNT(*) FROM message_routing_jobs)
                """
            ).fetchone()
        )

    assert counts == (0, 0, 0, 0, 0, 0, 0)
    assert (
        database.actor_v2_legacy_recovery_gate.snapshot().mode
        is LegacyRecoveryGateMode.FENCED_ONLY
    )
    with pytest.raises(LegacyRecoveryGateBlocked, match="fenced_only"):
        database.actor_v2_legacy_recovery_gate.acquire_legacy_recovery(
            holder_id="legacy-recovery-worker"
        )


def test_journal_accepts_core_ingress_drain_as_source_boundary_proof(
    tmp_path: Path,
) -> None:
    """Core ingress and adapter pause are distinct accepted quiescence paths."""

    now = [10.0]
    database = _database(tmp_path)
    journal, admission, _ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    preflighted = _begin_preflight(journal, key)
    grant = admission.reserve(
        key,
        holder_id="cutover-controller-a",
        ttl_seconds=60.0,
    )
    journal.record_admission_reserved(
        preflighted.identity.cutover_id,
        grant=grant,
        evidence=_bundle(
            ActorV2CutoverPhase.ADMISSION_RESERVED,
            proof_secret="admission-proof-secret",
        ),
    )

    quiesced = journal.record_legacy_quiesced(
        preflighted.identity.cutover_id,
        admission_grant=grant,
        evidence=_core_ingress_bundle(),
    )

    assert quiesced.phase is ActorV2CutoverPhase.LEGACY_QUIESCED
    assert {
        item.kind for item in quiesced.events[-1].evidence.evidence
    } == {
        ActorV2CutoverProofKind.CORE_INGRESS_DRAIN,
        ActorV2CutoverProofKind.LEGACY_QUIESCENCE,
    }
    database.initialize()


def test_clean_owner_commit_and_journal_phase_share_one_transaction(tmp_path: Path) -> None:
    """A clean Actor owner cannot become visible without its committed journal phase."""

    now = [10.0]
    database = _database(tmp_path)
    journal, admission, ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    quiesced, admission_grant = _progress_to_legacy_quiesced(journal, admission, key)

    committed = journal.commit_clean_actor_owner_and_record(
        quiesced.identity.cutover_id,
        admission_grant=admission_grant,
        reason="atomic clean cutover owner commit",
        requested_by="cutover-controller-a",
        evidence=_bundle(
            ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED,
            proof_secret="owner-commit-proof-secret",
        ),
    )

    owner = ownerships.get(key)
    fence = admission.get(key)
    assert owner is not None
    assert fence is not None
    assert committed.phase is ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED
    assert committed.ownership_generation == owner.generation == 1
    assert owner.mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert owner.admission_fence_id == admission_grant.fence.fence_id
    assert fence.status is ActorV2AdmissionFenceStatus.COMMITTED


def test_clean_admission_reservation_and_journal_phase_share_one_transaction(
    tmp_path: Path,
) -> None:
    """A clean reservation cannot become visible without its journal fence identity."""

    now = [10.0]
    database = _database(tmp_path)
    journal, admission, _ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    preflighted = _begin_preflight(journal, key)

    reserved, grant = journal.reserve_clean_admission_and_record(
        preflighted.identity.cutover_id,
        holder_id="cutover-controller-a",
        ttl_seconds=60.0,
        evidence=_bundle(
            ActorV2CutoverPhase.ADMISSION_RESERVED,
            proof_secret="admission-proof-secret",
        ),
    )

    fence = admission.get(key)
    assert fence is not None
    assert reserved.phase is ActorV2CutoverPhase.ADMISSION_RESERVED
    assert reserved.admission_fence_id == grant.fence.fence_id == fence.fence_id
    assert reserved.admission_fence_generation == grant.fence.generation == fence.generation
    assert fence.status is ActorV2AdmissionFenceStatus.RESERVED


def test_clean_admission_reservation_rolls_back_when_journal_phase_cannot_advance(
    tmp_path: Path,
) -> None:
    """A phase-write failure cannot leave an unrecorded reserved admission fence."""

    now = [10.0]
    database = _database(tmp_path)
    journal, admission, _ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    preflighted = _begin_preflight(journal, key)
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER fail_atomic_cutover_admission_phase
            BEFORE UPDATE OF phase ON agent_session_actor_v2_cutover_journal
            WHEN NEW.phase = 'admission_reserved'
            BEGIN
                SELECT RAISE(ABORT, 'forced admission journal failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="forced admission journal failure"):
        journal.reserve_clean_admission_and_record(
            preflighted.identity.cutover_id,
            holder_id="cutover-controller-a",
            ttl_seconds=60.0,
            evidence=_bundle(
                ActorV2CutoverPhase.ADMISSION_RESERVED,
                proof_secret="admission-proof-secret",
            ),
        )

    rolled_back = journal.get(preflighted.identity.cutover_id)
    assert rolled_back is not None
    assert rolled_back.phase is ActorV2CutoverPhase.PREFLIGHTED
    assert admission.get(key) is None


def test_clean_owner_commit_rolls_back_when_journal_phase_cannot_advance(
    tmp_path: Path,
) -> None:
    """A post-claim journal failure cannot strand a committed Actor owner."""

    now = [10.0]
    database = _database(tmp_path)
    journal, admission, ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    quiesced, admission_grant = _progress_to_legacy_quiesced(journal, admission, key)
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER fail_atomic_cutover_owner_phase
            BEFORE UPDATE OF phase ON agent_session_actor_v2_cutover_journal
            WHEN NEW.phase = 'actor_owner_committed'
            BEGIN
                SELECT RAISE(ABORT, 'forced actor owner journal failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="forced actor owner journal failure"):
        journal.commit_clean_actor_owner_and_record(
            quiesced.identity.cutover_id,
            admission_grant=admission_grant,
            reason="atomic clean cutover owner commit",
            requested_by="cutover-controller-a",
            evidence=_bundle(
                ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED,
                proof_secret="owner-commit-proof-secret",
            ),
        )

    rolled_back = journal.get(quiesced.identity.cutover_id)
    fence = admission.get(key)
    assert rolled_back is not None
    assert fence is not None
    assert rolled_back.phase is ActorV2CutoverPhase.LEGACY_QUIESCED
    assert ownerships.get(key) is None
    assert fence.status is ActorV2AdmissionFenceStatus.RESERVED


def test_journal_records_complete_forward_chain_without_raw_capabilities(
    tmp_path: Path,
) -> None:
    """Every forward phase binds the matching durable identity and proof digest."""

    now = [10.0]
    database = _database(tmp_path)
    journal, admission, ownerships, leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")

    published, _admission_grant, owner, target_grant = _progress_to_target_published(
        journal,
        admission,
        ownerships,
        leases,
        key,
    )
    resumed = journal.record_ingress_resumed(
        published.identity.cutover_id,
        target_grant=target_grant,
        evidence=_bundle(
            ActorV2CutoverPhase.INGRESS_RESUMED,
            proof_secret="ingress-resume-proof-secret",
        ),
    )

    assert resumed.phase is ActorV2CutoverPhase.INGRESS_RESUMED
    assert resumed.admission_fence_id == owner.admission_fence_id
    assert resumed.admission_fence_generation == owner.admission_fence_generation
    assert resumed.ownership_generation == owner.generation
    assert resumed.target_id == "actor-target-a"
    assert resumed.target_incarnation_id == "incarnation-a"
    assert resumed.target_lease_epoch == target_grant.lease.lease_epoch
    assert tuple(event.phase for event in resumed.events) == ACTOR_V2_CUTOVER_FORWARD_PHASES
    assert journal.get(resumed.identity.cutover_id) == resumed

    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT journal.cutover_id, journal.legacy_session_id,
                   journal.adapter_instance_ids_json, journal.phase,
                   event.evidence_json
            FROM agent_session_actor_v2_cutover_journal AS journal
            JOIN agent_session_actor_v2_cutover_events AS event
              ON event.cutover_id = journal.cutover_id
            ORDER BY event.event_seq
            """
        ).fetchall()
    persisted = json.dumps([dict(row) for row in rows], sort_keys=True)

    assert len(rows) == len(ACTOR_V2_CUTOVER_FORWARD_PHASES)
    assert "admission-holder-token-secret" not in persisted
    assert "target-holder-token-a" not in persisted
    assert "adapter-pause-token-secret" not in persisted


def test_journal_rejects_phase_skips_and_raw_history_mutation(tmp_path: Path) -> None:
    """Repository and trigger guards both reject an incomplete cutover path."""

    now = [10.0]
    database = _database(tmp_path)
    journal, admission, _ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    preflighted = _begin_preflight(journal, key)
    admission_grant = admission.reserve(
        key,
        holder_id="cutover-controller-a",
        ttl_seconds=60.0,
    )

    with pytest.raises(ActorV2CutoverJournalConflict, match="admission_reserved"):
        journal.record_legacy_quiesced(
            preflighted.identity.cutover_id,
            admission_grant=admission_grant,
            evidence=_bundle(
                ActorV2CutoverPhase.LEGACY_QUIESCED,
                proof_secret="adapter-pause-token-secret",
            ),
        )

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="phase transition"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_cutover_journal
                SET phase = 'admission_reserved'
                WHERE cutover_id = ?
                """,
                (preflighted.identity.cutover_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="events are immutable"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_cutover_events
                SET occurred_at = occurred_at + 1
                WHERE cutover_id = ?
                """,
                (preflighted.identity.cutover_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="event history cannot be deleted"):
            conn.execute(
                "DELETE FROM agent_session_actor_v2_cutover_events WHERE cutover_id = ?",
                (preflighted.identity.cutover_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="journal history cannot be deleted"):
            conn.execute(
                "DELETE FROM agent_session_actor_v2_cutover_journal WHERE cutover_id = ?",
                (preflighted.identity.cutover_id,),
            )


def test_lost_or_replaced_target_grant_cannot_resume_ingress(tmp_path: Path) -> None:
    """A target lease change leaves the journal blocked at publication."""

    now = [10.0]
    database = _database(tmp_path)
    journal, admission, ownerships, leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    published, _admission_grant, _owner, original_grant = _progress_to_target_published(
        journal,
        admission,
        ownerships,
        leases,
        key,
    )

    released = leases.release(original_grant)
    assert released.target == original_grant.lease.target
    replacement = leases.acquire(
        original_grant.lease.request,
        target=MailboxHandoffTarget("actor-target-a", "incarnation-b"),
        ttl_seconds=60.0,
    )

    with pytest.raises(FencedWakeTargetLeaseLost):
        journal.record_ingress_resumed(
            published.identity.cutover_id,
            target_grant=original_grant,
            evidence=_bundle(
                ActorV2CutoverPhase.INGRESS_RESUMED,
                proof_secret="ingress-resume-proof-secret",
            ),
        )
    with pytest.raises(ActorV2CutoverJournalConflict, match="lease changed"):
        journal.record_ingress_resumed(
            published.identity.cutover_id,
            target_grant=replacement,
            evidence=_bundle(
                ActorV2CutoverPhase.INGRESS_RESUMED,
                proof_secret="ingress-resume-proof-secret",
            ),
        )

    assert journal.get(published.identity.cutover_id) == published


@pytest.mark.asyncio
async def test_journal_residue_blocks_clean_activation_and_survives_restart(
    tmp_path: Path,
) -> None:
    """Journal history remains durable evidence rather than a reusable attempt."""

    now = [10.0]
    database = _database(tmp_path)
    journal, _admission, _ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    preflighted = _begin_preflight(journal, key)

    readiness = await SQLiteCleanSessionActivationPreflight(database).check()
    blockers = {blocker.code: blocker.count for blocker in readiness.blockers}
    assert readiness.permitted is False
    assert blockers["actor_v2_residual_agent_session_actor_v2_cutover_journal"] == 1
    assert blockers["actor_v2_residual_agent_session_actor_v2_cutover_events"] == 1

    database.initialize()

    assert database.actor_v2_cutover_journal.get(preflighted.identity.cutover_id) == preflighted
    with pytest.raises(ActorV2CutoverJournalConflict, match="history already exists"):
        _begin_preflight(database.actor_v2_cutover_journal, key)


def test_restart_rejects_evidence_with_a_valid_shape_but_wrong_phase_kind(
    tmp_path: Path,
) -> None:
    """Schema validation checks phase semantics, not only JSON shape."""

    now = [10.0]
    database = _database(tmp_path)
    journal, _admission, _ownerships, _leases = _repositories(database, now)
    key = SessionKey("profile-a", "profile-a:group:room")
    preflighted = _begin_preflight(journal, key)
    forged_evidence = json.dumps(
        [
            {
                "digest": "a" * 64,
                "issuer_id": "forged-issuer",
                "kind": "admission_reservation",
                "proof_epoch": 1,
                "summary_code": "proof.admission_reservation",
            }
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    with database.connect() as conn:
        conn.execute("DROP TRIGGER trg_actor_v2_cutover_event_immutable")
        conn.execute(
            """
            UPDATE agent_session_actor_v2_cutover_events
            SET evidence_json = ?
            WHERE cutover_id = ? AND phase = 'preflighted'
            """,
            (forged_evidence, preflighted.identity.cutover_id),
        )

    with pytest.raises(sqlite3.IntegrityError, match="invalid evidence"):
        database.initialize()
