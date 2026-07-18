"""Integration coverage for the dormant cross-process Actor v2 ingress drain."""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterable
from pathlib import Path

import pytest

from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.actor_v2_cutover import (
    ActorV2CutoverEvidence,
    ActorV2CutoverEvidenceBundle,
    ActorV2CutoverPhase,
    ActorV2CutoverProofKind,
    ActorV2CutoverRecord,
)
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressDrainConflict,
    ActorV2IngressDrainCoverageError,
    ActorV2IngressDrainNotReady,
    ActorV2IngressDrainProofKind,
    ActorV2IngressDrainReceipt,
    ActorV2IngressDrainStatus,
    ActorV2IngressParticipantStatus,
    ActorV2IngressStopProof,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_admission_fence import (
    ActorV2AdmissionFenceRepository,
)
from shinbot.persistence.repositories.actor_v2_cutover_journal import (
    ActorV2CutoverJournalRepository,
)
from shinbot.persistence.repositories.actor_v2_ingress_drain import (
    ActorV2IngressDrainRepository,
)


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized database for ingress-drain protocol tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _digest(value: str) -> str:
    """Build a stable opaque digest without persisting the source value."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _bundle(
    phase: ActorV2CutoverPhase,
    kinds: Iterable[ActorV2CutoverProofKind],
) -> ActorV2CutoverEvidenceBundle:
    """Build the minimal token-free journal evidence needed by this fixture."""

    return ActorV2CutoverEvidenceBundle(
        phase=phase,
        evidence=tuple(
            ActorV2CutoverEvidence(
                kind=kind,
                issuer_id=f"fixture-{kind.value}",
                proof_epoch=1,
                digest=_digest(f"fixture:{phase.value}:{kind.value}"),
                summary_code=f"fixture.{kind.value}",
            )
            for kind in kinds
        ),
    )


def _reserved_cutover(
    database: DatabaseManager,
    now: list[float],
    *,
    adapter_instance_ids: tuple[str, ...],
) -> tuple[ActorV2CutoverRecord, ActorV2AdmissionGrant]:
    """Create one journal row in the exact admission-reserved phase."""

    admission = ActorV2AdmissionFenceRepository(
        database,
        clock=lambda: now[0],
        fence_id_factory=lambda: "admission-fence-a",
        holder_token_factory=lambda: "admission-holder-token-secret",
    )
    journal = ActorV2CutoverJournalRepository(
        database,
        clock=lambda: now[0],
        cutover_id_factory=lambda: "cutover-a",
    )
    database.actor_v2_admission_fences = admission
    database.actor_v2_cutover_journal = journal
    key = SessionKey("profile-a", "profile-a:group:room")
    preflighted = journal.begin_preflight(
        key,
        legacy_session_id="legacy-session-a",
        adapter_instance_ids=adapter_instance_ids,
        initiated_by="cutover-controller-a",
        evidence=_bundle(
            ActorV2CutoverPhase.PREFLIGHTED,
            (ActorV2CutoverProofKind.CLEAN_PREFLIGHT,),
        ),
    )
    grant = admission.reserve(
        key,
        holder_id="cutover-controller-a",
        ttl_seconds=1_000_000_000.0,
    )
    return (
        journal.record_admission_reserved(
            preflighted.identity.cutover_id,
            grant=grant,
            evidence=_bundle(
                ActorV2CutoverPhase.ADMISSION_RESERVED,
                (ActorV2CutoverProofKind.ADMISSION_RESERVATION,),
            ),
        ),
        grant,
    )


def _repository(
    database: DatabaseManager,
    now: list[float],
    *,
    member_ids: Iterable[str],
    holder_tokens: Iterable[str],
) -> ActorV2IngressDrainRepository:
    """Install a deterministic ingress drain repository for one test domain."""

    member_id_values = iter(member_ids)
    holder_token_values = iter(holder_tokens)
    repository = ActorV2IngressDrainRepository(
        database,
        clock=lambda: now[0],
        member_id_factory=lambda: next(member_id_values),
        request_id_factory=lambda: "drain-request-a",
        holder_token_factory=lambda: next(holder_token_values),
    )
    database.actor_v2_ingress_drains = repository
    return repository


def _receipt(member_id: str) -> ActorV2IngressDrainReceipt:
    """Build one token-free local receipt for an exact member."""

    return ActorV2IngressDrainReceipt(
        adapter_pause_digest=_digest(f"adapter-pause:{member_id}"),
        legacy_quiescence_digest=_digest(f"legacy-quiescence:{member_id}"),
        proof_epoch=1,
        summary_code="local.quiescent",
    )


def test_membership_heartbeat_is_token_free_and_never_auto_retires(
    tmp_path: Path,
) -> None:
    """An old heartbeat remains an active member until an explicit terminal action."""

    now = [100.0]
    database = _database(tmp_path)
    repository = _repository(
        database,
        now,
        member_ids=("member-a",),
        holder_tokens=("participant-holder-token-secret",),
    )
    grant = repository.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )

    now[0] = 90.0
    heartbeated = repository.heartbeat(grant)

    assert heartbeated.status is ActorV2IngressParticipantStatus.ACTIVE
    assert heartbeated.last_heartbeat_at == 100.0
    with database.connect() as conn:
        row = conn.execute(
            "SELECT holder_token_digest FROM agent_runtime_actor_v2_ingress_participants"
        ).fetchone()
    assert row is not None
    assert str(row["holder_token_digest"]) != grant.holder_token


def test_drain_requires_coverage_and_seals_all_current_members(tmp_path: Path) -> None:
    """Every adapter needs coverage, and a sealed request rejects late members."""

    now = [100.0]
    database = _database(tmp_path)
    cutover, admission_grant = _reserved_cutover(
        database,
        now,
        adapter_instance_ids=("adapter-a", "adapter-b"),
    )
    repository = _repository(
        database,
        now,
        member_ids=("member-a-1", "member-a-2", "member-b-1", "member-late"),
        holder_tokens=("token-a-1", "token-a-2", "token-b-1", "token-late"),
    )
    repository.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-1",
        participant_epoch=1,
    )

    with pytest.raises(ActorV2IngressDrainCoverageError) as missing_coverage:
        repository.begin_drain(
            cutover_id=cutover.identity.cutover_id,
            admission_grant=admission_grant,
        )
    assert missing_coverage.value.missing_adapter_instance_ids == ("adapter-b",)

    first = repository.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-2",
        participant_epoch=1,
    )
    repository.register_participant(
        adapter_instance_id="adapter-b",
        participant_id="process-b:incarnation-1",
        participant_epoch=1,
    )
    request = repository.begin_drain(
        cutover_id=cutover.identity.cutover_id,
        admission_grant=admission_grant,
    )

    assert request.status is ActorV2IngressDrainStatus.OPEN
    assert request.cutover_epoch == cutover.identity.cutover_epoch
    assert tuple(member.member_id for member in request.members) == (
        "member-a-1",
        "member-a-2",
        "member-b-1",
    )
    now[0] = 10_000_000.0
    assert repository.get_participant(first.participant.member_id) is not None
    with pytest.raises(ActorV2IngressDrainConflict, match="membership is frozen"):
        repository.register_participant(
            adapter_instance_id="adapter-a",
            participant_id="process-late:incarnation-1",
            participant_epoch=1,
        )
    with pytest.raises(ActorV2IngressDrainNotReady, match="unacknowledged"):
        repository.confirm_drained(
            request_id=request.request_id,
            admission_grant=admission_grant,
        )


def test_unacknowledged_member_cannot_be_removed_by_stop_proof(tmp_path: Path) -> None:
    """A stop proof is not substituted for the missing pause/drain observation."""

    now = [100.0]
    database = _database(tmp_path)
    cutover, admission_grant = _reserved_cutover(
        database,
        now,
        adapter_instance_ids=("adapter-a",),
    )
    repository = _repository(
        database,
        now,
        member_ids=("member-a",),
        holder_tokens=("participant-token-a",),
    )
    grant = repository.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )
    repository.begin_drain(
        cutover_id=cutover.identity.cutover_id,
        admission_grant=admission_grant,
    )

    with pytest.raises(ActorV2IngressDrainConflict, match="before acknowledging"):
        repository.revoke_with_stop_proof(
            grant.participant,
            stop_proof=ActorV2IngressStopProof(
                issuer_id="orchestrator-a",
                proof_epoch=1,
                digest=_digest("external-stop-proof"),
                summary_code="external.process_stopped",
            ),
        )
    participant = repository.get_participant("member-a")
    assert participant is not None
    assert participant.active


def test_all_member_receipts_are_immutable_and_form_journal_safe_digests(
    tmp_path: Path,
) -> None:
    """Only a complete sealed set may become a token-free cutover proof."""

    now = [100.0]
    database = _database(tmp_path)
    cutover, admission_grant = _reserved_cutover(
        database,
        now,
        adapter_instance_ids=("adapter-a", "adapter-b"),
    )
    repository = _repository(
        database,
        now,
        member_ids=("member-a", "member-b"),
        holder_tokens=("participant-token-a", "participant-token-b"),
    )
    grant_a = repository.register_participant(
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )
    grant_b = repository.register_participant(
        adapter_instance_id="adapter-b",
        participant_id="process-b:incarnation-b",
        participant_epoch=1,
    )
    request = repository.begin_drain(
        cutover_id=cutover.identity.cutover_id,
        admission_grant=admission_grant,
    )

    acknowledgement_a = repository.acknowledge_quiescent(
        request_id=request.request_id,
        grant=grant_a,
        receipt=_receipt("member-a"),
    )
    assert acknowledgement_a == repository.acknowledge_quiescent(
        request_id=request.request_id,
        grant=grant_a,
        receipt=_receipt("member-a"),
    )
    repository.acknowledge_quiescent(
        request_id=request.request_id,
        grant=grant_b,
        receipt=_receipt("member-b"),
    )
    now[0] = 101.0
    drained = repository.confirm_drained(
        request_id=request.request_id,
        admission_grant=admission_grant,
    )

    assert drained.status is ActorV2IngressDrainStatus.DRAINED
    assert drained.durably_drained
    assert (
        drained.proof_digest(ActorV2IngressDrainProofKind.ADAPTER_PAUSE)
        != drained.proof_digest(ActorV2IngressDrainProofKind.LEGACY_QUIESCENCE)
    )
    with database.connect() as conn:
        persisted = conn.execute(
            """
            SELECT adapter_pause_digest, legacy_quiescence_digest, summary_code
            FROM agent_session_actor_v2_ingress_drain_acknowledgements
            """
        ).fetchall()
        serialized = str([dict(row) for row in persisted])
        assert "participant-token-a" not in serialized
        assert "participant-token-b" not in serialized
        with pytest.raises(sqlite3.IntegrityError, match="acknowledgement is immutable"):
            conn.execute(
                """
                UPDATE agent_session_actor_v2_ingress_drain_acknowledgements
                SET summary_code = 'tampered'
                WHERE request_id = ? AND member_id = ?
                """,
                (request.request_id, "member-a"),
            )
    database.initialize()
    assert repository.get_request(request.request_id) == drained
