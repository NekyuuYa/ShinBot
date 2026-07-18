"""Durable journal for a future, explicitly controlled Actor v2 cutover.

This repository is intentionally unable to route ingress or activate Actor v2.
It records a forward-only, token-free audit trail and verifies the durable
identities that already exist for admission, ownership, and wake-target
publication. A future controller still has to supply real adapter pause/drain
and legacy-quiescence proofs before it may use this journal as part of a
production protocol.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from collections.abc import Callable, Sequence
from sqlite3 import Connection, Row

from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.actor_v2_cutover import (
    ActorV2CutoverEvent,
    ActorV2CutoverEvidence,
    ActorV2CutoverEvidenceBundle,
    ActorV2CutoverIdentity,
    ActorV2CutoverJournalConflict,
    ActorV2CutoverJournalNotFound,
    ActorV2CutoverPhase,
    ActorV2CutoverProofKind,
    ActorV2CutoverRecord,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnership,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.core.dispatch.fenced_wake_target_lease import FencedWakeTargetLeaseGrant
from shinbot.persistence.repositories.base import Repository


class ActorV2CutoverJournalRepository(Repository):
    """Persist a no-skip cutover record without becoming a traffic authority.

    The repository is deliberately strict about phase order and keeps a session
    history non-reusable. A blocked or interrupted record cannot be silently
    replaced by a later caller; a future recovery controller must add an
    explicit stop-proof protocol before replacement is permitted.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        cutover_id_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize the durable journal with injectable monotonic identities."""

        super().__init__(db)
        self._clock = clock or time.time
        self._cutover_id_factory = cutover_id_factory or (lambda: uuid.uuid4().hex)

    def begin_preflight(
        self,
        key: SessionKey,
        *,
        legacy_session_id: str,
        adapter_instance_ids: Sequence[str],
        initiated_by: str,
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> ActorV2CutoverRecord:
        """Create the first, preflighted record for one never-before-cut session.

        This operation checks only durable negative evidence that is already
        available in this database. The supplied clean-preflight evidence is
        still external to this repository because legacy scheduler/workflow
        quiescence and cross-process adapter drain have no implementation yet.
        """

        if not isinstance(key, SessionKey):
            raise TypeError("cutover key must be a SessionKey")
        _require_bundle(evidence, ActorV2CutoverPhase.PREFLIGHTED)
        adapters = _adapter_instances(adapter_instance_ids)
        now = _finite_time(self._clock())
        cutover_id = _required_identifier(self._cutover_id_factory(), "cutover_id")
        identity = ActorV2CutoverIdentity(
            key=key,
            cutover_id=cutover_id,
            cutover_epoch=1,
            legacy_session_id=legacy_session_id,
            adapter_instance_ids=adapters,
        )
        initiated = _required_identifier(initiated_by, "initiated_by")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
                conn
            )
            # A committed journal is itself durable Actor v2 cutover history.
            # Fence broad legacy recovery in this same write transaction so a
            # later recovery pass cannot race the just-recorded preflight.
            self._db.actor_v2_legacy_recovery_gate.enter_fenced_only_in_transaction(
                conn
            )
            _require_no_cutover_history(conn, key)
            _require_no_actor_cutover_residue(conn, key)
            conn.execute(
                """
                INSERT INTO agent_session_actor_v2_cutover_journal (
                    profile_id, session_id, cutover_epoch, cutover_id,
                    legacy_session_id, adapter_instance_ids_json, phase,
                    initiated_by, admission_fence_id, admission_fence_generation,
                    ownership_generation, target_id, target_incarnation_id,
                    target_lease_epoch, blocked_code, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'preflighted', ?, '', 0, 0, '', '', 0, '', ?, ?)
                """,
                (
                    identity.key.profile_id,
                    identity.key.session_id,
                    identity.cutover_epoch,
                    identity.cutover_id,
                    identity.legacy_session_id,
                    _encode_adapter_instances(identity.adapter_instance_ids),
                    initiated,
                    now,
                    now,
                ),
            )
            _insert_event(conn, identity.cutover_id, evidence, now)
            return _load_required_record(conn, identity.cutover_id)

    def get(self, cutover_id: str) -> ActorV2CutoverRecord | None:
        """Return a token-free journal snapshot by its opaque cutover id."""

        normalized_id = _required_identifier(cutover_id, "cutover_id")
        with self.connect() as conn:
            row = _select_journal(conn, normalized_id)
            return _record_from_row(conn, row) if row is not None else None

    def get_for_key(self, key: SessionKey) -> ActorV2CutoverRecord | None:
        """Return the single historical journal for one stable session key."""

        if not isinstance(key, SessionKey):
            raise TypeError("cutover key must be a SessionKey")
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM agent_session_actor_v2_cutover_journal
                WHERE profile_id = ? AND session_id = ?
                ORDER BY cutover_epoch DESC
                LIMIT 1
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            return _record_from_row(conn, row) if row is not None else None

    def reserve_clean_admission_and_record(
        self,
        cutover_id: str,
        *,
        holder_id: str,
        ttl_seconds: float,
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> tuple[ActorV2CutoverRecord, ActorV2AdmissionGrant]:
        """Atomically reserve clean admission and append its journal phase.

        The returned opaque grant remains only with the explicit caller. The
        journal stores the token-free fence identity and evidence in the same
        transaction, so a reserve cannot become visible without the matching
        ``admission_reserved`` phase record.
        """

        _require_bundle(evidence, ActorV2CutoverPhase.ADMISSION_RESERVED)
        normalized_holder_id = str(holder_id or "").strip()
        if not normalized_holder_id:
            raise ValueError("holder_id must not be empty")
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = _load_required_record(conn, cutover_id)
            _require_phase(record, ActorV2CutoverPhase.PREFLIGHTED)
            grant = self._db.actor_v2_admission_fences.reserve_clean_in_transaction(
                conn,
                record.identity.key,
                holder_id=normalized_holder_id,
                ttl_seconds=ttl_seconds,
                now=now,
            )
            _advance_record(
                conn,
                record,
                phase=ActorV2CutoverPhase.ADMISSION_RESERVED,
                evidence=evidence,
                now=now,
                admission_fence_id=grant.fence.fence_id,
                admission_fence_generation=grant.fence.generation,
            )
            return _load_required_record(conn, record.identity.cutover_id), grant

    def record_admission_reserved(
        self,
        cutover_id: str,
        *,
        grant: ActorV2AdmissionGrant,
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> ActorV2CutoverRecord:
        """Bind the preflighted journal to one live reserved admission fence."""

        _require_bundle(evidence, ActorV2CutoverPhase.ADMISSION_RESERVED)
        if not isinstance(grant, ActorV2AdmissionGrant):
            raise TypeError("cutover admission grant must be an ActorV2AdmissionGrant")
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = _load_required_record(conn, cutover_id)
            _require_phase(record, ActorV2CutoverPhase.PREFLIGHTED)
            if grant.fence.key != record.identity.key:
                raise ActorV2CutoverJournalConflict(
                    "admission grant key does not match the cutover journal"
                )
            fence = self._db.actor_v2_admission_fences.require_reserved_in_transaction(
                conn,
                grant,
                now=now,
            )
            _advance_record(
                conn,
                record,
                phase=ActorV2CutoverPhase.ADMISSION_RESERVED,
                evidence=evidence,
                now=now,
                admission_fence_id=fence.fence_id,
                admission_fence_generation=fence.generation,
            )
            return _load_required_record(conn, record.identity.cutover_id)

    def record_legacy_quiesced(
        self,
        cutover_id: str,
        *,
        admission_grant: ActorV2AdmissionGrant,
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> ActorV2CutoverRecord:
        """Record legacy proof plus one accepted source-boundary drain proof."""

        _require_bundle(evidence, ActorV2CutoverPhase.LEGACY_QUIESCED)
        if not isinstance(admission_grant, ActorV2AdmissionGrant):
            raise TypeError("cutover admission grant must be an ActorV2AdmissionGrant")
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = _load_required_record(conn, cutover_id)
            _require_phase(record, ActorV2CutoverPhase.ADMISSION_RESERVED)
            _require_matching_admission(record, admission_grant)
            self._db.actor_v2_admission_fences.require_reserved_in_transaction(
                conn,
                admission_grant,
                now=now,
            )
            _advance_record(
                conn,
                record,
                phase=ActorV2CutoverPhase.LEGACY_QUIESCED,
                evidence=evidence,
                now=now,
            )
            return _load_required_record(conn, record.identity.cutover_id)

    def commit_clean_actor_owner_and_record(
        self,
        cutover_id: str,
        *,
        admission_grant: ActorV2AdmissionGrant,
        reason: str,
        requested_by: str = "",
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> ActorV2CutoverRecord:
        """Atomically claim a clean Actor owner and append its journal phase.

        The method is intentionally limited to the new-session admission path:
        it creates generation-one Actor v2 ownership from the journal's exact
        reserved fence and commits the phase event in the same SQLite writer
        transaction. It is not a legacy migration completion path and does not
        publish a target or resume ingress.
        """

        _require_bundle(evidence, ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED)
        if not isinstance(admission_grant, ActorV2AdmissionGrant):
            raise TypeError("cutover admission grant must be an ActorV2AdmissionGrant")
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("reason must not be empty")
        requester = str(requested_by or "").strip()
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = _load_required_record(conn, cutover_id)
            _require_phase(record, ActorV2CutoverPhase.LEGACY_QUIESCED)
            _require_matching_admission(record, admission_grant)
            self._db.actor_v2_admission_fences.require_reserved_in_transaction(
                conn,
                admission_grant,
                now=now,
            )
            claim = self._db.agent_runtime_ownership.claim_clean_actor_v2_in_transaction(
                conn,
                record.identity.key,
                reason=normalized_reason,
                legacy_session_id=record.identity.legacy_session_id,
                requested_by=requester or record.initiated_by,
                admission_grant=admission_grant,
                now=now,
            )
            ownership = claim.ownership
            _require_matching_owner(record, ownership)
            _advance_record(
                conn,
                record,
                phase=ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED,
                evidence=evidence,
                now=now,
                ownership_generation=ownership.generation,
            )
            return _load_required_record(conn, record.identity.cutover_id)

    def record_actor_owner_committed(
        self,
        cutover_id: str,
        *,
        ownership: AgentRuntimeOwnership,
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> ActorV2CutoverRecord:
        """Bind a quiesced journal to the currently committed Actor owner.

        This validates the durable ownership and committed admission identity in
        the same read transaction as the journal advance. It remains a journal
        primitive, not the required future atomic ownership-claim controller.
        A crash between a separate ownership claim and this call leaves the
        journal behind rather than fabricating a completed phase.
        """

        _require_bundle(evidence, ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED)
        if not isinstance(ownership, AgentRuntimeOwnership):
            raise TypeError("cutover ownership must be an AgentRuntimeOwnership")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = _load_required_record(conn, cutover_id)
            _require_phase(record, ActorV2CutoverPhase.LEGACY_QUIESCED)
            _require_matching_owner(record, ownership)
            current = self._db.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                record.identity.key,
                expected_generation=ownership.generation,
                expected_admission_fence_id=record.admission_fence_id,
                expected_admission_fence_generation=record.admission_fence_generation,
            )
            _advance_record(
                conn,
                record,
                phase=ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED,
                evidence=evidence,
                now=_finite_time(self._clock()),
                ownership_generation=current.generation,
            )
            return _load_required_record(conn, record.identity.cutover_id)

    def record_target_published(
        self,
        cutover_id: str,
        *,
        target_grant: FencedWakeTargetLeaseGrant,
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> ActorV2CutoverRecord:
        """Bind the committed owner phase to one exact live wake-target lease."""

        _require_bundle(evidence, ActorV2CutoverPhase.TARGET_PUBLISHED)
        if not isinstance(target_grant, FencedWakeTargetLeaseGrant):
            raise TypeError("cutover target_grant must be a FencedWakeTargetLeaseGrant")
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = _load_required_record(conn, cutover_id)
            _require_phase(record, ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED)
            lease = self._db.actor_v2_fenced_wake_target_leases.validate_in_transaction(
                conn,
                target_grant,
            )
            request = lease.request
            if (
                request.key != record.identity.key
                or request.ownership_generation != record.ownership_generation
                or request.admission_fence_id != record.admission_fence_id
                or request.admission_fence_generation
                != record.admission_fence_generation
            ):
                raise ActorV2CutoverJournalConflict(
                    "wake target lease does not match the committed cutover owner"
                )
            _advance_record(
                conn,
                record,
                phase=ActorV2CutoverPhase.TARGET_PUBLISHED,
                evidence=evidence,
                now=now,
                target_id=lease.target.target_id,
                target_incarnation_id=lease.target.incarnation_id,
                target_lease_epoch=lease.lease_epoch,
            )
            return _load_required_record(conn, record.identity.cutover_id)

    def record_ingress_resumed(
        self,
        cutover_id: str,
        *,
        target_grant: FencedWakeTargetLeaseGrant,
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> ActorV2CutoverRecord:
        """Record external ingress resume proof while the exact target remains live."""

        _require_bundle(evidence, ActorV2CutoverPhase.INGRESS_RESUMED)
        if not isinstance(target_grant, FencedWakeTargetLeaseGrant):
            raise TypeError("cutover target_grant must be a FencedWakeTargetLeaseGrant")
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = _load_required_record(conn, cutover_id)
            _require_phase(record, ActorV2CutoverPhase.TARGET_PUBLISHED)
            lease = self._db.actor_v2_fenced_wake_target_leases.validate_in_transaction(
                conn,
                target_grant,
            )
            if (
                lease.request.key != record.identity.key
                or lease.request.ownership_generation != record.ownership_generation
                or lease.request.admission_fence_id != record.admission_fence_id
                or lease.request.admission_fence_generation
                != record.admission_fence_generation
                or lease.target.target_id != record.target_id
                or lease.target.incarnation_id != record.target_incarnation_id
                or lease.lease_epoch != record.target_lease_epoch
            ):
                raise ActorV2CutoverJournalConflict(
                    "wake target lease changed before ingress resume could be recorded"
                )
            _advance_record(
                conn,
                record,
                phase=ActorV2CutoverPhase.INGRESS_RESUMED,
                evidence=evidence,
                now=now,
            )
            return _load_required_record(conn, record.identity.cutover_id)

    def block(
        self,
        cutover_id: str,
        *,
        blocked_code: str,
        evidence: ActorV2CutoverEvidenceBundle,
    ) -> ActorV2CutoverRecord:
        """Terminally block a non-complete cutover without reopening legacy work."""

        _require_bundle(evidence, ActorV2CutoverPhase.BLOCKED)
        code = _summary_code(blocked_code, "blocked_code")
        now = _finite_time(self._clock())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            record = _load_required_record(conn, cutover_id)
            if record.phase in {
                ActorV2CutoverPhase.INGRESS_RESUMED,
                ActorV2CutoverPhase.BLOCKED,
            }:
                raise ActorV2CutoverJournalConflict(
                    "completed or blocked cutover cannot be blocked again"
                )
            _advance_record(
                conn,
                record,
                phase=ActorV2CutoverPhase.BLOCKED,
                evidence=evidence,
                now=now,
                blocked_code=code,
            )
            return _load_required_record(conn, record.identity.cutover_id)


def _advance_record(
    conn: Connection,
    record: ActorV2CutoverRecord,
    *,
    phase: ActorV2CutoverPhase,
    evidence: ActorV2CutoverEvidenceBundle,
    now: float,
    admission_fence_id: str | None = None,
    admission_fence_generation: int | None = None,
    ownership_generation: int | None = None,
    target_id: str | None = None,
    target_incarnation_id: str | None = None,
    target_lease_epoch: int | None = None,
    blocked_code: str | None = None,
) -> None:
    """Compare-and-swap one record phase and append its immutable evidence."""

    _require_bundle(evidence, phase)
    expected_phase = _expected_previous_phase(phase)
    if phase is ActorV2CutoverPhase.BLOCKED:
        expected_phase = record.phase
    if record.phase is not expected_phase:
        raise ActorV2CutoverJournalConflict(
            f"cutover phase {record.phase.value!r} cannot advance to {phase.value!r}"
        )
    values = {
        "admission_fence_id": (
            record.admission_fence_id
            if admission_fence_id is None
            else _required_identifier(admission_fence_id, "admission_fence_id")
        ),
        "admission_fence_generation": (
            record.admission_fence_generation
            if admission_fence_generation is None
            else _positive_integer(admission_fence_generation, "admission_fence_generation")
        ),
        "ownership_generation": (
            record.ownership_generation
            if ownership_generation is None
            else _positive_integer(ownership_generation, "ownership_generation")
        ),
        "target_id": record.target_id if target_id is None else _required_identifier(target_id, "target_id"),
        "target_incarnation_id": (
            record.target_incarnation_id
            if target_incarnation_id is None
            else _required_identifier(target_incarnation_id, "target_incarnation_id")
        ),
        "target_lease_epoch": (
            record.target_lease_epoch
            if target_lease_epoch is None
            else _positive_integer(target_lease_epoch, "target_lease_epoch")
        ),
        "blocked_code": record.blocked_code if blocked_code is None else _summary_code(blocked_code, "blocked_code"),
    }
    _insert_event(conn, record.identity.cutover_id, evidence, now)
    updated = conn.execute(
        """
        UPDATE agent_session_actor_v2_cutover_journal
        SET phase = ?,
            admission_fence_id = ?,
            admission_fence_generation = ?,
            ownership_generation = ?,
            target_id = ?,
            target_incarnation_id = ?,
            target_lease_epoch = ?,
            blocked_code = ?,
            updated_at = ?
        WHERE cutover_id = ? AND phase = ?
        """,
        (
            phase.value,
            values["admission_fence_id"],
            values["admission_fence_generation"],
            values["ownership_generation"],
            values["target_id"],
            values["target_incarnation_id"],
            values["target_lease_epoch"],
            values["blocked_code"],
            now,
            record.identity.cutover_id,
            record.phase.value,
        ),
    )
    if updated.rowcount != 1:
        raise ActorV2CutoverJournalConflict("cutover record changed while advancing")


def _expected_previous_phase(phase: ActorV2CutoverPhase) -> ActorV2CutoverPhase:
    """Return the only successful predecessor for one non-terminal phase."""

    predecessors = {
        ActorV2CutoverPhase.ADMISSION_RESERVED: ActorV2CutoverPhase.PREFLIGHTED,
        ActorV2CutoverPhase.LEGACY_QUIESCED: ActorV2CutoverPhase.ADMISSION_RESERVED,
        ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED: ActorV2CutoverPhase.LEGACY_QUIESCED,
        ActorV2CutoverPhase.TARGET_PUBLISHED: ActorV2CutoverPhase.ACTOR_OWNER_COMMITTED,
        ActorV2CutoverPhase.INGRESS_RESUMED: ActorV2CutoverPhase.TARGET_PUBLISHED,
    }
    try:
        return predecessors[phase]
    except KeyError as exc:
        raise ValueError(f"cutover phase {phase.value!r} has no forward predecessor") from exc


def _require_phase(record: ActorV2CutoverRecord, expected: ActorV2CutoverPhase) -> None:
    """Require an exact journal phase before a dependent transition."""

    if record.phase is not expected:
        raise ActorV2CutoverJournalConflict(
            f"cutover is in {record.phase.value!r}, not required {expected.value!r} phase"
        )


def _require_matching_admission(
    record: ActorV2CutoverRecord,
    grant: ActorV2AdmissionGrant,
) -> None:
    """Require a holder capability to match the journal's fenced identity."""

    if (
        grant.fence.key != record.identity.key
        or grant.fence.fence_id != record.admission_fence_id
        or grant.fence.generation != record.admission_fence_generation
    ):
        raise ActorV2CutoverJournalConflict(
            "admission grant does not match the journal reservation identity"
        )


def _require_matching_owner(
    record: ActorV2CutoverRecord,
    ownership: AgentRuntimeOwnership,
) -> None:
    """Validate token-free owner identity before its durable revalidation."""

    if (
        ownership.key != record.identity.key
        or ownership.legacy_session_id != record.identity.legacy_session_id
        or ownership.mode is not AgentRuntimeOwnershipMode.ACTOR_V2
        or ownership.status is not AgentRuntimeOwnershipStatus.ACTIVE
        or ownership.admission_fence_id != record.admission_fence_id
        or ownership.admission_fence_generation != record.admission_fence_generation
    ):
        raise ActorV2CutoverJournalConflict(
            "Actor ownership does not match the journal identity and admission fence"
        )


def _require_bundle(
    evidence: ActorV2CutoverEvidenceBundle,
    phase: ActorV2CutoverPhase,
) -> None:
    """Require a typed exact-proof bundle for the requested phase."""

    if not isinstance(evidence, ActorV2CutoverEvidenceBundle):
        raise TypeError("cutover evidence must be an ActorV2CutoverEvidenceBundle")
    if evidence.phase is not phase:
        raise ValueError(
            f"cutover evidence phase {evidence.phase.value!r} does not match {phase.value!r}"
        )


def _require_no_cutover_history(conn: Connection, key: SessionKey) -> None:
    """Refuse an implicit replacement cutover until a future recovery protocol exists."""

    row = conn.execute(
        """
        SELECT cutover_id
        FROM agent_session_actor_v2_cutover_journal
        WHERE profile_id = ? AND session_id = ?
        LIMIT 1
        """,
        (key.profile_id, key.session_id),
    ).fetchone()
    if row is not None:
        raise ActorV2CutoverJournalConflict(
            "Actor v2 cutover journal history already exists for this session"
        )


def _require_no_actor_cutover_residue(conn: Connection, key: SessionKey) -> None:
    """Reject a journal start that would reinterpret existing durable ownership."""

    ownership = conn.execute(
        """
        SELECT 1
        FROM agent_session_runtime_ownership
        WHERE profile_id = ? AND session_id = ?
        LIMIT 1
        """,
        (key.profile_id, key.session_id),
    ).fetchone()
    if ownership is not None:
        raise ActorV2CutoverJournalConflict(
            "cutover preflight requires a session without runtime ownership"
        )
    admission = conn.execute(
        """
        SELECT 1
        FROM agent_session_actor_v2_admission_fences
        WHERE profile_id = ? AND session_id = ?
        LIMIT 1
        """,
        (key.profile_id, key.session_id),
    ).fetchone()
    if admission is not None:
        raise ActorV2CutoverJournalConflict(
            "cutover preflight requires a session without admission-fence history"
        )


def _load_required_record(conn: Connection, cutover_id: str) -> ActorV2CutoverRecord:
    """Load one journal record or report its absence with a typed error."""

    normalized_id = _required_identifier(cutover_id, "cutover_id")
    row = _select_journal(conn, normalized_id)
    if row is None:
        raise ActorV2CutoverJournalNotFound("Actor v2 cutover journal does not exist")
    return _record_from_row(conn, row)


def _select_journal(conn: Connection, cutover_id: str) -> Row | None:
    """Select one journal row by its opaque immutable id."""

    return conn.execute(
        "SELECT * FROM agent_session_actor_v2_cutover_journal WHERE cutover_id = ?",
        (_required_identifier(cutover_id, "cutover_id"),),
    ).fetchone()


def _insert_event(
    conn: Connection,
    cutover_id: str,
    evidence: ActorV2CutoverEvidenceBundle,
    occurred_at: float,
) -> None:
    """Append one exact phase event without persisting raw proof capabilities."""

    conn.execute(
        """
        INSERT INTO agent_session_actor_v2_cutover_events (
            cutover_id, phase, evidence_json, occurred_at
        ) VALUES (?, ?, ?, ?)
        """,
        (
            _required_identifier(cutover_id, "cutover_id"),
            evidence.phase.value,
            _encode_evidence(evidence),
            _finite_time(occurred_at),
        ),
    )


def _record_from_row(conn: Connection, row: Row) -> ActorV2CutoverRecord:
    """Decode one journal row and its immutable phase events fail closed."""

    try:
        identity = ActorV2CutoverIdentity(
            key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
            cutover_id=str(row["cutover_id"]),
            cutover_epoch=int(row["cutover_epoch"]),
            legacy_session_id=str(row["legacy_session_id"]),
            adapter_instance_ids=_decode_adapter_instances(row["adapter_instance_ids_json"]),
        )
        event_rows = conn.execute(
            """
            SELECT phase, evidence_json, occurred_at
            FROM agent_session_actor_v2_cutover_events
            WHERE cutover_id = ?
            ORDER BY event_seq ASC
            """,
            (identity.cutover_id,),
        ).fetchall()
        events = tuple(
            ActorV2CutoverEvent(
                cutover_id=identity.cutover_id,
                phase=ActorV2CutoverPhase(str(event_row["phase"])),
                evidence=_decode_evidence(
                    ActorV2CutoverPhase(str(event_row["phase"])),
                    event_row["evidence_json"],
                ),
                occurred_at=float(event_row["occurred_at"]),
            )
            for event_row in event_rows
        )
        return ActorV2CutoverRecord(
            identity=identity,
            phase=ActorV2CutoverPhase(str(row["phase"])),
            initiated_by=str(row["initiated_by"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            admission_fence_id=str(row["admission_fence_id"]),
            admission_fence_generation=int(row["admission_fence_generation"]),
            ownership_generation=int(row["ownership_generation"]),
            target_id=str(row["target_id"]),
            target_incarnation_id=str(row["target_incarnation_id"]),
            target_lease_epoch=int(row["target_lease_epoch"]),
            blocked_code=str(row["blocked_code"]),
            events=events,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActorV2CutoverJournalConflict(
            "Actor v2 cutover journal contains invalid durable state"
        ) from exc


def _encode_adapter_instances(adapter_instance_ids: tuple[str, ...]) -> str:
    """Write canonical adapter identities, never an implicit local default."""

    return json.dumps(list(adapter_instance_ids), ensure_ascii=True, separators=(",", ":"))


def _decode_adapter_instances(value: object) -> tuple[str, ...]:
    """Decode canonical adapter identity JSON into the strict value object."""

    decoded = json.loads(str(value))
    if not isinstance(decoded, list) or any(not isinstance(item, str) for item in decoded):
        raise ValueError("cutover adapter instances must be a JSON string list")
    return tuple(decoded)


def _encode_evidence(evidence: ActorV2CutoverEvidenceBundle) -> str:
    """Serialize only typed token-free evidence fields in canonical order."""

    payload = [
        {
            "digest": item.digest,
            "issuer_id": item.issuer_id,
            "kind": item.kind.value,
            "proof_epoch": item.proof_epoch,
            "summary_code": item.summary_code,
        }
        for item in evidence.evidence
    ]
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _decode_evidence(
    phase: ActorV2CutoverPhase,
    value: object,
) -> ActorV2CutoverEvidenceBundle:
    """Decode and validate one immutable event proof bundle."""

    decoded = json.loads(str(value))
    if not isinstance(decoded, list):
        raise ValueError("cutover evidence must be a JSON list")
    evidence = tuple(
        ActorV2CutoverEvidence(
            kind=ActorV2CutoverProofKind(str(item["kind"])),
            issuer_id=str(item["issuer_id"]),
            proof_epoch=int(item["proof_epoch"]),
            digest=str(item["digest"]),
            summary_code=str(item["summary_code"]),
        )
        for item in decoded
        if isinstance(item, dict)
    )
    if len(evidence) != len(decoded):
        raise ValueError("cutover evidence entries must be JSON objects")
    return ActorV2CutoverEvidenceBundle(phase=phase, evidence=evidence)


def _adapter_instances(value: Sequence[str]) -> tuple[str, ...]:
    """Normalize an adapter sequence before constructing a cutover identity."""

    if isinstance(value, str):
        raise TypeError("adapter_instance_ids must be a sequence of strings")
    return tuple(value)


def _required_identifier(value: object, field_name: str) -> str:
    """Normalize one required repository identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _positive_integer(value: object, field_name: str) -> int:
    """Normalize one positive immutable reference generation."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _summary_code(value: object, field_name: str) -> str:
    """Reuse typed blocked-code validation without exposing raw operator text."""

    evidence = ActorV2CutoverEvidence(
        kind=ActorV2CutoverProofKind.BLOCKED,
        issuer_id="journal",
        proof_epoch=1,
        digest="0" * 64,
        summary_code=str(value or ""),
    )
    return evidence.summary_code


def _finite_time(value: object) -> float:
    """Normalize a finite clock value without accepting booleans."""

    if isinstance(value, bool):
        raise ValueError("cutover clock must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError("cutover clock must be finite")
    return normalized


__all__ = ["ActorV2CutoverJournalRepository"]
