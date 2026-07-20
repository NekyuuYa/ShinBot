"""Durable membership and acknowledgement storage for future ingress drains.

This repository deliberately has no polling loop and invokes no adapter API.
It is a fail-closed control-plane primitive for a future controller: runtime
processes must register before they can receive callbacks, then use their
opaque local grants only to heartbeat, retire, or acknowledge an exact drain
request.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from collections.abc import Callable, Iterable
from sqlite3 import Connection, Row

from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressDrainAcknowledgement,
    ActorV2IngressDrainConflict,
    ActorV2IngressDrainCoverageError,
    ActorV2IngressDrainMember,
    ActorV2IngressDrainNotFound,
    ActorV2IngressDrainNotReady,
    ActorV2IngressDrainReceipt,
    ActorV2IngressDrainRequest,
    ActorV2IngressDrainStatus,
    ActorV2IngressParticipant,
    ActorV2IngressParticipantGrant,
    ActorV2IngressParticipantStatus,
    ActorV2IngressStopProof,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.persistence.repositories.base import Repository

_PENDING_REQUEST_STATUSES = (
    ActorV2IngressDrainStatus.ASSEMBLING.value,
    ActorV2IngressDrainStatus.OPEN.value,
    ActorV2IngressDrainStatus.DRAINED.value,
)

_PENDING_CORE_INGRESS_DRAIN_REQUEST_STATUSES = (
    "assembling",
    "open",
    "drained",
)


class ActorV2IngressDrainRepository(Repository):
    """Persist immutable ingress memberships and sealed drain snapshots.

    A heartbeat has no expiry behavior in this repository.  A stale member
    remains active until it retires with its holder capability or an external
    stop proof explicitly revokes its exact current snapshot.  Neither path
    turns a missing acknowledgement into a successful drain receipt.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        member_id_factory: Callable[[], str] | None = None,
        request_id_factory: Callable[[], str] | None = None,
        holder_token_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize the control plane with injectable durable identities."""

        super().__init__(db)
        self._clock = clock or time.time
        self._member_id_factory = member_id_factory or (lambda: uuid.uuid4().hex)
        self._request_id_factory = request_id_factory or (lambda: uuid.uuid4().hex)
        self._holder_token_factory = holder_token_factory or (lambda: uuid.uuid4().hex)

    @property
    def persistence_domain(self) -> object:
        """Return the exact database domain covered by this protocol."""

        return self._db

    def register_participant(
        self,
        *,
        adapter_instance_id: str,
        participant_id: str,
        participant_epoch: int,
    ) -> ActorV2IngressParticipantGrant:
        """Register one process incarnation before it can receive callbacks.

        A future adapter lifecycle wrapper must call this before accepting any
        platform callback.  Registration is rejected while an existing drain
        request covers the same adapter, so a late-starting process cannot
        escape a snapshot that has already begun.
        """

        return self.register_participants(
            adapter_instance_ids=(adapter_instance_id,),
            participant_id=participant_id,
            participant_epoch=participant_epoch,
        )[0]

    def register_participants(
        self,
        *,
        adapter_instance_ids: Iterable[str],
        participant_id: str,
        participant_epoch: int,
    ) -> tuple[ActorV2IngressParticipantGrant, ...]:
        """Atomically register one process incarnation for several adapters.

        A process that owns more than one adapter must not expose a partial
        membership set between individual registrations. The same writer
        transaction either creates every requested active member or creates
        none, so a concurrent drain can snapshot the full process scope or no
        scope at all. Returned holder capabilities remain local-only.
        """

        adapter_ids = _adapter_instance_ids(adapter_instance_ids)
        process_id = _identifier(participant_id, "participant_id")
        epoch = _positive_int(participant_epoch, "participant_epoch")
        registrations = tuple(
            (
                adapter_id,
                _identifier(self._member_id_factory(), "member_id"),
                _identifier(self._holder_token_factory(), "holder_token"),
            )
            for adapter_id in adapter_ids
        )
        member_ids = tuple(registration[1] for registration in registrations)
        if len(set(member_ids)) != len(member_ids):
            raise ActorV2IngressDrainConflict(
                "participant member identity factory repeated within one registration"
            )
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for adapter_id, _member_id, _token in registrations:
                _require_adapter_unfrozen(conn, adapter_id)
                existing = conn.execute(
                    """
                    SELECT member_id
                    FROM agent_runtime_actor_v2_ingress_participants
                    WHERE adapter_instance_id = ?
                      AND participant_id = ?
                      AND participant_epoch = ?
                    """,
                    (adapter_id, process_id, epoch),
                ).fetchone()
                if existing is not None:
                    raise ActorV2IngressDrainConflict(
                        "adapter participant incarnation has durable history already"
                    )
            for adapter_id, member_id, token in registrations:
                conn.execute(
                    """
                    INSERT INTO agent_runtime_actor_v2_ingress_participants (
                        member_id, adapter_instance_id, participant_id,
                        participant_epoch, holder_token_digest, status,
                        registered_at, last_heartbeat_at, updated_at,
                        retired_at, revoked_at, stop_proof_issuer_id,
                        stop_proof_epoch, stop_proof_digest, stop_proof_summary_code
                    ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, NULL, NULL, '', 0, '', '')
                    """,
                    (
                        member_id,
                        adapter_id,
                        process_id,
                        epoch,
                        _token_digest(token),
                        now,
                        now,
                        now,
                    ),
                )
            return tuple(
                ActorV2IngressParticipantGrant(
                    participant=_load_participant_required(conn, member_id),
                    holder_token=token,
                )
                for _adapter_id, member_id, token in registrations
            )

    def get_participant(self, member_id: str) -> ActorV2IngressParticipant | None:
        """Return one token-free membership snapshot by its stable member id."""

        normalized_id = _identifier(member_id, "member_id")
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM agent_runtime_actor_v2_ingress_participants
                WHERE member_id = ?
                """,
                (normalized_id,),
            ).fetchone()
            return _participant_from_row(row) if row is not None else None

    def list_participants(
        self,
        *,
        adapter_instance_id: str | None = None,
        active_only: bool = False,
    ) -> tuple[ActorV2IngressParticipant, ...]:
        """List durable member snapshots in deterministic registration order."""

        clauses: list[str] = []
        parameters: list[str] = []
        if adapter_instance_id is not None:
            clauses.append("adapter_instance_id = ?")
            parameters.append(_identifier(adapter_instance_id, "adapter_instance_id"))
        if active_only:
            clauses.append("status = 'active'")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM agent_runtime_actor_v2_ingress_participants
                {where}
                ORDER BY adapter_instance_id, participant_id, participant_epoch, member_id
                """,
                tuple(parameters),
            ).fetchall()
        return tuple(_participant_from_row(row) for row in rows)

    def heartbeat(
        self,
        grant: ActorV2IngressParticipantGrant,
    ) -> ActorV2IngressParticipant:
        """Update an advisory liveness observation for one exact active member."""

        return self.heartbeat_participants((grant,))[0]

    def heartbeat_participants(
        self,
        grants: Iterable[ActorV2IngressParticipantGrant],
    ) -> tuple[ActorV2IngressParticipant, ...]:
        """Atomically record advisory heartbeats for one local member set.

        This intentionally provides no lease or expiry semantics. It only
        prevents a process lifecycle from reporting a partially refreshed
        member set after a concurrent terminal change invalidates one of its
        local holder capabilities.
        """

        normalized_grants = _participant_grants(grants)
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current_pairs = tuple(
                (_require_active_grant(conn, grant), grant)
                for grant in normalized_grants
            )
            for current, grant in current_pairs:
                observed_at = max(now, current.last_heartbeat_at)
                updated = conn.execute(
                    """
                    UPDATE agent_runtime_actor_v2_ingress_participants
                    SET last_heartbeat_at = ?, updated_at = ?
                    WHERE member_id = ?
                      AND status = 'active'
                      AND holder_token_digest = ?
                      AND last_heartbeat_at = ?
                    """,
                    (
                        observed_at,
                        observed_at,
                        current.member_id,
                        _token_digest(grant.holder_token),
                        current.last_heartbeat_at,
                    ),
                )
                if updated.rowcount != 1:
                    raise ActorV2IngressDrainConflict(
                        "participant changed while recording heartbeat"
                    )
            return tuple(
                _load_participant_required(conn, current.member_id)
                for current, _grant in current_pairs
            )

    def validate_participant_grant(
        self,
        grant: ActorV2IngressParticipantGrant,
    ) -> ActorV2IngressParticipant:
        """Require one local participant capability to remain active and current."""

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self.validate_participant_grant_in_transaction(conn, grant)

    def validate_participant_grant_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2IngressParticipantGrant,
    ) -> ActorV2IngressParticipant:
        """Validate one active participant holder inside a caller transaction."""

        return _require_active_grant(conn, grant)

    def retire(
        self,
        grant: ActorV2IngressParticipantGrant,
    ) -> ActorV2IngressParticipant:
        """Record that a member stopped its adapter after local shutdown proof.

        The repository cannot stop an adapter.  Callers must stop local ingress
        first; this method records the terminal fact only after the exact local
        holder presents its capability.  An unacknowledged request snapshot
        blocks retirement, preserving the missing member as a hard failure.
        """

        return self.retire_participants((grant,))[0]

    def retire_participants(
        self,
        grants: Iterable[ActorV2IngressParticipantGrant],
    ) -> tuple[ActorV2IngressParticipant, ...]:
        """Atomically retire a stopped process's complete member set.

        Every member is validated and checked for unacknowledged adapter or
        core-ingress drain work before any terminal update occurs. A blocked
        member therefore leaves *all* supplied members active, preserving the
        process as a visible hard failure rather than allowing a partial
        process retirement to hide drain coverage.
        """

        normalized_grants = _participant_grants(grants)
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current_pairs = tuple(
                (_require_active_grant(conn, grant), grant)
                for grant in normalized_grants
            )
            for current, _grant in current_pairs:
                _require_no_unacknowledged_snapshot(conn, current.member_id)
            for current, grant in current_pairs:
                retired_at = max(now, current.updated_at)
                updated = conn.execute(
                    """
                    UPDATE agent_runtime_actor_v2_ingress_participants
                    SET status = 'retired', updated_at = ?, retired_at = ?
                    WHERE member_id = ?
                      AND status = 'active'
                      AND holder_token_digest = ?
                    """,
                    (
                        retired_at,
                        retired_at,
                        current.member_id,
                        _token_digest(grant.holder_token),
                    ),
                )
                if updated.rowcount != 1:
                    raise ActorV2IngressDrainConflict(
                        "participant changed while retiring"
                    )
            return tuple(
                _load_participant_required(conn, current.member_id)
                for current, _grant in current_pairs
            )

    def revoke_with_stop_proof(
        self,
        participant: ActorV2IngressParticipant,
        *,
        stop_proof: ActorV2IngressStopProof,
    ) -> ActorV2IngressParticipant:
        """Record an externally proven stop for one exact unresponsive member.

        The caller must pass the currently observed token-free snapshot.  An
        old snapshot cannot revoke a newer process incarnation or a member that
        has heartbeated since it was observed.  The proof does not satisfy any
        missing drain acknowledgement.
        """

        if not isinstance(participant, ActorV2IngressParticipant):
            raise TypeError("participant must be an ActorV2IngressParticipant")
        if not isinstance(stop_proof, ActorV2IngressStopProof):
            raise TypeError("stop_proof must be an ActorV2IngressStopProof")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = _load_participant_required(conn, participant.member_id)
            if current != participant or not current.active:
                raise ActorV2IngressDrainConflict(
                    "participant changed before external stop proof was applied"
                )
            _require_no_unacknowledged_snapshot(conn, current.member_id)
            revoked_at = max(now, current.updated_at)
            updated = conn.execute(
                """
                UPDATE agent_runtime_actor_v2_ingress_participants
                SET status = 'revoked', updated_at = ?, revoked_at = ?,
                    stop_proof_issuer_id = ?, stop_proof_epoch = ?,
                    stop_proof_digest = ?, stop_proof_summary_code = ?
                WHERE member_id = ?
                  AND status = 'active'
                  AND updated_at = ?
                """,
                (
                    revoked_at,
                    revoked_at,
                    stop_proof.issuer_id,
                    stop_proof.proof_epoch,
                    stop_proof.digest,
                    stop_proof.summary_code,
                    current.member_id,
                    current.updated_at,
                ),
            )
            if updated.rowcount != 1:
                raise ActorV2IngressDrainConflict(
                    "participant changed while applying external stop proof"
                )
            return _load_participant_required(conn, current.member_id)

    def begin_drain(
        self,
        *,
        cutover_id: str,
        admission_grant: ActorV2AdmissionGrant,
    ) -> ActorV2IngressDrainRequest:
        """Seal all current adapter memberships for an admission-reserved cutover.

        The snapshot and adapter registration gate are committed in one SQLite
        writer transaction.  A registration that wins the race is included;
        one that loses is rejected until this request is resolved by a future
        controller.  Every named adapter needs at least one active member.
        """

        normalized_cutover_id = _identifier(cutover_id, "cutover_id")
        if not isinstance(admission_grant, ActorV2AdmissionGrant):
            raise TypeError("admission_grant must be an ActorV2AdmissionGrant")
        request_id = _identifier(self._request_id_factory(), "request_id")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            fence = self._db.actor_v2_admission_fences.require_reserved_in_transaction(
                conn,
                admission_grant,
                now=now,
            )
            identity = _require_admission_reserved_cutover(
                conn,
                cutover_id=normalized_cutover_id,
                admission_fence_id=fence.fence_id,
                admission_fence_generation=fence.generation,
                key=fence.key,
            )
            existing = conn.execute(
                """
                SELECT request_id
                FROM agent_session_actor_v2_ingress_drain_requests
                WHERE cutover_id = ?
                """,
                (normalized_cutover_id,),
            ).fetchone()
            if existing is not None:
                raise ActorV2IngressDrainConflict(
                    "cutover already has a durable ingress drain request"
                )
            member_rows: list[Row] = []
            missing_adapters: list[str] = []
            for adapter_id in identity.adapter_instance_ids:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM agent_runtime_actor_v2_ingress_participants
                    WHERE adapter_instance_id = ? AND status = 'active'
                    ORDER BY participant_id, participant_epoch, member_id
                    """,
                    (adapter_id,),
                ).fetchall()
                if not rows:
                    missing_adapters.append(adapter_id)
                member_rows.extend(rows)
            if missing_adapters:
                raise ActorV2IngressDrainCoverageError(tuple(missing_adapters))
            conn.execute(
                """
                INSERT INTO agent_session_actor_v2_ingress_drain_requests (
                    request_id, cutover_id, cutover_epoch, profile_id, session_id,
                    legacy_session_id, adapter_instance_ids_json,
                    admission_fence_id, admission_fence_generation, status,
                    created_at, updated_at, drained_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'assembling', ?, ?, NULL)
                """,
                (
                    request_id,
                    normalized_cutover_id,
                    identity.cutover_epoch,
                    identity.key.profile_id,
                    identity.key.session_id,
                    identity.legacy_session_id,
                    _encode_adapter_instance_ids(identity.adapter_instance_ids),
                    fence.fence_id,
                    fence.generation,
                    now,
                    now,
                ),
            )
            for row in member_rows:
                conn.execute(
                    """
                    INSERT INTO agent_session_actor_v2_ingress_drain_members (
                        request_id, member_id, adapter_instance_id,
                        participant_id, participant_epoch
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        str(row["member_id"]),
                        str(row["adapter_instance_id"]),
                        str(row["participant_id"]),
                        int(row["participant_epoch"]),
                    ),
                )
            opened = conn.execute(
                """
                UPDATE agent_session_actor_v2_ingress_drain_requests
                SET status = 'open', updated_at = ?
                WHERE request_id = ? AND status = 'assembling'
                """,
                (now, request_id),
            )
            if opened.rowcount != 1:
                raise ActorV2IngressDrainConflict(
                    "ingress drain request changed while sealing its member snapshot"
                )
            return _load_request_required(conn, request_id)

    def get_request(self, request_id: str) -> ActorV2IngressDrainRequest | None:
        """Return one token-free request and all immutable acknowledgements."""

        normalized_id = _identifier(request_id, "request_id")
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM agent_session_actor_v2_ingress_drain_requests
                WHERE request_id = ?
                """,
                (normalized_id,),
            ).fetchone()
            return _request_from_row(conn, row) if row is not None else None

    def get_request_for_cutover(
        self,
        cutover_id: str,
    ) -> ActorV2IngressDrainRequest | None:
        """Return the single non-reusable request attached to one cutover id."""

        normalized_id = _identifier(cutover_id, "cutover_id")
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM agent_session_actor_v2_ingress_drain_requests
                WHERE cutover_id = ?
                """,
                (normalized_id,),
            ).fetchone()
            return _request_from_row(conn, row) if row is not None else None

    def acknowledge_quiescent(
        self,
        *,
        request_id: str,
        grant: ActorV2IngressParticipantGrant,
        receipt: ActorV2IngressDrainReceipt,
    ) -> ActorV2IngressDrainAcknowledgement:
        """Persist one exact participant's local pause-and-drain receipt.

        The raw local tickets remain in the process.  Only bounded digests and
        summary metadata are persisted.  Retrying the same receipt is
        idempotent; replacing a recorded receipt is rejected.
        """

        normalized_request_id = _identifier(request_id, "request_id")
        if not isinstance(grant, ActorV2IngressParticipantGrant):
            raise TypeError("grant must be an ActorV2IngressParticipantGrant")
        if not isinstance(receipt, ActorV2IngressDrainReceipt):
            raise TypeError("receipt must be an ActorV2IngressDrainReceipt")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            request = _load_request_required(conn, normalized_request_id)
            existing = _load_acknowledgement(conn, normalized_request_id, grant.participant.member_id)
            if existing is not None:
                if existing.receipt != receipt:
                    raise ActorV2IngressDrainConflict(
                        "participant attempted to replace an immutable drain receipt"
                    )
                return existing
            if request.status is not ActorV2IngressDrainStatus.OPEN:
                raise ActorV2IngressDrainConflict(
                    "ingress drain request no longer accepts acknowledgements"
                )
            participant = _require_active_grant(conn, grant)
            member = conn.execute(
                """
                SELECT request_id, member_id, adapter_instance_id,
                       participant_id, participant_epoch
                FROM agent_session_actor_v2_ingress_drain_members
                WHERE request_id = ? AND member_id = ?
                """,
                (normalized_request_id, participant.member_id),
            ).fetchone()
            if member is None:
                raise ActorV2IngressDrainConflict(
                    "participant is not covered by this ingress drain request"
                )
            if (
                str(member["adapter_instance_id"]) != participant.adapter_instance_id
                or str(member["participant_id"]) != participant.participant_id
                or int(member["participant_epoch"]) != participant.participant_epoch
            ):
                raise ActorV2IngressDrainConflict(
                    "participant incarnation does not match the frozen drain member"
                )
            acknowledged_at = max(now, request.updated_at)
            conn.execute(
                """
                INSERT INTO agent_session_actor_v2_ingress_drain_acknowledgements (
                    request_id, member_id, adapter_pause_digest,
                    legacy_quiescence_digest, proof_epoch, summary_code,
                    acknowledged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_request_id,
                    participant.member_id,
                    receipt.adapter_pause_digest,
                    receipt.legacy_quiescence_digest,
                    receipt.proof_epoch,
                    receipt.summary_code,
                    acknowledged_at,
                ),
            )
            updated_request = conn.execute(
                """
                UPDATE agent_session_actor_v2_ingress_drain_requests
                SET updated_at = ?
                WHERE request_id = ? AND status = 'open'
                """,
                (acknowledged_at, normalized_request_id),
            )
            if updated_request.rowcount != 1:
                raise ActorV2IngressDrainConflict(
                    "ingress drain request changed while recording acknowledgement"
                )
            acknowledgement = _load_acknowledgement(
                conn,
                normalized_request_id,
                participant.member_id,
            )
            if acknowledgement is None:
                raise ActorV2IngressDrainConflict(
                    "acknowledgement disappeared after insertion"
                )
            return acknowledgement

    def confirm_drained(
        self,
        *,
        request_id: str,
        admission_grant: ActorV2AdmissionGrant,
    ) -> ActorV2IngressDrainRequest:
        """Durably confirm that every frozen member acknowledged this request."""

        normalized_request_id = _identifier(request_id, "request_id")
        if not isinstance(admission_grant, ActorV2AdmissionGrant):
            raise TypeError("admission_grant must be an ActorV2AdmissionGrant")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            fence = self._db.actor_v2_admission_fences.require_reserved_in_transaction(
                conn,
                admission_grant,
                now=now,
            )
            request = _load_request_required(conn, normalized_request_id)
            _require_request_admission(request, fence.key, fence.fence_id, fence.generation)
            _require_admission_reserved_cutover(
                conn,
                cutover_id=request.cutover_id,
                admission_fence_id=fence.fence_id,
                admission_fence_generation=fence.generation,
                key=fence.key,
            )
            if request.status is ActorV2IngressDrainStatus.DRAINED:
                return request
            if request.status is not ActorV2IngressDrainStatus.OPEN:
                raise ActorV2IngressDrainConflict(
                    "ingress drain request is not open for confirmation"
                )
            missing_members = request.unacknowledged_members
            if missing_members:
                missing_ids = ", ".join(member.member_id for member in missing_members)
                raise ActorV2IngressDrainNotReady(
                    "ingress drain has unacknowledged members: " + missing_ids
                )
            drained_at = max(now, request.updated_at)
            updated = conn.execute(
                """
                UPDATE agent_session_actor_v2_ingress_drain_requests
                SET status = 'drained', updated_at = ?, drained_at = ?
                WHERE request_id = ? AND status = 'open'
                """,
                (drained_at, drained_at, normalized_request_id),
            )
            if updated.rowcount != 1:
                raise ActorV2IngressDrainConflict(
                    "ingress drain request changed while confirming"
                )
            return _load_request_required(conn, normalized_request_id)


def _require_active_grant(
    conn: Connection,
    grant: ActorV2IngressParticipantGrant,
) -> ActorV2IngressParticipant:
    """Require a current active row and matching opaque holder capability."""

    if not isinstance(grant, ActorV2IngressParticipantGrant):
        raise TypeError("grant must be an ActorV2IngressParticipantGrant")
    current = _load_participant_required(conn, grant.participant.member_id)
    expected = grant.participant
    if (
        current.adapter_instance_id != expected.adapter_instance_id
        or current.participant_id != expected.participant_id
        or current.participant_epoch != expected.participant_epoch
        or not current.active
    ):
        raise ActorV2IngressDrainConflict("participant grant no longer names an active member")
    row = conn.execute(
        """
        SELECT holder_token_digest
        FROM agent_runtime_actor_v2_ingress_participants
        WHERE member_id = ?
        """,
        (current.member_id,),
    ).fetchone()
    if row is None or str(row["holder_token_digest"]) != _token_digest(grant.holder_token):
        raise ActorV2IngressDrainConflict("participant holder capability no longer matches")
    return current


def _require_adapter_unfrozen(conn: Connection, adapter_instance_id: str) -> None:
    """Reject new members while either durable drain covers their adapter."""

    placeholders = ", ".join("?" for _ in _PENDING_REQUEST_STATUSES)
    row = conn.execute(
        f"""
        SELECT request.request_id
        FROM agent_session_actor_v2_ingress_drain_requests AS request
        JOIN agent_session_actor_v2_ingress_drain_members AS member
          ON member.request_id = request.request_id
        WHERE member.adapter_instance_id = ?
          AND request.status IN ({placeholders})
        LIMIT 1
        """,
        (adapter_instance_id, *_PENDING_REQUEST_STATUSES),
    ).fetchone()
    if row is not None:
        raise ActorV2IngressDrainConflict(
            "adapter ingress membership is frozen by request " + str(row["request_id"])
        )
    core_placeholders = ", ".join(
        "?" for _ in _PENDING_CORE_INGRESS_DRAIN_REQUEST_STATUSES
    )
    core_row = conn.execute(
        f"""
        SELECT request.request_id
        FROM agent_session_actor_v2_core_ingress_drain_requests AS request
        JOIN agent_session_actor_v2_core_ingress_drain_members AS member
          ON member.request_id = request.request_id
        WHERE member.adapter_instance_id = ?
          AND request.status IN ({core_placeholders})
        LIMIT 1
        """,
        (adapter_instance_id, *_PENDING_CORE_INGRESS_DRAIN_REQUEST_STATUSES),
    ).fetchone()
    if core_row is not None:
        raise ActorV2IngressDrainConflict(
            "adapter ingress membership is frozen by core request "
            + str(core_row["request_id"])
        )


def _require_no_unacknowledged_snapshot(conn: Connection, member_id: str) -> None:
    """Keep a member visible until every covered drain has its receipt."""

    placeholders = ", ".join("?" for _ in _PENDING_REQUEST_STATUSES)
    row = conn.execute(
        f"""
        SELECT request.request_id
        FROM agent_session_actor_v2_ingress_drain_requests AS request
        JOIN agent_session_actor_v2_ingress_drain_members AS member
          ON member.request_id = request.request_id
        LEFT JOIN agent_session_actor_v2_ingress_drain_acknowledgements AS acknowledgement
          ON acknowledgement.request_id = member.request_id
         AND acknowledgement.member_id = member.member_id
        WHERE member.member_id = ?
          AND request.status IN ({placeholders})
          AND acknowledgement.member_id IS NULL
        LIMIT 1
        """,
        (member_id, *_PENDING_REQUEST_STATUSES),
    ).fetchone()
    if row is not None:
        raise ActorV2IngressDrainConflict(
            "participant cannot terminate before acknowledging drain request "
            + str(row["request_id"])
        )
    core_placeholders = ", ".join(
        "?" for _ in _PENDING_CORE_INGRESS_DRAIN_REQUEST_STATUSES
    )
    core_row = conn.execute(
        f"""
        SELECT request.request_id
        FROM agent_session_actor_v2_core_ingress_drain_requests AS request
        JOIN agent_session_actor_v2_core_ingress_drain_members AS member
          ON member.request_id = request.request_id
        LEFT JOIN agent_session_actor_v2_core_ingress_drain_acknowledgements
          AS acknowledgement
          ON acknowledgement.request_id = member.request_id
         AND acknowledgement.member_id = member.member_id
        WHERE member.member_id = ?
          AND request.status IN ({core_placeholders})
          AND acknowledgement.member_id IS NULL
        LIMIT 1
        """,
        (member_id, *_PENDING_CORE_INGRESS_DRAIN_REQUEST_STATUSES),
    ).fetchone()
    if core_row is not None:
        raise ActorV2IngressDrainConflict(
            "participant cannot terminate before acknowledging core drain request "
            + str(core_row["request_id"])
        )


class _CutoverDrainIdentity:
    """Private decoded identity from the journal's admission-reserved phase."""

    def __init__(
        self,
        *,
        key: SessionKey,
        cutover_epoch: int,
        legacy_session_id: str,
        adapter_instance_ids: tuple[str, ...],
    ) -> None:
        self.key = key
        self.cutover_epoch = cutover_epoch
        self.legacy_session_id = legacy_session_id
        self.adapter_instance_ids = adapter_instance_ids


def _require_admission_reserved_cutover(
    conn: Connection,
    *,
    cutover_id: str,
    admission_fence_id: str,
    admission_fence_generation: int,
    key: SessionKey,
) -> _CutoverDrainIdentity:
    """Bind a request operation to the exact journal and live fence identity."""

    row = conn.execute(
        """
        SELECT profile_id, session_id, cutover_epoch, legacy_session_id,
               adapter_instance_ids_json, phase, admission_fence_id,
               admission_fence_generation
        FROM agent_session_actor_v2_cutover_journal
        WHERE cutover_id = ?
        """,
        (cutover_id,),
    ).fetchone()
    if row is None:
        raise ActorV2IngressDrainNotFound("cutover journal does not exist")
    if (
        str(row["phase"]) != "admission_reserved"
        or str(row["admission_fence_id"]) != admission_fence_id
        or int(row["admission_fence_generation"]) != admission_fence_generation
        or str(row["profile_id"]) != key.profile_id
        or str(row["session_id"]) != key.session_id
    ):
        raise ActorV2IngressDrainConflict(
            "cutover journal is not bound to this live admission reservation"
        )
    try:
        adapter_values = json.loads(str(row["adapter_instance_ids_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActorV2IngressDrainConflict(
            "cutover journal has invalid adapter membership identity"
        ) from exc
    adapter_instance_ids = _adapter_instance_ids(adapter_values)
    if adapter_values != list(adapter_instance_ids):
        raise ActorV2IngressDrainConflict(
            "cutover journal adapter identities are not canonical"
        )
    return _CutoverDrainIdentity(
        key=key,
        cutover_epoch=_positive_int(row["cutover_epoch"], "cutover_epoch"),
        legacy_session_id=_identifier(row["legacy_session_id"], "legacy_session_id"),
        adapter_instance_ids=adapter_instance_ids,
    )


def _require_request_admission(
    request: ActorV2IngressDrainRequest,
    key: SessionKey,
    fence_id: str,
    fence_generation: int,
) -> None:
    """Require the controller capability to name this exact drain request."""

    if (
        request.key != key
        or request.admission_fence_id != fence_id
        or request.admission_fence_generation != fence_generation
    ):
        raise ActorV2IngressDrainConflict(
            "admission reservation does not match ingress drain request"
        )


def _load_participant_required(conn: Connection, member_id: str) -> ActorV2IngressParticipant:
    """Load one membership row or fail closed when durable history is absent."""

    row = conn.execute(
        """
        SELECT *
        FROM agent_runtime_actor_v2_ingress_participants
        WHERE member_id = ?
        """,
        (member_id,),
    ).fetchone()
    if row is None:
        raise ActorV2IngressDrainNotFound("ingress participant does not exist")
    return _participant_from_row(row)


def _participant_from_row(row: Row) -> ActorV2IngressParticipant:
    """Decode one durable participant without exposing its holder token digest."""

    try:
        status = ActorV2IngressParticipantStatus(str(row["status"]))
        stop_proof = (
            ActorV2IngressStopProof(
                issuer_id=str(row["stop_proof_issuer_id"]),
                proof_epoch=int(row["stop_proof_epoch"]),
                digest=str(row["stop_proof_digest"]),
                summary_code=str(row["stop_proof_summary_code"]),
            )
            if status is ActorV2IngressParticipantStatus.REVOKED
            else None
        )
        return ActorV2IngressParticipant(
            member_id=str(row["member_id"]),
            adapter_instance_id=str(row["adapter_instance_id"]),
            participant_id=str(row["participant_id"]),
            participant_epoch=int(row["participant_epoch"]),
            status=status,
            registered_at=float(row["registered_at"]),
            last_heartbeat_at=float(row["last_heartbeat_at"]),
            updated_at=float(row["updated_at"]),
            retired_at=(float(row["retired_at"]) if row["retired_at"] is not None else None),
            revoked_at=(float(row["revoked_at"]) if row["revoked_at"] is not None else None),
            stop_proof=stop_proof,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActorV2IngressDrainConflict(
            "ingress participant contains invalid durable state"
        ) from exc


def _load_request_required(conn: Connection, request_id: str) -> ActorV2IngressDrainRequest:
    """Load one full request graph or reject a missing durable request."""

    row = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_ingress_drain_requests
        WHERE request_id = ?
        """,
        (request_id,),
    ).fetchone()
    if row is None:
        raise ActorV2IngressDrainNotFound("ingress drain request does not exist")
    return _request_from_row(conn, row)


def _request_from_row(conn: Connection, row: Row) -> ActorV2IngressDrainRequest:
    """Decode a request together with its immutable member and ack records."""

    request_id = str(row["request_id"])
    member_rows = conn.execute(
        """
        SELECT request_id, member_id, adapter_instance_id, participant_id, participant_epoch
        FROM agent_session_actor_v2_ingress_drain_members
        WHERE request_id = ?
        ORDER BY adapter_instance_id, participant_id, participant_epoch, member_id
        """,
        (request_id,),
    ).fetchall()
    acknowledgement_rows = conn.execute(
        """
        SELECT request_id, member_id, adapter_pause_digest, legacy_quiescence_digest,
               proof_epoch, summary_code, acknowledged_at
        FROM agent_session_actor_v2_ingress_drain_acknowledgements
        WHERE request_id = ?
        ORDER BY member_id
        """,
        (request_id,),
    ).fetchall()
    try:
        adapter_values = json.loads(str(row["adapter_instance_ids_json"]))
        adapter_instance_ids = _adapter_instance_ids(adapter_values)
        if adapter_values != list(adapter_instance_ids):
            raise ActorV2IngressDrainConflict(
                "ingress drain request adapter identities are not canonical"
            )
        members = tuple(
            ActorV2IngressDrainMember(
                request_id=str(member["request_id"]),
                member_id=str(member["member_id"]),
                adapter_instance_id=str(member["adapter_instance_id"]),
                participant_id=str(member["participant_id"]),
                participant_epoch=int(member["participant_epoch"]),
            )
            for member in member_rows
        )
        acknowledgements = tuple(
            ActorV2IngressDrainAcknowledgement(
                request_id=str(acknowledgement["request_id"]),
                member_id=str(acknowledgement["member_id"]),
                adapter_pause_digest=str(acknowledgement["adapter_pause_digest"]),
                legacy_quiescence_digest=str(
                    acknowledgement["legacy_quiescence_digest"]
                ),
                proof_epoch=int(acknowledgement["proof_epoch"]),
                summary_code=str(acknowledgement["summary_code"]),
                acknowledged_at=float(acknowledgement["acknowledged_at"]),
            )
            for acknowledgement in acknowledgement_rows
        )
        return ActorV2IngressDrainRequest(
            request_id=request_id,
            cutover_id=str(row["cutover_id"]),
            cutover_epoch=int(row["cutover_epoch"]),
            key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
            legacy_session_id=str(row["legacy_session_id"]),
            adapter_instance_ids=adapter_instance_ids,
            admission_fence_id=str(row["admission_fence_id"]),
            admission_fence_generation=int(row["admission_fence_generation"]),
            status=ActorV2IngressDrainStatus(str(row["status"])),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            drained_at=(float(row["drained_at"]) if row["drained_at"] is not None else None),
            members=members,
            acknowledgements=acknowledgements,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActorV2IngressDrainConflict(
            "ingress drain request contains invalid durable state"
        ) from exc


def _load_acknowledgement(
    conn: Connection,
    request_id: str,
    member_id: str,
) -> ActorV2IngressDrainAcknowledgement | None:
    """Return one immutable acknowledgement, if a member already supplied it."""

    row = conn.execute(
        """
        SELECT request_id, member_id, adapter_pause_digest, legacy_quiescence_digest,
               proof_epoch, summary_code, acknowledged_at
        FROM agent_session_actor_v2_ingress_drain_acknowledgements
        WHERE request_id = ? AND member_id = ?
        """,
        (request_id, member_id),
    ).fetchone()
    if row is None:
        return None
    try:
        return ActorV2IngressDrainAcknowledgement(
            request_id=str(row["request_id"]),
            member_id=str(row["member_id"]),
            adapter_pause_digest=str(row["adapter_pause_digest"]),
            legacy_quiescence_digest=str(row["legacy_quiescence_digest"]),
            proof_epoch=int(row["proof_epoch"]),
            summary_code=str(row["summary_code"]),
            acknowledged_at=float(row["acknowledged_at"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActorV2IngressDrainConflict(
            "ingress drain acknowledgement contains invalid durable state"
        ) from exc


def _encode_adapter_instance_ids(adapter_instance_ids: tuple[str, ...]) -> str:
    """Persist one canonical adapter identity list."""

    return json.dumps(list(adapter_instance_ids), ensure_ascii=True, separators=(",", ":"))


def _adapter_instance_ids(values: object) -> tuple[str, ...]:
    """Normalize the journal's adapter identity list without accepting strings."""

    if isinstance(values, str):
        raise TypeError("adapter_instance_ids must not be a string")
    try:
        normalized = tuple(_identifier(value, "adapter_instance_id") for value in values)
    except TypeError as exc:
        raise TypeError("adapter_instance_ids must be iterable") from exc
    if not normalized or len(set(normalized)) != len(normalized):
        raise ValueError("adapter_instance_ids must be a non-empty unique set")
    return tuple(sorted(normalized))


def _participant_grants(
    values: Iterable[ActorV2IngressParticipantGrant],
) -> tuple[ActorV2IngressParticipantGrant, ...]:
    """Normalize one non-empty, non-overlapping local holder capability set."""

    try:
        grants = tuple(values)
    except TypeError as exc:
        raise TypeError("participant grants must be iterable") from exc
    if not grants:
        raise ValueError("participant grants must not be empty")
    if any(not isinstance(grant, ActorV2IngressParticipantGrant) for grant in grants):
        raise TypeError("participant grants must contain ActorV2IngressParticipantGrant values")
    member_ids = tuple(grant.participant.member_id for grant in grants)
    if len(set(member_ids)) != len(member_ids):
        raise ValueError("participant grants cannot repeat a member")
    return tuple(
        sorted(
            grants,
            key=lambda grant: (
                grant.participant.adapter_instance_id,
                grant.participant.participant_id,
                grant.participant.participant_epoch,
                grant.participant.member_id,
            ),
        )
    )


def _identifier(value: object, field_name: str) -> str:
    """Normalize one durable non-empty identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _positive_int(value: object, field_name: str) -> int:
    """Require one positive integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _finite_time(value: object, field_name: str) -> float:
    """Reject non-finite timestamps before a durable write."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite")
    return normalized


def _token_digest(token: str) -> str:
    """Hash a local holder capability before it reaches durable storage."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


__all__ = ["ActorV2IngressDrainRepository"]
