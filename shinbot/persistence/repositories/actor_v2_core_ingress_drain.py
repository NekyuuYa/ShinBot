"""Durable per-process core-ingress drain receipts for a migration barrier."""

from __future__ import annotations

import json
import math
import time
import uuid
from collections.abc import Callable
from sqlite3 import Connection, Row

from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainAcknowledgement,
    ActorV2CoreIngressDrainConflict,
    ActorV2CoreIngressDrainCoverageError,
    ActorV2CoreIngressDrainDiscoveryCursor,
    ActorV2CoreIngressDrainDiscoveryPage,
    ActorV2CoreIngressDrainMember,
    ActorV2CoreIngressDrainNotFound,
    ActorV2CoreIngressDrainNotReady,
    ActorV2CoreIngressDrainReceipt,
    ActorV2CoreIngressDrainRequest,
    ActorV2CoreIngressDrainStatus,
)
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressParticipantGrant,
)
from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierGrant,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.persistence.repositories.base import Repository


class ActorV2CoreIngressDrainRepository(Repository):
    """Seal all core ingress process members for one active migration barrier.

    The repository does not contact processes, call ``MessageIngress``, or
    start a poller. It only persists a non-shrinking coverage set and validates
    token-free receipts that an unmounted local worker supplies.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        request_id_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize durable request identity creation with injectable clock."""

        super().__init__(db)
        self._clock = clock or time.time
        self._request_id_factory = request_id_factory or (lambda: uuid.uuid4().hex)

    @property
    def persistence_domain(self) -> object:
        """Return the shared database domain protecting barrier and membership."""

        return self._db

    def begin_drain(
        self,
        barrier_grant: ActorV2MigrationBarrierGrant,
    ) -> ActorV2CoreIngressDrainRequest:
        """Seal every active process member for the barrier's adapter topology."""

        if not isinstance(barrier_grant, ActorV2MigrationBarrierGrant):
            raise TypeError("barrier_grant must be an ActorV2MigrationBarrierGrant")
        request_id = _identifier(self._request_id_factory(), "request_id")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            barrier = self._db.actor_v2_migration_barriers.validate_in_transaction(
                conn,
                barrier_grant,
            )
            _require_migrating_ownership(conn, barrier)
            existing = conn.execute(
                """
                SELECT request_id
                FROM agent_session_actor_v2_core_ingress_drain_requests
                WHERE barrier_id = ?
                """,
                (barrier.barrier_id,),
            ).fetchone()
            if existing is not None:
                raise ActorV2CoreIngressDrainConflict(
                    "migration barrier already has a core ingress drain request"
                )
            member_rows: list[Row] = []
            missing_adapters: list[str] = []
            for adapter_instance_id in barrier.adapter_instance_ids:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM agent_runtime_actor_v2_ingress_participants
                    WHERE adapter_instance_id = ? AND status = 'active'
                    ORDER BY participant_id, participant_epoch, member_id
                    """,
                    (adapter_instance_id,),
                ).fetchall()
                if not rows:
                    missing_adapters.append(adapter_instance_id)
                member_rows.extend(rows)
            if missing_adapters:
                raise ActorV2CoreIngressDrainCoverageError(tuple(missing_adapters))
            conn.execute(
                """
                INSERT INTO agent_session_actor_v2_core_ingress_drain_requests (
                    request_id, barrier_id, profile_id, session_id,
                    legacy_session_id, adapter_instance_ids_json,
                    source_generation, migration_generation, status,
                    created_at, updated_at, drained_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'assembling', ?, ?, NULL)
                """,
                (
                    request_id,
                    barrier.barrier_id,
                    barrier.key.profile_id,
                    barrier.key.session_id,
                    barrier.legacy_session_id,
                    _encode_adapter_instance_ids(barrier.adapter_instance_ids),
                    barrier.source_generation,
                    barrier.migration_generation,
                    now,
                    now,
                ),
            )
            for member in member_rows:
                conn.execute(
                    """
                    INSERT INTO agent_session_actor_v2_core_ingress_drain_members (
                        request_id, member_id, adapter_instance_id,
                        participant_id, participant_epoch
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        str(member["member_id"]),
                        str(member["adapter_instance_id"]),
                        str(member["participant_id"]),
                        int(member["participant_epoch"]),
                    ),
                )
            opened = conn.execute(
                """
                UPDATE agent_session_actor_v2_core_ingress_drain_requests
                SET status = 'open', updated_at = ?
                WHERE request_id = ? AND status = 'assembling'
                """,
                (now, request_id),
            )
            if opened.rowcount != 1:
                raise ActorV2CoreIngressDrainConflict(
                    "core ingress request changed while sealing membership"
                )
            return _load_required_request(conn, request_id)

    def get(self, request_id: str) -> ActorV2CoreIngressDrainRequest | None:
        """Return one token-free core ingress request and its receipts."""

        normalized_request_id = _identifier(request_id, "request_id")
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM agent_session_actor_v2_core_ingress_drain_requests
                WHERE request_id = ?
                """,
                (normalized_request_id,),
            ).fetchone()
            return _request_from_row(conn, row) if row is not None else None

    def get_for_barrier(
        self,
        barrier_id: str,
    ) -> ActorV2CoreIngressDrainRequest | None:
        """Return the one non-reusable request bound to a migration barrier."""

        normalized_barrier_id = _identifier(barrier_id, "barrier_id")
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM agent_session_actor_v2_core_ingress_drain_requests
                WHERE barrier_id = ?
                """,
                (normalized_barrier_id,),
            ).fetchone()
            return _request_from_row(conn, row) if row is not None else None

    def discover_open_for_participant(
        self,
        participant_id: str,
        *,
        limit: int = 100,
        after: ActorV2CoreIngressDrainDiscoveryCursor | None = None,
    ) -> ActorV2CoreIngressDrainDiscoveryPage:
        """Return open requests with unacknowledged members for one process.

        Discovery exposes only frozen membership addressed to ``participant_id``.
        It neither proves local quiescence nor confirms the request; the caller
        must still hold the opaque participant grants used by the local worker.
        """

        normalized_participant_id = _identifier(participant_id, "participant_id")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer between 1 and 100")
        if after is not None and not isinstance(
            after,
            ActorV2CoreIngressDrainDiscoveryCursor,
        ):
            raise TypeError("after must be an ActorV2CoreIngressDrainDiscoveryCursor")
        query = """
            SELECT DISTINCT request.*
            FROM agent_session_actor_v2_core_ingress_drain_requests AS request
            JOIN agent_session_actor_v2_core_ingress_drain_members AS member
              ON member.request_id = request.request_id
            LEFT JOIN agent_session_actor_v2_core_ingress_drain_acknowledgements
              AS acknowledgement
              ON acknowledgement.request_id = member.request_id
             AND acknowledgement.member_id = member.member_id
            WHERE request.status = 'open'
              AND member.participant_id = ?
              AND acknowledgement.member_id IS NULL
        """
        parameters: list[object] = [normalized_participant_id]
        if after is not None:
            query += """
              AND (
                    request.created_at > ?
                    OR (
                        request.created_at = ?
                        AND request.request_id > ?
                    )
              )
            """
            parameters.extend((after.created_at, after.created_at, after.request_id))
        query += """
            ORDER BY request.created_at, request.request_id
            LIMIT ?
        """
        parameters.append(limit + 1)
        with self.connect() as conn:
            rows = conn.execute(query, tuple(parameters)).fetchall()
            selected_rows = rows[:limit]
            requests = tuple(_request_from_row(conn, row) for row in selected_rows)
        has_more = len(rows) > limit
        cursor = (
            ActorV2CoreIngressDrainDiscoveryCursor(
                created_at=requests[-1].created_at,
                request_id=requests[-1].request_id,
            )
            if has_more and requests
            else None
        )
        return ActorV2CoreIngressDrainDiscoveryPage(
            requests=requests,
            next_cursor=cursor,
            has_more=has_more,
        )

    def acknowledge_quiescent(
        self,
        *,
        request_id: str,
        participant_grant: ActorV2IngressParticipantGrant,
        receipt: ActorV2CoreIngressDrainReceipt,
    ) -> ActorV2CoreIngressDrainAcknowledgement:
        """Persist one exact process member's local core ingress drain receipt."""

        normalized_request_id = _identifier(request_id, "request_id")
        if not isinstance(participant_grant, ActorV2IngressParticipantGrant):
            raise TypeError("participant_grant must be an ActorV2IngressParticipantGrant")
        if not isinstance(receipt, ActorV2CoreIngressDrainReceipt):
            raise TypeError("receipt must be an ActorV2CoreIngressDrainReceipt")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            request = _load_required_request(conn, normalized_request_id)
            member_id = participant_grant.participant.member_id
            existing = _load_acknowledgement(conn, normalized_request_id, member_id)
            if existing is not None:
                if existing.receipt != receipt:
                    raise ActorV2CoreIngressDrainConflict(
                        "participant attempted to replace immutable core ingress receipt"
                    )
                return existing
            if request.status is not ActorV2CoreIngressDrainStatus.OPEN:
                raise ActorV2CoreIngressDrainConflict(
                    "core ingress request no longer accepts acknowledgements"
                )
            participant = (
                self._db.actor_v2_ingress_drains.validate_participant_grant_in_transaction(
                    conn,
                    participant_grant,
                )
            )
            member = conn.execute(
                """
                SELECT member_id, adapter_instance_id, participant_id, participant_epoch
                FROM agent_session_actor_v2_core_ingress_drain_members
                WHERE request_id = ? AND member_id = ?
                """,
                (normalized_request_id, participant.member_id),
            ).fetchone()
            if member is None:
                raise ActorV2CoreIngressDrainConflict(
                    "participant is not covered by this core ingress request"
                )
            if (
                str(member["adapter_instance_id"]) != participant.adapter_instance_id
                or str(member["participant_id"]) != participant.participant_id
                or int(member["participant_epoch"]) != participant.participant_epoch
            ):
                raise ActorV2CoreIngressDrainConflict(
                    "participant identity differs from frozen core ingress member"
                )
            acknowledged_at = max(now, request.updated_at)
            conn.execute(
                """
                INSERT INTO agent_session_actor_v2_core_ingress_drain_acknowledgements (
                    request_id, member_id, core_ingress_digest,
                    legacy_quiescence_digest, proof_epoch, summary_code,
                    acknowledged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_request_id,
                    participant.member_id,
                    receipt.core_ingress_digest,
                    receipt.legacy_quiescence_digest,
                    receipt.proof_epoch,
                    receipt.summary_code,
                    acknowledged_at,
                ),
            )
            updated = conn.execute(
                """
                UPDATE agent_session_actor_v2_core_ingress_drain_requests
                SET updated_at = ?
                WHERE request_id = ? AND status = 'open'
                """,
                (acknowledged_at, normalized_request_id),
            )
            if updated.rowcount != 1:
                raise ActorV2CoreIngressDrainConflict(
                    "core ingress request changed while recording acknowledgement"
                )
            acknowledgement = _load_acknowledgement(
                conn,
                normalized_request_id,
                participant.member_id,
            )
            if acknowledgement is None:
                raise ActorV2CoreIngressDrainConflict(
                    "core ingress acknowledgement disappeared after insertion"
                )
            return acknowledgement

    def confirm_drained(
        self,
        *,
        request_id: str,
        barrier_grant: ActorV2MigrationBarrierGrant,
    ) -> ActorV2CoreIngressDrainRequest:
        """Confirm all frozen process members acknowledged the active barrier."""

        normalized_request_id = _identifier(request_id, "request_id")
        if not isinstance(barrier_grant, ActorV2MigrationBarrierGrant):
            raise TypeError("barrier_grant must be an ActorV2MigrationBarrierGrant")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            barrier = self._db.actor_v2_migration_barriers.validate_in_transaction(
                conn,
                barrier_grant,
            )
            _require_migrating_ownership(conn, barrier)
            request = _load_required_request(conn, normalized_request_id)
            _require_request_barrier(request, barrier)
            if request.status is ActorV2CoreIngressDrainStatus.DRAINED:
                return request
            if request.status is not ActorV2CoreIngressDrainStatus.OPEN:
                raise ActorV2CoreIngressDrainConflict(
                    "core ingress request is not open for confirmation"
                )
            if request.unacknowledged_members:
                raise ActorV2CoreIngressDrainNotReady(
                    "core ingress request has unacknowledged members: "
                    + ", ".join(member.member_id for member in request.unacknowledged_members)
                )
            drained_at = max(now, request.updated_at)
            updated = conn.execute(
                """
                UPDATE agent_session_actor_v2_core_ingress_drain_requests
                SET status = 'drained', updated_at = ?, drained_at = ?
                WHERE request_id = ? AND status = 'open'
                """,
                (drained_at, drained_at, normalized_request_id),
            )
            if updated.rowcount != 1:
                raise ActorV2CoreIngressDrainConflict(
                    "core ingress request changed while confirming"
                )
            return _load_required_request(conn, normalized_request_id)

    def require_drained_for_barrier_in_transaction(
        self,
        conn: Connection,
        barrier_grant: ActorV2MigrationBarrierGrant,
    ) -> ActorV2CoreIngressDrainRequest:
        """Return the exact fully-drained request while a caller holds the writer lock.

        Future migration steps such as source-state manifest capture need the
        core-drain boundary and their own durable write in one transaction.
        Exposing this narrow validation method avoids reopening a second
        connection or duplicating the barrier/request identity checks.
        """

        if not isinstance(barrier_grant, ActorV2MigrationBarrierGrant):
            raise TypeError("barrier_grant must be an ActorV2MigrationBarrierGrant")
        barrier = self._db.actor_v2_migration_barriers.validate_in_transaction(
            conn,
            barrier_grant,
        )
        _require_migrating_ownership(conn, barrier)
        row = conn.execute(
            """
            SELECT request_id
            FROM agent_session_actor_v2_core_ingress_drain_requests
            WHERE barrier_id = ?
            """,
            (barrier.barrier_id,),
        ).fetchone()
        if row is None:
            raise ActorV2CoreIngressDrainNotFound(
                "migration barrier has no core ingress drain request"
            )
        request = _load_required_request(conn, str(row["request_id"]))
        _require_request_barrier(request, barrier)
        if not request.durably_drained:
            raise ActorV2CoreIngressDrainNotReady(
                "core ingress request is not durably drained"
            )
        return request


def _require_migrating_ownership(conn: Connection, barrier: object) -> None:
    """Require the exact durable ownership row protected by an active barrier."""

    key = getattr(barrier, "key", None)
    source_generation = getattr(barrier, "source_generation", None)
    migration_generation = getattr(barrier, "migration_generation", None)
    if not isinstance(key, SessionKey):
        raise TypeError("barrier must carry a SessionKey")
    if (
        isinstance(source_generation, bool)
        or not isinstance(source_generation, int)
        or source_generation < 1
        or isinstance(migration_generation, bool)
        or not isinstance(migration_generation, int)
        or migration_generation < 1
        or migration_generation != source_generation + 1
    ):
        raise ActorV2CoreIngressDrainConflict(
            "migration barrier has invalid ownership generation identity"
        )
    row = conn.execute(
        """
        SELECT mode, status, pending_mode, generation
        FROM agent_session_runtime_ownership
        WHERE profile_id = ? AND session_id = ?
        """,
        (key.profile_id, key.session_id),
    ).fetchone()
    if (
        row is None
        or str(row["mode"]) != AgentRuntimeOwnershipMode.LEGACY.value
        or str(row["status"]) != AgentRuntimeOwnershipStatus.MIGRATING.value
        or str(row["pending_mode"]) != AgentRuntimeOwnershipMode.ACTOR_V2.value
        or int(row["generation"]) != migration_generation
    ):
        raise ActorV2CoreIngressDrainConflict(
            "ownership no longer matches active migration barrier"
        )


def _require_request_barrier(
    request: ActorV2CoreIngressDrainRequest,
    barrier: object,
) -> None:
    """Require a controller grant to name the exact frozen request identity."""

    key = getattr(barrier, "key", None)
    barrier_id = getattr(barrier, "barrier_id", None)
    legacy_session_id = getattr(barrier, "legacy_session_id", None)
    adapter_instance_ids = getattr(barrier, "adapter_instance_ids", None)
    source_generation = getattr(barrier, "source_generation", None)
    migration_generation = getattr(barrier, "migration_generation", None)
    if (
        request.key != key
        or request.barrier_id != barrier_id
        or request.legacy_session_id != legacy_session_id
        or request.adapter_instance_ids != adapter_instance_ids
        or request.source_generation != source_generation
        or request.migration_generation != migration_generation
    ):
        raise ActorV2CoreIngressDrainConflict(
            "migration barrier does not match core ingress request"
        )


def _load_required_request(
    conn: Connection,
    request_id: str,
) -> ActorV2CoreIngressDrainRequest:
    """Load one request graph or fail closed if durable state is absent."""

    row = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_core_ingress_drain_requests
        WHERE request_id = ?
        """,
        (request_id,),
    ).fetchone()
    if row is None:
        raise ActorV2CoreIngressDrainNotFound("core ingress drain request does not exist")
    return _request_from_row(conn, row)


def _request_from_row(
    conn: Connection,
    row: Row,
) -> ActorV2CoreIngressDrainRequest:
    """Decode one request, frozen member set, and immutable receipts."""

    request_id = str(row["request_id"])
    member_rows = conn.execute(
        """
        SELECT request_id, member_id, adapter_instance_id, participant_id, participant_epoch
        FROM agent_session_actor_v2_core_ingress_drain_members
        WHERE request_id = ?
        ORDER BY adapter_instance_id, participant_id, participant_epoch, member_id
        """,
        (request_id,),
    ).fetchall()
    acknowledgement_rows = conn.execute(
        """
        SELECT request_id, member_id, core_ingress_digest, legacy_quiescence_digest,
               proof_epoch, summary_code, acknowledged_at
        FROM agent_session_actor_v2_core_ingress_drain_acknowledgements
        WHERE request_id = ?
        ORDER BY member_id
        """,
        (request_id,),
    ).fetchall()
    try:
        adapter_values = json.loads(str(row["adapter_instance_ids_json"]))
        adapter_instance_ids = _adapter_instance_ids(adapter_values)
        if adapter_values != list(adapter_instance_ids):
            raise ActorV2CoreIngressDrainConflict(
                "core ingress request adapter identities are not canonical"
            )
        members = tuple(
            ActorV2CoreIngressDrainMember(
                request_id=str(member["request_id"]),
                member_id=str(member["member_id"]),
                adapter_instance_id=str(member["adapter_instance_id"]),
                participant_id=str(member["participant_id"]),
                participant_epoch=int(member["participant_epoch"]),
            )
            for member in member_rows
        )
        acknowledgements = tuple(
            ActorV2CoreIngressDrainAcknowledgement(
                request_id=str(acknowledgement["request_id"]),
                member_id=str(acknowledgement["member_id"]),
                core_ingress_digest=str(acknowledgement["core_ingress_digest"]),
                legacy_quiescence_digest=str(
                    acknowledgement["legacy_quiescence_digest"]
                ),
                proof_epoch=int(acknowledgement["proof_epoch"]),
                summary_code=str(acknowledgement["summary_code"]),
                acknowledged_at=float(acknowledgement["acknowledged_at"]),
            )
            for acknowledgement in acknowledgement_rows
        )
        return ActorV2CoreIngressDrainRequest(
            request_id=request_id,
            barrier_id=str(row["barrier_id"]),
            key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
            legacy_session_id=str(row["legacy_session_id"]),
            adapter_instance_ids=adapter_instance_ids,
            source_generation=int(row["source_generation"]),
            migration_generation=int(row["migration_generation"]),
            status=ActorV2CoreIngressDrainStatus(str(row["status"])),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            drained_at=(float(row["drained_at"]) if row["drained_at"] is not None else None),
            members=members,
            acknowledgements=acknowledgements,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActorV2CoreIngressDrainConflict(
            "core ingress request contains invalid durable state"
        ) from exc


def _load_acknowledgement(
    conn: Connection,
    request_id: str,
    member_id: str,
) -> ActorV2CoreIngressDrainAcknowledgement | None:
    """Return one exact immutable acknowledgement, if it has been persisted."""

    row = conn.execute(
        """
        SELECT request_id, member_id, core_ingress_digest, legacy_quiescence_digest,
               proof_epoch, summary_code, acknowledged_at
        FROM agent_session_actor_v2_core_ingress_drain_acknowledgements
        WHERE request_id = ? AND member_id = ?
        """,
        (request_id, member_id),
    ).fetchone()
    if row is None:
        return None
    try:
        return ActorV2CoreIngressDrainAcknowledgement(
            request_id=str(row["request_id"]),
            member_id=str(row["member_id"]),
            core_ingress_digest=str(row["core_ingress_digest"]),
            legacy_quiescence_digest=str(row["legacy_quiescence_digest"]),
            proof_epoch=int(row["proof_epoch"]),
            summary_code=str(row["summary_code"]),
            acknowledged_at=float(row["acknowledged_at"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActorV2CoreIngressDrainConflict(
            "core ingress acknowledgement contains invalid durable state"
        ) from exc


def _encode_adapter_instance_ids(adapter_instance_ids: tuple[str, ...]) -> str:
    """Serialize canonical barrier adapter identity without runtime objects."""

    return json.dumps(list(adapter_instance_ids), ensure_ascii=True, separators=(",", ":"))


def _adapter_instance_ids(values: object) -> tuple[str, ...]:
    """Normalize one non-empty unique adapter instance set."""

    if isinstance(values, str):
        raise TypeError("adapter_instance_ids must not be a string")
    try:
        normalized = tuple(_identifier(value, "adapter_instance_id") for value in values)
    except TypeError as exc:
        raise TypeError("adapter_instance_ids must be iterable") from exc
    if not normalized or len(set(normalized)) != len(normalized):
        raise ValueError("adapter_instance_ids must be a non-empty unique set")
    return tuple(sorted(normalized))


def _identifier(value: object, field_name: str) -> str:
    """Normalize one required durable identity."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _finite_time(value: object, field_name: str) -> float:
    """Require finite timestamps before durable request updates."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field_name} must be finite")
    return numeric


__all__ = ["ActorV2CoreIngressDrainRepository"]
