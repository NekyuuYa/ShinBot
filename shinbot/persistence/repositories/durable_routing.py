"""SQLite unit-of-work for recoverable core-to-Agent message routing."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from shinbot.core.dispatch.agent_delivery import AgentRouteDelivery
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.durable_routing import (
    AGENT_ROUTE_MAILBOX_KIND,
    AGENT_ROUTE_MAILBOX_SOURCE,
    AGENT_ROUTE_OUTBOX_VERSION,
    AgentRouteOutboxStatus,
    MessageRoutingJobEnvelope,
    MessageRoutingJobStatus,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.routing import (
    MessageRoutingSkipReason,
    MessageRoutingStatus,
    routing_skip_reason_value,
    routing_status_value,
)

from .base import Repository

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager

_ROUTE_DECISION_VERSION = 1


def _routing_job_work_eligibility(table_name: str) -> str:
    """Return SQL that admits only active owners with a valid optional fence."""

    if table_name != "message_routing_jobs":
        raise ValueError("unsupported routing job table")
    return f"""
        (
            (
                {table_name}.profile_id = ''
                AND {table_name}.session_id = ''
                AND {table_name}.ownership_generation = 0
                AND {table_name}.admission_fence_id = ''
                AND {table_name}.admission_fence_generation = 0
            )
            OR EXISTS (
                SELECT 1
                FROM agent_session_runtime_ownership AS ownership
                WHERE ownership.profile_id = {table_name}.profile_id
                  AND ownership.session_id = {table_name}.session_id
                  AND ownership.status = 'active'
                  AND ownership.generation = {table_name}.ownership_generation
                  AND (
                      (
                          {table_name}.admission_fence_id = ''
                          AND {table_name}.admission_fence_generation = 0
                          AND ownership.admission_fence_id = ''
                          AND ownership.admission_fence_generation = 0
                      )
                      OR (
                          ownership.mode = 'actor_v2'
                          AND {table_name}.admission_fence_id != ''
                          AND {table_name}.admission_fence_generation >= 1
                          AND ownership.admission_fence_id =
                              {table_name}.admission_fence_id
                          AND ownership.admission_fence_generation =
                              {table_name}.admission_fence_generation
                          AND EXISTS (
                              SELECT 1
                              FROM agent_session_actor_v2_admission_fences AS fence
                              WHERE fence.profile_id = {table_name}.profile_id
                                AND fence.session_id = {table_name}.session_id
                                AND fence.fence_id = {table_name}.admission_fence_id
                                AND fence.generation =
                                    {table_name}.admission_fence_generation
                                AND fence.status = 'committed'
                                AND fence.expires_at > ?
                          )
                      )
                  )
            )
        )
    """


def _route_outbox_work_eligibility(table_name: str) -> str:
    """Return SQL that admits only the matching active Actor v2 owner."""

    if table_name != "agent_route_outbox":
        raise ValueError("unsupported Agent route outbox table")
    return f"""
        EXISTS (
            SELECT 1
            FROM agent_session_runtime_ownership AS ownership
            WHERE ownership.profile_id = {table_name}.profile_id
              AND ownership.session_id = {table_name}.session_id
              AND ownership.mode = 'actor_v2'
              AND ownership.status = 'active'
              AND ownership.generation = {table_name}.ownership_generation
              AND (
                  (
                      {table_name}.admission_fence_id = ''
                      AND {table_name}.admission_fence_generation = 0
                      AND ownership.admission_fence_id = ''
                      AND ownership.admission_fence_generation = 0
                  )
                  OR (
                      {table_name}.admission_fence_id != ''
                      AND {table_name}.admission_fence_generation >= 1
                      AND ownership.admission_fence_id =
                          {table_name}.admission_fence_id
                      AND ownership.admission_fence_generation =
                          {table_name}.admission_fence_generation
                      AND EXISTS (
                          SELECT 1
                          FROM agent_session_actor_v2_admission_fences AS fence
                          WHERE fence.profile_id = {table_name}.profile_id
                            AND fence.session_id = {table_name}.session_id
                            AND fence.fence_id = {table_name}.admission_fence_id
                            AND fence.generation =
                                {table_name}.admission_fence_generation
                            AND fence.status = 'committed'
                            AND fence.expires_at > ?
                      )
                  )
              )
        )
    """


class DurableRoutingError(RuntimeError):
    """Base error raised by durable routing persistence."""


class DurableRoutingConflict(DurableRoutingError):
    """Raised when an idempotent identity is reused for different work."""


class DurableRoutingLeaseLost(DurableRoutingError):
    """Raised when a routing worker no longer owns a live claim."""


class DurableRoutingRecordNotFound(DurableRoutingError):
    """Raised when a claimed durable routing record no longer exists."""


@dataclass(slots=True, frozen=True)
class PersistRoutingJobResult:
    """Result of atomically inserting a message log and routing job."""

    routing_job_id: str
    message_log_id: int
    inserted: bool
    status: MessageRoutingJobStatus

    @property
    def duplicate(self) -> bool:
        """Return whether the idempotent job already existed."""

        return not self.inserted


@dataclass(slots=True, frozen=True)
class PersistedMessageRoutingJob:
    """Read-only snapshot used to recognize duplicate live ingress."""

    envelope: MessageRoutingJobEnvelope
    message_log_id: int
    status: MessageRoutingJobStatus
    decision_kind: str = ""


@dataclass(slots=True, frozen=True)
class ClaimedMessageRoutingJob:
    """Lease-bound message routing work item."""

    envelope: MessageRoutingJobEnvelope
    message_log_id: int
    claim_id: str
    worker_id: str
    attempt_count: int
    claimed_at: float
    lease_expires_at: float

    @property
    def routing_job_id(self) -> str:
        """Return the durable job id."""

        return self.envelope.job_id


@dataclass(slots=True, frozen=True)
class RouteDecisionResult:
    """Committed routing decision and its Agent outbox identities."""

    routing_job_id: str
    decision_id: str
    delivery_ids: tuple[str, ...]
    inserted_delivery_count: int

    @property
    def duplicate(self) -> bool:
        """Return whether this was an idempotent replay of a committed decision."""

        return self.inserted_delivery_count == 0


@dataclass(slots=True, frozen=True)
class ClaimedAgentRouteDelivery:
    """Lease-bound Agent delivery claimed from the core routing outbox."""

    delivery: AgentRouteDelivery
    routing_job_id: str
    correlation_id: str
    causation_id: str
    ownership_generation: int
    admission_fence_id: str
    admission_fence_generation: int
    claim_id: str
    worker_id: str
    attempt_count: int
    claimed_at: float
    lease_expires_at: float

    @property
    def delivery_id(self) -> str:
        """Return the canonical delivery id."""

        return self.delivery.delivery_id


@dataclass(slots=True, frozen=True)
class RouteRelayResult:
    """Result of atomically enqueuing a mailbox event and settling outbox work."""

    delivery_id: str
    event_id: str
    mailbox_id: int
    profile_id: str
    session_id: str
    mailbox_inserted: bool
    wake_request: FencedMailboxWakeRequest

    def __post_init__(self) -> None:
        """Require the exact committed mailbox identity for post-commit hints."""

        if isinstance(self.mailbox_id, bool) or not isinstance(self.mailbox_id, int):
            raise ValueError("mailbox_id must be an integer")
        if self.mailbox_id < 1:
            raise ValueError("mailbox_id must be positive")

    @property
    def duplicate(self) -> bool:
        """Return whether the canonical mailbox event already existed."""

        return not self.mailbox_inserted


@dataclass(slots=True, frozen=True)
class RouteWakeCursor:
    """Stable keyset position for one current route mailbox wake debt."""

    mailbox_id: int
    profile_id: str
    session_id: str
    ownership_generation: int
    admission_fence_id: str = ""
    admission_fence_generation: int = 0

    def __post_init__(self) -> None:
        """Reject a cursor that cannot identify one ordered mailbox debt row."""

        if isinstance(self.mailbox_id, bool) or not isinstance(self.mailbox_id, int):
            raise ValueError("mailbox_id must be an integer")
        if self.mailbox_id < 1:
            raise ValueError("mailbox_id must be positive")
        profile_id = str(self.profile_id or "").strip()
        session_id = str(self.session_id or "").strip()
        if not profile_id or not session_id:
            raise ValueError("route wake cursor requires a complete session key")
        if (
            isinstance(self.ownership_generation, bool)
            or not isinstance(self.ownership_generation, int)
            or self.ownership_generation < 1
        ):
            raise ValueError("ownership_generation must be a positive integer")
        fence_id = str(self.admission_fence_id or "").strip()
        fence_generation = self.admission_fence_generation
        if isinstance(fence_generation, bool) or not isinstance(fence_generation, int):
            raise ValueError("admission_fence_generation must be an integer")
        if fence_generation < 0 or bool(fence_id) != bool(fence_generation):
            raise ValueError("route wake cursor fence identity is inconsistent")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "admission_fence_id", fence_id)


@dataclass(slots=True, frozen=True)
class PendingRouteWakeDebt:
    """One pending route mailbox event for an exact Actor incarnation.

    ``FencedMailboxWakeRequest`` deliberately identifies an Actor ownership
    incarnation, not an individual mailbox row.  Keeping the event identity
    alongside it lets a supervisor acknowledge one post-commit wake without
    suppressing a later route delivery for that same Actor.
    """

    request: FencedMailboxWakeRequest
    event_id: str
    cursor: RouteWakeCursor | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        """Reject incomplete cache identities before they suppress retries."""

        if not isinstance(self.request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        event_id = str(self.event_id or "").strip()
        if not event_id:
            raise ValueError("event_id must not be empty")
        cursor = self.cursor
        if cursor is not None:
            if not isinstance(cursor, RouteWakeCursor):
                raise TypeError("cursor must be a RouteWakeCursor")
            if (
                cursor.profile_id != self.request.key.profile_id
                or cursor.session_id != self.request.key.session_id
                or cursor.ownership_generation != self.request.ownership_generation
                or cursor.admission_fence_id != self.request.admission_fence_id
                or cursor.admission_fence_generation
                != self.request.admission_fence_generation
            ):
                raise ValueError("route wake cursor differs from its request identity")
        object.__setattr__(self, "event_id", event_id)


@dataclass(slots=True, frozen=True)
class _CapturedOutboxAdmission:
    """Identity captured before a route-decision candidate can mutate durable rows."""

    delivery_id: str
    request: FencedMailboxWakeRequest
    inserted: bool


class DurableMessageRoutingRepository(Repository):
    """Persist and relay recoverable message routing without Agent imports.

    Each public mutation uses a short ``BEGIN IMMEDIATE`` transaction. Model,
    adapter, tool, and runtime calls remain outside these commit boundaries.
    """

    def __init__(
        self,
        db: DatabaseManager,
        *,
        lease_seconds: float = 30.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Initialize the durable routing repository.

        Args:
            db: Database manager or compatible connection provider.
            lease_seconds: Duration of every routing and outbox claim.
            clock: Injectable wall clock for deterministic tests.
        """

        super().__init__(db)
        if lease_seconds <= 0 or not math.isfinite(float(lease_seconds)):
            raise ValueError("lease_seconds must be finite and positive")
        self._lease_seconds = float(lease_seconds)
        self._clock = clock or time.time
        self._database = db

    @property
    def legacy_recovery_gate(self) -> object:
        """Return the database-wide gate used before guarded legacy recovery."""

        return self._database.actor_v2_legacy_recovery_gate

    @property
    def persistence_domain(self) -> object:
        """Return the exact database identity shared by routing transactions."""

        return self._database

    @property
    def lease_seconds(self) -> float:
        """Return the configured claim lease duration."""

        return self._lease_seconds

    def persist_message_and_job(
        self,
        record: MessageLogRecord,
        envelope: MessageRoutingJobEnvelope,
    ) -> PersistRoutingJobResult:
        """Atomically insert one message log and its durable routing job.

        Reusing either ``job_id`` or ``idempotency_key`` returns the existing
        result only when the complete immutable input identity is equal.
        """

        self._validate_pending_message(record)
        now = self._clock()
        payload_json = _canonical_json_object(envelope.payload)
        payload_digest = _digest(payload_json)
        message_fingerprint = _message_fingerprint(record)
        occurred_at = envelope.occurred_at or now
        available_at = envelope.available_at or now
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            persisted_envelope = self._resolve_admission_envelope_for_persistence(
                conn,
                envelope,
            )
            existing = self._find_job_by_identity(conn, persisted_envelope)
            if existing is not None:
                self._validate_existing_job(
                    existing,
                    envelope=persisted_envelope,
                    message_fingerprint=message_fingerprint,
                    payload_json=payload_json,
                    payload_digest=payload_digest,
                )
                return PersistRoutingJobResult(
                    routing_job_id=persisted_envelope.job_id,
                    message_log_id=int(existing["message_log_id"]),
                    inserted=False,
                    status=_job_status(str(existing["status"])),
                )

            message_log_id = self._database.message_logs.insert_with_connection(conn, record)
            conn.execute(
                """
                INSERT INTO message_routing_jobs (
                    routing_job_id, idempotency_key, message_log_id, version,
                    profile_id, session_id, ownership_generation,
                    admission_fence_id, admission_fence_generation,
                    message_fingerprint, payload_json, payload_digest,
                    trace_id, correlation_id, causation_id, occurred_at, status,
                    attempt_count, available_at, claim_id, lease_owner,
                    lease_until, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, ?)
                """,
                (
                    persisted_envelope.job_id,
                    persisted_envelope.idempotency_key,
                    message_log_id,
                    persisted_envelope.version,
                    persisted_envelope.profile_id,
                    persisted_envelope.session_id,
                    persisted_envelope.ownership_generation,
                    persisted_envelope.admission_fence_id,
                    persisted_envelope.admission_fence_generation,
                    message_fingerprint,
                    payload_json,
                    payload_digest,
                    persisted_envelope.trace_id,
                    persisted_envelope.correlation_id,
                    persisted_envelope.causation_id,
                    occurred_at,
                    available_at,
                    now,
                    now,
                ),
            )
        record.id = message_log_id
        return PersistRoutingJobResult(
            routing_job_id=persisted_envelope.job_id,
            message_log_id=message_log_id,
            inserted=True,
            status=MessageRoutingJobStatus.PENDING,
        )

    def get_job(self, routing_job_id: str) -> PersistedMessageRoutingJob | None:
        """Return one durable routing job by id without claiming it."""

        job_id = _required_identifier(routing_job_id, "routing_job_id")
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM message_routing_jobs WHERE routing_job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return PersistedMessageRoutingJob(
            envelope=_job_envelope_from_row(row),
            message_log_id=int(row["message_log_id"]),
            status=_job_status(str(row["status"])),
            decision_kind=str(row["decision_kind"]),
        )

    def claim_next_job(
        self,
        *,
        worker_id: str,
        expected_fenced_request: FencedMailboxWakeRequest | None = None,
    ) -> ClaimedMessageRoutingJob | None:
        """Claim the oldest available routing job, optionally for one fence.

        ``expected_fenced_request`` is deliberately a complete ownership and
        admission-fence identity.  It lets an explicitly composed target relay
        only its own ingress work without widening that authority to another
        active Actor v2 session.
        """

        worker = _required_identifier(worker_id, "worker_id")
        now = self._clock()
        lease_expires_at = now + self._lease_seconds
        claim_id = uuid.uuid4().hex
        eligibility = _routing_job_work_eligibility("message_routing_jobs")
        scope_clause, scope_parameters = _fenced_request_scope_clause(
            "message_routing_jobs",
            expected_fenced_request,
        )
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"""
                SELECT *
                FROM message_routing_jobs
                WHERE (
                    (status = 'pending' AND available_at <= ?)
                    OR (status = 'processing' AND COALESCE(lease_until, 0) <= ?)
                )
                  {scope_clause}
                  AND {eligibility}
                ORDER BY routing_job_seq ASC
                LIMIT 1
                """,
                (now, now, *scope_parameters, now),
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                f"""
                UPDATE message_routing_jobs
                SET status = 'processing',
                    attempt_count = attempt_count + 1,
                    claim_id = ?,
                    lease_owner = ?,
                    lease_until = ?,
                    updated_at = ?,
                    last_error_code = '',
                    last_error_message = ''
                WHERE routing_job_seq = ?
                  AND (
                    (status = 'pending' AND available_at <= ?)
                    OR (status = 'processing' AND COALESCE(lease_until, 0) <= ?)
                  )
                  {scope_clause}
                  AND {eligibility}
                """,
                (
                    claim_id,
                    worker,
                    lease_expires_at,
                    now,
                    row["routing_job_seq"],
                    now,
                    now,
                    *scope_parameters,
                    now,
                ),
            )
            if updated.rowcount != 1:
                return None
            claimed = conn.execute(
                "SELECT * FROM message_routing_jobs WHERE routing_job_seq = ?",
                (row["routing_job_seq"],),
            ).fetchone()
            assert claimed is not None
            return _claimed_job_from_row(
                claimed,
                claim_id=claim_id,
                worker_id=worker,
                claimed_at=now,
                lease_expires_at=lease_expires_at,
            )

    def claim_job(
        self,
        routing_job_id: str,
        *,
        worker_id: str,
        ignore_available_at: bool = False,
    ) -> ClaimedMessageRoutingJob | None:
        """Claim one specific job for live routing after gates have completed.

        ``ignore_available_at`` is reserved for the live ingress owner. Recovery
        workers always respect the grace/retry deadline through
        :meth:`claim_next_job`.
        """

        job_id = _required_identifier(routing_job_id, "routing_job_id")
        worker = _required_identifier(worker_id, "worker_id")
        now = self._clock()
        lease_expires_at = now + self._lease_seconds
        claim_id = uuid.uuid4().hex
        ignore_deadline = 1 if ignore_available_at else 0
        eligibility = _routing_job_work_eligibility("message_routing_jobs")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"""
                SELECT *
                FROM message_routing_jobs
                WHERE routing_job_id = ?
                  AND (
                    (
                        status = 'pending'
                        AND (? = 1 OR available_at <= ?)
                    )
                    OR (
                        status = 'processing'
                        AND COALESCE(lease_until, 0) <= ?
                    )
                  )
                  AND {eligibility}
                """,
                (job_id, ignore_deadline, now, now, now),
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                f"""
                UPDATE message_routing_jobs
                SET status = 'processing',
                    attempt_count = attempt_count + 1,
                    claim_id = ?,
                    lease_owner = ?,
                    lease_until = ?,
                    updated_at = ?,
                    last_error_code = '',
                    last_error_message = ''
                WHERE routing_job_seq = ?
                  AND (
                    (
                        status = 'pending'
                        AND (? = 1 OR available_at <= ?)
                    )
                    OR (
                        status = 'processing'
                        AND COALESCE(lease_until, 0) <= ?
                    )
                  )
                  AND {eligibility}
                """,
                (
                    claim_id,
                    worker,
                    lease_expires_at,
                    now,
                    row["routing_job_seq"],
                    ignore_deadline,
                    now,
                    now,
                    now,
                ),
            )
            if updated.rowcount != 1:
                return None
            claimed = conn.execute(
                "SELECT * FROM message_routing_jobs WHERE routing_job_seq = ?",
                (row["routing_job_seq"],),
            ).fetchone()
            assert claimed is not None
            return _claimed_job_from_row(
                claimed,
                claim_id=claim_id,
                worker_id=worker,
                claimed_at=now,
                lease_expires_at=lease_expires_at,
            )

    def renew_job_claim(
        self,
        claim: ClaimedMessageRoutingJob,
    ) -> ClaimedMessageRoutingJob:
        """Renew a live routing claim using both worker and unique claim id."""

        now = self._clock()
        lease_expires_at = now + self._lease_seconds
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute(
                """
                UPDATE message_routing_jobs
                SET lease_until = ?, updated_at = ?
                WHERE routing_job_id = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    lease_expires_at,
                    now,
                    claim.routing_job_id,
                    claim.claim_id,
                    claim.worker_id,
                    now,
                ),
            )
            if updated.rowcount != 1:
                raise DurableRoutingLeaseLost("routing job lease is expired or no longer owned")
        return replace(claim, lease_expires_at=lease_expires_at)

    def complete_with_agent_deliveries(
        self,
        claim: ClaimedMessageRoutingJob,
        deliveries: Sequence[AgentRouteDelivery],
        *,
        metadata: Mapping[str, Any] | None = None,
        expected_ownership_generations: Mapping[SessionKey, int] | None = None,
    ) -> RouteDecisionResult:
        """Commit an Agent route decision and all delivery outbox rows atomically."""

        if not deliveries:
            raise ValueError("at least one Agent route delivery is required")
        return self._complete_decision(
            claim,
            deliveries=deliveries,
            decision_kind="agent_deliveries",
            routing_status=MessageRoutingStatus.DISPATCHED,
            routing_skip_reason=None,
            metadata=metadata,
            expected_ownership_generations=expected_ownership_generations,
        )

    def complete_dispatched_without_agent(
        self,
        claim: ClaimedMessageRoutingJob,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> RouteDecisionResult:
        """Commit a normal dispatched decision with no Agent actor delivery."""

        return self._complete_decision(
            claim,
            deliveries=(),
            decision_kind="dispatched_without_agent",
            routing_status=MessageRoutingStatus.DISPATCHED,
            routing_skip_reason=None,
            metadata=metadata,
            expected_ownership_generations=None,
        )

    def complete_without_agent_delivery(
        self,
        claim: ClaimedMessageRoutingJob,
        *,
        skip_reason: MessageRoutingSkipReason | str,
        metadata: Mapping[str, Any] | None = None,
    ) -> RouteDecisionResult:
        """Commit an explicit skipped routing decision without an Agent outbox."""

        reason = _required_identifier(
            routing_skip_reason_value(skip_reason),
            "skip_reason",
        )
        return self._complete_decision(
            claim,
            deliveries=(),
            decision_kind="no_agent_delivery",
            routing_status=MessageRoutingStatus.SKIPPED,
            routing_skip_reason=reason,
            metadata=metadata,
            expected_ownership_generations=None,
        )

    def retry_or_fail_job(
        self,
        claim: ClaimedMessageRoutingJob,
        *,
        error_code: str,
        error_message: str,
        retry_at: float | None = None,
    ) -> None:
        """Release a job for retry or settle it as terminally failed."""

        self._retry_or_fail_claim(
            table="message_routing_jobs",
            id_column="routing_job_id",
            record_id=claim.routing_job_id,
            claim_id=claim.claim_id,
            worker_id=claim.worker_id,
            error_code=error_code,
            error_message=error_message,
            retry_at=retry_at,
        )

    def claim_next_delivery(
        self,
        *,
        worker_id: str,
        expected_fenced_request: FencedMailboxWakeRequest | None = None,
    ) -> ClaimedAgentRouteDelivery | None:
        """Claim the oldest available Agent route delivery for an optional fence."""

        worker = _required_identifier(worker_id, "worker_id")
        now = self._clock()
        lease_expires_at = now + self._lease_seconds
        claim_id = uuid.uuid4().hex
        eligibility = _route_outbox_work_eligibility("agent_route_outbox")
        scope_clause, scope_parameters = _fenced_request_scope_clause(
            "agent_route_outbox",
            expected_fenced_request,
        )
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"""
                SELECT *
                FROM agent_route_outbox
                WHERE (
                    (status = 'pending' AND available_at <= ?)
                    OR (status = 'processing' AND COALESCE(lease_until, 0) <= ?)
                )
                  {scope_clause}
                  AND {eligibility}
                ORDER BY outbox_seq ASC
                LIMIT 1
                """,
                (now, now, *scope_parameters, now),
            ).fetchone()
            if row is None:
                return None
            delivery = self._delivery_from_outbox_row(row)
            updated = conn.execute(
                f"""
                UPDATE agent_route_outbox
                SET status = 'processing',
                    attempt_count = attempt_count + 1,
                    claim_id = ?,
                    lease_owner = ?,
                    lease_until = ?,
                    updated_at = ?,
                    last_error_code = '',
                    last_error_message = ''
                WHERE outbox_seq = ?
                  AND (
                    (status = 'pending' AND available_at <= ?)
                    OR (status = 'processing' AND COALESCE(lease_until, 0) <= ?)
                  )
                  {scope_clause}
                  AND {eligibility}
                """,
                (
                    claim_id,
                    worker,
                    lease_expires_at,
                    now,
                    row["outbox_seq"],
                    now,
                    now,
                    *scope_parameters,
                    now,
                ),
            )
            if updated.rowcount != 1:
                return None
            return ClaimedAgentRouteDelivery(
                delivery=delivery,
                routing_job_id=str(row["routing_job_id"]),
                correlation_id=str(row["correlation_id"]),
                causation_id=str(row["causation_id"]),
                ownership_generation=int(row["ownership_generation"]),
                admission_fence_id=str(row["admission_fence_id"]),
                admission_fence_generation=int(row["admission_fence_generation"]),
                claim_id=claim_id,
                worker_id=worker,
                attempt_count=int(row["attempt_count"]) + 1,
                claimed_at=now,
                lease_expires_at=lease_expires_at,
            )

    def renew_delivery_claim(
        self,
        claim: ClaimedAgentRouteDelivery,
    ) -> ClaimedAgentRouteDelivery:
        """Renew a live outbox claim using both worker and unique claim id."""

        now = self._clock()
        lease_expires_at = now + self._lease_seconds
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute(
                """
                UPDATE agent_route_outbox
                SET lease_until = ?, updated_at = ?
                WHERE delivery_id = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    lease_expires_at,
                    now,
                    claim.delivery_id,
                    claim.claim_id,
                    claim.worker_id,
                    now,
                ),
            )
            if updated.rowcount != 1:
                raise DurableRoutingLeaseLost(
                    "Agent route delivery lease is expired or no longer owned"
                )
        return replace(claim, lease_expires_at=lease_expires_at)

    def relay_delivery(self, claim: ClaimedAgentRouteDelivery) -> RouteRelayResult:
        """Atomically enqueue ``MessageReceived`` and complete the outbox row."""

        now = self._clock()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM agent_route_outbox WHERE delivery_id = ?",
                (claim.delivery_id,),
            ).fetchone()
            if row is None:
                raise DurableRoutingRecordNotFound(
                    f"Agent route delivery does not exist: {claim.delivery_id}"
                )
            delivery = self._delivery_from_outbox_row(row)
            self._validate_claimed_delivery(row, claim, delivery)
            wake_request = FencedMailboxWakeRequest(
                key=delivery.session_key,
                ownership_generation=int(row["ownership_generation"]),
                admission_fence_id=str(row["admission_fence_id"]),
                admission_fence_generation=int(row["admission_fence_generation"]),
            )
            if str(row["status"]) == AgentRouteOutboxStatus.COMPLETED.value:
                mailbox_id = self._validate_mailbox_event(conn, row, delivery)
                return self._relay_result(
                    delivery,
                    ownership_generation=wake_request.ownership_generation,
                    admission_fence_id=wake_request.admission_fence_id,
                    admission_fence_generation=wake_request.admission_fence_generation,
                    mailbox_id=mailbox_id,
                    mailbox_inserted=False,
                )
            self._validate_live_claim(
                row,
                claim_id=claim.claim_id,
                worker_id=claim.worker_id,
                now=now,
                subject="Agent route delivery",
            )
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                delivery.session_key,
                expected_generation=wake_request.ownership_generation,
                expected_admission_fence_id=wake_request.admission_fence_id,
                expected_admission_fence_generation=wake_request.admission_fence_generation,
            )
            conn.execute("SAVEPOINT route_relay_candidate")
            try:
                ownership_generation = wake_request.ownership_generation
                self._ensure_actor_aggregate(
                    conn,
                    delivery,
                    ownership_generation=ownership_generation,
                    now=now,
                )
                payload_json = _canonical_json_object(delivery.to_mailbox_payload())
                inserted = conn.execute(
                    """
                    INSERT OR IGNORE INTO agent_session_mailbox (
                        event_id, profile_id, session_id, ownership_generation,
                        kind, source, occurred_at, payload_json, causation_id,
                        correlation_id, trace_id,
                        status, attempt_count, available_at, claim_id, lease_owner,
                        lease_until, created_at, handled_at, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, NULL, '')
                    """,
                    (
                        delivery.event_id,
                        delivery.session_key.profile_id,
                        delivery.session_key.session_id,
                        ownership_generation,
                        AGENT_ROUTE_MAILBOX_KIND,
                        AGENT_ROUTE_MAILBOX_SOURCE,
                        delivery.observed_at,
                        payload_json,
                        str(row["routing_job_id"]),
                        str(row["routing_job_id"]),
                        delivery.trace_id,
                        now,
                        now,
                    ),
                )
                if inserted.rowcount == 1:
                    mailbox_id = int(inserted.lastrowid)
                    if mailbox_id < 1:
                        raise DurableRoutingConflict(
                            "new route mailbox does not have a durable primary key"
                        )
                    if wake_request.has_admission_fence:
                        self._database.actor_v2_mailbox_handoffs.record_fenced_handoff_in_transaction(
                            conn,
                            mailbox_id,
                            wake_request,
                        )
                    else:
                        self._database.actor_v2_mailbox_handoffs.record_unfenced_legacy_handoff_in_transaction(
                            conn,
                            mailbox_id,
                        )
                else:
                    mailbox_id = self._validate_mailbox_event(conn, row, delivery)
                updated = conn.execute(
                    """
                    UPDATE agent_route_outbox
                    SET status = 'completed',
                        claim_id = '',
                        lease_owner = '',
                        lease_until = NULL,
                        updated_at = ?,
                        completed_at = ?,
                        failed_at = NULL,
                        last_error_code = '',
                        last_error_message = ''
                    WHERE delivery_id = ?
                      AND status = 'processing'
                      AND claim_id = ?
                      AND lease_owner = ?
                      AND COALESCE(lease_until, 0) > ?
                    """,
                    (
                        now,
                        now,
                        delivery.delivery_id,
                        claim.claim_id,
                        claim.worker_id,
                        now,
                    ),
                )
                if updated.rowcount != 1:
                    raise DurableRoutingLeaseLost(
                        "Agent route delivery lease is expired or no longer owned"
                    )
                self._require_outbox_delivery_admission(
                    conn,
                    delivery_id=delivery.delivery_id,
                    routing_job_id=str(row["routing_job_id"]),
                    request=wake_request,
                )
                result = self._relay_result(
                    delivery,
                    ownership_generation=ownership_generation,
                    admission_fence_id=wake_request.admission_fence_id,
                    admission_fence_generation=wake_request.admission_fence_generation,
                    mailbox_id=mailbox_id,
                    mailbox_inserted=inserted.rowcount == 1,
                )
            except BaseException:
                conn.execute("ROLLBACK TO SAVEPOINT route_relay_candidate")
                conn.execute("RELEASE SAVEPOINT route_relay_candidate")
                raise
            conn.execute("RELEASE SAVEPOINT route_relay_candidate")
            return result

    def retry_or_fail_delivery(
        self,
        claim: ClaimedAgentRouteDelivery,
        *,
        error_code: str,
        error_message: str,
        retry_at: float | None = None,
    ) -> None:
        """Release an outbox delivery for retry or settle it as failed."""

        self._retry_or_fail_claim(
            table="agent_route_outbox",
            id_column="delivery_id",
            record_id=claim.delivery_id,
            claim_id=claim.claim_id,
            worker_id=claim.worker_id,
            error_code=error_code,
            error_message=error_message,
            retry_at=retry_at,
        )

    def next_job_available_at(
        self,
        *,
        expected_fenced_request: FencedMailboxWakeRequest | None = None,
    ) -> float | None:
        """Return the next routing deadline for an optional exact fence scope."""

        now = self._clock()
        eligibility = _routing_job_work_eligibility("message_routing_jobs")
        scope_clause, scope_parameters = _fenced_request_scope_clause(
            "message_routing_jobs",
            expected_fenced_request,
        )
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT MIN(
                    CASE
                        WHEN status = 'pending' THEN available_at
                        ELSE COALESCE(lease_until, available_at)
                    END
                ) AS next_at
                FROM message_routing_jobs
                WHERE status IN ('pending', 'processing')
                  {scope_clause}
                  AND {eligibility}
                """,
                (*scope_parameters, now),
            ).fetchone()
        if row is None or row["next_at"] is None:
            return None
        return float(row["next_at"])

    def next_delivery_available_at(
        self,
        *,
        expected_fenced_request: FencedMailboxWakeRequest | None = None,
    ) -> float | None:
        """Return the next Agent delivery deadline for an optional exact fence."""

        now = self._clock()
        eligibility = _route_outbox_work_eligibility("agent_route_outbox")
        scope_clause, scope_parameters = _fenced_request_scope_clause(
            "agent_route_outbox",
            expected_fenced_request,
        )
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT MIN(
                    CASE
                        WHEN status = 'pending' THEN available_at
                        ELSE COALESCE(lease_until, available_at)
                    END
                ) AS next_at
                FROM agent_route_outbox
                WHERE status IN ('pending', 'processing')
                  {scope_clause}
                  AND {eligibility}
                """,
                (*scope_parameters, now),
            ).fetchone()
        if row is None or row["next_at"] is None:
            return None
        return float(row["next_at"])

    def is_live_fenced_request(self, request: FencedMailboxWakeRequest) -> bool:
        """Return whether one complete fenced Actor ownership scope is current.

        This is an observation only.  It does not claim work, acquire a target
        lease, or prove that a local target is running; callers still need their
        own target lifecycle proof before treating the scope as consumable.
        """

        _fenced_request_scope_clause("message_routing_jobs", request)
        now = self._clock()
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM agent_session_runtime_ownership AS ownership
                JOIN agent_session_actor_v2_admission_fences AS fence
                  ON fence.profile_id = ownership.profile_id
                 AND fence.session_id = ownership.session_id
                 AND fence.fence_id = ownership.admission_fence_id
                 AND fence.generation = ownership.admission_fence_generation
                WHERE ownership.profile_id = ?
                  AND ownership.session_id = ?
                  AND ownership.mode = 'actor_v2'
                  AND ownership.status = 'active'
                  AND ownership.generation = ?
                  AND ownership.admission_fence_id = ?
                  AND ownership.admission_fence_generation = ?
                  AND fence.status = 'committed'
                  AND fence.expires_at > ?
                LIMIT 1
                """,
                (
                    request.key.profile_id,
                    request.key.session_id,
                    request.ownership_generation,
                    request.admission_fence_id,
                    request.admission_fence_generation,
                    now,
                ),
            ).fetchone()
        return row is not None

    def pending_counts(self) -> tuple[int, int]:
        """Return recoverable routing-job and Agent-delivery counts."""

        with self.connect() as conn:
            jobs = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM message_routing_jobs
                WHERE status IN ('pending', 'processing')
                """
            ).fetchone()
            deliveries = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM agent_route_outbox
                WHERE status IN ('pending', 'processing')
                """
            ).fetchone()
        return int(jobs["count"]), int(deliveries["count"])

    def pending_route_wake_debts(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        after: RouteWakeCursor | None = None,
        fenced_only: bool = False,
    ) -> tuple[PendingRouteWakeDebt, ...]:
        """Return the newest pending route event for each exact Actor identity.

        A post-commit wake reaches an Actor, rather than claiming a mailbox
        row. One current event is therefore sufficient to make durable debt
        discoverable, while its ``event_id`` keeps an accepted wake from
        masking a newer committed route event for the same Actor incarnation.
        ``after`` advances a stable event-versioned mailbox keyset; ``offset``
        remains available only for callers that cannot retain a cursor.
        """

        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative integer")
        if after is not None and not isinstance(after, RouteWakeCursor):
            raise TypeError("after must be a RouteWakeCursor")
        if after is not None and offset:
            raise ValueError("offset cannot be combined with a keyset cursor")
        if not isinstance(fenced_only, bool):
            raise TypeError("fenced_only must be a bool")
        now = self._clock()
        fenced_clause = " AND ownership.admission_fence_id != ''" if fenced_only else ""
        after_clause = ""
        after_params: tuple[object, ...] = ()
        if after is not None:
            after_clause = """
              AND (
                    debt.mailbox_id > ?
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id > ?
                 )
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id = ?
                    AND debt.session_id > ?
                 )
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id = ?
                    AND debt.session_id = ?
                    AND debt.ownership_generation > ?
                 )
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id = ?
                    AND debt.session_id = ?
                    AND debt.ownership_generation = ?
                    AND debt.admission_fence_id > ?
                 )
                 OR (
                        debt.mailbox_id = ?
                    AND debt.profile_id = ?
                    AND debt.session_id = ?
                    AND debt.ownership_generation = ?
                    AND debt.admission_fence_id = ?
                    AND debt.admission_fence_generation > ?
                 )
              )
            """
            after_params = _route_wake_cursor_parameters(after)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                WITH ranked_route_mailbox AS (
                    SELECT mailbox.profile_id,
                           mailbox.session_id,
                           mailbox.event_id,
                           mailbox.mailbox_id,
                           ownership.generation AS ownership_generation,
                           ownership.admission_fence_id AS admission_fence_id,
                           ownership.admission_fence_generation
                               AS admission_fence_generation,
                           ROW_NUMBER() OVER (
                               PARTITION BY mailbox.profile_id,
                                            mailbox.session_id,
                                            ownership.generation,
                                            ownership.admission_fence_id,
                                            ownership.admission_fence_generation
                               ORDER BY mailbox.mailbox_id DESC
                           ) AS mailbox_rank
                    FROM agent_session_mailbox AS mailbox
                    JOIN agent_session_runtime_ownership AS ownership
                      ON ownership.profile_id = mailbox.profile_id
                     AND ownership.session_id = mailbox.session_id
                     AND ownership.mode = 'actor_v2'
                     AND ownership.status = 'active'
                     AND ownership.generation = mailbox.ownership_generation
                    LEFT JOIN agent_session_actor_v2_admission_fences AS admission
                      ON admission.profile_id = ownership.profile_id
                     AND admission.session_id = ownership.session_id
                     AND admission.fence_id = ownership.admission_fence_id
                     AND admission.generation = ownership.admission_fence_generation
                    WHERE mailbox.kind = ?
                      AND mailbox.source = ?
                      AND mailbox.status IN ('pending', 'processing')
                      AND (
                            ownership.admission_fence_id = ''
                         OR (
                                admission.status = 'committed'
                            AND admission.expires_at > ?
                              )
                      )
                      {fenced_clause}
                )
                SELECT profile_id, session_id, event_id, mailbox_id,
                       ownership_generation, admission_fence_id,
                       admission_fence_generation
                FROM ranked_route_mailbox AS debt
                WHERE debt.mailbox_rank = 1
                {after_clause}
                ORDER BY mailbox_id ASC,
                         profile_id ASC,
                         session_id ASC,
                         ownership_generation ASC,
                         admission_fence_id ASC,
                         admission_fence_generation ASC
                LIMIT ? OFFSET ?
                """,
                (
                    AGENT_ROUTE_MAILBOX_KIND,
                    AGENT_ROUTE_MAILBOX_SOURCE,
                    now,
                    *after_params,
                    limit,
                    offset,
                ),
            ).fetchall()
        return tuple(
            PendingRouteWakeDebt(
                request=FencedMailboxWakeRequest(
                    key=SessionKey(
                        str(row["profile_id"]),
                        str(row["session_id"]),
                    ),
                    ownership_generation=int(row["ownership_generation"]),
                    admission_fence_id=str(row["admission_fence_id"]),
                    admission_fence_generation=int(
                        row["admission_fence_generation"]
                    ),
                ),
                event_id=str(row["event_id"]),
                cursor=RouteWakeCursor(
                    mailbox_id=int(row["mailbox_id"]),
                    profile_id=str(row["profile_id"]),
                    session_id=str(row["session_id"]),
                    ownership_generation=int(row["ownership_generation"]),
                    admission_fence_id=str(row["admission_fence_id"]),
                    admission_fence_generation=int(
                        row["admission_fence_generation"]
                    ),
                ),
            )
            for row in rows
        )

    def pending_route_wake_requests(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        after: RouteWakeCursor | None = None,
        fenced_only: bool = False,
    ) -> tuple[FencedMailboxWakeRequest, ...]:
        """Project route wake debt to exact Actor ownership identities.

        This compatibility projection intentionally omits the mailbox event
        id. Callers which cache accepted fenced wakes must use
        :meth:`pending_route_wake_debts` instead.
        """

        requests: list[FencedMailboxWakeRequest] = []
        seen: set[FencedMailboxWakeRequest] = set()
        for debt in self.pending_route_wake_debts(
            limit=limit,
            offset=offset,
            after=after,
            fenced_only=fenced_only,
        ):
            if debt.request in seen:
                continue
            seen.add(debt.request)
            requests.append(debt.request)
        return tuple(requests)

    def has_pending_fenced_route_wake_requests(self) -> bool:
        """Return whether a legacy broad wake would cross a live fence boundary."""

        return bool(self.pending_route_wake_requests(limit=1, fenced_only=True))

    def has_retained_fenced_mailbox_debt(self) -> bool:
        """Return whether broad legacy recovery could wake a fenced mailbox.

        ``AgentSessionActorRegistry.recover()`` scans every pending mailbox
        kind, not only route relay events. A currently active Actor v2 owner
        with a matching fenced generation therefore blocks that broad API even
        if the retained fence is revoked or expired. Once ownership changes,
        the registry's own generation join excludes the historical mailbox.
        """

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                WHERE mailbox.status IN ('pending', 'processing')
                  AND ownership.admission_fence_id != ''
                LIMIT 1
                """
            ).fetchone()
        return row is not None

    def has_retained_fenced_route_mailbox_debt(self) -> bool:
        """Return whether any pending route mailbox retains a fence identity.

        A legacy registry's ``recover()`` operates on mailbox keys alone and
        cannot distinguish a revoked or expired Actor incarnation.  This query
        therefore intentionally includes historical fenced deliveries instead
        of only currently live admission fences.
        """

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM agent_session_mailbox AS mailbox
                JOIN agent_route_outbox AS outbox
                  ON outbox.profile_id = mailbox.profile_id
                 AND outbox.session_id = mailbox.session_id
                 AND outbox.ownership_generation = mailbox.ownership_generation
                 AND outbox.event_id = mailbox.event_id
                WHERE mailbox.kind = ?
                  AND mailbox.source = ?
                  AND mailbox.status IN ('pending', 'processing')
                  AND outbox.admission_fence_id != ''
                LIMIT 1
                """,
                (AGENT_ROUTE_MAILBOX_KIND, AGENT_ROUTE_MAILBOX_SOURCE),
            ).fetchone()
        return row is not None

    def is_pending_route_wake_request(
        self,
        request: FencedMailboxWakeRequest,
    ) -> bool:
        """Return whether one exact wake identity still has live mailbox debt."""

        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        now = self._clock()
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                LEFT JOIN agent_session_actor_v2_admission_fences AS admission
                  ON admission.profile_id = ownership.profile_id
                 AND admission.session_id = ownership.session_id
                 AND admission.fence_id = ownership.admission_fence_id
                 AND admission.generation = ownership.admission_fence_generation
                WHERE mailbox.profile_id = ?
                  AND mailbox.session_id = ?
                  AND mailbox.ownership_generation = ?
                  AND ownership.admission_fence_id = ?
                  AND ownership.admission_fence_generation = ?
                  AND mailbox.kind = ?
                  AND mailbox.source = ?
                  AND mailbox.status IN ('pending', 'processing')
                  AND (
                        ownership.admission_fence_id = ''
                     OR (
                            admission.status = 'committed'
                        AND admission.expires_at > ?
                          )
                  )
                LIMIT 1
                """,
                (
                    request.key.profile_id,
                    request.key.session_id,
                    request.ownership_generation,
                    request.admission_fence_id,
                    request.admission_fence_generation,
                    AGENT_ROUTE_MAILBOX_KIND,
                    AGENT_ROUTE_MAILBOX_SOURCE,
                    now,
                ),
            ).fetchone()
        return row is not None

    def is_pending_route_wake_debt(self, debt: PendingRouteWakeDebt) -> bool:
        """Return whether one exact route mailbox event remains live.

        This is intentionally stricter than
        :meth:`is_pending_route_wake_request`: retry and accepted-wake caches
        must be invalidated for the event that they actually observed, while a
        newer event for the same Actor incarnation remains independently
        discoverable.
        """

        if not isinstance(debt, PendingRouteWakeDebt):
            raise TypeError("debt must be a PendingRouteWakeDebt")
        request = debt.request
        cursor_clause = ""
        cursor_params: tuple[object, ...] = ()
        if debt.cursor is not None:
            cursor_clause = " AND mailbox.mailbox_id = ?"
            cursor_params = (debt.cursor.mailbox_id,)
        now = self._clock()
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                LEFT JOIN agent_session_actor_v2_admission_fences AS admission
                  ON admission.profile_id = ownership.profile_id
                 AND admission.session_id = ownership.session_id
                 AND admission.fence_id = ownership.admission_fence_id
                 AND admission.generation = ownership.admission_fence_generation
                WHERE mailbox.profile_id = ?
                  AND mailbox.session_id = ?
                  AND mailbox.ownership_generation = ?
                  AND mailbox.event_id = ?
                  {cursor_clause}
                  AND ownership.admission_fence_id = ?
                  AND ownership.admission_fence_generation = ?
                  AND mailbox.kind = ?
                  AND mailbox.source = ?
                  AND mailbox.status IN ('pending', 'processing')
                  AND (
                        ownership.admission_fence_id = ''
                     OR (
                            admission.status = 'committed'
                        AND admission.expires_at > ?
                          )
                  )
                LIMIT 1
                """,
                (
                    request.key.profile_id,
                    request.key.session_id,
                    request.ownership_generation,
                    debt.event_id,
                    *cursor_params,
                    request.admission_fence_id,
                    request.admission_fence_generation,
                    AGENT_ROUTE_MAILBOX_KIND,
                    AGENT_ROUTE_MAILBOX_SOURCE,
                    now,
                ),
            ).fetchone()
        return row is not None

    def active_actor_ownership_count(self) -> int:
        """Return active actor-v2 owners used by supervisor readiness checks."""

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM agent_session_runtime_ownership
                WHERE mode = 'actor_v2' AND status = 'active'
                """
            ).fetchone()
        return int(row["count"])

    def _complete_decision(
        self,
        claim: ClaimedMessageRoutingJob,
        *,
        deliveries: Sequence[AgentRouteDelivery],
        decision_kind: str,
        routing_status: MessageRoutingStatus,
        routing_skip_reason: str | None,
        metadata: Mapping[str, Any] | None,
        expected_ownership_generations: Mapping[SessionKey, int] | None,
    ) -> RouteDecisionResult:
        now = self._clock()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM message_routing_jobs WHERE routing_job_id = ?",
                (claim.routing_job_id,),
            ).fetchone()
            if row is None:
                raise DurableRoutingRecordNotFound(
                    f"message routing job does not exist: {claim.routing_job_id}"
                )
            self._validate_claimed_job(row, claim)
            job_envelope = self._validate_routing_job_admission(conn, row)
            message_row = conn.execute(
                "SELECT session_id FROM message_logs WHERE id = ?",
                (claim.message_log_id,),
            ).fetchone()
            if message_row is None:
                raise DurableRoutingRecordNotFound(
                    f"message log does not exist: {claim.message_log_id}"
                )
            normalized = self._normalize_deliveries(
                claim,
                deliveries,
                message_session_id=str(message_row["session_id"]),
            )
            metadata_json = _canonical_json_object(metadata or {})
            decision_payload_json = _canonical_json_object(
                {
                    "version": _ROUTE_DECISION_VERSION,
                    "kind": decision_kind,
                    "delivery_ids": [delivery.delivery_id for delivery in normalized],
                    "routing_status": routing_status.value,
                    "routing_skip_reason": routing_skip_reason,
                    "metadata": json.loads(metadata_json),
                }
            )
            decision_payload_digest = _digest(decision_payload_json)
            decision_id = f"message-route-decision:v1:{decision_payload_digest}"
            if str(row["status"]) == MessageRoutingJobStatus.COMPLETED.value:
                self._validate_completed_decision(
                    conn,
                    row,
                    decision_kind=decision_kind,
                    decision_id=decision_id,
                    decision_payload_json=decision_payload_json,
                    decision_payload_digest=decision_payload_digest,
                    deliveries=normalized,
                )
                return RouteDecisionResult(
                    routing_job_id=claim.routing_job_id,
                    decision_id=decision_id,
                    delivery_ids=tuple(item.delivery_id for item in normalized),
                    inserted_delivery_count=0,
                )
            self._validate_live_claim(
                row,
                claim_id=claim.claim_id,
                worker_id=claim.worker_id,
                now=now,
                subject="message routing job",
            )
            conn.execute("SAVEPOINT route_decision_candidate")
            try:
                inserted_delivery_count = 0
                captured_deliveries: list[_CapturedOutboxAdmission] = []
                for delivery in normalized:
                    if (
                        expected_ownership_generations is not None
                        and delivery.session_key not in expected_ownership_generations
                    ):
                        raise DurableRoutingConflict(
                            "routing decision omitted an expected ownership generation"
                        )
                    captured_delivery = self._insert_outbox_delivery(
                        conn,
                        row,
                        delivery,
                        now=now,
                        expected_ownership_generation=(
                            expected_ownership_generations.get(delivery.session_key)
                            if expected_ownership_generations is not None
                            else None
                        ),
                    )
                    inserted_delivery_count += int(captured_delivery.inserted)
                    captured_deliveries.append(captured_delivery)
                conn.execute(
                    """
                    UPDATE message_logs
                    SET routing_status = ?, routed_at = ?, routing_skip_reason = ?
                    WHERE id = ?
                    """,
                    (
                        routing_status_value(routing_status),
                        now * 1000,
                        routing_skip_reason,
                        claim.message_log_id,
                    ),
                )
                updated = conn.execute(
                    """
                    UPDATE message_routing_jobs
                    SET status = 'completed',
                        claim_id = '',
                        lease_owner = '',
                        lease_until = NULL,
                        decision_version = ?,
                        decision_kind = ?,
                        decision_id = ?,
                        decision_payload_json = ?,
                        decision_payload_digest = ?,
                        updated_at = ?,
                        completed_at = ?,
                        failed_at = NULL,
                        last_error_code = '',
                        last_error_message = ''
                    WHERE routing_job_id = ?
                      AND status = 'processing'
                      AND claim_id = ?
                      AND lease_owner = ?
                      AND COALESCE(lease_until, 0) > ?
                    """,
                    (
                        _ROUTE_DECISION_VERSION,
                        decision_kind,
                        decision_id,
                        decision_payload_json,
                        decision_payload_digest,
                        now,
                        now,
                        claim.routing_job_id,
                        claim.claim_id,
                        claim.worker_id,
                        now,
                    ),
                )
                if updated.rowcount != 1:
                    raise DurableRoutingLeaseLost(
                        "message routing job lease is expired or no longer owned"
                    )
                self._require_persisted_routing_job_admission(conn, job_envelope)
                for captured_delivery in captured_deliveries:
                    self._require_outbox_delivery_admission(
                        conn,
                        delivery_id=captured_delivery.delivery_id,
                        routing_job_id=claim.routing_job_id,
                        request=captured_delivery.request,
                    )
                result = RouteDecisionResult(
                    routing_job_id=claim.routing_job_id,
                    decision_id=decision_id,
                    delivery_ids=tuple(item.delivery_id for item in normalized),
                    inserted_delivery_count=inserted_delivery_count,
                )
            except BaseException:
                conn.execute("ROLLBACK TO SAVEPOINT route_decision_candidate")
                conn.execute("RELEASE SAVEPOINT route_decision_candidate")
                raise
            conn.execute("RELEASE SAVEPOINT route_decision_candidate")
            return result

    def _normalize_deliveries(
        self,
        claim: ClaimedMessageRoutingJob,
        deliveries: Sequence[AgentRouteDelivery],
        *,
        message_session_id: str,
    ) -> tuple[AgentRouteDelivery, ...]:
        normalized: list[AgentRouteDelivery] = []
        seen_delivery_ids: set[str] = set()
        seen_keys: set[tuple[str, str, int, str]] = set()
        for candidate in deliveries:
            delivery = AgentRouteDelivery.from_payload(candidate.to_payload())
            if delivery.version != AGENT_ROUTE_OUTBOX_VERSION:
                raise DurableRoutingConflict(
                    f"unsupported Agent route outbox version: {delivery.version}"
                )
            if delivery.message_log_id != claim.message_log_id:
                raise DurableRoutingConflict(
                    "Agent delivery message_log_id does not match its routing job"
                )
            if delivery.base_session_id != message_session_id:
                raise DurableRoutingConflict(
                    "Agent delivery base_session_id does not match its message log"
                )
            if delivery.trace_id != claim.envelope.trace_id:
                raise DurableRoutingConflict("Agent delivery trace_id does not match its routing job")
            if delivery.delivery_id in seen_delivery_ids or delivery.delivery_key in seen_keys:
                raise DurableRoutingConflict("routing decision contains a duplicate Agent delivery")
            seen_delivery_ids.add(delivery.delivery_id)
            seen_keys.add(delivery.delivery_key)
            normalized.append(delivery)
        return tuple(sorted(normalized, key=lambda item: item.delivery_id))

    def _validate_routing_job_admission(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> MessageRoutingJobEnvelope:
        """Require a committed matching actor owner for a fenced routing job."""

        envelope = _job_envelope_from_row(row)
        self._require_routing_job_admission(conn, envelope)
        return envelope

    def _require_routing_job_admission(
        self,
        conn: sqlite3.Connection,
        envelope: MessageRoutingJobEnvelope,
    ) -> None:
        """Require the exact ownership evidence captured by one routing job."""

        if not envelope.has_admission_fence:
            return
        if envelope.ownership_generation < 1:
            raise DurableRoutingConflict(
                "reserved admission routing work cannot be decided before ownership commits"
            )
        self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            SessionKey(envelope.profile_id, envelope.session_id),
            expected_generation=envelope.ownership_generation,
            expected_admission_fence_id=envelope.admission_fence_id,
            expected_admission_fence_generation=envelope.admission_fence_generation,
        )

    def _require_persisted_routing_job_admission(
        self,
        conn: sqlite3.Connection,
        envelope: MessageRoutingJobEnvelope,
    ) -> None:
        """Verify a candidate did not rewrite the job identity before final gating."""

        row = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation,
                   admission_fence_id, admission_fence_generation
            FROM message_routing_jobs
            WHERE routing_job_id = ?
            """,
            (envelope.job_id,),
        ).fetchone()
        if row is None:
            raise DurableRoutingConflict(
                "routing job disappeared before final admission gate"
            )
        persisted = (
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["ownership_generation"]),
            str(row["admission_fence_id"]),
            int(row["admission_fence_generation"]),
        )
        captured = (
            envelope.profile_id,
            envelope.session_id,
            envelope.ownership_generation,
            envelope.admission_fence_id,
            envelope.admission_fence_generation,
        )
        if persisted != captured:
            raise DurableRoutingConflict(
                "routing job admission identity changed before final admission gate"
            )
        self._require_routing_job_admission(conn, envelope)

    def _require_outbox_delivery_admission(
        self,
        conn: sqlite3.Connection,
        *,
        delivery_id: str,
        routing_job_id: str,
        request: FencedMailboxWakeRequest,
    ) -> None:
        """Require one candidate delivery's original ownership identity."""

        row = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation,
                   admission_fence_id, admission_fence_generation
            FROM agent_route_outbox
            WHERE delivery_id = ? AND routing_job_id = ?
            """,
            (delivery_id, routing_job_id),
        ).fetchone()
        if row is None:
            raise DurableRoutingConflict(
                "route delivery disappeared before final admission gate"
            )
        persisted = (
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["ownership_generation"]),
            str(row["admission_fence_id"]),
            int(row["admission_fence_generation"]),
        )
        captured = (
            request.key.profile_id,
            request.key.session_id,
            request.ownership_generation,
            request.admission_fence_id,
            request.admission_fence_generation,
        )
        if persisted != captured:
            raise DurableRoutingConflict(
                "route delivery identity changed before final admission gate"
            )
        self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            request.key,
            expected_generation=request.ownership_generation,
            expected_admission_fence_id=request.admission_fence_id,
            expected_admission_fence_generation=request.admission_fence_generation,
        )

    def _insert_outbox_delivery(
        self,
        conn: sqlite3.Connection,
        job_row: sqlite3.Row,
        delivery: AgentRouteDelivery,
        *,
        now: float,
        expected_ownership_generation: int | None,
    ) -> _CapturedOutboxAdmission:
        payload_json = _canonical_json_object(delivery.to_payload())
        payload_digest = _digest(payload_json)
        job_envelope = self._validate_routing_job_admission(conn, job_row)
        if job_envelope.has_admission_fence and delivery.session_key != SessionKey(
            job_envelope.profile_id,
            job_envelope.session_id,
        ):
            raise DurableRoutingConflict(
                "fenced routing job cannot create an Agent delivery for another session"
            )
        ownership = self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            delivery.session_key,
            expected_generation=expected_ownership_generation,
            expected_admission_fence_id=job_envelope.admission_fence_id,
            expected_admission_fence_generation=job_envelope.admission_fence_generation,
        )
        request = FencedMailboxWakeRequest(
            key=delivery.session_key,
            ownership_generation=ownership.generation,
            admission_fence_id=job_envelope.admission_fence_id,
            admission_fence_generation=job_envelope.admission_fence_generation,
        )
        inserted = conn.execute(
            """
            INSERT OR IGNORE INTO agent_route_outbox (
                delivery_id, idempotency_key, routing_job_id, profile_id,
                session_id, message_log_id, route_rule_id, version,
                ownership_generation, admission_fence_id,
                admission_fence_generation, event_id, payload_json, payload_digest,
                trace_id, correlation_id,
                causation_id, status, attempt_count, available_at, claim_id,
                lease_owner, lease_until, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, ?)
            """,
            (
                delivery.delivery_id,
                delivery.idempotency_key,
                str(job_row["routing_job_id"]),
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
                delivery.message_log_id,
                delivery.route_rule_id,
                delivery.version,
                ownership.generation,
                job_envelope.admission_fence_id,
                job_envelope.admission_fence_generation,
                delivery.event_id,
                payload_json,
                payload_digest,
                delivery.trace_id,
                str(job_row["correlation_id"]),
                str(job_row["routing_job_id"]),
                now,
                now,
                now,
            ),
        )
        candidates = conn.execute(
            """
            SELECT *
            FROM agent_route_outbox
            WHERE delivery_id = ?
               OR idempotency_key = ?
               OR (
                    profile_id = ? AND session_id = ?
                    AND message_log_id = ? AND route_rule_id = ?
               )
            """,
            (
                delivery.delivery_id,
                delivery.idempotency_key,
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
                delivery.message_log_id,
                delivery.route_rule_id,
            ),
        ).fetchall()
        if len(candidates) != 1:
            raise DurableRoutingConflict(
                f"Agent delivery identity collides with {len(candidates)} durable rows"
            )
        self._validate_outbox_identity(
            candidates[0],
            delivery=delivery,
            routing_job_id=str(job_row["routing_job_id"]),
            correlation_id=str(job_row["correlation_id"]),
            ownership_generation=request.ownership_generation,
            admission_fence_id=request.admission_fence_id,
            admission_fence_generation=request.admission_fence_generation,
            payload_json=payload_json,
            payload_digest=payload_digest,
        )
        return _CapturedOutboxAdmission(
            delivery_id=delivery.delivery_id,
            request=request,
            inserted=inserted.rowcount == 1,
        )

    def _delivery_from_outbox_row(self, row: sqlite3.Row) -> AgentRouteDelivery:
        payload_json = str(row["payload_json"])
        if _digest(payload_json) != str(row["payload_digest"]):
            raise DurableRoutingConflict("Agent route outbox payload digest is invalid")
        payload = _json_object(payload_json)
        delivery = AgentRouteDelivery.from_payload(payload)
        canonical_payload_json = _canonical_json_object(delivery.to_payload())
        if payload_json != canonical_payload_json:
            raise DurableRoutingConflict(
                "Agent route outbox payload is not the canonical delivery contract"
            )
        self._validate_outbox_identity(
            row,
            delivery=delivery,
            routing_job_id=str(row["routing_job_id"]),
            correlation_id=str(row["correlation_id"]),
            ownership_generation=int(row["ownership_generation"]),
            admission_fence_id=str(row["admission_fence_id"]),
            admission_fence_generation=int(row["admission_fence_generation"]),
            payload_json=canonical_payload_json,
            payload_digest=_digest(canonical_payload_json),
        )
        return delivery

    @staticmethod
    def _validate_claimed_job(
        row: sqlite3.Row,
        claim: ClaimedMessageRoutingJob,
    ) -> None:
        payload_json = _canonical_json_object(claim.envelope.payload)
        persisted = (
            str(row["routing_job_id"]),
            str(row["idempotency_key"]),
            int(row["message_log_id"]),
            int(row["version"]),
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["ownership_generation"]),
            str(row["admission_fence_id"]),
            int(row["admission_fence_generation"]),
            str(row["payload_json"]),
            str(row["payload_digest"]),
            str(row["trace_id"]),
            str(row["correlation_id"]),
            str(row["causation_id"]),
            float(row["occurred_at"]),
        )
        claimed = (
            claim.routing_job_id,
            claim.envelope.idempotency_key,
            claim.message_log_id,
            claim.envelope.version,
            claim.envelope.profile_id,
            claim.envelope.session_id,
            claim.envelope.ownership_generation,
            claim.envelope.admission_fence_id,
            claim.envelope.admission_fence_generation,
            payload_json,
            _digest(payload_json),
            claim.envelope.trace_id,
            claim.envelope.correlation_id,
            claim.envelope.causation_id,
            claim.envelope.occurred_at,
        )
        if persisted != claimed:
            raise DurableRoutingConflict("routing job claim no longer matches durable work")

    @staticmethod
    def _validate_claimed_delivery(
        row: sqlite3.Row,
        claim: ClaimedAgentRouteDelivery,
        delivery: AgentRouteDelivery,
    ) -> None:
        persisted = (
            delivery,
            str(row["routing_job_id"]),
            str(row["correlation_id"]),
            str(row["causation_id"]),
            int(row["ownership_generation"]),
            str(row["admission_fence_id"]),
            int(row["admission_fence_generation"]),
        )
        claimed = (
            claim.delivery,
            claim.routing_job_id,
            claim.correlation_id,
            claim.causation_id,
            claim.ownership_generation,
            claim.admission_fence_id,
            claim.admission_fence_generation,
        )
        if persisted != claimed:
            raise DurableRoutingConflict("outbox claim no longer matches durable work")

    @staticmethod
    def _validate_outbox_identity(
        row: sqlite3.Row,
        *,
        delivery: AgentRouteDelivery,
        routing_job_id: str,
        correlation_id: str,
        ownership_generation: int,
        admission_fence_id: str,
        admission_fence_generation: int,
        payload_json: str,
        payload_digest: str,
    ) -> None:
        persisted = (
            str(row["delivery_id"]),
            str(row["idempotency_key"]),
            str(row["routing_job_id"]),
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["message_log_id"]),
            str(row["route_rule_id"]),
            int(row["version"]),
            int(row["ownership_generation"]),
            str(row["admission_fence_id"]),
            int(row["admission_fence_generation"]),
            str(row["event_id"]),
            str(row["payload_json"]),
            str(row["payload_digest"]),
            str(row["trace_id"]),
            str(row["correlation_id"]),
            str(row["causation_id"]),
        )
        requested = (
            delivery.delivery_id,
            delivery.idempotency_key,
            routing_job_id,
            delivery.session_key.profile_id,
            delivery.session_key.session_id,
            delivery.message_log_id,
            delivery.route_rule_id,
            delivery.version,
            ownership_generation,
            admission_fence_id,
            admission_fence_generation,
            delivery.event_id,
            payload_json,
            payload_digest,
            delivery.trace_id,
            correlation_id,
            routing_job_id,
        )
        if persisted != requested:
            raise DurableRoutingConflict(
                f"Agent delivery id {delivery.delivery_id!r} is already used by different work"
            )

    def _validate_completed_decision(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        decision_kind: str,
        decision_id: str,
        decision_payload_json: str,
        decision_payload_digest: str,
        deliveries: Sequence[AgentRouteDelivery],
    ) -> None:
        persisted = (
            int(row["decision_version"] or 0),
            str(row["decision_kind"]),
            str(row["decision_id"]),
            str(row["decision_payload_json"]),
            str(row["decision_payload_digest"]),
        )
        requested = (
            _ROUTE_DECISION_VERSION,
            decision_kind,
            decision_id,
            decision_payload_json,
            decision_payload_digest,
        )
        if persisted != requested:
            raise DurableRoutingConflict(
                f"routing job {row['routing_job_id']!r} already has a different decision"
            )
        for delivery in deliveries:
            outbox = conn.execute(
                "SELECT * FROM agent_route_outbox WHERE delivery_id = ?",
                (delivery.delivery_id,),
            ).fetchone()
            if outbox is None:
                raise DurableRoutingConflict("completed routing decision is missing its outbox row")
            payload_json = _canonical_json_object(delivery.to_payload())
            self._validate_outbox_identity(
                outbox,
                delivery=delivery,
                routing_job_id=str(row["routing_job_id"]),
                correlation_id=str(row["correlation_id"]),
                ownership_generation=int(outbox["ownership_generation"]),
                admission_fence_id=str(outbox["admission_fence_id"]),
                admission_fence_generation=int(outbox["admission_fence_generation"]),
                payload_json=payload_json,
                payload_digest=_digest(payload_json),
            )

    @staticmethod
    def _validate_live_claim(
        row: sqlite3.Row,
        *,
        claim_id: str,
        worker_id: str,
        now: float,
        subject: str,
    ) -> None:
        if (
            str(row["status"]) != "processing"
            or str(row["claim_id"]) != claim_id
            or str(row["lease_owner"]) != worker_id
            or float(row["lease_until"] or 0.0) <= now
        ):
            raise DurableRoutingLeaseLost(f"{subject} lease is expired or no longer owned")

    def _retry_or_fail_claim(
        self,
        *,
        table: str,
        id_column: str,
        record_id: str,
        claim_id: str,
        worker_id: str,
        error_code: str,
        error_message: str,
        retry_at: float | None,
    ) -> None:
        if (table, id_column) not in {
            ("message_routing_jobs", "routing_job_id"),
            ("agent_route_outbox", "delivery_id"),
        }:
            raise ValueError("unsupported durable routing claim table")
        now = self._clock()
        terminal = retry_at is None
        available_at = now if retry_at is None else _finite_nonnegative(retry_at, "retry_at")
        status = "failed" if terminal else "pending"
        failed_at = now if terminal else None
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            query = f"""
                UPDATE {table}
                SET status = ?,
                    available_at = ?,
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    updated_at = ?,
                    failed_at = ?,
                    last_error_code = ?,
                    last_error_message = ?
                WHERE {id_column} = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND COALESCE(lease_until, 0) > ?
            """
            updated = conn.execute(
                query,
                (
                    status,
                    available_at,
                    now,
                    failed_at,
                    str(error_code or "").strip(),
                    str(error_message or "").strip(),
                    record_id,
                    claim_id,
                    worker_id,
                    now,
                ),
            )
            if updated.rowcount != 1:
                raise DurableRoutingLeaseLost("durable routing lease is expired or no longer owned")
            if terminal and table == "message_routing_jobs":
                conn.execute(
                    """
                    UPDATE message_logs
                    SET routing_status = ?,
                        routed_at = ?,
                        routing_skip_reason = NULL
                    WHERE id = (
                        SELECT message_log_id
                        FROM message_routing_jobs
                        WHERE routing_job_id = ?
                    )
                    """,
                    (MessageRoutingStatus.FAILED.value, now * 1000, record_id),
                )

    @staticmethod
    def _ensure_actor_aggregate(
        conn: sqlite3.Connection,
        delivery: AgentRouteDelivery,
        *,
        ownership_generation: int,
        now: float,
    ) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, state,
                state_revision, event_sequence, activity_generation,
                active_epoch, review_plan_json,
                current_plan_id, review_plan_revision,
                active_reply_resume_json, active_chat_state_json,
                review_operation_id, active_reply_operation_id,
                active_chat_round_operation_id, idle_planning_operation_id,
                data_json, created_at, updated_at
            ) VALUES (?, ?, ?, 'idle', 0, 0, 0, 0, '{}', '', 0, '{}', '{}', '', '', '', '', '{}', ?, ?)
            """,
            (
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
                ownership_generation,
                now,
                now,
            ),
        )
        row = conn.execute(
            """
            SELECT ownership_generation
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
            ),
        ).fetchone()
        if row is None or int(row["ownership_generation"]) != ownership_generation:
            raise DurableRoutingConflict(
                "actor aggregate ownership generation differs from route delivery"
            )

    @staticmethod
    def _validate_mailbox_event(
        conn: sqlite3.Connection,
        outbox_row: sqlite3.Row,
        delivery: AgentRouteDelivery,
    ) -> int:
        row = conn.execute(
            """
            SELECT *
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (
                delivery.session_key.profile_id,
                delivery.session_key.session_id,
                delivery.event_id,
            ),
        ).fetchone()
        if row is None:
            raise DurableRoutingConflict("completed outbox delivery is missing its mailbox event")
        payload_json = _canonical_json_object(delivery.to_mailbox_payload())
        persisted = (
            int(row["ownership_generation"]),
            str(row["kind"]),
            str(row["source"]),
            float(row["occurred_at"]),
            str(row["payload_json"]),
            str(row["causation_id"]),
            str(row["correlation_id"]),
            str(row["trace_id"]),
        )
        expected = (
            int(outbox_row["ownership_generation"]),
            AGENT_ROUTE_MAILBOX_KIND,
            AGENT_ROUTE_MAILBOX_SOURCE,
            delivery.observed_at,
            payload_json,
            str(outbox_row["routing_job_id"]),
            str(outbox_row["routing_job_id"]),
            delivery.trace_id,
        )
        if persisted != expected:
            raise DurableRoutingConflict(
                f"mailbox event id {delivery.event_id!r} is already used by different work"
            )
        mailbox_id = int(row["mailbox_id"])
        if mailbox_id < 1:
            raise DurableRoutingConflict("route mailbox event does not have a durable primary key")
        return mailbox_id

    @staticmethod
    def _relay_result(
        delivery: AgentRouteDelivery,
        *,
        ownership_generation: int,
        admission_fence_id: str,
        admission_fence_generation: int,
        mailbox_id: int,
        mailbox_inserted: bool,
    ) -> RouteRelayResult:
        return RouteRelayResult(
            delivery_id=delivery.delivery_id,
            event_id=delivery.event_id,
            mailbox_id=mailbox_id,
            profile_id=delivery.session_key.profile_id,
            session_id=delivery.session_key.session_id,
            mailbox_inserted=mailbox_inserted,
            wake_request=FencedMailboxWakeRequest(
                key=delivery.session_key,
                ownership_generation=ownership_generation,
                admission_fence_id=admission_fence_id,
                admission_fence_generation=admission_fence_generation,
            ),
        )

    @staticmethod
    def _validate_pending_message(record: MessageLogRecord) -> None:
        if routing_status_value(record.routing_status) != MessageRoutingStatus.PENDING.value:
            raise ValueError("a new routing job requires a pending message log")
        if record.routed_at is not None or record.routing_skip_reason is not None:
            raise ValueError("a new routing job cannot already carry a routing decision")

    @staticmethod
    def _find_job_by_identity(
        conn: sqlite3.Connection,
        envelope: MessageRoutingJobEnvelope,
    ) -> sqlite3.Row | None:
        rows = conn.execute(
            """
            SELECT *
            FROM message_routing_jobs
            WHERE routing_job_id = ? OR idempotency_key = ?
            """,
            (envelope.job_id, envelope.idempotency_key),
        ).fetchall()
        if len(rows) > 1:
            raise DurableRoutingConflict(
                "routing job id and idempotency key resolve to different durable jobs"
            )
        return rows[0] if rows else None

    @staticmethod
    def _resolve_admission_envelope_for_persistence(
        conn: sqlite3.Connection,
        envelope: MessageRoutingJobEnvelope,
    ) -> MessageRoutingJobEnvelope:
        """Bind a stale reserved ingress envelope to its current fence state.

        Ingress may observe ``reserved`` immediately before the owner claim
        commits. This function runs inside the message/job transaction, so a
        post-commit insert is written directly against the committed owner
        rather than leaving a generation-zero job behind the retarget pass.
        """

        if not envelope.has_admission_fence:
            return envelope
        fence = conn.execute(
            """
            SELECT status
            FROM agent_session_actor_v2_admission_fences
            WHERE profile_id = ?
              AND session_id = ?
              AND fence_id = ?
              AND generation = ?
            """,
            (
                envelope.profile_id,
                envelope.session_id,
                envelope.admission_fence_id,
                envelope.admission_fence_generation,
            ),
        ).fetchone()
        if fence is None:
            raise DurableRoutingConflict(
                "routing job admission fence does not match durable reservation history"
            )
        status = str(fence["status"])
        if status == "reserved":
            if envelope.ownership_generation != 0:
                raise DurableRoutingConflict(
                    "reserved admission fence cannot persist an owned routing job"
                )
            return envelope
        if status == "revoked":
            return envelope
        if status != "committed":
            raise DurableRoutingConflict("routing job admission fence has an invalid status")
        ownership = conn.execute(
            """
            SELECT mode, status, generation, admission_fence_id,
                   admission_fence_generation
            FROM agent_session_runtime_ownership
            WHERE profile_id = ? AND session_id = ?
            """,
            (envelope.profile_id, envelope.session_id),
        ).fetchone()
        if ownership is None or (
            str(ownership["mode"]) != "actor_v2"
            or str(ownership["status"]) != "active"
            or str(ownership["admission_fence_id"])
            != envelope.admission_fence_id
            or int(ownership["admission_fence_generation"])
            != envelope.admission_fence_generation
        ):
            raise DurableRoutingConflict(
                "committed admission fence has no matching active Actor v2 ownership"
            )
        ownership_generation = int(ownership["generation"])
        if envelope.ownership_generation not in {0, ownership_generation}:
            raise DurableRoutingConflict(
                "routing job ownership generation differs from committed admission owner"
            )
        if envelope.ownership_generation == ownership_generation:
            return envelope
        return replace(envelope, ownership_generation=ownership_generation)

    @staticmethod
    def _validate_existing_job(
        row: sqlite3.Row,
        *,
        envelope: MessageRoutingJobEnvelope,
        message_fingerprint: str,
        payload_json: str,
        payload_digest: str,
    ) -> None:
        persisted = (
            str(row["routing_job_id"]),
            str(row["idempotency_key"]),
            int(row["version"]),
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["ownership_generation"]),
            str(row["admission_fence_id"]),
            int(row["admission_fence_generation"]),
            str(row["message_fingerprint"]),
            str(row["payload_json"]),
            str(row["payload_digest"]),
            str(row["trace_id"]),
            str(row["correlation_id"]),
            str(row["causation_id"]),
        )
        requested = (
            envelope.job_id,
            envelope.idempotency_key,
            envelope.version,
            envelope.profile_id,
            envelope.session_id,
            envelope.ownership_generation,
            envelope.admission_fence_id,
            envelope.admission_fence_generation,
            message_fingerprint,
            payload_json,
            payload_digest,
            envelope.trace_id,
            envelope.correlation_id,
            envelope.causation_id,
        )
        if persisted != requested or _digest(str(row["payload_json"])) != payload_digest:
            raise DurableRoutingConflict(
                f"routing job identity {envelope.job_id!r} is already used by different work"
            )


def _claimed_job_from_row(
    row: sqlite3.Row,
    *,
    claim_id: str,
    worker_id: str,
    claimed_at: float,
    lease_expires_at: float,
) -> ClaimedMessageRoutingJob:
    envelope = _job_envelope_from_row(row)
    return ClaimedMessageRoutingJob(
        envelope=envelope,
        message_log_id=int(row["message_log_id"]),
        claim_id=claim_id,
        worker_id=worker_id,
        attempt_count=int(row["attempt_count"]),
        claimed_at=claimed_at,
        lease_expires_at=lease_expires_at,
    )


def _fenced_request_scope_clause(
    table_name: str,
    request: FencedMailboxWakeRequest | None,
) -> tuple[str, tuple[object, ...]]:
    """Build an exact request filter for bounded fenced relay work.

    A caller may not scope work by a bare session key or ownership generation:
    both admission-fence columns are required so a replacement target cannot
    claim work retained for a prior incarnation of the same session.
    """

    if table_name not in {"message_routing_jobs", "agent_route_outbox"}:
        raise ValueError("unsupported durable routing scope table")
    if request is None:
        return "", ()
    if not isinstance(request, FencedMailboxWakeRequest):
        raise TypeError("expected_fenced_request must be a FencedMailboxWakeRequest")
    if not request.has_admission_fence:
        raise ValueError("expected_fenced_request must carry an admission fence")
    return (
        f"""
                  AND {table_name}.profile_id = ?
                  AND {table_name}.session_id = ?
                  AND {table_name}.ownership_generation = ?
                  AND {table_name}.admission_fence_id = ?
                  AND {table_name}.admission_fence_generation = ?
        """,
        (
            request.key.profile_id,
            request.key.session_id,
            request.ownership_generation,
            request.admission_fence_id,
            request.admission_fence_generation,
        ),
    )


def _job_envelope_from_row(row: sqlite3.Row) -> MessageRoutingJobEnvelope:
    payload_json = str(row["payload_json"])
    if _digest(payload_json) != str(row["payload_digest"]):
        raise DurableRoutingConflict("message routing job payload digest is invalid")
    payload = _json_object(payload_json)
    if payload_json != _canonical_json_object(payload):
        raise DurableRoutingConflict("message routing job payload is not canonical JSON")
    return MessageRoutingJobEnvelope(
        job_id=str(row["routing_job_id"]),
        idempotency_key=str(row["idempotency_key"]),
        trace_id=str(row["trace_id"]),
        payload=payload,
        profile_id=str(row["profile_id"]),
        session_id=str(row["session_id"]),
        ownership_generation=int(row["ownership_generation"]),
        admission_fence_id=str(row["admission_fence_id"]),
        admission_fence_generation=int(row["admission_fence_generation"]),
        correlation_id=str(row["correlation_id"]),
        causation_id=str(row["causation_id"]),
        occurred_at=float(row["occurred_at"]),
        available_at=float(row["available_at"]),
        version=int(row["version"]),
    )


def _message_fingerprint(record: MessageLogRecord) -> str:
    payload_json = _canonical_json_object(
        {
            "session_id": record.session_id,
            "platform_msg_id": record.platform_msg_id,
            "sender_id": record.sender_id,
            "sender_name": record.sender_name,
            "content_json": record.content_json,
            "raw_text": record.raw_text,
            "role": record.role,
            "is_read": bool(record.is_read),
            "is_mentioned": bool(record.is_mentioned),
            "created_at": record.created_at,
            "routing_status": routing_status_value(record.routing_status),
            "routed_at": record.routed_at,
            "routing_skip_reason": record.routing_skip_reason,
        }
    )
    return _digest(payload_json)


def _canonical_json_object(value: Mapping[str, Any]) -> str:
    normalized = _normalize_json_value(value)
    if not isinstance(normalized, dict):
        raise TypeError("durable routing payload must be a JSON object")
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _normalize_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("durable routing JSON numbers must be finite")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("durable routing JSON object keys must be strings")
            normalized[key] = _normalize_json_value(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item) for item in value]
    raise TypeError(f"value is not JSON serializable: {type(value).__name__}")


def _json_object(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DurableRoutingConflict("durable routing payload is invalid JSON") from exc
    if not isinstance(loaded, dict):
        raise DurableRoutingConflict("durable routing payload must be a JSON object")
    return {str(key): item for key, item in loaded.items()}


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _required_identifier(value: object, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _finite_nonnegative(value: float, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and non-negative")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return numeric


def _route_wake_cursor_parameters(
    cursor: RouteWakeCursor,
) -> tuple[object, ...]:
    """Expand one route wake cursor for the deterministic SQL keyset chain."""

    mailbox_id = cursor.mailbox_id
    profile_id = cursor.profile_id
    session_id = cursor.session_id
    ownership_generation = cursor.ownership_generation
    fence_id = cursor.admission_fence_id
    fence_generation = cursor.admission_fence_generation
    return (
        mailbox_id,
        mailbox_id,
        profile_id,
        mailbox_id,
        profile_id,
        session_id,
        mailbox_id,
        profile_id,
        session_id,
        ownership_generation,
        mailbox_id,
        profile_id,
        session_id,
        ownership_generation,
        fence_id,
        mailbox_id,
        profile_id,
        session_id,
        ownership_generation,
        fence_id,
        fence_generation,
    )


def _job_status(value: str) -> MessageRoutingJobStatus:
    try:
        return MessageRoutingJobStatus(value)
    except ValueError as exc:
        raise DurableRoutingConflict(f"unknown message routing job status: {value}") from exc


__all__ = [
    "ClaimedAgentRouteDelivery",
    "ClaimedMessageRoutingJob",
    "DurableMessageRoutingRepository",
    "DurableRoutingConflict",
    "DurableRoutingError",
    "DurableRoutingLeaseLost",
    "DurableRoutingRecordNotFound",
    "PendingRouteWakeDebt",
    "PersistRoutingJobResult",
    "PersistedMessageRoutingJob",
    "RouteWakeCursor",
    "RouteDecisionResult",
    "RouteRelayResult",
]
