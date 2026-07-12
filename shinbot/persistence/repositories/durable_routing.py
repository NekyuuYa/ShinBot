"""SQLite unit-of-work for recoverable core-to-Agent message routing."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
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
    profile_id: str
    session_id: str
    mailbox_inserted: bool

    @property
    def duplicate(self) -> bool:
        """Return whether the canonical mailbox event already existed."""

        return not self.mailbox_inserted


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
            existing = self._find_job_by_identity(conn, envelope)
            if existing is not None:
                self._validate_existing_job(
                    existing,
                    envelope=envelope,
                    message_fingerprint=message_fingerprint,
                    payload_json=payload_json,
                    payload_digest=payload_digest,
                )
                return PersistRoutingJobResult(
                    routing_job_id=envelope.job_id,
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
                    message_fingerprint, payload_json, payload_digest,
                    trace_id, correlation_id, causation_id, occurred_at, status,
                    attempt_count, available_at, claim_id, lease_owner,
                    lease_until, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, ?)
                """,
                (
                    envelope.job_id,
                    envelope.idempotency_key,
                    message_log_id,
                    envelope.version,
                    envelope.profile_id,
                    envelope.session_id,
                    envelope.ownership_generation,
                    message_fingerprint,
                    payload_json,
                    payload_digest,
                    envelope.trace_id,
                    envelope.correlation_id,
                    envelope.causation_id,
                    occurred_at,
                    available_at,
                    now,
                    now,
                ),
            )
        record.id = message_log_id
        return PersistRoutingJobResult(
            routing_job_id=envelope.job_id,
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

    def claim_next_job(self, *, worker_id: str) -> ClaimedMessageRoutingJob | None:
        """Claim the oldest available routing job, reclaiming expired leases."""

        worker = _required_identifier(worker_id, "worker_id")
        now = self._clock()
        lease_expires_at = now + self._lease_seconds
        claim_id = uuid.uuid4().hex
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT *
                FROM message_routing_jobs
                WHERE (
                    (status = 'pending' AND available_at <= ?)
                    OR (status = 'processing' AND COALESCE(lease_until, 0) <= ?)
                )
                  AND (
                    (
                        profile_id = ''
                        AND session_id = ''
                        AND ownership_generation = 0
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM agent_session_runtime_ownership AS ownership
                        WHERE ownership.profile_id = message_routing_jobs.profile_id
                          AND ownership.session_id = message_routing_jobs.session_id
                          AND ownership.status = 'active'
                          AND ownership.generation =
                              message_routing_jobs.ownership_generation
                    )
                )
                ORDER BY routing_job_seq ASC
                LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                """
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
                  AND (
                    (
                        profile_id = ''
                        AND session_id = ''
                        AND ownership_generation = 0
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM agent_session_runtime_ownership AS ownership
                        WHERE ownership.profile_id = message_routing_jobs.profile_id
                          AND ownership.session_id = message_routing_jobs.session_id
                          AND ownership.status = 'active'
                          AND ownership.generation =
                              message_routing_jobs.ownership_generation
                    )
                  )
                """,
                (
                    claim_id,
                    worker,
                    lease_expires_at,
                    now,
                    row["routing_job_seq"],
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
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
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
                  AND (
                    (
                        profile_id = ''
                        AND session_id = ''
                        AND ownership_generation = 0
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM agent_session_runtime_ownership AS ownership
                        WHERE ownership.profile_id = message_routing_jobs.profile_id
                          AND ownership.session_id = message_routing_jobs.session_id
                          AND ownership.status = 'active'
                          AND ownership.generation =
                              message_routing_jobs.ownership_generation
                    )
                  )
                """,
                (job_id, ignore_deadline, now, now),
            ).fetchone()
            if row is None:
                return None
            updated = conn.execute(
                """
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
                  AND (
                    (
                        profile_id = ''
                        AND session_id = ''
                        AND ownership_generation = 0
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM agent_session_runtime_ownership AS ownership
                        WHERE ownership.profile_id = message_routing_jobs.profile_id
                          AND ownership.session_id = message_routing_jobs.session_id
                          AND ownership.status = 'active'
                          AND ownership.generation =
                              message_routing_jobs.ownership_generation
                    )
                  )
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
    ) -> ClaimedAgentRouteDelivery | None:
        """Claim the oldest available Agent route outbox delivery."""

        worker = _required_identifier(worker_id, "worker_id")
        now = self._clock()
        lease_expires_at = now + self._lease_seconds
        claim_id = uuid.uuid4().hex
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT *
                FROM agent_route_outbox
                WHERE (
                    (status = 'pending' AND available_at <= ?)
                    OR (status = 'processing' AND COALESCE(lease_until, 0) <= ?)
                )
                  AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id = agent_route_outbox.profile_id
                      AND ownership.session_id = agent_route_outbox.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_route_outbox.ownership_generation
                )
                ORDER BY outbox_seq ASC
                LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                return None
            delivery = self._delivery_from_outbox_row(row)
            updated = conn.execute(
                """
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
                  AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id = agent_route_outbox.profile_id
                      AND ownership.session_id = agent_route_outbox.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_route_outbox.ownership_generation
                  )
                """,
                (
                    claim_id,
                    worker,
                    lease_expires_at,
                    now,
                    row["outbox_seq"],
                    now,
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
            if str(row["status"]) == AgentRouteOutboxStatus.COMPLETED.value:
                self._validate_mailbox_event(conn, row, delivery)
                return self._relay_result(delivery, mailbox_inserted=False)
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
                expected_generation=int(row["ownership_generation"]),
            )
            ownership_generation = int(row["ownership_generation"])
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
            if inserted.rowcount != 1:
                self._validate_mailbox_event(conn, row, delivery)
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
            return self._relay_result(
                delivery,
                mailbox_inserted=inserted.rowcount == 1,
            )

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

    def next_job_available_at(self) -> float | None:
        """Return the next pending deadline or expired-lease recovery time."""

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(
                    CASE
                        WHEN status = 'pending' THEN available_at
                        ELSE COALESCE(lease_until, available_at)
                    END
                ) AS next_at
                FROM message_routing_jobs
                WHERE status IN ('pending', 'processing')
                  AND (
                    (
                        profile_id = ''
                        AND session_id = ''
                        AND ownership_generation = 0
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM agent_session_runtime_ownership AS ownership
                        WHERE ownership.profile_id = message_routing_jobs.profile_id
                          AND ownership.session_id = message_routing_jobs.session_id
                          AND ownership.status = 'active'
                          AND ownership.generation =
                              message_routing_jobs.ownership_generation
                    )
                  )
                """
            ).fetchone()
        if row is None or row["next_at"] is None:
            return None
        return float(row["next_at"])

    def next_delivery_available_at(self) -> float | None:
        """Return the next Agent outbox deadline or lease recovery time."""

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(
                    CASE
                        WHEN status = 'pending' THEN available_at
                        ELSE COALESCE(lease_until, available_at)
                    END
                ) AS next_at
                FROM agent_route_outbox
                WHERE status IN ('pending', 'processing')
                  AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id = agent_route_outbox.profile_id
                      AND ownership.session_id = agent_route_outbox.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_route_outbox.ownership_generation
                  )
                """
            ).fetchone()
        if row is None or row["next_at"] is None:
            return None
        return float(row["next_at"])

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
            inserted_delivery_count = 0
            for delivery in normalized:
                if (
                    expected_ownership_generations is not None
                    and delivery.session_key not in expected_ownership_generations
                ):
                    raise DurableRoutingConflict(
                        "routing decision omitted an expected ownership generation"
                    )
                inserted_delivery_count += self._insert_outbox_delivery(
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
            return RouteDecisionResult(
                routing_job_id=claim.routing_job_id,
                decision_id=decision_id,
                delivery_ids=tuple(item.delivery_id for item in normalized),
                inserted_delivery_count=inserted_delivery_count,
            )

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

    def _insert_outbox_delivery(
        self,
        conn: sqlite3.Connection,
        job_row: sqlite3.Row,
        delivery: AgentRouteDelivery,
        *,
        now: float,
        expected_ownership_generation: int | None,
    ) -> int:
        payload_json = _canonical_json_object(delivery.to_payload())
        payload_digest = _digest(payload_json)
        ownership = self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            delivery.session_key,
            expected_generation=expected_ownership_generation,
        )
        inserted = conn.execute(
            """
            INSERT OR IGNORE INTO agent_route_outbox (
                delivery_id, idempotency_key, routing_job_id, profile_id,
                session_id, message_log_id, route_rule_id, version,
                ownership_generation, event_id, payload_json, payload_digest,
                trace_id, correlation_id,
                causation_id, status, attempt_count, available_at, claim_id,
                lease_owner, lease_until, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, ?)
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
            ownership_generation=ownership.generation,
            payload_json=payload_json,
            payload_digest=payload_digest,
        )
        return 1 if inserted.rowcount == 1 else 0

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
        )
        claimed = (
            claim.delivery,
            claim.routing_job_id,
            claim.correlation_id,
            claim.causation_id,
            claim.ownership_generation,
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
    ) -> None:
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

    @staticmethod
    def _relay_result(
        delivery: AgentRouteDelivery,
        *,
        mailbox_inserted: bool,
    ) -> RouteRelayResult:
        return RouteRelayResult(
            delivery_id=delivery.delivery_id,
            event_id=delivery.event_id,
            profile_id=delivery.session_key.profile_id,
            session_id=delivery.session_key.session_id,
            mailbox_inserted=mailbox_inserted,
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
    "PersistRoutingJobResult",
    "PersistedMessageRoutingJob",
    "RouteDecisionResult",
    "RouteRelayResult",
]
