"""Durable review-due dispatch for profile-scoped session actors.

The scanner performs no workflow work and does not inspect the actor's busy
state.  Its only job is to atomically fence one current review schedule,
enqueue a deterministic ``ReviewDue`` mailbox event, and mark that schedule
claimed.  Actor reduction decides what the event means for the current state.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from sqlite3 import Connection, Row
from typing import TYPE_CHECKING, Protocol

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    supervised_backoff_seconds,
)
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.review_due_identity import (
    REVIEW_DUE_EVENT_KIND,
    REVIEW_DUE_EVENT_SOURCE,
    review_due_event_id,
)
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.utils.logger import format_log_event, get_logger

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__, source="agent:review-due", color="yellow")

GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID = "__actor_v2_global__"
_SCHEDULE_EVENT_NAMESPACE = uuid.UUID("b022e214-5043-59f4-abcb-a55d654dd7f0")

type _DueScanCursor = tuple[float, float, str, str, int, str]
type _TypedSQLiteRecord = tuple[tuple[str, str, object], ...]


class ReviewDueRepositoryError(RuntimeError):
    """Base error raised by durable review-due persistence."""


class ReviewDueConflict(ReviewDueRepositoryError):
    """Raised when deterministic durable identity resolves to other work."""


class ReviewDueWakeError(RuntimeError):
    """Report committed ReviewDue events whose best-effort wake failed."""

    def __init__(self, keys: tuple[SessionKey, ...]) -> None:
        self.keys = keys
        rendered = ", ".join(f"{key.profile_id}:{key.session_id}" for key in keys)
        super().__init__(f"review-due wake failed for: {rendered}")


class ReviewDueDisposition(StrEnum):
    """Durable result of processing one due schedule row."""

    DISPATCHED = "dispatched"
    SUPERSEDED = "superseded"
    RETRY_DEFERRED = "retry_deferred"
    FENCE_SKIPPED = "fence_skipped"


@dataclass(slots=True, frozen=True)
class ReviewDueDispatchResult:
    """Outcome for one schedule inspected in a short transaction."""

    key: SessionKey
    plan_id: str
    plan_revision: int
    ownership_generation: int
    delivery_cycle: int
    disposition: ReviewDueDisposition
    event_id: str = ""
    mailbox_inserted: bool = False
    reason: str = ""
    retry_at: float | None = None


@dataclass(slots=True, frozen=True)
class ReviewDueScanSummary:
    """Bounded aggregate result for one repository scan pass."""

    results: tuple[ReviewDueDispatchResult, ...] = ()

    @property
    def attempted_count(self) -> int:
        """Return the number of schedule rows removed from the due page."""

        return len(self.results)

    @property
    def dispatched_count(self) -> int:
        """Return the number of current plans durably dispatched."""

        return sum(
            result.disposition is ReviewDueDisposition.DISPATCHED
            for result in self.results
        )

    @property
    def superseded_count(self) -> int:
        """Return the number of stale plans atomically superseded."""

        return sum(
            result.disposition is ReviewDueDisposition.SUPERSEDED
            for result in self.results
        )

    @property
    def deferred_count(self) -> int:
        """Return the number of unavailable rows assigned an explicit retry."""

        return sum(
            result.disposition is ReviewDueDisposition.RETRY_DEFERRED
            for result in self.results
        )

    @property
    def fence_skipped_count(self) -> int:
        """Return rows left untouched because no exact actor fence existed."""

        return sum(
            result.disposition is ReviewDueDisposition.FENCE_SKIPPED
            for result in self.results
        )

    @property
    def skipped_count(self) -> int:
        """Return rows that did not dispatch a ReviewDue mailbox event."""

        return self.attempted_count - self.dispatched_count

    @property
    def dispatched_keys(self) -> tuple[SessionKey, ...]:
        """Return unique keys which received a committed mailbox event."""

        return _unique_keys(
            result.key
            for result in self.results
            if result.disposition is ReviewDueDisposition.DISPATCHED
        )


class ReviewDueWakeTarget(Protocol):
    """Actor registry surface used only after the scanner transaction commits."""

    async def wake(self, key: SessionKey) -> None:
        """Wake one actor without inserting another mailbox event."""


class DurableReviewDueRepository:
    """SQLite repository for exact current-plan ReviewDue dispatch."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        retry_base_seconds: float = 5.0,
        retry_max_seconds: float = 300.0,
        clock: Callable[[], float] | None = None,
        profile_id: str | None = None,
    ) -> None:
        """Initialize bounded retry and optional profile filtering."""

        self._database = database
        self._retry_base_seconds = _positive_finite(
            retry_base_seconds,
            field_name="retry_base_seconds",
        )
        self._retry_max_seconds = _positive_finite(
            retry_max_seconds,
            field_name="retry_max_seconds",
        )
        if self._retry_max_seconds < self._retry_base_seconds:
            raise ValueError("retry_max_seconds cannot be below retry_base_seconds")
        self._clock = clock or time.time
        normalized_profile_id = None
        if profile_id is not None:
            normalized_profile_id = str(profile_id or "").strip()
            if not normalized_profile_id:
                raise ValueError("profile_id filter must not be empty")
        self._profile_id = normalized_profile_id

    @property
    def health_profile_id(self) -> str:
        """Return the durable service-health ownership key for this scanner."""

        return self._profile_id or GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID

    def dispatch_due(self, *, limit: int = 50) -> ReviewDueScanSummary:
        """Process at most ``limit`` rows, each in its own write transaction."""

        normalized_limit = _positive_int(limit, field_name="limit")
        results: list[ReviewDueDispatchResult] = []
        cursor: _DueScanCursor | None = None
        for _index in range(normalized_limit):
            scanned = self._dispatch_next_due(after=cursor)
            if scanned is None:
                break
            result, cursor = scanned
            results.append(result)
        return ReviewDueScanSummary(results=tuple(results))

    def pending_review_due_keys(self, *, limit: int = 100) -> tuple[SessionKey, ...]:
        """Discover durable wake debt from pending ReviewDue mailbox events."""

        normalized_limit = _positive_int(limit, field_name="limit")
        profile_clause = ""
        params: list[object] = []
        if self._profile_id is not None:
            profile_clause = " AND mailbox.profile_id = ?"
            params.append(self._profile_id)
        params.append(normalized_limit)
        with self._database.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT mailbox.profile_id, mailbox.session_id,
                       MIN(mailbox.mailbox_id) AS first_mailbox_id
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                WHERE mailbox.kind = ?
                  AND mailbox.source = ?
                  AND mailbox.status IN ('pending', 'processing')
                  {profile_clause}
                GROUP BY mailbox.profile_id, mailbox.session_id
                ORDER BY first_mailbox_id ASC,
                         mailbox.profile_id ASC, mailbox.session_id ASC
                LIMIT ?
                """,
                (REVIEW_DUE_EVENT_KIND, REVIEW_DUE_EVENT_SOURCE, *params),
            ).fetchall()
        return tuple(
            SessionKey(str(row["profile_id"]), str(row["session_id"]))
            for row in rows
        )

    def record_service_health(
        self,
        snapshot: RuntimeServiceHealthSnapshot,
        summary: ReviewDueScanSummary,
        *,
        runtime_id: str,
    ) -> None:
        """Persist one completed supervision pass for restart diagnostics."""

        normalized_runtime_id = str(runtime_id or "").strip()
        if not normalized_runtime_id:
            raise ValueError("runtime_id must not be empty")
        now = _nonnegative_finite(self._clock(), field_name="clock")
        skipped = summary.skipped_count
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO agent_runtime_service_health (
                    profile_id, service_name, runtime_id, status, expected,
                    started_at, heartbeat_at, last_scan_started_at,
                    last_scan_finished_at, last_success_at, last_error_at,
                    last_error_code, last_error_message, consecutive_failures,
                    restart_count, scan_count, due_seen_count, dispatch_count,
                    skip_count, in_flight_count, lease_owner, updated_at
                ) VALUES (?, 'durable_review_due_scanner', ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          0, 1, ?, ?, ?, 0, '', ?)
                ON CONFLICT(profile_id, service_name) DO UPDATE SET
                    runtime_id = excluded.runtime_id,
                    status = excluded.status,
                    expected = 1,
                    started_at = CASE
                        WHEN agent_runtime_service_health.started_at IS NULL
                             OR agent_runtime_service_health.started_at = 0
                        THEN excluded.started_at
                        ELSE agent_runtime_service_health.started_at
                    END,
                    heartbeat_at = excluded.heartbeat_at,
                    last_scan_started_at = excluded.last_scan_started_at,
                    last_scan_finished_at = excluded.last_scan_finished_at,
                    last_success_at = excluded.last_success_at,
                    last_error_at = excluded.last_error_at,
                    last_error_code = excluded.last_error_code,
                    last_error_message = excluded.last_error_message,
                    consecutive_failures = excluded.consecutive_failures,
                    scan_count = agent_runtime_service_health.scan_count + 1,
                    due_seen_count = agent_runtime_service_health.due_seen_count
                                     + excluded.due_seen_count,
                    dispatch_count = agent_runtime_service_health.dispatch_count
                                     + excluded.dispatch_count,
                    skip_count = agent_runtime_service_health.skip_count
                                 + excluded.skip_count,
                    in_flight_count = 0,
                    lease_owner = '',
                    updated_at = excluded.updated_at
                """,
                (
                    self.health_profile_id,
                    normalized_runtime_id,
                    snapshot.status.value,
                    snapshot.started_at,
                    now,
                    snapshot.last_scan_started_at,
                    now,
                    snapshot.last_success_at,
                    snapshot.last_error_at,
                    snapshot.last_error_code,
                    snapshot.last_error_message,
                    snapshot.consecutive_failures,
                    summary.attempted_count,
                    summary.dispatched_count,
                    skipped,
                    now,
                ),
            )

    def _dispatch_next_due(
        self,
        *,
        after: _DueScanCursor | None,
    ) -> tuple[ReviewDueDispatchResult, _DueScanCursor] | None:
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _nonnegative_finite(self._clock(), field_name="clock")
            schedule = self._select_next_due(conn, now=now, after=after)
            if schedule is None:
                return None
            cursor = _due_scan_cursor(schedule)
            key = SessionKey(
                str(schedule["profile_id"]),
                str(schedule["session_id"]),
            )
            owner = conn.execute(
                """
                SELECT mode, status, generation
                FROM agent_session_runtime_ownership
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            unavailable_reason = _ownership_unavailable_reason(owner)
            if unavailable_reason:
                return self._skip_unfenced_schedule(
                    schedule,
                    reason=unavailable_reason,
                ), cursor

            assert owner is not None
            owner_generation = int(owner["generation"])
            if int(schedule["ownership_generation"]) != owner_generation:
                return self._skip_unfenced_schedule(
                    schedule,
                    reason="schedule_generation_mismatch",
                ), cursor
            aggregate = conn.execute(
                """
                SELECT ownership_generation, current_plan_id,
                       review_plan_revision, state_revision
                FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            if aggregate is None:
                return self._skip_unfenced_schedule(
                    schedule,
                    reason="aggregate_missing",
                ), cursor
            if int(aggregate["ownership_generation"]) != owner_generation:
                return self._skip_unfenced_schedule(
                    schedule,
                    reason="aggregate_generation_mismatch",
                ), cursor

            plan_id = str(schedule["plan_id"])
            plan_revision = int(schedule["plan_revision"])
            if (
                str(aggregate["current_plan_id"]) != plan_id
                or int(aggregate["review_plan_revision"]) != plan_revision
            ):
                return (
                    self._supersede_stale_schedule(
                        conn,
                        schedule,
                        aggregate,
                        now=now,
                        reason="aggregate_current_plan_mismatch",
                    ),
                    cursor,
                )
            return (
                self._dispatch_current_schedule(
                    conn,
                    schedule,
                    aggregate,
                    now=now,
                ),
                cursor,
            )

    def _select_next_due(
        self,
        conn: Connection,
        *,
        now: float,
        after: _DueScanCursor | None,
    ) -> Row | None:
        profile_clause = ""
        params: list[object] = [now, now]
        if self._profile_id is not None:
            profile_clause = " AND schedule.profile_id = ?"
            params.append(self._profile_id)
        cursor_clause = ""
        if after is not None:
            cursor_clause = """
              AND (
                    schedule.available_at, schedule.next_review_at,
                    schedule.profile_id, schedule.session_id,
                    schedule.plan_revision, schedule.plan_id
                  ) > (?, ?, ?, ?, ?, ?)
            """
            params.extend(after)
        return conn.execute(
            f"""
            SELECT schedule.*
            FROM agent_review_schedules AS schedule
            WHERE schedule.status = 'scheduled'
              AND schedule.available_at <= ?
              AND schedule.next_review_at <= ?
              {profile_clause}
              {cursor_clause}
            ORDER BY CASE
                         WHEN EXISTS (
                             SELECT 1
                             FROM agent_session_runtime_ownership AS ownership
                             JOIN agent_session_aggregates AS aggregate
                               ON aggregate.profile_id = ownership.profile_id
                              AND aggregate.session_id = ownership.session_id
                              AND aggregate.ownership_generation =
                                  schedule.ownership_generation
                             WHERE ownership.profile_id = schedule.profile_id
                               AND ownership.session_id = schedule.session_id
                               AND ownership.mode = 'actor_v2'
                               AND ownership.status = 'active'
                               AND ownership.generation =
                                   schedule.ownership_generation
                         ) THEN 0
                         ELSE 1
                     END ASC,
                     schedule.available_at ASC,
                     schedule.next_review_at ASC,
                     schedule.profile_id ASC,
                     schedule.session_id ASC,
                     schedule.plan_revision ASC,
                     schedule.plan_id ASC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()

    @staticmethod
    def _skip_unfenced_schedule(
        schedule: Row,
        *,
        reason: str,
    ) -> ReviewDueDispatchResult:
        """Leave actor-owned state frozen when the exact writer fence is absent."""

        return _result(
            schedule,
            disposition=ReviewDueDisposition.FENCE_SKIPPED,
            reason=reason,
        )

    def _defer_schedule(
        self,
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        now: float,
        reason: str,
    ) -> ReviewDueDispatchResult:
        attempt_count = int(schedule["attempt_count"]) + 1
        retry_at = _nonnegative_finite(
            now + self._retry_delay(attempt_count),
            field_name="retry_at",
        )
        updated = conn.execute(
            """
            UPDATE agent_review_schedules
            SET available_at = ?, attempt_count = ?, last_error = ?,
                claim_owner = '', claim_until = NULL, updated_at = ?
            WHERE plan_id = ? AND profile_id = ? AND session_id = ?
              AND plan_revision = ? AND ownership_generation = ?
              AND status = 'scheduled'
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id =
                          agent_review_schedules.profile_id
                      AND ownership.session_id =
                          agent_review_schedules.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_review_schedules.ownership_generation
              )
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_aggregates AS aggregate
                    WHERE aggregate.profile_id =
                          agent_review_schedules.profile_id
                      AND aggregate.session_id =
                          agent_review_schedules.session_id
                      AND aggregate.ownership_generation =
                          agent_review_schedules.ownership_generation
                      AND aggregate.state_revision = ?
                      AND aggregate.current_plan_id = ?
                      AND aggregate.review_plan_revision = ?
              )
            """,
            (
                retry_at,
                attempt_count,
                reason,
                now,
                schedule["plan_id"],
                schedule["profile_id"],
                schedule["session_id"],
                schedule["plan_revision"],
                schedule["ownership_generation"],
                aggregate["state_revision"],
                aggregate["current_plan_id"],
                aggregate["review_plan_revision"],
            ),
        )
        if updated.rowcount != 1:
            raise ReviewDueConflict("due schedule changed while retry was committed")
        return _result(
            schedule,
            disposition=ReviewDueDisposition.RETRY_DEFERRED,
            reason=reason,
            retry_at=retry_at,
        )

    def _supersede_stale_schedule(
        self,
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        now: float,
        reason: str,
    ) -> ReviewDueDispatchResult:
        event_id = _review_due_event_id(schedule)
        updated = conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'superseded', claim_owner = '', claim_until = NULL,
                last_error = ?, updated_at = ?
            WHERE plan_id = ? AND profile_id = ? AND session_id = ?
              AND plan_revision = ? AND ownership_generation = ?
              AND status = 'scheduled'
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id =
                          agent_review_schedules.profile_id
                      AND ownership.session_id =
                          agent_review_schedules.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_review_schedules.ownership_generation
              )
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_aggregates AS current_aggregate
                    WHERE current_aggregate.profile_id =
                          agent_review_schedules.profile_id
                      AND current_aggregate.session_id =
                          agent_review_schedules.session_id
                      AND current_aggregate.ownership_generation =
                          agent_review_schedules.ownership_generation
                      AND current_aggregate.state_revision = ?
                      AND current_aggregate.current_plan_id = ?
                      AND current_aggregate.review_plan_revision = ?
              )
            """,
            (
                reason,
                now,
                schedule["plan_id"],
                schedule["profile_id"],
                schedule["session_id"],
                schedule["plan_revision"],
                schedule["ownership_generation"],
                aggregate["state_revision"],
                aggregate["current_plan_id"],
                aggregate["review_plan_revision"],
            ),
        )
        if updated.rowcount != 1:
            raise ReviewDueConflict("stale schedule changed while superseding")
        self._append_schedule_event(
            conn,
            schedule,
            aggregate,
            event_id=event_id,
            event_type="superseded",
            outcome="superseded",
            reason=reason,
            now=now,
        )
        return _result(
            schedule,
            disposition=ReviewDueDisposition.SUPERSEDED,
            event_id=event_id,
            reason=reason,
        )

    def _dispatch_current_schedule(
        self,
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        now: float,
    ) -> ReviewDueDispatchResult:
        event_id = _review_due_event_id(schedule)
        payload_json = _canonical_json(_review_due_payload(schedule, event_id=event_id))
        mailbox_record = _review_due_mailbox_record(
            schedule,
            event_id=event_id,
            payload_json=payload_json,
            now=now,
        )
        try:
            mailbox_exists = self._validate_mailbox_logical_key(
                conn,
                mailbox_record,
                allow_missing=True,
            )
        except ReviewDueConflict:
            return self._defer_schedule(
                conn,
                schedule,
                aggregate,
                now=now,
                reason="mailbox_identity_conflict",
            )
        self._fail_superseded_due_events(
            conn,
            schedule,
            current_event_id=event_id,
            now=now,
        )
        mailbox_inserted = False
        if not mailbox_exists:
            inserted = conn.execute(
                """
                INSERT INTO agent_session_mailbox (
                    event_id, profile_id, session_id, ownership_generation,
                    kind, source, occurred_at, payload_json, causation_id,
                    correlation_id, trace_id, status, attempt_count,
                    available_at, claim_id, lease_owner, lease_until,
                    created_at, handled_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, NULL, '')
                """,
                (
                    _sqlite_record_value(mailbox_record, "event_id"),
                    _sqlite_record_value(mailbox_record, "profile_id"),
                    _sqlite_record_value(mailbox_record, "session_id"),
                    _sqlite_record_value(
                        mailbox_record,
                        "ownership_generation",
                    ),
                    _sqlite_record_value(mailbox_record, "kind"),
                    _sqlite_record_value(mailbox_record, "source"),
                    _sqlite_record_value(mailbox_record, "occurred_at"),
                    _sqlite_record_value(mailbox_record, "payload_json"),
                    _sqlite_record_value(mailbox_record, "causation_id"),
                    _sqlite_record_value(mailbox_record, "correlation_id"),
                    _sqlite_record_value(mailbox_record, "trace_id"),
                    _sqlite_record_value(mailbox_record, "occurred_at"),
                    _sqlite_record_value(mailbox_record, "created_at"),
                ),
            )
            if inserted.rowcount != 1:
                raise ReviewDueConflict(
                    "ReviewDue mailbox insert did not create exactly one row"
                )
            mailbox_inserted = True
        self._validate_mailbox_logical_key(
            conn,
            mailbox_record,
            allow_missing=False,
        )
        # The row stores the next cycle. This transaction emits the old cycle
        # and advances it only if the exact schedule fence is claimed.
        delivery_cycle = _schedule_delivery_cycle(schedule)
        updated = conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'claimed', claim_owner = '', claim_until = NULL,
                delivery_cycle = ?, last_error = '', updated_at = ?
            WHERE plan_id = ? AND profile_id = ? AND session_id = ?
              AND plan_revision = ? AND ownership_generation = ?
              AND delivery_cycle = ?
              AND status = 'scheduled'
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id =
                          agent_review_schedules.profile_id
                      AND ownership.session_id =
                          agent_review_schedules.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                      AND ownership.generation =
                          agent_review_schedules.ownership_generation
              )
              AND EXISTS (
                    SELECT 1
                    FROM agent_session_aggregates AS current_aggregate
                    WHERE current_aggregate.profile_id =
                          agent_review_schedules.profile_id
                      AND current_aggregate.session_id =
                          agent_review_schedules.session_id
                      AND current_aggregate.ownership_generation =
                          agent_review_schedules.ownership_generation
                      AND current_aggregate.state_revision = ?
                      AND current_aggregate.current_plan_id = ?
                      AND current_aggregate.review_plan_revision = ?
              )
            """,
            (
                delivery_cycle + 1,
                now,
                schedule["plan_id"],
                schedule["profile_id"],
                schedule["session_id"],
                schedule["plan_revision"],
                schedule["ownership_generation"],
                delivery_cycle,
                aggregate["state_revision"],
                aggregate["current_plan_id"],
                aggregate["review_plan_revision"],
            ),
        )
        if updated.rowcount != 1:
            raise ReviewDueConflict("current schedule changed while dispatching")
        self._append_schedule_event(
            conn,
            schedule,
            aggregate,
            event_id=event_id,
            event_type="due_dispatched",
            outcome="claimed",
            reason="review_schedule_due",
            now=now,
        )
        return _result(
            schedule,
            disposition=ReviewDueDisposition.DISPATCHED,
            event_id=event_id,
            mailbox_inserted=mailbox_inserted,
            reason="review_schedule_due",
        )

    @staticmethod
    def _fail_superseded_due_events(
        conn: Connection,
        schedule: Row,
        *,
        current_event_id: str,
        now: float,
    ) -> None:
        """Fence generation-stale ReviewDue debt before dispatching its successor."""

        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET status = 'failed', handled_at = ?, claim_id = '',
                lease_owner = '', lease_until = NULL,
                last_error = 'review_due_exact_plan_fence_superseded'
            WHERE profile_id = ? AND session_id = ?
              AND kind = ? AND source = ? AND causation_id = ?
              AND event_id != ? AND status IN ('pending', 'processing')
            """,
            (
                now,
                schedule["profile_id"],
                schedule["session_id"],
                REVIEW_DUE_EVENT_KIND,
                REVIEW_DUE_EVENT_SOURCE,
                schedule["plan_id"],
                current_event_id,
            ),
        )

    @staticmethod
    def _validate_mailbox_logical_key(
        conn: Connection,
        expected: _TypedSQLiteRecord,
        *,
        allow_missing: bool,
    ) -> bool:
        rows = conn.execute(
            """
            SELECT mailbox_id,
                   CAST(event_id AS BLOB) AS event_id,
                   typeof(event_id) AS event_id_storage_class,
                   CAST(profile_id AS BLOB) AS profile_id,
                   typeof(profile_id) AS profile_id_storage_class,
                   CAST(session_id AS BLOB) AS session_id,
                   typeof(session_id) AS session_id_storage_class,
                   ownership_generation,
                   typeof(ownership_generation)
                       AS ownership_generation_storage_class,
                   CAST(kind AS BLOB) AS kind,
                   typeof(kind) AS kind_storage_class,
                   CAST(source AS BLOB) AS source,
                   typeof(source) AS source_storage_class,
                   occurred_at,
                   typeof(occurred_at) AS occurred_at_storage_class,
                   CAST(payload_json AS BLOB) AS payload_json,
                   typeof(payload_json) AS payload_json_storage_class,
                   CAST(causation_id AS BLOB) AS causation_id,
                   typeof(causation_id) AS causation_id_storage_class,
                   CAST(correlation_id AS BLOB) AS correlation_id,
                   typeof(correlation_id) AS correlation_id_storage_class,
                   CAST(trace_id AS BLOB) AS trace_id,
                   typeof(trace_id) AS trace_id_storage_class,
                   created_at, typeof(created_at) AS created_at_storage_class
            FROM agent_session_mailbox
            WHERE CAST(profile_id AS BLOB) = ?
              AND CAST(session_id AS BLOB) = ?
              AND CAST(event_id AS BLOB) = ?
            ORDER BY mailbox_id
            """,
            (
                _sqlite_text_key_bytes(expected, "profile_id"),
                _sqlite_text_key_bytes(expected, "session_id"),
                _sqlite_text_key_bytes(expected, "event_id"),
            ),
        ).fetchall()
        if not rows and allow_missing:
            return False
        if not rows:
            raise ReviewDueConflict("deterministic ReviewDue mailbox row disappeared")
        if len(rows) != 1:
            raise ReviewDueConflict(
                "deterministic ReviewDue logical key contains multiple rows"
            )
        # Delivery status, attempts, claims, leases, handled/error fields, and
        # available_at are mutable after enqueue. Everything selected here is
        # the immutable event envelope and must retain its SQLite representation.
        _validate_exact_sqlite_record(
            rows[0],
            expected,
            conflict_message=(
                "deterministic ReviewDue event id contains conflicting payload"
            ),
        )
        return True

    @staticmethod
    def _append_schedule_event(
        conn: Connection,
        schedule: Row,
        aggregate: Row,
        *,
        event_id: str,
        event_type: str,
        outcome: str,
        reason: str,
        now: float,
    ) -> None:
        profile_id = _canonical_text(
            schedule["profile_id"],
            field_name="schedule.profile_id",
        )
        session_id = _canonical_text(
            schedule["session_id"],
            field_name="schedule.session_id",
        )
        ownership_generation = _canonical_integer(
            schedule["ownership_generation"],
            field_name="schedule.ownership_generation",
        )
        plan_id = _canonical_text(
            schedule["plan_id"],
            field_name="schedule.plan_id",
        )
        plan_revision = _canonical_integer(
            schedule["plan_revision"],
            field_name="schedule.plan_revision",
        )
        trigger = _canonical_text(
            schedule["trigger"],
            field_name="schedule.trigger",
        )
        current_plan_id = _canonical_text(
            aggregate["current_plan_id"],
            field_name="aggregate.current_plan_id",
        )
        current_plan_revision = _canonical_integer(
            aggregate["review_plan_revision"],
            field_name="aggregate.review_plan_revision",
        )
        committed_state_revision = _canonical_integer(
            aggregate["state_revision"],
            field_name="aggregate.state_revision",
        )
        event_id = _canonical_text(event_id, field_name="event_id")
        event_type = _canonical_text(event_type, field_name="event_type")
        outcome = _canonical_text(outcome, field_name="outcome")
        reason = _canonical_text(reason, field_name="reason")
        created_at = _canonical_real(now, field_name="now")
        schedule_event_id = _schedule_event_id(schedule, event_type=event_type)
        metadata_json = _canonical_json(
            {
                "current_plan_id": current_plan_id,
                "current_plan_revision": current_plan_revision,
                "schedule_plan_id": plan_id,
                "schedule_plan_revision": plan_revision,
                "ownership_generation": ownership_generation,
                "delivery_cycle": _schedule_delivery_cycle(schedule),
            }
        )
        expected: _TypedSQLiteRecord = (
            ("schedule_event_id", "text", schedule_event_id),
            ("profile_id", "text", profile_id),
            ("session_id", "text", session_id),
            ("ownership_generation", "integer", ownership_generation),
            ("event_id", "text", event_id),
            ("plan_id", "text", plan_id),
            ("previous_plan_id", "text", plan_id),
            ("event_type", "text", event_type),
            ("trigger", "text", trigger),
            ("outcome", "text", outcome),
            ("source", "text", REVIEW_DUE_EVENT_SOURCE),
            ("requested_delay_seconds", "null", None),
            ("applied_delay_seconds", "null", None),
            ("scheduled_from", "null", None),
            ("next_review_at", "null", None),
            ("reason", "text", reason),
            ("fallback_reason", "text", ""),
            ("model_execution_id", "text", ""),
            ("prompt_signature", "text", ""),
            ("expected_active_epoch", "null", None),
            ("expected_activity_generation", "null", None),
            (
                "committed_state_revision",
                "integer",
                committed_state_revision,
            ),
            ("operation_id", "text", ""),
            ("trace_id", "text", event_id),
            ("metadata_json", "text", metadata_json),
            ("created_at", "real", created_at),
        )
        if _validate_schedule_event_logical_key(
            conn,
            expected,
            allow_missing=True,
        ):
            return
        inserted = conn.execute(
            """
            INSERT INTO agent_review_schedule_events (
                schedule_event_id, profile_id, session_id,
                ownership_generation, event_id, plan_id, previous_plan_id,
                event_type, trigger, outcome, source,
                requested_delay_seconds, applied_delay_seconds, scheduled_from,
                next_review_at, reason, fallback_reason, model_execution_id,
                prompt_signature, expected_active_epoch,
                expected_activity_generation, committed_state_revision,
                operation_id, trace_id, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            _sqlite_record_values(expected),
        )
        if inserted.rowcount != 1:
            raise ReviewDueConflict(
                "review schedule event insert did not create exactly one row"
            )
        _validate_schedule_event_logical_key(
            conn,
            expected,
            allow_missing=False,
        )

    def _retry_delay(self, attempt_count: int) -> float:
        exponent = min(30, max(0, attempt_count - 1))
        return min(
            self._retry_max_seconds,
            self._retry_base_seconds * (2.0**exponent),
        )


class DurableReviewDueScannerService:
    """Supervised bounded loop around durable ReviewDue dispatch and wake debt."""

    def __init__(
        self,
        repository: DurableReviewDueRepository,
        *,
        wake_target: ReviewDueWakeTarget | None = None,
        tick_interval_seconds: float = 5.0,
        batch_limit: int = 50,
        wake_limit: int = 100,
        runtime_id: str | None = None,
    ) -> None:
        """Initialize scanner supervision and bounded pass limits."""

        self._repository = repository
        self._wake_target = wake_target
        self._tick_interval_seconds = _positive_finite(
            tick_interval_seconds,
            field_name="tick_interval_seconds",
        )
        self._batch_limit = _positive_int(batch_limit, field_name="batch_limit")
        self._wake_limit = _positive_int(wake_limit, field_name="wake_limit")
        self._runtime_id = str(
            runtime_id or f"review-due-scanner:{uuid.uuid4().hex}"
        ).strip()
        if not self._runtime_id:
            raise ValueError("runtime_id must not be empty")
        self._task: asyncio.Task[None] | None = None
        self._health = RuntimeServiceHealth("durable_review_due_scanner")
        self._last_summary = ReviewDueScanSummary()

    @property
    def last_summary(self) -> ReviewDueScanSummary:
        """Return the last committed repository pass summary."""

        return self._last_summary

    def health_snapshot(self) -> RuntimeServiceHealthSnapshot:
        """Return current bounded-loop supervision health."""

        return self._health.snapshot()

    def bind_wake_target(self, wake_target: ReviewDueWakeTarget | None) -> None:
        """Replace the optional post-commit actor wake target."""

        self._wake_target = wake_target

    def start(self) -> None:
        """Start the supervised scanner loop when an event loop is running."""

        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("agent.review_due_scanner.start_skipped | no_running_loop")
            return
        self._health.start()
        self._task = loop.create_task(
            self._run_loop(),
            name="agent-durable-review-due-scanner",
        )

    async def shutdown(self) -> None:
        """Stop the scanner without changing any durable schedule or mailbox."""

        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._health.stop()

    async def run_once(self) -> ReviewDueScanSummary:
        """Commit one bounded pass, then best-effort wake durable mailbox debt."""

        self._health.scan_started()
        summary = ReviewDueScanSummary()
        try:
            summary = self._repository.dispatch_due(limit=self._batch_limit)
            self._last_summary = summary
            wake_target = self._wake_target
            if wake_target is not None:
                keys = _unique_keys(
                    (
                        *summary.dispatched_keys,
                        *self._repository.pending_review_due_keys(
                            limit=self._wake_limit
                        ),
                    )
                )
                failures: list[SessionKey] = []
                for key in keys:
                    try:
                        await wake_target.wake(key)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        failures.append(key)
                        logger.exception(
                            format_log_event(
                                "agent.review_due_scanner.wake_failed",
                                profile_id=key.profile_id,
                                session_id=key.session_id,
                            )
                        )
                if failures:
                    raise ReviewDueWakeError(tuple(failures))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._health.failed(exc)
            self._repository.record_service_health(
                self._health.snapshot(),
                summary,
                runtime_id=self._runtime_id,
            )
            raise
        self._health.succeeded()
        self._repository.record_service_health(
            self._health.snapshot(),
            summary,
            runtime_id=self._runtime_id,
        )
        return summary

    async def _run_loop(self) -> None:
        delay = 0.0
        try:
            while True:
                if delay > 0:
                    await asyncio.sleep(delay)
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        format_log_event(
                            "agent.review_due_scanner.iteration_failed",
                            error_code=type(exc).__name__,
                            consecutive_failures=(
                                self._health.snapshot().consecutive_failures
                            ),
                        )
                    )
                    delay = supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=(
                            self._health.snapshot().consecutive_failures
                        ),
                    )
                    continue
                delay = self._tick_interval_seconds
        finally:
            self._health.stop()


def _ownership_unavailable_reason(owner: Row | None) -> str:
    if owner is None:
        return "ownership_missing"
    try:
        status = AgentRuntimeOwnershipStatus(str(owner["status"]))
        mode = AgentRuntimeOwnershipMode(str(owner["mode"]))
    except ValueError:
        return "ownership_invalid"
    if status is AgentRuntimeOwnershipStatus.MIGRATING:
        return "ownership_migrating"
    if status is not AgentRuntimeOwnershipStatus.ACTIVE:
        return "ownership_not_active"
    if mode is not AgentRuntimeOwnershipMode.ACTOR_V2:
        return "ownership_not_actor_v2"
    return ""


def _review_due_mailbox_record(
    schedule: Row,
    *,
    event_id: str,
    payload_json: str,
    now: float,
) -> _TypedSQLiteRecord:
    canonical_event_id = _canonical_text(event_id, field_name="event_id")
    plan_id = _canonical_text(
        schedule["plan_id"],
        field_name="schedule.plan_id",
    )
    return (
        ("event_id", "text", canonical_event_id),
        (
            "profile_id",
            "text",
            _canonical_text(
                schedule["profile_id"],
                field_name="schedule.profile_id",
            ),
        ),
        (
            "session_id",
            "text",
            _canonical_text(
                schedule["session_id"],
                field_name="schedule.session_id",
            ),
        ),
        (
            "ownership_generation",
            "integer",
            _canonical_integer(
                schedule["ownership_generation"],
                field_name="schedule.ownership_generation",
            ),
        ),
        ("kind", "text", REVIEW_DUE_EVENT_KIND),
        ("source", "text", REVIEW_DUE_EVENT_SOURCE),
        (
            "occurred_at",
            "real",
            _canonical_real(
                schedule["next_review_at"],
                field_name="schedule.next_review_at",
            ),
        ),
        (
            "payload_json",
            "text",
            _canonical_text(payload_json, field_name="payload_json"),
        ),
        ("causation_id", "text", plan_id),
        ("correlation_id", "text", plan_id),
        ("trace_id", "text", canonical_event_id),
        ("created_at", "real", _canonical_real(now, field_name="now")),
    )


def _review_due_payload(schedule: Row, *, event_id: str) -> dict[str, object]:
    delivery_cycle = _schedule_delivery_cycle(schedule)
    payload: dict[str, object] = {
        "version": 1 if delivery_cycle == 0 else 2,
        "event_id": event_id,
        "session_key": {
            "profile_id": str(schedule["profile_id"]),
            "session_id": str(schedule["session_id"]),
        },
        "plan_id": str(schedule["plan_id"]),
        "plan_revision": int(schedule["plan_revision"]),
        "ownership_generation": int(schedule["ownership_generation"]),
        "trigger": str(schedule["trigger"]),
        "outcome": str(schedule["outcome"]),
        "reason": str(schedule["reason"]),
        "scheduled_from": float(schedule["scheduled_from"]),
        "next_review_at": float(schedule["next_review_at"]),
        "attempt_count": int(schedule["attempt_count"]),
        "committed_state_revision": int(schedule["committed_state_revision"]),
        "expected_active_epoch": _optional_int(schedule["expected_active_epoch"]),
        "expected_activity_generation": _optional_int(
            schedule["expected_activity_generation"]
        ),
    }
    if delivery_cycle > 0:
        payload["delivery_cycle"] = delivery_cycle
    return payload


def _review_due_event_id(schedule: Mapping[str, object]) -> str:
    return review_due_event_id(
        key=SessionKey(
            str(schedule["profile_id"]),
            str(schedule["session_id"]),
        ),
        plan_id=str(schedule["plan_id"]),
        plan_revision=int(schedule["plan_revision"]),
        ownership_generation=int(schedule["ownership_generation"]),
        delivery_cycle=_schedule_delivery_cycle(schedule),
    )


def _schedule_event_id(
    schedule: Mapping[str, object],
    *,
    event_type: str,
) -> str:
    identity_parts: list[object] = [
        str(schedule["profile_id"]),
        str(schedule["session_id"]),
        str(schedule["plan_id"]),
        int(schedule["plan_revision"]),
        int(schedule["ownership_generation"]),
        event_type,
    ]
    delivery_cycle = _schedule_delivery_cycle(schedule)
    if delivery_cycle > 0:
        identity_parts.append(delivery_cycle)
    identity = _canonical_json(identity_parts)
    digest = uuid.uuid5(_SCHEDULE_EVENT_NAMESPACE, identity).hex
    return f"schedule-event:{event_type}:{digest}"


def _result(
    schedule: Row,
    *,
    disposition: ReviewDueDisposition,
    event_id: str = "",
    mailbox_inserted: bool = False,
    reason: str = "",
    retry_at: float | None = None,
) -> ReviewDueDispatchResult:
    return ReviewDueDispatchResult(
        key=SessionKey(
            str(schedule["profile_id"]),
            str(schedule["session_id"]),
        ),
        plan_id=str(schedule["plan_id"]),
        plan_revision=int(schedule["plan_revision"]),
        ownership_generation=int(schedule["ownership_generation"]),
        delivery_cycle=_schedule_delivery_cycle(schedule),
        disposition=disposition,
        event_id=event_id,
        mailbox_inserted=mailbox_inserted,
        reason=reason,
        retry_at=retry_at,
    )


def _due_scan_cursor(schedule: Mapping[str, object]) -> _DueScanCursor:
    return (
        float(schedule["available_at"]),
        float(schedule["next_review_at"]),
        str(schedule["profile_id"]),
        str(schedule["session_id"]),
        int(schedule["plan_revision"]),
        str(schedule["plan_id"]),
    )


def _unique_keys(keys: Iterable[SessionKey]) -> tuple[SessionKey, ...]:
    result: list[SessionKey] = []
    seen: set[SessionKey] = set()
    for key in keys:
        if not isinstance(key, SessionKey):
            raise TypeError("wake debt must contain SessionKey values")
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
    return tuple(result)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _canonical_text(value: object, *, field_name: str) -> str:
    if type(value) is not str:
        raise ReviewDueConflict(f"{field_name} must use SQLite TEXT storage")
    return value


def _canonical_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int:
        raise ReviewDueConflict(f"{field_name} must use SQLite INTEGER storage")
    return value


def _canonical_real(value: object, *, field_name: str) -> float:
    if type(value) is not float or not math.isfinite(value):
        raise ReviewDueConflict(f"{field_name} must use finite SQLite REAL storage")
    return value


def _sqlite_record_values(record: _TypedSQLiteRecord) -> tuple[object, ...]:
    return tuple(value for _field, _storage_class, value in record)


def _sqlite_record_value(record: _TypedSQLiteRecord, field_name: str) -> object:
    for field, _storage_class, value in record:
        if field == field_name:
            return value
    raise ReviewDueConflict(f"expected SQLite record is missing {field_name}")


def _sqlite_text_key_bytes(
    record: _TypedSQLiteRecord,
    field_name: str,
) -> bytes:
    value = _canonical_text(
        _sqlite_record_value(record, field_name),
        field_name=field_name,
    )
    return value.encode("utf-8", errors="strict")


def _validate_schedule_event_logical_key(
    conn: Connection,
    expected: _TypedSQLiteRecord,
    *,
    allow_missing: bool,
) -> bool:
    rows = conn.execute(
        """
        SELECT schedule_event_seq,
               CAST(schedule_event_id AS BLOB) AS schedule_event_id,
               typeof(schedule_event_id)
                   AS schedule_event_id_storage_class,
               CAST(profile_id AS BLOB) AS profile_id,
               typeof(profile_id) AS profile_id_storage_class,
               CAST(session_id AS BLOB) AS session_id,
               typeof(session_id) AS session_id_storage_class,
               ownership_generation,
               typeof(ownership_generation)
                   AS ownership_generation_storage_class,
               CAST(event_id AS BLOB) AS event_id,
               typeof(event_id) AS event_id_storage_class,
               CAST(plan_id AS BLOB) AS plan_id,
               typeof(plan_id) AS plan_id_storage_class,
               CAST(previous_plan_id AS BLOB) AS previous_plan_id,
               typeof(previous_plan_id)
                   AS previous_plan_id_storage_class,
               CAST(event_type AS BLOB) AS event_type,
               typeof(event_type) AS event_type_storage_class,
               CAST(trigger AS BLOB) AS trigger,
               typeof(trigger) AS trigger_storage_class,
               CAST(outcome AS BLOB) AS outcome,
               typeof(outcome) AS outcome_storage_class,
               CAST(source AS BLOB) AS source,
               typeof(source) AS source_storage_class,
               requested_delay_seconds,
               typeof(requested_delay_seconds)
                   AS requested_delay_seconds_storage_class,
               applied_delay_seconds,
               typeof(applied_delay_seconds)
                   AS applied_delay_seconds_storage_class,
               scheduled_from,
               typeof(scheduled_from) AS scheduled_from_storage_class,
               next_review_at,
               typeof(next_review_at) AS next_review_at_storage_class,
               CAST(reason AS BLOB) AS reason,
               typeof(reason) AS reason_storage_class,
               CAST(fallback_reason AS BLOB) AS fallback_reason,
               typeof(fallback_reason) AS fallback_reason_storage_class,
               CAST(model_execution_id AS BLOB) AS model_execution_id,
               typeof(model_execution_id)
                   AS model_execution_id_storage_class,
               CAST(prompt_signature AS BLOB) AS prompt_signature,
               typeof(prompt_signature)
                   AS prompt_signature_storage_class,
               expected_active_epoch,
               typeof(expected_active_epoch)
                   AS expected_active_epoch_storage_class,
               expected_activity_generation,
               typeof(expected_activity_generation)
                   AS expected_activity_generation_storage_class,
               committed_state_revision,
               typeof(committed_state_revision)
                   AS committed_state_revision_storage_class,
               CAST(operation_id AS BLOB) AS operation_id,
               typeof(operation_id) AS operation_id_storage_class,
               CAST(trace_id AS BLOB) AS trace_id,
               typeof(trace_id) AS trace_id_storage_class,
               CAST(metadata_json AS BLOB) AS metadata_json,
               typeof(metadata_json) AS metadata_json_storage_class,
               created_at, typeof(created_at) AS created_at_storage_class
        FROM agent_review_schedule_events
        WHERE CAST(schedule_event_id AS BLOB) = ?
        ORDER BY schedule_event_seq
        """,
        (_sqlite_text_key_bytes(expected, "schedule_event_id"),),
    ).fetchall()
    if not rows and allow_missing:
        return False
    if not rows:
        raise ReviewDueConflict("deterministic schedule event disappeared")
    if len(rows) != 1:
        raise ReviewDueConflict(
            "deterministic schedule event logical key contains multiple rows"
        )
    # A committed row can only be an exact replay when every value,
    # including created_at, came from the same deterministic attempt. A prior
    # failed transaction cannot leave this row behind in SQLite.
    _validate_exact_sqlite_record(
        rows[0],
        expected,
        conflict_message=(
            "deterministic review schedule event contains conflicting payload"
        ),
    )
    return True


def _validate_exact_sqlite_record(
    row: Row,
    expected: _TypedSQLiteRecord,
    *,
    conflict_message: str,
) -> None:
    for field, storage_class, expected_value in expected:
        actual_storage_class = row[f"{field}_storage_class"]
        if actual_storage_class != storage_class:
            raise ReviewDueConflict(f"{conflict_message}: {field}")
        actual_value = row[field]
        if storage_class == "text":
            if type(actual_value) is not bytes:
                raise ReviewDueConflict(f"{conflict_message}: {field}")
            try:
                actual_value = actual_value.decode("utf-8", errors="strict")
            except UnicodeDecodeError as exc:
                raise ReviewDueConflict(f"{conflict_message}: {field}") from exc
        if actual_value != expected_value:
            raise ReviewDueConflict(f"{conflict_message}: {field}")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReviewDueConflict("schedule integer fence is invalid")
    return value


def _schedule_delivery_cycle(schedule: Mapping[str, object]) -> int:
    value = schedule["delivery_cycle"]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReviewDueConflict("schedule delivery_cycle fence is invalid")
    return value


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _positive_finite(value: object, *, field_name: str) -> float:
    numeric = _nonnegative_finite(value, field_name=field_name)
    if numeric <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return numeric


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and non-negative")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be finite and non-negative") from exc
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return numeric


__all__ = [
    "REVIEW_DUE_EVENT_KIND",
    "REVIEW_DUE_EVENT_SOURCE",
    "GLOBAL_REVIEW_DUE_HEALTH_PROFILE_ID",
    "DurableReviewDueRepository",
    "DurableReviewDueScannerService",
    "ReviewDueConflict",
    "ReviewDueDispatchResult",
    "ReviewDueDisposition",
    "ReviewDueRepositoryError",
    "ReviewDueScanSummary",
    "ReviewDueWakeError",
    "ReviewDueWakeTarget",
    "review_due_event_id",
]
