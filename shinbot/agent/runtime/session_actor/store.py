"""SQLite persistence for durable profile-scoped Agent session actors."""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    EventEnqueueResult,
    MailboxEventStatus,
    ReviewScheduleStatus,
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionOperationStatus,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.external_actions import ExternalActionKind
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    MessageLedgerEntry,
    MessageLedgerProjectionKind,
    MessageLedgerRangeProjection,
)
from shinbot.agent.runtime.session_actor.message_ledger_persistence import (
    MessageLedgerConflict,
    apply_message_ledger_appends,
    apply_message_ledger_consumptions,
    count_message_ledger_entries,
    load_captured_unread_message_ledger_entries,
    load_message_ledger_entries,
    load_message_ledger_ranges,
)

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


_EXTERNAL_ACTION_EFFECT_KINDS = frozenset(
    action_kind.value for action_kind in ExternalActionKind
)


class SessionStoreError(RuntimeError):
    """Base error raised by the durable session store."""


class SessionAggregateNotFound(SessionStoreError):
    """Raised when a requested session aggregate does not exist."""


class AggregateVersionConflict(SessionStoreError):
    """Raised when an aggregate compare-and-swap precondition is stale."""


class MailboxEventConflict(SessionStoreError):
    """Raised when an event id is reused for a different actor or payload."""


class MailboxLeaseConflict(SessionStoreError):
    """Raised when a worker no longer owns the mailbox event it is completing."""


class DurableRecordConflict(SessionStoreError):
    """Raised when a durable operation or schedule id changes ownership."""


@dataclass(slots=True, frozen=True)
class _OperationInputFence:
    """Store-resolved input boundary for one workflow operation."""

    input_watermark: int
    input_ledger_sequence: int
    requires_pending_mapping: bool = False


class SQLiteSessionActorStore:
    """Durable mailbox and atomic commit boundary for Agent session actors.

    SQLite calls are synchronous, but every transaction is deliberately short
    and contains no model, adapter, network, or tool I/O. The async surface
    matches the actor protocol and permits a different persistence adapter
    later without changing actor code.
    """

    def __init__(
        self,
        database: DatabaseManager,
        *,
        lease_seconds: float = 30.0,
        retry_delay_seconds: float = 1.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Initialize the store.

        Args:
            database: Initialized ShinBot database manager.
            lease_seconds: Mailbox claim duration before recovery is allowed.
            retry_delay_seconds: Delay applied when a claimed event is released.
            clock: Injectable wall clock used by tests and persistence records.
        """

        normalized_lease_seconds = float(lease_seconds)
        if not math.isfinite(normalized_lease_seconds) or normalized_lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        normalized_retry_delay_seconds = float(retry_delay_seconds)
        if (
            not math.isfinite(normalized_retry_delay_seconds)
            or normalized_retry_delay_seconds < 0
        ):
            raise ValueError("retry_delay_seconds must be finite and non-negative")
        self._database = database
        self._lease_seconds = normalized_lease_seconds
        self._retry_delay_seconds = normalized_retry_delay_seconds
        self._clock = clock or time.time

    async def ensure(
        self,
        key: SessionKey,
        *,
        ownership_generation: int | None = None,
    ) -> AgentSessionAggregate:
        """Create the aggregate if needed and return its current snapshot."""

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership = (
                self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                    conn,
                    key,
                    expected_generation=ownership_generation,
                )
            )
            now = self._now()
            self._ensure_with_connection(
                conn,
                key,
                ownership_generation=ownership.generation,
                now=now,
            )
            row = self._load_row(conn, key)
        assert row is not None
        return _aggregate_from_row(row)

    async def load(self, key: SessionKey) -> AgentSessionAggregate:
        """Load one aggregate or raise when the actor has not been created."""

        with self._database.connect() as conn:
            row = self._load_row(conn, key)
        if row is None:
            raise SessionAggregateNotFound(
                f"Agent session aggregate does not exist: {key.profile_id}:{key.session_id}"
            )
        return _aggregate_from_row(row)

    async def list_message_ledger(
        self,
        key: SessionKey,
        *,
        projection: MessageLedgerProjectionKind | None = None,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Read one profile-scoped message projection in ledger order."""

        with self._database.connect() as conn:
            return load_message_ledger_entries(
                conn,
                key,
                projection=projection,
            )

    async def list_unread_messages(
        self,
        key: SessionKey,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Return messages consumed by neither review nor active chat."""

        return await self.list_message_ledger(
            key,
            projection=MessageLedgerProjectionKind.UNREAD,
        )

    async def list_captured_unread(
        self,
        *,
        key: SessionKey,
        input_watermark: int,
        input_ledger_sequence: int,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Return unread messages owned by one operation's frozen boundary.

        This is the concrete port consumed by actor workflow adapters. It does
        not mutate or consume ledger rows; the reducer remains responsible for
        accepting a completion and applying the operation-scoped consumption.
        """

        with self._database.connect() as conn:
            return load_captured_unread_message_ledger_entries(
                conn,
                key,
                input_watermark=input_watermark,
                input_ledger_sequence=input_ledger_sequence,
            )

    async def count_unread_messages(self, key: SessionKey) -> int:
        """Count unread messages without maintaining a second count model."""

        with self._database.connect() as conn:
            return count_message_ledger_entries(
                conn,
                key,
                projection=MessageLedgerProjectionKind.UNREAD,
            )

    async def list_unread_ranges(
        self,
        key: SessionKey,
    ) -> tuple[MessageLedgerRangeProjection, ...]:
        """Derive unread ranges from the complete per-message ledger."""

        with self._database.connect() as conn:
            return load_message_ledger_ranges(
                conn,
                key,
                projection=MessageLedgerProjectionKind.UNREAD,
            )

    async def enqueue(self, envelope: SessionEventEnvelope) -> EventEnqueueResult:
        """Persist one mailbox event idempotently.

        Reusing an event id within one actor for different event identity is
        rejected instead of silently treating unrelated work as a duplicate.
        """

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                envelope.ownership_generation
            )
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                envelope.key,
                expected_generation=ownership_generation,
            )
            now = self._now()
            self._ensure_with_connection(
                conn,
                envelope.key,
                ownership_generation=ownership_generation,
                now=now,
            )
            return self._enqueue_with_connection(conn, envelope, now=now)

    async def claim_next(
        self,
        key: SessionKey,
        *,
        worker_id: str,
    ) -> ClaimedSessionEvent | None:
        """Claim the oldest available event for one actor using a lease."""

        normalized_worker_id = str(worker_id or "").strip()
        if not normalized_worker_id:
            raise ValueError("worker_id must not be empty")
        claim_id = uuid.uuid4().hex
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now()
            lease_until = _nonnegative_finite(
                now + self._lease_seconds,
                field_name="lease_until",
            )
            row = conn.execute(
                """
                SELECT mailbox.*
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                WHERE mailbox.profile_id = ?
                  AND mailbox.session_id = ?
                  AND mailbox.ownership_generation >= 1
                  AND mailbox.status IN ('pending', 'processing')
                ORDER BY mailbox.mailbox_id ASC
                LIMIT 1
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            if row is None:
                return None
            ownership_generation = int(row["ownership_generation"])
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                key,
                expected_generation=ownership_generation,
            )
            if float(row["available_at"] or 0.0) > now:
                return None
            if (
                str(row["status"]) == MailboxEventStatus.PROCESSING.value
                and row["lease_until"] is not None
                and float(row["lease_until"]) > now
            ):
                return None
            updated = conn.execute(
                """
                UPDATE agent_session_mailbox
                SET status = 'processing',
                    attempt_count = attempt_count + 1,
                    claim_id = ?,
                    lease_owner = ?,
                    lease_until = ?,
                    last_error = ''
                WHERE mailbox_id = ?
                  AND ownership_generation = ?
                  AND (
                      status = 'pending'
                      OR (status = 'processing' AND COALESCE(lease_until, 0) <= ?)
                  )
                """,
                (
                    claim_id,
                    normalized_worker_id,
                    lease_until,
                    row["mailbox_id"],
                    ownership_generation,
                    now,
                ),
            )
            if updated.rowcount != 1:
                return None
            claimed_row = conn.execute(
                "SELECT * FROM agent_session_mailbox WHERE mailbox_id = ?",
                (row["mailbox_id"],),
            ).fetchone()
        assert claimed_row is not None
        return ClaimedSessionEvent(
            claim_id=claim_id,
            envelope=_envelope_from_row(claimed_row),
            worker_id=normalized_worker_id,
            attempt_count=int(claimed_row["attempt_count"]),
            claimed_at=now,
            lease_expires_at=lease_until,
        )

    async def commit(
        self,
        claim: ClaimedSessionEvent,
        transition: SessionTransition,
        *,
        expected_revision: int,
    ) -> AgentSessionAggregate:
        """Atomically commit an event transition, journals, and durable effects.

        Operation, review-schedule, schedule-journal, and outbox records carried
        by the transition are part of the same transaction.
        """

        target = transition.aggregate
        if target.key != claim.key:
            raise ValueError("transition aggregate key does not match mailbox claim")
        if expected_revision < 0:
            raise ValueError("expected_revision must not be negative")
        self._validate_message_ledger_transition(claim, transition)

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now()
            ownership_generation = _persistable_ownership_generation(
                claim.envelope.ownership_generation
            )
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            mailbox_row = conn.execute(
                """
                SELECT * FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ? AND event_id = ?
                """,
                (
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.envelope.event_id,
                ),
            ).fetchone()
            if mailbox_row is None:
                raise MailboxLeaseConflict("mailbox event no longer exists")
            self._validate_mailbox_identity(mailbox_row, claim)
            if str(mailbox_row["status"]) == MailboxEventStatus.COMPLETED.value:
                current = self._load_row(conn, claim.key)
                if current is None:
                    raise SessionAggregateNotFound(claim.key.session_id)
                return _aggregate_from_row(current)
            self._validate_claim_lease(mailbox_row, claim, now=now)

            current_row = self._load_row(conn, claim.key)
            if current_row is None:
                raise SessionAggregateNotFound(claim.key.session_id)
            current = _aggregate_from_row(current_row)
            if (
                current.ownership_generation != ownership_generation
                or target.ownership_generation != ownership_generation
            ):
                raise AggregateVersionConflict(
                    "aggregate ownership generation does not match mailbox claim"
                )
            schedule_timings = self._resolve_schedule_timings(
                conn,
                transition,
                now=now,
            )
            effect_timings = _resolve_effect_timings(transition, now=now)
            target = _apply_review_schedule_clock(target, schedule_timings)
            target = _apply_effect_commit_clock(target, effect_timings)
            input_ledger_sequence = apply_message_ledger_appends(
                conn,
                key=claim.key,
                ownership_generation=ownership_generation,
                source_event_id=claim.envelope.event_id,
                mutations=transition.message_ledger_mutations,
                committed_at=now,
            )
            operation_fences = self._resolve_operation_input_fences(
                conn,
                claim,
                transition.operations,
                input_ledger_sequence=input_ledger_sequence,
            )
            target = _stamp_pending_operation_input_fences(
                target,
                operation_fences,
            )
            effects = _stamp_effect_input_fences(
                transition.effects,
                operation_fences,
            )
            target = replace(target, updated_at=max(target.updated_at, now))
            self._validate_aggregate_transition(
                current,
                target,
                expected_revision=expected_revision,
            )
            updated = conn.execute(
                """
                UPDATE agent_session_aggregates
                SET state = ?,
                    state_revision = ?,
                    event_sequence = ?,
                    activity_generation = ?,
                    active_epoch = ?,
                    review_plan_json = ?,
                    current_plan_id = ?,
                    review_plan_revision = ?,
                    active_reply_resume_json = ?,
                    active_chat_state_json = ?,
                    review_operation_id = ?,
                    active_reply_operation_id = ?,
                    active_chat_round_operation_id = ?,
                    idle_planning_operation_id = ?,
                    data_json = ?,
                    updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND ownership_generation = ?
                  AND state_revision = ?
                  AND event_sequence = ?
                """,
                (
                    target.state,
                    target.state_revision,
                    target.event_sequence,
                    target.activity_generation,
                    target.active_epoch,
                    _json_dumps(target.review_plan),
                    target.current_plan_id,
                    target.review_plan_revision,
                    _json_dumps(target.active_reply_resume),
                    _json_dumps(target.active_chat_state),
                    target.review_operation_id,
                    target.active_reply_operation_id,
                    target.active_chat_round_operation_id,
                    target.idle_planning_operation_id,
                    _json_dumps(target.data),
                    target.updated_at,
                    claim.key.profile_id,
                    claim.key.session_id,
                    ownership_generation,
                    expected_revision,
                    current.event_sequence,
                ),
            )
            if updated.rowcount != 1:
                raise AggregateVersionConflict(
                    f"stale aggregate revision {expected_revision} for {claim.key}"
                )

            for operation in transition.operations:
                operation_record = _apply_operation_commit_clock(
                    operation.to_record(),
                    target,
                )
                operation_record = _stamp_operation_input_fence(
                    operation_record,
                    operation_fences.get(operation.operation_id),
                )
                self._upsert_operation(
                    conn,
                    claim,
                    target,
                    operation_record,
                    now=now,
                )
            apply_message_ledger_consumptions(
                conn,
                key=claim.key,
                ownership_generation=ownership_generation,
                source_event_id=claim.envelope.event_id,
                mutations=transition.message_ledger_mutations,
                committed_at=now,
            )
            for schedule in transition.review_schedules:
                timing = schedule_timings[str(schedule.plan_id).strip()]
                self._upsert_review_schedule(
                    conn,
                    claim,
                    current,
                    target,
                    schedule.to_record(),
                    scheduled_from=timing[0],
                    next_review_at=timing[1],
                    now=now,
                )
            for schedule_event in transition.review_schedule_events:
                timing = schedule_timings.get(str(schedule_event.plan_id).strip())
                self._append_review_schedule_event(
                    conn,
                    claim,
                    target,
                    schedule_event.to_record(),
                    schedule_timing=timing,
                    now=now,
                )
            self._append_transition(
                conn,
                claim,
                current=current,
                target=target,
                transition=transition,
                now=now,
            )
            for effect in effects:
                self._append_effect(
                    conn,
                    claim,
                    effect,
                    timing=effect_timings[effect.effect_id],
                    now=now,
                )

            completed = conn.execute(
                """
                UPDATE agent_session_mailbox
                SET status = 'completed',
                    handled_at = ?,
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    last_error = ''
                WHERE event_id = ?
                  AND profile_id = ?
                  AND session_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                """,
                (
                    now,
                    claim.envelope.event_id,
                    claim.key.profile_id,
                    claim.key.session_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if completed.rowcount != 1:
                raise MailboxLeaseConflict("mailbox lease changed during commit")
        return target

    @staticmethod
    def _validate_message_ledger_transition(
        claim: ClaimedSessionEvent,
        transition: SessionTransition,
    ) -> None:
        """Require exactly one append for MessageReceived and nowhere else."""

        append_count = sum(
            isinstance(mutation, AppendMessageLedgerEntry)
            for mutation in transition.message_ledger_mutations
        )
        if claim.envelope.kind == "MessageReceived":
            if append_count != 1:
                raise MessageLedgerConflict(
                    "MessageReceived must commit exactly one message ledger append"
                )
            return
        if append_count:
            raise MessageLedgerConflict(
                "only MessageReceived may append a message ledger entry"
            )

    async def release(self, claim: ClaimedSessionEvent, *, error: str) -> None:
        """Release a claimed event for retry after a bounded delay."""

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.envelope.ownership_generation
            )
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            now = self._now()
            released = conn.execute(
                """
                UPDATE agent_session_mailbox
                SET status = 'pending',
                    available_at = ?,
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    last_error = ?
                WHERE event_id = ?
                  AND profile_id = ?
                  AND session_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                """,
                (
                    _nonnegative_finite(
                        now + self._retry_delay_seconds,
                        field_name="available_at",
                    ),
                    str(error or ""),
                    claim.envelope.event_id,
                    claim.key.profile_id,
                    claim.key.session_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if released.rowcount != 1:
                row = conn.execute(
                    """
                    SELECT status FROM agent_session_mailbox
                    WHERE profile_id = ? AND session_id = ? AND event_id = ?
                    """,
                    (
                        claim.key.profile_id,
                        claim.key.session_id,
                        claim.envelope.event_id,
                    ),
                ).fetchone()
                if row is None or str(row["status"]) != MailboxEventStatus.COMPLETED.value:
                    raise MailboxLeaseConflict("mailbox event is not owned by this claim")

    async def fail(self, claim: ClaimedSessionEvent, *, error: str) -> None:
        """Atomically dead-letter one event and advance its causal sequence."""

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.envelope.ownership_generation
            )
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            now = self._now()
            mailbox_row = conn.execute(
                """
                SELECT * FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ? AND event_id = ?
                """,
                (
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.envelope.event_id,
                ),
            ).fetchone()
            if mailbox_row is None:
                raise MailboxLeaseConflict("mailbox event no longer exists")
            self._validate_mailbox_identity(mailbox_row, claim)
            self._validate_claim_lease(mailbox_row, claim, now=now)
            current_row = self._load_row(conn, claim.key)
            if current_row is None:
                raise SessionAggregateNotFound(claim.key.session_id)
            current = _aggregate_from_row(current_row)
            if current.ownership_generation != ownership_generation:
                raise AggregateVersionConflict(
                    "aggregate ownership generation does not match mailbox claim"
                )
            next_event_sequence = current.event_sequence + 1
            advanced = conn.execute(
                """
                UPDATE agent_session_aggregates
                SET event_sequence = ?, updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND ownership_generation = ?
                  AND state_revision = ?
                  AND event_sequence = ?
                """,
                (
                    next_event_sequence,
                    now,
                    claim.key.profile_id,
                    claim.key.session_id,
                    ownership_generation,
                    current.state_revision,
                    current.event_sequence,
                ),
            )
            if advanced.rowcount != 1:
                raise AggregateVersionConflict(
                    f"stale aggregate while failing event for {claim.key}"
                )
            self._append_failed_transition(
                conn,
                claim,
                current=current,
                next_event_sequence=next_event_sequence,
                error=str(error or ""),
                now=now,
            )
            failed = conn.execute(
                """
                UPDATE agent_session_mailbox
                SET status = 'failed',
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    last_error = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND event_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                """,
                (
                    str(error or ""),
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.envelope.event_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if failed.rowcount != 1:
                raise MailboxLeaseConflict("mailbox event is not owned by this claim")

    async def recover(self, key: SessionKey, *, worker_id: str) -> int:
        """Release stale mailbox leases left by a previous actor worker."""

        normalized_worker_id = str(worker_id or "").strip()
        if not normalized_worker_id:
            raise ValueError("worker_id must not be empty")
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            aggregate_row = self._load_row(conn, key)
            if aggregate_row is None:
                raise SessionAggregateNotFound(key.session_id)
            ownership_generation = _persistable_ownership_generation(
                aggregate_row["ownership_generation"]
            )
            self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                key,
                expected_generation=ownership_generation,
            )
            now = self._now()
            recovered = conn.execute(
                """
                UPDATE agent_session_mailbox
                SET status = 'pending',
                    available_at = MIN(available_at, ?),
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    last_error = CASE
                        WHEN last_error = '' THEN 'mailbox_lease_recovered'
                        ELSE last_error
                    END
                WHERE profile_id = ?
                  AND session_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND COALESCE(lease_until, 0) <= ?
                """,
                (
                    now,
                    key.profile_id,
                    key.session_id,
                    ownership_generation,
                    now,
                ),
            )
            return int(recovered.rowcount)

    async def enqueue_recovery_requests(self) -> int:
        """Enqueue fenced recovery events for orphaned non-idle aggregates.

        Discovery and insertion share one immediate transaction. An aggregate
        is orphaned only when it has neither mailbox work nor a pending effect
        tied to one of its authoritative operation ids. The deterministic event
        identity makes concurrent and repeated startup scans idempotent.

        Returns:
            The number of newly inserted recovery events.
        """

        inserted_count = 0
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now()
            rows = conn.execute(
                """
                SELECT aggregate.*
                FROM agent_session_aggregates AS aggregate
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = aggregate.profile_id
                 AND ownership.session_id = aggregate.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = aggregate.ownership_generation
                WHERE aggregate.state != 'idle'
                  AND aggregate.ownership_generation >= 1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM agent_session_mailbox AS mailbox
                      WHERE mailbox.profile_id = aggregate.profile_id
                        AND mailbox.session_id = aggregate.session_id
                        AND mailbox.status IN ('pending', 'processing')
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM agent_effect_outbox AS effect
                      WHERE effect.profile_id = aggregate.profile_id
                        AND effect.session_id = aggregate.session_id
                        AND effect.status IN ('pending', 'processing')
                        AND effect.operation_id != ''
                        AND effect.operation_id IN (
                            aggregate.review_operation_id,
                            aggregate.active_reply_operation_id,
                            aggregate.active_chat_round_operation_id,
                            aggregate.idle_planning_operation_id
                        )
                  )
                ORDER BY aggregate.profile_id ASC, aggregate.session_id ASC
                """
            ).fetchall()
            for row in rows:
                aggregate = _aggregate_from_row(row)
                self._database.agent_runtime_ownership.require_actor_v2_in_transaction(
                    conn,
                    aggregate.key,
                    expected_generation=aggregate.ownership_generation,
                )
                result = self._enqueue_with_connection(
                    conn,
                    _recovery_event(aggregate, now=now),
                    now=now,
                )
                inserted_count += int(result.inserted)
        return inserted_count

    async def pending_keys(self) -> list[SessionKey]:
        """Return actor keys with pending or recoverable mailbox work."""

        with self._database.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT mailbox.profile_id, mailbox.session_id
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                WHERE mailbox.ownership_generation >= 1
                  AND mailbox.status IN ('pending', 'processing')
                ORDER BY mailbox.profile_id ASC, mailbox.session_id ASC
                """
            ).fetchall()
        return [SessionKey(str(row["profile_id"]), str(row["session_id"])) for row in rows]

    async def next_available_at(self, key: SessionKey) -> float | None:
        """Return the earliest time at which the actor can claim its head event."""

        with self._database.connect() as conn:
            row = conn.execute(
                """
                SELECT mailbox.status, mailbox.available_at, mailbox.lease_until
                FROM agent_session_mailbox AS mailbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = mailbox.profile_id
                 AND ownership.session_id = mailbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = mailbox.ownership_generation
                WHERE mailbox.profile_id = ?
                  AND mailbox.session_id = ?
                  AND mailbox.ownership_generation >= 1
                  AND mailbox.status IN ('pending', 'processing')
                ORDER BY mailbox.mailbox_id ASC
                LIMIT 1
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
        if row is None:
            return None
        if str(row["status"]) == MailboxEventStatus.PROCESSING.value:
            return _optional_float(row["lease_until"])
        return float(row["available_at"])

    def _now(self) -> float:
        """Return a validated persistence clock value."""

        return _nonnegative_finite(self._clock(), field_name="clock")

    def _enqueue_with_connection(
        self,
        conn: sqlite3.Connection,
        envelope: SessionEventEnvelope,
        *,
        now: float,
    ) -> EventEnqueueResult:
        """Insert one envelope using an existing write transaction."""

        occurred_at = envelope.occurred_at or now
        available_at = envelope.available_at or now
        created_at = envelope.created_at or now
        payload_json = _json_dumps(envelope.payload)
        inserted = conn.execute(
            """
            INSERT OR IGNORE INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at,
                payload_json, causation_id, correlation_id, trace_id,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, handled_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, NULL, '')
            """,
            (
                envelope.event_id,
                envelope.key.profile_id,
                envelope.key.session_id,
                _persistable_ownership_generation(envelope.ownership_generation),
                envelope.kind,
                envelope.source,
                occurred_at,
                payload_json,
                envelope.causation_id,
                envelope.correlation_id,
                envelope.trace_id,
                available_at,
                created_at,
            ),
        )
        row = conn.execute(
            """
            SELECT profile_id, session_id, kind, source, payload_json,
                   ownership_generation, causation_id, correlation_id,
                   trace_id, status
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (
                envelope.key.profile_id,
                envelope.key.session_id,
                envelope.event_id,
            ),
        ).fetchone()
        assert row is not None
        if inserted.rowcount != 1:
            self._validate_duplicate_event(row, envelope, payload_json)
        return EventEnqueueResult(
            event_id=envelope.event_id,
            key=envelope.key,
            inserted=inserted.rowcount == 1,
            status=_mailbox_status(str(row["status"])),
        )

    def _ensure_with_connection(
        self,
        conn: sqlite3.Connection,
        key: SessionKey,
        *,
        ownership_generation: int,
        now: float,
    ) -> None:
        normalized_generation = _persistable_ownership_generation(
            ownership_generation
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, state,
                state_revision, event_sequence,
                activity_generation, active_epoch, review_plan_json,
                current_plan_id, review_plan_revision,
                active_reply_resume_json, active_chat_state_json,
                review_operation_id, active_reply_operation_id,
                active_chat_round_operation_id, idle_planning_operation_id,
                data_json, created_at, updated_at
            ) VALUES (?, ?, ?, 'idle', 0, 0, 0, 0, '{}', '', 0, '{}', '{}', '', '', '', '', '{}', ?, ?)
            """,
            (key.profile_id, key.session_id, normalized_generation, now, now),
        )
        row = SQLiteSessionActorStore._load_row(conn, key)
        assert row is not None
        if int(row["ownership_generation"]) != normalized_generation:
            raise AggregateVersionConflict(
                "aggregate belongs to a different ownership generation"
            )

    @staticmethod
    def _load_row(conn: sqlite3.Connection, key: SessionKey) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT *
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()

    @staticmethod
    def _validate_duplicate_event(
        row: sqlite3.Row,
        envelope: SessionEventEnvelope,
        payload_json: str,
    ) -> None:
        identity = (
            str(row["profile_id"]),
            str(row["session_id"]),
            str(row["kind"]),
            str(row["source"]),
            str(row["payload_json"]),
            int(row["ownership_generation"]),
            str(row["causation_id"]),
            str(row["correlation_id"]),
            str(row["trace_id"]),
        )
        requested = (
            envelope.key.profile_id,
            envelope.key.session_id,
            envelope.kind,
            envelope.source,
            payload_json,
            envelope.ownership_generation,
            envelope.causation_id,
            envelope.correlation_id,
            envelope.trace_id,
        )
        if identity != requested:
            raise MailboxEventConflict(
                f"event id {envelope.event_id!r} is already used by different work"
            )

    @staticmethod
    def _validate_mailbox_identity(
        row: sqlite3.Row,
        claim: ClaimedSessionEvent,
    ) -> None:
        if (
            str(row["profile_id"]) != claim.key.profile_id
            or str(row["session_id"]) != claim.key.session_id
            or str(row["kind"]) != claim.envelope.kind
            or int(row["ownership_generation"])
            != claim.envelope.ownership_generation
        ):
            raise MailboxEventConflict("mailbox claim does not match its persisted event")

    @staticmethod
    def _validate_claim_lease(
        row: sqlite3.Row,
        claim: ClaimedSessionEvent,
        *,
        now: float,
    ) -> None:
        if (
            str(row["status"]) != MailboxEventStatus.PROCESSING.value
            or str(row["claim_id"]) != claim.claim_id
            or str(row["lease_owner"]) != claim.worker_id
            or float(row["lease_until"] or 0.0) <= now
        ):
            raise MailboxLeaseConflict("mailbox event is not owned by this claim")

    @staticmethod
    def _validate_aggregate_transition(
        current: AgentSessionAggregate,
        target: AgentSessionAggregate,
        *,
        expected_revision: int,
    ) -> None:
        if current.state_revision != expected_revision:
            raise AggregateVersionConflict(
                f"expected revision {expected_revision}, found {current.state_revision}"
            )
        if target.event_sequence != current.event_sequence + 1:
            raise ValueError("target event_sequence must advance by exactly one")
        if target.state_revision not in {
            current.state_revision,
            current.state_revision + 1,
        }:
            raise ValueError("target state_revision must stay unchanged or advance by one")
        current_state = replace(
            current,
            state_revision=0,
            event_sequence=0,
            updated_at=0.0,
        )
        target_state = replace(
            target,
            state_revision=0,
            event_sequence=0,
            updated_at=0.0,
        )
        state_changed = current_state != target_state
        expected_state_revision = current.state_revision + (1 if state_changed else 0)
        if target.state_revision != expected_state_revision:
            raise ValueError(
                "target state_revision must reflect the canonical aggregate diff"
            )
        if target.activity_generation < current.activity_generation:
            raise ValueError("activity_generation cannot move backwards")
        if target.active_epoch < current.active_epoch:
            raise ValueError("active_epoch cannot move backwards")
        if target.updated_at < current.updated_at:
            raise ValueError("updated_at cannot move backwards")

    @staticmethod
    def _append_transition(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        *,
        current: AgentSessionAggregate,
        target: AgentSessionAggregate,
        transition: SessionTransition,
        now: float,
    ) -> None:
        operation_id = transition.caused_operation_id
        plan_id = transition.caused_plan_id
        disposition = transition.disposition
        conn.execute(
            """
            INSERT INTO agent_state_transitions (
                transition_id, profile_id, session_id, ownership_generation,
                event_id, from_state,
                to_state, trigger, disposition, state_revision, event_sequence,
                operation_id, plan_id, trace_id, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _transition_id_for_claim(claim),
                claim.key.profile_id,
                claim.key.session_id,
                target.ownership_generation,
                claim.envelope.event_id,
                current.state,
                target.state,
                transition.reason,
                disposition,
                target.state_revision,
                target.event_sequence,
                operation_id,
                plan_id,
                claim.envelope.trace_id,
                _json_dumps({"result": transition.result}),
                now,
            ),
        )

    @staticmethod
    def _append_failed_transition(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        *,
        current: AgentSessionAggregate,
        next_event_sequence: int,
        error: str,
        now: float,
    ) -> None:
        conn.execute(
            """
            INSERT INTO agent_state_transitions (
                transition_id, profile_id, session_id, ownership_generation,
                event_id, from_state,
                to_state, trigger, disposition, state_revision, event_sequence,
                operation_id, plan_id, trace_id, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?)
            """,
            (
                _transition_id_for_claim(claim),
                claim.key.profile_id,
                claim.key.session_id,
                current.ownership_generation,
                claim.envelope.event_id,
                current.state,
                current.state,
                "mailbox_failed",
                "failed",
                current.state_revision,
                next_event_sequence,
                claim.envelope.trace_id,
                _json_dumps({"error": error}),
                now,
            ),
        )

    @staticmethod
    def _append_effect(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        effect: SessionEffect,
        *,
        timing: tuple[float, float, float | None],
        now: float,
    ) -> None:
        scheduled_from, available_at, delay_seconds = timing
        payload = dict(effect.payload)
        if delay_seconds is not None:
            payload.update(
                {
                    "available_after_seconds": delay_seconds,
                    "scheduled_from": scheduled_from,
                    "available_at": available_at,
                }
            )
            if effect.kind == "enqueue_idle_review_planning_deadline":
                payload["deadline_at"] = available_at
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id,
                operation_id, kind, contract_version, contract_signature,
                payload_json, status, attempt_count,
                available_at, lease_owner, lease_until, created_at, updated_at,
                completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', NULL, ?, ?, NULL, '')
            """,
            (
                effect.effect_id,
                effect.idempotency_key,
                claim.key.profile_id,
                claim.key.session_id,
                claim.envelope.ownership_generation,
                claim.envelope.event_id,
                effect.operation_id,
                effect.kind,
                effect.contract_version,
                effect.contract_signature,
                _json_dumps(payload),
                available_at,
                now,
                now,
            ),
        )

    @staticmethod
    def _resolve_operation_input_fences(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        operations: tuple[SessionOperation, ...],
        *,
        input_ledger_sequence: int,
    ) -> dict[str, _OperationInputFence]:
        """Resolve workflow input boundaries under the open actor transaction."""

        if input_ledger_sequence < 0:
            raise ValueError("input_ledger_sequence must not be negative")
        fences: dict[str, _OperationInputFence] = {}
        seen_operation_ids: set[str] = set()
        for operation in operations:
            operation_id = str(operation.operation_id).strip()
            if operation_id in seen_operation_ids:
                raise DurableRecordConflict(
                    f"operation id {operation_id!r} occurs twice in one transition"
                )
            seen_operation_ids.add(operation_id)
            existing = conn.execute(
                """
                SELECT profile_id, session_id, ownership_generation, kind,
                       input_watermark, input_ledger_sequence
                FROM agent_session_operations
                WHERE operation_id = ?
                """,
                (operation_id,),
            ).fetchone()
            if existing is None:
                if operation.input_watermark is None:
                    if operation.input_ledger_sequence is not None:
                        raise DurableRecordConflict(
                            "operation without an input watermark cannot carry "
                            "an input ledger sequence"
                        )
                    continue
                if (
                    operation.input_ledger_sequence is not None
                    and operation.input_ledger_sequence != input_ledger_sequence
                ):
                    raise DurableRecordConflict(
                        "new operation supplied a stale input ledger sequence"
                    )
                fences[operation_id] = _OperationInputFence(
                    input_watermark=operation.input_watermark,
                    input_ledger_sequence=input_ledger_sequence,
                    requires_pending_mapping=operation.status
                    in {
                        SessionOperationStatus.PENDING,
                        SessionOperationStatus.RUNNING,
                    },
                )
                continue

            existing_identity = (
                str(existing["profile_id"]),
                str(existing["session_id"]),
                int(existing["ownership_generation"]),
                str(existing["kind"]),
            )
            expected_identity = (
                claim.key.profile_id,
                claim.key.session_id,
                claim.envelope.ownership_generation,
                operation.kind,
            )
            if existing_identity != expected_identity:
                raise DurableRecordConflict(
                    f"operation id {operation_id!r} is already used by different work"
                )
            durable_watermark = _optional_int(existing["input_watermark"])
            durable_sequence = _optional_int(existing["input_ledger_sequence"])
            if (durable_watermark is None) != (durable_sequence is None):
                raise DurableRecordConflict(
                    f"operation id {operation_id!r} has an incomplete input fence"
                )
            if operation.input_watermark is not None and (
                operation.input_watermark != durable_watermark
            ):
                raise DurableRecordConflict(
                    f"operation id {operation_id!r} changed its input watermark"
                )
            if operation.input_ledger_sequence is not None and (
                operation.input_ledger_sequence != durable_sequence
            ):
                raise DurableRecordConflict(
                    f"operation id {operation_id!r} changed its ledger boundary"
                )
            if durable_watermark is not None and durable_sequence is not None:
                fences[operation_id] = _OperationInputFence(
                    input_watermark=durable_watermark,
                    input_ledger_sequence=durable_sequence,
                    requires_pending_mapping=operation.status
                    in {
                        SessionOperationStatus.PENDING,
                        SessionOperationStatus.RUNNING,
                    },
                )
        return fences

    @staticmethod
    def _upsert_operation(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        target: AgentSessionAggregate,
        operation: Mapping[str, object],
        *,
        now: float,
    ) -> None:
        operation_id = _required_text(operation, "operation_id")
        kind = _required_text(operation, "kind")
        status = SessionOperationStatus(
            str(operation.get("status") or SessionOperationStatus.PENDING.value)
        ).value
        existing = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation, kind, status,
                   launched_by_event_id, state_revision, active_epoch,
                   activity_generation, input_watermark,
                   input_ledger_sequence, started_at,
                   metadata_json
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()
        if existing is not None and (
            str(existing["profile_id"]) != claim.key.profile_id
            or str(existing["session_id"]) != claim.key.session_id
            or int(existing["ownership_generation"])
            != target.ownership_generation
            or str(existing["kind"]) != kind
        ):
            raise DurableRecordConflict(
                f"operation id {operation_id!r} is already used by different work"
            )
        if existing is not None:
            _validate_operation_status_transition(str(existing["status"]), status)
        launched_by_event_id = str(operation.get("launched_by_event_id") or "")
        if not launched_by_event_id:
            launched_by_event_id = (
                str(existing["launched_by_event_id"])
                if existing is not None
                else claim.envelope.event_id
            )
        state_revision = int(
            _existing_or_default(
                existing,
                operation,
                "state_revision",
                target.state_revision,
            )
        )
        active_epoch = int(
            _existing_or_default(
                existing,
                operation,
                "active_epoch",
                target.active_epoch,
            )
        )
        activity_generation = int(
            _existing_or_default(
                existing,
                operation,
                "activity_generation",
                target.activity_generation,
            )
        )
        input_watermark = _optional_int(
            _existing_or_default(existing, operation, "input_watermark", None)
        )
        input_ledger_sequence = _optional_int(
            _existing_or_default(
                existing,
                operation,
                "input_ledger_sequence",
                None,
            )
        )
        if (input_watermark is None) != (input_ledger_sequence is None):
            raise DurableRecordConflict(
                "operation input watermark and ledger sequence must be paired"
            )
        started_at = float(
            _existing_or_default(existing, operation, "started_at", now)
        )
        if existing is not None:
            immutable_identity = (
                str(existing["launched_by_event_id"]),
                int(existing["state_revision"]),
                int(existing["active_epoch"]),
                int(existing["activity_generation"]),
                _optional_int(existing["input_watermark"]),
                _optional_int(existing["input_ledger_sequence"]),
                float(existing["started_at"]),
            )
            requested_identity = (
                launched_by_event_id,
                state_revision,
                active_epoch,
                activity_generation,
                input_watermark,
                input_ledger_sequence,
                started_at,
            )
            if immutable_identity != requested_identity:
                raise DurableRecordConflict(
                    f"operation id {operation_id!r} changed its immutable fences"
                )
        metadata = _json_mapping(existing["metadata_json"]) if existing is not None else {}
        metadata.update(_mapping(operation.get("metadata")))
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status,
                launched_by_event_id, state_revision, active_epoch,
                activity_generation, input_watermark,
                input_ledger_sequence, started_at, lease_owner,
                lease_until, superseded_at, finished_at, failure_code,
                failure_message, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(operation_id) DO UPDATE SET
                status = excluded.status,
                lease_owner = excluded.lease_owner,
                lease_until = excluded.lease_until,
                superseded_at = excluded.superseded_at,
                finished_at = excluded.finished_at,
                failure_code = excluded.failure_code,
                failure_message = excluded.failure_message,
                metadata_json = excluded.metadata_json
            """,
            (
                operation_id,
                claim.key.profile_id,
                claim.key.session_id,
                target.ownership_generation,
                kind,
                status,
                launched_by_event_id,
                state_revision,
                active_epoch,
                activity_generation,
                input_watermark,
                input_ledger_sequence,
                started_at,
                str(operation.get("lease_owner") or ""),
                _optional_float(operation.get("lease_until")),
                _optional_float(operation.get("superseded_at")),
                _optional_float(operation.get("finished_at")),
                str(operation.get("failure_code") or ""),
                str(operation.get("failure_message") or ""),
                _json_dumps(metadata),
            ),
        )

    @staticmethod
    def _resolve_schedule_timings(
        conn: sqlite3.Connection,
        transition: SessionTransition,
        *,
        now: float,
    ) -> dict[str, tuple[float, float, float]]:
        timings: dict[str, tuple[float, float, float]] = {}
        for schedule in transition.review_schedules:
            plan_id = str(schedule.plan_id).strip()
            row = conn.execute(
                """
                SELECT profile_id, session_id, ownership_generation,
                       applied_delay_seconds,
                       scheduled_from, next_review_at
                FROM agent_review_schedules
                WHERE plan_id = ?
                """,
                (plan_id,),
            ).fetchone()
            if row is None:
                applied_delay = float(schedule.applied_delay_seconds)
                next_review_at = _nonnegative_finite(
                    now + applied_delay,
                    field_name="next_review_at",
                )
                timings[plan_id] = (now, next_review_at, applied_delay)
                continue
            if (
                str(row["profile_id"]) != transition.aggregate.profile_id
                or str(row["session_id"]) != transition.aggregate.session_id
                or int(row["ownership_generation"])
                != transition.aggregate.ownership_generation
            ):
                raise DurableRecordConflict(
                    f"review plan id {plan_id!r} is already used by different work"
                )
            timings[plan_id] = (
                float(row["scheduled_from"]),
                float(row["next_review_at"]),
                float(row["applied_delay_seconds"]),
            )
        return timings

    @staticmethod
    def _upsert_review_schedule(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        current: AgentSessionAggregate,
        target: AgentSessionAggregate,
        schedule: Mapping[str, object],
        *,
        scheduled_from: float,
        next_review_at: float,
        now: float,
    ) -> None:
        plan_id = _required_text(schedule, "plan_id")
        plan_revision = int(schedule.get("plan_revision") or 0)
        applied_delay = float(schedule["applied_delay_seconds"])
        status = ReviewScheduleStatus(
            str(schedule.get("status") or ReviewScheduleStatus.SCHEDULED.value)
        ).value
        existing = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation,
                   plan_revision, status,
                   applied_delay_seconds
            FROM agent_review_schedules
            WHERE plan_id = ?
            """,
            (plan_id,),
        ).fetchone()
        if existing is not None and (
            str(existing["profile_id"]) != claim.key.profile_id
            or str(existing["session_id"]) != claim.key.session_id
            or int(existing["ownership_generation"])
            != target.ownership_generation
            or int(existing["plan_revision"]) != plan_revision
        ):
            raise DurableRecordConflict(
                f"review plan id {plan_id!r} is already used by different work"
            )
        if target.current_plan_id != plan_id or target.review_plan_revision != plan_revision:
            raise DurableRecordConflict(
                "review schedule does not match the aggregate current plan fence"
            )
        if existing is None:
            if plan_revision != current.review_plan_revision + 1:
                raise DurableRecordConflict(
                    "new review plan revision must advance exactly once"
                )
            SQLiteSessionActorStore._supersede_prior_review_schedules(
                conn,
                claim,
                target,
                superseded_by_plan_id=plan_id,
                now=now,
            )
        else:
            _validate_review_schedule_status_transition(
                str(existing["status"]),
                status,
            )
            if float(existing["applied_delay_seconds"]) != applied_delay:
                raise DurableRecordConflict(
                    f"review plan id {plan_id!r} changed its applied delay"
                )
        conn.execute(
            """
            INSERT INTO agent_review_schedules (
                plan_id, profile_id, session_id, ownership_generation,
                plan_revision, status,
                trigger, outcome, source, requested_delay_seconds,
                applied_delay_seconds, scheduled_from, next_review_at, reason,
                fallback_reason, mention_sensitivity,
                active_reply_threshold_json, model_execution_id,
                prompt_signature, expected_active_epoch,
                expected_activity_generation, committed_state_revision,
                available_at, claim_owner, claim_until, attempt_count,
                last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(plan_id) DO UPDATE SET
                status = excluded.status,
                available_at = excluded.available_at,
                claim_owner = excluded.claim_owner,
                claim_until = excluded.claim_until,
                attempt_count = excluded.attempt_count,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (
                plan_id,
                claim.key.profile_id,
                claim.key.session_id,
                target.ownership_generation,
                plan_revision,
                status,
                str(schedule.get("trigger") or ""),
                str(schedule.get("outcome") or ""),
                str(schedule.get("source") or ""),
                _optional_float(schedule.get("requested_delay_seconds")),
                applied_delay,
                scheduled_from,
                next_review_at,
                str(schedule.get("reason") or ""),
                str(schedule.get("fallback_reason") or ""),
                str(schedule.get("mention_sensitivity") or "normal"),
                _json_dumps(_mapping(schedule.get("active_reply_threshold"))),
                str(schedule.get("model_execution_id") or ""),
                str(schedule.get("prompt_signature") or ""),
                _optional_int(schedule.get("expected_active_epoch")),
                _optional_int(schedule.get("expected_activity_generation")),
                int(
                    _value_or_default(
                        schedule,
                        "committed_state_revision",
                        target.state_revision,
                    )
                ),
                float(_value_or_default(schedule, "available_at", next_review_at)),
                str(schedule.get("claim_owner") or ""),
                _optional_float(schedule.get("claim_until")),
                int(schedule.get("attempt_count") or 0),
                str(schedule.get("last_error") or ""),
                float(_value_or_default(schedule, "created_at", now)),
                float(_value_or_default(schedule, "updated_at", now)),
            ),
        )

    @staticmethod
    def _supersede_prior_review_schedules(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        target: AgentSessionAggregate,
        *,
        superseded_by_plan_id: str,
        now: float,
    ) -> None:
        rows = conn.execute(
            """
            SELECT plan_id
            FROM agent_review_schedules
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND status IN ('scheduled', 'claimed')
              AND plan_id != ?
            ORDER BY plan_revision
            """,
            (
                claim.key.profile_id,
                claim.key.session_id,
                target.ownership_generation,
                superseded_by_plan_id,
            ),
        ).fetchall()
        if not rows:
            return
        conn.execute(
            """
            UPDATE agent_review_schedules
            SET status = 'superseded',
                claim_owner = '',
                claim_until = NULL,
                updated_at = ?
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND status IN ('scheduled', 'claimed')
              AND plan_id != ?
            """,
            (
                now,
                claim.key.profile_id,
                claim.key.session_id,
                target.ownership_generation,
                superseded_by_plan_id,
            ),
        )
        for row in rows:
            previous_plan_id = str(row["plan_id"])
            SQLiteSessionActorStore._append_review_schedule_event(
                conn,
                claim,
                target,
                {
                    "schedule_event_id": _supersede_schedule_event_id(
                        claim,
                        previous_plan_id=previous_plan_id,
                        superseded_by_plan_id=superseded_by_plan_id,
                    ),
                    "event_type": "superseded",
                    "plan_id": previous_plan_id,
                    "previous_plan_id": previous_plan_id,
                    "outcome": "superseded",
                    "source": "session_actor_store",
                    "committed_state_revision": target.state_revision,
                    "metadata": {"superseded_by_plan_id": superseded_by_plan_id},
                },
                schedule_timing=None,
                now=now,
            )

    @staticmethod
    def _append_review_schedule_event(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        target: AgentSessionAggregate,
        schedule_event: Mapping[str, object],
        *,
        schedule_timing: tuple[float, float, float] | None,
        now: float,
    ) -> None:
        schedule_event_id = _required_text(schedule_event, "schedule_event_id")
        event_type = _required_text(schedule_event, "event_type")
        applied_delay = _optional_float(schedule_event.get("applied_delay_seconds"))
        scheduled_from = _optional_float(schedule_event.get("scheduled_from"))
        next_review_at = _optional_float(schedule_event.get("next_review_at"))
        if schedule_timing is not None:
            scheduled_from, next_review_at, applied_delay = schedule_timing
        conn.execute(
            """
            INSERT INTO agent_review_schedule_events (
                schedule_event_id, profile_id, session_id,
                ownership_generation, event_id, plan_id,
                previous_plan_id, event_type, trigger, outcome, source,
                requested_delay_seconds, applied_delay_seconds, scheduled_from,
                next_review_at, reason, fallback_reason, model_execution_id,
                prompt_signature, expected_active_epoch,
                expected_activity_generation, committed_state_revision,
                operation_id, trace_id, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                schedule_event_id,
                claim.key.profile_id,
                claim.key.session_id,
                target.ownership_generation,
                claim.envelope.event_id,
                str(schedule_event.get("plan_id") or ""),
                str(schedule_event.get("previous_plan_id") or ""),
                event_type,
                str(schedule_event.get("trigger") or ""),
                str(schedule_event.get("outcome") or ""),
                str(schedule_event.get("source") or ""),
                _optional_float(schedule_event.get("requested_delay_seconds")),
                applied_delay,
                scheduled_from,
                next_review_at,
                str(schedule_event.get("reason") or ""),
                str(schedule_event.get("fallback_reason") or ""),
                str(schedule_event.get("model_execution_id") or ""),
                str(schedule_event.get("prompt_signature") or ""),
                _optional_int(schedule_event.get("expected_active_epoch")),
                _optional_int(schedule_event.get("expected_activity_generation")),
                int(
                    _value_or_default(
                        schedule_event,
                        "committed_state_revision",
                        target.state_revision,
                    )
                ),
                str(schedule_event.get("operation_id") or ""),
                str(schedule_event.get("trace_id") or claim.envelope.trace_id),
                _json_dumps(_mapping(schedule_event.get("metadata"))),
                float(_value_or_default(schedule_event, "created_at", now)),
            ),
        )


def _aggregate_from_row(row: sqlite3.Row) -> AgentSessionAggregate:
    return AgentSessionAggregate(
        key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
        ownership_generation=int(row["ownership_generation"]),
        state=str(row["state"]),
        state_revision=int(row["state_revision"]),
        event_sequence=int(row["event_sequence"]),
        activity_generation=int(row["activity_generation"]),
        active_epoch=int(row["active_epoch"]),
        current_plan_id=str(row["current_plan_id"] or ""),
        review_plan_revision=int(row["review_plan_revision"]),
        review_plan=_json_mapping(row["review_plan_json"]),
        active_reply_resume=_json_mapping(row["active_reply_resume_json"]),
        active_chat_state=_json_mapping(row["active_chat_state_json"]),
        review_operation_id=str(row["review_operation_id"] or ""),
        active_reply_operation_id=str(row["active_reply_operation_id"] or ""),
        active_chat_round_operation_id=str(row["active_chat_round_operation_id"] or ""),
        idle_planning_operation_id=str(row["idle_planning_operation_id"] or ""),
        data=_json_mapping(row["data_json"]),
        updated_at=float(row["updated_at"]),
    )


def _envelope_from_row(row: sqlite3.Row) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=str(row["event_id"]),
        key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
        kind=str(row["kind"]),
        ownership_generation=int(row["ownership_generation"]),
        payload=_json_mapping(row["payload_json"]),
        source=str(row["source"] or ""),
        occurred_at=float(row["occurred_at"]),
        causation_id=str(row["causation_id"] or ""),
        correlation_id=str(row["correlation_id"] or ""),
        trace_id=str(row["trace_id"] or ""),
        available_at=float(row["available_at"]),
        created_at=float(row["created_at"]),
    )


def _mailbox_status(value: str) -> MailboxEventStatus:
    try:
        return MailboxEventStatus(value)
    except ValueError as exc:
        raise SessionStoreError(f"unknown mailbox event status: {value!r}") from exc


def _json_dumps(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_mapping(value: object) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SessionStoreError("invalid JSON in durable Agent session state") from exc
    if not isinstance(payload, dict):
        raise SessionStoreError("durable Agent session JSON must contain an object")
    return payload


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _required_text(value: Mapping[str, object], field_name: str) -> str:
    result = str(value.get(field_name) or "").strip()
    if not result:
        raise ValueError(f"{field_name} must not be empty")
    return result


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


def _nonnegative_finite(value: object, *, field_name: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be finite and non-negative") from exc
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _persistable_ownership_generation(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("ownership_generation must be at least one")
    try:
        generation = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("ownership_generation must be at least one") from exc
    if generation < 1:
        raise ValueError("ownership_generation must be at least one")
    return generation


def _required_nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DurableRecordConflict(f"{field_name} must be a non-negative integer")
    return value


def _optional_nonnegative_int(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    return _required_nonnegative_int(value, field_name=field_name)


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)


def _value_or_default(
    values: Mapping[str, object],
    field_name: str,
    default: object,
) -> object:
    value = values.get(field_name)
    return default if value is None else value


def _existing_or_default(
    existing: sqlite3.Row | None,
    values: Mapping[str, object],
    field_name: str,
    default: object,
) -> object:
    value = values.get(field_name)
    if value is not None:
        return value
    if existing is not None:
        return existing[field_name]
    return default


def _validate_operation_status_transition(current: str, target: str) -> None:
    current_status = SessionOperationStatus(current)
    target_status = SessionOperationStatus(target)
    if current_status == target_status:
        return
    allowed = {
        SessionOperationStatus.PENDING: {
            SessionOperationStatus.RUNNING,
            SessionOperationStatus.COMPLETED,
            SessionOperationStatus.FAILED,
            SessionOperationStatus.SUPERSEDED,
            SessionOperationStatus.CANCELLED,
        },
        SessionOperationStatus.RUNNING: {
            SessionOperationStatus.COMPLETED,
            SessionOperationStatus.FAILED,
            SessionOperationStatus.SUPERSEDED,
            SessionOperationStatus.CANCELLED,
        },
    }
    if target_status not in allowed.get(current_status, set()):
        raise DurableRecordConflict(
            "operation status cannot move backwards: "
            f"{current_status.value} -> {target_status.value}"
        )


def _validate_review_schedule_status_transition(current: str, target: str) -> None:
    current_status = ReviewScheduleStatus(current)
    target_status = ReviewScheduleStatus(target)
    if current_status == target_status:
        return
    terminal = {
        ReviewScheduleStatus.COMPLETED,
        ReviewScheduleStatus.FAILED,
        ReviewScheduleStatus.SUPERSEDED,
    }
    if current_status in terminal:
        raise DurableRecordConflict(
            "review schedule status cannot leave a terminal state: "
            f"{current_status.value} -> {target_status.value}"
        )


def _apply_review_schedule_clock(
    aggregate: AgentSessionAggregate,
    timings: Mapping[str, tuple[float, float, float]],
) -> AgentSessionAggregate:
    plan_id = aggregate.current_plan_id or str(
        aggregate.review_plan.get("plan_id") or ""
    ).strip()
    timing = timings.get(plan_id)
    if timing is None:
        return aggregate
    scheduled_from, next_review_at, applied_delay = timing
    review_plan = dict(aggregate.review_plan)
    review_plan.update(
        {
            "scheduled_from": scheduled_from,
            "next_review_at": next_review_at,
            "applied_delay_seconds": applied_delay,
            "plan_id": plan_id,
        }
    )
    return replace(aggregate, review_plan=review_plan)


def _resolve_effect_timings(
    transition: SessionTransition,
    *,
    now: float,
) -> dict[str, tuple[float, float, float | None]]:
    timings: dict[str, tuple[float, float, float | None]] = {}
    for effect in transition.effects:
        if effect.effect_id in timings:
            raise DurableRecordConflict(
                f"duplicate effect id in one transition: {effect.effect_id!r}"
            )
        delay_seconds = effect.available_after_seconds
        if delay_seconds is None:
            available_at = effect.available_at or now
            scheduled_from = now if effect.available_at == 0 else effect.available_at
        else:
            scheduled_from = now
            available_at = _nonnegative_finite(
                now + delay_seconds,
                field_name="effect.available_at",
            )
        timings[effect.effect_id] = (
            scheduled_from,
            available_at,
            delay_seconds,
        )
    return timings


def _apply_effect_commit_clock(
    aggregate: AgentSessionAggregate,
    timings: Mapping[str, tuple[float, float, float | None]],
) -> AgentSessionAggregate:
    data = dict(aggregate.data)
    idle_exit = _mapping(data.get("idle_exit"))
    deadline_effect_id = str(idle_exit.get("deadline_effect_id") or "").strip()
    timing = timings.get(deadline_effect_id)
    if timing is None:
        return aggregate
    scheduled_from, deadline_at, delay_seconds = timing
    if delay_seconds is None:
        raise DurableRecordConflict(
            "idle planning deadline effect must use relative availability"
        )
    idle_exit.update(
        {
            "deadline_scheduled_from": scheduled_from,
            "deadline_at": deadline_at,
            "deadline_delay_seconds": delay_seconds,
        }
    )
    data["idle_exit"] = idle_exit
    return replace(aggregate, data=data)


def _stamp_pending_operation_input_fences(
    aggregate: AgentSessionAggregate,
    fences: Mapping[str, _OperationInputFence],
) -> AgentSessionAggregate:
    """Fill only the aggregate's explicit operation-fence registry."""

    if not fences:
        return aggregate
    data = dict(aggregate.data)
    raw_registry = data.get("operation_fences")
    if raw_registry is None:
        registry: dict[str, object] = {}
    elif isinstance(raw_registry, Mapping):
        registry = dict(raw_registry)
    else:
        raise DurableRecordConflict("aggregate operation_fences must be an object")
    changed = False
    for operation_id, fence in fences.items():
        raw_pending = registry.get(operation_id)
        if raw_pending is None:
            if fence.requires_pending_mapping:
                raise DurableRecordConflict(
                    f"pending operation {operation_id!r} omitted its aggregate input fence"
                )
            continue
        if not isinstance(raw_pending, Mapping):
            raise DurableRecordConflict(
                f"operation fence {operation_id!r} must be an object"
            )
        pending = dict(raw_pending)
        watermark = _required_nonnegative_int(
            pending.get("input_watermark"),
            field_name=f"operation_fences[{operation_id!r}].input_watermark",
        )
        if watermark != fence.input_watermark:
            raise DurableRecordConflict(
                f"operation fence {operation_id!r} changed its input watermark"
            )
        if "input_ledger_sequence" not in pending:
            raise DurableRecordConflict(
                f"operation fence {operation_id!r} omitted its ledger placeholder"
            )
        supplied_sequence = _optional_nonnegative_int(
            pending["input_ledger_sequence"],
            field_name=(
                f"operation_fences[{operation_id!r}].input_ledger_sequence"
            ),
        )
        if supplied_sequence is not None and (
            supplied_sequence != fence.input_ledger_sequence
        ):
            raise DurableRecordConflict(
                f"operation fence {operation_id!r} changed its ledger boundary"
            )
        pending["input_ledger_sequence"] = fence.input_ledger_sequence
        registry[operation_id] = pending
        changed = True
    if not changed:
        return aggregate
    data["operation_fences"] = registry
    return replace(aggregate, data=data)


def _stamp_effect_input_fences(
    effects: tuple[SessionEffect, ...],
    fences: Mapping[str, _OperationInputFence],
) -> tuple[SessionEffect, ...]:
    """Copy each resolved operation boundary into its own workflow effects."""

    stamped: list[SessionEffect] = []
    for effect in effects:
        payload = dict(effect.payload)
        fence = fences.get(effect.operation_id)
        if fence is None:
            if payload.get("input_ledger_sequence") is not None:
                raise DurableRecordConflict(
                    f"effect {effect.effect_id!r} has no operation input fence"
                )
            stamped.append(effect)
            continue
        if effect.kind in _EXTERNAL_ACTION_EFFECT_KINDS:
            if (
                "input_watermark" in payload
                or "input_ledger_sequence" in payload
            ):
                raise DurableRecordConflict(
                    f"external action effect {effect.effect_id!r} must keep its "
                    "canonical request payload free of workflow input fences"
                )
            stamped.append(effect)
            continue
        watermark = _required_nonnegative_int(
            payload.get("input_watermark"),
            field_name=f"effect {effect.effect_id!r} input_watermark",
        )
        if watermark != fence.input_watermark:
            raise DurableRecordConflict(
                f"effect {effect.effect_id!r} changed its input watermark"
            )
        supplied_sequence = _optional_nonnegative_int(
            payload.get("input_ledger_sequence"),
            field_name=f"effect {effect.effect_id!r} input_ledger_sequence",
        )
        if supplied_sequence is not None and (
            supplied_sequence != fence.input_ledger_sequence
        ):
            raise DurableRecordConflict(
                f"effect {effect.effect_id!r} changed its ledger boundary"
            )
        payload["input_ledger_sequence"] = fence.input_ledger_sequence
        stamped.append(replace(effect, payload=payload))
    return tuple(stamped)


def _stamp_operation_input_fence(
    operation: dict[str, object],
    fence: _OperationInputFence | None,
) -> dict[str, object]:
    if fence is None:
        return operation
    stamped = dict(operation)
    stamped["input_watermark"] = fence.input_watermark
    stamped["input_ledger_sequence"] = fence.input_ledger_sequence
    return stamped


def _apply_operation_commit_clock(
    operation: dict[str, object],
    target: AgentSessionAggregate,
) -> dict[str, object]:
    metadata = _mapping(operation.get("metadata"))
    committed_idle_exit = _mapping(target.data.get("idle_exit"))
    if not committed_idle_exit:
        return operation
    has_nested_idle_exit = "idle_exit" in metadata
    is_idle_planning_input = (
        str(operation.get("kind") or "") == "idle_review_planning"
        and str(metadata.get("operation_id") or "")
        == str(committed_idle_exit.get("operation_id") or "")
    )
    if not has_nested_idle_exit and not is_idle_planning_input:
        return operation
    updated = dict(operation)
    if has_nested_idle_exit:
        metadata["idle_exit"] = committed_idle_exit
    else:
        metadata.update(committed_idle_exit)
    updated["metadata"] = metadata
    return updated


def _recovery_event(
    aggregate: AgentSessionAggregate,
    *,
    now: float,
) -> SessionEventEnvelope:
    operation_ids = {
        "review_operation_id": aggregate.review_operation_id,
        "active_reply_operation_id": aggregate.active_reply_operation_id,
        "active_chat_round_operation_id": aggregate.active_chat_round_operation_id,
        "idle_planning_operation_id": aggregate.idle_planning_operation_id,
    }
    operation_id = next((value for value in operation_ids.values() if value), "")
    identity = _json_dumps(
        [
            aggregate.key.profile_id,
            aggregate.key.session_id,
            aggregate.ownership_generation,
            aggregate.state,
            aggregate.state_revision,
            aggregate.event_sequence,
            aggregate.active_epoch,
            aggregate.activity_generation,
            operation_ids,
        ]
    )
    event_id = f"recovery-requested:{uuid.uuid5(uuid.NAMESPACE_URL, identity).hex}"
    return SessionEventEnvelope(
        event_id=event_id,
        key=aggregate.key,
        kind="RecoveryRequested",
        ownership_generation=aggregate.ownership_generation,
        payload={
            "reason": "non_idle_without_live_completion",
            "expected_state": aggregate.state,
            "expected_state_revision": aggregate.state_revision,
            "expected_event_sequence": aggregate.event_sequence,
            "expected_active_epoch": aggregate.active_epoch,
            "expected_activity_generation": aggregate.activity_generation,
            "operation_id": operation_id,
            **operation_ids,
        },
        source="session_actor_recovery",
        occurred_at=now,
        correlation_id=operation_id,
        trace_id=event_id,
        available_at=now,
        created_at=now,
    )


def _supersede_schedule_event_id(
    claim: ClaimedSessionEvent,
    *,
    previous_plan_id: str,
    superseded_by_plan_id: str,
) -> str:
    identity = _json_dumps(
        [
            claim.key.profile_id,
            claim.key.session_id,
            claim.envelope.event_id,
            previous_plan_id,
            superseded_by_plan_id,
        ]
    )
    return f"schedule-event:supersede:{uuid.uuid5(uuid.NAMESPACE_URL, identity).hex}"


def _transition_id_for_claim(claim: ClaimedSessionEvent) -> str:
    identity = _json_dumps(
        [
            claim.key.profile_id,
            claim.key.session_id,
            claim.envelope.event_id,
        ]
    )
    return f"transition:{uuid.uuid5(uuid.NAMESPACE_URL, identity).hex}"


__all__ = [
    "AggregateVersionConflict",
    "DurableRecordConflict",
    "MailboxEventConflict",
    "MailboxLeaseConflict",
    "MessageLedgerConflict",
    "SQLiteSessionActorStore",
    "SessionAggregateNotFound",
    "SessionStoreError",
]
