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
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    EffectDeclarationValidationError,
    builtin_effect_contract_authority,
    validate_effect_declaration,
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
    ConsumeMessageLedgerEntries,
    MessageLedgerConsumptionKind,
    MessageLedgerConsumptionSelection,
    MessageLedgerEntry,
    MessageLedgerMutation,
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
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
)
from shinbot.agent.runtime.session_actor.recovery_commit import RecoveryCommitIntent
from shinbot.agent.runtime.session_actor.recovery_commit_coordinator import (
    RecoveryCommitResolution,
    SQLiteRecoveryCommitCoordinator,
)
from shinbot.agent.runtime.session_actor.transition_validation import (
    ReviewPlanTransitionValidationError,
    validate_review_plan_transition,
    validate_session_transition,
)

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


_EXTERNAL_ACTION_EFFECT_KINDS = frozenset(
    action_kind.value for action_kind in ExternalActionKind
)
_ACTIVE_CHAT_BOOTSTRAP_HANDOFF_METADATA_FIELDS = frozenset(
    {
        "handoff_input_ledger_sequence",
        "handoff_input_watermark",
        "handoff_message_log_ids",
        "handoff_operation_id",
    }
)
_ACTOR_NATIVE_ACTIVE_CHAT_BOOTSTRAP_EFFECT_KIND = "run_active_chat_bootstrap"
_ACTOR_NATIVE_ACTIVE_CHAT_ROUND_EFFECT_KIND = "run_active_chat_round"
_ACTOR_NATIVE_ACTIVE_CHAT_CONTRACT_VERSION = 3
_LEGACY_RECOVERY_EVENT_KIND = "RecoveryRequested"
_LEGACY_RECOVERY_EVENT_SOURCE = "session_actor_recovery"


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
class _VerifiedActiveChatBootstrapHandoff:
    """Review-owned selection proven safe for one v3 bootstrap operation."""

    operation_id: str
    message_log_ids: tuple[int, ...]


@dataclass(slots=True, frozen=True)
class _OperationInputFence:
    """Store-resolved input boundary for one workflow operation."""

    input_watermark: int
    input_ledger_sequence: int
    requires_pending_mapping: bool = False
    verified_active_chat_bootstrap_handoff: (
        _VerifiedActiveChatBootstrapHandoff | None
    ) = None


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
        effect_contract_authority: EffectContractAuthority | None = None,
        recovery_commit_coordinator: SQLiteRecoveryCommitCoordinator | None = None,
    ) -> None:
        """Initialize the store.

        Args:
            database: Initialized ShinBot database manager.
            lease_seconds: Mailbox claim duration before recovery is allowed.
            retry_delay_seconds: Delay applied when a claimed event is released.
            clock: Injectable wall clock used by tests and persistence records.
            effect_contract_authority: Sealed contract graph shared with the
                actor and durable effect executor.
            recovery_commit_coordinator: Optional raw-authority coordinator for
                typed recovery deliveries in this same database domain.
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
        authority = effect_contract_authority or builtin_effect_contract_authority()
        if not isinstance(authority, EffectContractAuthority):
            raise TypeError(
                "effect_contract_authority must be an EffectContractAuthority"
            )
        if not authority.sealed:
            raise TypeError("effect_contract_authority must be sealed")
        self._effect_contract_authority = authority
        if (
            recovery_commit_coordinator is not None
            and recovery_commit_coordinator.persistence_domain is not database
        ):
            raise ValueError(
                "recovery_commit_coordinator must share the store persistence domain"
            )
        self._recovery_commit_coordinator = recovery_commit_coordinator

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the exact sealed effect authority used by durable commits."""

        return self._effect_contract_authority

    @property
    def persistence_domain(self) -> object:
        """Return the DatabaseManager that owns this transaction domain."""

        return self._database

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
        ownership_generation: int,
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
                ownership_generation=ownership_generation,
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

        recovery_intent = transition.recovery_commit_intent
        if recovery_intent is not None and not isinstance(
            recovery_intent,
            RecoveryCommitIntent,
        ):
            raise TypeError("recovery_commit_intent must be a RecoveryCommitIntent")
        typed_recovery = (
            claim.envelope.kind == RECOVERY_DELIVERY_EVENT_KIND
            and claim.envelope.source == RECOVERY_DELIVERY_EVENT_SOURCE
        )
        if typed_recovery and recovery_intent is None:
            raise DurableRecordConflict(
                "typed recovery delivery requires a recovery commit intent"
            )
        if not typed_recovery and recovery_intent is not None:
            raise DurableRecordConflict(
                "recovery commit intent requires a typed recovery delivery"
            )
        target = transition.aggregate
        if expected_revision < 0:
            raise ValueError("expected_revision must not be negative")
        if not typed_recovery:
            if target.key != claim.key:
                raise ValueError("transition aggregate key does not match mailbox claim")
            self._validate_message_ledger_transition(claim, transition)
            self._validate_effect_declarations(transition)

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._now()
            recovery_resolution: RecoveryCommitResolution | None = None
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
            if not typed_recovery:
                self._validate_mailbox_identity(mailbox_row, claim)
                if str(mailbox_row["status"]) == MailboxEventStatus.COMPLETED.value:
                    current = self._load_row(conn, claim.key)
                    if current is None:
                        raise SessionAggregateNotFound(claim.key.session_id)
                    return _aggregate_from_row(current)
                self._validate_claim_lease(mailbox_row, claim, now=now)
            elif self._recovery_commit_coordinator is None:
                raise DurableRecordConflict(
                    "typed recovery delivery requires a recovery commit coordinator"
                )
            else:
                assert recovery_intent is not None
                prepared_recovery = self._recovery_commit_coordinator.prepare(
                    conn,
                    claim=claim,
                    intent=recovery_intent,
                    provisional_transition=transition,
                    commit_now=now,
                )
                if prepared_recovery.delivery.mailbox_id != int(
                    mailbox_row["mailbox_id"]
                ):
                    raise DurableRecordConflict(
                        "recovery delivery physical mailbox identity changed"
                    )

            current_row = self._load_row(conn, claim.key)
            if current_row is None:
                raise SessionAggregateNotFound(claim.key.session_id)
            current = _aggregate_from_row(current_row)
            commit_expected_revision = expected_revision
            if typed_recovery:
                assert self._recovery_commit_coordinator is not None
                recovery_resolution = self._recovery_commit_coordinator.resolve(
                    prepared_recovery,
                    aggregate=current,
                    transition_validator=(
                        lambda materialized: self._validate_recovery_materialized_transition(
                            current=current,
                            claim=claim,
                            ownership_generation=ownership_generation,
                            transition=materialized,
                        )
                    ),
                )
                transition = recovery_resolution.transition
                target = transition.aggregate
                if recovery_resolution.mailbox_id != prepared_recovery.delivery.mailbox_id:
                    raise DurableRecordConflict(
                        "recovery resolution mailbox identity changed"
                    )
                validate_session_transition(
                    current,
                    transition,
                    effect_contract_authority=self._effect_contract_authority,
                )
                self._validate_message_ledger_transition(claim, transition)
                self._validate_effect_declarations(transition)
                # The coordinator re-proves the current aggregate in this write
                # transaction; its resolved transition is not authorized by the
                # actor's pre-claim aggregate snapshot.
                commit_expected_revision = current.state_revision
            if (
                current.ownership_generation != ownership_generation
                or target.ownership_generation != ownership_generation
            ):
                raise AggregateVersionConflict(
                    "aggregate ownership generation does not match mailbox claim"
                )
            self._validate_review_plan_forward_transition(
                current,
                target,
                transition,
            )
            schedule_timings = self._resolve_schedule_timings(
                conn,
                transition,
                now=now,
            )
            effect_timings = _resolve_effect_timings(transition, now=now)
            plan_advanced = (
                target.current_plan_id != current.current_plan_id
                or target.review_plan_revision != current.review_plan_revision
            )
            if plan_advanced:
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
                message_ledger_mutations=transition.message_ledger_mutations,
                actor_native_bootstrap_operation_ids=frozenset(
                    effect.operation_id
                    for effect in transition.effects
                    if _is_actor_native_active_chat_bootstrap_effect(effect)
                ),
            )
            target = _stamp_pending_operation_input_fences(
                target,
                operation_fences,
            )
            effects = _stamp_effect_input_fences(
                transition.effects,
                operation_fences,
                aggregate=target,
            )
            self._validate_effect_declarations(
                replace(transition, effects=effects)
            )
            target = replace(target, updated_at=max(target.updated_at, now))
            self._validate_aggregate_transition(
                current,
                target,
                expected_revision=commit_expected_revision,
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
                    commit_expected_revision,
                    current.event_sequence,
                ),
            )
            if updated.rowcount != 1:
                raise AggregateVersionConflict(
                    f"stale aggregate revision {commit_expected_revision} for {claim.key}"
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
                  AND mailbox_id = ?
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
                    int(mailbox_row["mailbox_id"]),
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if completed.rowcount != 1:
                raise MailboxLeaseConflict("mailbox lease changed during commit")
            if recovery_resolution is not None:
                assert self._recovery_commit_coordinator is not None
                self._recovery_commit_coordinator.finalize_case(
                    conn,
                    recovery_resolution,
                    commit_now=now,
                )
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

    def _validate_effect_declarations(self, transition: SessionTransition) -> None:
        """Reject malformed effects at each durable declaration boundary."""

        for effect in transition.effects:
            try:
                validate_effect_declaration(
                    effect,
                    authority=self._effect_contract_authority,
                )
            except EffectDeclarationValidationError as exc:
                raise DurableRecordConflict(str(exc)) from exc

    def _validate_recovery_materialized_transition(
        self,
        *,
        current: AgentSessionAggregate,
        claim: ClaimedSessionEvent,
        ownership_generation: int,
        transition: SessionTransition,
    ) -> None:
        """Preflight a proven materializer result before it can mutate storage.

        Recovery coordinators own the fallback to a durable blocker. This store
        callback supplies the actor/effect/review-plan contracts that require
        the composed persistence authority but perform no writes themselves.
        """

        validate_session_transition(
            current,
            transition,
            effect_contract_authority=self._effect_contract_authority,
        )
        self._validate_message_ledger_transition(claim, transition)
        self._validate_effect_declarations(transition)
        target = transition.aggregate
        if (
            current.ownership_generation != ownership_generation
            or target.ownership_generation != ownership_generation
        ):
            raise AggregateVersionConflict(
                "aggregate ownership generation does not match mailbox claim"
            )
        self._validate_review_plan_forward_transition(
            current,
            target,
            transition,
        )
        self._validate_aggregate_transition(
            current,
            target,
            expected_revision=current.state_revision,
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
    def _validate_review_plan_forward_transition(
        current: AgentSessionAggregate,
        target: AgentSessionAggregate,
        transition: SessionTransition,
    ) -> None:
        """Require aggregate plan payload, fence, and schedule to stay aligned."""

        try:
            validate_review_plan_transition(current, transition)
        except ReviewPlanTransitionValidationError as exc:
            raise DurableRecordConflict(str(exc)) from exc

        plan_id_changed = target.current_plan_id != current.current_plan_id
        plan_revision_changed = (
            target.review_plan_revision != current.review_plan_revision
        )
        plan_advanced = plan_id_changed or plan_revision_changed
        plan_payload_changed = _json_dumps(target.review_plan) != _json_dumps(
            current.review_plan
        )
        if not plan_advanced:
            if plan_payload_changed:
                raise DurableRecordConflict(
                    "an existing review plan payload is immutable"
                )

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
        message_ledger_mutations: tuple[MessageLedgerMutation, ...],
        actor_native_bootstrap_operation_ids: frozenset[str],
    ) -> dict[str, _OperationInputFence]:
        """Resolve workflow input boundaries under the open actor transaction."""

        if input_ledger_sequence < 0:
            raise ValueError("input_ledger_sequence must not be negative")
        fences: dict[str, _OperationInputFence] = {}
        seen_operation_ids: set[str] = set()
        operations_by_id = {
            operation.operation_id: operation for operation in operations
        }
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
                resolved_input_ledger_sequence = input_ledger_sequence
                verified_handoff: _VerifiedActiveChatBootstrapHandoff | None = None
                if operation.input_ledger_sequence is not None:
                    resolved_input_ledger_sequence = operation.input_ledger_sequence
                    if _declares_active_chat_bootstrap_handoff(operation):
                        verified_handoff = (
                            SQLiteSessionActorStore._validate_active_chat_bootstrap_handoff(
                                conn,
                                claim,
                                operation,
                                operations_by_id=operations_by_id,
                                current_input_ledger_sequence=input_ledger_sequence,
                                message_ledger_mutations=message_ledger_mutations,
                                require_handoff_certificate=(
                                    operation_id
                                    in actor_native_bootstrap_operation_ids
                                ),
                            )
                        )
                    elif operation.input_ledger_sequence != input_ledger_sequence:
                        raise DurableRecordConflict(
                            "new operation supplied a stale input ledger sequence"
                        )
                fences[operation_id] = _OperationInputFence(
                    input_watermark=operation.input_watermark,
                    input_ledger_sequence=resolved_input_ledger_sequence,
                    requires_pending_mapping=operation.status
                    in {
                        SessionOperationStatus.PENDING,
                        SessionOperationStatus.RUNNING,
                    },
                    verified_active_chat_bootstrap_handoff=verified_handoff,
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
    def _validate_active_chat_bootstrap_handoff(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        operation: SessionOperation,
        *,
        operations_by_id: Mapping[str, SessionOperation],
        current_input_ledger_sequence: int,
        message_ledger_mutations: tuple[MessageLedgerMutation, ...],
        require_handoff_certificate: bool,
    ) -> _VerifiedActiveChatBootstrapHandoff:
        """Authorize one v3 bootstrap operation to retain a review handoff fence.

        Most newly-created operations must snapshot the ledger sequence of the
        event that creates them.  A receipt-gated Active Chat bootstrap is the
        narrow exception: it may need to use the completed review's older
        boundary after later messages have arrived.  The exception is safe only
        when the bootstrap declares, and the store proves, the exact completed
        review operation and consumed handoff selection that own that boundary.
        """

        if (
            operation.kind != "active_chat_bootstrap"
            or operation.input_watermark is None
            or operation.input_ledger_sequence is None
        ):
            raise DurableRecordConflict(
                "historical input fences are restricted to active chat bootstrap"
            )
        if operation.input_ledger_sequence > current_input_ledger_sequence:
            raise DurableRecordConflict(
                "active chat bootstrap handoff exceeds the current ledger boundary"
            )

        metadata = _mapping(operation.metadata)
        handoff_operation_id = _required_text(
            metadata,
            "handoff_operation_id",
        )
        if handoff_operation_id == operation.operation_id:
            raise DurableRecordConflict(
                "active chat bootstrap cannot hand off from itself"
            )
        handoff_input_watermark = _optional_nonnegative_int(
            metadata.get("handoff_input_watermark"),
            field_name="active chat bootstrap handoff_input_watermark",
        )
        handoff_input_ledger_sequence = _optional_nonnegative_int(
            metadata.get("handoff_input_ledger_sequence"),
            field_name="active chat bootstrap handoff_input_ledger_sequence",
        )
        if (
            handoff_input_watermark is None
            or handoff_input_ledger_sequence is None
            or handoff_input_watermark != operation.input_watermark
            or handoff_input_ledger_sequence != operation.input_ledger_sequence
        ):
            raise DurableRecordConflict(
                "active chat bootstrap handoff boundary does not match its operation"
            )
        handoff_message_log_ids = _message_log_id_tuple(
            metadata.get("handoff_message_log_ids"),
            field_name="active chat bootstrap handoff_message_log_ids",
        )

        source = conn.execute(
            """
            SELECT profile_id, session_id, ownership_generation, kind, status,
                   active_epoch, activity_generation,
                   input_watermark, input_ledger_sequence, metadata_json
            FROM agent_session_operations
            WHERE operation_id = ?
            """,
            (handoff_operation_id,),
        ).fetchone()
        if source is None:
            raise DurableRecordConflict(
                "active chat bootstrap handoff review operation does not exist"
            )
        source_identity = (
            str(source["profile_id"]),
            str(source["session_id"]),
            int(source["ownership_generation"]),
        )
        expected_identity = (
            claim.key.profile_id,
            claim.key.session_id,
            claim.envelope.ownership_generation,
        )
        if source_identity != expected_identity:
            raise DurableRecordConflict(
                "active chat bootstrap handoff review belongs to another actor"
            )
        durable_source_input_watermark = _optional_int(source["input_watermark"])
        durable_source_input_ledger_sequence = _optional_int(
            source["input_ledger_sequence"]
        )

        source_operation = operations_by_id.get(handoff_operation_id)
        if source_operation is not None:
            if source_operation.kind != str(source["kind"]):
                raise DurableRecordConflict(
                    "active chat bootstrap handoff changed the review kind"
                )
            if (
                source_operation.input_watermark != durable_source_input_watermark
                or source_operation.input_ledger_sequence
                != durable_source_input_ledger_sequence
            ):
                raise DurableRecordConflict(
                    "active chat bootstrap handoff changed the review input fence"
                )
            source_kind = source_operation.kind
            source_status = source_operation.status.value
            source_active_epoch = source_operation.active_epoch
            source_activity_generation = source_operation.activity_generation
            source_input_watermark = source_operation.input_watermark
            source_input_ledger_sequence = source_operation.input_ledger_sequence
            source_metadata = _mapping(source_operation.metadata)
        else:
            source_kind = str(source["kind"])
            source_status = str(source["status"])
            source_active_epoch = _optional_int(source["active_epoch"])
            source_activity_generation = _optional_int(
                source["activity_generation"]
            )
            source_input_watermark = durable_source_input_watermark
            source_input_ledger_sequence = durable_source_input_ledger_sequence
            source_metadata = _json_mapping(source["metadata_json"])

        if source_kind != "review" or source_status != SessionOperationStatus.COMPLETED.value:
            raise DurableRecordConflict(
                "active chat bootstrap handoff must reference a completed review"
            )
        if (
            source_input_watermark != handoff_input_watermark
            or source_input_ledger_sequence != handoff_input_ledger_sequence
        ):
            raise DurableRecordConflict(
                "active chat bootstrap handoff does not match the review input fence"
            )
        if source_metadata.get("enter_active_chat") is not True:
            raise DurableRecordConflict(
                "active chat bootstrap handoff review did not enter active chat"
            )
        source_consumed_message_log_ids = _message_log_id_tuple(
            source_metadata.get("consumed_message_log_ids"),
            field_name="review consumed_message_log_ids",
        )
        if source_consumed_message_log_ids != handoff_message_log_ids:
            raise DurableRecordConflict(
                "active chat bootstrap handoff does not match review consumption"
            )
        if require_handoff_certificate:
            SQLiteSessionActorStore._validate_active_chat_bootstrap_handoff_certificate(
                conn,
                claim,
                bootstrap_metadata=metadata,
                source_metadata=source_metadata,
                source_operation_id=handoff_operation_id,
                source_active_epoch=source_active_epoch,
                source_activity_generation=source_activity_generation,
                source_input_watermark=source_input_watermark,
                source_input_ledger_sequence=source_input_ledger_sequence,
                source_message_log_ids=source_consumed_message_log_ids,
                source_is_current_transition=source_operation is not None,
                message_ledger_mutations=message_ledger_mutations,
            )
        return _VerifiedActiveChatBootstrapHandoff(
            operation_id=handoff_operation_id,
            message_log_ids=handoff_message_log_ids,
        )

    @staticmethod
    def _validate_active_chat_bootstrap_handoff_certificate(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        *,
        bootstrap_metadata: Mapping[str, object],
        source_metadata: Mapping[str, object],
        source_operation_id: str,
        source_active_epoch: int | None,
        source_activity_generation: int | None,
        source_input_watermark: int | None,
        source_input_ledger_sequence: int | None,
        source_message_log_ids: tuple[int, ...],
        source_is_current_transition: bool,
        message_ledger_mutations: tuple[MessageLedgerMutation, ...],
    ) -> None:
        """Prove a v3 handoff certificate against review and ledger records.

        A review can create bootstrap in its own transition or after an
        external-action receipt. The former has not yet applied its ledger
        mutation, while the latter must have a persisted consumption row. Both
        cases are checked before the bootstrap effect enters the outbox.
        """

        certificate = _active_chat_handoff_certificate(
            bootstrap_metadata.get("handoff_certificate"),
            field_name="active chat bootstrap handoff_certificate",
        )
        source_certificate = _active_chat_handoff_certificate(
            source_metadata.get("active_chat_handoff"),
            field_name="review active_chat_handoff",
        )
        if certificate != source_certificate:
            raise DurableRecordConflict(
                "active chat bootstrap handoff certificate changed review proof"
            )
        if certificate["review_operation_id"] != source_operation_id:
            raise DurableRecordConflict(
                "active chat bootstrap handoff certificate changed review operation"
            )
        if (
            source_active_epoch is None
            or source_activity_generation is None
            or source_input_watermark is None
            or source_input_ledger_sequence is None
        ):
            raise DurableRecordConflict(
                "active chat bootstrap handoff source has incomplete fences"
            )
        if (
            certificate["source_active_epoch"] != source_active_epoch
            or certificate["source_activity_generation"]
            != source_activity_generation
            or certificate["input_watermark"] != source_input_watermark
            or certificate["input_ledger_sequence"]
            != source_input_ledger_sequence
            or certificate["message_log_ids"] != source_message_log_ids
        ):
            raise DurableRecordConflict(
                "active chat bootstrap handoff certificate changed review fences"
            )
        if source_message_log_ids:
            source_completion_event_id = _required_exact_text(
                source_metadata.get("completion_event_id"),
                field_name="review completion_event_id",
            )
            if certificate["review_completion_event_id"] != source_completion_event_id:
                raise DurableRecordConflict(
                    "active chat bootstrap handoff certificate changed review completion"
                )

        if source_is_current_transition:
            SQLiteSessionActorStore._validate_current_transition_handoff_consumption(
                claim,
                certificate=certificate,
                source_operation_id=source_operation_id,
                message_ledger_mutations=message_ledger_mutations,
            )
            return
        SQLiteSessionActorStore._validate_persisted_handoff_consumption(
            conn,
            claim,
            certificate=certificate,
            source_operation_id=source_operation_id,
        )

    @staticmethod
    def _validate_current_transition_handoff_consumption(
        claim: ClaimedSessionEvent,
        *,
        certificate: Mapping[str, object],
        source_operation_id: str,
        message_ledger_mutations: tuple[MessageLedgerMutation, ...],
    ) -> None:
        """Match one newly-completed review to its same-transaction mutation."""

        consumptions = tuple(
            mutation
            for mutation in message_ledger_mutations
            if isinstance(mutation, ConsumeMessageLedgerEntries)
            and mutation.kind is MessageLedgerConsumptionKind.REVIEW
            and mutation.operation_id == source_operation_id
        )
        message_log_ids = tuple(certificate["message_log_ids"])
        if not message_log_ids:
            if consumptions:
                raise DurableRecordConflict(
                    "empty active chat handoff has review consumption"
                )
            return
        if len(consumptions) != 1:
            raise DurableRecordConflict(
                "active chat handoff requires exactly one review consumption"
            )
        consumption = consumptions[0]
        if (
            consumption.key != claim.key
            or consumption.ownership_generation
            != claim.envelope.ownership_generation
            or consumption.selection
            is not MessageLedgerConsumptionSelection.EXPLICIT_IDS
            or consumption.consumption_id
            != certificate["review_consumption_id"]
            or consumption.idempotency_key
            != certificate["review_consumption_idempotency_key"]
            or consumption.source_event_id
            != certificate["review_completion_event_id"]
            or consumption.input_watermark != certificate["input_watermark"]
            or consumption.input_ledger_sequence
            != certificate["input_ledger_sequence"]
            or consumption.explicit_message_log_ids
            != tuple(sorted(message_log_ids))
        ):
            raise DurableRecordConflict(
                "active chat handoff certificate changed review consumption"
            )

    @staticmethod
    def _validate_persisted_handoff_consumption(
        conn: sqlite3.Connection,
        claim: ClaimedSessionEvent,
        *,
        certificate: Mapping[str, object],
        source_operation_id: str,
    ) -> None:
        """Match a receipt-gated bootstrap to its committed review consumption."""

        message_log_ids = tuple(certificate["message_log_ids"])
        rows = conn.execute(
            """
            SELECT consumption_id, idempotency_key, source_event_id,
                   ownership_generation, selection, input_watermark,
                   input_ledger_sequence, explicit_message_log_ids_json
            FROM agent_message_ledger_consumptions
            WHERE profile_id = ?
              AND session_id = ?
              AND operation_id = ?
              AND kind = ?
            ORDER BY committed_at, consumption_id
            """,
            (
                claim.key.profile_id,
                claim.key.session_id,
                source_operation_id,
                MessageLedgerConsumptionKind.REVIEW.value,
            ),
        ).fetchall()
        if not message_log_ids:
            if rows:
                raise DurableRecordConflict(
                    "empty active chat handoff has persisted review consumption"
                )
            return
        if len(rows) != 1:
            raise DurableRecordConflict(
                "active chat handoff requires one persisted review consumption"
            )
        row = rows[0]
        persisted_message_log_ids = _json_message_log_id_tuple(
            row["explicit_message_log_ids_json"],
            field_name="persisted active chat handoff message_log_ids",
        )
        if (
            str(row["consumption_id"]) != certificate["review_consumption_id"]
            or str(row["idempotency_key"])
            != certificate["review_consumption_idempotency_key"]
            or str(row["source_event_id"])
            != certificate["review_completion_event_id"]
            or int(row["ownership_generation"])
            != claim.envelope.ownership_generation
            or str(row["selection"])
            != MessageLedgerConsumptionSelection.EXPLICIT_IDS.value
            or int(row["input_watermark"]) != certificate["input_watermark"]
            or int(row["input_ledger_sequence"])
            != certificate["input_ledger_sequence"]
            or persisted_message_log_ids != tuple(sorted(message_log_ids))
        ):
            raise DurableRecordConflict(
                "active chat handoff certificate changed persisted consumption"
            )
        applied_rows = conn.execute(
            """
            SELECT message_log_id
            FROM agent_message_ledger
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND review_consumption_id = ?
            ORDER BY ledger_sequence
            """,
            (
                claim.key.profile_id,
                claim.key.session_id,
                claim.envelope.ownership_generation,
                str(row["consumption_id"]),
            ),
        ).fetchall()
        if tuple(sorted(int(row["message_log_id"]) for row in applied_rows)) != tuple(
            sorted(message_log_ids)
        ):
            raise DurableRecordConflict(
                "active chat handoff review consumption was not applied to its selection"
            )

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


def _declares_active_chat_bootstrap_handoff(operation: SessionOperation) -> bool:
    """Return whether an operation opts into the verified review handoff path."""

    if operation.kind != "active_chat_bootstrap":
        return False
    metadata = _mapping(operation.metadata)
    return bool(_ACTIVE_CHAT_BOOTSTRAP_HANDOFF_METADATA_FIELDS.intersection(metadata))


def _message_log_id_tuple(value: object, *, field_name: str) -> tuple[int, ...]:
    """Return one exact, duplicate-free durable message selection."""

    if not isinstance(value, (list, tuple)):
        raise DurableRecordConflict(f"{field_name} must be an array")
    result: list[int] = []
    seen: set[int] = set()
    for index, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, int) or item < 1:
            raise DurableRecordConflict(
                f"{field_name}[{index}] must be a positive integer"
            )
        if item in seen:
            raise DurableRecordConflict(
                f"{field_name} must not contain duplicate message log ids"
            )
        result.append(item)
        seen.add(item)
    return tuple(result)


def _active_chat_handoff_certificate(
    value: object,
    *,
    field_name: str,
) -> dict[str, object]:
    """Return one strict, durable review-to-bootstrap handoff certificate."""

    if not isinstance(value, Mapping):
        raise DurableRecordConflict(f"{field_name} must be an object")
    version = _required_nonnegative_int(
        value.get("version"),
        field_name=f"{field_name}.version",
    )
    if version != 1:
        raise DurableRecordConflict(f"{field_name}.version is unsupported")
    message_log_ids = _message_log_id_tuple(
        value.get("message_log_ids"),
        field_name=f"{field_name}.message_log_ids",
    )
    certificate: dict[str, object] = {
        "version": version,
        "review_operation_id": _required_exact_text(
            value.get("review_operation_id"),
            field_name=f"{field_name}.review_operation_id",
        ),
        "source_active_epoch": _required_nonnegative_int(
            value.get("source_active_epoch"),
            field_name=f"{field_name}.source_active_epoch",
        ),
        "source_activity_generation": _required_nonnegative_int(
            value.get("source_activity_generation"),
            field_name=f"{field_name}.source_activity_generation",
        ),
        "input_watermark": _required_nonnegative_int(
            value.get("input_watermark"),
            field_name=f"{field_name}.input_watermark",
        ),
        "input_ledger_sequence": _required_nonnegative_int(
            value.get("input_ledger_sequence"),
            field_name=f"{field_name}.input_ledger_sequence",
        ),
        "message_log_ids": message_log_ids,
        "review_consumption_id": _optional_handoff_certificate_text(
            value.get("review_consumption_id"),
            field_name=f"{field_name}.review_consumption_id",
        ),
        "review_consumption_idempotency_key": _optional_handoff_certificate_text(
            value.get("review_consumption_idempotency_key"),
            field_name=(
                f"{field_name}.review_consumption_idempotency_key"
            ),
        ),
        "review_completion_event_id": _optional_handoff_certificate_text(
            value.get("review_completion_event_id"),
            field_name=f"{field_name}.review_completion_event_id",
        ),
    }
    consumption_fields = (
        "review_consumption_id",
        "review_consumption_idempotency_key",
        "review_completion_event_id",
    )
    if message_log_ids and any(not certificate[field] for field in consumption_fields):
        raise DurableRecordConflict(
            f"{field_name} omits review consumption identity"
        )
    if not message_log_ids and any(certificate[field] for field in consumption_fields):
        raise DurableRecordConflict(
            f"{field_name} has consumption identity without messages"
        )
    return certificate


def _optional_handoff_certificate_text(value: object, *, field_name: str) -> str:
    """Return optional exact JSON text from a handoff certificate."""

    if value is None:
        return ""
    if not isinstance(value, str):
        raise DurableRecordConflict(f"{field_name} must be JSON text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise DurableRecordConflict(f"{field_name} must contain valid UTF-8") from exc
    return value


def _json_message_log_id_tuple(value: object, *, field_name: str) -> tuple[int, ...]:
    """Decode one persisted JSON message selection without coercing its values."""

    if not isinstance(value, str):
        raise DurableRecordConflict(f"{field_name} must be JSON text")
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DurableRecordConflict(f"{field_name} is invalid JSON") from exc
    return _message_log_id_tuple(decoded, field_name=field_name)


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
            "plan_revision": aggregate.review_plan_revision,
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
    *,
    aggregate: AgentSessionAggregate,
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
        if _is_actor_native_active_chat_bootstrap_effect(effect):
            _validate_active_chat_bootstrap_effect_handoff(
                effect,
                payload,
                fence,
            )
        if _is_actor_native_active_chat_round_effect(effect):
            _validate_active_chat_round_effect_fence(
                effect,
                payload,
                aggregate=aggregate,
            )
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


def _is_actor_native_active_chat_bootstrap_effect(effect: SessionEffect) -> bool:
    """Return whether an effect requires the v3 review-handoff binding."""

    return (
        effect.kind == _ACTOR_NATIVE_ACTIVE_CHAT_BOOTSTRAP_EFFECT_KIND
        and effect.contract_version == _ACTOR_NATIVE_ACTIVE_CHAT_CONTRACT_VERSION
    )


def _is_actor_native_active_chat_round_effect(effect: SessionEffect) -> bool:
    """Return whether an effect requires the v3 round-fence binding."""

    return (
        effect.kind == _ACTOR_NATIVE_ACTIVE_CHAT_ROUND_EFFECT_KIND
        and effect.contract_version == _ACTOR_NATIVE_ACTIVE_CHAT_CONTRACT_VERSION
    )


def _validate_active_chat_bootstrap_effect_handoff(
    effect: SessionEffect,
    payload: Mapping[str, object],
    fence: _OperationInputFence,
) -> None:
    """Require one v3 bootstrap effect to retain its proven review handoff."""

    verified_handoff = fence.verified_active_chat_bootstrap_handoff
    if verified_handoff is None:
        raise DurableRecordConflict(
            f"v3 active chat bootstrap effect {effect.effect_id!r} has no "
            "verified review handoff"
        )
    handoff_operation_id = _required_text(payload, "handoff_operation_id")
    if handoff_operation_id != verified_handoff.operation_id:
        raise DurableRecordConflict(
            f"v3 active chat bootstrap effect {effect.effect_id!r} changed "
            "the verified review handoff operation"
        )
    handoff_message_log_ids = _message_log_id_tuple(
        payload.get("handoff_message_log_ids"),
        field_name=f"effect {effect.effect_id!r} handoff_message_log_ids",
    )
    if handoff_message_log_ids != verified_handoff.message_log_ids:
        raise DurableRecordConflict(
            f"v3 active chat bootstrap effect {effect.effect_id!r} changed "
            "the verified review handoff messages"
        )


def _validate_active_chat_round_effect_fence(
    effect: SessionEffect,
    payload: Mapping[str, object],
    *,
    aggregate: AgentSessionAggregate,
) -> None:
    """Require one v3 round effect to retain its aggregate operation fence."""

    operation_fence = _active_chat_round_operation_fence(
        aggregate,
        operation_id=effect.operation_id,
        effect_id=effect.effect_id,
    )
    expected_message_log_ids = _active_chat_round_message_log_id_tuple(
        operation_fence.get("message_log_ids"),
        field_name="active chat round operation fence message_log_ids",
    )
    supplied_message_log_ids = _active_chat_round_message_log_id_tuple(
        payload.get("message_log_ids"),
        field_name=f"effect {effect.effect_id!r} message_log_ids",
    )
    if supplied_message_log_ids != expected_message_log_ids:
        raise DurableRecordConflict(
            f"v3 active chat round effect {effect.effect_id!r} changed "
            "the ordered operation-fence message selection"
        )

    _require_matching_active_chat_round_text_fence(
        effect,
        field_name="round_schedule_id",
        supplied=payload.get("round_schedule_id"),
        expected=operation_fence.get("round_schedule_id"),
    )
    _require_matching_active_chat_round_number_fence(
        effect,
        field_name="active_chat_interest_value",
        supplied=payload.get("active_chat_interest_value"),
        expected=operation_fence.get("active_chat_interest_value"),
    )
    _require_matching_active_chat_round_text_fence(
        effect,
        field_name="bootstrap_disposition",
        supplied=payload.get("bootstrap_disposition"),
        expected=operation_fence.get("bootstrap_disposition"),
    )


def _active_chat_round_operation_fence(
    aggregate: AgentSessionAggregate,
    *,
    operation_id: str,
    effect_id: str,
) -> Mapping[str, object]:
    """Return one v3 round's durable aggregate operation fence."""

    registry = aggregate.data.get("operation_fences")
    if not isinstance(registry, Mapping):
        raise DurableRecordConflict(
            f"v3 active chat round effect {effect_id!r} has no operation fence registry"
        )
    operation_fence = registry.get(operation_id)
    if not isinstance(operation_fence, Mapping):
        raise DurableRecordConflict(
            f"v3 active chat round effect {effect_id!r} has no operation fence"
        )
    return operation_fence


def _active_chat_round_message_log_id_tuple(
    value: object,
    *,
    field_name: str,
) -> tuple[int, ...]:
    """Return one non-empty, ordered v3 round message selection."""

    if not isinstance(value, list):
        raise DurableRecordConflict(f"{field_name} must be a JSON array")
    message_log_ids = _message_log_id_tuple(value, field_name=field_name)
    if not message_log_ids:
        raise DurableRecordConflict(f"{field_name} must not be empty")
    return message_log_ids


def _require_matching_active_chat_round_text_fence(
    effect: SessionEffect,
    *,
    field_name: str,
    supplied: object,
    expected: object,
) -> None:
    """Reject a v3 round effect that changes one exact text operation fence."""

    expected_text = _required_exact_text(
        expected,
        field_name=f"active chat round operation fence {field_name}",
    )
    supplied_text = _required_exact_text(
        supplied,
        field_name=f"effect {effect.effect_id!r} {field_name}",
    )
    if supplied_text != expected_text:
        raise DurableRecordConflict(
            f"v3 active chat round effect {effect.effect_id!r} changed "
            f"the operation-fence {field_name}"
        )


def _require_matching_active_chat_round_number_fence(
    effect: SessionEffect,
    *,
    field_name: str,
    supplied: object,
    expected: object,
) -> None:
    """Reject a v3 round effect that changes one exact numeric operation fence."""

    expected_number = _required_nonnegative_finite_number(
        expected,
        field_name=f"active chat round operation fence {field_name}",
    )
    supplied_number = _required_nonnegative_finite_number(
        supplied,
        field_name=f"effect {effect.effect_id!r} {field_name}",
    )
    if (
        type(supplied_number) is not type(expected_number)
        or supplied_number != expected_number
    ):
        raise DurableRecordConflict(
            f"v3 active chat round effect {effect.effect_id!r} changed "
            f"the operation-fence {field_name}"
        )


def _required_exact_text(value: object, *, field_name: str) -> str:
    """Return non-empty JSON text without coercion or whitespace normalization."""

    if not isinstance(value, str) or not value:
        raise DurableRecordConflict(f"{field_name} must be non-empty JSON text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise DurableRecordConflict(
            f"{field_name} must contain valid UTF-8 text"
        ) from exc
    return value


def _required_nonnegative_finite_number(
    value: object,
    *,
    field_name: str,
) -> int | float:
    """Return one JSON numeric fence value without lossy coercion."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DurableRecordConflict(f"{field_name} must be a JSON number")
    if not math.isfinite(float(value)) or value < 0:
        raise DurableRecordConflict(
            f"{field_name} must be a non-negative finite JSON number"
        )
    return value


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
        kind=_LEGACY_RECOVERY_EVENT_KIND,
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
        source=_LEGACY_RECOVERY_EVENT_SOURCE,
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
