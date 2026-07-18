"""Holder-fenced finalization of the supported idle legacy handoff subset.

This module is an unmounted persistence primitive.  It can prove and commit a
small, explicitly supported legacy-to-Actor v2 target in one SQLite writer
transaction, but it does not publish a wake target, start workers, or resume
ingress on its own.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from sqlite3 import Connection
from typing import TYPE_CHECKING

from shinbot.agent.runtime.session_actor.legacy_state_handoff import (
    ActorV2LegacyIdleStatePreparationBlocked,
    ActorV2LegacyIdleStateTargetPreparer,
)
from shinbot.agent.runtime.session_actor.message_ledger import (
    AppendMessageLedgerEntry,
    ConsumeMessageLedgerEntries,
    MessageLedgerConsumptionKind,
    MessageLedgerConsumptionSelection,
    MessagePriorityFlags,
    append_message_ledger_entry_from_payload,
)
from shinbot.agent.runtime.session_actor.message_ledger_persistence import (
    apply_message_ledger_appends,
    apply_message_ledger_consumptions,
)
from shinbot.core.dispatch.actor_v2_legacy_state_handoff import (
    ActorV2LegacyStateHandoffManifest,
    ActorV2LegacyStateHandoffMaterialization,
)
from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2MigrationBarrier,
    ActorV2MigrationBarrierGrant,
)
from shinbot.core.dispatch.agent_delivery import AgentRouteDelivery
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnership

if TYPE_CHECKING:
    from shinbot.persistence import DatabaseManager


_FINALIZER_SOURCE = "legacy_state_handoff"
_FINALIZER_TRIGGER = "legacy_state_handoff"
_FINALIZER_OUTCOME = "preserved"
_FINALIZER_ROUTE_TERMINAL_CODE = "legacy_state_handoff_materialized"
_FINALIZER_ROUTE_TERMINAL_MESSAGE = (
    "route delivery was represented by an immutable legacy-to-Actor ledger handoff"
)
_HANDOFF_PROVENANCE_DATA_KEY = "legacy_state_handoff"
_DELIVERY_CONTEXT_DATA_KEY = "delivery_context"
_MESSAGE_WATERMARK_DATA_KEY = "message_watermark"
_PENDING_HIGH_PRIORITY_DATA_KEY = "pending_high_priority_message_log_ids"

_APPEND_RECORD_FIELDS = frozenset(
    {
        "profile_id",
        "session_id",
        "message_log_id",
        "source_event_id",
        "actor_event_id",
        "delivery_version",
        "event_source",
        "sender_id",
        "instance_id",
        "event_type",
        "bot_id",
        "bot_binding_id",
        "base_session_id",
        "bot_session_id",
        "platform",
        "self_id",
        "is_private",
        "is_mentioned",
        "is_mention_to_other",
        "is_reply_to_bot",
        "is_poke_to_bot",
        "is_poke_to_other",
        "already_handled",
        "is_stopped",
        "is_self_message",
        "eligible_for_work",
        "suppression_reason",
        "response_profile",
        "priority",
        "causation_id",
        "correlation_id",
        "trace_id",
        "observed_at",
        "occurred_at",
        "event_created_at",
        "metadata",
    }
)

_ACTOR_TARGET_RESIDUAL_TABLES = (
    "agent_session_aggregates",
    "agent_session_mailbox",
    "agent_session_mailbox_handoffs",
    "agent_session_operations",
    "agent_message_ledger_consumptions",
    "agent_message_ledger",
    "agent_review_schedules",
    "agent_state_transitions",
    "agent_review_schedule_events",
    "agent_effect_outbox",
    "agent_review_cancellation_gates",
    "agent_review_execution_runs",
    "agent_model_execution_runs",
    "agent_model_execution_cancellation_gates",
    "agent_session_recovery_cases",
    "agent_session_recovery_findings",
    "agent_external_action_receipts",
    "agent_session_actor_v2_fenced_wake_target_leases",
)


@dataclass(slots=True)
class ActorV2LegacyIdleStateFinalizationBlocked(RuntimeError):
    """Stable reasons why a prepared target cannot become a live Actor owner."""

    blockers: tuple[str, ...]

    def __post_init__(self) -> None:
        """Canonicalize non-empty diagnostic codes without exposing source data."""

        blockers = tuple(str(blocker or "").strip() for blocker in self.blockers)
        if not blockers or any(not blocker for blocker in blockers):
            raise ValueError("legacy idle handoff finalization requires blocker codes")
        if len(set(blockers)) != len(blockers):
            raise ValueError("legacy idle handoff finalization blockers must be unique")
        object.__setattr__(self, "blockers", tuple(sorted(blockers)))
        RuntimeError.__init__(
            self,
            "legacy idle handoff cannot finalize: " + ", ".join(self.blockers),
        )


@dataclass(slots=True, frozen=True)
class ActorV2LegacyIdleStateFinalization:
    """Result of one atomic idle-target ownership handoff."""

    manifest_id: str
    target_digest: str
    ownership: AgentRuntimeOwnership
    barrier: ActorV2MigrationBarrier
    ledger_entry_count: int
    review_plan_id: str = ""

    def __post_init__(self) -> None:
        """Require a completed source boundary and active Actor target owner."""

        manifest_id = str(self.manifest_id or "").strip()
        target_digest = str(self.target_digest or "").strip()
        if not manifest_id or len(target_digest) != 64:
            raise ValueError("idle handoff finalization requires immutable identities")
        if not isinstance(self.ownership, AgentRuntimeOwnership):
            raise TypeError("idle handoff finalization requires typed ownership")
        if not isinstance(self.barrier, ActorV2MigrationBarrier):
            raise TypeError("idle handoff finalization requires a typed barrier")
        if not self.ownership.actor_v2_active:
            raise ValueError("idle handoff finalization requires active Actor ownership")
        if self.ownership.key != self.barrier.key:
            raise ValueError("idle handoff completion owner belongs to another barrier")
        if self.ownership.generation != self.barrier.migration_generation + 1:
            raise ValueError("idle handoff completion has an invalid ownership generation")
        if self.barrier.completion_manifest_id != manifest_id:
            raise ValueError("completed barrier is bound to another source manifest")
        if isinstance(self.ledger_entry_count, bool) or self.ledger_entry_count < 0:
            raise ValueError("idle handoff ledger entry count must be non-negative")
        object.__setattr__(self, "manifest_id", manifest_id)
        object.__setattr__(self, "target_digest", target_digest)
        object.__setattr__(self, "review_plan_id", str(self.review_plan_id or "").strip())


@dataclass(slots=True, frozen=True)
class _SeededLedgerEntry:
    """One ownership-unbound prepared ledger append and source consumption facts."""

    append: AppendMessageLedgerEntry
    legacy_review_consumed: bool
    legacy_chat_consumed: bool


@dataclass(slots=True, frozen=True)
class _PreparedReviewPlan:
    """Validated legacy review timing before it is anchored to commit time."""

    next_review_at: float
    reason: str
    mention_sensitivity: str
    active_reply_threshold: dict[str, object]


class SQLiteActorV2LegacyIdleStateFinalizer:
    """Atomically materialize a proven idle source and complete ownership.

    The finalizer deliberately supports less than the snapshot stager: it can
    preserve durable unread and consumption facts plus a pending review plan,
    but it does not reinterpret prompt summaries, recent-mention windows, or
    an active workflow. Those states stay blocked until they have dedicated
    Actor-side materializers.
    """

    def __init__(
        self,
        database: DatabaseManager,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Bind finalization to one durable database and commit clock."""

        self._database = database
        self._clock = clock or time.time
        self._preparer = ActorV2LegacyIdleStateTargetPreparer()

    @property
    def persistence_domain(self) -> object:
        """Return the exact SQLite domain used for the all-or-nothing handoff."""

        return self._database

    def finalize(
        self,
        *,
        barrier_grant: ActorV2MigrationBarrierGrant,
        manifest_id: str,
        reason: str,
        requested_by: str = "",
    ) -> ActorV2LegacyIdleStateFinalization:
        """Commit one supported idle target under the barrier holder capability.

        No external I/O happens while the SQLite writer lock is held.  Any
        blocker or conflict rolls back the target rows, route terminalization,
        ownership change, and completion sidecar together.
        """

        if not isinstance(barrier_grant, ActorV2MigrationBarrierGrant):
            raise TypeError("barrier_grant must be an ActorV2MigrationBarrierGrant")
        normalized_manifest_id = _required_text(manifest_id, "manifest_id")
        normalized_reason = _required_text(reason, "reason")
        requester = str(requested_by or "").strip()
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _nonnegative_finite(self._clock(), "clock")
            barrier, _drain, manifest, materialization = (
                self._database.actor_v2_legacy_state_handoffs.require_materialization_for_finalization_in_transaction(
                    conn,
                    barrier_grant=barrier_grant,
                    manifest_id=normalized_manifest_id,
                    materializer_id=self._preparer.materializer_id,
                    materializer_version=self._preparer.materializer_version,
                    target_schema_version=self._preparer.target_schema_version,
                )
            )
            prepared = self._prepared_target(manifest, materialization)
            seeded_entries = _seeded_ledger_entries(
                prepared["ledger_seeds"],
                key=manifest.key,
                ownership_generation=barrier.migration_generation,
            )
            plan_seed = _review_plan_seed(prepared["review_plan_seed"])
            high_priority_handled, pending_priority_ids = _high_priority_state(
                prepared["high_priority_events"],
                seeded_entries,
            )
            semantic_blockers = _semantic_blockers(
                prepared,
                seeded_entries,
                plan_seed=plan_seed,
                pending_priority_ids=pending_priority_ids,
            )
            if semantic_blockers:
                raise ActorV2LegacyIdleStateFinalizationBlocked(tuple(semantic_blockers))
            _require_empty_actor_target(conn, manifest.key)
            _terminalize_materialized_route_deliveries(
                conn,
                key=manifest.key,
                seeded_entries=seeded_entries,
                now=now,
            )
            timing = _review_timing(plan_seed, now=now)
            review_plan_id = _review_plan_id(manifest) if timing is not None else ""
            data = _initial_aggregate_data(
                manifest,
                materialization,
                seeded_entries,
                pending_priority_ids=pending_priority_ids,
            )
            _insert_initial_aggregate(
                conn,
                key=manifest.key,
                ownership_generation=barrier.migration_generation,
                review_plan_id=review_plan_id,
                plan_seed=plan_seed,
                timing=timing,
                data=data,
                now=now,
            )
            _append_seeded_ledger_entries(
                conn,
                key=manifest.key,
                ownership_generation=barrier.migration_generation,
                seeded_entries=seeded_entries,
                committed_at=now,
            )
            _preserve_legacy_consumptions(
                conn,
                manifest=manifest,
                ownership_generation=barrier.migration_generation,
                seeded_entries=seeded_entries,
                high_priority_handled=high_priority_handled,
                committed_at=now,
            )
            if timing is not None and plan_seed is not None:
                _insert_review_schedule(
                    conn,
                    manifest=manifest,
                    ownership_generation=barrier.migration_generation,
                    plan_id=review_plan_id,
                    plan_seed=plan_seed,
                    timing=timing,
                    now=now,
                )
            ownership = (
                self._database.agent_runtime_ownership.complete_legacy_to_actor_v2_from_barrier_in_transaction(
                    conn,
                    barrier_grant=barrier_grant,
                    reason=normalized_reason,
                    requested_by=requester or barrier.holder_id,
                    now=now,
                )
            )
            completed_barrier = (
                self._database.actor_v2_migration_barriers.complete_legacy_state_handoff_in_transaction(
                    conn,
                    barrier_grant,
                    manifest_id=manifest.manifest_id,
                    materializer_id=materialization.materializer_id,
                    materializer_version=materialization.materializer_version,
                    target_schema_version=materialization.target_schema_version,
                    source_digest=manifest.source_digest,
                    target_digest=materialization.target_digest,
                    ownership=ownership,
                    reason=normalized_reason,
                    requested_by=requester or barrier.holder_id,
                    now=now,
                )
            )
        return ActorV2LegacyIdleStateFinalization(
            manifest_id=manifest.manifest_id,
            target_digest=materialization.target_digest,
            ownership=ownership,
            barrier=completed_barrier,
            ledger_entry_count=len(seeded_entries),
            review_plan_id=review_plan_id,
        )

    def _prepared_target(
        self,
        manifest: ActorV2LegacyStateHandoffManifest,
        materialization: ActorV2LegacyStateHandoffMaterialization,
    ) -> dict[str, object]:
        """Recompute and compare the exact pure materializer contract output."""

        try:
            recomputed = self._preparer.materialize(manifest)
        except ActorV2LegacyIdleStatePreparationBlocked as exc:
            raise ActorV2LegacyIdleStateFinalizationBlocked(exc.blockers) from exc
        persisted = materialization.target_payload_as_dict()
        if _canonical_json(recomputed) != _canonical_json(persisted):
            raise ActorV2LegacyIdleStateFinalizationBlocked(
                ("legacy_idle_materialization_drift",)
            )
        expected_fields = {
            "schema_version",
            "kind",
            "manifest_id",
            "source_digest",
            "session_key",
            "scope",
            "review_plan_seed",
            "ledger_seeds",
            "high_priority_events",
            "recent_mentions",
            "review_summaries",
            "summaries",
        }
        if set(persisted) != expected_fields:
            raise ActorV2LegacyIdleStateFinalizationBlocked(
                ("legacy_idle_materialization_shape_invalid",)
            )
        return persisted


def _seeded_ledger_entries(
    value: object,
    *,
    key: SessionKey,
    ownership_generation: int,
) -> tuple[_SeededLedgerEntry, ...]:
    """Rehydrate canonical prepared append rows under the barrier generation."""

    if not isinstance(value, list):
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_idle_materialization_shape_invalid",)
        )
    entries: list[_SeededLedgerEntry] = []
    message_log_ids: set[int] = set()
    source_event_ids: set[str] = set()
    for seed in value:
        if not isinstance(seed, Mapping) or set(seed) != {
            "append",
            "legacy_chat_consumed",
            "legacy_review_consumed",
        }:
            raise ActorV2LegacyIdleStateFinalizationBlocked(
                ("legacy_idle_materialization_shape_invalid",)
            )
        append = _append_from_seed(
            seed["append"],
            key=key,
            ownership_generation=ownership_generation,
        )
        review_consumed = _strict_bool(
            seed["legacy_review_consumed"],
            "legacy_review_consumed",
        )
        chat_consumed = _strict_bool(
            seed["legacy_chat_consumed"],
            "legacy_chat_consumed",
        )
        if append.message_log_id in message_log_ids or append.source_event_id in source_event_ids:
            raise ActorV2LegacyIdleStateFinalizationBlocked(
                ("legacy_idle_materialization_ledger_identity_conflict",)
            )
        message_log_ids.add(append.message_log_id)
        source_event_ids.add(append.source_event_id)
        entries.append(
            _SeededLedgerEntry(
                append=append,
                legacy_review_consumed=review_consumed,
                legacy_chat_consumed=chat_consumed,
            )
        )
    return tuple(entries)


def _append_from_seed(
    value: object,
    *,
    key: SessionKey,
    ownership_generation: int,
) -> AppendMessageLedgerEntry:
    """Decode one ownership-unbound append seed through the Actor ledger type."""

    if not isinstance(value, Mapping) or set(value) != _APPEND_RECORD_FIELDS:
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_idle_materialization_ledger_seed_invalid",)
        )
    if value.get("profile_id") != key.profile_id or value.get("session_id") != key.session_id:
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_idle_materialization_ledger_seed_invalid",)
        )
    priority_value = value.get("priority")
    if not isinstance(priority_value, Mapping) or set(priority_value) != {
        "mention",
        "reply_to_bot",
        "repeated_mention",
        "poke_to_bot",
        "should_wake_active_reply",
        "reasons",
    }:
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_idle_materialization_ledger_seed_invalid",)
        )
    try:
        return AppendMessageLedgerEntry(
            key=key,
            ownership_generation=ownership_generation,
            message_log_id=value["message_log_id"],
            source_event_id=value["source_event_id"],
            actor_event_id=value["actor_event_id"],
            delivery_version=value["delivery_version"],
            event_source=value["event_source"],
            sender_id=value["sender_id"],
            instance_id=value["instance_id"],
            event_type=value["event_type"],
            bot_id=value["bot_id"],
            bot_binding_id=value["bot_binding_id"],
            base_session_id=value["base_session_id"],
            bot_session_id=value["bot_session_id"],
            platform=value["platform"],
            self_id=value["self_id"],
            is_private=value["is_private"],
            is_mentioned=value["is_mentioned"],
            is_mention_to_other=value["is_mention_to_other"],
            is_reply_to_bot=value["is_reply_to_bot"],
            is_poke_to_bot=value["is_poke_to_bot"],
            is_poke_to_other=value["is_poke_to_other"],
            already_handled=value["already_handled"],
            is_stopped=value["is_stopped"],
            is_self_message=value["is_self_message"],
            eligible_for_work=value["eligible_for_work"],
            suppression_reason=value["suppression_reason"],
            response_profile=value["response_profile"],
            priority=MessagePriorityFlags(
                mention=priority_value["mention"],
                reply_to_bot=priority_value["reply_to_bot"],
                repeated_mention=priority_value["repeated_mention"],
                poke_to_bot=priority_value["poke_to_bot"],
                should_wake_active_reply=priority_value["should_wake_active_reply"],
                reasons=dict(_mapping(priority_value["reasons"], "priority.reasons")),
            ),
            causation_id=value["causation_id"],
            correlation_id=value["correlation_id"],
            trace_id=value["trace_id"],
            observed_at=value["observed_at"],
            occurred_at=value["occurred_at"],
            event_created_at=value["event_created_at"],
            metadata=dict(_mapping(value["metadata"], "append.metadata")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_idle_materialization_ledger_seed_invalid",)
        ) from exc


def _review_plan_seed(value: object) -> _PreparedReviewPlan | None:
    """Validate one legacy absolute review plan without anchoring it yet."""

    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {
        "next_review_at",
        "reason",
        "mention_sensitivity",
        "active_reply_threshold",
        "updated_at",
    }:
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_review_plan_seed_invalid",)
        )
    try:
        next_review_at = _nonnegative_finite(value["next_review_at"], "next_review_at")
        _nonnegative_finite(value["updated_at"], "updated_at")
        reason = _optional_text(value["reason"], "reason")
        mention_sensitivity = _required_text(
            value["mention_sensitivity"],
            "mention_sensitivity",
        )
        if mention_sensitivity not in {"low", "normal", "high"}:
            raise ValueError("unsupported mention_sensitivity")
        threshold = dict(
            _mapping(value["active_reply_threshold"], "active_reply_threshold")
        )
    except (TypeError, ValueError) as exc:
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_review_plan_seed_invalid",)
        ) from exc
    return _PreparedReviewPlan(
        next_review_at=next_review_at,
        reason=reason,
        mention_sensitivity=mention_sensitivity,
        active_reply_threshold=threshold,
    )


def _high_priority_state(
    value: object,
    seeded_entries: Sequence[_SeededLedgerEntry],
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Map legacy priority handling into Actor ledger and aggregate facts."""

    if not isinstance(value, list):
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_high_priority_state_invalid",)
        )
    entries_by_message_id = {
        entry.append.message_log_id: entry for entry in seeded_entries
    }
    handled_by_message_id: dict[int, bool] = {}
    blockers: list[str] = []
    required_priority = {
        "mention": "mention",
        "reply_to_bot": "reply_to_bot",
        "repeated_mention": "mention",
        "poke": "poke_to_bot",
    }
    for event in value:
        if not isinstance(event, Mapping):
            blockers.append("legacy_high_priority_state_invalid")
            continue
        message_log_id = event.get("message_log_id")
        handled = event.get("handled")
        kind = event.get("kind")
        if (
            isinstance(message_log_id, bool)
            or not isinstance(message_log_id, int)
            or not isinstance(handled, bool)
            or not isinstance(kind, str)
            or kind not in required_priority
        ):
            blockers.append("legacy_high_priority_state_invalid")
            continue
        entry = entries_by_message_id.get(message_log_id)
        if entry is None:
            if not handled:
                blockers.append("legacy_high_priority_event_without_unread")
            continue
        if not bool(getattr(entry.append.priority, required_priority[kind])):
            blockers.append("legacy_high_priority_event_delivery_mismatch")
            continue
        previous = handled_by_message_id.setdefault(message_log_id, handled)
        if previous != handled:
            blockers.append("legacy_high_priority_event_handling_ambiguous")
    if blockers:
        raise ActorV2LegacyIdleStateFinalizationBlocked(tuple(dict.fromkeys(blockers)))
    handled_ids = tuple(
        sorted(message_log_id for message_log_id, handled in handled_by_message_id.items() if handled)
    )
    pending_ids = tuple(
        sorted(
            message_log_id
            for message_log_id, handled in handled_by_message_id.items()
            if not handled
        )
    )
    return handled_ids, pending_ids


def _semantic_blockers(
    prepared: Mapping[str, object],
    seeded_entries: Sequence[_SeededLedgerEntry],
    *,
    plan_seed: _PreparedReviewPlan | None,
    pending_priority_ids: Sequence[int],
) -> list[str]:
    """Reject source facts that lack a semantics-preserving v1 target mapping."""

    blockers: list[str] = []
    for field_name, blocker in (
        ("recent_mentions", "legacy_recent_mentions_materializer_unavailable"),
        ("review_summaries", "legacy_review_summaries_materializer_unavailable"),
        ("summaries", "legacy_prompt_summaries_materializer_unavailable"),
    ):
        value = prepared.get(field_name)
        if not isinstance(value, list):
            blockers.append("legacy_idle_materialization_shape_invalid")
        elif value:
            blockers.append(blocker)
    entries_by_id = {entry.append.message_log_id: entry for entry in seeded_entries}
    for entry in seeded_entries:
        if entry.legacy_review_consumed and entry.legacy_chat_consumed:
            blockers.append("legacy_unread_consumption_overlap")
        if (
            (entry.legacy_review_consumed or entry.legacy_chat_consumed)
            and not entry.append.eligible_for_work
        ):
            blockers.append("legacy_consumed_suppressed_message")
    for message_log_id in pending_priority_ids:
        entry = entries_by_id[message_log_id]
        if entry.legacy_review_consumed or entry.legacy_chat_consumed:
            blockers.append("legacy_pending_high_priority_already_consumed")
    if pending_priority_ids and plan_seed is None:
        blockers.append("legacy_pending_high_priority_without_review_plan")
    return list(dict.fromkeys(blockers))


def _require_empty_actor_target(conn: Connection, key: SessionKey) -> None:
    """Reject residual Actor state instead of merging unproven history."""

    blockers: list[str] = []
    for table_name in _ACTOR_TARGET_RESIDUAL_TABLES:
        row = conn.execute(
            f"""
            SELECT 1
            FROM {table_name}
            WHERE profile_id = ? AND session_id = ?
            LIMIT 1
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        if row is not None:
            blockers.append("actor_target_residual_" + table_name)
    if blockers:
        raise ActorV2LegacyIdleStateFinalizationBlocked(tuple(blockers))


def _terminalize_materialized_route_deliveries(
    conn: Connection,
    *,
    key: SessionKey,
    seeded_entries: Sequence[_SeededLedgerEntry],
    now: float,
) -> None:
    """Prevent a verified source delivery from becoming a duplicate Actor event."""

    blockers: list[str] = []
    for entry in seeded_entries:
        append = entry.append
        rows = conn.execute(
            """
            SELECT *
            FROM agent_route_outbox
            WHERE profile_id = ?
              AND session_id = ?
              AND message_log_id = ?
              AND event_id = ?
            ORDER BY outbox_seq
            """,
            (
                key.profile_id,
                key.session_id,
                append.message_log_id,
                append.source_event_id,
            ),
        ).fetchall()
        if not rows:
            blockers.append("legacy_verified_route_delivery_missing_from_live_outbox")
            continue
        for row in rows:
            if not _outbox_row_matches_seed(row, append):
                blockers.append("legacy_verified_route_delivery_drift")
                continue
            status = str(row["status"])
            if status == "pending":
                if row["claim_id"] or row["lease_owner"] or row["lease_until"] is not None:
                    blockers.append("legacy_verified_route_delivery_live_lease")
                    continue
                updated = conn.execute(
                    """
                    UPDATE agent_route_outbox
                    SET status = 'failed',
                        updated_at = ?,
                        failed_at = ?,
                        claim_id = '',
                        lease_owner = '',
                        lease_until = NULL,
                        last_error_code = ?,
                        last_error_message = ?
                    WHERE outbox_seq = ?
                      AND status = 'pending'
                      AND claim_id = ''
                      AND lease_owner = ''
                      AND lease_until IS NULL
                    """,
                    (
                        now,
                        now,
                        _FINALIZER_ROUTE_TERMINAL_CODE,
                        _FINALIZER_ROUTE_TERMINAL_MESSAGE,
                        int(row["outbox_seq"]),
                    ),
                )
                if updated.rowcount != 1:
                    blockers.append("legacy_verified_route_delivery_changed")
            elif status == "failed":
                if row["claim_id"] or row["lease_owner"] or row["lease_until"] is not None:
                    blockers.append("legacy_verified_route_delivery_live_lease")
            elif status == "processing":
                blockers.append("legacy_verified_route_delivery_live_lease")
            elif status == "completed":
                blockers.append("legacy_verified_route_delivery_already_relayed")
            else:
                blockers.append("legacy_verified_route_delivery_state_invalid")
    if blockers:
        raise ActorV2LegacyIdleStateFinalizationBlocked(tuple(dict.fromkeys(blockers)))


def _outbox_row_matches_seed(row: Mapping[str, object], append: AppendMessageLedgerEntry) -> bool:
    """Verify live outbox payload still projects to the frozen ledger entry."""

    try:
        payload_json = str(row["payload_json"])
        if hashlib.sha256(payload_json.encode("utf-8")).hexdigest() != str(
            row["payload_digest"]
        ):
            return False
        payload = json.loads(payload_json)
        delivery = AgentRouteDelivery.from_payload(payload)
        if _canonical_json(delivery.to_payload()) != payload_json:
            return False
        candidate = append_message_ledger_entry_from_payload(
            delivery.to_mailbox_payload(),
            key=append.key,
            ownership_generation=append.ownership_generation,
            source_event_id=append.source_event_id,
            event_source=append.event_source,
            response_profile=append.response_profile,
            priority=append.priority,
            causation_id=append.causation_id,
            correlation_id=append.correlation_id,
            trace_id=append.trace_id,
            occurred_at=append.occurred_at,
            event_created_at=append.event_created_at,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False
    return (
        delivery.session_key == append.key
        and delivery.message_log_id == append.message_log_id
        and delivery.event_id == append.source_event_id
        and candidate.canonical_json == append.canonical_json
    )


def _review_timing(
    plan_seed: _PreparedReviewPlan | None,
    *,
    now: float,
) -> tuple[float, float, float] | None:
    """Map an old absolute due time to a commit-clock relative Actor schedule."""

    if plan_seed is None:
        return None
    applied_delay = max(0.0, plan_seed.next_review_at - now)
    next_review_at = _nonnegative_finite(now + applied_delay, "next_review_at")
    return now, next_review_at, applied_delay


def _initial_aggregate_data(
    manifest: ActorV2LegacyStateHandoffManifest,
    materialization: ActorV2LegacyStateHandoffMaterialization,
    seeded_entries: Sequence[_SeededLedgerEntry],
    *,
    pending_priority_ids: Sequence[int],
) -> dict[str, object]:
    """Build only Actor-native idle data plus immutable handoff provenance."""

    data: dict[str, object] = {
        _HANDOFF_PROVENANCE_DATA_KEY: {
            "manifest_id": manifest.manifest_id,
            "source_digest": manifest.source_digest,
            "target_digest": materialization.target_digest,
            "materializer_id": materialization.materializer_id,
            "materializer_version": materialization.materializer_version,
            "target_schema_version": materialization.target_schema_version,
        }
    }
    if not seeded_entries:
        return data
    last_append = seeded_entries[-1].append
    data[_MESSAGE_WATERMARK_DATA_KEY] = max(
        entry.append.message_log_id for entry in seeded_entries
    )
    data[_DELIVERY_CONTEXT_DATA_KEY] = {
        "instance_id": last_append.instance_id,
        "target_session_id": last_append.base_session_id,
    }
    if pending_priority_ids:
        data[_PENDING_HIGH_PRIORITY_DATA_KEY] = list(pending_priority_ids)
    return data


def _insert_initial_aggregate(
    conn: Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    review_plan_id: str,
    plan_seed: _PreparedReviewPlan | None,
    timing: tuple[float, float, float] | None,
    data: Mapping[str, object],
    now: float,
) -> None:
    """Insert the idle aggregate before writing its foreign-key-owned facts."""

    review_plan: dict[str, object] = {}
    plan_revision = 0
    if timing is not None and plan_seed is not None:
        scheduled_from, next_review_at, applied_delay = timing
        plan_revision = 1
        review_plan = {
            "plan_id": review_plan_id,
            "plan_revision": plan_revision,
            "trigger": _FINALIZER_TRIGGER,
            "kind": _FINALIZER_OUTCOME,
            "source": _FINALIZER_SOURCE,
            "requested_delay_seconds": None,
            "applied_delay_seconds": applied_delay,
            "reason": plan_seed.reason,
            "fallback_reason": "",
            "mention_sensitivity": plan_seed.mention_sensitivity,
            "active_reply_threshold": plan_seed.active_reply_threshold,
            "model_execution_id": "",
            "prompt_signature": "",
            "expected_active_epoch": 0,
            "expected_activity_generation": 0,
            "committed_state_revision": 0,
            "scheduled_from": scheduled_from,
            "next_review_at": next_review_at,
        }
    conn.execute(
        """
        INSERT INTO agent_session_aggregates (
            profile_id, session_id, ownership_generation, state,
            state_revision, event_sequence, activity_generation, active_epoch,
            review_plan_json, current_plan_id, review_plan_revision,
            active_reply_resume_json, active_chat_state_json,
            review_operation_id, active_reply_operation_id,
            active_chat_round_operation_id, idle_planning_operation_id,
            data_json, created_at, updated_at
        ) VALUES (?, ?, ?, 'idle', 0, 0, 0, 0, ?, ?, ?, '{}', '{}', '', '', '', '', ?, ?, ?)
        """,
        (
            key.profile_id,
            key.session_id,
            ownership_generation,
            _canonical_json(review_plan),
            review_plan_id,
            plan_revision,
            _canonical_json(data),
            now,
            now,
        ),
    )


def _append_seeded_ledger_entries(
    conn: Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    seeded_entries: Sequence[_SeededLedgerEntry],
    committed_at: float,
) -> None:
    """Append each frozen route-derived message through normal ledger validation."""

    for entry in seeded_entries:
        apply_message_ledger_appends(
            conn,
            key=key,
            ownership_generation=ownership_generation,
            source_event_id=entry.append.source_event_id,
            mutations=(entry.append,),
            committed_at=committed_at,
        )


def _preserve_legacy_consumptions(
    conn: Connection,
    *,
    manifest: ActorV2LegacyStateHandoffManifest,
    ownership_generation: int,
    seeded_entries: Sequence[_SeededLedgerEntry],
    high_priority_handled: Sequence[int],
    committed_at: float,
) -> None:
    """Represent old consumed flags with terminal migration provenance operations."""

    input_watermark = max(
        (entry.append.message_log_id for entry in seeded_entries),
        default=0,
    )
    input_ledger_sequence = len(seeded_entries)
    selections = (
        (
            MessageLedgerConsumptionKind.REVIEW,
            tuple(
                entry.append.message_log_id
                for entry in seeded_entries
                if entry.legacy_review_consumed
            ),
        ),
        (
            MessageLedgerConsumptionKind.CHAT,
            tuple(
                entry.append.message_log_id
                for entry in seeded_entries
                if entry.legacy_chat_consumed
            ),
        ),
        (MessageLedgerConsumptionKind.HIGH_PRIORITY, tuple(high_priority_handled)),
    )
    for kind, message_log_ids in selections:
        if not message_log_ids:
            continue
        operation_id = _migration_operation_id(manifest, kind)
        source_event_id = operation_id + ":event"
        _insert_terminal_migration_operation(
            conn,
            key=manifest.key,
            ownership_generation=ownership_generation,
            operation_id=operation_id,
            source_event_id=source_event_id,
            kind=kind,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            manifest=manifest,
            committed_at=committed_at,
        )
        consumption = ConsumeMessageLedgerEntries(
            key=manifest.key,
            kind=kind,
            consumption_id=operation_id + ":consumption",
            idempotency_key=operation_id + ":consumption",
            operation_id=operation_id,
            source_event_id=source_event_id,
            ownership_generation=ownership_generation,
            input_watermark=input_watermark,
            input_ledger_sequence=input_ledger_sequence,
            selection=MessageLedgerConsumptionSelection.EXPLICIT_IDS,
            explicit_message_log_ids=tuple(sorted(message_log_ids)),
            reason="legacy_state_handoff_preserved",
            occurred_at=manifest.captured_at,
            metadata={
                "manifest_id": manifest.manifest_id,
                "source_digest": manifest.source_digest,
            },
        )
        apply_message_ledger_consumptions(
            conn,
            key=manifest.key,
            ownership_generation=ownership_generation,
            source_event_id=source_event_id,
            mutations=(consumption,),
            committed_at=committed_at,
        )


def _insert_terminal_migration_operation(
    conn: Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    operation_id: str,
    source_event_id: str,
    kind: MessageLedgerConsumptionKind,
    input_watermark: int,
    input_ledger_sequence: int,
    manifest: ActorV2LegacyStateHandoffManifest,
    committed_at: float,
) -> None:
    """Insert one completed non-live operation required by ledger provenance FKs."""

    conn.execute(
        """
        INSERT INTO agent_session_operations (
            operation_id, profile_id, session_id, ownership_generation,
            kind, status, launched_by_event_id, state_revision, active_epoch,
            activity_generation, input_watermark, input_ledger_sequence,
            started_at, lease_owner, lease_until, superseded_at, finished_at,
            failure_code, failure_message, metadata_json
        ) VALUES (?, ?, ?, ?, ?, 'completed', ?, 0, 0, 0, ?, ?, ?, '', NULL, NULL, ?, '', '', ?)
        """,
        (
            operation_id,
            key.profile_id,
            key.session_id,
            ownership_generation,
            "legacy_state_handoff_" + kind.value + "_consumption",
            source_event_id,
            input_watermark,
            input_ledger_sequence,
            committed_at,
            committed_at,
            _canonical_json(
                {
                    "manifest_id": manifest.manifest_id,
                    "source_digest": manifest.source_digest,
                    "kind": kind.value,
                }
            ),
        ),
    )


def _insert_review_schedule(
    conn: Connection,
    *,
    manifest: ActorV2LegacyStateHandoffManifest,
    ownership_generation: int,
    plan_id: str,
    plan_seed: _PreparedReviewPlan,
    timing: tuple[float, float, float],
    now: float,
) -> None:
    """Persist the initial plan and its audit record with commit-clock timing."""

    scheduled_from, next_review_at, applied_delay = timing
    conn.execute(
        """
        INSERT INTO agent_review_schedules (
            plan_id, profile_id, session_id, ownership_generation,
            plan_revision, status, trigger, outcome, source,
            requested_delay_seconds, applied_delay_seconds, scheduled_from,
            next_review_at, reason, fallback_reason, mention_sensitivity,
            active_reply_threshold_json, model_execution_id, prompt_signature,
            expected_active_epoch, expected_activity_generation,
            committed_state_revision, available_at, claim_owner, claim_until,
            attempt_count, delivery_cycle, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 1, 'scheduled', ?, ?, ?, NULL, ?, ?, ?, ?, '', ?, ?, '', '', 0, 0, 0, ?, '', NULL, 0, 0, '', ?, ?)
        """,
        (
            plan_id,
            manifest.key.profile_id,
            manifest.key.session_id,
            ownership_generation,
            _FINALIZER_TRIGGER,
            _FINALIZER_OUTCOME,
            _FINALIZER_SOURCE,
            applied_delay,
            scheduled_from,
            next_review_at,
            plan_seed.reason,
            plan_seed.mention_sensitivity,
            _canonical_json(plan_seed.active_reply_threshold),
            next_review_at,
            now,
            now,
        ),
    )
    schedule_event_id = plan_id + ":scheduled"
    conn.execute(
        """
        INSERT INTO agent_review_schedule_events (
            schedule_event_id, profile_id, session_id, ownership_generation,
            event_id, plan_id, previous_plan_id, event_type, trigger, outcome,
            source, requested_delay_seconds, applied_delay_seconds, scheduled_from,
            next_review_at, reason, fallback_reason, model_execution_id,
            prompt_signature, expected_active_epoch, expected_activity_generation,
            committed_state_revision, operation_id, trace_id, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, '', 'scheduled', ?, ?, ?, NULL, ?, ?, ?, ?, '', '', '', 0, 0, 0, '', '', ?, ?)
        """,
        (
            schedule_event_id,
            manifest.key.profile_id,
            manifest.key.session_id,
            ownership_generation,
            _handoff_event_id(manifest),
            plan_id,
            _FINALIZER_TRIGGER,
            _FINALIZER_OUTCOME,
            _FINALIZER_SOURCE,
            applied_delay,
            scheduled_from,
            next_review_at,
            plan_seed.reason,
            _canonical_json(
                {
                    "manifest_id": manifest.manifest_id,
                    "source_digest": manifest.source_digest,
                    "plan_revision": 1,
                }
            ),
            now,
        ),
    )


def _migration_operation_id(
    manifest: ActorV2LegacyStateHandoffManifest,
    kind: MessageLedgerConsumptionKind,
) -> str:
    """Return deterministic provenance identity for one old consumption channel."""

    return f"legacy-state-handoff:{manifest.manifest_id}:{kind.value}:consumption"


def _review_plan_id(manifest: ActorV2LegacyStateHandoffManifest) -> str:
    """Return the stable first Actor plan identity for one source manifest."""

    return f"legacy-state-handoff:{manifest.manifest_id}:review-plan"


def _handoff_event_id(manifest: ActorV2LegacyStateHandoffManifest) -> str:
    """Return token-free journal identity for the finalization decision."""

    return f"legacy-state-handoff:{manifest.manifest_id}:finalized"


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    """Require a JSON object without accepting string-like mappings."""

    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


def _strict_bool(value: object, field_name: str) -> bool:
    """Require a real boolean rather than truthy source data."""

    if not isinstance(value, bool):
        raise ActorV2LegacyIdleStateFinalizationBlocked(
            ("legacy_idle_materialization_shape_invalid",)
        )
    return value


def _required_text(value: object, field_name: str) -> str:
    """Normalize one non-empty identifier-like text field."""

    if not isinstance(value, str) or not (normalized := value.strip()):
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized


def _optional_text(value: object, field_name: str) -> str:
    """Normalize optional string content without coercing arbitrary values."""

    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    return value.strip()


def _nonnegative_finite(value: object, field_name: str) -> float:
    """Require one finite non-negative timestamp or duration."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"{field_name} must be a non-negative finite number")
    return numeric


def _canonical_json(value: object) -> str:
    """Serialize immutable handoff values with the repository canonical JSON form."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


__all__ = [
    "ActorV2LegacyIdleStateFinalization",
    "ActorV2LegacyIdleStateFinalizationBlocked",
    "SQLiteActorV2LegacyIdleStateFinalizer",
]
