"""Durable legacy source-state manifests for a future Actor v2 cutover.

The repository is intentionally a staging primitive.  It can capture and
materialize an immutable source snapshot only after the holder-fenced core
ingress drain is confirmed.  It does not write Actor aggregate tables, switch
ownership, or publish a target.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from collections.abc import Callable, Mapping
from sqlite3 import Connection, Row
from typing import Any

from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainRequest,
)
from shinbot.core.dispatch.actor_v2_legacy_state_handoff import (
    ActorV2LegacyStateHandoffConflict,
    ActorV2LegacyStateHandoffManifest,
    ActorV2LegacyStateHandoffMaterialization,
    ActorV2LegacyStateHandoffMaterializer,
    ActorV2LegacyStateHandoffNotFound,
    ActorV2LegacyStateHandoffScope,
    ActorV2LegacyStateHandoffScopeConflict,
    ActorV2LegacyStateHandoffSourceInvalid,
)
from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2MigrationBarrier,
    ActorV2MigrationBarrierGrant,
)
from shinbot.core.dispatch.agent_delivery import AgentRouteDelivery
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.persistence.canonical_json import validate_canonical_json_object
from shinbot.persistence.repositories.base import Repository


class ActorV2LegacyStateHandoffRepository(Repository):
    """Capture and stage one complete legacy source under a drained barrier."""

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        manifest_id_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize deterministic source-manifest persistence dependencies."""

        super().__init__(db)
        self._clock = clock or time.time
        self._manifest_id_factory = manifest_id_factory or (lambda: uuid.uuid4().hex)

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain shared by barrier, drain, and staging rows."""

        return self._db

    def capture(
        self,
        barrier_grant: ActorV2MigrationBarrierGrant,
    ) -> ActorV2LegacyStateHandoffManifest:
        """Persist one complete canonical source snapshot after core drain.

        Repeating the call with unchanged frozen source state returns the same
        manifest.  A changed source digest is a conflict rather than a second
        snapshot, because a barrier owns one irreversible source boundary.
        """

        if not isinstance(barrier_grant, ActorV2MigrationBarrierGrant):
            raise TypeError("barrier_grant must be an ActorV2MigrationBarrierGrant")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            barrier, drain = self._require_drained_boundary(conn, barrier_grant)
            scope = _load_scope(conn, barrier.legacy_session_id)
            _require_v1_scope(scope, barrier.key)
            source_payload = _capture_source_payload(
                conn,
                key=barrier.key,
                legacy_session_id=barrier.legacy_session_id,
            )
            candidate = ActorV2LegacyStateHandoffManifest.create(
                manifest_id=_identifier(self._manifest_id_factory(), "manifest_id"),
                barrier_id=barrier.barrier_id,
                core_ingress_drain_request_id=drain.request_id,
                key=barrier.key,
                scope=scope,
                legacy_session_id=barrier.legacy_session_id,
                source_generation=barrier.source_generation,
                migration_generation=barrier.migration_generation,
                source_payload=source_payload,
                core_ingress_digest=drain.core_ingress_proof_digest(),
                legacy_quiescence_digest=drain.legacy_quiescence_proof_digest(),
                captured_at=now,
            )
            existing = _load_manifest_for_barrier(conn, barrier.barrier_id)
            if existing is not None:
                if existing.source_digest != candidate.source_digest:
                    raise ActorV2LegacyStateHandoffConflict(
                        "legacy source changed after its handoff manifest was captured"
                    )
                return existing
            conn.execute(
                """
                INSERT INTO agent_session_actor_v2_legacy_state_handoff_manifests (
                    manifest_id, barrier_id, core_ingress_drain_request_id,
                    profile_id, session_id, legacy_session_id,
                    source_generation, migration_generation, manifest_version,
                    scope_json, source_payload_json, core_ingress_digest,
                    legacy_quiescence_digest, source_digest, captured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.manifest_id,
                    candidate.barrier_id,
                    candidate.core_ingress_drain_request_id,
                    candidate.key.profile_id,
                    candidate.key.session_id,
                    candidate.legacy_session_id,
                    candidate.source_generation,
                    candidate.migration_generation,
                    candidate.manifest_version,
                    _canonical_json(candidate.scope.to_payload()),
                    _canonical_json(candidate.source_payload_as_dict()),
                    candidate.core_ingress_digest,
                    candidate.legacy_quiescence_digest,
                    candidate.source_digest,
                    candidate.captured_at,
                ),
            )
            return candidate

    def get(self, manifest_id: str) -> ActorV2LegacyStateHandoffManifest | None:
        """Return one immutable source manifest by durable identity."""

        normalized_manifest_id = _identifier(manifest_id, "manifest_id")
        with self.connect() as conn:
            return _load_manifest(conn, normalized_manifest_id)

    def get_for_barrier(
        self,
        barrier_id: str,
    ) -> ActorV2LegacyStateHandoffManifest | None:
        """Return the unique source manifest associated with one barrier."""

        normalized_barrier_id = _identifier(barrier_id, "barrier_id")
        with self.connect() as conn:
            return _load_manifest_for_barrier(conn, normalized_barrier_id)

    def materialize(
        self,
        *,
        barrier_grant: ActorV2MigrationBarrierGrant,
        manifest_id: str,
        materializer: ActorV2LegacyStateHandoffMaterializer,
    ) -> ActorV2LegacyStateHandoffMaterialization:
        """Persist one deterministic Actor-side staging record for a manifest.

        ``materializer`` is required to be pure and side-effect free.  It runs
        while the repository holds the SQLite writer transaction so both its
        input boundary and persisted output are checked against the same live
        barrier and core-drain generation.
        """

        if not isinstance(barrier_grant, ActorV2MigrationBarrierGrant):
            raise TypeError("barrier_grant must be an ActorV2MigrationBarrierGrant")
        normalized_manifest_id = _identifier(manifest_id, "manifest_id")
        materializer_id, materializer_version, target_schema_version = (
            _materializer_identity(materializer)
        )
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            barrier, drain = self._require_drained_boundary(conn, barrier_grant)
            manifest = _load_required_manifest(conn, normalized_manifest_id)
            _require_manifest_boundary(manifest, barrier, drain)
            target_payload = materializer.materialize(manifest)
            candidate = ActorV2LegacyStateHandoffMaterialization.create(
                manifest_id=manifest.manifest_id,
                materializer_id=materializer_id,
                materializer_version=materializer_version,
                target_schema_version=target_schema_version,
                source_digest=manifest.source_digest,
                target_payload=target_payload,
                materialized_at=now,
            )
            existing = _load_materialization(
                conn,
                manifest_id=manifest.manifest_id,
                materializer_id=materializer_id,
                materializer_version=materializer_version,
            )
            if existing is not None:
                if (
                    existing.source_digest != candidate.source_digest
                    or existing.target_digest != candidate.target_digest
                    or existing.target_schema_version != candidate.target_schema_version
                ):
                    raise ActorV2LegacyStateHandoffConflict(
                        "materializer produced a different target for the same source manifest"
                    )
                return existing
            conn.execute(
                """
                INSERT INTO agent_session_actor_v2_legacy_state_handoff_materializations (
                    manifest_id, materializer_id, materializer_version,
                    target_schema_version, source_digest, target_payload_json,
                    target_digest, materialized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.manifest_id,
                    candidate.materializer_id,
                    candidate.materializer_version,
                    candidate.target_schema_version,
                    candidate.source_digest,
                    _canonical_json(candidate.target_payload_as_dict()),
                    candidate.target_digest,
                    candidate.materialized_at,
                ),
            )
            return candidate

    def list_materializations(
        self,
        manifest_id: str,
    ) -> tuple[ActorV2LegacyStateHandoffMaterialization, ...]:
        """Return all immutable target staging records for one source manifest."""

        normalized_manifest_id = _identifier(manifest_id, "manifest_id")
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM agent_session_actor_v2_legacy_state_handoff_materializations
                WHERE manifest_id = ?
                ORDER BY materializer_id, materializer_version
                """,
                (normalized_manifest_id,),
            ).fetchall()
            return tuple(_materialization_from_row(row) for row in rows)

    def require_materialization_for_finalization_in_transaction(
        self,
        conn: Connection,
        *,
        barrier_grant: ActorV2MigrationBarrierGrant,
        manifest_id: str,
        materializer_id: str,
        materializer_version: int,
        target_schema_version: int,
    ) -> tuple[
        ActorV2MigrationBarrier,
        ActorV2CoreIngressDrainRequest,
        ActorV2LegacyStateHandoffManifest,
        ActorV2LegacyStateHandoffMaterialization,
    ]:
        """Revalidate one exact staged target inside a finalizer transaction.

        This is deliberately narrower than a general activation API.  It proves
        that the holder capability, drain boundary, immutable source manifest,
        and one versioned materializer output all still describe the same
        migrating source while the caller retains the SQLite writer lock.
        """

        if not isinstance(barrier_grant, ActorV2MigrationBarrierGrant):
            raise TypeError("barrier_grant must be an ActorV2MigrationBarrierGrant")
        normalized_manifest_id = _identifier(manifest_id, "manifest_id")
        normalized_materializer_id = _identifier(materializer_id, "materializer_id")
        normalized_materializer_version = _positive_integer(
            materializer_version,
            "materializer_version",
        )
        normalized_target_schema_version = _positive_integer(
            target_schema_version,
            "target_schema_version",
        )
        barrier, drain = self._require_drained_boundary(conn, barrier_grant)
        manifest = _load_required_manifest(conn, normalized_manifest_id)
        _require_manifest_boundary(manifest, barrier, drain)
        materialization = _load_materialization(
            conn,
            manifest_id=manifest.manifest_id,
            materializer_id=normalized_materializer_id,
            materializer_version=normalized_materializer_version,
        )
        if materialization is None:
            raise ActorV2LegacyStateHandoffNotFound(
                "required legacy handoff materialization does not exist"
            )
        if (
            materialization.source_digest != manifest.source_digest
            or materialization.target_schema_version != normalized_target_schema_version
        ):
            raise ActorV2LegacyStateHandoffConflict(
                "legacy handoff materialization does not match finalizer contract"
            )
        return barrier, drain, manifest, materialization

    def _require_drained_boundary(
        self,
        conn: Connection,
        barrier_grant: ActorV2MigrationBarrierGrant,
    ) -> tuple[ActorV2MigrationBarrier, ActorV2CoreIngressDrainRequest]:
        """Validate the active barrier, migrating owner, and confirmed core drain."""

        barrier = self._db.actor_v2_migration_barriers.validate_in_transaction(
            conn,
            barrier_grant,
        )
        drain = self._db.actor_v2_core_ingress_drains.require_drained_for_barrier_in_transaction(
            conn,
            barrier_grant,
        )
        row = conn.execute(
            """
            SELECT legacy_session_id, mode, status, pending_mode, generation
            FROM agent_session_runtime_ownership
            WHERE profile_id = ? AND session_id = ?
            """,
            (barrier.key.profile_id, barrier.key.session_id),
        ).fetchone()
        if (
            row is None
            or str(row["legacy_session_id"]) != barrier.legacy_session_id
            or str(row["mode"]) != AgentRuntimeOwnershipMode.LEGACY.value
            or str(row["status"]) != AgentRuntimeOwnershipStatus.MIGRATING.value
            or str(row["pending_mode"]) != AgentRuntimeOwnershipMode.ACTOR_V2.value
            or int(row["generation"]) != barrier.migration_generation
        ):
            raise ActorV2LegacyStateHandoffConflict(
                "ownership no longer matches the active legacy source boundary"
            )
        return barrier, drain


def _load_scope(conn: Connection, legacy_session_id: str) -> ActorV2LegacyStateHandoffScope:
    """Load every current profile owner sharing the legacy scheduler namespace."""

    rows = conn.execute(
        """
        SELECT profile_id, session_id
        FROM agent_session_runtime_ownership
        WHERE legacy_session_id = ?
        ORDER BY profile_id, session_id
        """,
        (legacy_session_id,),
    ).fetchall()
    return ActorV2LegacyStateHandoffScope(
        legacy_session_id=legacy_session_id,
        members=tuple(
            SessionKey(str(row["profile_id"]), str(row["session_id"])) for row in rows
        ),
    )


def _require_v1_scope(
    scope: ActorV2LegacyStateHandoffScope,
    key: SessionKey,
) -> None:
    """Keep v1 fail-closed until multi-profile base-session transfer exists."""

    if not scope.is_single_owner or scope.members != (key,):
        raise ActorV2LegacyStateHandoffScopeConflict(scope)


def _capture_source_payload(
    conn: Connection,
    *,
    key: SessionKey,
    legacy_session_id: str,
) -> dict[str, object]:
    """Capture every legacy scheduling projection in stable durable order."""

    try:
        scheduler_row = conn.execute(
            """
            SELECT state, next_review_at, review_reason, mention_sensitivity,
                   active_reply_threshold_json, active_chat_state_json,
                   state_resume_json, updated_at
            FROM agent_scheduler_states
            WHERE session_id = ?
            """,
            (legacy_session_id,),
        ).fetchone()
        scheduler_state = (
            None
            if scheduler_row is None
            else {
                "state": _text(scheduler_row["state"]),
                "next_review_at": _optional_number(scheduler_row["next_review_at"]),
                "review_reason": _text(scheduler_row["review_reason"]),
                "mention_sensitivity": _text(scheduler_row["mention_sensitivity"]),
                "active_reply_threshold": _json_object(
                    scheduler_row["active_reply_threshold_json"],
                    "active_reply_threshold_json",
                ),
                "active_chat_state": _json_object(
                    scheduler_row["active_chat_state_json"],
                    "active_chat_state_json",
                ),
                "state_resume": _json_object(
                    scheduler_row["state_resume_json"],
                    "state_resume_json",
                ),
                "updated_at": _number(scheduler_row["updated_at"], "updated_at"),
            }
        )
        unread_messages = _capture_unread_messages(conn, legacy_session_id)
        return {
            "schema_version": 1,
            "scheduler_state": scheduler_state,
            "unread_messages": unread_messages,
            "route_deliveries": _capture_route_deliveries(
                conn,
                key=key,
                unread_messages=unread_messages,
            ),
            "unread_ranges": _capture_unread_ranges(conn, legacy_session_id),
            "high_priority_events": _capture_high_priority_events(conn, legacy_session_id),
            "recent_mentions": _capture_recent_mentions(conn, legacy_session_id),
            "review_summaries": _capture_review_summaries(conn, legacy_session_id),
            "summaries": _capture_summaries(conn, legacy_session_id),
        }
    except ActorV2LegacyStateHandoffSourceInvalid:
        raise
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActorV2LegacyStateHandoffSourceInvalid(
            "legacy scheduler state is not a canonical handoff source"
        ) from exc


def _capture_unread_messages(
    conn: Connection,
    legacy_session_id: str,
) -> list[dict[str, object]]:
    """Capture individual unread facts including their independent consumption flags."""

    rows = conn.execute(
        """
        SELECT id, message_log_id, sender_id, created_at, response_profile,
               is_mentioned, is_reply_to_bot, is_mention_to_other,
               is_poke_to_bot, is_poke_to_other, self_platform_id, trace_id,
               review_consumed, chat_consumed
        FROM agent_unread_messages
        WHERE session_id = ?
        ORDER BY id
        """,
        (legacy_session_id,),
    ).fetchall()
    return [
        {
            "id": _positive_integer(row["id"], "unread_messages.id"),
            "message_log_id": _positive_integer(
                row["message_log_id"],
                "unread_messages.message_log_id",
            ),
            "sender_id": _text(row["sender_id"]),
            "created_at": _number(row["created_at"], "unread_messages.created_at"),
            "response_profile": _text(row["response_profile"]),
            "is_mentioned": _flag(row["is_mentioned"], "unread_messages.is_mentioned"),
            "is_reply_to_bot": _flag(
                row["is_reply_to_bot"],
                "unread_messages.is_reply_to_bot",
            ),
            "is_mention_to_other": _flag(
                row["is_mention_to_other"],
                "unread_messages.is_mention_to_other",
            ),
            "is_poke_to_bot": _flag(row["is_poke_to_bot"], "unread_messages.is_poke_to_bot"),
            "is_poke_to_other": _flag(
                row["is_poke_to_other"],
                "unread_messages.is_poke_to_other",
            ),
            "self_platform_id": _text(row["self_platform_id"]),
            "trace_id": _text(row["trace_id"]),
            "review_consumed": _flag(
                row["review_consumed"],
                "unread_messages.review_consumed",
            ),
            "chat_consumed": _flag(row["chat_consumed"], "unread_messages.chat_consumed"),
        }
        for row in rows
    ]


def _capture_unread_ranges(
    conn: Connection,
    legacy_session_id: str,
) -> list[dict[str, object]]:
    """Capture legacy unread-range projection rows without recomputing them."""

    rows = conn.execute(
        """
        SELECT id, start_msg_log_id, end_msg_log_id, start_at, end_at,
               message_count, review_consumed, chat_consumed
        FROM agent_unread_ranges
        WHERE session_id = ?
        ORDER BY id
        """,
        (legacy_session_id,),
    ).fetchall()
    return [
        {
            "id": _positive_integer(row["id"], "unread_ranges.id"),
            "start_msg_log_id": _positive_integer(
                row["start_msg_log_id"],
                "unread_ranges.start_msg_log_id",
            ),
            "end_msg_log_id": _positive_integer(
                row["end_msg_log_id"],
                "unread_ranges.end_msg_log_id",
            ),
            "start_at": _number(row["start_at"], "unread_ranges.start_at"),
            "end_at": _number(row["end_at"], "unread_ranges.end_at"),
            "message_count": _nonnegative_integer(
                row["message_count"],
                "unread_ranges.message_count",
            ),
            "review_consumed": _flag(
                row["review_consumed"],
                "unread_ranges.review_consumed",
            ),
            "chat_consumed": _flag(row["chat_consumed"], "unread_ranges.chat_consumed"),
        }
        for row in rows
    ]


def _capture_route_deliveries(
    conn: Connection,
    *,
    key: SessionKey,
    unread_messages: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Freeze exact durable delivery coverage for every legacy unread message.

    Legacy scheduler rows do not carry the canonical route-delivery identity
    needed by Actor v2's message ledger.  When the durable outbox has retained
    that identity, it is captured as a verified mailbox payload.  Old rows with
    no outbox evidence remain explicitly ``missing``; a future finalizer must
    reject that status instead of synthesizing an event identity.
    """

    evidence: list[dict[str, object]] = []
    for unread in unread_messages:
        message_log_id = _positive_integer(
            unread.get("message_log_id"),
            "route_deliveries.message_log_id",
        )
        rows = conn.execute(
            """
            SELECT delivery_id, event_id, payload_json, payload_digest
            FROM agent_route_outbox
            WHERE profile_id = ? AND session_id = ? AND message_log_id = ?
            ORDER BY outbox_seq
            """,
            (key.profile_id, key.session_id, message_log_id),
        ).fetchall()
        if not rows:
            evidence.append(
                {
                    "message_log_id": message_log_id,
                    "status": "missing",
                }
            )
            continue
        candidates: dict[str, dict[str, object]] = {}
        event_ids: set[str] = set()
        for row in rows:
            payload_json = _text(row["payload_json"])
            validation = validate_canonical_json_object(payload_json)
            if (
                validation.payload is None
                or validation.canonical_json is None
                or validation.violations
                or hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
                != _text(row["payload_digest"])
            ):
                raise ActorV2LegacyStateHandoffSourceInvalid(
                    "legacy route delivery evidence is not canonical"
                )
            try:
                delivery = AgentRouteDelivery.from_payload(validation.payload)
            except (TypeError, ValueError) as exc:
                raise ActorV2LegacyStateHandoffSourceInvalid(
                    "legacy route delivery evidence cannot be verified"
                ) from exc
            if (
                delivery.session_key != key
                or delivery.message_log_id != message_log_id
                or delivery.delivery_id != _text(row["delivery_id"])
                or delivery.event_id != _text(row["event_id"])
            ):
                raise ActorV2LegacyStateHandoffSourceInvalid(
                    "legacy route delivery evidence differs from its outbox identity"
                )
            mailbox_payload = delivery.to_mailbox_payload()
            candidates[_canonical_json(mailbox_payload)] = mailbox_payload
            event_ids.add(delivery.event_id)
        if len(candidates) == 1:
            evidence.append(
                {
                    "message_log_id": message_log_id,
                    "status": "verified",
                    "mailbox_payload": next(iter(candidates.values())),
                }
            )
        else:
            evidence.append(
                {
                    "message_log_id": message_log_id,
                    "status": "ambiguous",
                    "event_ids": sorted(event_ids),
                }
            )
    return evidence


def _capture_high_priority_events(
    conn: Connection,
    legacy_session_id: str,
) -> list[dict[str, object]]:
    """Capture both pending and handled high-priority event history."""

    rows = conn.execute(
        """
        SELECT id, message_log_id, sender_id, kind, reason, created_at, handled
        FROM agent_high_priority_events
        WHERE session_id = ?
        ORDER BY id
        """,
        (legacy_session_id,),
    ).fetchall()
    return [
        {
            "id": _positive_integer(row["id"], "high_priority_events.id"),
            "message_log_id": _positive_integer(
                row["message_log_id"],
                "high_priority_events.message_log_id",
            ),
            "sender_id": _text(row["sender_id"]),
            "kind": _text(row["kind"]),
            "reason": _text(row["reason"]),
            "created_at": _number(
                row["created_at"],
                "high_priority_events.created_at",
            ),
            "handled": _flag(row["handled"], "high_priority_events.handled"),
        }
        for row in rows
    ]


def _capture_recent_mentions(
    conn: Connection,
    legacy_session_id: str,
) -> list[dict[str, object]]:
    """Capture every recent-mention timestamp retained by the legacy policy."""

    rows = conn.execute(
        """
        SELECT id, timestamp
        FROM agent_recent_mentions
        WHERE session_id = ?
        ORDER BY id
        """,
        (legacy_session_id,),
    ).fetchall()
    return [
        {
            "id": _positive_integer(row["id"], "recent_mentions.id"),
            "timestamp": _number(row["timestamp"], "recent_mentions.timestamp"),
        }
        for row in rows
    ]


def _capture_review_summaries(
    conn: Connection,
    legacy_session_id: str,
) -> list[dict[str, object]]:
    """Capture review-summary context including selected candidate identities."""

    rows = conn.execute(
        """
        SELECT id, start_msg_log_id, end_msg_log_id, start_at, end_at,
               message_count, summary, candidate_message_ids_json, reason, created_at
        FROM agent_review_summaries
        WHERE session_id = ?
        ORDER BY id
        """,
        (legacy_session_id,),
    ).fetchall()
    return [
        {
            "id": _positive_integer(row["id"], "review_summaries.id"),
            "start_msg_log_id": _positive_integer(
                row["start_msg_log_id"],
                "review_summaries.start_msg_log_id",
            ),
            "end_msg_log_id": _positive_integer(
                row["end_msg_log_id"],
                "review_summaries.end_msg_log_id",
            ),
            "start_at": _number(row["start_at"], "review_summaries.start_at"),
            "end_at": _number(row["end_at"], "review_summaries.end_at"),
            "message_count": _nonnegative_integer(
                row["message_count"],
                "review_summaries.message_count",
            ),
            "summary": _text(row["summary"]),
            "candidate_message_ids": _json_array(
                row["candidate_message_ids_json"],
                "candidate_message_ids_json",
            ),
            "reason": _text(row["reason"]),
            "created_at": _number(row["created_at"], "review_summaries.created_at"),
        }
        for row in rows
    ]


def _capture_summaries(
    conn: Connection,
    legacy_session_id: str,
) -> list[dict[str, object]]:
    """Capture prompt/review summary content and its message-log bounds."""

    rows = conn.execute(
        """
        SELECT id, summary_type, content, source_run_id, msg_log_start,
               msg_log_end, metadata_json, created_at
        FROM agent_summaries
        WHERE session_id = ?
        ORDER BY id
        """,
        (legacy_session_id,),
    ).fetchall()
    return [
        {
            "id": _positive_integer(row["id"], "summaries.id"),
            "summary_type": _text(row["summary_type"]),
            "content": _text(row["content"]),
            "source_run_id": _text(row["source_run_id"]),
            "msg_log_start": _optional_positive_integer(
                row["msg_log_start"],
                "summaries.msg_log_start",
            ),
            "msg_log_end": _optional_positive_integer(
                row["msg_log_end"],
                "summaries.msg_log_end",
            ),
            "metadata": _json_object(row["metadata_json"], "metadata_json"),
            "created_at": _number(row["created_at"], "summaries.created_at"),
        }
        for row in rows
    ]


def _load_manifest(
    conn: Connection,
    manifest_id: str,
) -> ActorV2LegacyStateHandoffManifest | None:
    """Load one manifest by id and validate its canonical persisted payloads."""

    row = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_legacy_state_handoff_manifests
        WHERE manifest_id = ?
        """,
        (manifest_id,),
    ).fetchone()
    return _manifest_from_row(row) if row is not None else None


def _load_required_manifest(
    conn: Connection,
    manifest_id: str,
) -> ActorV2LegacyStateHandoffManifest:
    """Load one immutable manifest or fail closed on absent history."""

    manifest = _load_manifest(conn, manifest_id)
    if manifest is None:
        raise ActorV2LegacyStateHandoffNotFound("legacy source-state handoff manifest does not exist")
    return manifest


def _load_manifest_for_barrier(
    conn: Connection,
    barrier_id: str,
) -> ActorV2LegacyStateHandoffManifest | None:
    """Load the one manifest associated with an active or historical barrier."""

    row = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_legacy_state_handoff_manifests
        WHERE barrier_id = ?
        """,
        (barrier_id,),
    ).fetchone()
    return _manifest_from_row(row) if row is not None else None


def _manifest_from_row(row: Row) -> ActorV2LegacyStateHandoffManifest:
    """Decode one row and reverify every identity-bound source digest."""

    try:
        scope_payload = _canonical_object(row["scope_json"], "scope_json")
        source_payload = _canonical_object(row["source_payload_json"], "source_payload_json")
        return ActorV2LegacyStateHandoffManifest(
            manifest_id=str(row["manifest_id"]),
            barrier_id=str(row["barrier_id"]),
            core_ingress_drain_request_id=str(row["core_ingress_drain_request_id"]),
            key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
            scope=ActorV2LegacyStateHandoffScope.from_payload(scope_payload),
            legacy_session_id=str(row["legacy_session_id"]),
            source_generation=int(row["source_generation"]),
            migration_generation=int(row["migration_generation"]),
            manifest_version=int(row["manifest_version"]),
            source_payload=source_payload,
            core_ingress_digest=str(row["core_ingress_digest"]),
            legacy_quiescence_digest=str(row["legacy_quiescence_digest"]),
            source_digest=str(row["source_digest"]),
            captured_at=float(row["captured_at"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActorV2LegacyStateHandoffConflict(
            "legacy handoff manifest contains invalid durable state"
        ) from exc


def _require_manifest_boundary(
    manifest: ActorV2LegacyStateHandoffManifest,
    barrier: ActorV2MigrationBarrier,
    drain: ActorV2CoreIngressDrainRequest,
) -> None:
    """Require manifest identity and proof digests to match the live boundary."""

    if (
        manifest.key != barrier.key
        or manifest.barrier_id != barrier.barrier_id
        or manifest.legacy_session_id != barrier.legacy_session_id
        or manifest.source_generation != barrier.source_generation
        or manifest.migration_generation != barrier.migration_generation
        or manifest.core_ingress_drain_request_id != drain.request_id
        or manifest.core_ingress_digest != drain.core_ingress_proof_digest()
        or manifest.legacy_quiescence_digest
        != drain.legacy_quiescence_proof_digest()
    ):
        raise ActorV2LegacyStateHandoffConflict(
            "legacy handoff manifest does not match the active drain boundary"
        )


def _load_materialization(
    conn: Connection,
    *,
    manifest_id: str,
    materializer_id: str,
    materializer_version: int,
) -> ActorV2LegacyStateHandoffMaterialization | None:
    """Load one exact immutable materializer output, if already persisted."""

    row = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_legacy_state_handoff_materializations
        WHERE manifest_id = ? AND materializer_id = ? AND materializer_version = ?
        """,
        (manifest_id, materializer_id, materializer_version),
    ).fetchone()
    return _materialization_from_row(row) if row is not None else None


def _materialization_from_row(row: Row) -> ActorV2LegacyStateHandoffMaterialization:
    """Decode one persisted target staging record and check its target digest."""

    try:
        return ActorV2LegacyStateHandoffMaterialization(
            manifest_id=str(row["manifest_id"]),
            materializer_id=str(row["materializer_id"]),
            materializer_version=int(row["materializer_version"]),
            target_schema_version=int(row["target_schema_version"]),
            source_digest=str(row["source_digest"]),
            target_payload=_canonical_object(row["target_payload_json"], "target_payload_json"),
            target_digest=str(row["target_digest"]),
            materialized_at=float(row["materialized_at"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActorV2LegacyStateHandoffConflict(
            "legacy handoff materialization contains invalid durable state"
        ) from exc


def _materializer_identity(
    materializer: ActorV2LegacyStateHandoffMaterializer,
) -> tuple[str, int, int]:
    """Validate the narrow pure-materializer surface before transaction work."""

    if not callable(getattr(materializer, "materialize", None)):
        raise TypeError("materializer must provide a callable materialize method")
    return (
        _identifier(getattr(materializer, "materializer_id", None), "materializer_id"),
        _positive_integer(
            getattr(materializer, "materializer_version", None),
            "materializer_version",
        ),
        _positive_integer(
            getattr(materializer, "target_schema_version", None),
            "target_schema_version",
        ),
    )


def _canonical_object(value: object, field_name: str) -> dict[str, Any]:
    """Decode a persisted canonical JSON object or fail closed on drift."""

    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be JSON text")
    validation = validate_canonical_json_object(value)
    if validation.payload is None or validation.violations:
        raise ValueError(f"{field_name} is not canonical JSON")
    return validation.payload


def _canonical_json(value: Mapping[str, object]) -> str:
    """Encode one canonical object compatible with startup schema validation."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_object(value: object, field_name: str) -> dict[str, Any]:
    """Decode one source JSON object without accepting invalid scheduler state."""

    decoded = _decode_json(value, field_name)
    if not isinstance(decoded, dict):
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} must be a JSON object"
        )
    return decoded


def _json_array(value: object, field_name: str) -> list[Any]:
    """Decode one source JSON array without coercing malformed summary data."""

    decoded = _decode_json(value, field_name)
    if not isinstance(decoded, list):
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} must be a JSON array"
        )
    return decoded


def _decode_json(value: object, field_name: str) -> object:
    """Parse bounded JSON source text and reject non-finite constants."""

    if not isinstance(value, str):
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} must be JSON text"
        )
    try:
        return json.loads(value, parse_constant=_reject_json_constant)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} is invalid JSON"
        ) from exc


def _reject_json_constant(value: str) -> None:
    """Reject NaN and infinity values that cannot enter canonical staging data."""

    raise ValueError(f"invalid JSON constant: {value}")


def _identifier(value: object, field_name: str) -> str:
    """Normalize one required repository identity."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _text(value: object) -> str:
    """Return a stored text column without silently coercing bytes or null."""

    if not isinstance(value, str):
        raise ActorV2LegacyStateHandoffSourceInvalid("legacy source contains non-text data")
    return value


def _positive_integer(value: object, field_name: str) -> int:
    """Require one positive non-boolean persisted identifier or version."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} must be a positive integer"
        )
    return value


def _nonnegative_integer(value: object, field_name: str) -> int:
    """Require one non-negative non-boolean persisted count."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} must be a non-negative integer"
        )
    return value


def _optional_positive_integer(value: object, field_name: str) -> int | None:
    """Normalize one optional positive message-log reference."""

    return None if value is None else _positive_integer(value, field_name)


def _number(value: object, field_name: str) -> float:
    """Normalize one finite scheduler timestamp or scalar number."""

    if isinstance(value, bool):
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} must be finite"
        )
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} must be finite"
        )
    return numeric


def _optional_number(value: object) -> float | None:
    """Normalize an optional finite schedule timestamp."""

    return None if value is None else _number(value, "next_review_at")


def _flag(value: object, field_name: str) -> bool:
    """Require SQLite's exact 0/1 boolean representation from legacy rows."""

    if value not in (0, 1, False, True):
        raise ActorV2LegacyStateHandoffSourceInvalid(
            f"legacy {field_name} must be a 0/1 flag"
        )
    return bool(value)


def _finite_time(value: object, field_name: str) -> float:
    """Normalize the repository clock before durable writes."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field_name} must be finite")
    return numeric


__all__ = ["ActorV2LegacyStateHandoffRepository"]
