"""Fenced legacy-to-Actor migration barrier for a future cutover controller.

The repository starts one durable ``ownership=migrating`` barrier and retains a
holder capability for its only legal abort path.  It deliberately has no
complete/activate method: publishing and supervising the Actor target remains
outside this unmounted primitive.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
import uuid
from collections.abc import Callable, Sequence
from sqlite3 import Connection, Row

from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2LegacyStateHandoffRequired,
    ActorV2MigrationBarrier,
    ActorV2MigrationBarrierAbortResult,
    ActorV2MigrationBarrierConflict,
    ActorV2MigrationBarrierGrant,
    ActorV2MigrationBarrierLost,
    ActorV2MigrationBarrierNotFound,
    ActorV2MigrationBarrierStatus,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnership,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipStatus,
)
from shinbot.persistence.repositories.base import Repository


class ActorV2MigrationBarrierRepository(Repository):
    """Own a holder-fenced legacy-to-Actor migration barrier.

    A barrier has no automatic expiry.  If its holder disappears, the
    ownership row remains ``migrating`` and durable routing stays deferred
    until an explicit controller recovery can prove how to stop or restore the
    source.  This availability loss is intentional.
    """

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
        barrier_id_factory: Callable[[], str] | None = None,
        holder_token_factory: Callable[[], str] | None = None,
    ) -> None:
        """Initialize durable barrier identities with injectable test factories."""

        super().__init__(db)
        self._clock = clock or time.time
        self._barrier_id_factory = barrier_id_factory or (lambda: uuid.uuid4().hex)
        self._holder_token_factory = holder_token_factory or (lambda: uuid.uuid4().hex)

    @property
    def persistence_domain(self) -> object:
        """Return the exact database domain shared by ownership and barrier rows."""

        return self._db

    def start_legacy_to_actor_v2(
        self,
        key: SessionKey,
        *,
        expected_generation: int,
        adapter_instance_ids: Sequence[str],
        holder_id: str,
        reason: str,
        requested_by: str = "",
    ) -> ActorV2MigrationBarrierGrant:
        """Atomically fence broad recovery and start one holder-owned migration.

        Only an active legacy owner may enter this barrier.  The same SQLite
        write transaction first closes broad legacy recovery, changes ownership
        to ``migrating`` with pending Actor v2 mode, refences routing work, and
        persists the opaque-holder barrier.  No production caller invokes this
        method yet.
        """

        if not isinstance(key, SessionKey):
            raise TypeError("key must be a SessionKey")
        source_generation = _positive_generation(expected_generation, "expected_generation")
        adapters = _adapter_instance_ids(adapter_instance_ids)
        holder = _identifier(holder_id, "holder_id")
        normalized_reason = _identifier(reason, "reason")
        requester = str(requested_by or "").strip()
        barrier_id = _identifier(self._barrier_id_factory(), "barrier_id")
        token = _identifier(self._holder_token_factory(), "holder_token")
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_no_history_in_transaction(conn, key)
            self._require_active_legacy_source_in_transaction(
                conn,
                key,
                expected_generation=source_generation,
            )
            self._require_legacy_source_handoff_preflight_in_transaction(
                conn,
                key,
            )
            self._db.actor_v2_legacy_recovery_gate.enter_fenced_only_in_transaction(
                conn
            )
            migrating = self._db.agent_runtime_ownership.begin_migration_in_transaction(
                conn,
                key,
                AgentRuntimeOwnershipMode.ACTOR_V2,
                expected_generation=source_generation,
                reason=normalized_reason,
                requested_by=requester or holder,
                now=now,
            )
            if (
                migrating.mode is not AgentRuntimeOwnershipMode.LEGACY
                or migrating.status is not AgentRuntimeOwnershipStatus.MIGRATING
                or migrating.pending_mode is not AgentRuntimeOwnershipMode.ACTOR_V2
                or migrating.generation != source_generation + 1
            ):
                raise ActorV2MigrationBarrierConflict(
                    "ownership transition did not create the expected Actor v2 barrier"
                )
            conn.execute(
                """
                INSERT INTO agent_session_actor_v2_migration_barriers (
                    profile_id, session_id, barrier_id, legacy_session_id,
                    adapter_instance_ids_json, source_generation,
                    migration_generation, holder_id, holder_token_digest, status,
                    created_at, updated_at, aborted_at, abort_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'migrating', ?, ?, NULL, '')
                """,
                (
                    key.profile_id,
                    key.session_id,
                    barrier_id,
                    migrating.legacy_session_id,
                    _encode_adapter_instance_ids(adapters),
                    source_generation,
                    migrating.generation,
                    holder,
                    _token_digest(token),
                    now,
                    now,
                ),
            )
            barrier = _load_required_barrier(conn, key)
        return ActorV2MigrationBarrierGrant(barrier=barrier, holder_token=token)

    def get(self, key: SessionKey) -> ActorV2MigrationBarrier | None:
        """Return one token-free barrier snapshot by stable session key."""

        if not isinstance(key, SessionKey):
            raise TypeError("key must be a SessionKey")
        with self.connect() as conn:
            return _load_barrier(conn, key)

    def validate(
        self,
        grant: ActorV2MigrationBarrierGrant,
    ) -> ActorV2MigrationBarrier:
        """Require that one holder capability still names the active barrier."""

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self.validate_in_transaction(conn, grant)

    def validate_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2MigrationBarrierGrant,
    ) -> ActorV2MigrationBarrier:
        """Validate an active holder capability inside a caller transaction."""

        if not isinstance(grant, ActorV2MigrationBarrierGrant):
            raise TypeError("grant must be an ActorV2MigrationBarrierGrant")
        current = _load_required_barrier(conn, grant.barrier.key)
        if (
            not current.active
            or current.barrier_id != grant.barrier.barrier_id
            or current.source_generation != grant.barrier.source_generation
            or current.migration_generation != grant.barrier.migration_generation
            or current.holder_id != grant.barrier.holder_id
        ):
            raise ActorV2MigrationBarrierLost(
                "migration barrier no longer matches the active holder epoch"
            )
        row = _select_barrier(conn, current.key)
        if row is None or str(row["holder_token_digest"]) != _token_digest(grant.holder_token):
            raise ActorV2MigrationBarrierLost(
                "migration barrier holder capability no longer matches"
            )
        return current

    def abort(
        self,
        grant: ActorV2MigrationBarrierGrant,
        *,
        reason: str,
        requested_by: str = "",
    ) -> ActorV2MigrationBarrierAbortResult:
        """Restore the source legacy owner through the exact barrier holder.

        This is a controlled rollback of the ownership barrier only.  A caller
        must separately thaw all local freezes and source-side pause state
        before it invokes this method.  The durable row remains terminal so a
        later controller cannot silently reuse the old cutover identity.
        """

        normalized_reason = _identifier(reason, "reason")
        requester = str(requested_by or "").strip()
        now = _finite_time(self._clock(), "clock")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            barrier = self.validate_in_transaction(conn, grant)
            self._require_no_core_ingress_drain_in_transaction(conn, barrier.barrier_id)
            ownership = self._db.agent_runtime_ownership.abort_migration_in_transaction(
                conn,
                barrier.key,
                expected_generation=barrier.migration_generation,
                reason=normalized_reason,
                requested_by=requester or barrier.holder_id,
                now=now,
            )
            updated = conn.execute(
                """
                UPDATE agent_session_actor_v2_migration_barriers
                SET status = 'aborted', updated_at = ?, aborted_at = ?, abort_reason = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND barrier_id = ?
                  AND status = 'migrating'
                  AND holder_token_digest = ?
                """,
                (
                    now,
                    now,
                    normalized_reason,
                    barrier.key.profile_id,
                    barrier.key.session_id,
                    barrier.barrier_id,
                    _token_digest(grant.holder_token),
                ),
            )
            if updated.rowcount != 1:
                raise ActorV2MigrationBarrierLost(
                    "migration barrier changed while its holder was aborting"
                )
            terminal = _load_required_barrier(conn, barrier.key)
        return ActorV2MigrationBarrierAbortResult(
            barrier=terminal,
            ownership=ownership,
        )

    def complete_legacy_state_handoff_in_transaction(
        self,
        conn: Connection,
        grant: ActorV2MigrationBarrierGrant,
        *,
        manifest_id: str,
        materializer_id: str,
        materializer_version: int,
        target_schema_version: int,
        source_digest: str,
        target_digest: str,
        ownership: AgentRuntimeOwnership,
        reason: str,
        requested_by: str = "",
        now: float | None = None,
    ) -> ActorV2MigrationBarrier:
        """Seal a holder-owned source barrier after one atomic target commit.

        The original barrier row remains immutable source-boundary evidence.
        Completion is represented by an immutable sidecar that names the exact
        manifest and materializer output consumed by the Actor target.  This
        avoids a second mutable lifecycle authority while making the effective
        barrier state terminal for all repository callers.
        """

        barrier = self.validate_in_transaction(conn, grant)
        normalized_manifest_id = _identifier(manifest_id, "manifest_id")
        normalized_materializer_id = _identifier(materializer_id, "materializer_id")
        normalized_materializer_version = _positive_generation(
            materializer_version,
            "materializer_version",
        )
        normalized_target_schema_version = _positive_generation(
            target_schema_version,
            "target_schema_version",
        )
        normalized_source_digest = _digest(source_digest, "source_digest")
        normalized_target_digest = _digest(target_digest, "target_digest")
        normalized_reason = _identifier(reason, "completion_reason")
        requester = str(requested_by or "").strip()
        completed_at = _finite_time(self._clock() if now is None else now, "completed_at")
        if not isinstance(ownership, AgentRuntimeOwnership):
            raise TypeError("ownership must be an AgentRuntimeOwnership")
        if (
            ownership.key != barrier.key
            or ownership.legacy_session_id != barrier.legacy_session_id
            or ownership.mode is not AgentRuntimeOwnershipMode.ACTOR_V2
            or ownership.status is not AgentRuntimeOwnershipStatus.ACTIVE
            or ownership.generation != barrier.migration_generation + 1
        ):
            raise ActorV2MigrationBarrierConflict(
                "handoff completion requires the immediate active Actor v2 owner"
            )
        existing = _select_completion_for_barrier(conn, barrier.barrier_id)
        if existing is not None:
            raise ActorV2MigrationBarrierLost(
                "migration barrier already has immutable handoff completion evidence"
            )
        conn.execute(
            """
            INSERT INTO agent_session_actor_v2_legacy_state_handoff_finalizations (
                barrier_id, manifest_id, materializer_id, materializer_version,
                target_schema_version, source_digest, target_digest,
                ownership_generation, completion_reason, requested_by, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                barrier.barrier_id,
                normalized_manifest_id,
                normalized_materializer_id,
                normalized_materializer_version,
                normalized_target_schema_version,
                normalized_source_digest,
                normalized_target_digest,
                ownership.generation,
                normalized_reason,
                requester,
                completed_at,
            ),
        )
        terminal = _load_required_barrier(conn, barrier.key)
        if terminal.status is not ActorV2MigrationBarrierStatus.COMPLETED:
            raise ActorV2MigrationBarrierConflict(
                "handoff completion sidecar did not produce a terminal barrier"
            )
        return terminal

    def require_no_active_for_key_in_transaction(
        self,
        conn: Connection,
        key: SessionKey,
    ) -> None:
        """Reject generic ownership operations while a barrier owns migration."""

        if not isinstance(key, SessionKey):
            raise TypeError("key must be a SessionKey")
        barrier = _load_barrier(conn, key)
        if barrier is None:
            return
        if barrier.active:
            raise ActorV2MigrationBarrierConflict(
                "active Actor v2 migration barrier requires its holder capability"
            )

    @staticmethod
    def _require_no_core_ingress_drain_in_transaction(
        conn: Connection,
        barrier_id: str,
    ) -> None:
        """Keep abort from orphaning a sealed core-ingress freeze snapshot.

        A future controller needs an explicit thaw-and-abort protocol for a
        started core drain.  The existing ownership-only abort path cannot
        prove that local freezes were released, so it remains available only
        before a core request is created.
        """

        row = conn.execute(
            """
            SELECT request_id
            FROM agent_session_actor_v2_core_ingress_drain_requests
            WHERE barrier_id = ?
            LIMIT 1
            """,
            (barrier_id,),
        ).fetchone()
        if row is not None:
            raise ActorV2MigrationBarrierConflict(
                "migration barrier cannot abort after core ingress drain request "
                + str(row["request_id"])
            )

    @staticmethod
    def _require_no_history_in_transaction(conn: Connection, key: SessionKey) -> None:
        """Keep one session's barrier history non-reusable after any attempt."""

        if _select_barrier(conn, key) is not None:
            raise ActorV2MigrationBarrierConflict(
                "Actor v2 migration barrier history already exists for this session"
            )

    @staticmethod
    def _require_active_legacy_source_in_transaction(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        """Require the exact active legacy source before an irreversible fence."""

        row = conn.execute(
            """
            SELECT mode, status, generation, admission_fence_id, admission_fence_generation
            FROM agent_session_runtime_ownership
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        if row is None:
            raise ActorV2MigrationBarrierNotFound("legacy ownership source does not exist")
        if (
            str(row["mode"]) != AgentRuntimeOwnershipMode.LEGACY.value
            or str(row["status"]) != AgentRuntimeOwnershipStatus.ACTIVE.value
            or int(row["generation"]) != expected_generation
            or str(row["admission_fence_id"]) != ""
            or int(row["admission_fence_generation"]) != 0
        ):
            raise ActorV2MigrationBarrierConflict(
                "migration barrier requires the exact active unfenced legacy source"
            )

    @staticmethod
    def _require_legacy_source_handoff_preflight_in_transaction(
        conn: Connection,
        key: SessionKey,
    ) -> None:
        """Reject source state that no versioned migration manifest can transfer.

        The current Actor v2 reducer has no semantic mapping for legacy review
        plans, active-chat state, unread queues, or prompt summaries.  Starting
        a barrier in their presence would freeze ingress and later fail at
        ownership completion, leaving an avoidable stuck migration.  This
        check runs in the same writer transaction as the ownership transition.
        """

        source = conn.execute(
            """
            SELECT legacy_session_id
            FROM agent_session_runtime_ownership
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        if source is None:
            raise ActorV2MigrationBarrierNotFound("legacy ownership source does not exist")
        legacy_session_id = str(source["legacy_session_id"])
        evidence = _legacy_source_handoff_evidence(conn, legacy_session_id)
        other_owners = conn.execute(
            """
            SELECT profile_id, session_id
            FROM agent_session_runtime_ownership
            WHERE legacy_session_id = ?
              AND NOT (profile_id = ? AND session_id = ?)
            ORDER BY profile_id, session_id
            """,
            (legacy_session_id, key.profile_id, key.session_id),
        ).fetchall()
        evidence.extend(
            "shared_legacy_session_owner:"
            + str(row["profile_id"])
            + ":"
            + str(row["session_id"])
            for row in other_owners
        )
        if evidence:
            raise ActorV2LegacyStateHandoffRequired(tuple(evidence))


def _select_barrier(conn: Connection, key: SessionKey) -> Row | None:
    """Select one barrier row inside an existing transaction."""

    return conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_migration_barriers
        WHERE profile_id = ? AND session_id = ?
        """,
        (key.profile_id, key.session_id),
    ).fetchone()


def _legacy_source_handoff_evidence(
    conn: Connection,
    legacy_session_id: str,
) -> list[str]:
    """Return stable classes of legacy state that require a future manifest."""

    evidence: list[str] = []
    row = conn.execute(
        """
        SELECT state, next_review_at, review_reason, mention_sensitivity,
               active_reply_threshold_json, active_chat_state_json,
               state_resume_json
        FROM agent_scheduler_states
        WHERE session_id = ?
        """,
        (legacy_session_id,),
    ).fetchone()
    if row is not None and not _is_empty_legacy_scheduler_state(row):
        evidence.append("legacy_scheduler_state")
    for table_name, label in (
        ("agent_unread_messages", "legacy_unread_messages"),
        ("agent_unread_ranges", "legacy_unread_ranges"),
        ("agent_high_priority_events", "legacy_high_priority_events"),
        ("agent_recent_mentions", "legacy_recent_mentions"),
        ("agent_review_summaries", "legacy_review_summaries"),
        ("agent_summaries", "legacy_summaries"),
    ):
        row = conn.execute(
            f"SELECT 1 FROM {table_name} WHERE session_id = ? LIMIT 1",
            (legacy_session_id,),
        ).fetchone()
        if row is not None:
            evidence.append(label)
    return evidence


def _is_empty_legacy_scheduler_state(row: Row) -> bool:
    """Recognize a default idle scheduler row that carries no future decision."""

    try:
        active_reply_threshold = json.loads(str(row["active_reply_threshold_json"]))
        active_chat_state = json.loads(str(row["active_chat_state_json"]))
        state_resume = json.loads(str(row["state_resume_json"]))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False
    return (
        str(row["state"]) == "idle"
        and row["next_review_at"] is None
        and str(row["review_reason"]) == ""
        and str(row["mention_sensitivity"]) == "normal"
        and active_reply_threshold == {}
        and active_chat_state == {}
        and state_resume == {}
    )


def _load_required_barrier(conn: Connection, key: SessionKey) -> ActorV2MigrationBarrier:
    """Load one barrier snapshot or fail closed on missing durable state."""

    barrier = _load_barrier(conn, key)
    if barrier is None:
        raise ActorV2MigrationBarrierNotFound("migration barrier does not exist")
    return barrier


def _load_barrier(
    conn: Connection,
    key: SessionKey,
) -> ActorV2MigrationBarrier | None:
    """Load a barrier and project any immutable handoff completion sidecar."""

    row = _select_barrier(conn, key)
    if row is None:
        return None
    return _barrier_from_row(
        row,
        completion=_select_completion_for_barrier(conn, str(row["barrier_id"])),
    )


def _barrier_from_row(
    row: Row,
    *,
    completion: Row | None = None,
) -> ActorV2MigrationBarrier:
    """Decode one token-free barrier row without exposing its holder digest."""

    try:
        raw_status = ActorV2MigrationBarrierStatus(str(row["status"]))
        completed_at = (
            float(completion["completed_at"]) if completion is not None else None
        )
        completion_reason = (
            str(completion["completion_reason"]) if completion is not None else ""
        )
        completion_manifest_id = (
            str(completion["manifest_id"]) if completion is not None else ""
        )
        return ActorV2MigrationBarrier(
            key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
            barrier_id=str(row["barrier_id"]),
            legacy_session_id=str(row["legacy_session_id"]),
            adapter_instance_ids=_decode_adapter_instance_ids(
                row["adapter_instance_ids_json"]
            ),
            source_generation=int(row["source_generation"]),
            migration_generation=int(row["migration_generation"]),
            status=(
                ActorV2MigrationBarrierStatus.COMPLETED
                if completion is not None
                else raw_status
            ),
            holder_id=str(row["holder_id"]),
            created_at=float(row["created_at"]),
            updated_at=(completed_at if completed_at is not None else float(row["updated_at"])),
            aborted_at=(
                None
                if completion is not None
                else (float(row["aborted_at"]) if row["aborted_at"] is not None else None)
            ),
            abort_reason="" if completion is not None else str(row["abort_reason"]),
            completed_at=completed_at,
            completion_reason=completion_reason,
            completion_manifest_id=completion_manifest_id,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActorV2MigrationBarrierConflict(
            "migration barrier contains invalid durable state"
        ) from exc


def _select_completion_for_barrier(
    conn: Connection,
    barrier_id: str,
) -> Row | None:
    """Return the one immutable finalization sidecar for a source barrier."""

    return conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_legacy_state_handoff_finalizations
        WHERE barrier_id = ?
        """,
        (barrier_id,),
    ).fetchone()


def _identifier(value: object, field_name: str) -> str:
    """Normalize one required opaque durable identifier."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _digest(value: object, field_name: str) -> str:
    """Normalize one canonical SHA-256 digest used by immutable handoff proof."""

    normalized = str(value or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return normalized


def _positive_generation(value: object, field_name: str) -> int:
    """Require one positive non-boolean ownership generation."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be positive")
    return value


def _finite_time(value: object, field_name: str) -> float:
    """Require finite time before it reaches durable migration state."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field_name} must be finite")
    return numeric


def _token_digest(token: str) -> str:
    """Hash an opaque local holder capability before persistence."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _adapter_instance_ids(values: Sequence[str]) -> tuple[str, ...]:
    """Normalize a non-empty adapter-instance set before durable insertion."""

    if isinstance(values, str):
        raise TypeError("adapter_instance_ids must be an iterable, not a string")
    normalized = tuple(_identifier(value, "adapter_instance_id") for value in values)
    if not normalized or len(set(normalized)) != len(normalized):
        raise ValueError("adapter_instance_ids must be a non-empty unique set")
    return tuple(sorted(normalized))


def _encode_adapter_instance_ids(adapter_instance_ids: tuple[str, ...]) -> str:
    """Serialize canonical adapter identities without retaining runtime objects."""

    return json.dumps(list(adapter_instance_ids), ensure_ascii=True, separators=(",", ":"))


def _decode_adapter_instance_ids(value: object) -> tuple[str, ...]:
    """Decode and require the canonical adapter identity sequence from storage."""

    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActorV2MigrationBarrierConflict(
            "migration barrier has invalid adapter instance identities"
        ) from exc
    normalized = _adapter_instance_ids(decoded)
    if decoded != list(normalized):
        raise ActorV2MigrationBarrierConflict(
            "migration barrier adapter instance identities are not canonical"
        )
    return normalized


__all__ = ["ActorV2MigrationBarrierRepository"]
