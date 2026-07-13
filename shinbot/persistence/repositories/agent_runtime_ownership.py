"""SQLite repository for durable Agent session runtime ownership."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from sqlite3 import Connection, Row

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnership,
    AgentRuntimeOwnershipClaim,
    AgentRuntimeOwnershipConflict,
    AgentRuntimeOwnershipEvent,
    AgentRuntimeOwnershipEventType,
    AgentRuntimeOwnershipEvidenceConflict,
    AgentRuntimeOwnershipGenerationConflict,
    AgentRuntimeOwnershipMigrationConflict,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipNotFound,
    AgentRuntimeOwnershipRequired,
    AgentRuntimeOwnershipStatus,
)
from shinbot.persistence.repositories.agent_external_action_reconciliation import (
    reconcile_abandoned_before_dispatch_receipts,
)
from shinbot.persistence.repositories.base import Repository

_OWNERSHIP_EVENT_NAMESPACE = uuid.UUID("2355e792-d140-59f5-b882-81c24854a27f")
_TYPED_RECOVERY_EVENT_KIND = "RecoveryRequested"
_TYPED_RECOVERY_EVENT_SOURCE = "durable_session_recovery_scanner"


class AgentRuntimeOwnershipRepository(Repository):
    """Atomically select and migrate one runtime owner per stable session key."""

    def __init__(
        self,
        db: object,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Initialize the repository with an injectable commit clock."""

        super().__init__(db)
        self._clock = clock or time.time

    def get(self, key: SessionKey) -> AgentRuntimeOwnership | None:
        """Return the current ownership record for a stable session key."""

        with self.connect() as conn:
            row = self._select(conn, key)
        return _ownership_from_row(row) if row is not None else None

    def claim(
        self,
        key: SessionKey,
        mode: AgentRuntimeOwnershipMode,
        *,
        reason: str,
        legacy_session_id: str | None = None,
        requested_by: str = "",
    ) -> AgentRuntimeOwnershipClaim:
        """Atomically create or idempotently reuse the first ownership choice."""

        selected_mode = AgentRuntimeOwnershipMode(mode)
        normalized_reason = _required_reason(reason)
        legacy_id = _legacy_session_id(key, legacy_session_id)
        requester = str(requested_by or "").strip()
        now = self._clock()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing_row = self._select(conn, key)
            if existing_row is not None:
                existing = _ownership_from_row(existing_row)
                if existing.status is not AgentRuntimeOwnershipStatus.ACTIVE:
                    raise AgentRuntimeOwnershipMigrationConflict(
                        f"ownership migration is already in progress for {key}"
                    )
                if existing.mode is not selected_mode:
                    raise AgentRuntimeOwnershipConflict(
                        "session ownership is already claimed by "
                        f"{existing.mode.value}, not {selected_mode.value}"
                    )
                if existing.legacy_session_id != legacy_id:
                    raise AgentRuntimeOwnershipConflict(
                        "idempotent ownership claim changed legacy_session_id"
                    )
                return AgentRuntimeOwnershipClaim(existing, created=False)

            self._validate_external_action_receipts_for_transition(conn, key, now=now)
            self._validate_target_evidence(
                conn,
                key,
                selected_mode,
                legacy_session_id=legacy_id,
            )
            conn.execute(
                """
                INSERT INTO agent_session_runtime_ownership (
                    profile_id, session_id, legacy_session_id, mode, status,
                    pending_mode, generation, selection_reason,
                    migration_reason, requested_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'active', '', 1, ?, '', ?, ?, ?)
                """,
                (
                    key.profile_id,
                    key.session_id,
                    legacy_id,
                    selected_mode.value,
                    normalized_reason,
                    requester,
                    now,
                    now,
                ),
            )
            ownership = AgentRuntimeOwnership(
                key=key,
                legacy_session_id=legacy_id,
                mode=selected_mode,
                status=AgentRuntimeOwnershipStatus.ACTIVE,
                generation=1,
                selection_reason=normalized_reason,
                requested_by=requester,
                created_at=now,
                updated_at=now,
            )
            self._append_event(
                conn,
                ownership,
                event_type=AgentRuntimeOwnershipEventType.CLAIMED,
                from_mode=None,
                reason=normalized_reason,
                requested_by=requester,
            )
        return AgentRuntimeOwnershipClaim(ownership, created=True)

    def begin_migration(
        self,
        key: SessionKey,
        target_mode: AgentRuntimeOwnershipMode,
        *,
        expected_generation: int,
        reason: str,
        requested_by: str = "",
    ) -> AgentRuntimeOwnership:
        """CAS an active owner into an explicit migrating state."""

        target = AgentRuntimeOwnershipMode(target_mode)
        normalized_reason = _required_reason(reason)
        requester = str(requested_by or "").strip()
        now = self._clock()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._required(conn, key)
            self._require_generation(current, expected_generation)
            if current.status is not AgentRuntimeOwnershipStatus.ACTIVE:
                raise AgentRuntimeOwnershipMigrationConflict(
                    "ownership must be active before migration can begin"
                )
            if current.mode is target:
                raise AgentRuntimeOwnershipMigrationConflict(
                    "migration target must differ from the active ownership mode"
                )
            self._validate_external_action_receipts_for_transition(conn, key, now=now)
            if current.mode is AgentRuntimeOwnershipMode.ACTOR_V2:
                self._validate_recovery_migration_boundary(
                    conn,
                    key,
                    expected_generation=current.generation,
                )
            next_generation = current.generation + 1
            updated = conn.execute(
                """
                UPDATE agent_session_runtime_ownership
                SET status = 'migrating',
                    pending_mode = ?,
                    generation = ?,
                    migration_reason = ?,
                    requested_by = ?,
                    updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND status = 'active'
                  AND generation = ?
                """,
                (
                    target.value,
                    next_generation,
                    normalized_reason,
                    requester,
                    now,
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                ),
            )
            if updated.rowcount != 1:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "ownership changed while migration was starting"
                )
            self._refence_routing_state(
                conn,
                key,
                expected_generation=current.generation,
                target_generation=next_generation,
                now=now,
                release_leases=True,
            )
            migrating = AgentRuntimeOwnership(
                key=key,
                legacy_session_id=current.legacy_session_id,
                mode=current.mode,
                status=AgentRuntimeOwnershipStatus.MIGRATING,
                pending_mode=target,
                generation=next_generation,
                selection_reason=current.selection_reason,
                migration_reason=normalized_reason,
                requested_by=requester,
                created_at=current.created_at,
                updated_at=now,
            )
            self._append_event(
                conn,
                migrating,
                event_type=AgentRuntimeOwnershipEventType.MIGRATION_STARTED,
                from_mode=current.mode,
                to_mode=target,
                reason=normalized_reason,
                requested_by=requester,
            )
        return migrating

    def complete_migration(
        self,
        key: SessionKey,
        *,
        expected_generation: int,
        reason: str,
        requested_by: str = "",
    ) -> AgentRuntimeOwnership:
        """Activate a migrated target after target-mode evidence is clean."""

        normalized_reason = _required_reason(reason)
        requester = str(requested_by or "").strip()
        now = self._clock()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._required(conn, key)
            self._require_generation(current, expected_generation)
            if (
                current.status is not AgentRuntimeOwnershipStatus.MIGRATING
                or current.pending_mode is None
            ):
                raise AgentRuntimeOwnershipMigrationConflict(
                    "ownership is not awaiting migration completion"
                )
            self._validate_external_action_receipts_for_transition(conn, key, now=now)
            target = current.pending_mode
            self._validate_target_evidence(
                conn,
                key,
                target,
                legacy_session_id=current.legacy_session_id,
                exclude_current_ownership=True,
            )
            if target is AgentRuntimeOwnershipMode.ACTOR_V2:
                self._validate_actor_state_for_activation(
                    conn,
                    key,
                    expected_generation=current.generation,
                )
            next_generation = current.generation + 1
            updated = conn.execute(
                """
                UPDATE agent_session_runtime_ownership
                SET mode = ?,
                    status = 'active',
                    pending_mode = '',
                    generation = ?,
                    migration_reason = ?,
                    requested_by = ?,
                    updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND status = 'migrating'
                  AND generation = ?
                  AND pending_mode = ?
                """,
                (
                    target.value,
                    next_generation,
                    normalized_reason,
                    requester,
                    now,
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                    target.value,
                ),
            )
            if updated.rowcount != 1:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "ownership changed while migration was completing"
                )
            if target is AgentRuntimeOwnershipMode.ACTOR_V2:
                self._refence_actor_state(
                    conn,
                    key,
                    expected_generation=current.generation,
                    target_generation=next_generation,
                    now=now,
                    release_leases=False,
                )
                self._refence_routing_state(
                    conn,
                    key,
                    expected_generation=current.generation,
                    target_generation=next_generation,
                    now=now,
                    release_leases=False,
                )
            completed = AgentRuntimeOwnership(
                key=key,
                legacy_session_id=current.legacy_session_id,
                mode=target,
                status=AgentRuntimeOwnershipStatus.ACTIVE,
                generation=next_generation,
                selection_reason=current.selection_reason,
                migration_reason=normalized_reason,
                requested_by=requester,
                created_at=current.created_at,
                updated_at=now,
            )
            self._append_event(
                conn,
                completed,
                event_type=AgentRuntimeOwnershipEventType.MIGRATION_COMPLETED,
                from_mode=current.mode,
                reason=normalized_reason,
                requested_by=requester,
            )
        return completed

    def abort_migration(
        self,
        key: SessionKey,
        *,
        expected_generation: int,
        reason: str,
        requested_by: str = "",
    ) -> AgentRuntimeOwnership:
        """CAS a migrating owner back to its unchanged source mode."""

        normalized_reason = _required_reason(reason)
        requester = str(requested_by or "").strip()
        now = self._clock()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._required(conn, key)
            self._require_generation(current, expected_generation)
            if (
                current.status is not AgentRuntimeOwnershipStatus.MIGRATING
                or current.pending_mode is None
            ):
                raise AgentRuntimeOwnershipMigrationConflict(
                    "ownership is not in a migration that can be aborted"
                )
            self._validate_external_action_receipts_for_transition(conn, key, now=now)
            next_generation = current.generation + 1
            updated = conn.execute(
                """
                UPDATE agent_session_runtime_ownership
                SET status = 'active',
                    pending_mode = '',
                    generation = ?,
                    migration_reason = ?,
                    requested_by = ?,
                    updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND status = 'migrating'
                  AND generation = ?
                """,
                (
                    next_generation,
                    normalized_reason,
                    requester,
                    now,
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                ),
            )
            if updated.rowcount != 1:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "ownership changed while migration was aborting"
                )
            if current.mode is AgentRuntimeOwnershipMode.ACTOR_V2:
                self._refence_actor_state(
                    conn,
                    key,
                    expected_generation=current.generation - 1,
                    target_generation=next_generation,
                    now=now,
                    release_leases=True,
                )
            self._refence_routing_state(
                conn,
                key,
                expected_generation=current.generation,
                target_generation=next_generation,
                now=now,
                release_leases=True,
            )
            aborted = AgentRuntimeOwnership(
                key=key,
                legacy_session_id=current.legacy_session_id,
                mode=current.mode,
                status=AgentRuntimeOwnershipStatus.ACTIVE,
                generation=next_generation,
                selection_reason=current.selection_reason,
                migration_reason=normalized_reason,
                requested_by=requester,
                created_at=current.created_at,
                updated_at=now,
            )
            self._append_event(
                conn,
                aborted,
                event_type=AgentRuntimeOwnershipEventType.MIGRATION_ABORTED,
                from_mode=current.pending_mode,
                to_mode=current.mode,
                reason=normalized_reason,
                requested_by=requester,
            )
        return aborted

    def require_actor_v2_in_transaction(
        self,
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int | None = None,
    ) -> AgentRuntimeOwnership:
        """Validate active actor ownership using the caller's transaction."""

        ownership = self._required(conn, key)
        if expected_generation is not None:
            self._require_generation(ownership, expected_generation)
        if not ownership.actor_v2_active:
            raise AgentRuntimeOwnershipRequired(
                f"active actor_v2 ownership is required for {key}"
            )
        return ownership

    def list_events(self, key: SessionKey) -> list[AgentRuntimeOwnershipEvent]:
        """Return ownership audit events in committed generation order."""

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM agent_session_runtime_ownership_events
                WHERE profile_id = ? AND session_id = ?
                ORDER BY event_seq ASC
                """,
                (key.profile_id, key.session_id),
            ).fetchall()
        return [_event_from_row(row) for row in rows]

    def _validate_external_action_receipts_for_transition(
        self,
        conn: Connection,
        key: SessionKey,
        *,
        now: float,
    ) -> None:
        reconcile_abandoned_before_dispatch_receipts(conn, key, now=now)
        rows = conn.execute(
            """
            SELECT idempotency_key, status
            FROM agent_external_action_receipts
            WHERE profile_id = ? AND session_id = ?
              AND status IN (
                  'prepared', 'executing', 'rejected_before_dispatch'
              )
            ORDER BY receipt_seq
            LIMIT 10
            """,
            (key.profile_id, key.session_id),
        ).fetchall()
        if not rows:
            return
        evidence = ", ".join(
            f"{row['idempotency_key']}:{row['status']}" for row in rows
        )
        raise AgentRuntimeOwnershipMigrationConflict(
            "live external-action receipts block ownership transition: "
            + evidence
        )

    @staticmethod
    def _validate_actor_state_for_activation(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        generation_tables = (
            "agent_session_aggregates",
            "agent_session_mailbox",
            "agent_session_operations",
            "agent_message_ledger",
            "agent_message_ledger_consumptions",
            "agent_review_schedules",
            "agent_state_transitions",
            "agent_review_schedule_events",
            "agent_effect_outbox",
        )
        mismatched: list[str] = []
        for table_name in generation_tables:
            extra_predicate = ""
            parameters: tuple[object, ...] = (
                key.profile_id,
                key.session_id,
                expected_generation,
            )
            if table_name == "agent_session_mailbox":
                extra_predicate = (
                    " AND NOT (kind = ? AND source = ?)"
                )
                parameters += (
                    _TYPED_RECOVERY_EVENT_KIND,
                    _TYPED_RECOVERY_EVENT_SOURCE,
                )
            row = conn.execute(
                f"""
                SELECT ownership_generation
                FROM {table_name}
                WHERE profile_id = ?
                  AND session_id = ?
                  AND ownership_generation != ?
                  {extra_predicate}
                LIMIT 1
                """,
                parameters,
            ).fetchone()
            if row is not None:
                mismatched.append(
                    f"{table_name}:generation={int(row['ownership_generation'])}"
                )
        for table_name in ("message_routing_jobs", "agent_route_outbox"):
            row = conn.execute(
                f"""
                SELECT ownership_generation
                FROM {table_name}
                WHERE profile_id = ?
                  AND session_id = ?
                  AND status IN ('pending', 'processing')
                  AND ownership_generation != ?
                LIMIT 1
                """,
                (key.profile_id, key.session_id, expected_generation),
            ).fetchone()
            if row is not None:
                mismatched.append(
                    f"{table_name}:generation={int(row['ownership_generation'])}"
                )
        if mismatched:
            raise AgentRuntimeOwnershipGenerationConflict(
                "target actor state is not fenced by the migrating generation: "
                + ", ".join(mismatched)
            )
        live_lease_queries = (
            (
                "agent_session_mailbox",
                "(status = 'processing' OR claim_id != '' OR lease_owner != '' "
                "OR lease_until IS NOT NULL) "
                "AND NOT (kind = 'RecoveryRequested' "
                "AND source = 'durable_session_recovery_scanner')",
            ),
            (
                "agent_effect_outbox",
                "status = 'processing' OR claim_id != '' OR lease_owner != '' "
                "OR lease_until IS NOT NULL",
            ),
            (
                "agent_review_schedules",
                "status = 'claimed' OR claim_owner != '' OR claim_until IS NOT NULL",
            ),
            (
                "agent_session_operations",
                "lease_owner != '' OR lease_until IS NOT NULL",
            ),
            (
                "message_routing_jobs",
                "status = 'processing' OR claim_id != '' OR lease_owner != '' "
                "OR lease_until IS NOT NULL",
            ),
            (
                "agent_route_outbox",
                "status = 'processing' OR claim_id != '' OR lease_owner != '' "
                "OR lease_until IS NOT NULL",
            ),
        )
        leased = [
            table_name
            for table_name, predicate in live_lease_queries
            if conn.execute(
                f"""
                SELECT 1 FROM {table_name}
                WHERE profile_id = ? AND session_id = ? AND ({predicate})
                LIMIT 1
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            is not None
        ]
        if leased:
            raise AgentRuntimeOwnershipMigrationConflict(
                "target actor state contains live leases: " + ", ".join(leased)
            )

    @staticmethod
    def _refence_actor_state(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
        target_generation: int,
        now: float,
        release_leases: bool,
    ) -> None:
        AgentRuntimeOwnershipRepository._validate_recovery_migration_boundary(
            conn,
            key,
            expected_generation=expected_generation,
        )
        AgentRuntimeOwnershipRepository._validate_actor_state_generations(
            conn,
            key,
            expected_generation=expected_generation,
        )
        if release_leases:
            conn.execute(
                """
                UPDATE agent_session_mailbox
                SET status = CASE
                        WHEN status = 'processing' THEN 'pending'
                        ELSE status
                    END,
                    available_at = CASE
                        WHEN status = 'processing' THEN MIN(available_at, ?)
                        ELSE available_at
                    END,
                    claim_id = '', lease_owner = '', lease_until = NULL,
                    last_error = CASE
                        WHEN status = 'processing' AND last_error = ''
                        THEN 'ownership_migration_aborted'
                        ELSE last_error
                    END
                WHERE profile_id = ? AND session_id = ?
                  AND ownership_generation = ?
                  AND NOT (kind = ? AND source = ?)
                """,
                (
                    now,
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                    _TYPED_RECOVERY_EVENT_KIND,
                    _TYPED_RECOVERY_EVENT_SOURCE,
                ),
            )
            conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = CASE
                        WHEN status = 'processing' THEN 'pending'
                        ELSE status
                    END,
                    available_at = CASE
                        WHEN status = 'processing' THEN MIN(available_at, ?)
                        ELSE available_at
                    END,
                    claim_id = '', lease_owner = '', lease_until = NULL,
                    updated_at = ?,
                    last_error = CASE
                        WHEN status = 'processing' AND last_error = ''
                        THEN 'ownership_migration_aborted'
                        ELSE last_error
                    END
                WHERE profile_id = ? AND session_id = ?
                  AND ownership_generation = ?
                """,
                (
                    now,
                    now,
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                ),
            )
            conn.execute(
                """
                UPDATE agent_review_schedules
                SET status = CASE
                        WHEN status = 'claimed' THEN 'scheduled'
                        ELSE status
                    END,
                    claim_owner = '', claim_until = NULL, updated_at = ?
                WHERE profile_id = ? AND session_id = ?
                  AND ownership_generation = ?
                """,
                (now, key.profile_id, key.session_id, expected_generation),
            )
            conn.execute(
                """
                UPDATE agent_session_operations
                SET lease_owner = '', lease_until = NULL
                WHERE profile_id = ? AND session_id = ?
                  AND ownership_generation = ?
                """,
                (key.profile_id, key.session_id, expected_generation),
            )
        for table_name in (
            "agent_session_aggregates",
            "agent_session_mailbox",
            "agent_session_operations",
            "agent_message_ledger",
            "agent_message_ledger_consumptions",
            "agent_review_schedules",
            "agent_state_transitions",
            "agent_review_schedule_events",
            "agent_effect_outbox",
        ):
            extra_predicate = ""
            parameters: tuple[object, ...] = (
                target_generation,
                key.profile_id,
                key.session_id,
                expected_generation,
            )
            if table_name == "agent_session_mailbox":
                extra_predicate = " AND NOT (kind = ? AND source = ?)"
                parameters += (
                    _TYPED_RECOVERY_EVENT_KIND,
                    _TYPED_RECOVERY_EVENT_SOURCE,
                )
            conn.execute(
                f"""
                UPDATE {table_name}
                SET ownership_generation = ?
                WHERE profile_id = ? AND session_id = ?
                  AND ownership_generation = ?
                  {extra_predicate}
                """,
                parameters,
            )

    @staticmethod
    def _refence_routing_state(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
        target_generation: int,
        now: float,
        release_leases: bool,
    ) -> None:
        AgentRuntimeOwnershipRepository._validate_routing_state_generations(
            conn,
            key,
            expected_generation=expected_generation,
        )
        if release_leases:
            conn.execute(
                """
                UPDATE message_routing_jobs
                SET status = CASE
                        WHEN status = 'processing' THEN 'pending'
                        ELSE status
                    END,
                    available_at = CASE
                        WHEN status = 'processing' THEN MIN(available_at, ?)
                        ELSE available_at
                    END,
                    claim_id = '', lease_owner = '', lease_until = NULL,
                    updated_at = ?,
                    last_error_code = CASE
                        WHEN status = 'processing' AND last_error_code = ''
                        THEN 'ownership_refenced'
                        ELSE last_error_code
                    END,
                    last_error_message = CASE
                        WHEN status = 'processing' AND last_error_message = ''
                        THEN 'routing claim released by ownership generation change'
                        ELSE last_error_message
                    END
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                  AND ownership_generation = ?
                """,
                (
                    now,
                    now,
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                ),
            )
            conn.execute(
                """
                UPDATE agent_route_outbox
                SET status = CASE
                        WHEN status = 'processing' THEN 'pending'
                        ELSE status
                    END,
                    available_at = CASE
                        WHEN status = 'processing' THEN MIN(available_at, ?)
                        ELSE available_at
                    END,
                    claim_id = '', lease_owner = '', lease_until = NULL,
                    updated_at = ?,
                    last_error_code = CASE
                        WHEN status = 'processing' AND last_error_code = ''
                        THEN 'ownership_refenced'
                        ELSE last_error_code
                    END,
                    last_error_message = CASE
                        WHEN status = 'processing' AND last_error_message = ''
                        THEN 'delivery claim released by ownership generation change'
                        ELSE last_error_message
                    END
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                  AND ownership_generation = ?
                """,
                (
                    now,
                    now,
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                ),
            )
        for table_name in ("message_routing_jobs", "agent_route_outbox"):
            conn.execute(
                f"""
                UPDATE {table_name}
                SET ownership_generation = ?
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                  AND ownership_generation = ?
                """,
                (
                    target_generation,
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                ),
            )

    @staticmethod
    def _validate_actor_state_generations(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        for table_name in (
            "agent_session_aggregates",
            "agent_session_mailbox",
            "agent_session_operations",
            "agent_message_ledger",
            "agent_message_ledger_consumptions",
            "agent_review_schedules",
            "agent_state_transitions",
            "agent_review_schedule_events",
            "agent_effect_outbox",
        ):
            extra_predicate = ""
            parameters: tuple[object, ...] = (
                key.profile_id,
                key.session_id,
                expected_generation,
            )
            if table_name == "agent_session_mailbox":
                extra_predicate = " AND NOT (kind = ? AND source = ?)"
                parameters += (
                    _TYPED_RECOVERY_EVENT_KIND,
                    _TYPED_RECOVERY_EVENT_SOURCE,
                )
            row = conn.execute(
                f"""
                SELECT ownership_generation FROM {table_name}
                WHERE profile_id = ? AND session_id = ?
                  AND ownership_generation != ?
                  {extra_predicate}
                LIMIT 1
                """,
                parameters,
            ).fetchone()
            if row is not None:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "actor state contains a stale ownership generation in "
                    f"{table_name}: {int(row['ownership_generation'])}"
                )

    @staticmethod
    def _validate_recovery_migration_boundary(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        """Keep typed recovery certificates immutable across owner changes."""

        open_case = conn.execute(
            """
            SELECT case_id
            FROM agent_session_recovery_cases
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND status = 'open'
            ORDER BY case_id
            LIMIT 1
            """,
            (key.profile_id, key.session_id, expected_generation),
        ).fetchone()
        if open_case is not None:
            raise AgentRuntimeOwnershipMigrationConflict(
                "open typed recovery case blocks ownership migration: "
                f"{open_case['case_id']}"
            )
        active_delivery = conn.execute(
            """
            SELECT event_id, status
            FROM agent_session_mailbox
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND kind = ?
              AND source = ?
              AND status IN ('pending', 'processing')
            ORDER BY mailbox_id
            LIMIT 1
            """,
            (
                key.profile_id,
                key.session_id,
                expected_generation,
                _TYPED_RECOVERY_EVENT_KIND,
                _TYPED_RECOVERY_EVENT_SOURCE,
            ),
        ).fetchone()
        if active_delivery is not None:
            raise AgentRuntimeOwnershipMigrationConflict(
                "active typed recovery delivery blocks ownership migration: "
                f"{active_delivery['event_id']}:{active_delivery['status']}"
            )
    @staticmethod
    def _validate_routing_state_generations(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        for table_name in ("message_routing_jobs", "agent_route_outbox"):
            row = conn.execute(
                f"""
                SELECT ownership_generation FROM {table_name}
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                  AND ownership_generation != ?
                LIMIT 1
                """,
                (key.profile_id, key.session_id, expected_generation),
            ).fetchone()
            if row is not None:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "actor state contains a stale ownership generation in "
                    f"{table_name}: {int(row['ownership_generation'])}"
                )

    @staticmethod
    def _select(conn: Connection, key: SessionKey) -> Row | None:
        return conn.execute(
            """
            SELECT *
            FROM agent_session_runtime_ownership
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()

    def _required(self, conn: Connection, key: SessionKey) -> AgentRuntimeOwnership:
        row = self._select(conn, key)
        if row is None:
            raise AgentRuntimeOwnershipNotFound(
                f"no runtime ownership exists for {key}"
            )
        return _ownership_from_row(row)

    @staticmethod
    def _require_generation(
        ownership: AgentRuntimeOwnership,
        expected_generation: int,
    ) -> None:
        if ownership.generation != expected_generation:
            raise AgentRuntimeOwnershipGenerationConflict(
                "stale ownership generation: "
                f"expected {expected_generation}, found {ownership.generation}"
            )

    def _validate_target_evidence(
        self,
        conn: Connection,
        key: SessionKey,
        target: AgentRuntimeOwnershipMode,
        *,
        legacy_session_id: str,
        exclude_current_ownership: bool = False,
    ) -> None:
        evidence: list[str] = []
        if target is AgentRuntimeOwnershipMode.LEGACY:
            if conn.execute(
                """
                SELECT 1 FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ? LIMIT 1
                """,
                (key.profile_id, key.session_id),
            ).fetchone() is not None:
                evidence.append("actor_aggregate")
            if conn.execute(
                """
                SELECT 1 FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ? LIMIT 1
                """,
                (key.profile_id, key.session_id),
            ).fetchone() is not None:
                evidence.append("actor_mailbox")
            if conn.execute(
                """
                SELECT 1 FROM message_routing_jobs
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                LIMIT 1
                """,
                (key.profile_id, key.session_id),
            ).fetchone() is not None:
                evidence.append("actor_message_routing_job")
            if conn.execute(
                """
                SELECT 1 FROM agent_route_outbox
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                LIMIT 1
                """,
                (key.profile_id, key.session_id),
            ).fetchone() is not None:
                evidence.append("actor_route_outbox")
            evidence.extend(
                self._conflicting_ownership_evidence(
                    conn,
                    key,
                    legacy_session_id=legacy_session_id,
                    conflicting_mode=AgentRuntimeOwnershipMode.ACTOR_V2,
                    exclude_current=exclude_current_ownership,
                )
            )
            evidence.extend(
                self._conflicting_ownership_evidence(
                    conn,
                    key,
                    legacy_session_id=legacy_session_id,
                    conflicting_mode=AgentRuntimeOwnershipMode.LEGACY,
                    exclude_current=exclude_current_ownership,
                )
            )
        else:
            legacy_queries = (
                (
                    "SELECT 1 FROM agent_scheduler_states "
                    "WHERE session_id = ? LIMIT 1",
                    "legacy_scheduler_state",
                ),
                (
                    "SELECT 1 FROM agent_unread_messages "
                    "WHERE session_id = ? LIMIT 1",
                    "legacy_unread_messages",
                ),
                (
                    "SELECT 1 FROM agent_unread_ranges "
                    "WHERE session_id = ? LIMIT 1",
                    "legacy_unread_ranges",
                ),
            )
            for query, label in legacy_queries:
                row = conn.execute(query, (legacy_session_id,)).fetchone()
                if row is not None:
                    evidence.append(label)
            evidence.extend(
                self._conflicting_ownership_evidence(
                    conn,
                    key,
                    legacy_session_id=legacy_session_id,
                    conflicting_mode=AgentRuntimeOwnershipMode.LEGACY,
                    exclude_current=exclude_current_ownership,
                )
            )
        if evidence:
            raise AgentRuntimeOwnershipEvidenceConflict(
                f"durable state conflicts with {target.value} ownership for {key}: "
                + ", ".join(evidence),
                evidence=tuple(evidence),
            )

    @staticmethod
    def _conflicting_ownership_evidence(
        conn: Connection,
        key: SessionKey,
        *,
        legacy_session_id: str,
        conflicting_mode: AgentRuntimeOwnershipMode,
        exclude_current: bool,
    ) -> list[str]:
        query = """
            SELECT profile_id, session_id
            FROM agent_session_runtime_ownership
            WHERE legacy_session_id = ?
              AND mode = ?
        """
        params: list[object] = [legacy_session_id, conflicting_mode.value]
        if exclude_current:
            query += " AND NOT (profile_id = ? AND session_id = ?)"
            params.extend((key.profile_id, key.session_id))
        rows = conn.execute(query, params).fetchall()
        return [
            f"{conflicting_mode.value}_ownership:{row['profile_id']}:{row['session_id']}"
            for row in rows
        ]

    @staticmethod
    def _append_event(
        conn: Connection,
        ownership: AgentRuntimeOwnership,
        *,
        event_type: AgentRuntimeOwnershipEventType,
        from_mode: AgentRuntimeOwnershipMode | None,
        to_mode: AgentRuntimeOwnershipMode | None = None,
        reason: str,
        requested_by: str,
    ) -> None:
        event_id = _event_id(ownership.key, ownership.generation, event_type)
        conn.execute(
            """
            INSERT INTO agent_session_runtime_ownership_events (
                event_id, profile_id, session_id, event_type, generation,
                from_mode, to_mode, status, reason, requested_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                ownership.key.profile_id,
                ownership.key.session_id,
                event_type.value,
                ownership.generation,
                from_mode.value if from_mode is not None else "",
                (to_mode or ownership.mode).value,
                ownership.status.value,
                reason,
                requested_by,
                ownership.updated_at,
            ),
        )


def _ownership_from_row(row: Row) -> AgentRuntimeOwnership:
    pending = str(row["pending_mode"] or "")
    return AgentRuntimeOwnership(
        key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
        legacy_session_id=str(row["legacy_session_id"]),
        mode=AgentRuntimeOwnershipMode(str(row["mode"])),
        status=AgentRuntimeOwnershipStatus(str(row["status"])),
        pending_mode=AgentRuntimeOwnershipMode(pending) if pending else None,
        generation=int(row["generation"]),
        selection_reason=str(row["selection_reason"]),
        migration_reason=str(row["migration_reason"]),
        requested_by=str(row["requested_by"]),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
    )


def _event_from_row(row: Row) -> AgentRuntimeOwnershipEvent:
    from_mode = str(row["from_mode"] or "")
    return AgentRuntimeOwnershipEvent(
        event_id=str(row["event_id"]),
        key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
        event_type=AgentRuntimeOwnershipEventType(str(row["event_type"])),
        generation=int(row["generation"]),
        from_mode=AgentRuntimeOwnershipMode(from_mode) if from_mode else None,
        to_mode=AgentRuntimeOwnershipMode(str(row["to_mode"])),
        status=AgentRuntimeOwnershipStatus(str(row["status"])),
        reason=str(row["reason"]),
        requested_by=str(row["requested_by"]),
        created_at=float(row["created_at"]),
    )


def _legacy_session_id(key: SessionKey, value: str | None) -> str:
    normalized = str(value or key.session_id).strip()
    if not normalized:
        raise ValueError("legacy_session_id must not be empty")
    return normalized


def _required_reason(reason: str) -> str:
    normalized = str(reason or "").strip()
    if not normalized:
        raise ValueError("ownership audit reason must not be empty")
    return normalized


def _event_id(
    key: SessionKey,
    generation: int,
    event_type: AgentRuntimeOwnershipEventType,
) -> str:
    identity = json.dumps(
        [key.profile_id, key.session_id, generation, event_type.value],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return f"agent-runtime-ownership:{uuid.uuid5(_OWNERSHIP_EVENT_NAMESPACE, identity).hex}"


__all__ = ["AgentRuntimeOwnershipRepository"]
