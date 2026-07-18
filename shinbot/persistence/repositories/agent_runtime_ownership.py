"""SQLite repository for durable Agent session runtime ownership."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Mapping
from sqlite3 import Connection, Row

from shinbot.core.dispatch.actor_v2_admission import (
    ActorV2AdmissionFenceConflict,
    ActorV2AdmissionFenceStatus,
    ActorV2AdmissionGrant,
)
from shinbot.core.dispatch.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierGrant,
)
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
_TERMINAL_FENCED_MAILBOX_HANDOFF_PREDICATE = """
AND NOT EXISTS (
    SELECT 1
    FROM agent_session_mailbox_handoffs AS handoff
    WHERE handoff.mailbox_id = agent_session_mailbox.mailbox_id
      AND handoff.evidence_state = 'fenced'
      AND handoff.state = 'settled'
)
"""
_REVIEW_EFFECT_KIND = "run_review_workflow"
_CANCEL_REVIEW_EFFECT_KIND = "cancel_review_workflow"
_CANCEL_REVIEW_EFFECT_VERSION = 2
# This is the immutable signature of the public v2 cancellation contract. It
# lives here because ownership validation must remain import-safe before the
# agent package is loaded; a signature change requires a new contract version.
_CANCEL_REVIEW_EFFECT_V2_SIGNATURE = (
    "bfa750297968f3f079d2e6070dccb3d5c40dd4916bee27e299144ee6752a68a9"
)
_CANCEL_MODEL_EXECUTION_EFFECT_KIND = "cancel_model_execution"
_CANCEL_MODEL_EXECUTION_EFFECT_VERSION = 3
# The v3 generic cancellation wire contract is intentionally copied here so
# ownership validation remains import-safe during early bootstrap.
_CANCEL_MODEL_EXECUTION_EFFECT_V3_SIGNATURE = (
    "e0c7317e54e9b042aa33ece91eea488b1e20fa029dd42d484f589967672e2ece"
)
_TERMINAL_EFFECT_STATUSES = frozenset({"completed", "failed", "cancelled"})
_MODEL_EXECUTION_EFFECT_KINDS = frozenset(
    {
        "run_active_reply_workflow",
        "run_active_chat_bootstrap",
        "run_active_chat_round",
        "run_idle_review_planning",
    }
)


def _review_cancellation_control_payload_matches(
    payload_json: object,
    *,
    review_operation_id: str,
    review_effect_id: str,
    review_effect_kind: str,
    review_contract_version: int,
    review_contract_signature: str,
    ownership_generation: int,
) -> bool:
    """Return whether one persisted v2 control payload retains its exact fence.

    This repository cannot import the Agent contract registry without creating
    an ownership/bootstrap cycle.  It therefore validates the stable v2 wire
    shape directly and fails closed on every decoding or type mismatch.
    """

    if not isinstance(payload_json, str):
        return False
    try:
        payload = json.loads(payload_json)
    except (json.JSONDecodeError, TypeError, ValueError):
        return False
    if not isinstance(payload, Mapping):
        return False
    fence = payload.get("cancelled_operation_fence")
    if not isinstance(fence, Mapping):
        return False
    contract_version = fence.get("contract_version")
    fence_generation = fence.get("ownership_generation")
    return (
        payload.get("operation_id") == review_operation_id
        and fence.get("operation_id") == review_operation_id
        and fence.get("effect_id") == review_effect_id
        and fence.get("effect_kind") == review_effect_kind
        and isinstance(contract_version, int)
        and not isinstance(contract_version, bool)
        and contract_version == review_contract_version
        and fence.get("contract_signature") == review_contract_signature
        and isinstance(fence_generation, int)
        and not isinstance(fence_generation, bool)
        and fence_generation == ownership_generation
    )


def _model_execution_cancellation_control_payload_matches(
    payload_json: object,
    *,
    target_operation_id: str,
    target_effect_id: str,
    target_effect_kind: str,
    target_contract_version: int,
    target_contract_signature: str,
    ownership_generation: int,
) -> bool:
    """Return whether one v3 control payload retains its exact model fence."""

    if not isinstance(payload_json, str):
        return False
    try:
        payload = json.loads(payload_json)
    except (json.JSONDecodeError, TypeError, ValueError):
        return False
    if not isinstance(payload, Mapping):
        return False
    fence = payload.get("cancelled_model_effect_fence")
    if not isinstance(fence, Mapping):
        return False
    contract_version = fence.get("contract_version")
    fence_generation = fence.get("ownership_generation")
    return (
        payload.get("operation_id") == target_operation_id
        and fence.get("operation_id") == target_operation_id
        and fence.get("effect_id") == target_effect_id
        and fence.get("effect_kind") == target_effect_kind
        and isinstance(contract_version, int)
        and not isinstance(contract_version, bool)
        and contract_version == target_contract_version
        and fence.get("contract_signature") == target_contract_signature
        and isinstance(fence_generation, int)
        and not isinstance(fence_generation, bool)
        and fence_generation == ownership_generation
    )


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
        admission_grant: ActorV2AdmissionGrant | None = None,
    ) -> AgentRuntimeOwnershipClaim:
        """Atomically create or idempotently reuse the first ownership choice.

        A supplied Actor v2 admission grant is consumed in the same transaction
        that creates ownership.  The matching reservation then becomes a
        committed fence and any ingress jobs buffered behind it gain this
        ownership generation before either mutation is observable.
        """

        selected_mode = AgentRuntimeOwnershipMode(mode)
        normalized_reason = _required_reason(reason)
        legacy_id = _legacy_session_id(key, legacy_session_id)
        requester = str(requested_by or "").strip()
        if admission_grant is not None:
            if selected_mode is not AgentRuntimeOwnershipMode.ACTOR_V2:
                raise ValueError("an admission grant can only claim actor_v2 ownership")
            if admission_grant.fence.key != key:
                raise ActorV2AdmissionFenceConflict(
                    "admission grant session key does not match ownership claim"
                )
        now = self._clock()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if selected_mode is AgentRuntimeOwnershipMode.ACTOR_V2:
                self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
                    conn
                )
            if selected_mode is AgentRuntimeOwnershipMode.LEGACY:
                self._db.actor_v2_admission_fences.require_legacy_admission_open_in_transaction(
                    conn,
                    key,
                )
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
                self._validate_existing_admission_claim(
                    conn,
                    existing,
                    admission_grant=admission_grant,
                    now=now,
                )
                return AgentRuntimeOwnershipClaim(existing, created=False)

            admission_fence_id = ""
            admission_fence_generation = 0
            if admission_grant is not None:
                reserved = self._db.actor_v2_admission_fences.require_reserved_in_transaction(
                    conn,
                    admission_grant,
                    now=now,
                )
                self._db.actor_v2_legacy_recovery_gate.require_fenced_only_in_transaction(
                    conn
                )
                admission_fence_id = reserved.fence_id
                admission_fence_generation = reserved.generation
            elif selected_mode is AgentRuntimeOwnershipMode.ACTOR_V2:
                self._db.actor_v2_admission_fences.require_actor_v2_claim_open_in_transaction(
                    conn,
                    key,
                )
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
                    pending_mode, generation, admission_fence_id,
                    admission_fence_generation, selection_reason,
                    migration_reason, requested_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'active', '', 1, ?, ?, ?, '', ?, ?, ?)
                """,
                (
                    key.profile_id,
                    key.session_id,
                    legacy_id,
                    selected_mode.value,
                    admission_fence_id,
                    admission_fence_generation,
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
                admission_fence_id=admission_fence_id,
                admission_fence_generation=admission_fence_generation,
            )
            if admission_grant is not None:
                self._db.actor_v2_admission_fences.commit_in_transaction(
                    conn,
                    admission_grant,
                    now=now,
                )
                self._retarget_reserved_admission_routing_jobs(
                    conn,
                    key,
                    fence_id=admission_fence_id,
                    fence_generation=admission_fence_generation,
                    ownership_generation=ownership.generation,
                    now=now,
                )
                self._db.actor_v2_legacy_recovery_gate.require_fenced_only_in_transaction(
                    conn
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

    def claim_clean_actor_v2_in_transaction(
        self,
        conn: Connection,
        key: SessionKey,
        *,
        reason: str,
        legacy_session_id: str | None,
        requested_by: str = "",
        admission_grant: ActorV2AdmissionGrant,
        now: float | None = None,
    ) -> AgentRuntimeOwnershipClaim:
        """Claim a new fenced Actor v2 owner inside a caller transaction.

        This is deliberately narrower than :meth:`claim`: it accepts only a
        previously unowned clean session and is intended for a cutover journal
        transaction that must commit the ownership row and its phase evidence
        together. It does not authorize a legacy migration or an idempotent
        owner replay.
        """

        if not isinstance(conn, Connection):
            raise TypeError("conn must be a sqlite3 Connection")
        if not isinstance(key, SessionKey):
            raise TypeError("key must be a SessionKey")
        if not isinstance(admission_grant, ActorV2AdmissionGrant):
            raise TypeError("admission_grant must be an ActorV2AdmissionGrant")
        if admission_grant.fence.key != key:
            raise ActorV2AdmissionFenceConflict(
                "admission grant session key does not match ownership claim"
            )
        normalized_reason = _required_reason(reason)
        legacy_id = _legacy_session_id(key, legacy_session_id)
        requester = str(requested_by or "").strip()
        observed_at = self._clock() if now is None else now

        self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
            conn
        )
        if self._select(conn, key) is not None:
            raise AgentRuntimeOwnershipConflict(
                "clean Actor v2 ownership requires an unowned session"
            )
        reserved = self._db.actor_v2_admission_fences.require_reserved_in_transaction(
            conn,
            admission_grant,
            now=observed_at,
        )
        self._db.actor_v2_legacy_recovery_gate.require_fenced_only_in_transaction(conn)
        self._validate_external_action_receipts_for_transition(
            conn,
            key,
            now=observed_at,
        )
        self._validate_target_evidence(
            conn,
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            legacy_session_id=legacy_id,
        )
        conn.execute(
            """
            INSERT INTO agent_session_runtime_ownership (
                profile_id, session_id, legacy_session_id, mode, status,
                pending_mode, generation, admission_fence_id,
                admission_fence_generation, selection_reason,
                migration_reason, requested_by, created_at, updated_at
            ) VALUES (?, ?, ?, 'actor_v2', 'active', '', 1, ?, ?, ?, '', ?, ?, ?)
            """,
            (
                key.profile_id,
                key.session_id,
                legacy_id,
                reserved.fence_id,
                reserved.generation,
                normalized_reason,
                requester,
                observed_at,
                observed_at,
            ),
        )
        ownership = AgentRuntimeOwnership(
            key=key,
            legacy_session_id=legacy_id,
            mode=AgentRuntimeOwnershipMode.ACTOR_V2,
            status=AgentRuntimeOwnershipStatus.ACTIVE,
            generation=1,
            selection_reason=normalized_reason,
            requested_by=requester,
            created_at=observed_at,
            updated_at=observed_at,
            admission_fence_id=reserved.fence_id,
            admission_fence_generation=reserved.generation,
        )
        self._db.actor_v2_admission_fences.commit_in_transaction(
            conn,
            admission_grant,
            now=observed_at,
        )
        self._retarget_reserved_admission_routing_jobs(
            conn,
            key,
            fence_id=reserved.fence_id,
            fence_generation=reserved.generation,
            ownership_generation=ownership.generation,
            now=observed_at,
        )
        self._db.actor_v2_legacy_recovery_gate.require_fenced_only_in_transaction(conn)
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

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._db.actor_v2_migration_barriers.require_no_active_for_key_in_transaction(
                conn,
                key,
            )
            return self.begin_migration_in_transaction(
                conn,
                key,
                target_mode,
                expected_generation=expected_generation,
                reason=reason,
                requested_by=requested_by,
            )

    def begin_migration_in_transaction(
        self,
        conn: Connection,
        key: SessionKey,
        target_mode: AgentRuntimeOwnershipMode,
        *,
        expected_generation: int,
        reason: str,
        requested_by: str = "",
        now: float | None = None,
    ) -> AgentRuntimeOwnership:
        """Begin migration inside a caller-owned write transaction.

        The caller is responsible for any higher-level cutover capability and
        lifecycle interlocks. Generic callers must use :meth:`begin_migration`,
        which rejects an active Actor v2 migration barrier before entering this
        lower-level transition primitive.
        """

        target = AgentRuntimeOwnershipMode(target_mode)
        normalized_reason = _required_reason(reason)
        requester = str(requested_by or "").strip()
        observed_at = self._clock() if now is None else now
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
        if (
            current.mode is AgentRuntimeOwnershipMode.ACTOR_V2
            or target is AgentRuntimeOwnershipMode.ACTOR_V2
        ):
            self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
                conn
            )
        if target is AgentRuntimeOwnershipMode.ACTOR_V2:
            self._db.actor_v2_admission_fences.require_actor_v2_claim_open_in_transaction(
                conn,
                key,
            )
        if current.has_admission_fence:
            raise ActorV2AdmissionFenceConflict(
                "fenced Actor v2 ownership cannot migrate through the generic "
                "legacy transition"
            )
        self._validate_external_action_receipts_for_transition(
            conn,
            key,
            now=observed_at,
        )
        if current.mode is AgentRuntimeOwnershipMode.ACTOR_V2:
            self._validate_actor_admission_fence(conn, current)
            self._validate_recovery_migration_boundary(
                conn,
                key,
                expected_generation=current.generation,
            )
            self._validate_review_execution_migration_boundary(
                conn,
                key,
                expected_generation=current.generation,
            )
            self._validate_mailbox_handoff_refence_boundary(
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
                observed_at,
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
            now=observed_at,
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
            updated_at=observed_at,
            admission_fence_id=current.admission_fence_id,
            admission_fence_generation=current.admission_fence_generation,
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
            self._db.actor_v2_migration_barriers.require_no_active_for_key_in_transaction(
                conn,
                key,
            )
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
            if (
                current.mode is AgentRuntimeOwnershipMode.ACTOR_V2
                or target is AgentRuntimeOwnershipMode.ACTOR_V2
            ):
                self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
                    conn
                )
            if current.mode is AgentRuntimeOwnershipMode.ACTOR_V2:
                self._validate_actor_admission_fence(conn, current)
            if current.has_admission_fence:
                raise ActorV2AdmissionFenceConflict(
                    "fenced Actor v2 ownership cannot complete a generic legacy "
                    "transition"
                )
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
                self._validate_mailbox_handoff_refence_boundary(
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
                    admission_fence_id = '',
                    admission_fence_generation = 0,
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
                admission_fence_id="",
                admission_fence_generation=0,
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

    def complete_legacy_to_actor_v2_from_barrier_in_transaction(
        self,
        conn: Connection,
        *,
        barrier_grant: ActorV2MigrationBarrierGrant,
        reason: str,
        requested_by: str = "",
        now: float | None = None,
    ) -> AgentRuntimeOwnership:
        """Complete one holder-fenced legacy handoff after target materialization.

        Generic migration completion intentionally rejects an active migration
        barrier and rejects all legacy scheduler evidence.  A state-handoff
        finalizer has proved a narrower replacement: the same holder token,
        drained source boundary, immutable manifest, and Actor target state
        share this outer transaction.  This method is only the ownership half
        of that transaction; it neither captures source state nor publishes a
        wake target.
        """

        if not isinstance(barrier_grant, ActorV2MigrationBarrierGrant):
            raise TypeError("barrier_grant must be an ActorV2MigrationBarrierGrant")
        normalized_reason = _required_reason(reason)
        requester = str(requested_by or "").strip()
        observed_at = self._clock() if now is None else now
        barrier = self._db.actor_v2_migration_barriers.validate_in_transaction(
            conn,
            barrier_grant,
        )
        current = self._required(conn, barrier.key)
        if (
            current.mode is not AgentRuntimeOwnershipMode.LEGACY
            or current.status is not AgentRuntimeOwnershipStatus.MIGRATING
            or current.pending_mode is not AgentRuntimeOwnershipMode.ACTOR_V2
            or current.generation != barrier.migration_generation
            or current.legacy_session_id != barrier.legacy_session_id
        ):
            raise AgentRuntimeOwnershipMigrationConflict(
                "ownership no longer matches the holder-fenced legacy source"
            )
        if current.has_admission_fence:
            raise ActorV2AdmissionFenceConflict(
                "legacy source ownership cannot carry an Actor v2 admission fence"
            )
        self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
            conn
        )
        self._validate_external_action_receipts_for_transition(
            conn,
            barrier.key,
            now=observed_at,
        )
        # The finalizer has inserted only target rows at the barrier generation.
        # Do not call _validate_target_evidence(): the immutable legacy source
        # tables intentionally remain as historical provenance after activation.
        self._validate_actor_state_for_activation(
            conn,
            barrier.key,
            expected_generation=current.generation,
        )
        self._validate_mailbox_handoff_refence_boundary(
            conn,
            barrier.key,
            expected_generation=current.generation,
        )
        next_generation = current.generation + 1
        updated = conn.execute(
            """
            UPDATE agent_session_runtime_ownership
            SET mode = 'actor_v2',
                status = 'active',
                pending_mode = '',
                generation = ?,
                admission_fence_id = '',
                admission_fence_generation = 0,
                migration_reason = ?,
                requested_by = ?,
                updated_at = ?
            WHERE profile_id = ?
              AND session_id = ?
              AND mode = 'legacy'
              AND status = 'migrating'
              AND pending_mode = 'actor_v2'
              AND generation = ?
            """,
            (
                next_generation,
                normalized_reason,
                requester,
                observed_at,
                barrier.key.profile_id,
                barrier.key.session_id,
                current.generation,
            ),
        )
        if updated.rowcount != 1:
            raise AgentRuntimeOwnershipGenerationConflict(
                "ownership changed while holder-fenced handoff was completing"
            )
        self._refence_actor_state(
            conn,
            barrier.key,
            expected_generation=current.generation,
            target_generation=next_generation,
            now=observed_at,
            release_leases=False,
        )
        self._refence_routing_state(
            conn,
            barrier.key,
            expected_generation=current.generation,
            target_generation=next_generation,
            now=observed_at,
            release_leases=False,
        )
        completed = AgentRuntimeOwnership(
            key=barrier.key,
            legacy_session_id=current.legacy_session_id,
            mode=AgentRuntimeOwnershipMode.ACTOR_V2,
            status=AgentRuntimeOwnershipStatus.ACTIVE,
            generation=next_generation,
            selection_reason=current.selection_reason,
            migration_reason=normalized_reason,
            requested_by=requester,
            created_at=current.created_at,
            updated_at=observed_at,
            admission_fence_id="",
            admission_fence_generation=0,
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

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._db.actor_v2_migration_barriers.require_no_active_for_key_in_transaction(
                conn,
                key,
            )
            return self.abort_migration_in_transaction(
                conn,
                key,
                expected_generation=expected_generation,
                reason=reason,
                requested_by=requested_by,
            )

    def abort_migration_in_transaction(
        self,
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
        reason: str,
        requested_by: str = "",
        now: float | None = None,
    ) -> AgentRuntimeOwnership:
        """Abort one migration inside a caller-owned write transaction.

        A higher-level fenced controller may use this method only after it has
        validated its own exact holder capability. Generic callers must use
        :meth:`abort_migration`, which rejects active Actor v2 barriers.
        """

        normalized_reason = _required_reason(reason)
        requester = str(requested_by or "").strip()
        observed_at = self._clock() if now is None else now
        current = self._required(conn, key)
        self._require_generation(current, expected_generation)
        if (
            current.status is not AgentRuntimeOwnershipStatus.MIGRATING
            or current.pending_mode is None
        ):
            raise AgentRuntimeOwnershipMigrationConflict(
                "ownership is not in a migration that can be aborted"
            )
        if (
            current.mode is AgentRuntimeOwnershipMode.ACTOR_V2
            or current.pending_mode is AgentRuntimeOwnershipMode.ACTOR_V2
        ):
            self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
                conn
            )
        self._validate_external_action_receipts_for_transition(
            conn,
            key,
            now=observed_at,
        )
        if current.mode is AgentRuntimeOwnershipMode.ACTOR_V2:
            self._validate_actor_admission_fence(conn, current)
            self._validate_review_execution_migration_boundary(
                conn,
                key,
                expected_generation=current.generation - 1,
            )
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
                observed_at,
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
                now=observed_at,
                release_leases=True,
            )
        self._refence_routing_state(
            conn,
            key,
            expected_generation=current.generation,
            target_generation=next_generation,
            now=observed_at,
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
            updated_at=observed_at,
            admission_fence_id=current.admission_fence_id,
            admission_fence_generation=current.admission_fence_generation,
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
        expected_admission_fence_id: str | None = None,
        expected_admission_fence_generation: int | None = None,
    ) -> AgentRuntimeOwnership:
        """Validate active actor ownership using the caller's transaction.

        A fenced owner additionally requires the matching committed admission
        fence to remain live.  Callers carrying an outbox/job fence can provide
        it explicitly so a row from another actor incarnation is never reused.
        """

        self._db.actor_v2_canary_isolation_leases.require_no_active_isolation_in_transaction(
            conn
        )
        ownership = self._required(conn, key)
        if expected_generation is not None:
            self._require_generation(ownership, expected_generation)
        if not ownership.actor_v2_active:
            raise AgentRuntimeOwnershipRequired(f"active actor_v2 ownership is required for {key}")
        self._validate_actor_admission_fence(
            conn,
            ownership,
            expected_admission_fence_id=expected_admission_fence_id,
            expected_admission_fence_generation=expected_admission_fence_generation,
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

    def _validate_existing_admission_claim(
        self,
        conn: Connection,
        ownership: AgentRuntimeOwnership,
        *,
        admission_grant: ActorV2AdmissionGrant | None,
        now: float,
    ) -> None:
        """Require an idempotent Actor claim to retain its original fence proof."""

        if admission_grant is None:
            self._validate_actor_admission_fence(conn, ownership)
            return
        if not ownership.has_admission_fence:
            raise ActorV2AdmissionFenceConflict(
                "idempotent ownership claim cannot attach an admission fence"
            )
        if (
            ownership.admission_fence_id != admission_grant.fence.fence_id
            or ownership.admission_fence_generation != admission_grant.fence.generation
        ):
            raise ActorV2AdmissionFenceConflict(
                "idempotent ownership claim changed the admission fence"
            )
        committed = self._db.actor_v2_admission_fences.require_live_holder_in_transaction(
            conn,
            admission_grant,
            now=now,
        )
        if committed.status is not ActorV2AdmissionFenceStatus.COMMITTED:
            raise ActorV2AdmissionFenceConflict(
                "idempotent ownership claim requires a committed admission fence"
            )

    def _validate_actor_admission_fence(
        self,
        conn: Connection,
        ownership: AgentRuntimeOwnership,
        *,
        expected_admission_fence_id: str | None = None,
        expected_admission_fence_generation: int | None = None,
    ) -> None:
        """Validate one optional owner fence and an optional caller-held identity."""

        if (expected_admission_fence_id is None) != (
            expected_admission_fence_generation is None
        ):
            raise ValueError(
                "expected admission fence id and generation must be provided together"
            )
        if expected_admission_fence_id is not None:
            expected_id = str(expected_admission_fence_id or "").strip()
            expected_generation = expected_admission_fence_generation
            if isinstance(expected_generation, bool) or not isinstance(
                expected_generation,
                int,
            ):
                raise ValueError("expected admission fence generation must be an integer")
            if bool(expected_id) != bool(expected_generation):
                raise ValueError(
                    "expected admission fence id and positive generation must be "
                    "provided together"
                )
            if expected_generation < 0:
                raise ValueError("expected admission fence generation must not be negative")
            if (
                ownership.admission_fence_id != expected_id
                or ownership.admission_fence_generation != expected_generation
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "ownership admission fence differs from durable work"
                )
        if not ownership.has_admission_fence:
            return
        self._db.actor_v2_admission_fences.require_committed_in_transaction(
            conn,
            key=ownership.key,
            fence_id=ownership.admission_fence_id,
            generation=ownership.admission_fence_generation,
        )

    @staticmethod
    def _retarget_reserved_admission_routing_jobs(
        conn: Connection,
        key: SessionKey,
        *,
        fence_id: str,
        fence_generation: int,
        ownership_generation: int,
        now: float,
    ) -> None:
        """Attach reserved ingress work to its atomically committed owner."""

        blocked = conn.execute(
            """
            SELECT routing_job_id
            FROM message_routing_jobs
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = 0
              AND admission_fence_id = ?
              AND admission_fence_generation = ?
              AND (
                  status != 'pending'
                  OR claim_id != ''
                  OR lease_owner != ''
                  OR lease_until IS NOT NULL
              )
            LIMIT 1
            """,
            (
                key.profile_id,
                key.session_id,
                fence_id,
                fence_generation,
            ),
        ).fetchone()
        if blocked is not None:
            raise ActorV2AdmissionFenceConflict(
                "reserved ingress job has an unexpected live routing claim: "
                + str(blocked["routing_job_id"])
            )
        conn.execute(
            """
            UPDATE message_routing_jobs
            SET ownership_generation = ?, updated_at = ?
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = 0
              AND admission_fence_id = ?
              AND admission_fence_generation = ?
              AND status = 'pending'
            """,
            (
                ownership_generation,
                now,
                key.profile_id,
                key.session_id,
                fence_id,
                fence_generation,
            ),
        )

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
        evidence = ", ".join(f"{row['idempotency_key']}:{row['status']}" for row in rows)
        raise AgentRuntimeOwnershipMigrationConflict(
            "live external-action receipts block ownership transition: " + evidence
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
            "agent_review_cancellation_gates",
            "agent_review_execution_runs",
            "agent_model_execution_runs",
            "agent_model_execution_cancellation_gates",
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
                extra_predicate = " AND NOT (kind = ? AND source = ?)"
                parameters += (
                    _TYPED_RECOVERY_EVENT_KIND,
                    _TYPED_RECOVERY_EVENT_SOURCE,
                )
                extra_predicate += _TERMINAL_FENCED_MAILBOX_HANDOFF_PREDICATE
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
                mismatched.append(f"{table_name}:generation={int(row['ownership_generation'])}")
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
                mismatched.append(f"{table_name}:generation={int(row['ownership_generation'])}")
        if mismatched:
            raise AgentRuntimeOwnershipGenerationConflict(
                "target actor state is not fenced by the migrating generation: "
                + ", ".join(mismatched)
            )
        AgentRuntimeOwnershipRepository._validate_review_execution_migration_boundary(
            conn,
            key,
            expected_generation=expected_generation,
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
                "status = 'processing' OR lease_owner != '' OR "
                "lease_until IS NOT NULL OR "
                "(status IN ('pending', 'cancelled') AND claim_id != '')",
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
    def _validate_mailbox_handoff_refence_boundary(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        """Require every refenced mailbox to have terminal immutable handoff evidence.

        A mailbox identity becomes immutable as soon as a handoff sidecar
        exists.  Refencing an unproven, pending, or claimed source row would
        either rewrite its evidence generation or leave a future typed wake
        attached to the wrong Actor incarnation.  Typed recovery deliveries
        retain their existing certificate-specific migration boundary.

        Missing sidecars are treated as untrusted historical evidence.  Every
        runtime mailbox producer now writes the sidecar in the same candidate
        transaction, so a missing row at a migration boundary is either old
        history or a partial/corrupt write and must block refencing.
        """

        row = conn.execute(
            """
            SELECT mailbox.mailbox_id,
                   mailbox.event_id,
                   COALESCE(handoff.evidence_state, 'missing') AS evidence_state,
                   COALESCE(handoff.state, 'missing') AS handoff_state
            FROM agent_session_mailbox AS mailbox
            LEFT JOIN agent_session_mailbox_handoffs AS handoff
              ON handoff.mailbox_id = mailbox.mailbox_id
            WHERE mailbox.profile_id = ?
              AND mailbox.session_id = ?
              AND mailbox.ownership_generation = ?
              AND NOT (mailbox.kind = ? AND mailbox.source = ?)
              AND (
                  COALESCE(handoff.evidence_state, 'missing') != 'fenced'
                  OR COALESCE(handoff.state, 'missing') != 'settled'
              )
            ORDER BY mailbox.mailbox_id
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
        if row is not None:
            raise AgentRuntimeOwnershipMigrationConflict(
                "mailbox handoff blocks actor refence: "
                f"mailbox_id={int(row['mailbox_id'])}, "
                f"event_id={str(row['event_id'])}, "
                f"evidence_state={str(row['evidence_state'])}, "
                f"state={str(row['handoff_state'])}; "
                "only a fenced settled handoff may remain historical"
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
        AgentRuntimeOwnershipRepository._validate_review_execution_migration_boundary(
            conn,
            key,
            expected_generation=expected_generation,
        )
        AgentRuntimeOwnershipRepository._validate_mailbox_handoff_refence_boundary(
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
                  AND NOT EXISTS (
                      SELECT 1
                      FROM agent_session_mailbox_handoffs AS handoff
                      WHERE handoff.mailbox_id = agent_session_mailbox.mailbox_id
                        AND handoff.evidence_state = 'fenced'
                        AND handoff.state = 'settled'
                  )
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
                    claim_id = CASE
                        WHEN status IN ('completed', 'failed') THEN claim_id
                        ELSE ''
                    END,
                    lease_owner = '', lease_until = NULL,
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
            "agent_review_cancellation_gates",
            "agent_review_execution_runs",
            "agent_model_execution_runs",
            "agent_model_execution_cancellation_gates",
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
                extra_predicate += _TERMINAL_FENCED_MAILBOX_HANDOFF_PREDICATE
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
            "agent_review_cancellation_gates",
            "agent_review_execution_runs",
            "agent_model_execution_runs",
            "agent_model_execution_cancellation_gates",
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
                extra_predicate += _TERMINAL_FENCED_MAILBOX_HANDOFF_PREDICATE
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
                f"open typed recovery case blocks ownership migration: {open_case['case_id']}"
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
    def _validate_review_execution_migration_boundary(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        """Require durable model execution evidence to be quiescent before refencing.

        A review handler can continue past an expired outbox lease in a remote
        process. Its durable execution witness is the only authority that can
        block a migration from turning that lease into a second model call.
        """

        for table_name in (
            "agent_review_cancellation_gates",
            "agent_review_execution_runs",
            "agent_model_execution_cancellation_gates",
        ):
            stale = conn.execute(
                f"""
                SELECT ownership_generation
                FROM {table_name}
                WHERE profile_id = ?
                  AND session_id = ?
                  AND ownership_generation != ?
                LIMIT 1
                """,
                (key.profile_id, key.session_id, expected_generation),
            ).fetchone()
            if stale is not None:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "review execution state contains a stale ownership generation in "
                    f"{table_name}: {int(stale['ownership_generation'])}"
                )

        stale_model_witness = conn.execute(
            """
            SELECT ownership_generation
            FROM agent_model_execution_runs
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation != ?
            LIMIT 1
            """,
            (key.profile_id, key.session_id, expected_generation),
        ).fetchone()
        if stale_model_witness is not None:
            raise AgentRuntimeOwnershipGenerationConflict(
                "model execution state contains a stale ownership generation in "
                "agent_model_execution_runs: "
                f"{int(stale_model_witness['ownership_generation'])}"
            )

        live_witness = conn.execute(
            """
            SELECT review_effect_id, claim_id, ownership_generation,
                   execution_status
            FROM agent_review_execution_runs
            WHERE profile_id = ?
              AND session_id = ?
              AND execution_status IN ('running', 'unknown')
            ORDER BY run_seq
            LIMIT 1
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        if live_witness is not None:
            witness_status = str(live_witness["execution_status"])
            raise AgentRuntimeOwnershipMigrationConflict(
                f"{witness_status} review execution witness blocks ownership migration: "
                f"{live_witness['review_effect_id']}:{live_witness['claim_id']}:"
                f"generation={int(live_witness['ownership_generation'])}"
            )

        live_model_witness = conn.execute(
            """
            SELECT effect_id, claim_id, ownership_generation, execution_status
            FROM agent_model_execution_runs
            WHERE profile_id = ?
              AND session_id = ?
              AND execution_status IN ('running', 'unknown')
            ORDER BY run_seq
            LIMIT 1
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        if live_model_witness is not None:
            witness_status = str(live_model_witness["execution_status"])
            raise AgentRuntimeOwnershipMigrationConflict(
                f"{witness_status} model execution witness blocks ownership migration: "
                f"{live_model_witness['effect_id']}:{live_model_witness['claim_id']}:"
                f"generation={int(live_model_witness['ownership_generation'])}"
            )

        terminal_runs = conn.execute(
            """
            SELECT review_effect_id, review_operation_id, review_effect_kind,
                   review_contract_version, review_contract_signature,
                   claim_id, worker_id, execution_status, finished_at
            FROM agent_review_execution_runs
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            ORDER BY run_seq
            """,
            (key.profile_id, key.session_id, expected_generation),
        ).fetchall()
        for run in terminal_runs:
            run_status = str(run["execution_status"])
            if (
                run_status not in {"finished", "cancelled"}
                or run["finished_at"] is None
                or str(run["review_effect_kind"]) != _REVIEW_EFFECT_KIND
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "terminal review execution witness state changed: "
                    f"{run['review_effect_id']}:{run['claim_id']}"
                )
            target = conn.execute(
                """
                SELECT ownership_generation, operation_id, kind,
                       contract_version, contract_signature, status,
                       claim_id, lease_owner, lease_until, completed_at
                FROM agent_effect_outbox
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                """,
                (key.profile_id, key.session_id, str(run["review_effect_id"])),
            ).fetchone()
            if target is None or (
                int(target["ownership_generation"]) != expected_generation
                or str(target["operation_id"]) != str(run["review_operation_id"])
                or str(target["kind"]) != str(run["review_effect_kind"])
                or int(target["contract_version"]) != int(run["review_contract_version"])
                or str(target["contract_signature"]) != str(run["review_contract_signature"])
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "terminal review execution witness target identity changed: "
                    f"{run['review_effect_id']}:{run['claim_id']}"
                )
            if run_status != "cancelled":
                # ``finished`` only proves that one physical task ended.  The
                # executor writes it before it knows whether the outbox will
                # settle or retry, so binding it to the target's current claim
                # would permanently block normal retry history.
                continue
            if (
                str(target["status"]) != "cancelled"
                or target["claim_id"]
                or target["lease_owner"]
                or target["lease_until"] is not None
                or target["completed_at"] is None
            ):
                raise AgentRuntimeOwnershipMigrationConflict(
                    "cancelled review execution witness awaits durable "
                    "cancellation settlement: "
                    f"{run['review_effect_id']}:{run['claim_id']}"
                )
            gate = conn.execute(
                """
                SELECT 1
                FROM agent_review_cancellation_gates
                WHERE profile_id = ?
                  AND session_id = ?
                  AND ownership_generation = ?
                  AND review_effect_id = ?
                """,
                (
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                    str(run["review_effect_id"]),
                ),
            ).fetchone()
            if gate is None:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "cancelled review execution witness lacks a cancellation gate: "
                    f"{run['review_effect_id']}:{run['claim_id']}"
                )

        terminal_model_runs = conn.execute(
            """
            SELECT effect_id, operation_id, effect_kind, contract_version,
                   contract_signature, claim_id, worker_id, execution_status,
                   finished_at
            FROM agent_model_execution_runs
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            ORDER BY run_seq
            """,
            (key.profile_id, key.session_id, expected_generation),
        ).fetchall()
        for run in terminal_model_runs:
            if (
                str(run["execution_status"]) != "finished"
                or run["finished_at"] is None
                or str(run["effect_kind"]) not in _MODEL_EXECUTION_EFFECT_KINDS
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "terminal model execution witness state changed: "
                    f"{run['effect_id']}:{run['claim_id']}"
                )
            target = conn.execute(
                """
                SELECT ownership_generation, operation_id, kind,
                       contract_version, contract_signature
                FROM agent_effect_outbox
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                """,
                (key.profile_id, key.session_id, str(run["effect_id"])),
            ).fetchone()
            if target is None or (
                int(target["ownership_generation"]) != expected_generation
                or str(target["operation_id"]) != str(run["operation_id"])
                or str(target["kind"]) != str(run["effect_kind"])
                or int(target["contract_version"]) != int(run["contract_version"])
                or str(target["contract_signature"]) != str(run["contract_signature"])
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "terminal model execution witness target identity changed: "
                    f"{run['effect_id']}:{run['claim_id']}"
                )

        gates = conn.execute(
            """
            SELECT cancellation_effect_id, request_event_id, review_operation_id,
                   review_effect_id, review_effect_kind, review_contract_version,
                   review_contract_signature, gate_status, target_effect_status,
                   target_effect_claim_id, target_effect_attempt_count,
                   target_effect_terminal_at
            FROM agent_review_cancellation_gates
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            ORDER BY gate_seq
            """,
            (key.profile_id, key.session_id, expected_generation),
        ).fetchall()
        for gate in gates:
            cancellation_effect_id = str(gate["cancellation_effect_id"])
            review_effect_id = str(gate["review_effect_id"])
            review_operation_id = str(gate["review_operation_id"])
            target = conn.execute(
                """
                SELECT ownership_generation, operation_id, kind,
                       contract_version, contract_signature, status,
                       attempt_count, claim_id, lease_owner, lease_until,
                       completed_at
                FROM agent_effect_outbox
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                """,
                (key.profile_id, key.session_id, review_effect_id),
            ).fetchone()
            if target is None or (
                int(target["ownership_generation"]) != expected_generation
                or str(target["operation_id"]) != review_operation_id
                or str(gate["review_effect_kind"]) != "run_review_workflow"
                or str(target["kind"]) != str(gate["review_effect_kind"])
                or int(target["contract_version"]) != int(gate["review_contract_version"])
                or str(target["contract_signature"]) != str(gate["review_contract_signature"])
                or str(target["status"]) != str(gate["target_effect_status"])
                or int(target["attempt_count"]) != int(gate["target_effect_attempt_count"])
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    f"review cancellation gate target identity changed: {cancellation_effect_id}"
                )

            operation = conn.execute(
                """
                SELECT ownership_generation, kind, status
                FROM agent_session_operations
                WHERE profile_id = ?
                  AND session_id = ?
                  AND operation_id = ?
                """,
                (key.profile_id, key.session_id, review_operation_id),
            ).fetchone()
            if operation is None or (
                int(operation["ownership_generation"]) != expected_generation
                or str(operation["kind"]) != "review"
                or str(operation["status"]) != "superseded"
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    f"review cancellation gate operation identity changed: {cancellation_effect_id}"
                )

            control = conn.execute(
                """
                SELECT ownership_generation, event_id, operation_id, kind,
                       idempotency_key, contract_version, contract_signature,
                       payload_json, status, claim_id, lease_owner, lease_until,
                       completed_at
                FROM agent_effect_outbox
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                """,
                (key.profile_id, key.session_id, cancellation_effect_id),
            ).fetchone()
            if control is None or (
                int(control["ownership_generation"]) != expected_generation
                or str(control["event_id"]) != str(gate["request_event_id"])
                or str(control["operation_id"]) != review_operation_id
                or str(control["kind"]) != _CANCEL_REVIEW_EFFECT_KIND
                or str(control["idempotency_key"]) != cancellation_effect_id
                or int(control["contract_version"]) != _CANCEL_REVIEW_EFFECT_VERSION
                or str(control["contract_signature"]) != _CANCEL_REVIEW_EFFECT_V2_SIGNATURE
                or not _review_cancellation_control_payload_matches(
                    control["payload_json"],
                    review_operation_id=review_operation_id,
                    review_effect_id=review_effect_id,
                    review_effect_kind=str(gate["review_effect_kind"]),
                    review_contract_version=int(gate["review_contract_version"]),
                    review_contract_signature=str(gate["review_contract_signature"]),
                    ownership_generation=expected_generation,
                )
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    f"review cancellation gate control identity changed: {cancellation_effect_id}"
                )

            gate_status = str(gate["gate_status"])
            target_status = str(target["status"])
            if gate_status == "requested":
                if (
                    target_status != "processing"
                    or gate["target_effect_terminal_at"] is not None
                    or str(target["claim_id"]) != str(gate["target_effect_claim_id"])
                    or not str(target["claim_id"])
                    or not str(target["lease_owner"])
                    or target["lease_until"] is None
                ):
                    raise AgentRuntimeOwnershipGenerationConflict(
                        "requested review cancellation gate state changed: "
                        f"{cancellation_effect_id}"
                    )
                raise AgentRuntimeOwnershipMigrationConflict(
                    "requested review cancellation gate blocks ownership migration: "
                    f"{cancellation_effect_id}"
                )
            if gate_status == "cancelled":
                if (
                    target_status != "cancelled"
                    or target["completed_at"] != gate["target_effect_terminal_at"]
                    or target["claim_id"]
                    or target["lease_owner"]
                    or target["lease_until"] is not None
                ):
                    raise AgentRuntimeOwnershipGenerationConflict(
                        "cancelled review cancellation gate state changed: "
                        f"{cancellation_effect_id}"
                    )
            elif gate_status == "terminal":
                if (
                    target_status not in _TERMINAL_EFFECT_STATUSES
                    or target["completed_at"] != gate["target_effect_terminal_at"]
                    or target["completed_at"] is None
                    or target["lease_owner"]
                    or target["lease_until"] is not None
                    or (target_status == "cancelled" and str(target["claim_id"]))
                    or (
                        target_status in {"completed", "failed"}
                        and str(target["claim_id"]) != str(gate["target_effect_claim_id"])
                    )
                ):
                    raise AgentRuntimeOwnershipGenerationConflict(
                        f"terminal review cancellation gate state changed: {cancellation_effect_id}"
                    )
            else:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "review cancellation gate has an unknown status: "
                    f"{cancellation_effect_id}:{gate_status}"
                )
            if (
                str(control["status"]) not in _TERMINAL_EFFECT_STATUSES
                or str(control["lease_owner"])
                or control["lease_until"] is not None
                or control["completed_at"] is None
            ):
                raise AgentRuntimeOwnershipMigrationConflict(
                    "review cancellation control remains live during ownership "
                    f"migration: {cancellation_effect_id}:{control['status']}"
                )
        AgentRuntimeOwnershipRepository._validate_model_execution_cancellation_migration_boundary(
            conn,
            key,
            expected_generation=expected_generation,
        )

    @staticmethod
    def _validate_model_execution_cancellation_migration_boundary(
        conn: Connection,
        key: SessionKey,
        *,
        expected_generation: int,
    ) -> None:
        """Reject migration while a v3 generic model cancellation is unresolved."""

        gates = conn.execute(
            """
            SELECT cancellation_effect_id, request_event_id,
                   target_operation_id, target_effect_id, target_effect_kind,
                   target_contract_version, target_contract_signature,
                   target_effect_status, target_claim_id, target_worker_id,
                   target_effect_attempt_count, target_execution_status,
                   gate_status, target_effect_terminal_at, blocker_code
            FROM agent_model_execution_cancellation_gates
            WHERE profile_id = ? AND session_id = ?
              AND ownership_generation = ?
            ORDER BY gate_seq
            """,
            (key.profile_id, key.session_id, expected_generation),
        ).fetchall()
        for gate in gates:
            cancellation_effect_id = str(gate["cancellation_effect_id"])
            target_effect_id = str(gate["target_effect_id"])
            target_operation_id = str(gate["target_operation_id"])
            target = conn.execute(
                """
                SELECT ownership_generation, operation_id, kind,
                       contract_version, contract_signature, status,
                       attempt_count, claim_id, lease_owner, lease_until,
                       completed_at
                FROM agent_effect_outbox
                WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                """,
                (key.profile_id, key.session_id, target_effect_id),
            ).fetchone()
            if target is None or (
                int(target["ownership_generation"]) != expected_generation
                or str(target["operation_id"]) != target_operation_id
                or str(target["kind"]) != str(gate["target_effect_kind"])
                or int(target["contract_version"])
                != int(gate["target_contract_version"])
                or str(target["contract_signature"])
                != str(gate["target_contract_signature"])
                or str(target["status"]) != str(gate["target_effect_status"])
                or int(target["attempt_count"])
                != int(gate["target_effect_attempt_count"])
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "model execution cancellation gate target identity changed: "
                    + cancellation_effect_id
                )
            operation = conn.execute(
                """
                SELECT ownership_generation, kind, status
                FROM agent_session_operations
                WHERE profile_id = ? AND session_id = ? AND operation_id = ?
                """,
                (key.profile_id, key.session_id, target_operation_id),
            ).fetchone()
            if operation is None or (
                int(operation["ownership_generation"]) != expected_generation
                or str(operation["kind"]) != "idle_review_planning"
                or str(operation["status"]) != "superseded"
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "model execution cancellation gate operation identity changed: "
                    + cancellation_effect_id
                )
            control = conn.execute(
                """
                SELECT ownership_generation, event_id, operation_id, kind,
                       idempotency_key, contract_version, contract_signature,
                       payload_json, status, claim_id, lease_owner, lease_until,
                       completed_at
                FROM agent_effect_outbox
                WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                """,
                (key.profile_id, key.session_id, cancellation_effect_id),
            ).fetchone()
            if control is None or (
                int(control["ownership_generation"]) != expected_generation
                or str(control["event_id"]) != str(gate["request_event_id"])
                or str(control["operation_id"]) != target_operation_id
                or str(control["kind"]) != _CANCEL_MODEL_EXECUTION_EFFECT_KIND
                or str(control["idempotency_key"]) != cancellation_effect_id
                or int(control["contract_version"])
                != _CANCEL_MODEL_EXECUTION_EFFECT_VERSION
                or str(control["contract_signature"])
                != _CANCEL_MODEL_EXECUTION_EFFECT_V3_SIGNATURE
                or not _model_execution_cancellation_control_payload_matches(
                    control["payload_json"],
                    target_operation_id=target_operation_id,
                    target_effect_id=target_effect_id,
                    target_effect_kind=str(gate["target_effect_kind"]),
                    target_contract_version=int(gate["target_contract_version"]),
                    target_contract_signature=str(gate["target_contract_signature"]),
                    ownership_generation=expected_generation,
                )
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "model execution cancellation gate control identity changed: "
                    + cancellation_effect_id
                )
            gate_status = str(gate["gate_status"])
            target_execution_status = str(gate["target_execution_status"])
            if gate_status in {"requested", "cancelled", "blocked"}:
                if gate_status == "blocked" and target_execution_status != "unknown":
                    raise AgentRuntimeOwnershipGenerationConflict(
                        "blocked model execution cancellation gate lost unknown evidence: "
                        + cancellation_effect_id
                    )
                raise AgentRuntimeOwnershipMigrationConflict(
                    "unresolved model execution cancellation gate blocks ownership migration: "
                    + cancellation_effect_id
                )
            if gate_status != "terminal":
                raise AgentRuntimeOwnershipGenerationConflict(
                    "model execution cancellation gate has an unknown status: "
                    + cancellation_effect_id
                    + ":"
                    + gate_status
                )
            target_execution_status = str(gate["target_execution_status"])
            if (
                str(target["status"]) not in _TERMINAL_EFFECT_STATUSES
                or str(target["status"]) != str(gate["target_effect_status"])
                or target["lease_owner"]
                or target["lease_until"] is not None
                or target["completed_at"] is None
                or target["completed_at"] != gate["target_effect_terminal_at"]
                or str(gate["blocker_code"])
            ):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "terminal model execution cancellation gate changed state: "
                    + cancellation_effect_id
                )
            if str(target["status"]) == "cancelled":
                if str(target["claim_id"]):
                    raise AgentRuntimeOwnershipGenerationConflict(
                        "terminal model execution cancellation gate retained a "
                        "cancelled target claim: "
                        + cancellation_effect_id
                    )
            elif str(target["claim_id"]) != str(gate["target_claim_id"]):
                raise AgentRuntimeOwnershipGenerationConflict(
                    "terminal model execution cancellation gate target claim changed: "
                    + cancellation_effect_id
                )
            witness_rows = conn.execute(
                """
                SELECT effect_id, operation_id, effect_kind, contract_version,
                       contract_signature, claim_id, worker_id, execution_status,
                       finished_at
                FROM agent_model_execution_runs
                WHERE profile_id = ? AND session_id = ?
                  AND ownership_generation = ? AND effect_id = ?
                ORDER BY run_seq
                """,
                (
                    key.profile_id,
                    key.session_id,
                    expected_generation,
                    target_effect_id,
                ),
            ).fetchall()
            if target_execution_status == "none":
                if witness_rows:
                    raise AgentRuntimeOwnershipGenerationConflict(
                        "terminal model execution cancellation gate unexpectedly "
                        "retains a witness: "
                        + cancellation_effect_id
                    )
            elif target_execution_status == "finished":
                if len(witness_rows) != 1:
                    raise AgentRuntimeOwnershipGenerationConflict(
                        "terminal model execution cancellation gate finished witness "
                        "is missing or ambiguous: "
                        + cancellation_effect_id
                    )
                witness = witness_rows[0]
                if (
                    str(witness["operation_id"]) != target_operation_id
                    or str(witness["effect_kind"]) != str(gate["target_effect_kind"])
                    or int(witness["contract_version"])
                    != int(gate["target_contract_version"])
                    or str(witness["contract_signature"])
                    != str(gate["target_contract_signature"])
                    or str(witness["claim_id"]) != str(gate["target_claim_id"])
                    or (
                        str(gate["target_worker_id"])
                        and str(witness["worker_id"])
                        != str(gate["target_worker_id"])
                    )
                    or str(witness["execution_status"]) != "finished"
                    or witness["finished_at"] is None
                ):
                    raise AgentRuntimeOwnershipGenerationConflict(
                        "terminal model execution cancellation gate witness changed: "
                        + cancellation_effect_id
                    )
            else:
                raise AgentRuntimeOwnershipGenerationConflict(
                    "terminal model execution cancellation gate retained a live "
                    "execution state: "
                    + cancellation_effect_id
                )
            if (
                str(control["status"]) not in _TERMINAL_EFFECT_STATUSES
                or control["lease_owner"]
                or control["lease_until"] is not None
                or control["completed_at"] is None
            ):
                raise AgentRuntimeOwnershipMigrationConflict(
                    "model execution cancellation control remains live during ownership "
                    "migration: "
                    + cancellation_effect_id
                    + ":"
                    + str(control["status"])
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
            raise AgentRuntimeOwnershipNotFound(f"no runtime ownership exists for {key}")
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
            if (
                conn.execute(
                    """
                SELECT 1 FROM agent_session_aggregates
                WHERE profile_id = ? AND session_id = ? LIMIT 1
                """,
                    (key.profile_id, key.session_id),
                ).fetchone()
                is not None
            ):
                evidence.append("actor_aggregate")
            if (
                conn.execute(
                    """
                SELECT 1 FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ? LIMIT 1
                """,
                    (key.profile_id, key.session_id),
                ).fetchone()
                is not None
            ):
                evidence.append("actor_mailbox")
            for table_name, label in (
                ("agent_review_cancellation_gates", "actor_review_cancellation_gate"),
                ("agent_review_execution_runs", "actor_review_execution_run"),
                ("agent_model_execution_runs", "actor_model_execution_run"),
                (
                    "agent_model_execution_cancellation_gates",
                    "actor_model_execution_cancellation_gate",
                ),
            ):
                if (
                    conn.execute(
                        f"""
                    SELECT 1 FROM {table_name}
                    WHERE profile_id = ? AND session_id = ?
                    LIMIT 1
                    """,
                        (key.profile_id, key.session_id),
                    ).fetchone()
                    is not None
                ):
                    evidence.append(label)
            if (
                conn.execute(
                    """
                SELECT 1 FROM message_routing_jobs
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                LIMIT 1
                """,
                    (key.profile_id, key.session_id),
                ).fetchone()
                is not None
            ):
                evidence.append("actor_message_routing_job")
            if (
                conn.execute(
                    """
                SELECT 1 FROM agent_route_outbox
                WHERE profile_id = ? AND session_id = ?
                  AND status IN ('pending', 'processing')
                LIMIT 1
                """,
                    (key.profile_id, key.session_id),
                ).fetchone()
                is not None
            ):
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
                    "SELECT 1 FROM agent_scheduler_states WHERE session_id = ? LIMIT 1",
                    "legacy_scheduler_state",
                ),
                (
                    "SELECT 1 FROM agent_unread_messages WHERE session_id = ? LIMIT 1",
                    "legacy_unread_messages",
                ),
                (
                    "SELECT 1 FROM agent_unread_ranges WHERE session_id = ? LIMIT 1",
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
        admission_fence_id=str(row["admission_fence_id"] or ""),
        admission_fence_generation=int(row["admission_fence_generation"] or 0),
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
