"""SQLite adapter for the durable session-actor effect executor."""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    EffectContractAuthorityError,
    builtin_effect_contract_authority,
    resolved_outcome_fence_fields,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    ClaimedEffect,
    DurableEffectEnvelope,
    DurableEffectStatus,
    EffectClaimLost,
    EffectSettlementResult,
    EffectSettlementStatus,
    failure_event_id,
    skipped_event_id,
)
from shinbot.agent.runtime.session_actor.events import SessionEventEnvelope
from shinbot.agent.runtime.session_actor.external_actions import ExternalActionKind
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


class EffectStoreConflict(RuntimeError):
    """Raised when a durable effect or completion id changes identity."""


_EXTERNAL_ACTION_EFFECT_KINDS = tuple(kind.value for kind in ExternalActionKind)


class SQLiteDurableEffectStore:
    """Lease-fenced SQLite outbox with atomic mailbox settlement."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        lease_seconds: float = 30.0,
        clock: Callable[[], float] | None = None,
        contract_authority: EffectContractAuthority | None = None,
    ) -> None:
        """Initialize the durable effect store.

        Args:
            database: Initialized ShinBot database manager.
            lease_seconds: Claim duration before another worker may reclaim it.
            clock: Injectable wall clock for tests.
            contract_authority: Immutable policy snapshot allowed to settle
                effects. Omitting it uses the complete built-in Actor v2 graph.
        """

        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        self._database = database
        self._lease_seconds = float(lease_seconds)
        self._clock = clock or time.time
        if contract_authority is not None and not isinstance(
            contract_authority,
            EffectContractAuthority,
        ):
            raise TypeError("contract_authority must be an EffectContractAuthority")
        self._contract_authority = (
            contract_authority or builtin_effect_contract_authority()
        )

    async def claim_next(
        self,
        *,
        worker_id: str,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
    ) -> ClaimedEffect | None:
        """Claim the oldest available or expired effect with a fresh claim id."""

        normalized_worker_id = str(worker_id or "").strip()
        if not normalized_worker_id:
            raise ValueError("worker_id must not be empty")
        claim_id = uuid.uuid4().hex
        filter_sql, filter_params, priority_sql, priority_params = (
            _contract_filter_sql(
                effect_contracts,
                excluded_effect_contracts,
                table_alias="effect",
            )
        )
        action_order_sql, action_order_params = _external_action_order_gate_sql(
            "effect"
        )
        if effect_contracts == ():
            return None
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _nonnegative_finite(self._clock(), field_name="clock")
            lease_until = _nonnegative_finite(
                now + self._lease_seconds,
                field_name="lease_until",
            )
            row = conn.execute(
                f"""
                SELECT effect.*,
                       COALESCE(source.trace_id, '') AS source_trace_id
                FROM agent_effect_outbox AS effect
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = effect.profile_id
                 AND ownership.session_id = effect.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = effect.ownership_generation
                LEFT JOIN agent_session_mailbox AS source
                  ON source.profile_id = effect.profile_id
                 AND source.session_id = effect.session_id
                 AND source.event_id = effect.event_id
                WHERE (
                    (
                        effect.status = 'pending'
                        AND effect.available_at <= ?
                    ) OR (
                        effect.status = 'processing'
                        AND COALESCE(effect.lease_until, 0) <= ?
                    )
                )
                AND effect.ownership_generation >= 1
                {filter_sql}
                {action_order_sql}
                ORDER BY {priority_sql} effect.effect_seq ASC
                LIMIT 1
                """,
                (
                    now,
                    now,
                    *filter_params,
                    *action_order_params,
                    *priority_params,
                ),
            ).fetchone()
            if row is None:
                return None
            ownership_generation = int(row["ownership_generation"])
            _require_effect_ownership(
                self._database,
                conn,
                SessionKey(str(row["profile_id"]), str(row["session_id"])),
                expected_generation=ownership_generation,
            )
            updated = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = 'processing',
                    attempt_count = attempt_count + 1,
                    claim_id = ?,
                    lease_owner = ?,
                    lease_until = ?,
                    updated_at = ?,
                    last_error = ''
                WHERE effect_seq = ?
                  AND ownership_generation = ?
                  AND (
                      (status = 'pending' AND available_at <= ?)
                      OR (
                          status = 'processing'
                          AND COALESCE(lease_until, 0) <= ?
                      )
                  )
                  AND kind = ?
                  AND contract_version = ?
                  AND contract_signature = ?
                """,
                (
                    claim_id,
                    normalized_worker_id,
                    lease_until,
                    now,
                    row["effect_seq"],
                    ownership_generation,
                    now,
                    now,
                    row["kind"],
                    row["contract_version"],
                    row["contract_signature"],
                ),
            )
            if updated.rowcount != 1:
                return None
            claimed_row = conn.execute(
                """
                SELECT effect.*,
                       COALESCE(source.trace_id, '') AS source_trace_id
                FROM agent_effect_outbox AS effect
                LEFT JOIN agent_session_mailbox AS source
                  ON source.profile_id = effect.profile_id
                 AND source.session_id = effect.session_id
                 AND source.event_id = effect.event_id
                WHERE effect.effect_seq = ?
                """,
                (row["effect_seq"],),
            ).fetchone()
        assert claimed_row is not None
        return ClaimedEffect(
            claim_id=claim_id,
            effect=_effect_from_row(claimed_row),
            worker_id=normalized_worker_id,
            attempt_count=int(claimed_row["attempt_count"]),
            claimed_at=now,
            lease_expires_at=lease_until,
        )

    async def renew_lease(self, claim: ClaimedEffect) -> ClaimedEffect:
        """Extend a current, unexpired effect lease without changing its fence."""

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.effect.ownership_generation
            )
            _require_effect_ownership(
                self._database,
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            now = _nonnegative_finite(self._clock(), field_name="clock")
            lease_until = _nonnegative_finite(
                now + self._lease_seconds,
                field_name="lease_until",
            )
            renewed = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET lease_until = ?, updated_at = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    lease_until,
                    now,
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.effect.effect_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                    now,
                ),
            )
            if renewed.rowcount != 1:
                raise EffectClaimLost("effect lease is expired or no longer owned")
        return replace(claim, lease_expires_at=lease_until)

    async def complete_with_event(
        self,
        claim: ClaimedEffect,
        completion_envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...] | None = None,
    ) -> EffectSettlementResult:
        """Atomically complete an effect and insert its mailbox completion.

        ``outcome_fence_fields`` remains a compatibility assertion for older
        callers. The store always resolves the projection from its sealed
        contract authority and rejects a caller value that differs.
        """

        return self._settle_with_event(
            claim,
            completion_envelope,
            target_status=DurableEffectStatus.COMPLETED,
            error="",
            outcome_fence_fields=outcome_fence_fields,
        )

    async def release_for_retry(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        available_at: float,
    ) -> None:
        """Return a current claim to pending at an explicit retry time."""

        await self._release(
            claim,
            error=error,
            available_at=max(
                _nonnegative_finite(self._clock(), field_name="clock"),
                _nonnegative_finite(available_at, field_name="available_at"),
            ),
        )

    async def fail_with_event(
        self,
        claim: ClaimedEffect,
        failure_envelope: SessionEventEnvelope,
        *,
        error: str,
        outcome_fence_fields: tuple[str, ...] | None = None,
    ) -> EffectSettlementResult:
        """Atomically fail an effect and insert its terminal failure event.

        ``outcome_fence_fields`` remains a compatibility assertion for older
        callers. The store always resolves the projection from its sealed
        contract authority and rejects a caller value that differs.
        """

        return self._settle_with_event(
            claim,
            failure_envelope,
            target_status=DurableEffectStatus.FAILED,
            error=error,
            outcome_fence_fields=outcome_fence_fields,
        )

    async def release(self, claim: ClaimedEffect, *, error: str) -> None:
        """Immediately release a live claim so shutdown never strands it."""

        await self._release(claim, error=error, available_at=self._clock())

    async def recover_expired(self, *, worker_id: str) -> int:
        """Release all expired effect leases, regardless of former owner."""

        if not str(worker_id or "").strip():
            raise ValueError("worker_id must not be empty")
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _nonnegative_finite(self._clock(), field_name="clock")
            rows = conn.execute(
                """
                SELECT effect.effect_seq, effect.profile_id, effect.session_id,
                       effect.ownership_generation
                FROM agent_effect_outbox AS effect
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = effect.profile_id
                 AND ownership.session_id = effect.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = effect.ownership_generation
                WHERE effect.ownership_generation >= 1
                  AND effect.status = 'processing'
                  AND COALESCE(effect.lease_until, 0) <= ?
                ORDER BY effect.effect_seq
                """,
                (now,),
            ).fetchall()
            recovered_count = 0
            for row in rows:
                key = SessionKey(str(row["profile_id"]), str(row["session_id"]))
                generation = int(row["ownership_generation"])
                _require_effect_ownership(
                    self._database,
                    conn,
                    key,
                    expected_generation=generation,
                )
                recovered = conn.execute(
                    """
                    UPDATE agent_effect_outbox
                    SET status = 'pending',
                        available_at = MIN(available_at, ?),
                        claim_id = '',
                        lease_owner = '',
                        lease_until = NULL,
                        updated_at = ?,
                        last_error = CASE
                            WHEN last_error = '' THEN 'effect_lease_recovered'
                            ELSE last_error
                        END
                    WHERE effect_seq = ?
                      AND ownership_generation = ?
                      AND status = 'processing'
                      AND COALESCE(lease_until, 0) <= ?
                    """,
                    (now, now, row["effect_seq"], generation, now),
                )
                recovered_count += int(recovered.rowcount)
            return recovered_count

    async def next_available_at(
        self,
        *,
        effect_contracts: tuple[tuple[str, int], ...] | None = None,
        excluded_effect_contracts: tuple[tuple[str, int], ...] = (),
    ) -> float | None:
        """Return the next pending availability or processing lease expiry."""

        filter_sql, filter_params, _priority_sql, _priority_params = (
            _contract_filter_sql(
                effect_contracts,
                excluded_effect_contracts,
                table_alias="agent_effect_outbox",
            )
        )
        action_order_sql, action_order_params = _external_action_order_gate_sql(
            "agent_effect_outbox"
        )
        if effect_contracts == ():
            return None
        with self._database.connect() as conn:
            row = conn.execute(
                f"""
                SELECT MIN(
                    CASE
                        WHEN agent_effect_outbox.status = 'pending'
                        THEN agent_effect_outbox.available_at
                        ELSE agent_effect_outbox.lease_until
                    END
                ) AS next_available_at
                FROM agent_effect_outbox
                JOIN agent_session_runtime_ownership AS ownership
                  ON ownership.profile_id = agent_effect_outbox.profile_id
                 AND ownership.session_id = agent_effect_outbox.session_id
                 AND ownership.mode = 'actor_v2'
                 AND ownership.status = 'active'
                 AND ownership.generation = agent_effect_outbox.ownership_generation
                WHERE agent_effect_outbox.ownership_generation >= 1
                  AND agent_effect_outbox.status IN ('pending', 'processing')
                {filter_sql}
                {action_order_sql}
                """,
                (*filter_params, *action_order_params),
            ).fetchone()
        if row is None or row["next_available_at"] is None:
            return None
        return float(row["next_available_at"])

    async def _release(
        self,
        claim: ClaimedEffect,
        *,
        error: str,
        available_at: float,
    ) -> None:
        normalized_available_at = _nonnegative_finite(
            available_at,
            field_name="available_at",
        )
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.effect.ownership_generation
            )
            _require_effect_ownership(
                self._database,
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            now = _nonnegative_finite(self._clock(), field_name="clock")
            released = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = 'pending',
                    available_at = ?,
                    claim_id = '',
                    lease_owner = '',
                    lease_until = NULL,
                    updated_at = ?,
                    last_error = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                """,
                (
                    normalized_available_at,
                    now,
                    str(error or ""),
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.effect.effect_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                ),
            )
            if released.rowcount != 1:
                raise EffectClaimLost("effect is not owned by this claim")

    def _settle_with_event(
        self,
        claim: ClaimedEffect,
        envelope: SessionEventEnvelope,
        *,
        target_status: DurableEffectStatus,
        error: str,
        outcome_fence_fields: tuple[str, ...] | None,
    ) -> EffectSettlementResult:
        if envelope.key != claim.key:
            raise ValueError("completion event key does not match effect ownership")
        resolved_fence_fields = self._resolve_outcome_fence_fields(
            claim.effect,
            caller_fence_fields=outcome_fence_fields,
        )
        self._validate_settlement_contract(claim.effect, envelope)
        if target_status not in {
            DurableEffectStatus.COMPLETED,
            DurableEffectStatus.FAILED,
        }:
            raise ValueError("effect settlement target must be terminal")
        if target_status is DurableEffectStatus.FAILED:
            self._validate_failure_envelope(
                claim,
                envelope,
                outcome_fence_fields=resolved_fence_fields,
            )
        else:
            self._validate_completion_fences(
                claim.effect,
                envelope,
                outcome_fence_fields=resolved_fence_fields,
            )
        payload_json = _json_dumps(envelope.payload)
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ownership_generation = _persistable_ownership_generation(
                claim.effect.ownership_generation
            )
            if envelope.ownership_generation != ownership_generation:
                raise EffectStoreConflict(
                    "effect settlement ownership_generation changed identity"
                )
            _require_effect_ownership(
                self._database,
                conn,
                claim.key,
                expected_generation=ownership_generation,
            )
            now = _nonnegative_finite(self._clock(), field_name="clock")
            row = conn.execute(
                """
                SELECT * FROM agent_effect_outbox
                WHERE profile_id = ? AND session_id = ? AND effect_id = ?
                """,
                (
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.effect.effect_id,
                ),
            ).fetchone()
            if row is None:
                raise EffectClaimLost("durable effect no longer exists")
            self._validate_effect_identity(row, claim.effect)
            persisted_status = DurableEffectStatus(str(row["status"]))
            if persisted_status == target_status:
                if str(row["claim_id"]) != claim.claim_id:
                    raise EffectClaimLost("a different claim already settled this effect")
                if target_status == DurableEffectStatus.COMPLETED:
                    skipped = self._load_existing_skip(
                        conn,
                        claim.effect,
                        envelope,
                        outcome_fence_fields=resolved_fence_fields,
                    )
                    if skipped is not None:
                        return skipped
                self._validate_persisted_event(conn, envelope, payload_json)
                return EffectSettlementResult(
                    status=EffectSettlementStatus.ALREADY_COMMITTED,
                    effect_id=claim.effect.effect_id,
                    event_id=envelope.event_id,
                    key=claim.key,
                )
            self._validate_live_claim(row, claim, now=now)
            settled_envelope = envelope
            settlement_status = EffectSettlementStatus.COMMITTED
            actual_operation_status = None
            if target_status == DurableEffectStatus.COMPLETED:
                actual_operation_status = self._precondition_failure_status(
                    conn,
                    claim.effect,
                )
            if actual_operation_status is not None:
                settled_envelope = self._skipped_envelope(
                    claim.effect,
                    intended=envelope,
                    actual_operation_status=actual_operation_status,
                    now=now,
                    outcome_fence_fields=resolved_fence_fields,
                )
                payload_json = _json_dumps(settled_envelope.payload)
                settlement_status = EffectSettlementStatus.PRECONDITION_SKIPPED
            self._insert_event(
                conn,
                settled_envelope,
                payload_json=payload_json,
                now=now,
            )
            settled = conn.execute(
                """
                UPDATE agent_effect_outbox
                SET status = ?,
                    lease_owner = '',
                    lease_until = NULL,
                    updated_at = ?,
                    completed_at = ?,
                    last_error = ?
                WHERE profile_id = ?
                  AND session_id = ?
                  AND effect_id = ?
                  AND ownership_generation = ?
                  AND status = 'processing'
                  AND claim_id = ?
                  AND lease_owner = ?
                  AND COALESCE(lease_until, 0) > ?
                """,
                (
                    target_status.value,
                    now,
                    now,
                    str(error or ""),
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.effect.effect_id,
                    ownership_generation,
                    claim.claim_id,
                    claim.worker_id,
                    now,
                ),
            )
            if settled.rowcount != 1:
                raise EffectClaimLost("effect claim changed during settlement")
        return EffectSettlementResult(
            status=settlement_status,
            effect_id=claim.effect.effect_id,
            event_id=settled_envelope.event_id,
            key=claim.key,
        )

    def _resolve_outcome_fence_fields(
        self,
        effect: DurableEffectEnvelope,
        *,
        caller_fence_fields: tuple[str, ...] | None,
    ) -> tuple[str, ...]:
        """Resolve a settlement projection from the immutable contract authority."""

        try:
            contract = self._contract_authority.resolve(
                effect_kind=effect.kind,
                version=effect.contract_version,
                signature=effect.contract_signature,
            )
        except EffectContractAuthorityError as exc:
            raise EffectStoreConflict(
                "effect settlement contract is not authorized: " + str(exc)
            ) from exc
        resolved_fields = resolved_outcome_fence_fields(contract)
        if caller_fence_fields is None:
            return resolved_fields
        if isinstance(caller_fence_fields, str) or not isinstance(
            caller_fence_fields,
            tuple,
        ):
            raise EffectStoreConflict(
                "settlement outcome_fence_fields must be a tuple when supplied"
            )
        if caller_fence_fields != resolved_fields:
            raise EffectStoreConflict(
                "settlement outcome_fence_fields differ from the sealed contract"
            )
        return resolved_fields

    @staticmethod
    def _validate_completion_fences(
        effect: DurableEffectEnvelope,
        envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...],
    ) -> None:
        """Require a successful outcome to retain every persisted fence value."""

        for field_name, expected in effect.outcome_fence_payload(
            outcome_fence_fields
        ).items():
            if (
                field_name not in envelope.payload
                or envelope.payload[field_name] != expected
            ):
                raise EffectStoreConflict(
                    "effect completion changed fence " + field_name
                )

    @staticmethod
    def _precondition_failure_status(
        conn: sqlite3.Connection,
        effect: DurableEffectEnvelope,
    ) -> str | None:
        raw_expected = effect.payload.get("enqueue_only_if_operation_status")
        if raw_expected is None:
            return None
        if not isinstance(raw_expected, list) or not raw_expected:
            raise EffectStoreConflict(
                "enqueue_only_if_operation_status must be a non-empty list"
            )
        expected = {str(value or "").strip() for value in raw_expected}
        if "" in expected:
            raise EffectStoreConflict(
                "enqueue_only_if_operation_status contains an empty status"
            )
        disposition = str(
            effect.payload.get("terminal_operation_disposition") or ""
        ).strip()
        if disposition != "skip":
            raise EffectStoreConflict(
                "operation-status effect preconditions require skip disposition"
            )
        if not effect.operation_id:
            raise EffectStoreConflict(
                "operation-status effect preconditions require operation_id"
            )
        row = conn.execute(
            """
            SELECT status FROM agent_session_operations
            WHERE operation_id = ? AND profile_id = ? AND session_id = ?
            """,
            (
                effect.operation_id,
                effect.key.profile_id,
                effect.key.session_id,
            ),
        ).fetchone()
        actual = str(row["status"]) if row is not None else "missing"
        return None if actual in expected else actual

    @staticmethod
    def _skipped_envelope(
        effect: DurableEffectEnvelope,
        *,
        intended: SessionEventEnvelope,
        actual_operation_status: str,
        now: float,
        outcome_fence_fields: tuple[str, ...],
    ) -> SessionEventEnvelope:
        raw_expected = effect.payload["enqueue_only_if_operation_status"]
        assert isinstance(raw_expected, list)
        return SessionEventEnvelope(
            event_id=skipped_event_id(effect),
            key=effect.key,
            kind="EffectSkipped",
            ownership_generation=effect.ownership_generation,
            payload={
                **effect.outcome_fence_payload(outcome_fence_fields),
                "effect_id": effect.effect_id,
                "effect_kind": effect.kind,
                "contract_version": effect.contract_version,
                "contract_signature": effect.contract_signature,
                "operation_id": effect.operation_id,
                "idempotency_key": effect.idempotency_key,
                "reason": "operation_status_precondition_not_met",
                "expected_operation_statuses": [
                    str(value) for value in raw_expected
                ],
                "actual_operation_status": actual_operation_status,
                "intended_event_id": intended.event_id,
                "intended_event_kind": intended.kind,
            },
            source="effect_executor",
            occurred_at=now,
            causation_id=effect.source_event_id,
            correlation_id=effect.operation_id or effect.effect_id,
            trace_id=effect.trace_id,
            available_at=now,
            created_at=now,
        )

    @staticmethod
    def _load_existing_skip(
        conn: sqlite3.Connection,
        effect: DurableEffectEnvelope,
        intended: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...],
    ) -> EffectSettlementResult | None:
        event_id = skipped_event_id(effect)
        row = conn.execute(
            """
            SELECT kind, ownership_generation, payload_json
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (effect.key.profile_id, effect.key.session_id, event_id),
        ).fetchone()
        if row is None:
            return None
        if str(row["kind"]) != "EffectSkipped":
            raise EffectStoreConflict("deterministic skipped event id changed identity")
        if int(row["ownership_generation"]) != effect.ownership_generation:
            raise EffectStoreConflict(
                "deterministic skipped event ownership generation changed identity"
            )
        payload = _json_object(str(row["payload_json"]))
        expected_identity = {
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "contract_version": effect.contract_version,
            "contract_signature": effect.contract_signature,
            "operation_id": effect.operation_id,
            "idempotency_key": effect.idempotency_key,
            "intended_event_id": intended.event_id,
            "intended_event_kind": intended.kind,
        }
        if any(payload.get(key) != value for key, value in expected_identity.items()):
            raise EffectStoreConflict("persisted skipped event changed identity")
        for field_name, expected in effect.outcome_fence_payload(
            outcome_fence_fields
        ).items():
            if payload.get(field_name) != expected:
                raise EffectStoreConflict(
                    "persisted skipped event changed fence " + field_name
                )
        return EffectSettlementResult(
            status=EffectSettlementStatus.PRECONDITION_SKIPPED,
            effect_id=effect.effect_id,
            event_id=event_id,
            key=effect.key,
        )

    @staticmethod
    def _validate_effect_identity(
        row: sqlite3.Row,
        effect: DurableEffectEnvelope,
    ) -> None:
        persisted = (
            str(row["profile_id"]),
            str(row["session_id"]),
            int(row["ownership_generation"]),
            str(row["kind"]),
            int(row["contract_version"]),
            str(row["contract_signature"]),
            str(row["idempotency_key"]),
            str(row["event_id"]),
            str(row["operation_id"]),
            str(row["payload_json"]),
        )
        requested = (
            effect.key.profile_id,
            effect.key.session_id,
            effect.ownership_generation,
            effect.kind,
            effect.contract_version,
            effect.contract_signature,
            effect.idempotency_key,
            effect.source_event_id,
            effect.operation_id,
            _json_dumps(effect.payload),
        )
        if persisted != requested:
            raise EffectStoreConflict(
                f"effect id {effect.effect_id!r} is already used by different work"
            )

    @staticmethod
    def _validate_settlement_contract(
        effect: DurableEffectEnvelope,
        envelope: SessionEventEnvelope,
    ) -> None:
        if envelope.payload.get("contract_version") != effect.contract_version:
            raise EffectStoreConflict(
                "effect settlement contract_version changed identity"
            )
        if envelope.payload.get("contract_signature") != effect.contract_signature:
            raise EffectStoreConflict(
                "effect settlement contract_signature changed identity"
            )

    @staticmethod
    def _validate_failure_envelope(
        claim: ClaimedEffect,
        envelope: SessionEventEnvelope,
        *,
        outcome_fence_fields: tuple[str, ...],
    ) -> None:
        """Reject forged terminal failures before changing the durable outbox."""

        effect = claim.effect
        if envelope.kind != "EffectFailed":
            raise EffectStoreConflict("terminal effect failure changed event kind")
        if envelope.event_id != failure_event_id(effect):
            raise EffectStoreConflict("terminal effect failure changed event id")
        if envelope.source != "effect_executor":
            raise EffectStoreConflict("terminal effect failure changed source")
        if envelope.causation_id != effect.source_event_id:
            raise EffectStoreConflict("terminal effect failure changed causation id")
        if envelope.correlation_id != (effect.operation_id or effect.effect_id):
            raise EffectStoreConflict("terminal effect failure changed correlation id")
        if envelope.trace_id != effect.trace_id:
            raise EffectStoreConflict("terminal effect failure changed trace id")
        if envelope.ownership_generation != effect.ownership_generation:
            raise EffectStoreConflict(
                "terminal effect failure changed ownership generation"
            )

        expected_identity = {
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "idempotency_key": effect.idempotency_key,
            "operation_id": effect.operation_id,
            "contract_signature": effect.contract_signature,
        }
        for field_name, expected in expected_identity.items():
            if envelope.payload.get(field_name) != expected:
                raise EffectStoreConflict(
                    "terminal effect failure changed " + field_name
                )
        if envelope.payload.get("contract_version") != effect.contract_version:
            raise EffectStoreConflict(
                "terminal effect failure changed contract_version"
            )
        if envelope.payload.get("attempt_count") != claim.attempt_count:
            raise EffectStoreConflict("terminal effect failure changed attempt count")
        if (
            isinstance(envelope.payload.get("attempt_count"), bool)
            or not isinstance(envelope.payload.get("attempt_count"), int)
            or int(envelope.payload["attempt_count"]) < 1
        ):
            raise EffectStoreConflict("terminal effect failure has an invalid attempt")

        for field_name, expected in effect.outcome_fence_payload(
            outcome_fence_fields
        ).items():
            if (
                field_name not in envelope.payload
                or envelope.payload[field_name] != expected
            ):
                raise EffectStoreConflict(
                    "terminal effect failure changed fence " + field_name
                )
        for field_name in ("action_ordinal", "request_digest"):
            if (
                field_name in effect.payload
                and envelope.payload.get(field_name) != effect.payload[field_name]
            ):
                raise EffectStoreConflict(
                    "terminal effect failure changed action " + field_name
                )
        failure_code = envelope.payload.get("failure_code")
        if not isinstance(failure_code, str) or not failure_code.strip():
            raise EffectStoreConflict("terminal effect failure has no failure code")
        if not isinstance(envelope.payload.get("failure_message"), str):
            raise EffectStoreConflict("terminal effect failure has an invalid message")

    @staticmethod
    def _validate_live_claim(
        row: sqlite3.Row,
        claim: ClaimedEffect,
        *,
        now: float,
    ) -> None:
        if (
            str(row["status"]) != DurableEffectStatus.PROCESSING.value
            or str(row["claim_id"]) != claim.claim_id
            or str(row["lease_owner"]) != claim.worker_id
            or float(row["lease_until"] or 0.0) <= now
        ):
            raise EffectClaimLost("effect lease is expired or no longer owned")

    @classmethod
    def _insert_event(
        cls,
        conn: sqlite3.Connection,
        envelope: SessionEventEnvelope,
        *,
        payload_json: str,
        now: float,
    ) -> None:
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
                envelope.occurred_at or now,
                payload_json,
                envelope.causation_id,
                envelope.correlation_id,
                envelope.trace_id,
                envelope.available_at or now,
                envelope.created_at or now,
            ),
        )
        if inserted.rowcount != 1:
            cls._validate_persisted_event(conn, envelope, payload_json)

    @staticmethod
    def _validate_persisted_event(
        conn: sqlite3.Connection,
        envelope: SessionEventEnvelope,
        payload_json: str,
    ) -> None:
        row = conn.execute(
            """
            SELECT kind, source, ownership_generation, payload_json,
                   causation_id, correlation_id, trace_id
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (
                envelope.key.profile_id,
                envelope.key.session_id,
                envelope.event_id,
            ),
        ).fetchone()
        if row is None:
            raise EffectStoreConflict("settled effect is missing its mailbox event")
        persisted = (
            str(row["kind"]),
            str(row["source"]),
            int(row["ownership_generation"]),
            str(row["payload_json"]),
            str(row["causation_id"]),
            str(row["correlation_id"]),
            str(row["trace_id"]),
        )
        requested = (
            envelope.kind,
            envelope.source,
            envelope.ownership_generation,
            payload_json,
            envelope.causation_id,
            envelope.correlation_id,
            envelope.trace_id,
        )
        if persisted != requested:
            raise EffectStoreConflict(
                f"mailbox event id {envelope.event_id!r} is already used by different work"
            )


def _effect_from_row(row: sqlite3.Row) -> DurableEffectEnvelope:
    payload = _json_object(str(row["payload_json"] or "{}"))
    return DurableEffectEnvelope(
        effect_id=str(row["effect_id"]),
        key=SessionKey(str(row["profile_id"]), str(row["session_id"])),
        kind=str(row["kind"]),
        idempotency_key=str(row["idempotency_key"]),
        ownership_generation=int(row["ownership_generation"]),
        contract_version=int(row["contract_version"]),
        contract_signature=str(row["contract_signature"]),
        payload=payload,
        source_event_id=str(row["event_id"]),
        operation_id=str(row["operation_id"]),
        trace_id=str(row["source_trace_id"]),
        available_at=float(row["available_at"]),
        created_at=float(row["created_at"]),
    )


def _json_object(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise EffectStoreConflict("durable effect payload is invalid JSON") from exc
    if not isinstance(loaded, dict):
        raise EffectStoreConflict("durable effect payload must be a JSON object")
    return {str(key): item for key, item in loaded.items()}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


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


def _require_effect_ownership(
    database: DatabaseManager,
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    expected_generation: int,
) -> None:
    try:
        database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            key,
            expected_generation=expected_generation,
        )
    except AgentRuntimeOwnershipError as exc:
        raise EffectClaimLost("effect ownership generation is no longer active") from exc


def _external_action_order_gate_sql(
    effect_alias: str,
) -> tuple[str, tuple[object, ...]]:
    """Build the durable claim predicate for ordered external-action effects.

    Action effects are intentionally filtered before a worker receives an
    outbox lease. The immediate predecessor must already have a succeeded
    receipt. This makes actions from separate operations independently
    claimable while preventing a follower from reaching adapter code early.
    Invalid legacy or tampered action payloads evaluate to false and remain
    pending for diagnosis instead of being treated as ordinal zero.
    """

    kinds = ", ".join("?" for _kind in _EXTERNAL_ACTION_EFFECT_KINDS)
    payload_json = f"{effect_alias}.payload_json"
    ordinal = f"""
        CASE
            WHEN json_valid({payload_json}) THEN CASE
                WHEN json_type({payload_json}, '$.action_ordinal') = 'integer'
                THEN CAST(json_extract({payload_json}, '$.action_ordinal') AS INTEGER)
                ELSE -1
            END
            ELSE -1
        END
    """
    operation_matches = f"""
        CASE
            WHEN json_valid({payload_json}) THEN CASE
                WHEN json_type({payload_json}, '$.operation_id') = 'text'
                 AND json_extract({payload_json}, '$.operation_id')
                     = {effect_alias}.operation_id
                THEN 1
                ELSE 0
            END
            ELSE 0
        END
    """
    payload_is_ordered_action = f"""
        {effect_alias}.operation_id != ''
        AND ({operation_matches}) = 1
        AND ({ordinal}) >= 0
    """
    sql = f"""
        AND (
            {effect_alias}.kind NOT IN ({kinds})
            OR (
                ({payload_is_ordered_action}) = 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM agent_external_action_receipts AS occupied_slot
                    WHERE occupied_slot.profile_id = {effect_alias}.profile_id
                      AND occupied_slot.session_id = {effect_alias}.session_id
                      AND occupied_slot.ownership_generation
                          = {effect_alias}.ownership_generation
                      AND occupied_slot.operation_id = {effect_alias}.operation_id
                      AND occupied_slot.action_ordinal = {ordinal}
                      AND occupied_slot.idempotency_key != {effect_alias}.idempotency_key
                )
                AND (
                    {ordinal} = 0
                    OR EXISTS (
                        SELECT 1
                        FROM agent_external_action_receipts AS predecessor
                        WHERE predecessor.profile_id = {effect_alias}.profile_id
                          AND predecessor.session_id = {effect_alias}.session_id
                          AND predecessor.ownership_generation
                              = {effect_alias}.ownership_generation
                          AND predecessor.operation_id = {effect_alias}.operation_id
                          AND predecessor.action_ordinal = {ordinal} - 1
                          AND predecessor.status = 'succeeded'
                    )
                )
            )
        )
    """
    return sql, tuple(_EXTERNAL_ACTION_EFFECT_KINDS)


def _contract_filter_sql(
    effect_contracts: tuple[tuple[str, int], ...] | None,
    excluded_effect_contracts: tuple[tuple[str, int], ...],
    *,
    table_alias: str,
) -> tuple[str, tuple[object, ...], str, tuple[object, ...]]:
    if effect_contracts is not None:
        predicate = " OR ".join(
            (
                f"({table_alias}.kind = ? "
                f"AND {table_alias}.contract_version = ?)"
            )
            for _contract in effect_contracts
        )
        filter_sql = f"AND ({predicate})"
        cases = " ".join(
            (
                f"WHEN {table_alias}.kind = ? "
                f"AND {table_alias}.contract_version = ? THEN {index}"
            )
            for index, _contract in enumerate(effect_contracts)
        )
        priority_sql = f"CASE {cases} ELSE {len(effect_contracts)} END,"
        params = tuple(item for contract in effect_contracts for item in contract)
        return filter_sql, params, priority_sql, params
    if excluded_effect_contracts:
        predicate = " OR ".join(
            (
                f"({table_alias}.kind = ? "
                f"AND {table_alias}.contract_version = ?)"
            )
            for _contract in excluded_effect_contracts
        )
        return (
            f"AND NOT ({predicate})",
            tuple(
                item for contract in excluded_effect_contracts for item in contract
            ),
            "",
            (),
        )
    return "", (), "", ()


__all__ = ["EffectStoreConflict", "SQLiteDurableEffectStore"]
