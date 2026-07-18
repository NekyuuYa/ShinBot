"""Durable liveness witnesses for non-review Actor model workflow effects.

Review cancellation has an additional control gate and remains implemented in
``review_execution_gate``.  This module covers the other model workflows whose
lease expiry also cannot prove that an upstream model request did not begin.
It intentionally contains no cancellation semantics: an ``unknown`` witness is
a durable blocker until a future explicit reconciliation protocol exists.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.execution_binding import (
    require_live_execution_binding_in_transaction,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError
from shinbot.core.dispatch.fenced_wake_target_lease import FencedActorExecutionBinding

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


MODEL_EXECUTION_WITNESSED_EFFECT_KINDS = frozenset(
    {
        "run_active_reply_workflow",
        "run_active_chat_bootstrap",
        "run_active_chat_round",
        "run_idle_review_planning",
    }
)
"""Model workflow kinds protected by this non-review witness protocol."""

MODEL_EXECUTION_UNKNOWN_EVENT_KIND = "ModelExecutionUnknown"
MODEL_EXECUTION_UNKNOWN_EVENT_SOURCE = "durable_model_execution_recovery"
_MODEL_EXECUTION_UNKNOWN_EVENT_VERSION = 1
_MODEL_EXECUTION_UNKNOWN_PAYLOAD_FIELDS = frozenset(
    {
        "version",
        "event_id",
        "session_key",
        "ownership_generation",
        "effect_id",
        "operation_id",
        "effect_kind",
        "contract_version",
        "contract_signature",
        "claim_id",
        "worker_id",
        "attempt_count",
        "unknown_at",
        "unknown_reason",
    }
)


class ModelExecutionWitnessError(RuntimeError):
    """Raised when durable non-review model execution evidence is inconsistent."""


class ModelExecutionPermitDisposition(StrEnum):
    """Whether a worker may create a non-review model handler task."""

    STARTED = "started"
    DEFERRED = "deferred"
    CANCELLED = "cancelled"


@dataclass(slots=True, frozen=True)
class ModelExecutionClaim:
    """Exact durable identity of one non-review model effect claim."""

    key: SessionKey
    ownership_generation: int
    effect_id: str
    operation_id: str
    effect_kind: str
    contract_version: int
    contract_signature: str
    claim_id: str
    worker_id: str

    def __post_init__(self) -> None:
        """Reject a widened or malformed durable effect identity."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("model execution claim key must be a SessionKey")
        for field_name in ("ownership_generation", "contract_version"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")
        for field_name in (
            "effect_id",
            "operation_id",
            "effect_kind",
            "contract_signature",
            "claim_id",
            "worker_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        if self.effect_kind not in MODEL_EXECUTION_WITNESSED_EFFECT_KINDS:
            raise ValueError("model execution claim has an unsupported effect kind")


@dataclass(slots=True, frozen=True)
class ModelExecutionUnknownNotice:
    """Mailbox authority emitted when a witnessed model execution expires."""

    claim: ModelExecutionClaim
    attempt_count: int
    unknown_at: float
    unknown_reason: str

    def __post_init__(self) -> None:
        """Validate durable unknown evidence before it becomes actor input."""

        if not isinstance(self.claim, ModelExecutionClaim):
            raise TypeError("model execution unknown notice claim is invalid")
        if (
            isinstance(self.attempt_count, bool)
            or not isinstance(self.attempt_count, int)
            or self.attempt_count < 1
        ):
            raise ValueError("model execution unknown attempt_count must be positive")
        object.__setattr__(
            self,
            "unknown_at",
            _nonnegative_time(self.unknown_at, field_name="unknown_at"),
        )
        object.__setattr__(
            self,
            "unknown_reason",
            _required_text(self.unknown_reason, field_name="unknown_reason"),
        )

    @property
    def event_id(self) -> str:
        """Return the deterministic mailbox identity for this evidence."""

        claim = self.claim
        identity = json.dumps(
            (
                claim.key.profile_id,
                claim.key.session_id,
                claim.ownership_generation,
                claim.effect_id,
                claim.operation_id,
                claim.effect_kind,
                claim.contract_version,
                claim.contract_signature,
                claim.claim_id,
                claim.worker_id,
                self.attempt_count,
                self.unknown_at,
                self.unknown_reason,
            ),
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(identity.encode("ascii")).hexdigest()
        return "model-execution-unknown:v1:" + digest

    def to_payload(self) -> dict[str, Any]:
        """Serialize complete immutable evidence for one actor mailbox event."""

        claim = self.claim
        return {
            "version": _MODEL_EXECUTION_UNKNOWN_EVENT_VERSION,
            "event_id": self.event_id,
            "session_key": {
                "profile_id": claim.key.profile_id,
                "session_id": claim.key.session_id,
            },
            "ownership_generation": claim.ownership_generation,
            "effect_id": claim.effect_id,
            "operation_id": claim.operation_id,
            "effect_kind": claim.effect_kind,
            "contract_version": claim.contract_version,
            "contract_signature": claim.contract_signature,
            "claim_id": claim.claim_id,
            "worker_id": claim.worker_id,
            "attempt_count": self.attempt_count,
            "unknown_at": self.unknown_at,
            "unknown_reason": self.unknown_reason,
        }

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, object],
        *,
        event_id: str,
        key: SessionKey,
        ownership_generation: int,
    ) -> ModelExecutionUnknownNotice:
        """Decode and fence one persisted unknown-execution mailbox payload."""

        if frozenset(payload) != _MODEL_EXECUTION_UNKNOWN_PAYLOAD_FIELDS:
            raise ValueError("model execution unknown payload fields changed")
        if payload.get("version") != _MODEL_EXECUTION_UNKNOWN_EVENT_VERSION:
            raise ValueError("model execution unknown payload version changed")
        raw_key = payload.get("session_key")
        if not isinstance(raw_key, Mapping):
            raise ValueError("model execution unknown session_key is invalid")
        payload_key = SessionKey(
            _required_text(raw_key.get("profile_id"), field_name="profile_id"),
            _required_text(raw_key.get("session_id"), field_name="session_id"),
        )
        if payload_key != key:
            raise ValueError("model execution unknown session_key changed")
        payload_generation = _positive_int(
            payload.get("ownership_generation"),
            field_name="ownership_generation",
        )
        if payload_generation != ownership_generation:
            raise ValueError("model execution unknown ownership_generation changed")
        notice = cls(
            claim=ModelExecutionClaim(
                key=key,
                ownership_generation=payload_generation,
                effect_id=_required_text(payload.get("effect_id"), field_name="effect_id"),
                operation_id=_required_text(
                    payload.get("operation_id"),
                    field_name="operation_id",
                ),
                effect_kind=_required_text(
                    payload.get("effect_kind"),
                    field_name="effect_kind",
                ),
                contract_version=_positive_int(
                    payload.get("contract_version"),
                    field_name="contract_version",
                ),
                contract_signature=_required_text(
                    payload.get("contract_signature"),
                    field_name="contract_signature",
                ),
                claim_id=_required_text(payload.get("claim_id"), field_name="claim_id"),
                worker_id=_required_text(payload.get("worker_id"), field_name="worker_id"),
            ),
            attempt_count=_positive_int(
                payload.get("attempt_count"),
                field_name="attempt_count",
            ),
            unknown_at=_nonnegative_time(
                payload.get("unknown_at"),
                field_name="unknown_at",
            ),
            unknown_reason=_required_text(
                payload.get("unknown_reason"),
                field_name="unknown_reason",
            ),
        )
        if _required_text(payload.get("event_id"), field_name="event_id") != notice.event_id:
            raise ValueError("model execution unknown event_id changed")
        if _required_text(event_id, field_name="event_id") != notice.event_id:
            raise ValueError("model execution unknown mailbox event_id changed")
        return notice


@dataclass(slots=True, frozen=True)
class ModelExecutionPermit:
    """Start/finish result for one durable non-review execution witness."""

    disposition: ModelExecutionPermitDisposition
    claim: ModelExecutionClaim
    blocker_code: str = ""
    cancellation_effect_id: str = ""

    def __post_init__(self) -> None:
        """Normalize the deferred reason without widening claim identity."""

        try:
            disposition = ModelExecutionPermitDisposition(self.disposition)
        except (TypeError, ValueError) as exc:
            raise ValueError("model execution permit disposition is invalid") from exc
        if not isinstance(self.claim, ModelExecutionClaim):
            raise TypeError("model execution permit claim is invalid")
        blocker_code = _optional_text(self.blocker_code, field_name="blocker_code")
        cancellation_effect_id = _optional_text(
            self.cancellation_effect_id,
            field_name="cancellation_effect_id",
        )
        if disposition is ModelExecutionPermitDisposition.DEFERRED and not blocker_code:
            raise ValueError("deferred model execution permit requires a blocker")
        if (
            disposition is ModelExecutionPermitDisposition.CANCELLED
            and not cancellation_effect_id
        ):
            raise ValueError("cancelled model execution permit requires a gate id")
        if disposition is ModelExecutionPermitDisposition.STARTED and blocker_code:
            raise ValueError("started model execution permit cannot carry a blocker")
        if disposition is ModelExecutionPermitDisposition.STARTED and cancellation_effect_id:
            raise ValueError("started model execution permit cannot carry a gate id")
        if (
            disposition is ModelExecutionPermitDisposition.DEFERRED
            and cancellation_effect_id
        ):
            raise ValueError("deferred model execution permit cannot carry a gate id")
        if disposition is ModelExecutionPermitDisposition.CANCELLED and blocker_code:
            raise ValueError("cancelled model execution permit cannot carry a blocker")
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(self, "blocker_code", blocker_code)
        object.__setattr__(self, "cancellation_effect_id", cancellation_effect_id)

    @property
    def deferred(self) -> bool:
        """Return whether a prior durable witness prevents task creation."""

        return self.disposition is ModelExecutionPermitDisposition.DEFERRED

    @property
    def cancelled(self) -> bool:
        """Return whether a committed gate prohibited model task execution."""

        return self.disposition is ModelExecutionPermitDisposition.CANCELLED


class ModelExecutionWitnessStorePort(Protocol):
    """Executor lifecycle calls needed by non-review model witnesses."""

    @property
    def persistence_domain(self) -> object:
        """Return the database shared with the durable effect store."""

    async def begin_execution(
        self,
        claim: ModelExecutionClaim,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ModelExecutionPermit:
        """Write a start witness before a model handler task is created."""

    async def finish_execution(
        self,
        claim: ModelExecutionClaim,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ModelExecutionPermit:
        """Write a finish witness only after that task has ended."""


class SQLiteModelExecutionWitnessStore:
    """SQLite implementation of no-replay liveness witnesses for model effects."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Bind one witness store to the Actor v2 persistence domain."""

        self._database = database
        self._clock = clock or time.time

    @property
    def persistence_domain(self) -> object:
        """Return the database owning every witness transaction."""

        return self._database

    async def begin_execution(
        self,
        claim: ModelExecutionClaim,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ModelExecutionPermit:
        """Persist a start witness or defer when earlier execution is unresolved."""

        now = _clock_now(self._clock)
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _require_actor_ownership(self._database, conn, claim)
            require_live_execution_binding_in_transaction(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                ownership_generation=claim.ownership_generation,
            )
            effect = _load_model_effect(conn, claim)
            # The actor commits a generic cancellation gate before the control
            # effect is visible.  It must be consulted before a worker can turn
            # a durable claim into a Python handler task.
            from shinbot.agent.runtime.session_actor.model_execution_cancellation_gate import (
                permit_model_execution_start_if_gated,
            )

            gate_permit = permit_model_execution_start_if_gated(
                conn,
                claim=claim,
                now=now,
            )
            if gate_permit is not None:
                return gate_permit
            existing = _load_execution_run_for_effect(conn, claim)
            if existing is not None:
                if not _run_matches_claim(existing, claim):
                    return _deferred_permit(claim, "model_execution_witness_identity_conflict")
                return _deferred_permit(
                    claim,
                    "model_execution_witness_" + str(existing["execution_status"]),
                )
            _require_live_claim(effect, claim, now=now)
            conn.execute(
                """
                INSERT INTO agent_model_execution_runs (
                    profile_id, session_id, ownership_generation,
                    effect_id, operation_id, effect_kind,
                    contract_version, contract_signature,
                    claim_id, worker_id, execution_status, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?)
                """,
                (
                    claim.key.profile_id,
                    claim.key.session_id,
                    claim.ownership_generation,
                    claim.effect_id,
                    claim.operation_id,
                    claim.effect_kind,
                    claim.contract_version,
                    claim.contract_signature,
                    claim.claim_id,
                    claim.worker_id,
                    now,
                ),
            )
        return ModelExecutionPermit(
            disposition=ModelExecutionPermitDisposition.STARTED,
            claim=claim,
        )

    async def finish_execution(
        self,
        claim: ModelExecutionClaim,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ModelExecutionPermit:
        """Record a real task end, preserving unknown evidence over a late finish."""

        now = _clock_now(self._clock)
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _require_actor_ownership(self._database, conn, claim)
            require_live_execution_binding_in_transaction(
                self._database,
                conn,
                execution_binding,
                key=claim.key,
                ownership_generation=claim.ownership_generation,
            )
            run = _load_execution_run_for_claim(conn, claim)
            if run is None:
                raise ModelExecutionWitnessError("model execution witness is missing")
            if not _run_matches_claim(run, claim):
                raise ModelExecutionWitnessError("model execution witness changed identity")
            status = str(run["execution_status"])
            if status == "unknown":
                return _deferred_permit(claim, "model_execution_witness_unknown")
            if status != "running":
                raise ModelExecutionWitnessError("model execution witness is already terminal")
            updated = conn.execute(
                """
                UPDATE agent_model_execution_runs
                SET execution_status = 'finished', finished_at = ?
                WHERE run_seq = ? AND execution_status = 'running'
                """,
                (now, run["run_seq"]),
            )
            if updated.rowcount != 1:
                raise ModelExecutionWitnessError("model execution witness changed concurrently")
            from shinbot.agent.runtime.session_actor.model_execution_cancellation_gate import (
                finish_model_execution_if_gated,
            )

            gate_permit = finish_model_execution_if_gated(
                conn,
                claim=claim,
                now=now,
            )
            if gate_permit is not None:
                return gate_permit
        return ModelExecutionPermit(
            disposition=ModelExecutionPermitDisposition.STARTED,
            claim=claim,
        )


def mark_expired_model_execution_unknown(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    effect_id: str,
    claim_id: str,
    worker_id: str,
    now: float,
    reason: str,
) -> bool:
    """Turn one expired running/finished witness into irrecoverable evidence.

    The caller owns the surrounding effect-store transaction and must already
    have fenced active ownership.  The update intentionally leaves the outbox
    claim non-terminal: only explicit reconciliation can decide what a model
    provider may have observed.
    """

    normalized_effect_id = _required_text(effect_id, field_name="effect_id")
    normalized_claim_id = _required_text(claim_id, field_name="claim_id")
    normalized_worker_id = _required_text(worker_id, field_name="worker_id")
    normalized_now = _nonnegative_time(now, field_name="now")
    normalized_reason = _required_text(reason, field_name="reason")
    row = conn.execute(
        """
        SELECT run_seq, execution_status
        FROM agent_model_execution_runs
        WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
          AND effect_id = ? AND claim_id = ? AND worker_id = ?
        """,
        (
            key.profile_id,
            key.session_id,
            ownership_generation,
            normalized_effect_id,
            normalized_claim_id,
            normalized_worker_id,
        ),
    ).fetchone()
    if row is None:
        return False
    status = str(row["execution_status"])
    if status == "unknown":
        return False
    if status not in {"running", "finished"}:
        raise ModelExecutionWitnessError("expired model execution witness is terminal")
    updated = conn.execute(
        """
        UPDATE agent_model_execution_runs
        SET execution_status = 'unknown', finished_at = NULL,
            unknown_at = ?, unknown_reason = ?
        WHERE run_seq = ? AND execution_status IN ('running', 'finished')
        """,
        (normalized_now, normalized_reason, row["run_seq"]),
    )
    if updated.rowcount != 1:
        raise ModelExecutionWitnessError("model execution witness changed concurrently")
    return True


def _deferred_permit(claim: ModelExecutionClaim, blocker_code: str) -> ModelExecutionPermit:
    return ModelExecutionPermit(
        disposition=ModelExecutionPermitDisposition.DEFERRED,
        claim=claim,
        blocker_code=blocker_code,
    )


def _load_model_effect(
    conn: sqlite3.Connection,
    claim: ModelExecutionClaim,
) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT status, attempt_count, claim_id, lease_owner, lease_until,
               ownership_generation, operation_id, kind,
               contract_version, contract_signature
        FROM agent_effect_outbox
        WHERE profile_id = ? AND session_id = ? AND effect_id = ?
        """,
        (claim.key.profile_id, claim.key.session_id, claim.effect_id),
    ).fetchone()
    if row is None:
        raise ModelExecutionWitnessError("model effect outbox row is missing")
    identity = (
        int(row["ownership_generation"]),
        str(row["operation_id"]),
        str(row["kind"]),
        int(row["contract_version"]),
        str(row["contract_signature"]),
    )
    expected = (
        claim.ownership_generation,
        claim.operation_id,
        claim.effect_kind,
        claim.contract_version,
        claim.contract_signature,
    )
    if identity != expected:
        raise ModelExecutionWitnessError("model effect outbox identity changed")
    return row


def _require_live_claim(
    effect: sqlite3.Row,
    claim: ModelExecutionClaim,
    *,
    now: float,
) -> None:
    if str(effect["status"]) != "processing":
        raise ModelExecutionWitnessError("model effect is not processing")
    if str(effect["claim_id"]) != claim.claim_id:
        raise ModelExecutionWitnessError("model effect claim id changed")
    if str(effect["lease_owner"]) != claim.worker_id:
        raise ModelExecutionWitnessError("model effect lease owner changed")
    lease_until = _nonnegative_time(effect["lease_until"], field_name="lease_until")
    if lease_until <= now:
        raise ModelExecutionWitnessError("model effect lease expired")
    attempt_count = effect["attempt_count"]
    if isinstance(attempt_count, bool) or not isinstance(attempt_count, int) or attempt_count < 1:
        raise ModelExecutionWitnessError("model effect attempt count is invalid")


def _load_execution_run_for_effect(
    conn: sqlite3.Connection,
    claim: ModelExecutionClaim,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM agent_model_execution_runs
        WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
          AND effect_id = ?
        ORDER BY run_seq DESC
        LIMIT 1
        """,
        (
            claim.key.profile_id,
            claim.key.session_id,
            claim.ownership_generation,
            claim.effect_id,
        ),
    ).fetchone()


def _load_execution_run_for_claim(
    conn: sqlite3.Connection,
    claim: ModelExecutionClaim,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM agent_model_execution_runs
        WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
          AND effect_id = ? AND claim_id = ? AND worker_id = ?
        """,
        (
            claim.key.profile_id,
            claim.key.session_id,
            claim.ownership_generation,
            claim.effect_id,
            claim.claim_id,
            claim.worker_id,
        ),
    ).fetchone()


def _run_matches_claim(row: sqlite3.Row, claim: ModelExecutionClaim) -> bool:
    return (
        str(row["profile_id"]) == claim.key.profile_id
        and str(row["session_id"]) == claim.key.session_id
        and int(row["ownership_generation"]) == claim.ownership_generation
        and str(row["effect_id"]) == claim.effect_id
        and str(row["operation_id"]) == claim.operation_id
        and str(row["effect_kind"]) == claim.effect_kind
        and int(row["contract_version"]) == claim.contract_version
        and str(row["contract_signature"]) == claim.contract_signature
        and str(row["claim_id"]) == claim.claim_id
        and str(row["worker_id"]) == claim.worker_id
    )


def _require_actor_ownership(
    database: DatabaseManager,
    conn: sqlite3.Connection,
    claim: ModelExecutionClaim,
) -> None:
    try:
        database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            claim.key,
            expected_generation=claim.ownership_generation,
        )
    except AgentRuntimeOwnershipError as exc:
        raise ModelExecutionWitnessError("model execution ownership changed") from exc


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized or normalized != value:
        raise ValueError(f"{field_name} must be canonical non-empty text")
    return normalized


def _optional_text(value: object, *, field_name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if normalized != value:
        raise ValueError(f"{field_name} must not contain surrounding whitespace")
    return normalized


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_time(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite non-negative number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be a finite non-negative number")
    return normalized


def _clock_now(clock: Callable[[], float]) -> float:
    return _nonnegative_time(clock(), field_name="clock")


__all__ = [
    "MODEL_EXECUTION_UNKNOWN_EVENT_KIND",
    "MODEL_EXECUTION_UNKNOWN_EVENT_SOURCE",
    "MODEL_EXECUTION_WITNESSED_EFFECT_KINDS",
    "ModelExecutionClaim",
    "ModelExecutionPermit",
    "ModelExecutionPermitDisposition",
    "ModelExecutionUnknownNotice",
    "ModelExecutionWitnessError",
    "ModelExecutionWitnessStorePort",
    "SQLiteModelExecutionWitnessStore",
    "mark_expired_model_execution_unknown",
]
