"""Durable execution witnesses for fenced Actor v2 review cancellation.

The gate declaration is written by ``SQLiteSessionActorStore`` in the same
transaction that supersedes a review. This module owns the executor-side
protocol: a worker records its handler claim before creating a task, and only
an ended claim can acknowledge the gate. A missing local task is therefore
never used as cross-process proof.
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
from shinbot.agent.runtime.session_actor.execution_control import (
    ReviewCancellationGateObservation,
    ReviewCancellationGateRequest,
    ReviewCancellationGateStatus,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError
from shinbot.core.dispatch.fenced_wake_target_lease import FencedActorExecutionBinding

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


class ReviewExecutionGateError(RuntimeError):
    """Base error raised by durable review execution witness operations."""


REVIEW_EXECUTION_UNKNOWN_EVENT_KIND = "ReviewExecutionUnknown"
REVIEW_EXECUTION_UNKNOWN_EVENT_SOURCE = "durable_review_execution_recovery"
_REVIEW_EXECUTION_UNKNOWN_EVENT_VERSION = 1
_REVIEW_EXECUTION_UNKNOWN_PAYLOAD_FIELDS = frozenset(
    {
        "version",
        "event_id",
        "session_key",
        "ownership_generation",
        "review_effect_id",
        "review_operation_id",
        "review_effect_kind",
        "review_contract_version",
        "review_contract_signature",
        "claim_id",
        "worker_id",
        "attempt_count",
        "unknown_at",
        "unknown_reason",
    }
)


class ReviewExecutionPermitDisposition(StrEnum):
    """Whether a worker may create the review handler task for its claim."""

    STARTED = "started"
    CANCELLED = "cancelled"
    DEFERRED = "deferred"


@dataclass(slots=True, frozen=True)
class ReviewExecutionClaim:
    """Exact review-effect claim identity held by one executor worker."""

    key: SessionKey
    ownership_generation: int
    review_effect_id: str
    review_operation_id: str
    review_effect_kind: str
    review_contract_version: int
    review_contract_signature: str
    claim_id: str
    worker_id: str

    def __post_init__(self) -> None:
        """Validate immutable outbox and lease identities."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("review execution claim key must be a SessionKey")
        for field_name in (
            "ownership_generation",
            "review_contract_version",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")
        for field_name in (
            "review_effect_id",
            "review_operation_id",
            "review_effect_kind",
            "review_contract_signature",
            "claim_id",
            "worker_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        if self.review_effect_kind != "run_review_workflow":
            raise ValueError("review execution claim must target run_review_workflow")


@dataclass(slots=True, frozen=True)
class ReviewExecutionUnknownNotice:
    """One immutable mailbox notice for an unrecoverable review execution.

    The notice is emitted in the same transaction that turns a durable
    execution witness from ``running`` or unsettled ``finished`` into
    ``unknown``. It gives the actor state machine the same hard fence as the
    effect store, rather than relying on a later cancellation control effect.
    """

    claim: ReviewExecutionClaim
    attempt_count: int
    unknown_at: float
    unknown_reason: str

    def __post_init__(self) -> None:
        """Validate execution evidence before it becomes mailbox authority."""

        if not isinstance(self.claim, ReviewExecutionClaim):
            raise TypeError("review execution unknown notice claim is invalid")
        if (
            isinstance(self.attempt_count, bool)
            or not isinstance(self.attempt_count, int)
            or self.attempt_count < 1
        ):
            raise ValueError("review execution unknown attempt_count must be positive")
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
        """Return the deterministic event identity for this exact evidence."""

        claim = self.claim
        identity = json.dumps(
            (
                claim.key.profile_id,
                claim.key.session_id,
                claim.ownership_generation,
                claim.review_effect_id,
                claim.review_operation_id,
                claim.review_effect_kind,
                claim.review_contract_version,
                claim.review_contract_signature,
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
        return "review-execution-unknown:v1:" + hashlib.sha256(identity.encode("ascii")).hexdigest()

    def to_payload(self) -> dict[str, Any]:
        """Serialize complete immutable evidence for the actor mailbox."""

        claim = self.claim
        return {
            "version": _REVIEW_EXECUTION_UNKNOWN_EVENT_VERSION,
            "event_id": self.event_id,
            "session_key": {
                "profile_id": claim.key.profile_id,
                "session_id": claim.key.session_id,
            },
            "ownership_generation": claim.ownership_generation,
            "review_effect_id": claim.review_effect_id,
            "review_operation_id": claim.review_operation_id,
            "review_effect_kind": claim.review_effect_kind,
            "review_contract_version": claim.review_contract_version,
            "review_contract_signature": claim.review_contract_signature,
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
    ) -> ReviewExecutionUnknownNotice:
        """Decode mailbox evidence and verify its sealed envelope identity."""

        if frozenset(payload) != _REVIEW_EXECUTION_UNKNOWN_PAYLOAD_FIELDS:
            raise ValueError("review execution unknown payload fields changed")
        if payload.get("version") != _REVIEW_EXECUTION_UNKNOWN_EVENT_VERSION:
            raise ValueError("review execution unknown payload version changed")
        raw_key = payload.get("session_key")
        if not isinstance(raw_key, Mapping):
            raise ValueError("review execution unknown session_key is invalid")
        payload_key = SessionKey(
            _required_text(raw_key.get("profile_id"), field_name="profile_id"),
            _required_text(raw_key.get("session_id"), field_name="session_id"),
        )
        if payload_key != key:
            raise ValueError("review execution unknown session_key changed")
        payload_generation = _positive_int(
            payload.get("ownership_generation"),
            field_name="ownership_generation",
        )
        if payload_generation != ownership_generation:
            raise ValueError("review execution unknown ownership_generation changed")
        notice = cls(
            claim=ReviewExecutionClaim(
                key=key,
                ownership_generation=payload_generation,
                review_effect_id=_required_text(
                    payload.get("review_effect_id"),
                    field_name="review_effect_id",
                ),
                review_operation_id=_required_text(
                    payload.get("review_operation_id"),
                    field_name="review_operation_id",
                ),
                review_effect_kind=_required_text(
                    payload.get("review_effect_kind"),
                    field_name="review_effect_kind",
                ),
                review_contract_version=_positive_int(
                    payload.get("review_contract_version"),
                    field_name="review_contract_version",
                ),
                review_contract_signature=_required_text(
                    payload.get("review_contract_signature"),
                    field_name="review_contract_signature",
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
            raise ValueError("review execution unknown event_id changed")
        if _required_text(event_id, field_name="event_id") != notice.event_id:
            raise ValueError("review execution unknown mailbox event_id changed")
        return notice


@dataclass(slots=True, frozen=True)
class ReviewExecutionPermit:
    """Result of starting or ending a durable review execution witness."""

    disposition: ReviewExecutionPermitDisposition
    claim: ReviewExecutionClaim
    cancellation_effect_id: str = ""
    blocker_code: str = ""

    def __post_init__(self) -> None:
        """Normalize optional gate evidence without widening the target identity."""

        try:
            disposition = ReviewExecutionPermitDisposition(self.disposition)
        except (TypeError, ValueError) as exc:
            raise ValueError("review execution permit disposition is invalid") from exc
        if not isinstance(self.claim, ReviewExecutionClaim):
            raise TypeError("review execution permit claim is invalid")
        cancellation_effect_id = _optional_text(
            self.cancellation_effect_id,
            field_name="cancellation_effect_id",
        )
        blocker_code = _optional_text(self.blocker_code, field_name="blocker_code")
        if disposition is ReviewExecutionPermitDisposition.CANCELLED and not cancellation_effect_id:
            raise ValueError("cancelled review execution permit requires a gate id")
        if disposition is ReviewExecutionPermitDisposition.DEFERRED and not blocker_code:
            raise ValueError("deferred review execution permit requires a blocker code")
        if disposition is not ReviewExecutionPermitDisposition.CANCELLED and cancellation_effect_id:
            raise ValueError("only cancelled review execution permits carry a gate id")
        if disposition is not ReviewExecutionPermitDisposition.DEFERRED and blocker_code:
            raise ValueError("only deferred review execution permits carry a blocker")
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(self, "cancellation_effect_id", cancellation_effect_id)
        object.__setattr__(self, "blocker_code", blocker_code)

    @property
    def cancelled(self) -> bool:
        """Return whether a durable gate prohibited review handler execution."""

        return self.disposition is ReviewExecutionPermitDisposition.CANCELLED

    @property
    def deferred(self) -> bool:
        """Return whether another durable execution witness still owns the task."""

        return self.disposition is ReviewExecutionPermitDisposition.DEFERRED


class ReviewExecutionGateStorePort(Protocol):
    """Executor lifecycle calls required to witness review task quiescence."""

    @property
    def persistence_domain(self) -> object:
        """Return the stable transaction domain shared with the effect store."""

    async def begin_execution(
        self,
        claim: ReviewExecutionClaim,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ReviewExecutionPermit:
        """Authorize or cancel one review claim before a task is created."""

    async def finish_execution(
        self,
        claim: ReviewExecutionClaim,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ReviewExecutionPermit:
        """Record that a previously-authorized review task has ended."""


class SQLiteReviewExecutionGateStore:
    """Persist review handler start/finish witnesses and observe gate state."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Bind one store to the Actor v2 database domain.

        Args:
            database: Initialized database that owns actor outbox transactions.
            clock: Injectable wall clock for deterministic integration tests.
        """

        self._database = database
        self._clock = clock or time.time

    @property
    def persistence_domain(self) -> object:
        """Return the database domain that owns execution witnesses."""

        return self._database

    async def begin_execution(
        self,
        claim: ReviewExecutionClaim,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ReviewExecutionPermit:
        """Record a task-start witness or atomically honour an existing gate."""

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
            effect = _load_review_effect(conn, claim)
            _require_live_claim(effect, claim, now=now)
            gate = _load_gate_for_claim(conn, claim)
            if gate is not None:
                running_count, _running_identity_conflict = _running_execution_state(
                    conn,
                    claim,
                )
                unknown_count, _unknown_identity_conflict = _unknown_execution_state(
                    conn,
                    claim,
                )
                if unknown_count:
                    return ReviewExecutionPermit(
                        disposition=ReviewExecutionPermitDisposition.DEFERRED,
                        claim=claim,
                        blocker_code="review_execution_witness_unknown",
                    )
                if running_count:
                    return ReviewExecutionPermit(
                        disposition=ReviewExecutionPermitDisposition.DEFERRED,
                        claim=claim,
                        blocker_code="review_execution_witness_running",
                    )
                _cancel_claimed_review_effect(
                    conn,
                    claim=claim,
                    gate=gate,
                    now=now,
                    evidence="review_gate_before_execution_start",
                )
                return ReviewExecutionPermit(
                    disposition=ReviewExecutionPermitDisposition.CANCELLED,
                    claim=claim,
                    cancellation_effect_id=str(gate["cancellation_effect_id"]),
                )
            existing = _load_execution_run_for_claim(conn, claim)
            if existing is not None:
                if not _run_matches_claim(existing, claim):
                    raise ReviewExecutionGateError("review execution witness changed identity")
                existing_status = str(existing["execution_status"])
                if existing_status == "unknown":
                    return ReviewExecutionPermit(
                        disposition=ReviewExecutionPermitDisposition.DEFERRED,
                        claim=claim,
                        blocker_code="review_execution_witness_unknown",
                    )
                if existing_status != "running":
                    raise ReviewExecutionGateError("review execution claim was already terminal")
                # A durable start witness is deliberately not a replay token.
                # Once it exists, a retry cannot know whether the first caller
                # already created the task.  Deferring preserves the no-second-
                # model-call invariant until the witness reaches a real end.
                return ReviewExecutionPermit(
                    disposition=ReviewExecutionPermitDisposition.DEFERRED,
                    claim=claim,
                    blocker_code="review_execution_witness_running",
                )
            running = _load_running_execution_runs(conn, claim)
            if running:
                return ReviewExecutionPermit(
                    disposition=ReviewExecutionPermitDisposition.DEFERRED,
                    claim=claim,
                    blocker_code="review_execution_witness_running",
                )
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO agent_review_execution_runs (
                        profile_id, session_id, ownership_generation,
                        review_effect_id, review_operation_id, review_effect_kind,
                        review_contract_version, review_contract_signature,
                        claim_id, worker_id, execution_status, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?)
                    """,
                    (
                        claim.key.profile_id,
                        claim.key.session_id,
                        claim.ownership_generation,
                        claim.review_effect_id,
                        claim.review_operation_id,
                        claim.review_effect_kind,
                        claim.review_contract_version,
                        claim.review_contract_signature,
                        claim.claim_id,
                        claim.worker_id,
                        now,
                    ),
                )
        return ReviewExecutionPermit(
            disposition=ReviewExecutionPermitDisposition.STARTED,
            claim=claim,
        )

    async def finish_execution(
        self,
        claim: ReviewExecutionClaim,
        *,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ReviewExecutionPermit:
        """Record a finished task and atomically acknowledge a later gate."""

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
                raise ReviewExecutionGateError("review execution witness is missing")
            if not _run_matches_claim(run, claim):
                raise ReviewExecutionGateError("review execution witness changed identity")
            if str(run["execution_status"]) != "running":
                raise ReviewExecutionGateError("review execution witness is already terminal")
            gate = _load_gate_for_claim(conn, claim)
            if gate is None:
                _finish_execution_run(conn, claim=claim, status="finished", now=now)
                return ReviewExecutionPermit(
                    disposition=ReviewExecutionPermitDisposition.STARTED,
                    claim=claim,
                )
            _cancel_claimed_review_effect(
                conn,
                claim=claim,
                gate=gate,
                now=now,
                evidence="review_gate_execution_finished",
            )
            _finish_execution_run(conn, claim=claim, status="cancelled", now=now)
            remaining_running_count, _remaining_identity_conflict = _running_execution_state(
                conn, claim
            )
            remaining_unknown_count, _remaining_unknown_identity_conflict = (
                _unknown_execution_state(conn, claim)
            )
            if not remaining_running_count and not remaining_unknown_count:
                target = _load_review_effect(conn, claim)
                target_terminal_at = _nullable_time(target["completed_at"])
                if target_terminal_at is None:
                    raise ReviewExecutionGateError(
                        "cancelled review target lacks terminal timestamp"
                    )
                _mark_gate_terminal(
                    conn,
                    gate=gate,
                    target_effect_status="cancelled",
                    target_effect_claim_id=claim.claim_id,
                    target_effect_attempt_count=_effect_attempt_count(conn, claim),
                    target_effect_terminal_at=target_terminal_at,
                    now=now,
                )
            return ReviewExecutionPermit(
                disposition=ReviewExecutionPermitDisposition.CANCELLED,
                claim=claim,
                cancellation_effect_id=str(gate["cancellation_effect_id"]),
            )

    async def observe_gate(
        self,
        request: ReviewCancellationGateRequest,
    ) -> ReviewCancellationGateObservation:
        """Return a durable quiescence proof or stable pending blocker for a gate."""

        now = _clock_now(self._clock)
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            claim = _claim_from_request(request, worker_id="gate-observer")
            _require_actor_ownership(self._database, conn, claim)
            gate = _load_gate_for_request(conn, request)
            if gate is None:
                return ReviewCancellationGateObservation(
                    status=ReviewCancellationGateStatus.PENDING,
                    cancellation_effect_id=request.cancellation_effect_id,
                    review_effect_id=request.review_effect_id,
                    blocker_code="review_cancellation_gate_missing",
                )
            _validate_gate_request(gate, request)
            running_count, running_identity_conflict = _running_execution_state(
                conn,
                claim,
            )
            unknown_count, unknown_identity_conflict = _unknown_execution_state(
                conn,
                claim,
            )
            if unknown_count:
                return ReviewCancellationGateObservation(
                    status=ReviewCancellationGateStatus.BLOCKED,
                    cancellation_effect_id=request.cancellation_effect_id,
                    review_effect_id=request.review_effect_id,
                    durable_unknown_count=unknown_count,
                    blocker_code=(
                        "review_execution_witness_unknown_identity_conflict"
                        if unknown_identity_conflict
                        else "review_execution_witness_unknown"
                    ),
                )
            if running_count:
                return ReviewCancellationGateObservation(
                    status=ReviewCancellationGateStatus.PENDING,
                    cancellation_effect_id=request.cancellation_effect_id,
                    review_effect_id=request.review_effect_id,
                    durable_running_count=running_count,
                    blocker_code=(
                        "review_execution_witness_identity_conflict"
                        if running_identity_conflict
                        else "review_execution_running"
                    ),
                )
            effect = _load_review_effect(conn, claim)
            effect_status = str(effect["status"])
            if effect_status == "processing":
                _cancel_unstarted_review_effect(
                    conn,
                    claim=claim,
                    gate=gate,
                    now=now,
                    evidence="review_gate_no_execution_witness",
                )
            elif effect_status == "pending":
                _cancel_pending_review_effect(
                    conn,
                    claim=claim,
                    gate=gate,
                    now=now,
                    evidence="review_gate_pending_after_commit",
                )
            elif effect_status in {"completed", "failed", "cancelled"}:
                _mark_gate_terminal(
                    conn,
                    gate=gate,
                    target_effect_status=effect_status,
                    target_effect_claim_id=str(effect["claim_id"]),
                    target_effect_attempt_count=int(effect["attempt_count"]),
                    target_effect_terminal_at=_nullable_time(effect["completed_at"]),
                    now=now,
                )
            else:
                raise ReviewExecutionGateError("review target has an invalid status")
        return ReviewCancellationGateObservation(
            status=ReviewCancellationGateStatus.CONFIRMED,
            cancellation_effect_id=request.cancellation_effect_id,
            review_effect_id=request.review_effect_id,
        )

    async def ensure_review_cancelled(
        self,
        request: ReviewCancellationGateRequest,
    ) -> ReviewCancellationGateObservation:
        """Implement the review cancellation control port with durable evidence."""

        return await self.observe_gate(request)


def _claim_from_request(
    request: ReviewCancellationGateRequest,
    *,
    worker_id: str,
) -> ReviewExecutionClaim:
    """Project a request's exact target identity for read-only gate queries."""

    return ReviewExecutionClaim(
        key=request.key,
        ownership_generation=request.ownership_generation,
        review_effect_id=request.review_effect_id,
        review_operation_id=request.review_operation_id,
        review_effect_kind=request.review_effect_kind,
        review_contract_version=request.review_contract_version,
        review_contract_signature=request.review_contract_signature,
        claim_id="gate-observer",
        worker_id=worker_id,
    )


def _load_review_effect(
    conn: sqlite3.Connection,
    claim: ReviewExecutionClaim,
) -> sqlite3.Row:
    """Load and validate the immutable outbox target identity for one claim."""

    row = conn.execute(
        """
        SELECT status, attempt_count, claim_id, lease_owner, lease_until,
               completed_at, ownership_generation, operation_id, kind,
               contract_version, contract_signature
        FROM agent_effect_outbox
        WHERE profile_id = ?
          AND session_id = ?
          AND effect_id = ?
        """,
        (claim.key.profile_id, claim.key.session_id, claim.review_effect_id),
    ).fetchone()
    if row is None:
        raise ReviewExecutionGateError("review effect outbox row is missing")
    identity = (
        int(row["ownership_generation"]),
        str(row["operation_id"]),
        str(row["kind"]),
        int(row["contract_version"]),
        str(row["contract_signature"]),
    )
    expected = (
        claim.ownership_generation,
        claim.review_operation_id,
        claim.review_effect_kind,
        claim.review_contract_version,
        claim.review_contract_signature,
    )
    if identity != expected:
        raise ReviewExecutionGateError("review effect outbox identity changed")
    return row


def _load_gate_for_claim(
    conn: sqlite3.Connection,
    claim: ReviewExecutionClaim,
) -> sqlite3.Row | None:
    """Return the exact durable cancellation gate for one review effect."""

    return conn.execute(
        """
        SELECT *
        FROM agent_review_cancellation_gates
        WHERE profile_id = ?
          AND session_id = ?
          AND ownership_generation = ?
          AND review_effect_id = ?
          AND review_operation_id = ?
          AND review_effect_kind = ?
          AND review_contract_version = ?
          AND review_contract_signature = ?
        """,
        (
            claim.key.profile_id,
            claim.key.session_id,
            claim.ownership_generation,
            claim.review_effect_id,
            claim.review_operation_id,
            claim.review_effect_kind,
            claim.review_contract_version,
            claim.review_contract_signature,
        ),
    ).fetchone()


def _load_gate_for_request(
    conn: sqlite3.Connection,
    request: ReviewCancellationGateRequest,
) -> sqlite3.Row | None:
    """Load a gate by its control effect identity for the handler boundary."""

    return conn.execute(
        """
        SELECT *
        FROM agent_review_cancellation_gates
        WHERE profile_id = ?
          AND session_id = ?
          AND ownership_generation = ?
          AND cancellation_effect_id = ?
        """,
        (
            request.key.profile_id,
            request.key.session_id,
            request.ownership_generation,
            request.cancellation_effect_id,
        ),
    ).fetchone()


def cancel_claimed_review_effect_if_gated(
    conn: sqlite3.Connection,
    *,
    claim: ReviewExecutionClaim,
    now: float,
    evidence: str,
) -> str | None:
    """Cancel one exact live review claim when its actor gate already exists.

    This is intentionally synchronous and transaction-local so every effect
    store mutation can apply the same fence before emitting a mailbox event.
    It changes only the target outbox row and gate evidence; a running handler
    must still write its own finish witness before control completion can pass.
    """

    gate = _load_gate_for_claim(conn, claim)
    if gate is None:
        return None
    _cancel_claimed_review_effect(
        conn,
        claim=claim,
        gate=gate,
        now=now,
        evidence=evidence,
    )
    return str(gate["cancellation_effect_id"])


def _validate_gate_request(
    gate: sqlite3.Row,
    request: ReviewCancellationGateRequest,
) -> None:
    """Reject a control effect whose gate identity no longer matches its payload."""

    persisted = (
        str(gate["request_event_id"]),
        str(gate["review_operation_id"]),
        str(gate["review_effect_id"]),
        str(gate["review_effect_kind"]),
        int(gate["review_contract_version"]),
        str(gate["review_contract_signature"]),
    )
    requested = (
        request.request_event_id,
        request.review_operation_id,
        request.review_effect_id,
        request.review_effect_kind,
        request.review_contract_version,
        request.review_contract_signature,
    )
    if persisted != requested:
        raise ReviewExecutionGateError("review cancellation gate identity changed")


def _require_live_claim(
    effect: sqlite3.Row,
    claim: ReviewExecutionClaim,
    *,
    now: float,
) -> None:
    """Ensure a worker starts only from its exact current durable lease."""

    if (
        str(effect["status"]) != "processing"
        or str(effect["claim_id"]) != claim.claim_id
        or str(effect["lease_owner"]) != claim.worker_id
        or _nullable_time(effect["lease_until"]) is None
        or float(effect["lease_until"]) <= now
    ):
        raise ReviewExecutionGateError("review execution claim is no longer live")


def _load_execution_run_for_claim(
    conn: sqlite3.Connection,
    claim: ReviewExecutionClaim,
) -> sqlite3.Row | None:
    """Load the exact persistent execution witness for one fenced claim."""

    return conn.execute(
        """
        SELECT *
        FROM agent_review_execution_runs
        WHERE profile_id = ?
          AND session_id = ?
          AND ownership_generation = ?
          AND review_effect_id = ?
          AND claim_id = ?
        """,
        (
            claim.key.profile_id,
            claim.key.session_id,
            claim.ownership_generation,
            claim.review_effect_id,
            claim.claim_id,
        ),
    ).fetchone()


def _load_running_execution_runs(
    conn: sqlite3.Connection,
    claim: ReviewExecutionClaim,
) -> tuple[sqlite3.Row, ...]:
    """Return every running witness for this durable review effect.

    The effect id is intentionally the primary selector. A malformed or stale
    witness with the same id still blocks a new model task rather than being
    ignored because its auxiliary fence fields differ.
    """

    rows = conn.execute(
        """
        SELECT *
        FROM agent_review_execution_runs
        WHERE profile_id = ?
          AND session_id = ?
          AND ownership_generation = ?
          AND review_effect_id = ?
          AND execution_status = 'running'
        """,
        (
            claim.key.profile_id,
            claim.key.session_id,
            claim.ownership_generation,
            claim.review_effect_id,
        ),
    ).fetchall()
    return tuple(rows)


def _running_execution_state(
    conn: sqlite3.Connection,
    claim: ReviewExecutionClaim,
) -> tuple[int, bool]:
    """Return durable running count and whether every witness matches the fence."""

    running = _load_running_execution_runs(conn, claim)
    return len(running), any(not _run_matches_target(row, claim) for row in running)


def _unknown_execution_state(
    conn: sqlite3.Connection,
    claim: ReviewExecutionClaim,
) -> tuple[int, bool]:
    """Return durable unknown executions, which forbid implicit replay."""

    unknown = conn.execute(
        """
        SELECT *
        FROM agent_review_execution_runs
        WHERE profile_id = ?
          AND session_id = ?
          AND ownership_generation = ?
          AND review_effect_id = ?
          AND execution_status = 'unknown'
        """,
        (
            claim.key.profile_id,
            claim.key.session_id,
            claim.ownership_generation,
            claim.review_effect_id,
        ),
    ).fetchall()
    return len(unknown), any(not _run_matches_target(row, claim) for row in unknown)


def _run_matches_target(
    run: sqlite3.Row,
    claim: ReviewExecutionClaim,
) -> bool:
    """Return whether a witness retains the review target's immutable fence."""

    try:
        persisted = (
            int(run["ownership_generation"]),
            str(run["review_effect_id"]),
            str(run["review_operation_id"]),
            str(run["review_effect_kind"]),
            int(run["review_contract_version"]),
            str(run["review_contract_signature"]),
        )
    except (KeyError, TypeError, ValueError):
        return False
    expected = (
        claim.ownership_generation,
        claim.review_effect_id,
        claim.review_operation_id,
        claim.review_effect_kind,
        claim.review_contract_version,
        claim.review_contract_signature,
    )
    return persisted == expected


def _run_matches_claim(
    run: sqlite3.Row,
    claim: ReviewExecutionClaim,
) -> bool:
    """Return whether an execution witness retains the full immutable claim fence."""

    if not _run_matches_target(run, claim):
        return False
    try:
        return str(run["claim_id"]) == claim.claim_id and str(run["worker_id"]) == claim.worker_id
    except (KeyError, TypeError, ValueError):
        return False


def _cancel_claimed_review_effect(
    conn: sqlite3.Connection,
    *,
    claim: ReviewExecutionClaim,
    gate: sqlite3.Row,
    now: float,
    evidence: str,
) -> None:
    """Terminalize one live review effect without creating a mailbox outcome."""

    current_effect = _load_review_effect(conn, claim)
    if str(gate["gate_status"]) == "terminal":
        _require_terminal_gate_quiescence(
            conn,
            claim=claim,
            effect=current_effect,
        )
        return
    if str(gate["gate_status"]) not in {"requested", "cancelled"}:
        raise ReviewExecutionGateError("review cancellation gate has invalid status")

    updated = conn.execute(
        """
        UPDATE agent_effect_outbox
        SET status = 'cancelled',
            claim_id = '',
            lease_owner = '',
            lease_until = NULL,
            completed_at = ?,
            updated_at = ?,
            last_error = ?
        WHERE profile_id = ?
          AND session_id = ?
          AND effect_id = ?
          AND ownership_generation = ?
          AND operation_id = ?
          AND kind = ?
          AND contract_version = ?
          AND contract_signature = ?
          AND status = 'processing'
          AND claim_id = ?
          AND lease_owner = ?
        """,
        (
            now,
            now,
            evidence + ":" + str(gate["cancellation_effect_id"]),
            claim.key.profile_id,
            claim.key.session_id,
            claim.review_effect_id,
            claim.ownership_generation,
            claim.review_operation_id,
            claim.review_effect_kind,
            claim.review_contract_version,
            claim.review_contract_signature,
            claim.claim_id,
            claim.worker_id,
        ),
    )
    if updated.rowcount != 1:
        effect = _load_review_effect(conn, claim)
        if str(effect["status"]) != "cancelled":
            raise ReviewExecutionGateError("review cancellation target claim changed")
    target = _load_review_effect(conn, claim)
    target_terminal_at = _nullable_time(target["completed_at"])
    if target_terminal_at is None:
        raise ReviewExecutionGateError("cancelled review target lacks terminal timestamp")
    attempt_count = _effect_attempt_count(conn, claim)
    running_count, _running_identity_conflict = _running_execution_state(conn, claim)
    unknown_count, _unknown_identity_conflict = _unknown_execution_state(conn, claim)
    if running_count or unknown_count:
        _mark_gate_cancelled(
            conn,
            gate=gate,
            target_effect_claim_id=claim.claim_id,
            target_effect_attempt_count=attempt_count,
            target_effect_terminal_at=target_terminal_at,
            now=now,
        )
        return

    # This path is shared by every effect-store mutation fence.  If no durable
    # start witness exists in the same transaction, no review handler can still
    # be alive: a worker must insert that witness before it creates the task.
    # Do not leave a historical ``cancelled`` gate that no control effect will
    # necessarily observe again.
    _mark_gate_terminal(
        conn,
        gate=gate,
        target_effect_status="cancelled",
        target_effect_claim_id=claim.claim_id,
        target_effect_attempt_count=attempt_count,
        target_effect_terminal_at=target_terminal_at,
        now=now,
    )


def _cancel_unstarted_review_effect(
    conn: sqlite3.Connection,
    *,
    claim: ReviewExecutionClaim,
    gate: sqlite3.Row,
    now: float,
    evidence: str,
) -> None:
    """Cancel a processing row only after proving no task-start witness exists."""

    updated = conn.execute(
        """
        UPDATE agent_effect_outbox
        SET status = 'cancelled',
            claim_id = '',
            lease_owner = '',
            lease_until = NULL,
            completed_at = ?,
            updated_at = ?,
            last_error = ?
        WHERE profile_id = ?
          AND session_id = ?
          AND effect_id = ?
          AND ownership_generation = ?
          AND status = 'processing'
        """,
        (
            now,
            now,
            evidence + ":" + str(gate["cancellation_effect_id"]),
            claim.key.profile_id,
            claim.key.session_id,
            claim.review_effect_id,
            claim.ownership_generation,
        ),
    )
    if updated.rowcount != 1:
        raise ReviewExecutionGateError("unstarted review target changed before cancel")
    _mark_gate_terminal(
        conn,
        gate=gate,
        target_effect_status="cancelled",
        target_effect_claim_id=str(gate["target_effect_claim_id"]),
        target_effect_attempt_count=_effect_attempt_count(conn, claim),
        target_effect_terminal_at=now,
        now=now,
    )


def _cancel_pending_review_effect(
    conn: sqlite3.Connection,
    *,
    claim: ReviewExecutionClaim,
    gate: sqlite3.Row,
    now: float,
    evidence: str,
) -> None:
    """Repair an unexpected pending gated target without creating an event."""

    updated = conn.execute(
        """
        UPDATE agent_effect_outbox
        SET status = 'cancelled',
            completed_at = ?,
            updated_at = ?,
            last_error = ?
        WHERE profile_id = ?
          AND session_id = ?
          AND effect_id = ?
          AND ownership_generation = ?
          AND status = 'pending'
          AND claim_id = ''
          AND lease_owner = ''
          AND lease_until IS NULL
        """,
        (
            now,
            now,
            evidence + ":" + str(gate["cancellation_effect_id"]),
            claim.key.profile_id,
            claim.key.session_id,
            claim.review_effect_id,
            claim.ownership_generation,
        ),
    )
    if updated.rowcount != 1:
        raise ReviewExecutionGateError("pending review target changed before cancel")
    _mark_gate_terminal(
        conn,
        gate=gate,
        target_effect_status="cancelled",
        target_effect_claim_id="",
        target_effect_attempt_count=_effect_attempt_count(conn, claim),
        target_effect_terminal_at=now,
        now=now,
    )


def _mark_gate_cancelled(
    conn: sqlite3.Connection,
    *,
    gate: sqlite3.Row,
    target_effect_claim_id: str,
    target_effect_attempt_count: int,
    target_effect_terminal_at: float,
    now: float,
) -> None:
    """Record the actual terminal cancellation evidence for one gate."""

    updated = conn.execute(
        """
        UPDATE agent_review_cancellation_gates
        SET gate_status = 'cancelled',
            target_effect_status = 'cancelled',
            target_effect_claim_id = ?,
            target_effect_attempt_count = ?,
            target_effect_terminal_at = ?,
            updated_at = ?
        WHERE gate_seq = ?
          AND gate_status IN ('requested', 'cancelled')
        """,
        (
            target_effect_claim_id,
            target_effect_attempt_count,
            target_effect_terminal_at,
            now,
            int(gate["gate_seq"]),
        ),
    )
    if updated.rowcount != 1:
        raise ReviewExecutionGateError("review cancellation gate cannot transition to cancelled")


def _require_terminal_gate_quiescence(
    conn: sqlite3.Connection,
    *,
    claim: ReviewExecutionClaim,
    effect: sqlite3.Row,
) -> None:
    """Reject a stale mutation that would regress an already-terminal gate."""

    running_count, _running_identity_conflict = _running_execution_state(conn, claim)
    unknown_count, _unknown_identity_conflict = _unknown_execution_state(conn, claim)
    if (
        running_count
        or unknown_count
        or str(effect["status"]) != "cancelled"
        or str(effect["claim_id"])
        or str(effect["lease_owner"])
        or _nullable_time(effect["lease_until"]) is not None
        or _nullable_time(effect["completed_at"]) is None
    ):
        raise ReviewExecutionGateError(
            "terminal review cancellation gate no longer proves quiescence"
        )


def _mark_gate_terminal(
    conn: sqlite3.Connection,
    *,
    gate: sqlite3.Row,
    target_effect_status: str,
    target_effect_claim_id: str,
    target_effect_attempt_count: int,
    target_effect_terminal_at: float | None,
    now: float,
) -> None:
    """Record an already-terminal review target as safe quiescence evidence."""

    if target_effect_status not in {"completed", "failed", "cancelled"}:
        raise ReviewExecutionGateError("gate terminal evidence has invalid status")
    conn.execute(
        """
        UPDATE agent_review_cancellation_gates
        SET gate_status = 'terminal',
            target_effect_status = ?,
            target_effect_claim_id = ?,
            target_effect_attempt_count = ?,
            target_effect_terminal_at = ?,
            updated_at = ?
        WHERE gate_seq = ?
        """,
        (
            target_effect_status,
            target_effect_claim_id,
            target_effect_attempt_count,
            target_effect_terminal_at,
            now,
            int(gate["gate_seq"]),
        ),
    )


def _finish_execution_run(
    conn: sqlite3.Connection,
    *,
    claim: ReviewExecutionClaim,
    status: str,
    now: float,
) -> None:
    """Set one exact execution witness terminal without touching another claim."""

    updated = conn.execute(
        """
        UPDATE agent_review_execution_runs
        SET execution_status = ?, finished_at = ?
        WHERE profile_id = ?
          AND session_id = ?
          AND ownership_generation = ?
          AND review_effect_id = ?
          AND review_operation_id = ?
          AND review_effect_kind = ?
          AND review_contract_version = ?
          AND review_contract_signature = ?
          AND claim_id = ?
          AND worker_id = ?
          AND execution_status = 'running'
        """,
        (
            status,
            now,
            claim.key.profile_id,
            claim.key.session_id,
            claim.ownership_generation,
            claim.review_effect_id,
            claim.review_operation_id,
            claim.review_effect_kind,
            claim.review_contract_version,
            claim.review_contract_signature,
            claim.claim_id,
            claim.worker_id,
        ),
    )
    if updated.rowcount != 1:
        existing = _load_execution_run_for_claim(conn, claim)
        if (
            existing is None
            or not _run_matches_claim(existing, claim)
            or str(existing["execution_status"]) != status
        ):
            raise ReviewExecutionGateError("review execution witness changed")


def mark_expired_review_execution_unknown(
    conn: sqlite3.Connection,
    *,
    key: SessionKey,
    ownership_generation: int,
    review_effect_id: str,
    claim_id: str,
    worker_id: str,
    now: float,
    reason: str,
) -> int:
    """Mark a lost review execution lease unknown without authorizing replay.

    The outbox lease only proves that a worker stopped renewing it. It cannot
    prove whether a model task reached the provider, so both a still-running
    and an already-finished-but-unsettled witness become ``unknown``.
    """

    if not conn.in_transaction:
        raise ValueError("review execution unknown transition requires a transaction")
    normalized_reason = _required_text(reason, field_name="reason")
    if isinstance(ownership_generation, bool) or ownership_generation < 1:
        raise ValueError("ownership_generation must be a positive integer")
    updated = conn.execute(
        """
        UPDATE agent_review_execution_runs
        SET execution_status = 'unknown',
            finished_at = NULL,
            unknown_at = ?,
            unknown_reason = ?
        WHERE profile_id = ?
          AND session_id = ?
          AND ownership_generation = ?
          AND review_effect_id = ?
          AND claim_id = ?
          AND worker_id = ?
          AND execution_status IN ('running', 'finished')
        """,
        (
            now,
            normalized_reason,
            key.profile_id,
            key.session_id,
            ownership_generation,
            review_effect_id,
            claim_id,
            worker_id,
        ),
    )
    return int(updated.rowcount)


def _effect_attempt_count(conn: sqlite3.Connection, claim: ReviewExecutionClaim) -> int:
    """Read the target attempt count after a fenced cancellation update."""

    row = conn.execute(
        """
        SELECT attempt_count
        FROM agent_effect_outbox
        WHERE profile_id = ?
          AND session_id = ?
          AND effect_id = ?
          AND ownership_generation = ?
        """,
        (
            claim.key.profile_id,
            claim.key.session_id,
            claim.review_effect_id,
            claim.ownership_generation,
        ),
    ).fetchone()
    if row is None:
        raise ReviewExecutionGateError("review target disappeared during cancellation")
    return int(row["attempt_count"])


def _require_actor_ownership(
    database: DatabaseManager,
    conn: sqlite3.Connection,
    claim: ReviewExecutionClaim,
) -> None:
    """Require the same active Actor v2 generation as the outbox claim."""

    try:
        database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            claim.key,
            expected_generation=claim.ownership_generation,
        )
    except AgentRuntimeOwnershipError as exc:
        raise ReviewExecutionGateError("review execution ownership changed") from exc


def _clock_now(clock: Callable[[], float]) -> float:
    value = float(clock())
    if not math.isfinite(value) or value < 0:
        raise ValueError("review execution gate clock must be finite and non-negative")
    return value


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_time(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a finite non-negative number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be a finite non-negative number")
    return normalized


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _optional_text(value: object, *, field_name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    return value.strip()


def _nullable_time(value: object) -> float | None:
    if value is None:
        return None
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ReviewExecutionGateError("review execution time is invalid")
    return normalized


__all__ = [
    "REVIEW_EXECUTION_UNKNOWN_EVENT_KIND",
    "REVIEW_EXECUTION_UNKNOWN_EVENT_SOURCE",
    "ReviewExecutionClaim",
    "ReviewExecutionGateError",
    "ReviewExecutionUnknownNotice",
    "ReviewExecutionPermit",
    "ReviewExecutionPermitDisposition",
    "ReviewExecutionGateStorePort",
    "SQLiteReviewExecutionGateStore",
    "cancel_claimed_review_effect_if_gated",
    "mark_expired_review_execution_unknown",
]
