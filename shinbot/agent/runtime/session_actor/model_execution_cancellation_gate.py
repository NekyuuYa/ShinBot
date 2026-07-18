"""Durable cancellation gates for Actor-native non-review model work.

The generic model witness records whether a handler task crossed the model-call
start boundary.  A cancellation gate is a separate, actor-committed authority:
it makes one exact model effect unclaimable and waits for any witnessed task to
end.  It intentionally does not infer cancellation from a lease expiry or a
missing local task.

Only the first Actor-native idle-review-planning contract is enabled here.  The
protocol is generic in the target identity so later model effects can opt in
without reusing the historical v1/v2 control effects.
"""

from __future__ import annotations

import math
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_execution_errors import (
    EffectExecutionDeferred,
)
from shinbot.agent.runtime.session_actor.model_execution_witness import (
    ModelExecutionClaim,
    ModelExecutionPermit,
    ModelExecutionPermitDisposition,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


MODEL_EXECUTION_CANCELLATION_EFFECT_KIND = "cancel_model_execution"
"""Actor-native control effect that proves an exact model target is quiescent."""

MODEL_EXECUTION_CANCELLATION_CONTRACT_VERSION = 3
"""The first contract version with a generic durable cancellation declaration."""

MODEL_EXECUTION_CANCELLATION_COMPLETION_EVENT_KIND = (
    "ModelExecutionCancellationCompleted"
)

MODEL_EXECUTION_CANCELLATION_TARGETS = frozenset(
    {
        ("run_idle_review_planning", 3),
    }
)
"""Exact model contracts that have opted into this gate protocol."""


class ModelExecutionCancellationGateStatus(StrEnum):
    """Whether a generic model cancellation has durable quiescence proof."""

    CONFIRMED = "confirmed"
    PENDING = "pending"
    BLOCKED = "blocked"


class ModelExecutionCancellationGateError(RuntimeError):
    """Raised when a durable generic cancellation gate is inconsistent."""


class ModelExecutionCancellationBlocked(ModelExecutionCancellationGateError):
    """Raised when cancellation finds durable unknown model execution evidence."""


class ModelExecutionCancellationQuiescencePending(
    EffectExecutionDeferred,
    ModelExecutionCancellationGateError,
):
    """Raised while a witnessed model task has not yet reached a real exit."""

    def __init__(
        self,
        observation: ModelExecutionCancellationGateObservation,
    ) -> None:
        """Retain exact liveness evidence for executor retry diagnostics."""

        self.observation = observation
        EffectExecutionDeferred.__init__(
            self,
            "model execution cancellation quiescence remains pending: "
            + observation.blocker_code,
            delay_seconds=1.0,
        )


@dataclass(slots=True, frozen=True)
class ModelExecutionCancellationGateRequest:
    """One immutable actor declaration for a superseded model effect."""

    key: SessionKey
    ownership_generation: int
    cancellation_effect_id: str
    request_event_id: str
    target_operation_id: str
    target_effect_id: str
    target_effect_kind: str
    target_contract_version: int
    target_contract_signature: str

    def __post_init__(self) -> None:
        """Require the complete target fence before it reaches persistence."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("model execution cancellation key must be a SessionKey")
        _positive_int(self.ownership_generation, field_name="ownership_generation")
        for field_name in (
            "cancellation_effect_id",
            "request_event_id",
            "target_operation_id",
            "target_effect_id",
            "target_effect_kind",
            "target_contract_signature",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        _positive_int(
            self.target_contract_version,
            field_name="target_contract_version",
        )
        if not is_model_execution_cancellation_target(
            effect_kind=self.target_effect_kind,
            contract_version=self.target_contract_version,
        ):
            raise ValueError(
                "model execution cancellation target has not opted into the v3 gate"
            )


@dataclass(slots=True, frozen=True)
class ModelExecutionCancellationGateObservation:
    """Bounded durable proof returned by the generic cancellation control."""

    status: ModelExecutionCancellationGateStatus
    cancellation_effect_id: str
    target_effect_id: str
    target_effect_kind: str
    target_operation_id: str
    target_claim_id: str = ""
    target_worker_id: str = ""
    durable_running_count: int = 0
    durable_unknown_count: int = 0
    blocker_code: str = ""

    def __post_init__(self) -> None:
        """Reject ambiguous observations before they become mailbox payloads."""

        try:
            status = ModelExecutionCancellationGateStatus(self.status)
        except (TypeError, ValueError) as exc:
            raise ValueError("model execution cancellation gate status is invalid") from exc
        for field_name in (
            "cancellation_effect_id",
            "target_effect_id",
            "target_effect_kind",
            "target_operation_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        for field_name in ("target_claim_id", "target_worker_id", "blocker_code"):
            object.__setattr__(
                self,
                field_name,
                _optional_text(getattr(self, field_name), field_name=field_name),
            )
        for field_name in ("durable_running_count", "durable_unknown_count"):
            _nonnegative_int(getattr(self, field_name), field_name=field_name)
        if status is ModelExecutionCancellationGateStatus.CONFIRMED:
            if self.durable_running_count or self.durable_unknown_count:
                raise ValueError("confirmed model cancellation cannot retain live evidence")
            if self.blocker_code:
                raise ValueError("confirmed model cancellation cannot have a blocker")
        elif not self.blocker_code:
            raise ValueError("non-confirmed model cancellation requires a blocker code")
        elif status is ModelExecutionCancellationGateStatus.BLOCKED:
            if self.durable_running_count:
                raise ValueError("blocked model cancellation cannot claim running tasks")
            if not self.durable_unknown_count:
                raise ValueError("blocked model cancellation requires unknown evidence")
        object.__setattr__(self, "status", status)

    @property
    def confirmed(self) -> bool:
        """Return whether the superseded model task is proven quiescent."""

        return self.status is ModelExecutionCancellationGateStatus.CONFIRMED

    def to_payload(self) -> dict[str, object]:
        """Serialize the complete bounded control outcome for the reducer."""

        return {
            "status": self.status.value,
            "cancellation_effect_id": self.cancellation_effect_id,
            "target_effect_id": self.target_effect_id,
            "target_effect_kind": self.target_effect_kind,
            "target_operation_id": self.target_operation_id,
            "target_claim_id": self.target_claim_id,
            "target_worker_id": self.target_worker_id,
            "durable_running_count": self.durable_running_count,
            "durable_unknown_count": self.durable_unknown_count,
            "blocker_code": self.blocker_code,
        }


class ModelExecutionCancellationControlPort(Protocol):
    """Executor-owned boundary that proves one model target is quiescent."""

    async def ensure_model_execution_cancelled(
        self,
        request: ModelExecutionCancellationGateRequest,
    ) -> ModelExecutionCancellationGateObservation:
        """Observe a committed target gate without inferring task liveness."""


def is_model_execution_cancellation_target(
    *,
    effect_kind: str,
    contract_version: int,
) -> bool:
    """Return whether an exact model contract opted into generic cancellation."""

    if isinstance(contract_version, bool) or not isinstance(contract_version, int):
        return False
    return (str(effect_kind or "").strip(), contract_version) in (
        MODEL_EXECUTION_CANCELLATION_TARGETS
    )


class SQLiteModelExecutionCancellationGateStore:
    """Observe generic cancellation gates using the witness persistence domain."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Bind the control port to the same database as effects and witnesses."""

        self._database = database
        self._clock = clock or time.time

    @property
    def persistence_domain(self) -> object:
        """Return the transaction domain shared by all gate participants."""

        return self._database

    async def ensure_model_execution_cancelled(
        self,
        request: ModelExecutionCancellationGateRequest,
    ) -> ModelExecutionCancellationGateObservation:
        """Return a durable quiescence proof or a stable explicit blocker."""

        now = _clock_now(self._clock)
        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _require_actor_ownership(
                self._database,
                conn,
                request.key,
                ownership_generation=request.ownership_generation,
            )
            gate = _load_gate_for_request(conn, request)
            if gate is None:
                return ModelExecutionCancellationGateObservation(
                    status=ModelExecutionCancellationGateStatus.PENDING,
                    cancellation_effect_id=request.cancellation_effect_id,
                    target_effect_id=request.target_effect_id,
                    target_effect_kind=request.target_effect_kind,
                    target_operation_id=request.target_operation_id,
                    blocker_code="model_execution_cancellation_gate_missing",
                )
            _validate_gate_request(gate, request)
            return _observe_gate(conn, request=request, gate=gate, now=now)


def permit_model_execution_start_if_gated(
    conn: sqlite3.Connection,
    *,
    claim: ModelExecutionClaim,
    now: float,
) -> ModelExecutionPermit | None:
    """Cancel or defer one claimed model effect before it creates a task.

    The caller already owns the witness transaction.  A gate is checked before
    any new start witness is inserted, which is the ordering that closes the
    claim-to-task-start race.
    """

    gate = _load_gate_for_claim(conn, claim)
    if gate is None:
        return None
    gate_status = _gate_status(gate)
    if gate_status == "blocked":
        return _deferred_permit(claim, "model_execution_cancellation_blocked")
    runs = _load_execution_runs(conn, claim)
    _validate_runs_for_claim(runs, claim)
    if any(str(run["execution_status"]) == "unknown" for run in runs):
        _mark_gate_blocked(
            conn,
            gate=gate,
            blocker_code="model_execution_witness_unknown",
            now=now,
        )
        return _deferred_permit(claim, "model_execution_witness_unknown")
    if any(str(run["execution_status"]) == "running" for run in runs):
        return _deferred_permit(claim, "model_execution_witness_running")
    if any(str(run["execution_status"]) == "finished" for run in runs):
        cancellation_effect_id = _cancel_gate_target(
            conn,
            claim=claim,
            gate=gate,
            now=now,
            evidence="model_execution_gate_finished_before_replay",
        )
        return _cancelled_permit(claim, cancellation_effect_id)
    cancellation_effect_id = _cancel_gate_target(
        conn,
        claim=claim,
        gate=gate,
        now=now,
        evidence="model_execution_gate_before_task_start",
    )
    return _cancelled_permit(claim, cancellation_effect_id)


def finish_model_execution_if_gated(
    conn: sqlite3.Connection,
    *,
    claim: ModelExecutionClaim,
    now: float,
) -> ModelExecutionPermit | None:
    """Acknowledge a gate after the real handler task has already exited."""

    gate = _load_gate_for_claim(conn, claim)
    if gate is None:
        return None
    if _gate_status(gate) == "blocked":
        return _deferred_permit(claim, "model_execution_cancellation_blocked")
    cancellation_effect_id = _cancel_gate_target(
        conn,
        claim=claim,
        gate=gate,
        now=now,
        evidence="model_execution_gate_task_finished",
    )
    return _cancelled_permit(claim, cancellation_effect_id)


def cancel_claimed_model_execution_if_gated(
    conn: sqlite3.Connection,
    *,
    claim: ModelExecutionClaim,
    now: float,
    evidence: str,
) -> str | None:
    """Fence every outbox mutation against a committed generic gate.

    A running task is allowed to unwind, but its effect cannot settle after the
    actor superseded it.  Unknown evidence is deliberately surfaced as a
    blocker instead of being rewritten as a cancellation result.
    """

    gate = _load_gate_for_claim(conn, claim)
    if gate is None:
        return None
    if _gate_status(gate) == "blocked":
        raise ModelExecutionCancellationBlocked(
            "model execution cancellation gate is blocked by unknown witness"
        )
    return _cancel_gate_target(
        conn,
        claim=claim,
        gate=gate,
        now=now,
        evidence=evidence,
    )


def _observe_gate(
    conn: sqlite3.Connection,
    *,
    request: ModelExecutionCancellationGateRequest,
    gate: sqlite3.Row,
    now: float,
) -> ModelExecutionCancellationGateObservation:
    """Observe one pre-committed gate without treating a lease as liveness."""

    gate_status = _gate_status(gate)
    if gate_status == "blocked":
        return _blocked_observation(
            gate,
            request=request,
            blocker_code=(
                _optional_text(gate["blocker_code"], field_name="blocker_code")
                or "model_execution_witness_unknown"
            ),
        )
    if gate_status == "terminal":
        _require_terminal_gate_quiescence(conn, gate=gate, request=request)
        return _confirmed_observation(request)

    target_claim = _claim_from_gate(gate, request=request)
    runs = _load_execution_runs(conn, target_claim)
    _validate_runs_for_claim(runs, target_claim)
    statuses = {str(run["execution_status"]) for run in runs}
    if "unknown" in statuses:
        _mark_gate_blocked(
            conn,
            gate=gate,
            blocker_code="model_execution_witness_unknown",
            now=now,
        )
        return _blocked_observation(
            gate,
            request=request,
            blocker_code="model_execution_witness_unknown",
        )
    if "running" in statuses:
        _cancel_gate_target(
            conn,
            claim=target_claim,
            gate=gate,
            now=now,
            evidence="model_execution_gate_control_running",
        )
        return ModelExecutionCancellationGateObservation(
            status=ModelExecutionCancellationGateStatus.PENDING,
            cancellation_effect_id=request.cancellation_effect_id,
            target_effect_id=request.target_effect_id,
            target_effect_kind=request.target_effect_kind,
            target_operation_id=request.target_operation_id,
            target_claim_id=target_claim.claim_id,
            target_worker_id=target_claim.worker_id,
            durable_running_count=1,
            blocker_code="model_execution_running",
        )
    cancellation_effect_id = _cancel_gate_target(
        conn,
        claim=target_claim,
        gate=gate,
        now=now,
        evidence=(
            "model_execution_gate_finished" if "finished" in statuses else "model_execution_gate_unstarted"
        ),
    )
    if cancellation_effect_id != request.cancellation_effect_id:
        raise ModelExecutionCancellationGateError("model cancellation gate id changed")
    return _confirmed_observation(request)


def _load_gate_for_request(
    conn: sqlite3.Connection,
    request: ModelExecutionCancellationGateRequest,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM agent_model_execution_cancellation_gates
        WHERE profile_id = ? AND session_id = ?
          AND ownership_generation = ? AND cancellation_effect_id = ?
        """,
        (
            request.key.profile_id,
            request.key.session_id,
            request.ownership_generation,
            request.cancellation_effect_id,
        ),
    ).fetchone()


def _load_gate_for_claim(
    conn: sqlite3.Connection,
    claim: ModelExecutionClaim,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM agent_model_execution_cancellation_gates
        WHERE profile_id = ? AND session_id = ?
          AND ownership_generation = ? AND target_effect_id = ?
          AND target_operation_id = ? AND target_effect_kind = ?
          AND target_contract_version = ? AND target_contract_signature = ?
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
        ),
    ).fetchone()


def _validate_gate_request(
    gate: sqlite3.Row,
    request: ModelExecutionCancellationGateRequest,
) -> None:
    persisted = (
        _required_text(gate["request_event_id"], field_name="request_event_id"),
        _required_text(gate["target_operation_id"], field_name="target_operation_id"),
        _required_text(gate["target_effect_id"], field_name="target_effect_id"),
        _required_text(gate["target_effect_kind"], field_name="target_effect_kind"),
        _positive_int(gate["target_contract_version"], field_name="target_contract_version"),
        _required_text(
            gate["target_contract_signature"],
            field_name="target_contract_signature",
        ),
    )
    requested = (
        request.request_event_id,
        request.target_operation_id,
        request.target_effect_id,
        request.target_effect_kind,
        request.target_contract_version,
        request.target_contract_signature,
    )
    if persisted != requested:
        raise ModelExecutionCancellationGateError(
            "model execution cancellation gate identity changed"
        )


def _claim_from_gate(
    gate: sqlite3.Row,
    *,
    request: ModelExecutionCancellationGateRequest,
) -> ModelExecutionClaim:
    claim_id = _required_text(gate["target_claim_id"], field_name="target_claim_id")
    worker_id = _required_text(gate["target_worker_id"], field_name="target_worker_id")
    return ModelExecutionClaim(
        key=request.key,
        ownership_generation=request.ownership_generation,
        effect_id=request.target_effect_id,
        operation_id=request.target_operation_id,
        effect_kind=request.target_effect_kind,
        contract_version=request.target_contract_version,
        contract_signature=request.target_contract_signature,
        claim_id=claim_id,
        worker_id=worker_id,
    )


def _load_execution_runs(
    conn: sqlite3.Connection,
    claim: ModelExecutionClaim,
) -> tuple[sqlite3.Row, ...]:
    rows = conn.execute(
        """
        SELECT * FROM agent_model_execution_runs
        WHERE profile_id = ? AND session_id = ?
          AND ownership_generation = ? AND effect_id = ?
        ORDER BY run_seq ASC
        """,
        (
            claim.key.profile_id,
            claim.key.session_id,
            claim.ownership_generation,
            claim.effect_id,
        ),
    ).fetchall()
    return tuple(rows)


def _validate_runs_for_claim(
    runs: tuple[sqlite3.Row, ...],
    claim: ModelExecutionClaim,
) -> None:
    if len(runs) > 1:
        raise ModelExecutionCancellationGateError(
            "model execution cancellation target has multiple witnesses"
        )
    for run in runs:
        persisted = (
            str(run["profile_id"]),
            str(run["session_id"]),
            int(run["ownership_generation"]),
            str(run["effect_id"]),
            str(run["operation_id"]),
            str(run["effect_kind"]),
            int(run["contract_version"]),
            str(run["contract_signature"]),
            str(run["claim_id"]),
            str(run["worker_id"]),
        )
        expected = (
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
        )
        if persisted != expected:
            raise ModelExecutionCancellationGateError(
                "model execution witness changed gate claim evidence"
            )


def _cancel_gate_target(
    conn: sqlite3.Connection,
    *,
    claim: ModelExecutionClaim,
    gate: sqlite3.Row,
    now: float,
    evidence: str,
) -> str:
    """Cancel an exact gated target while preserving witness truthfulness."""

    cancellation_effect_id = _required_text(
        gate["cancellation_effect_id"],
        field_name="cancellation_effect_id",
    )
    gate_status = _gate_status(gate)
    if gate_status == "blocked":
        raise ModelExecutionCancellationBlocked(
            "model execution cancellation gate is blocked by unknown witness"
        )
    _validate_gate_claim(gate, claim)
    effect = _load_target_effect(conn, claim)
    runs = _load_execution_runs(conn, claim)
    _validate_runs_for_claim(runs, claim)
    statuses = {str(run["execution_status"]) for run in runs}
    if "unknown" in statuses:
        _mark_gate_blocked(
            conn,
            gate=gate,
            blocker_code="model_execution_witness_unknown",
            now=now,
        )
        raise ModelExecutionCancellationBlocked(
            "model execution cancellation gate is blocked by unknown witness"
        )
    execution_status = "running" if "running" in statuses else (
        "finished" if "finished" in statuses else "none"
    )
    effect_status = str(effect["status"])
    target_effect_terminal_at = now
    if effect_status == "processing":
        updated = conn.execute(
            """
            UPDATE agent_effect_outbox
            SET status = 'cancelled', claim_id = '', lease_owner = '',
                lease_until = NULL, completed_at = ?, updated_at = ?, last_error = ?
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
              AND ownership_generation = ? AND operation_id = ? AND kind = ?
              AND contract_version = ? AND contract_signature = ?
              AND status = 'processing' AND claim_id = ? AND lease_owner = ?
            """,
            (
                now,
                now,
                _required_text(evidence, field_name="evidence")
                + ":"
                + cancellation_effect_id,
                claim.key.profile_id,
                claim.key.session_id,
                claim.effect_id,
                claim.ownership_generation,
                claim.operation_id,
                claim.effect_kind,
                claim.contract_version,
                claim.contract_signature,
                claim.claim_id,
                claim.worker_id,
            ),
        )
        if updated.rowcount != 1:
            raise ModelExecutionCancellationGateError(
                "model execution cancellation target claim changed"
            )
        effect_status = "cancelled"
    elif effect_status == "cancelled":
        target_effect_terminal_at = _nonnegative_time(
            effect["completed_at"],
            field_name="target_effect_terminal_at",
        )
    else:
        raise ModelExecutionCancellationGateError(
            "model execution cancellation target is unexpectedly terminal"
        )

    if execution_status == "running":
        _update_gate(
            conn,
            gate=gate,
            gate_status="cancelled",
            target_effect_status="cancelled",
            target_execution_status="running",
            target_effect_terminal_at=target_effect_terminal_at,
            blocker_code="",
            now=now,
        )
    else:
        _update_gate(
            conn,
            gate=gate,
            gate_status="terminal",
            target_effect_status="cancelled",
            target_execution_status=execution_status,
            target_effect_terminal_at=target_effect_terminal_at,
            blocker_code="",
            now=now,
        )
    return cancellation_effect_id


def _load_target_effect(
    conn: sqlite3.Connection,
    claim: ModelExecutionClaim,
) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT * FROM agent_effect_outbox
        WHERE profile_id = ? AND session_id = ? AND effect_id = ?
        """,
        (claim.key.profile_id, claim.key.session_id, claim.effect_id),
    ).fetchone()
    if row is None:
        raise ModelExecutionCancellationGateError("model cancellation target outbox row is missing")
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
        raise ModelExecutionCancellationGateError(
            "model cancellation target outbox identity changed"
        )
    return row


def _validate_gate_claim(gate: sqlite3.Row, claim: ModelExecutionClaim) -> None:
    if (
        _required_text(gate["target_claim_id"], field_name="target_claim_id")
        != claim.claim_id
        or _required_text(gate["target_worker_id"], field_name="target_worker_id")
        != claim.worker_id
    ):
        raise ModelExecutionCancellationGateError(
            "model cancellation gate claim evidence changed"
        )


def _mark_gate_blocked(
    conn: sqlite3.Connection,
    *,
    gate: sqlite3.Row,
    blocker_code: str,
    now: float,
) -> None:
    _update_gate(
        conn,
        gate=gate,
        gate_status="blocked",
        target_effect_status="processing",
        target_execution_status="unknown",
        target_effect_terminal_at=None,
        blocker_code=_required_text(blocker_code, field_name="blocker_code"),
        now=now,
    )


def _update_gate(
    conn: sqlite3.Connection,
    *,
    gate: sqlite3.Row,
    gate_status: str,
    target_effect_status: str,
    target_execution_status: str,
    target_effect_terminal_at: float | None,
    blocker_code: str,
    now: float,
) -> None:
    updated = conn.execute(
        """
        UPDATE agent_model_execution_cancellation_gates
        SET gate_status = ?, target_effect_status = ?,
            target_execution_status = ?, target_effect_terminal_at = ?,
            blocker_code = ?, updated_at = ?
        WHERE gate_seq = ?
        """,
        (
            gate_status,
            target_effect_status,
            target_execution_status,
            target_effect_terminal_at,
            blocker_code,
            now,
            int(gate["gate_seq"]),
        ),
    )
    if updated.rowcount != 1:
        raise ModelExecutionCancellationGateError("model execution gate changed concurrently")


def _require_terminal_gate_quiescence(
    conn: sqlite3.Connection,
    *,
    gate: sqlite3.Row,
    request: ModelExecutionCancellationGateRequest,
) -> None:
    if _gate_status(gate) != "terminal":
        raise ModelExecutionCancellationGateError("model execution gate is not terminal")
    target_claim_id = _optional_text(
        gate["target_claim_id"],
        field_name="target_claim_id",
    )
    target_worker_id = _optional_text(
        gate["target_worker_id"],
        field_name="target_worker_id",
    )
    target_execution_status = _required_text(
        gate["target_execution_status"],
        field_name="target_execution_status",
    )
    runs = conn.execute(
        """
        SELECT * FROM agent_model_execution_runs
        WHERE profile_id = ? AND session_id = ?
          AND ownership_generation = ? AND effect_id = ?
        ORDER BY run_seq ASC
        """,
        (
            request.key.profile_id,
            request.key.session_id,
            request.ownership_generation,
            request.target_effect_id,
        ),
    ).fetchall()
    if target_execution_status == "none":
        if runs:
            raise ModelExecutionCancellationGateError(
                "terminal model execution gate unexpectedly retains a witness"
            )
    elif target_execution_status == "finished":
        if len(runs) != 1:
            raise ModelExecutionCancellationGateError(
                "terminal model execution gate finished witness is missing or ambiguous"
            )
        run = runs[0]
        if (
            str(run["operation_id"]) != request.target_operation_id
            or str(run["effect_kind"]) != request.target_effect_kind
            or int(run["contract_version"]) != request.target_contract_version
            or str(run["contract_signature"]) != request.target_contract_signature
            or str(run["claim_id"]) != target_claim_id
            or (target_worker_id and str(run["worker_id"]) != target_worker_id)
            or str(run["execution_status"]) != "finished"
            or run["finished_at"] is None
        ):
            raise ModelExecutionCancellationGateError(
                "terminal model execution gate witness changed identity"
            )
    else:
        raise ModelExecutionCancellationGateError(
            "terminal model execution gate retains a live execution status"
        )
    effect = conn.execute(
        """
        SELECT ownership_generation, operation_id, kind, contract_version,
               contract_signature, status, attempt_count, claim_id,
               lease_owner, lease_until, completed_at
        FROM agent_effect_outbox
        WHERE profile_id = ? AND session_id = ? AND effect_id = ?
        """,
        (request.key.profile_id, request.key.session_id, request.target_effect_id),
    ).fetchone()
    if effect is None or (
        int(effect["ownership_generation"]) != request.ownership_generation
        or str(effect["operation_id"]) != request.target_operation_id
        or str(effect["kind"]) != request.target_effect_kind
        or int(effect["contract_version"]) != request.target_contract_version
        or str(effect["contract_signature"]) != request.target_contract_signature
        or str(effect["status"]) != str(gate["target_effect_status"])
        or int(effect["attempt_count"]) != int(gate["target_effect_attempt_count"])
        or str(effect["status"]) not in {"completed", "failed", "cancelled"}
        or effect["completed_at"] != gate["target_effect_terminal_at"]
    ):
        raise ModelExecutionCancellationGateError(
            "terminal model execution gate target changed identity"
        )
    if effect["lease_owner"] or effect["lease_until"] is not None or effect["completed_at"] is None:
        raise ModelExecutionCancellationGateError(
            "terminal model execution gate target retains a lease"
        )
    if str(effect["status"]) == "cancelled":
        if effect["claim_id"]:
            raise ModelExecutionCancellationGateError(
                "terminal model execution gate retained a cancelled target claim"
            )
    elif str(effect["claim_id"]) != target_claim_id:
        raise ModelExecutionCancellationGateError(
            "terminal model execution gate target claim changed"
        )


def _confirmed_observation(
    request: ModelExecutionCancellationGateRequest,
) -> ModelExecutionCancellationGateObservation:
    return ModelExecutionCancellationGateObservation(
        status=ModelExecutionCancellationGateStatus.CONFIRMED,
        cancellation_effect_id=request.cancellation_effect_id,
        target_effect_id=request.target_effect_id,
        target_effect_kind=request.target_effect_kind,
        target_operation_id=request.target_operation_id,
    )


def _blocked_observation(
    gate: sqlite3.Row,
    *,
    request: ModelExecutionCancellationGateRequest,
    blocker_code: str,
) -> ModelExecutionCancellationGateObservation:
    return ModelExecutionCancellationGateObservation(
        status=ModelExecutionCancellationGateStatus.BLOCKED,
        cancellation_effect_id=request.cancellation_effect_id,
        target_effect_id=request.target_effect_id,
        target_effect_kind=request.target_effect_kind,
        target_operation_id=request.target_operation_id,
        target_claim_id=_optional_text(
            gate["target_claim_id"],
            field_name="target_claim_id",
        ),
        target_worker_id=_optional_text(
            gate["target_worker_id"],
            field_name="target_worker_id",
        ),
        durable_unknown_count=1,
        blocker_code=blocker_code,
    )


def _cancelled_permit(
    claim: ModelExecutionClaim,
    cancellation_effect_id: str,
) -> ModelExecutionPermit:
    return ModelExecutionPermit(
        disposition=ModelExecutionPermitDisposition.CANCELLED,
        claim=claim,
        cancellation_effect_id=cancellation_effect_id,
    )


def _deferred_permit(
    claim: ModelExecutionClaim,
    blocker_code: str,
) -> ModelExecutionPermit:
    return ModelExecutionPermit(
        disposition=ModelExecutionPermitDisposition.DEFERRED,
        claim=claim,
        blocker_code=blocker_code,
    )


def _gate_status(gate: sqlite3.Row) -> str:
    value = _required_text(gate["gate_status"], field_name="gate_status")
    if value not in {"requested", "cancelled", "terminal", "blocked"}:
        raise ModelExecutionCancellationGateError("model execution gate has invalid status")
    return value


def _require_actor_ownership(
    database: DatabaseManager,
    conn: sqlite3.Connection,
    key: SessionKey,
    *,
    ownership_generation: int,
) -> None:
    try:
        database.agent_runtime_ownership.require_actor_v2_in_transaction(
            conn,
            key,
            expected_generation=ownership_generation,
        )
    except AgentRuntimeOwnershipError as exc:
        raise ModelExecutionCancellationGateError(
            "model execution cancellation ownership changed"
        ) from exc


def _clock_now(clock: Callable[[], float]) -> float:
    value = float(clock())
    if not math.isfinite(value) or value < 0:
        raise ValueError("model execution cancellation clock must be finite and non-negative")
    return value


def _nonnegative_time(value: object, *, field_name: str) -> float:
    """Return one persisted finite timestamp or reject missing terminal proof."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ModelExecutionCancellationGateError(f"{field_name} must be numeric")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ModelExecutionCancellationGateError(
            f"{field_name} must be finite and non-negative"
        )
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


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


__all__ = [
    "MODEL_EXECUTION_CANCELLATION_COMPLETION_EVENT_KIND",
    "MODEL_EXECUTION_CANCELLATION_CONTRACT_VERSION",
    "MODEL_EXECUTION_CANCELLATION_EFFECT_KIND",
    "MODEL_EXECUTION_CANCELLATION_TARGETS",
    "ModelExecutionCancellationControlPort",
    "ModelExecutionCancellationBlocked",
    "ModelExecutionCancellationGateError",
    "ModelExecutionCancellationGateObservation",
    "ModelExecutionCancellationGateRequest",
    "ModelExecutionCancellationGateStatus",
    "ModelExecutionCancellationQuiescencePending",
    "SQLiteModelExecutionCancellationGateStore",
    "cancel_claimed_model_execution_if_gated",
    "finish_model_execution_if_gated",
    "is_model_execution_cancellation_target",
    "permit_model_execution_start_if_gated",
]
