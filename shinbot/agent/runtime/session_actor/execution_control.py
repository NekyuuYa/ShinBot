"""Durable quiescence vocabulary for Actor v2 review cancellation.

The review cancellation path is deliberately narrow.  It is the only current
control effect whose completion releases a second model workflow, so it needs
proof that the superseded review cannot still be running or commit a result.
Other control families have different target and reconciliation semantics and
must not inherit this protocol by superficial similarity.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_execution_errors import (
    EffectExecutionDeferred,
)


class ReviewCancellationGateStatus(StrEnum):
    """Whether a review-cancellation gate has a safe quiescence proof."""

    CONFIRMED = "confirmed"
    PENDING = "pending"
    BLOCKED = "blocked"


class ReviewCancellationControlError(RuntimeError):
    """Base error raised while proving a review cancellation gate."""


class ReviewCancellationQuiescencePending(
    EffectExecutionDeferred,
    ReviewCancellationControlError,
):
    """Raised when a review cancellation has not yet reached quiescence."""

    def __init__(self, observation: ReviewCancellationGateObservation) -> None:
        """Retain structured proof state for executor retry diagnostics."""

        self.observation = observation
        EffectExecutionDeferred.__init__(
            self,
            "review cancellation quiescence remains pending: "
            f"{observation.blocker_code or 'unknown'}",
            delay_seconds=1.0,
        )


@dataclass(slots=True, frozen=True)
class ReviewCancellationGateRequest:
    """One immutable gate declaration for a superseded review effect."""

    key: SessionKey
    ownership_generation: int
    cancellation_effect_id: str
    request_event_id: str
    review_operation_id: str
    review_effect_id: str
    review_effect_kind: str
    review_contract_version: int
    review_contract_signature: str

    def __post_init__(self) -> None:
        """Require the exact persisted identity of both control and target work."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("review cancellation gate key must be a SessionKey")
        if (
            isinstance(self.ownership_generation, bool)
            or not isinstance(self.ownership_generation, int)
            or self.ownership_generation < 1
        ):
            raise ValueError("ownership_generation must be a positive integer")
        for field_name in (
            "cancellation_effect_id",
            "request_event_id",
            "review_operation_id",
            "review_effect_id",
            "review_effect_kind",
            "review_contract_signature",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        if self.review_effect_kind != "run_review_workflow":
            raise ValueError("review cancellation target must be run_review_workflow")
        if (
            isinstance(self.review_contract_version, bool)
            or not isinstance(self.review_contract_version, int)
            or self.review_contract_version < 1
        ):
            raise ValueError("review_contract_version must be a positive integer")


@dataclass(slots=True, frozen=True)
class ReviewCancellationGateObservation:
    """Structured status returned by the executor-owned review control port."""

    status: ReviewCancellationGateStatus
    cancellation_effect_id: str
    review_effect_id: str
    local_task_count: int = 0
    durable_running_count: int = 0
    durable_unknown_count: int = 0
    blocker_code: str = ""

    def __post_init__(self) -> None:
        """Reject ambiguous liveness facts and normalize diagnostics."""

        try:
            status = ReviewCancellationGateStatus(self.status)
        except (TypeError, ValueError) as exc:
            raise ValueError("review cancellation gate status is invalid") from exc
        for field_name in ("cancellation_effect_id", "review_effect_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        for field_name in (
            "local_task_count",
            "durable_running_count",
            "durable_unknown_count",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer")
        blocker_code = _optional_text(self.blocker_code, field_name="blocker_code")
        if status is ReviewCancellationGateStatus.CONFIRMED:
            if (
                self.local_task_count
                or self.durable_running_count
                or self.durable_unknown_count
            ):
                raise ValueError("confirmed review cancellation cannot have live tasks")
            if blocker_code:
                raise ValueError("confirmed review cancellation cannot have a blocker")
        elif not blocker_code:
            raise ValueError("non-confirmed review cancellation requires a blocker code")
        elif status is ReviewCancellationGateStatus.BLOCKED:
            if self.local_task_count or self.durable_running_count:
                raise ValueError(
                    "blocked review cancellation cannot claim live task counts"
                )
            if not self.durable_unknown_count:
                raise ValueError(
                    "blocked review cancellation requires unknown execution evidence"
                )
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "blocker_code", blocker_code)

    @property
    def confirmed(self) -> bool:
        """Return whether this observation proves the old review has quiesced."""

        return self.status is ReviewCancellationGateStatus.CONFIRMED

    def to_payload(self) -> dict[str, object]:
        """Return bounded evidence suitable for the control completion event."""

        return {
            "status": self.status.value,
            "cancellation_effect_id": self.cancellation_effect_id,
            "review_effect_id": self.review_effect_id,
            "local_task_count": self.local_task_count,
            "durable_running_count": self.durable_running_count,
            "durable_unknown_count": self.durable_unknown_count,
            "blocker_code": self.blocker_code,
        }


class ReviewCancellationControlPort(Protocol):
    """Executor-owned boundary for review cancellation proof and observation."""

    async def ensure_review_cancelled(
        self,
        request: ReviewCancellationGateRequest,
    ) -> ReviewCancellationGateObservation:
        """Observe a pre-committed gate and prove its review task is quiescent."""


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


__all__ = [
    "ReviewCancellationControlError",
    "ReviewCancellationControlPort",
    "ReviewCancellationGateObservation",
    "ReviewCancellationGateRequest",
    "ReviewCancellationGateStatus",
    "ReviewCancellationQuiescencePending",
]
